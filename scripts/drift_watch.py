# /scripts/drift_watch.py
"""
DriftWatch: Monitoreo de drift de datos y disparo de reentrenos (Spark + HDFS + Prometheus + Jenkins).

Este módulo ejecuta un bucle continuo que:
  1) Lee datos de HDFS mediante Spark.
  2) Muestra aleatoria estable (acotada) a pandas para columnas numéricas y categóricas.
  3) Evalúa drift por columna:
       - Numéricas: Kolmogorov–Smirnov (KS).
       - Categóricas: Chi-cuadrado (tabla de contingencia).
  4) Calcula PSI (Population Stability Index) sobre una columna de score.
  5) Expone métricas Prometheus (p-values, flags de drift, PSI, ratio de positivos).
  6) Dispara un job de Jenkins para reentrenar el modelo si el drift supera umbrales,
     con política de enfriamiento (cooldown) y anti-retigger por “borde” (EDGE_ONLY).

Arquitectura
------------
- Spark: lectura/parquet desde HDFS y muestreo controlado.
- Prometheus: servidor HTTP embebido para scrapeo.
- Jenkins: integración con autenticación por API Token/Crumb.

Métricas Prometheus
-------------------
- drift_score_psi{model}: PSI de la columna de score (float).
- predicted_positive_ratio{model}: proporción de predicciones positivas (0–1).
- jenkins_retrain_triggers_total{model}: contador de triggers enviados a Jenkins.
- drift_detected{col}: 1 si hay drift por columna, 0 en caso contrario.
- pvalue{col}: valor-p por columna (KS o Chi-cuadrado según tipo).

Variables de Entorno (principales)
----------------------------------
- SPARK_MASTER_URL (str): URL del Spark Master (p.ej. "spark://spark-master:7077").
- HDFS_URI (str): URI del HDFS (p.ej. "hdfs://namenode:9000").
- DATA_PATH (str): Ruta parquet en HDFS (p.ej. "hdfs:///datalake/raw/bank_transactions").
- DRIFT_ALPHA (float): Umbral de significancia para p-values (default: 0.01).
- EXPORTER_PORT (int): Puerto del exporter Prometheus (default: 8010).
- LOOP_SECONDS (int): Periodicidad del loop principal en segundos (default: 30).
- DRIFT_COOLDOWN_SECONDS (int): Enfriamiento entre triggers (default: 300).
- TRIGGER_EDGE_ONLY ("1"/"0"): Evita retriggers mientras se mantenga el estado de alerta (default: "1").
- DRIFT_CLEAR_STREAK (int): Rachas sin drift para “rearmar” el trigger (default: 3).
- MODEL_NAME (str): Etiqueta de modelo para métricas (default: "fraud").
- SCORE_COL (str): Nombre preferido de la columna de score (default: "score").
- POSITIVE_THRESHOLD (float): Umbral para positivos (default: 0.5).
- PSI_ALERT (float): Umbral de alerta para PSI (default: 0.2).
- SAMPLE_MAX (int): Techo de filas para muestras pandas (default: 1000).

Jenkins
-------
- JENKINS_URL (str): Base URL de Jenkins (p.ej. "http://jenkins:8080").
- JENKINS_JOB (str): Nombre del job a disparar (p.ej. "retrain-model").
- JENKINS_USER (str) y JENKINS_API_TOKEN (str): Autenticación recomendada.
- JENKINS_TOKEN (str): Modo legacy (no recomendado).

Notas operativas
----------------
- El muestreo usa `sample(...).limit(n)` para estabilidad y techo duro de filas.
- Si no se encuentra `SCORE_COL`, se intenta auto-detección; si falla, PSI queda en NaN.
- EDGE_ONLY evita retriggers continuos: se dispara al “flanco de subida” y se re-arma
  tras `DRIFT_CLEAR_STREAK` ciclos sin drift.
"""

import os
import time
import csv
from pathlib import Path
import numpy as np
import pandas as pd
import requests

from prometheus_client import start_http_server, Gauge, Counter
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import NumericType, StringType
from pyspark.sql.functions import col, lit
from scipy.stats import ks_2samp, chi2_contingency

# ========= Config =========
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
HDFS_URI     = os.getenv("HDFS_URI", "hdfs://namenode:9000")
DATA_PATH    = os.getenv("DATA_PATH", "hdfs:///datalake/raw/bank_transactions")

ALPHA         = float(os.getenv("DRIFT_ALPHA", "0.01"))
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "8010"))
LOOP_SECONDS  = int(os.getenv("LOOP_SECONDS", "30"))
COOLDOWN_S    = int(os.getenv("DRIFT_COOLDOWN_SECONDS", "300"))  # 5 min
WINDOW_MIN    = int(os.getenv("DRIFT_WINDOW_MINUTES", "5"))

EDGE_ONLY = os.getenv("TRIGGER_EDGE_ONLY", "1") == "1"
DRIFT_CLEAR_STREAK = int(os.getenv("DRIFT_CLEAR_STREAK", "3"))

MODEL_NAME         = os.getenv("MODEL_NAME", "fraud")
SCORE_COL          = os.getenv("SCORE_COL", "score")
POSITIVE_THRESHOLD = float(os.getenv("POSITIVE_THRESHOLD", "0.5"))
PSI_ALERT          = float(os.getenv("PSI_ALERT", "0.2"))
SAMPLE_MAX         = int(os.getenv("SAMPLE_MAX", "1000"))
#SAMPLE_MAX         = int(os.getenv("SAMPLE_MAX", "5000"))

# Jenkins
JENKINS_URL       = os.getenv("JENKINS_URL", "http://jenkins:8080")
JENKINS_JOB       = os.getenv("JENKINS_JOB", "retrain-model")
# Ruta recomendada: API token de usuario
JENKINS_USER      = os.getenv("JENKINS_USER", "")
JENKINS_API_TOKEN = os.getenv("JENKINS_API_TOKEN", "")
# Ruta legacy (no recomendada, mejor dejar vacío)
JENKINS_TOKEN     = os.getenv("JENKINS_TOKEN", "")

# ========= Métricas Prometheus =========
# (1) Mini panel (con etiqueta de modelo)
g_score_psi = Gauge("drift_score_psi", "Population Stability Index for score", ["model"])
g_pos_ratio = Gauge("predicted_positive_ratio", "Ratio of positive predictions", ["model"])
c_triggers  = Counter("jenkins_retrain_triggers_total", "Total Jenkins retrain triggers", ["model"])

# (2) Por columna
g_drift_col = Gauge("drift_detected", "1 if drift detected else 0", ["col"])
g_pval_col  = Gauge("pvalue", "p-value per column", ["col"])

_last_trigger_ts = 0.0
_prev_alert = False          # True = ya disparamos para el drift actual
_clear_ok_streak = 0         # rachas sin drift para rearmar

# ========= CSV logging =========
def _append_drift_log(row: dict):
    """Append a single row with drift metrics to a CSV.

    Controlled by the env var DRIFT_LOG_FILE; if empty, does nothing.
    The file is created with a header if it doesn't exist.
    """
    path = os.getenv("DRIFT_LOG_FILE", "")
    if not path:
        return
    fp = Path(path)
    fp.parent.mkdir(parents=True, exist_ok=True)
    # stable header: timestamp, scenario, model, psi, pos_ratio, drift_any, score_col
    # plus dynamic p-values per column as p_<col>
    base_fields = ["timestamp", "scenario", "model", "psi", "pos_ratio", "drift_any", "score_col"]
    # Ensure stable order for p_ columns by sorting keys
    p_keys = sorted(k for k in row.keys() if k.startswith("p_"))
    fieldnames = base_fields + p_keys
    file_exists = fp.exists()
    with fp.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in fieldnames})

# ========= Spark =========
def get_spark():
    """Crea y retorna una sesión de Spark configurada para DriftWatch.

    La configuración incluye:
      - spark.master desde `SPARK_MASTER_URL` (o "local[1]" por defecto).
      - fs.defaultFS para acceso a HDFS desde `HDFS_URI`.
      - Deshabilita UI de Spark para reducir overhead.
      - Parámetros conservadores de memoria/timeout para estabilidad en contenedores.

    Returns:
        pyspark.sql.SparkSession: Sesión de Spark lista para leer parquet desde HDFS.
    """
    master = os.getenv("SPARK_MASTER_URL", "local[1]")
    conf = (
        SparkConf()
        .setAppName("DriftWatch")
        .set("spark.master", master)
        .set("spark.hadoop.fs.defaultFS", os.getenv("HDFS_URI", "hdfs://namenode:9000"))
        .set("spark.ui.enabled", "false")
        .set("spark.driver.memory", "584m")                # 384m alcanza y baja presión
        .set("spark.network.timeout", "120s")              # más tolerancia si hay GC/IO
        .set("spark.executor.heartbeatInterval", "30s")
    )
    return SparkSession.builder.config(conf=conf).getOrCreate()

# ========= Utils =========
def numeric_columns(sdf):
    """Devuelve los nombres de columnas numéricas relevantes (excluye year/month/day/hour).

    Args:
        sdf (pyspark.sql.DataFrame): DataFrame de Spark.

    Returns:
        list[str]: Lista de nombres de columnas numéricas útiles para pruebas KS/PSI.
    """
    return [
        f.name for f in sdf.schema.fields
        if isinstance(f.dataType, NumericType) and f.name not in ("year", "month", "day", "hour")
    ]

def string_columns(sdf):
    """Devuelve los nombres de columnas categóricas (tipo StringType).

    Args:
        sdf (pyspark.sql.DataFrame): DataFrame de Spark.

    Returns:
        list[str]: Lista de nombres de columnas de tipo string para Chi-cuadrado.
    """
    return [f.name for f in sdf.schema.fields if isinstance(f.dataType, StringType)]

def to_pdf_sample(sdf, cols, n=SAMPLE_MAX, seed=42):
    """Extrae una muestra estable (acotada) a pandas para un subconjunto de columnas.

    El muestreo combina `sample(False, 0.2, seed)` con `limit(n)` para:
      - Evitar dependencia de fracciones exactas.
      - Poner un techo duro de filas (n).
      - Mejorar la estabilidad en escenarios distribuidos.

    Si algo falla, aplica un fallback con `limit(min(200, n))`.

    Args:
        sdf (pyspark.sql.DataFrame): DataFrame de Spark fuente.
        cols (list[str]): Columnas a seleccionar (se filtran a las existentes).
        n (int): Máximo de filas a retornar.
        seed (int): Semilla pseudoaleatoria para `sample`.

    Returns:
        pandas.DataFrame: Subconjunto en pandas sin nulos para las columnas requeridas.
    """
    cols = [c for c in cols if c in sdf.columns]
    if not cols or n <= 0:
        return pd.DataFrame()

    # limit() garantiza un techo de filas: no dependemos de frac
    # (la muestra no es perfectamente aleatoria pero es MUCHO más estable)
    try:
        pdf = (sdf.select(*cols)
                 .dropna()
                 .sample(False, 0.2, seed)   # pequeña fracción
                 .limit(int(n))              # techo duro
                 .toPandas())
    except Exception:
        # fallback aún más chico
        pdf = (sdf.select(*cols).dropna().limit(min(200, int(n))).toPandas())

    return pdf

def ks_num(a, b):
    """Calcula el p-valor de Kolmogorov–Smirnov para dos muestras numéricas.

    Si alguna muestra tiene < 20 observaciones, retorna 1.0 (sin evidencia de diferencia).

    Args:
        a (array-like): Muestra de referencia.
        b (array-like): Muestra reciente/comparativa.

    Returns:
        float: p-valor de KS (0–1). Valores < ALPHA sugieren drift.
    """
    a = np.asarray(a)
    b = np.asarray(b)
    if len(a) < 20 or len(b) < 20:
        return 1.0
    return float(ks_2samp(a, b).pvalue)

def chi2_cat(a, b):
    """Calcula el p-valor de Chi-cuadrado para variables categóricas.

    Construye una tabla de contingencia a partir de las frecuencias en cada muestra.
    Si hay pocas observaciones o cardinalidad insuficiente, retorna 1.0.

    Args:
        a (iterable): Valores categóricos referencia.
        b (iterable): Valores categóricos recientes.

    Returns:
        float: p-valor del test Chi-cuadrado (0–1). Valores < ALPHA sugieren drift.
    """
    sa = pd.Series(list(a), dtype="string")
    sb = pd.Series(list(b), dtype="string")
    if len(sa) < 20 or len(sb) < 20:
        return 1.0
    ca = sa.value_counts()
    cb = sb.value_counts()
    df = pd.concat([ca, cb], axis=1).fillna(0.0)
    df.columns = ["a", "b"]
    if df.sum().sum() == 0 or df.shape[0] < 2:
        return 1.0
    _, p, _, _ = chi2_contingency(df.values)
    return float(p)

def psi(ref, cur, bins=10, eps=1e-6):
    """Calcula el Population Stability Index (PSI) entre dos distribuciones numéricas.

    La discretización de bins usa cuantiles de la referencia; si no es viable,
    usa partición lineal. Aplica recorte por `eps` para evitar log(0).

    Args:
        ref (array-like): Serie de referencia (histórica/entrenamiento).
        cur (array-like): Serie reciente (producción/actual).
        bins (int): Número de intervalos de discretización.
        eps (float): Mínimo permitido para proporciones.

    Returns:
        float: PSI acumulado. Regla práctica: > 0.2 alerta; > 0.3 severo.
    """
    ref = pd.Series(ref, dtype="float64").dropna()
    cur = pd.Series(cur, dtype="float64").dropna()
    if len(ref) < 50 or len(cur) < 50:
        return 0.0
    qs = np.linspace(0, 1, bins + 1)
    try:
        edges = np.unique(np.quantile(ref, qs))
        if len(edges) < 3:
            edges = np.linspace(ref.min(), ref.max(), bins + 1)
    except Exception:
        edges = np.linspace(ref.min(), ref.max(), bins + 1)

    ref_hist, _ = np.histogram(ref, bins=edges)
    cur_hist, _ = np.histogram(cur, bins=edges)

    ref_prop = ref_hist / max(ref_hist.sum(), 1)
    cur_prop = cur_hist / max(cur_hist.sum(), 1)

    ref_prop = np.clip(ref_prop, eps, None)
    cur_prop = np.clip(cur_prop, eps, None)
    return float(np.sum((ref_prop - cur_prop) * np.log(ref_prop / cur_prop)))

def find_score_column(num_cols):
    """Intenta localizar el nombre de la columna de score entre columnas numéricas.

    Prioriza `SCORE_COL` (por entorno) y alias comunes. Si no encuentra match,
    retorna la primera columna numérica disponible.

    Args:
        num_cols (list[str]): Lista de nombres de columnas numéricas.

    Returns:
        str|None: Nombre de columna de score o `None` si no hay columnas.
    """
    candidates = [
        SCORE_COL, "prob", "proba", "proba_1", "p1", "prob_positive",
        "score_1", "prediction_score"
    ]
    for c in candidates:
        if c in num_cols:
            return c
    return num_cols[0] if num_cols else None

# ===== Jenkins helpers =====
def _jenkins_session():
    """Crea una sesión HTTP para interactuar con Jenkins.

    Usa autenticación básica si `JENKINS_USER` y `JENKINS_API_TOKEN` están definidos.

    Returns:
        requests.Session: Sesión configurada.
    """
    s = requests.Session()
    if JENKINS_USER and JENKINS_API_TOKEN:
        s.auth = (JENKINS_USER, JENKINS_API_TOKEN)
    return s

def _get_crumb(s: requests.Session):
    """Obtiene el par (campo, valor) del Jenkins-Crumb para solicitudes CSRF-safe.

    Args:
        s (requests.Session): Sesión autenticada (o no) contra Jenkins.

    Returns:
        tuple[str|None, str|None]: (nombre_del_campo, valor_del_crumb) o (None, None) si falla.
    """
    try:
        url = f"{JENKINS_URL}/crumbIssuer/api/json"
        r = s.get(url, timeout=10)
        if r.status_code == 200 and "application/json" in r.headers.get("content-type",""):
            j = r.json()
            return j.get("crumbRequestField","Jenkins-Crumb"), j.get("crumb","")
    except Exception as e:
        print("[WARN] No se pudo obtener Jenkins-Crumb:", e)
    return None, None

def maybe_trigger_jenkins() -> bool:
    """Intenta disparar el job de Jenkins respetando el cooldown y el modo de auth.

    Lógica:
      - Si no ha pasado `COOLDOWN_S` desde el último intento, no envía trigger.
      - Prefiere autenticación por `JENKINS_USER`/`JENKINS_API_TOKEN` (CSRF/Crumb).
      - Fallback legacy con `JENKINS_TOKEN` en querystring.
      - Sin credenciales: informa y retorna False.

    Returns:
        bool: True si la respuesta HTTP indica aceptación del trigger (200/201/202/302).
    """
    global _last_trigger_ts
    now_ts = time.time()
    if now_ts - _last_trigger_ts < COOLDOWN_S:
        return False

    s = _jenkins_session()
    target = f"{JENKINS_URL}/job/{JENKINS_JOB}/build?delay=0sec"

    try:
        if JENKINS_USER and JENKINS_API_TOKEN:
            headers = {}
            field, crumb = _get_crumb(s)
            if field and crumb:
                headers[field] = crumb
            r = s.post(target, headers=headers, timeout=15)

        elif JENKINS_TOKEN:
            r = requests.post(f"{JENKINS_URL}/job/{JENKINS_JOB}/build?token={JENKINS_TOKEN}", timeout=15)

        else:
            print("[INFO] Drift detectado pero no hay credenciales Jenkins configuradas.")
            _last_trigger_ts = now_ts
            return False

        ok_codes = {200, 201, 202, 302}
        msg = f"[TRIGGER] Jenkins {JENKINS_JOB} → HTTP {r.status_code}"
        if r.status_code in ok_codes:
            print(msg)
            _last_trigger_ts = now_ts
            return True
        else:
            body = ""
            try:
                body = r.text[:200].replace("\n"," ")
            except Exception:
                pass
            print(msg, "| resp:", body)
            _last_trigger_ts = now_ts
            return False

    except Exception as e:
        print("[ERROR] Trigger Jenkins:", e)
        _last_trigger_ts = now_ts
        return False

# ========= Loop principal =========
def detect_and_export_loop():
    """Bucle principal de monitoreo, exportación de métricas y disparo de reentrenos.

    Flujo por iteración:
      1) Lee parquet desde HDFS (`DATA_PATH`) a Spark DataFrame.
      2) Identifica columnas numéricas y categóricas.
      3) Obtiene muestras pandas (ref/rec) con distintas semillas.
      4) Calcula p-values (KS/Chi²) y actualiza métricas por columna.
      5) Calcula PSI sobre la columna de score (si disponible).
      6) Estima `pos_ratio` según `POSITIVE_THRESHOLD` o columnas binarias alternativas.
      7) Determina condición de disparo (`should_trigger`) por KS/Chi² o PSI.
      8) Aplica política EDGE_ONLY para evitar retriggers mientras persista el estado.

    Side effects:
        - Emite logs informativos/warn/error.
        - Actualiza métricas Prometheus (servidor iniciado en `main()`).
        - Puede disparar Jenkins según umbrales y cooldown.
    """
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[START] DriftWatch leyendo {DATA_PATH} cada {LOOP_SECONDS}s (alpha={ALPHA}, psi_alert={PSI_ALERT})")

    while True:
        try:
            sdf_all = spark.read.parquet(DATA_PATH)

            # Ventanas móviles por tiempo de ingesta: ref=[-2W,-W), rec=[-W,now)
            recent_from = pd.Timestamp.utcnow() - pd.Timedelta(minutes=WINDOW_MIN)
            ref_from    = pd.Timestamp.utcnow() - pd.Timedelta(minutes=2*WINDOW_MIN)
            ref_to      = recent_from

            sdf = sdf_all
            sdf_ref = sdf_all
            sdf_rec = sdf_all
            if "ingest_ts" in sdf_all.columns:
                sdf_ref = sdf_all.filter((col("ingest_ts") >= lit(ref_from.to_pydatetime())) & (col("ingest_ts") < lit(ref_to.to_pydatetime())))
                sdf_rec = sdf_all.filter(col("ingest_ts") >= lit(recent_from.to_pydatetime()))
            else:
                # Fallback: usar todo y hacer split aleatorio (menor fidelidad)
                sdf_ref = sdf_all.sample(False, 0.5, 13)
                sdf_rec = sdf_all.subtract(sdf_ref)

            num_cols = numeric_columns(sdf_all)
            cat_cols = string_columns(sdf_all)

            pdf_num_ref = to_pdf_sample(sdf_ref, num_cols, n=SAMPLE_MAX, seed=42)
            pdf_num_rec = to_pdf_sample(sdf_rec, num_cols, n=SAMPLE_MAX, seed=7)
            pdf_cat_ref = to_pdf_sample(sdf_ref, cat_cols, n=SAMPLE_MAX, seed=42)
            pdf_cat_rec = to_pdf_sample(sdf_rec, cat_cols, n=SAMPLE_MAX, seed=7)

            drift_any = False
            pvals_row = {}
            for c in num_cols:
                try:
                    p = ks_num(pdf_num_rec[c].values, pdf_num_ref[c].values)
                except Exception:
                    p = 1.0
                g_pval_col.labels(c).set(p)
                g_drift_col.labels(c).set(1 if p < ALPHA else 0)
                drift_any = drift_any or (p < ALPHA)
                pvals_row[f"p_{c}"] = float(p)

            for c in cat_cols:
                try:
                    p = chi2_cat(pdf_cat_rec[c].astype(str), pdf_cat_ref[c].astype(str))
                except Exception:
                    p = 1.0
                g_pval_col.labels(c).set(p)
                g_drift_col.labels(c).set(1 if p < ALPHA else 0)
                drift_any = drift_any or (p < ALPHA)
                pvals_row[f"p_{c}"] = float(p)

            score_col = find_score_column(num_cols)
            psi_val = 0.0
            if score_col and (score_col in pdf_num_rec.columns) and (score_col in pdf_num_ref.columns):
                psi_val = psi(pdf_num_ref[score_col], pdf_num_rec[score_col], bins=10)
                g_score_psi.labels(MODEL_NAME).set(psi_val)
            else:
                g_score_psi.labels(MODEL_NAME).set(float("nan"))
                print(f"[WARN] No se encontró columna de score. Revisa SCORE_COL (actual: '{SCORE_COL}').")

            pos_ratio = float("nan")
            if score_col and (score_col in pdf_num_rec.columns):
                try:
                    pos_ratio = float((pdf_num_rec[score_col] >= POSITIVE_THRESHOLD).mean())
                except Exception:
                    pos_ratio = float("nan")
            else:
                for c in ["prediction", "y_pred", "label", "target"]:
                    if c in pdf_num_rec.columns:
                        s = pd.Series(pdf_num_rec[c]).dropna()
                        if set(s.unique()).issubset({0, 1}):
                            pos_ratio = float((s == 1).mean())
                            break
            g_pos_ratio.labels(MODEL_NAME).set(pos_ratio)

            should_trigger = drift_any or (psi_val is not None and psi_val > PSI_ALERT)

            # CSV log row
            try:
                _append_drift_log({
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
                    "scenario": os.getenv("EVAL_SCENARIO", ""),
                    "model": MODEL_NAME,
                    "psi": float(psi_val),
                    "pos_ratio": float(pos_ratio) if pos_ratio == pos_ratio else "",  # keep empty for NaN
                    "drift_any": int(bool(drift_any)),
                    "score_col": score_col or "",
                    **pvals_row,
                })
            except Exception as _e:
                # Non-fatal
                pass

            global _prev_alert, _clear_ok_streak

            if EDGE_ONLY:
                if should_trigger and not _prev_alert:
                    print(f"[DRIFT][FIRST] KS/χ²={drift_any}, PSI={psi_val:.4f} (>{PSI_ALERT}) ⇒ Jenkins (cooldown {COOLDOWN_S}s)")
                    if maybe_trigger_jenkins():
                        c_triggers.labels(MODEL_NAME).inc()
                    _prev_alert = True
                    _clear_ok_streak = 0
                elif should_trigger and _prev_alert:
                    # Drift sigue presente, pero ya se disparó antes → no retrigger.
                    pass
                else:
                    # No hay drift en este ciclo
                    if _prev_alert:
                        _clear_ok_streak += 1
                        if _clear_ok_streak >= DRIFT_CLEAR_STREAK:
                            print("[INFO] Drift despejado. Re-armado del trigger.")
                            _prev_alert = False
                            _clear_ok_streak = 0
            else:
                # Comportamiento anterior (por si quieres mantenerlo)
                if should_trigger:
                    print(f"[DRIFT] KS/χ²={drift_any}, PSI={psi_val:.4f} (>{PSI_ALERT}) ⇒ Jenkins (cooldown {COOLDOWN_S}s)")
                    if maybe_trigger_jenkins():
                        c_triggers.labels(MODEL_NAME).inc()

        except Exception as e:
            print("[ERROR] drift-watch:", e)

        time.sleep(LOOP_SECONDS)

def main():
    """Punto de entrada del módulo.

    Inicia el servidor de métricas Prometheus en `EXPORTER_PORT` y arranca
    el loop de detección/exportación. Bloqueante por diseño.
    """
    start_http_server(EXPORTER_PORT, addr="0.0.0.0")
    detect_and_export_loop()

if __name__ == "__main__":
    main()
