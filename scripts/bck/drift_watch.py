# /scripts/drift_watch.py
import os
import time
import numpy as np
import pandas as pd
import requests

from prometheus_client import start_http_server, Gauge, Counter
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import NumericType, StringType
from scipy.stats import ks_2samp, chi2_contingency

# ========= Config =========
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
HDFS_URI     = os.getenv("HDFS_URI", "hdfs://namenode:9000")
DATA_PATH    = os.getenv("DATA_PATH", "hdfs:///datalake/raw/bank_transactions")

ALPHA         = float(os.getenv("DRIFT_ALPHA", "0.01"))
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "8010"))
LOOP_SECONDS  = int(os.getenv("LOOP_SECONDS", "30"))
COOLDOWN_S    = int(os.getenv("DRIFT_COOLDOWN_SECONDS", "300"))  # 5 min

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

# ========= Spark =========
def get_spark():
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
    return [
        f.name for f in sdf.schema.fields
        if isinstance(f.dataType, NumericType) and f.name not in ("year", "month", "day", "hour")
    ]

def string_columns(sdf):
    return [f.name for f in sdf.schema.fields if isinstance(f.dataType, StringType)]

def to_pdf_sample(sdf, cols, n=SAMPLE_MAX, seed=42):
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
    a = np.asarray(a)
    b = np.asarray(b)
    if len(a) < 20 or len(b) < 20:
        return 1.0
    return float(ks_2samp(a, b).pvalue)

def chi2_cat(a, b):
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
    s = requests.Session()
    if JENKINS_USER and JENKINS_API_TOKEN:
        s.auth = (JENKINS_USER, JENKINS_API_TOKEN)
    return s

def _get_crumb(s: requests.Session):
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
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[START] DriftWatch leyendo {DATA_PATH} cada {LOOP_SECONDS}s (alpha={ALPHA}, psi_alert={PSI_ALERT})")

    while True:
        try:
            sdf = spark.read.parquet(DATA_PATH)

            num_cols = numeric_columns(sdf)
            cat_cols = string_columns(sdf)

            pdf_num_ref = to_pdf_sample(sdf, num_cols, n=SAMPLE_MAX, seed=42)
            pdf_num_rec = to_pdf_sample(sdf, num_cols, n=SAMPLE_MAX, seed=7)
            pdf_cat_ref = to_pdf_sample(sdf, cat_cols, n=SAMPLE_MAX, seed=42)
            pdf_cat_rec = to_pdf_sample(sdf, cat_cols, n=SAMPLE_MAX, seed=7)

            drift_any = False
            for c in num_cols:
                try:
                    p = ks_num(pdf_num_rec[c].values, pdf_num_ref[c].values)
                except Exception:
                    p = 1.0
                g_pval_col.labels(c).set(p)
                g_drift_col.labels(c).set(1 if p < ALPHA else 0)
                drift_any = drift_any or (p < ALPHA)

            for c in cat_cols:
                try:
                    p = chi2_cat(pdf_cat_rec[c].astype(str), pdf_cat_ref[c].astype(str))
                except Exception:
                    p = 1.0
                g_pval_col.labels(c).set(p)
                g_drift_col.labels(c).set(1 if p < ALPHA else 0)
                drift_any = drift_any or (p < ALPHA)

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
    start_http_server(EXPORTER_PORT, addr="0.0.0.0")
    detect_and_export_loop()

if __name__ == "__main__":
    main()
