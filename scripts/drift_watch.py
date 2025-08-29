# /scripts/drift_watch.py
import os, time
from datetime import datetime, timedelta

import requests
import pandas as pd
from prometheus_client import start_http_server, Gauge
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import NumericType, StringType
from scipy.stats import ks_2samp, chi2_contingency

# ---- Config ----
SPARK_MASTER  = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
HDFS_URI      = os.getenv("HDFS_URI", "hdfs://namenode:9000")
DATA_PATH     = os.getenv("DATA_PATH", "hdfs:///datalake/raw/bank_transactions")
ALPHA         = float(os.getenv("DRIFT_ALPHA", "0.01"))
LOOKBACK_H    = int(os.getenv("LOOKBACK_HOURS", "1"))   # ventana reciente (h)
REF_H         = int(os.getenv("REF_HOURS", "6"))        # ventana de referencia (h)
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "8010")) # <-- usa 8010 por defecto
COOLDOWN_S    = int(os.getenv("DRIFT_COOLDOWN_SECONDS", "300"))  # 5 min

JENKINS_URL   = os.getenv("JENKINS_URL", "http://jenkins:8080")
JENKINS_JOB   = os.getenv("JENKINS_JOB", "retrain-model")
JENKINS_TOKEN = os.getenv("JENKINS_TOKEN", "")

# ---- Métricas ----
g_drift = Gauge("drift_detected", "1 if drift detected else 0", ["col"])
g_pval  = Gauge("pvalue", "p-value per column", ["col"])

# ---- Spark ----
def get_spark():
    return (
        SparkSession.builder
        .appName("DriftWatch")
        .master(SPARK_MASTER)
        .config("spark.hadoop.fs.defaultFS", HDFS_URI)
        .getOrCreate()
    )

# ---- Utilidades ----
def numeric_columns(sdf):
    return [f.name for f in sdf.schema.fields
            if isinstance(f.dataType, NumericType) and f.name not in ("year","month","day","hour")]

def string_columns(sdf):
    return [f.name for f in sdf.schema.fields if isinstance(f.dataType, StringType)]

def to_pdf(sdf, cols):
    cols = [c for c in cols if c in sdf.columns]
    if not cols:
        return pd.DataFrame()
    return sdf.select(*cols).dropna().toPandas()

def ks_num(a, b):
    # Evita errores con tamaños chicos
    if len(a) < 20 or len(b) < 20:  # regla práctica
        return 1.0
    return float(ks_2samp(a, b).pvalue)

def chi2_cat(a, b):
    # a y b: iterables (categorías)
    sa = pd.Series(list(a), dtype="string")
    sb = pd.Series(list(b), dtype="string")
    if len(sa) < 20 or len(sb) < 20:
        return 1.0
    ca = sa.value_counts()
    cb = sb.value_counts()
    df = pd.concat([ca, cb], axis=1)
    df.columns = ["a", "b"]
    df = df.fillna(0)
    # Si todo cero o 1 sola categoría, no hay test significativo
    if df.sum().sum() == 0 or df.shape[0] < 2:
        return 1.0
    stat, p, dof, exp = chi2_contingency(df.values)
    return float(p)

_last_trigger_ts = 0.0

def maybe_trigger_jenkins():
    global _last_trigger_ts
    now_ts = time.time()
    if now_ts - _last_trigger_ts < COOLDOWN_S:
        return
    if not JENKINS_TOKEN:
        print("[INFO] Drift detectado, pero no hay JENKINS_TOKEN configurado.")
        _last_trigger_ts = now_ts
        return
    url = f"{JENKINS_URL}/job/{JENKINS_JOB}/build?token={JENKINS_TOKEN}"
    try:
        r = requests.post(url, timeout=10)
        print(f"[TRIGGER] Jenkins {JENKINS_JOB} → HTTP {r.status_code}")
    except Exception as e:
        print("[WARN] No se pudo invocar Jenkins:", e)
    _last_trigger_ts = now_ts

def detect_and_export_loop():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[START] DriftWatch leyendo {DATA_PATH} cada 30s (alpha={ALPHA})")

    while True:
        try:
            # Lee todo el parquet (para POC). Si usas particiones dt/hour, filtra aquí.
            sdf = spark.read.parquet(DATA_PATH)

            num_cols = numeric_columns(sdf)
            cat_cols = string_columns(sdf)

            # En este POC tomamos dos muestras aleatorias como “recent” y “ref”.
            # Si tienes columnas dt/hour, reemplaza por filtros de tiempo.
            pdf_all_num = to_pdf(sdf, num_cols)
            pdf_all_cat = to_pdf(sdf, cat_cols)

            # Muestreo para hacer el test liviano
            pdf_r_num = pdf_all_num.sample(n=min(5000, len(pdf_all_num)), random_state=42) if not pdf_all_num.empty else pdf_all_num
            pdf_f_num = pdf_all_num.sample(n=min(5000, len(pdf_all_num)), random_state=7)  if not pdf_all_num.empty else pdf_all_num

            pdf_r_cat = pdf_all_cat.sample(n=min(5000, len(pdf_all_cat)), random_state=42) if not pdf_all_cat.empty else pdf_all_cat
            pdf_f_cat = pdf_all_cat.sample(n=min(5000, len(pdf_all_cat)), random_state=7)  if not pdf_all_cat.empty else pdf_all_cat

            drift_any = False

            # Numéricas → KS
            for c in num_cols:
                try:
                    p = ks_num(pdf_r_num[c].values, pdf_f_num[c].values)
                except Exception:
                    p = 1.0
                g_pval.labels(c).set(p)
                g_drift.labels(c).set(1 if p < ALPHA else 0)
                drift_any = drift_any or (p < ALPHA)

            # Categóricas → χ²
            for c in cat_cols:
                try:
                    p = chi2_cat(pdf_r_cat[c].astype(str), pdf_f_cat[c].astype(str))
                except Exception:
                    p = 1.0
                g_pval.labels(c).set(p)
                g_drift.labels(c).set(1 if p < ALPHA else 0)
                drift_any = drift_any or (p < ALPHA)

            if drift_any:
                print("[DRIFT] detectado → Jenkins trigger (con cooldown).")
                maybe_trigger_jenkins()
        except Exception as e:
            print("[ERROR] drift-watch:", e)

        time.sleep(30)  # cada 30s

def main():
    # IMPORTANTÍSIMO: bind en 0.0.0.0 y solo UNA llamada
    start_http_server(EXPORTER_PORT, addr="0.0.0.0")
    detect_and_export_loop()

if __name__ == "__main__":
    main()
