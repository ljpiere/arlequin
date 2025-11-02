# /scripts/train_model.py
"""
Entrenamiento de modelo de fraude bancario con Spark + MLflow + scikit-learn.

Descripción general
-------------------
Este módulo:
  1) Lee datos particionados desde HDFS con Spark.
  2) Asegura una etiqueta binaria razonable para entrenamiento (reutiliza
     `is_suspicious` si es válida o crea una etiqueta sintética mediante reglas).
  3) Selecciona features candidatas, convierte a pandas y entrena un modelo
     `LogisticRegression` (scikit-learn) con `class_weight="balanced"`.
  4) Evalúa con F1 en un hold-out (train_test_split) y registra artefactos,
     parámetros y métricas en MLflow.

Integraciones
-------------
- **Spark**: lectura desde HDFS y preparación de datos.
- **scikit-learn**: entrenamiento de modelo supervisado (Logistic Regression).
- **MLflow**: tracking de experimentos (params, metrics y artefacto del modelo).

Variables de entorno
--------------------
- SPARK_MASTER_URL (str): URL del Spark master (por defecto: "spark://spark-master:7077").
- HDFS_URI (str): URI de HDFS (por defecto: "hdfs://namenode:9000").
- DATA_PATH (str): Ruta parquet con datos (por defecto: "hdfs:///datalake/raw/bank_transactions").
- MLFLOW_TRACKING_URI (str): URI del servidor MLflow (por defecto: "http://mlflow:5000").

Hiperparámetros y convenciones
------------------------------
- EXPERIMENT_NAME: nombre del experimento en MLflow ("bank-fraud").
- FEATURES_CANDIDATES: columnas numéricas candidatas a features.
- Etiquetado:
  - Si `is_suspicious` existe y contiene ambas clases → se usa como `label`.
  - Si no, se genera `label` con regla OR:
      (risk_score >= 0.80) OR (amount_usd >= p95 de amount_usd).
  - Se registra `label_strategy` en MLflow.

Métricas
--------
- F1 (conjunto de test): métrica principal registrada en MLflow.

Notas operativas
----------------
- Si después del etiquetado solo hay una clase, se registra la corrida en MLflow
  y se omite el entrenamiento (retorna 0 para no fallar pipelines).
- El uso de `.toPandas()` supone un dataset manejable tras filtros y selección.
"""

import os, sys, time
import csv
from pathlib import Path
from datetime import datetime
os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")  # silencia el warning de git en MLflow

import mlflow
import mlflow.sklearn
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, expr
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score
import numpy as np

# ========= Configuración por entorno =========
SPARK_MASTER = os.getenv("SPARK_MASTER_URL","spark://spark-master:7077")
HDFS_URI     = os.getenv("HDFS_URI","hdfs://namenode:9000")
DATA_PATH    = os.getenv("DATA_PATH","hdfs:///datalake/raw/bank_transactions")
MLFLOW_URI   = os.getenv("MLFLOW_TRACKING_URI","http://mlflow:5000")

EXPERIMENT_NAME = "bank-fraud"
FEATURES_CANDIDATES = ["amount","fee","amount_usd","risk_score"]


def _append_training_log(filepath: str, row: dict):
    """Append a single CSV row with training/test metrics.

    The file is created with a header if it does not exist. This is intentionally
    light-weight to avoid adding new dependencies.

    Columns written are the keys of ``row`` in a stable order.
    """
    if not filepath:
        return
    fp = Path(filepath)
    fp.parent.mkdir(parents=True, exist_ok=True)
    # Stable column order
    fieldnames = [
        "timestamp", "scenario", "mlflow_run_id", "label_strategy",
        "total_rows", "positives", "negatives", "model", "f1"
    ]
    # Ensure all keys exist
    for k in fieldnames:
        row.setdefault(k, "")
    file_exists = fp.exists()
    with fp.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in fieldnames})

def get_spark():
    """Crea y retorna una SparkSession adecuada para lectura desde HDFS.

    Configura:
      - `spark.master` con `SPARK_MASTER`.
      - `spark.hadoop.fs.defaultFS` con `HDFS_URI`.

    Returns:
        pyspark.sql.SparkSession: Sesión Spark lista para leer Parquet.
    """
    return (SparkSession.builder
            .appName("TrainModel")
            .master(SPARK_MASTER)
            .config("spark.hadoop.fs.defaultFS", HDFS_URI)
            .getOrCreate())

def ensure_label(sdf):
    """Garantiza una columna de etiqueta binaria para entrenamiento.

    Estrategia:
      1) Si existe `is_suspicious` y hay presencia de ambas clases (0/1),
         se usa directamente como `label`.
      2) En caso contrario, se genera `label` con la regla:
         (risk_score >= 0.80) OR (amount_usd >= p95(amount_usd)),
         y se dejan únicamente las columnas de `FEATURES_CANDIDATES`
         que existan más `label`.

    Args:
        sdf (pyspark.sql.DataFrame): DataFrame de Spark de entrada.

    Returns:
        tuple[pyspark.sql.DataFrame, str]:
            - DataFrame con columnas de features existentes + `label`.
            - Nombre de la estrategia utilizada:
                * "existing_is_suspicious"
                * "synthetic_rule_risk_or_p95"
    """
    cols = set(sdf.columns)
    if "is_suspicious" in cols:
        base = sdf.select(*FEATURES_CANDIDATES, "is_suspicious").dropna()
        pos = base.filter(col("is_suspicious") == True).count()
        neg = base.filter(col("is_suspicious") == False).count()
        if pos > 0 and neg > 0:
            return base.withColumnRenamed("is_suspicious", "label"), "existing_is_suspicious"

    # label sintética con p95 de amount_usd + risk_score
    has_amount_usd = "amount_usd" in cols
    has_risk = "risk_score" in cols
    sdf2 = sdf

    p95 = None
    if has_amount_usd:
        # percentil aproximado para estabilidad en grandes volúmenes
        p95 = sdf.selectExpr("percentile_approx(amount_usd, 0.95) as p95").first()["p95"]
        if p95 is None:
            p95 = 1e9  # evita etiquetar por p95 si viene None

    rule = lit(False)
    if has_risk:
        rule = rule | (col("risk_score") >= lit(0.80))
    if has_amount_usd:
        rule = rule | (col("amount_usd") >= lit(p95))

    labeled = (sdf2
               .withColumn("label", when(rule, lit(1)).otherwise(lit(0)))
               .select(*[c for c in FEATURES_CANDIDATES if c in cols], "label")
               .dropna())

    return labeled, "synthetic_rule_risk_or_p95"

def main():
    """Punto de entrada: entrena el modelo y registra resultados en MLflow.

    Flujo:
      1) Configura MLflow (tracking URI y experimento).
      2) Crea SparkSession y carga los datos desde `DATA_PATH`.
      3) Asegura etiqueta con `ensure_label` y verifica balance de clases.
      4) Selecciona features existentes, convierte a pandas y separa train/test
         (estratificado cuando hay más de una clase).
      5) Entrena `LogisticRegression(class_weight="balanced")`.
      6) Calcula F1 en test y registra parámetros, métricas y artefacto en MLflow.

    Returns:
        int: 0 si ejecuta correctamente (incluye omitir entrenamiento por clase única).
    """
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    sdf_all = spark.read.parquet(DATA_PATH)
    if sdf_all.rdd.isEmpty():
        print("No data found in:", DATA_PATH)
        return 0

    df_labeled, label_strategy = ensure_label(sdf_all)

    # chequeos de clases
    pos = df_labeled.filter(col("label") == 1).count()
    neg = df_labeled.filter(col("label") == 0).count()
    total = df_labeled.count()

    with mlflow.start_run(run_name=f"train_{int(time.time())}"):
        mlflow.log_param("label_strategy", label_strategy)
        mlflow.log_metric("total_rows", total)
        mlflow.log_metric("positives", pos)
        mlflow.log_metric("negatives", neg)

        if pos == 0 or neg == 0:
            # Evita fallar pipelines cuando no hay ambas clases
            mlflow.log_param("skip_reason", "single_class_after_labeling")
            print(f"[SKIP] Solo una clase (pos={pos}, neg={neg}). Run registrado en MLflow, pero sin entrenar.")
            spark.stop()
            return 0  # éxito (evita fallo en Jenkins)

        # selecciona features que existan
        feats = [c for c in FEATURES_CANDIDATES if c in df_labeled.columns]
        pdf = df_labeled.select(*(feats + ["label"])).toPandas()
        X = pdf[feats].values
        y = pdf["label"].astype(int).values

        # split estratificado si se puede
        strat = y if len(np.unique(y)) > 1 else None
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=.2, random_state=42, stratify=strat
        )

        clf = LogisticRegression(max_iter=300, class_weight="balanced")
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        f1 = f1_score(y_test, y_pred)

        mlflow.log_param("model", "logreg")
        mlflow.log_param("class_weight", "balanced")
        mlflow.log_metric("f1", float(f1))
        mlflow.sklearn.log_model(clf, artifact_path="model")
        print("Logged to MLflow:", mlflow.get_tracking_uri(), "F1:", f1)

        # Optional CSV log for offline analysis
        try:
            active = mlflow.active_run()
            run_id = active.info.run_id if active else ""
        except Exception:
            run_id = ""
        _append_training_log(
            os.getenv("EXPERIMENT_LOG_FILE", ""),
            {
                "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
                "scenario": os.getenv("EVAL_SCENARIO", ""),
                "mlflow_run_id": run_id,
                "label_strategy": label_strategy,
                "total_rows": total,
                "positives": pos,
                "negatives": neg,
                "model": "logreg",
                "f1": float(f1),
            },
        )

    spark.stop()
    return 0

if __name__ == "__main__":
    sys.exit(main())
