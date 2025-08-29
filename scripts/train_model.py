# /scripts/train_model.py
import os, sys, time
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

SPARK_MASTER = os.getenv("SPARK_MASTER_URL","spark://spark-master:7077")
HDFS_URI     = os.getenv("HDFS_URI","hdfs://namenode:9000")
DATA_PATH    = os.getenv("DATA_PATH","hdfs:///datalake/raw/bank_transactions")
MLFLOW_URI   = os.getenv("MLFLOW_TRACKING_URI","http://mlflow:5000")

EXPERIMENT_NAME = "bank-fraud"
FEATURES_CANDIDATES = ["amount","fee","amount_usd","risk_score"]

def get_spark():
    return (SparkSession.builder
            .appName("TrainModel")
            .master(SPARK_MASTER)
            .config("spark.hadoop.fs.defaultFS", HDFS_URI)
            .getOrCreate())

def ensure_label(sdf):
    """
    Garantiza una etiqueta binaria razonable:
    1) Si existe is_suspicious y tiene positivos -> úsala
    2) Si no, crea etiqueta por regla: risk_score>=0.8 o amount_usd >= p95
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
        p95 = sdf.selectExpr("percentile_approx(amount_usd, 0.95) as p95").first()["p95"]
        if p95 is None:
            p95 = 1e9  # algo enorme para no etiquetar por p95 si viene None

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

    spark.stop()
    return 0

if __name__ == "__main__":
    sys.exit(main())
