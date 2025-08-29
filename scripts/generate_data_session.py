# -*- coding: utf-8 -*-
"""
Genera datos sintéticos de banca con drift y los escribe en HDFS con múltiples particiones.
- Más columnas (amplio) y pocas filas por batch (para no saturar el equipo)
- Particiona por dt (YYYY-MM-DD), hour (HH) y account_type
- Escribe en formato Parquet (append)

Ejecución típica dentro de tu red de docker-compose:
  spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    pyspark_generate_bank_data_partitions.py
"""

import time, random, socket
from datetime import datetime, timedelta
from faker import Faker

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, TimestampType, IntegerType
)
from pyspark.sql.functions import lit, col, to_date, date_format

# ----------------- Parámetros -----------------
HDFS_BASE        = "hdfs:///datalake/raw"
TABLE_NAME       = "bank_transactions"
OUTPUT_PATH      = f"{HDFS_BASE}/{TABLE_NAME}"

# Control de tamaño: pocos registros, muchas columnas
NUM_BATCHES              = 3           # cuantos lotes escribir
NUM_RECORDS_PER_BATCH    = 200         # filas por lote (ajusta si quieres aún menos)
INTERVAL_SECONDS         = 5           # pausa entre lotes
DRIFT_CHANGE_EACH_MIN    = 1           # cambia el drift cada X minutos

# ----------------- Faker -----------------
fake = Faker("es_ES")

# ----------------- Esquema (muchas columnas) -----------------
schema = StructType([
    # Identificadores / tiempos
    StructField("transaction_id",              StringType(),  False),
    StructField("event_ts",                    TimestampType(), True),
    StructField("ingest_ts",                   TimestampType(), True),

    # Cliente / cuenta
    StructField("customer_id",                 StringType(),  True),
    StructField("account_number",              StringType(),  True),
    StructField("account_type",                StringType(),  True),   # checking|savings|credit_card|loan
    StructField("customer_age",                IntegerType(), True),
    StructField("customer_gender",             StringType(),  True),   # M|F|O

    # Transacción
    StructField("transaction_type",            StringType(),  True),   # deposit|withdrawal|transfer|payment|fee
    StructField("amount",                      DoubleType(),  True),
    StructField("currency",                    StringType(),  True),
    StructField("fee",                         DoubleType(),  True),
    StructField("exchange_rate_to_usd",        DoubleType(),  True),
    StructField("amount_usd",                  DoubleType(),  True),
    StructField("balance_after_transaction",   DoubleType(),  True),
    StructField("is_suspicious",               BooleanType(), True),
    StructField("risk_score",                  DoubleType(),  True),

    # Comercio / canal
    StructField("merchant_name",               StringType(),  True),
    StructField("merchant_category",           StringType(),  True),   # retail|grocery|fuel|online|services...
    StructField("channel",                     StringType(),  True),   # web|mobile|branch|atm|ivr
    StructField("device_type",                 StringType(),  True),   # ios|android|web|pos
    StructField("ip_address",                  StringType(),  True),

    # Ubicación
    StructField("country",                     StringType(),  True),
    StructField("city",                        StringType(),  True),
    StructField("latitude",                    DoubleType(),  True),
    StructField("longitude",                   DoubleType(),  True),

    # Particiones lógicas derivadas
    StructField("dt",                          StringType(),  True),   # YYYY-MM-DD
    StructField("hour",                        StringType(),  True)     # HH
])

ACCOUNT_TYPES       = ["checking", "savings", "credit_card", "loan"]
TX_TYPES            = ["deposit", "withdrawal", "transfer", "payment", "fee"]
MERCHANT_CATEGORIES = ["retail", "grocery", "fuel", "online", "services", "travel", "health"]
CHANNELS            = ["web", "mobile", "branch", "atm", "ivr"]
DEVICE_TYPES        = ["ios", "android", "web", "pos"]
GENDERS             = ["M", "F", "O"]
CURS                = ["USD", "EUR", "GBP", "COP"]

def base_amount():
    """Monta una estacionalidad simple por día del mes."""
    d = datetime.now().day
    if 5 <= d <= 10:
        return random.uniform(20, 500)
    elif 20 <= d <= 25:
        return random.uniform(50, 2000)
    return random.uniform(10, 1000)

def fx_to_usd(cur: str) -> float:
    """Tasa FX simplificada para convertir a USD."""
    return {"USD": 1.0, "EUR": 1.08, "GBP": 1.27, "COP": 0.00026}.get(cur, 1.0)

def generate_one(ts: datetime, drift_factor: float = 0.0) -> dict:
    """Genera un registro con posible drift en monto y riesgo."""
    amt = base_amount()
    # Drift: montos más altos y mayor riesgo
    if random.random() < drift_factor:
        amt *= random.uniform(1.5, 4.0)

    cur  = random.choice(CURS)
    fx   = fx_to_usd(cur)
    fee  = round(amt * random.uniform(0.000, 0.015), 2)
    amt_usd = round((amt - fee) * fx, 2)

    risk = min(1.0, random.random() * (0.3 + drift_factor))  # riesgo sube con drift
    suspicious = (amt > 1000 and random.random() < (0.02 + drift_factor * 0.2)) or risk > 0.8

    acc_type = random.choice(ACCOUNT_TYPES)
    tx_type  = random.choice(TX_TYPES)
    lat = float(fake.latitude())
    lon = float(fake.longitude())

    return {
        "transaction_id": fake.uuid4(),
        "event_ts": ts,
        "ingest_ts": datetime.utcnow(),
        "customer_id": fake.uuid4(),
        "account_number": fake.bban(),
        "account_type": acc_type,
        "customer_age": random.randint(18, 85),
        "customer_gender": random.choice(GENDERS),

        "transaction_type": tx_type,
        "amount": round(amt, 2),
        "currency": cur,
        "fee": fee,
        "exchange_rate_to_usd": fx,
        "amount_usd": amt_usd,
        "balance_after_transaction": round(random.uniform(100, 10000), 2),
        "is_suspicious": bool(suspicious),
        "risk_score": round(risk, 3),

        "merchant_name": fake.company() if random.random() > 0.25 else None,
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "channel": random.choice(CHANNELS),
        "device_type": random.choice(DEVICE_TYPES),
        "ip_address": fake.ipv4_public(),

        "country": fake.current_country(),
        "city": fake.city(),
        "latitude": lat,
        "longitude": lon,

        # columnas de partición
        "dt": ts.strftime("%Y-%m-%d"),
        "hour": ts.strftime("%H")
    }

def wait_for_master(host="spark-master", port=7077, timeout=120):
    start = time.time()
    print(f"⏳ Esperando Spark master en {host}:{port} ...")
    while time.time() - start < timeout:
        try:
            import socket
            with socket.create_connection((host, port), timeout=5):
                print("Spark master disponible.")
                return
        except Exception:
            time.sleep(5)
    raise TimeoutError(f"No se pudo contactar al Spark master en {host}:{port} dentro de {timeout}s")

def main():
    wait_for_master()

    spark = (
        SparkSession.builder
        .appName("BankDataGeneratorPartitions")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.sql.shuffle.partitions", "4")  # evita demasiados archivos
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"Escribiendo en: {OUTPUT_PATH}  (partitions: dt, hour, account_type)")

    start_cycle = datetime.now()
    drift_factor = 0.0

    for b in range(NUM_BATCHES):
        # Actualiza drift cada cierto tiempo
        if (datetime.now() - start_cycle).total_seconds() / 60.0 >= DRIFT_CHANGE_EACH_MIN:
            drift_factor = random.uniform(0.0, 0.5)
            start_cycle = datetime.now()
            print(f"--- Data Drift factor actualizado a: {drift_factor:.2f} ---")

        now_ts = datetime.now()
        batch = [generate_one(now_ts, drift_factor) for _ in range(NUM_RECORDS_PER_BATCH)]

        df = spark.createDataFrame(batch, schema=schema)

        # Re-particiona para tener pocas salidas por partición
        # (usa columnas de partición para el shuffle)
        df = df.repartition(4, "dt", "hour", "account_type")

        (
            df.write
              .mode("append")
              .partitionBy("dt", "hour", "account_type")
              .parquet(OUTPUT_PATH)
        )
        print(f"Lote {b+1}/{NUM_BATCHES} -> {NUM_RECORDS_PER_BATCH} filas escrito.")

        time.sleep(INTERVAL_SECONDS)

    print("✔ Generación finalizada.")
    spark.stop()

if __name__ == "__main__":
    main()
