# -*- coding: utf-8 -*-
"""
Generador de datos sintéticos bancarios con drift y escritura particionada en HDFS.

Descripción general
-------------------
Este módulo genera datos transaccionales bancarios sintéticos con la opción de
introducir "data drift" controlado y los escribe en HDFS en formato Parquet
usando Spark. El dataset es "ancho" (muchas columnas) y se controla el volumen
por lote para no saturar recursos locales en entornos de desarrollo.

Características clave
---------------------
- Columnas variadas: identificadores, tiempos, cliente/cuenta, transacción,
  comercio/canal, geografía y particiones lógicas derivadas.
- Particionado por `dt` (YYYY-MM-DD), `hour` (HH) y `account_type`.
- Escritura en modo `append` para facilitar ingestas sucesivas.
- Control de drift: factor que incrementa montos y riesgo para simular cambios
  de distribución en producción.

Arquitectura y ejecución
------------------------
- Spark: creación de sesión con configuración conservadora de recursos.
- HDFS: escritura a ruta `hdfs:///datalake/raw/bank_transactions` (por defecto).
- Faker: generación de campos de identidad, negocio y geografía.
- Bucle por lotes: pausa configurable entre lotes, con re-particionado para
  limitar número de archivos por partición.

Ejecución típica (dentro de docker-compose)
-------------------------------------------
spark-submit \\
  --master spark://spark-master:7077 \\
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \\
  pyspark_generate_bank_data_partitions.py

Parámetros principales
----------------------
- NUM_BATCHES (int): número de lotes a generar.
- NUM_RECORDS_PER_BATCH (int): filas por lote (bajo para no cargar el equipo).
- INTERVAL_SECONDS (int): pausa entre lotes.
- DRIFT_CHANGE_EACH_MIN (int): cada cuántos minutos se actualiza el drift.
- OUTPUT_PATH (str): ruta HDFS de salida (derivada de HDFS_BASE y TABLE_NAME).

Consideraciones operativas
--------------------------
- La función `wait_for_master` comprueba la disponibilidad del Spark master
  antes de iniciar, con timeout configurable.
- `repartition(4, "dt", "hour", "account_type")` ayuda a consolidar archivos
  por partición (evita excesiva fragmentación).
- El drift se modela elevando montos y riesgo de manera probabilística.
"""

import time, random, socket, os
import argparse
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
NUM_BATCHES              = 5           # cuantos lotes escribir
NUM_RECORDS_PER_BATCH    = 500         # filas por lote (ajusta si quieres aún menos)
INTERVAL_SECONDS         = 5           # pausa entre lotes
DRIFT_CHANGE_EACH_MIN    = 0           # cambia el drift cada X minutos

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
    """Genera una base de monto con estacionalidad simple por día del mes.

    La estacionalidad se modela con rangos de valores aleatorios dependientes
    del día actual (`datetime.now().day`).

    Returns:
        float: Monto base sugerido para la transacción (sin comisiones).
    """
    d = datetime.now().day
    if 5 <= d <= 10:
        return random.uniform(20, 500)
    elif 20 <= d <= 25:
        return random.uniform(50, 2000)
    return random.uniform(10, 1000)

def fx_to_usd(cur: str) -> float:
    """Devuelve una tasa de cambio simplificada hacia USD.

    Args:
        cur (str): Código de moneda ISO (p.ej., "USD", "EUR", "GBP", "COP").

    Returns:
        float: Tasa multiplicativa para convertir a USD. Si la moneda no está
        mapeada, retorna 1.0.
    """
    return {"USD": 1.0, "EUR": 1.08, "GBP": 1.27, "COP": 0.00026}.get(cur, 1.0)

def generate_one(ts: datetime, drift_factor: float = 0.0) -> dict:
    """Crea un registro transaccional sintético con posible drift.

    El drift se introduce aumentando tanto el monto como el riesgo de forma
    probabilística, en función de `drift_factor`.

    Args:
        ts (datetime): Marca de tiempo del evento (`event_ts`).
        drift_factor (float): Intensidad del drift en [0, 1]. Valores más altos
            incrementan la probabilidad de montos elevados y mayor `risk_score`.

    Returns:
        dict: Diccionario con todas las columnas esperadas por el `schema`,
        incluyendo particiones lógicas `dt` y `hour`.
    """
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
    """Bloquea la ejecución hasta que el Spark master sea accesible por TCP.

    Realiza intentos de conexión al host/puerto indicados hasta `timeout`
    segundos. Útil en entornos orquestados donde los servicios inician en
    distinto orden.

    Args:
        host (str): Hostname o IP donde escucha el Spark master.
        port (int): Puerto TCP del Spark master (por defecto 7077).
        timeout (int): Tiempo máximo de espera en segundos.

    Raises:
        TimeoutError: Si no se logra establecer conexión dentro del `timeout`.
    """
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
    """Punto de entrada del generador.

    Flujo:
      1) Verifica disponibilidad del Spark master.
      2) Crea `SparkSession` con configuración de HDFS y shuffles moderados.
      3) Itera `NUM_BATCHES`:
         - Actualiza `drift_factor` según política temporal.
         - Genera lote de `NUM_RECORDS_PER_BATCH` registros.
         - Reparticiona por columnas de partición lógicas.
         - Escribe en Parquet (modo append) en `OUTPUT_PATH`.
         - Pausa `INTERVAL_SECONDS`.
      4) Finaliza la sesión Spark.

    Side effects:
        - Escritura de archivos Parquet particionados en HDFS.
        - Mensajes informativos por consola sobre progreso y drift aplicado.
    """
    # CLI/env args
    ap = argparse.ArgumentParser()
    ap.add_argument("--drift-factor", type=float, default=None, help="Fija el drift_factor en [0,1] para todas las filas (ignora política interna)")
    ap.add_argument("--batches", type=int, default=None, help="Sobrescribe NUM_BATCHES")
    ap.add_argument("--batch-size", type=int, default=None, help="Sobrescribe NUM_RECORDS_PER_BATCH")
    ap.add_argument("--interval", type=int, default=None, help="Sobrescribe INTERVAL_SECONDS entre lotes")
    args, _ = ap.parse_known_args()

    env_df = os.getenv("DRIFT_FACTOR")
    fixed_drift = args.drift_factor if args.drift_factor is not None else (float(env_df) if env_df else None)

    nb = args.batches if args.batches is not None else NUM_BATCHES
    bs = args.batch_size if args.batch_size is not None else NUM_RECORDS_PER_BATCH
    itv = args.interval if args.interval is not None else INTERVAL_SECONDS

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
    drift_factor = 0.0 if fixed_drift is None else float(max(0.0, min(1.0, fixed_drift)))

    for b in range(nb):
        # Política de drift: si se fija por CLI/env, se usa todo el tiempo;
        # si no, se mantiene el comportamiento anterior "forzado a 1.0".
        if fixed_drift is None:
            if (datetime.now() - start_cycle).total_seconds() / 60.0 >= DRIFT_CHANGE_EACH_MIN:
                drift_factor = 1.0
                start_cycle = datetime.now()
        print(f"--- Drift factor activo: {drift_factor:.2f} ---")

        now_ts = datetime.now()
        batch = [generate_one(now_ts, drift_factor) for _ in range(bs)]

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
        print(f"Lote {b+1}/{nb} -> {bs} filas escrito.")

        time.sleep(itv)

    print("✔ Generación finalizada.")
    spark.stop()

if __name__ == "__main__":
    main()
