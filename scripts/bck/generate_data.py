# scripts/generate_data.py
import json
import time
import random
import socket
from datetime import datetime, timedelta
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
from pyspark.sql.functions import lit

# Configuración de HDFS
HDFS_OUTPUT_PATH = "hdfs:///user/bank_data"
TABLE_NAME = "bank_transactions" # Nombre lógico para la tabla en HDFS

# Inicializar Faker
fake = Faker('es_ES')

# Definir el esquema para el DataFrame de Spark (MUY IMPORTANTE para Parquet y consistencia)
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("customer_id", StringType(), True),
    StructField("account_number", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("balance_after_transaction", DoubleType(), True),
    StructField("is_suspicious", BooleanType(), True)
])

def get_base_transaction_amount():
    """Genera un monto base de transacción, que puede variar a lo largo del tiempo."""
    today = datetime.now().day
    if 5 <= today <= 10:
        return random.uniform(20.0, 500.0)
    elif 20 <= today <= 25:
        return random.uniform(50.0, 2000.0)
    else:
        return random.uniform(10.0, 1000.0)

def generate_single_record(record_timestamp, drift_factor=0):
    """Genera una única transacción bancaria como un diccionario."""
    transaction_amount = get_base_transaction_amount()

    if random.random() < drift_factor:
        transaction_amount *= random.uniform(1.5, 5.0)

    account_type = random.choice(['checking', 'savings', 'credit_card', 'loan'])
    transaction_type = random.choice(['deposit', 'withdrawal', 'transfer', 'payment', 'fee'])

    is_suspicious = False
    if transaction_amount > 1000 and random.random() < (0.01 + drift_factor * 0.1):
        is_suspicious = True

    return {
        "transaction_id": fake.uuid4(),
        "timestamp": record_timestamp,
        "customer_id": fake.uuid4(),
        "account_number": fake.bban(),
        "account_type": account_type,
        "transaction_type": transaction_type,
        "amount": round(transaction_amount, 2),
        "currency": random.choice(['USD', 'EUR', 'GBP', 'COP']),
        "merchant_name": fake.company() if random.random() > 0.3 else None,
        "location": fake.city(),
        "balance_after_transaction": round(random.uniform(100.0, 10000.0), 2),
        "is_suspicious": is_suspicious
    }

def generate_and_write_data(spark, num_records_per_batch=1000, interval_seconds=10, drift_change_interval_minutes=5, num_batches=3):
    """Genera datos en un número limitado de lotes y los escribe en HDFS."""
    print("Iniciando la generación y escritura de datos en HDFS...")
    
    start_time_drift_cycle = datetime.now()
    current_drift_factor = 0.0

    for i in range(num_batches):
        elapsed_minutes = (datetime.now() - start_time_drift_cycle).total_seconds() / 60
        if elapsed_minutes >= drift_change_interval_minutes:
            current_drift_factor = random.uniform(0.0, 0.5)
            start_time_drift_cycle = datetime.now()
            print(f"--- Data Drift factor actualizado a: {current_drift_factor:.2f} ---")

        batch_records = []
        current_timestamp = datetime.now()
        for _ in range(num_records_per_batch):
            batch_records.append(generate_single_record(current_timestamp, current_drift_factor))

        df = spark.createDataFrame(batch_records, schema=schema)
        write_path = f"{HDFS_OUTPUT_PATH}/{TABLE_NAME}"
        
        try:
            df.write \
              .mode("append") \
              .partitionBy("timestamp") \
              .parquet(write_path)
            print(f"Batch {i+1} de {num_records_per_batch} registros escrito en HDFS en {write_path}.")
        except Exception as e:
            print(f"ERROR al escribir a HDFS: {e}")
            break

        time.sleep(interval_seconds)

    print("Generación de datos finalizada.")


def wait_for_spark_master(host, port, timeout=120):
    """Espera a que el maestro de Spark esté disponible en el puerto especificado."""
    start_time = time.time()
    print(f"⏳ Esperando que el maestro de Spark esté disponible en {host}:{port}...")
    while True:
        if time.time() - start_time > timeout:
            raise TimeoutError(f"❌ Tiempo de espera agotado ({timeout}s) para el maestro de Spark en {host}:{port}")
        
        try:
            with socket.create_connection((host, port), timeout=5):
                print(f"✅ Maestro de Spark en {host}:{port} está disponible.")
                return
        except (socket.timeout, ConnectionRefusedError):
            time.sleep(5)


if __name__ == "__main__":
    spark_master_host = "spark-master"
    spark_master_port = 7077

    try:
        # 1. Esperar a que el maestro de Spark esté disponible
        wait_for_spark_master(spark_master_host, spark_master_port)

        # 2. Inicializar SparkSession
        spark = SparkSession.builder \
            .appName("BankDataGenerator") \
            .master(f"spark://{spark_master_host}:{spark_master_port}") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("WARN")

        # 3. Ejecutar la lógica de generación de datos
        generate_and_write_data(spark, num_records_per_batch=50, interval_seconds=5, drift_change_interval_minutes=1, num_batches=3)
        
        # 4. Detener la sesión de Spark
        spark.stop()

    except Exception as e:
        print(f"❌ Un error crítico ha ocurrido: {e}")
        exit(1)