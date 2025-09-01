# Jenkins Pipeline – Entrenamiento con Spark

Este repositorio contiene un pipeline de Jenkins diseñado para ejecutar un proceso de entrenamiento de modelo utilizando Apache Spark en un clúster desplegado con Docker Compose.

## Descripción del Pipeline

El pipeline definido en Jenkinsfile contiene una etapa principal:

### Stage: Train with Spark

- Ejecuta un script de entrenamiento (train_model.py) mediante spark-submit.
- Usa el contenedor pyspark-client definido en docker-compose.yml.
- Conexión al Spark Master (spark://spark-master:7077).
- Acceso al sistema de archivos distribuido HDFS (hdfs://namenode:9000).

## Flujo de Ejecución

1. Jenkins levanta un agente disponible (agent any).
2. En la etapa Train with Spark:
    - Se ejecuta el comando:

```bash
docker compose exec -T pyspark-client bash -lc "
  spark-submit --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    /scripts/train_model.py
"
```

- Esto envía el job de entrenamiento a Spark para procesar los datos y generar el modelo.

3. En la sección post, se imprime un mensaje de cierre (Done.) siempre que el job termine, exitoso o no.

## Requisitos Previos

- Docker y Docker Compose instalados.

- Clúster de Spark y HDFS desplegado con Docker Compose (servicios spark-master, pyspark-client, namenode).

- Jenkins configurado con permisos para ejecutar docker compose.

- El script de entrenamiento disponible en la ruta:

```bash
./scripts/train_model.py
```

## Ejecución Manual

Si deseas probar el entrenamiento sin Jenkins:

```bash
docker compose exec -T pyspark-client bash -lc "
  spark-submit --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    /scripts/train_model.py
"
```

### Notas

Asegúrate de que los datos necesarios para el entrenamiento estén disponibles en HDFS en la ruta configurada por train_model.py.

Si cambias la ubicación del script o los parámetros de Spark, actualiza el Jenkinsfile en consecuencia.

Puedes añadir más stages (ej. pruebas, despliegue de modelo, validación) para extender este pipeline.