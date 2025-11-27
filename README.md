# Arlequin – Sistema de reentrenamiento automatizado ante data drift

Arlequin es un prototipo de investigación orientado a la detección temprana de data drift y al reentrenamiento automático de modelos de ML en entornos Big Data. El objetivo es mantener la exactitud de los modelos cuando la distribución de entrada cambia, maximizando trazabilidad y reduciendo intervención manual.

## Demo en video

- Video de ejecución end-to-end: https://youtu.be/veM20n46DP8

## ¿Qué problema resuelve?

- Monitoreo continuo de desvíos estadísticos en producción (Kolmogorov–Smirnov, Chi-cuadrado, Kullback–Leibler, PSI).
- Reentrenamiento y despliegue automático vía Jenkins cuando se detecta drift.
- Registro completo de experimentos, artefactos y métricas con MLflow y CSVs operativos.

## Arquitectura

- Generador de datos sintéticos (`scripts/generate_data_session.py`) que simula transacciones bancarias e inyecta drift controlado.
- Almacenamiento en HDFS (NameNode + DataNode) y procesamiento con Spark.
- Monitoreo con `scripts/drift_watch.py`, exportación a Prometheus y disparo HTTP a Jenkins (`retrain-model`).
- Entrenamiento (`scripts/train_model.py`) y evaluación estadística (`scripts/eval_stats.py`) con artefactos en `metrics/`.
- Diagramas Mermaid y explicación en `docs/architecture.md`; guía de evaluación en `docs/evaluation.md`.

## Requisitos

- Docker y Docker Compose.
- Python 3.8+ (para scripts auxiliares).
- Java 8/11 para Hadoop/Spark.
- Paquetes Python: ver `scripts/requirements-drift.txt`.

## Puesta en marcha rápida

```bash
git clone git@github.com:ljpcastroc/arlequin.git
cd arlequin
```

1) Levanta los servicios base (Hadoop, Spark, cliente PySpark, Jenkins, monitoreo y MLflow):

```bash
docker compose up -d namenode datanode spark-master spark-worker pyspark-client jenkins grafana prometheus mlflow
```

2) Copia el generador y genera datos (E1 sin drift / E2 con drift):

```bash
docker compose exec spark-master bash -lc "mkdir -p /opt/spark/app"
SPARK=$(docker compose ps -q spark-master)
docker cp scripts/generate_data_session.py "$SPARK":/opt/spark/app/generate_data_session.py

# Base sin drift
docker compose exec spark-master bash -lc "\
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    /opt/spark/app/generate_data_session.py --drift-factor 0.0"

# Con drift
docker compose exec spark-master bash -lc "\
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    /opt/spark/app/generate_data_session.py --drift-factor 1.0"
```

Parámetros opcionales: `--batches`, `--batch-size`, `--interval`, `--drift-factor` (o env `DRIFT_FACTOR`).

## Monitoreo de drift (DriftWatch)

Ejecuta el watcher (en `pyspark-client` o como servicio `drift-watch`) exportando métricas y grabando PSI/p-valores a CSV:

```bash
export DRIFT_LOG_FILE=/tmp/arlequin-logs/drift_log.csv
export DRIFT_WINDOW_MINUTES=5      # referencia=[t-10,t-5), reciente=[t-5,t)
export DRIFT_COOLDOWN_SECONDS=300  # evita retriggers
export TRIGGER_EDGE_ONLY=1         # solo en flanco de subida
export DRIFT_ALPHA=0.01            # KS/Chi^2
python3 /scripts/drift_watch.py
```

- Exporta métricas Prometheus en `EXPORTER_PORT` (8010 por defecto).
- Usa ventanas móviles; no requiere `EVAL_SCENARIO` manual.
- `DRIFT_LOG_FILE` crea cabecera y agrega una fila por iteración.

Para correr como contenedor:

```bash
docker compose up -d --no-deps --force-recreate drift-watch
```

## Pipeline de reentrenamiento y evaluación

- `drift_watch.py` compara las últimas dos ventanas y, si `p < DRIFT_ALPHA` o `PSI > PSI_ALERT`, dispara Jenkins (`retrain-model`) respetando `DRIFT_COOLDOWN_SECONDS` y `TRIGGER_EDGE_ONLY`.
- Jenkins ejecuta `scripts/train_model.py`, registra en MLflow y copia CSVs a `metrics/` del workspace: `jenkins_runs.csv`, `training_log.csv`, `drift_log.csv`, `mannwhitney_results.csv`.
- Evaluación manual opcional:

```bash
python3 scripts/eval_stats.py \
  --input metrics/training_log.csv \
  --drift-input metrics/drift_log.csv \
  --out metrics/mannwhitney_results.csv --fdr
```

## Artefactos y trazabilidad

- `scripts/train_model.py`: escribe métricas si defines `EXPERIMENT_LOG_FILE` (timestamp, scenario, mlflow_run_id, label_strategy, total_rows, positives, negatives, model, f1).
- `scripts/drift_watch.py`: registra PSI y p-valores por columna (`DRIFT_LOG_FILE`) con columnas `timestamp, scenario, model, pos_ratio, drift_any, score_col`.
- Jenkins guarda consumo de CPU/Memoria de `pyspark-client` en `metrics/jenkins_runs.csv`.
- Resultados estadísticos combinados en `metrics/mannwhitney_results.csv` (Cliff’s delta y p-valores con/ sin FDR).

## Scripts clave

- `scripts/generate_data_session.py`: genera datos sintéticos con o sin drift.
- `scripts/train_model.py`: entrena (regresión logística), registra en MLflow y CSV.
- `scripts/drift_watch.py`: monitoreo de drift, exporta Prometheus y dispara Jenkins.
- `scripts/eval_stats.py`: compara escenarios (E1/E2/E3) con Mann–Whitney y Cliff’s delta.

## Próximos pasos

- Pruebas unitarias e integración (`tests/`).
- Dashboards y diagramas adicionales (`docs/`).
- Afinar thresholds y escenarios de drift controlado.

## Estructura rápida del repo

```bash
├── docker-compose.yml
├── scripts/              # generación, entrenamiento, monitoreo, evaluación
├── jenkins/              # pipeline automatizado
├── grafana/              # dashboards
├── docs/                 # diagramas Mermaid y guías
└── tests/                # pruebas (en progreso)
```

## Contribuciones

Las contribuciones son bienvenidas. Abre un issue para bugs o propuestas; para cambios grandes, envía un PR explicando motivación y alcance.

## Licencia

MIT. Puedes usar, modificar y distribuir el código manteniendo los avisos de copyright y licencia.
