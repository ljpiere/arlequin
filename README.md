# Arlequin – Sistema de reentrenamiento automatizado ante data drift

Arlequin es un prototipo de investigación de maestría orientado a la detección temprana de data drift y al reentrenamiento automático de modelos de machine learning en entornos Big Data. El objetivo es garantizar que los modelos se mantengan precisos cuando la distribución de los datos de entrada cambia, minimizando la intervención humana y maximizando la trazabilidad y la eficiencia operativa.

## ¿Qué problema resuelve?

En contextos como salud, finanzas, industria o comercio electrónico, los modelos de ML pueden perder exactitud cuando los datos en producción ya no siguen la misma distribución que los datos de entrenamiento. Arlequin propone:

- Monitoreo continuo de los datos de entrada para detectar desviaciones estadísticas significativas (pruebas de Kolmogorov–Smirnov, Chi‑cuadrado y Kullback–Leibler).

- Orquestación automática con Jenkins de pipelines que reentrenan, validan y despliegan nuevos modelos cuando se detecta data drift.

- Registro y trazabilidad de experimentos, modelos y métricas mediante MLflow.

## Arquitectura y componentes principales

La siguiente lista describe los componentes clave; encontrarás diagramas detallados en la carpeta docs/ (por crear):

- **Generador de datos sintéticos (scripts/generate_data.py):** simula transacciones bancarias y permite inducir drift en los montos de las transacciones.

- **Almacenamiento en HDFS:** los datos se persisten en un clúster pseudo‑distribuido de Hadoop (NameNode + DataNode). Los scripts start-namenode.sh y start-spark-*.sh permiten levantar los servicios dentro de contenedores.

- **Procesamiento con Spark:** lectura y transformación de datos usando pyspark y preparación de datasets para entrenamiento.

- **Script de entrenamiento (scripts/train_model.py):** genera etiquetas a partir de reglas o columnas existentes y entrena un modelo de regresión logística. Registra métricas y artefactos en MLflow.

- **Script de detección de drift:** módulo que implementará pruebas estadísticas para detectar cambios significativos en la distribución de los datos y disparará el pipeline de reentrenamiento.

- **Jenkins/CI:** define el pipeline de CI/CD que automatiza la detección de drift, el reentrenamiento y el registro de modelos.

- **MLflow:** gestiona versiones de modelos, métricas y parámetros de experimentos.

## Requisitos de software

- Docker y Docker Compose para orquestar contenedores.

- Python 3.8 o superior.

- Java 8/11 (necesario para Hadoop/Spark).

- Apache Hadoop ≥ 3.4 y Apache Spark ≥ 3.5.

- Python packages (ver `scripts/requirements-drift.txt`).

## Instalación y puesta en marcha

1. Clonar el repositorio:

```bash
git clone git@github.com:ljpcastroc/arlequin.git
cd arlequin
```

### Ejecutar DriftWatch con logging a CSV

En un terminal (o como servicio), ejecuta DriftWatch exportando Prometheus y escribiendo PSI/p-values a CSV. Ejemplo dentro del contenedor `pyspark-client`:

```bash
export DRIFT_LOG_FILE=/tmp/arlequin-logs/drift_log.csv
python3 /scripts/drift_watch.py
```

Notas:
- No necesitas cambiar EVAL_SCENARIO: el watcher funciona de manera continua y compara ventanas móviles de 5 minutos (configurable con `DRIFT_WINDOW_MINUTES`). Referencia = [t-10, t-5), Reciente = [t-5, t).
- El fichero se crea con cabecera automáticamente y agrega una fila por iteración del loop.
- Para parar, Ctrl+C. El exporter Prometheus usa el puerto `EXPORTER_PORT` (8010 por defecto).

### Generar datos con o sin drift desde spark-submit

El generador acepta `--drift-factor` (o env `DRIFT_FACTOR`) para fijar el drift sin editar el código:

```bash
docker compose exec spark-master bash -lc "\
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    /opt/spark/app/generate_data_session.py --drift-factor 0.0"

# con drift
docker compose exec spark-master bash -lc "\
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    /opt/spark/app/generate_data_session.py --drift-factor 1.0"
```

Parámetros adicionales opcionales: `--batches`, `--batch-size`, `--interval`.


## Ejecucion paso a paso (Windows/PowerShell)

1) Copiar el generador al contenedor de Spark master

```powershell
docker compose exec spark-master bash -lc "mkdir -p /opt/spark/app"
$SPARK = docker compose ps -q spark-master
docker cp .\scripts\generate_data_session.py $SPARK`:/opt/spark/app/generate_data_session.py
```

2) Levantar los servicios principales

```powershell
docker compose up -d namenode datanode spark-master spark-worker pyspark-client jenkins grafana prometheus mlflow
```

3) Configurar y lanzar el watcher (ventana movil)

```powershell
$env:DRIFT_WINDOW_MINUTES = "5"       # referencia=[t-10,t-5), reciente=[t-5,t)
$env:DRIFT_COOLDOWN_SECONDS = "300"   # evita retriggers
$env:TRIGGER_EDGE_ONLY = "1"          # solo al flanco de subida
$env:DRIFT_ALPHA = "0.01"             # significancia para KS/Chi^2
$env:DRIFT_LOG_FILE = "/tmp/arlequin-logs/drift_log.csv"  # opcional

docker compose up -d --no-deps --force-recreate drift-watch
```

4) Generar datos de prueba

- Base sin drift (E1):

```powershell
docker compose exec spark-master bash -lc `
"/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /opt/spark/app/generate_data_session.py --drift-factor 0.0"
```

- Con drift (E2):

```powershell
docker compose exec spark-master bash -lc `
"/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  /opt/spark/app/generate_data_session.py --drift-factor 1.0"
```

5) Comportamiento esperado
- DriftWatch compara siempre las dos ultimas ventanas y, si detecta drift (p < `DRIFT_ALPHA` o PSI > `PSI_ALERT`), dispara Jenkins (`retrain-model`) respetando `DRIFT_COOLDOWN_SECONDS` y `TRIGGER_EDGE_ONLY`.
- El pipeline entrena y evalua, dejando CSVs en `metrics/` del workspace: `jenkins_runs.csv`, `training_log.csv`, `drift_log.csv`, `mannwhitney_results.csv`.

6) Evaluacion manual (opcional)

```powershell
python3 scripts/eval_stats.py `
  --input metrics/training_log.csv `
  --drift-input metrics/drift_log.csv `
  --out metrics/mannwhitney_results.csv --fdr
```

## Logs operativos y evaluación 

- El script `scripts/train_model.py` ahora puede escribir un CSV de métricas si defines `EXPERIMENT_LOG_FILE` (ruta dentro del contenedor o del host). Se registran: `timestamp, scenario, mlflow_run_id, label_strategy, total_rows, positives, negatives, model, f1`.
- El pipeline de Jenkins guarda consumo de CPU/Memoria del contenedor `pyspark-client` antes y después de cada build en `metrics/jenkins_runs.csv` y copia `training_log.csv` desde el contenedor a `metrics/`.
- `scripts/drift_watch.py` ahora puede registrar directamente PSI y p-values por columna a CSV si defines `DRIFT_LOG_FILE`. También incluye las columnas `timestamp`, `scenario`, `model`, `pos_ratio`, `drift_any` y `score_col`. Esto conecta automáticamente con el evaluador.
- Se añadió `scripts/eval_stats.py` para calcular pruebas de Mann–Whitney y tamaño de efecto de Cliff (δ) a partir de `metrics/training_log.csv`.

Cómo probar rápidamente:
- Ejecuta el job `retrain-model`. Tras finalizar, en el workspace del job encontrarás `metrics/jenkins_runs.csv` y, si el entrenamiento generó datos, `metrics/training_log.csv`.
- Opcional: define `EVAL_SCENARIO` (por ejemplo, `E1`, `E2`, `E3`) en el entorno del job para etiquetar corridas.
- Para obtener la tabla estadística combinando F1 (entrenamiento) y PSI (drift):
  - Opción Jenkins (automática): el pipeline añade una etapa "Evaluate stats" que ejecuta `scripts/eval_stats.py` dentro del contenedor y guarda `metrics/mannwhitney_results.csv` en el workspace.
  - Opción manual: `python3 scripts/eval_stats.py --input metrics/training_log.csv --drift-input metrics/drift_log.csv --out metrics/mannwhitney_results.csv --fdr`.

## Próximos pasos

El proyecto está en desarrollo activo. Las siguientes características están planificadas:

(**Done**) Implementación del módulo detect_drift.py para aplicar pruebas de Kolmogorov–Smirnov, Chi‑cuadrado y Kullback–Leibler con umbrales configurables.

(**Done**)Creación de un Jenkinsfile que automatice la detección de drift, el reentrenamiento y el registro de modelos.

Incorporación de pruebas unitarias e integración para asegurar la calidad del código (carpeta tests/).

Inclusión de diagramas de arquitectura y documentación adicional en docs/.


## Estructura recomendada

```bash
├── .env.template        # Plantilla de variables de entorno
├── docker-compose.yml   # Configuración de servicios (por crear)
├── scripts/
│   ├── generate_data.py  # Generación de datos sintéticos
│   ├── train_model.py    # Entrenamiento de modelos
│   └── detect_drift.py   # Detección de *data drift* (por implementar)
├── jenkins/             # Configuración de Jenkins (pipeline automatizado)
├── grafana/             # Configuración de dashboards (opcional)
├── docs/                # Documentación, diagramas y cronograma
└── tests/               # Pruebas unitarias e integración (por crear)
```

## Contribuciones

Las contribuciones son bienvenidas. Por favor abre un issue para reportar errores o proponer mejoras. Para cambios importantes, crea un pull request describiendo la motivación y los cambios propuestos.

## Licencia

Este proyecto se publica bajo la Licencia [MIT](https://opensource.org/license/MIT)
. Puedes usar, modificar y distribuir el código siempre que mantengas los avisos de copyright y licencia.