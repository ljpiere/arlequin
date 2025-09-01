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

2. Configurar variables de entorno:

- Copiar el archivo de plantilla y completar valores según tu entorno:

```bash
cp .env.template .env
# editar .env para ajustar rutas y URLs de servicios
```

3. Levantar el entorno con Docker Compose (archivo docker-compose.yml por crear):

```bash
docker compose up -d
```

Esto iniciará los contenedores de Hadoop, Spark, MLflow y Jenkins.

4. Generar datos sintéticos:
En otra terminal o a través de Jenkins, ejecuta:

```bash
python3 scripts/generate_data.py
```

Ajusta los parámetros (tamaño de lote, intervalo, factor de drift) según sea necesario.

5. Entrenar el modelo manualmente:

```bash
python3 scripts/train_model.py
```

Este script leerá los datos desde HDFS, generará las etiquetas, entrenará el modelo y registrará resultados en MLflow.

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