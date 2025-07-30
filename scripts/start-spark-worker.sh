#!/bin/bash
# Script para iniciar un Spark Worker

# Espera a que el Spark Master esté disponible (ajusta según tu red)
# Puedes usar un bucle simple o herramientas como wait-for-it.sh
echo "Esperando que Spark Master esté disponible..."
# En Docker Compose, los nombres de servicio son resolubles
SPARK_MASTER_URL="spark://spark-master:7077"

echo "Iniciando Spark Worker conectando a ${SPARK_MASTER_URL}..."
${SPARK_HOME}/sbin/start-worker.sh ${SPARK_MASTER_URL}

# Mantener el contenedor en ejecución
tail -f /dev/null