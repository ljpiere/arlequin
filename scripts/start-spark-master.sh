#!/bin/bash
# Script para iniciar el Spark Master

echo "Iniciando Spark Master (Standalone Mode)..."
${SPARK_HOME}/sbin/start-master.sh -h 0.0.0.0

# Mantener el contenedor en ejecución
tail -f /dev/null