# Usa la misma imagen base con Java que Hadoop
FROM openjdk:11-jre-slim

ENV SPARK_VERSION="3.5.1"
ENV SPARK_HOME="/opt/spark"
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

# Instala dependencias necesarias
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Instala Python libraries for PySpark
RUN pip3 install pyspark findspark

# Descarga e instala Spark
WORKDIR /opt
RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz && \
    tar -xzf spark-$SPARK_VERSION-bin-hadoop3.tgz && \
    mv spark-$SPARK_VERSION-bin-hadoop3 spark && \
    rm spark-$SPARK_VERSION-bin-hadoop3.tgz

# Copia los archivos de configuración de Spark
COPY spark-conf/ $SPARK_HOME/conf/

# --- AGREGAR ESTAS LÍNEAS ---
# Copia los scripts de inicio al contenedor y dales permisos de ejecución
# COPY scripts/ /scripts/
# RUN chmod +x /scripts/*.sh
# -----------------------------

# Permiso para scripts de inicio
# RUN chmod +x $SPARK_HOME/sbin/*

EXPOSE 7077 8080 4040 
# Puertos para Spark Master, Spark UI