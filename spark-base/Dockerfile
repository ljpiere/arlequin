FROM openjdk:11-jre-slim

ENV SPARK_VERSION="3.5.1"
ENV SPARK_HOME="/opt/spark"
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
ENV PYSPARK_PYTHON=python3
ENV PIP_DEFAULT_TIMEOUT=180

# Paquetes base + certificados + curl
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl ca-certificates python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Actualiza pip (mejores reintentos)
RUN python3 -m pip install --no-cache-dir --upgrade pip

# Instala pyspark y findspark con reintentos y más timeout
RUN set -eux; \
  for i in 1 2 3 4 5; do \
    pip3 install --no-cache-dir --disable-pip-version-check \
      --timeout 300 pyspark==3.5.1 findspark faker pyarrow && break || \
    (echo "pip falló intento $i; reintentando en 10s" && sleep 10); \
  done

WORKDIR /opt

# Descarga Spark binario (si lo necesitas además del wheel; de lo contrario, puedes omitirlo)
RUN curl -fSL --retry 5 --retry-delay 5 \
    https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    -o spark.tgz \
 && tar -xzf spark.tgz \
 && mv spark-${SPARK_VERSION}-bin-hadoop3 spark \
 && rm -f spark.tgz

# Copia configs (si existen)
COPY spark-conf/ $SPARK_HOME/conf/

EXPOSE 7077 8080 4040
