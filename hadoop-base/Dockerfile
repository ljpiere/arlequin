# Usa una imagen base con Java preinstalado
FROM openjdk:11-jre-slim

ENV HADOOP_VERSION="3.3.6"
ENV HADOOP_HOME="/opt/hadoop"
ENV PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

# Instala dependencias necesarias
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    openssh-server \
    net-tools \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Descarga e instala Hadoop
WORKDIR /opt
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar -xzf hadoop-$HADOOP_VERSION.tar.gz && \
    mv hadoop-$HADOOP_VERSION hadoop && \
    rm hadoop-$HADOOP_VERSION.tar.gz

# Configura SSH para comunicación interna (solo generación de claves y authorized_keys)
RUN mkdir -p /root/.ssh && \
    chmod 700 /root/.ssh && \
    ssh-keygen -t rsa -f /root/.ssh/id_rsa -N '' && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys

# Crea directorios para HDFS
RUN mkdir -p /tmp/hadoop-namenode && \
    mkdir -p /tmp/hadoop-datanode && \
    mkdir -p /tmp/hadoop-tmp

# Copia los archivos de configuración de Hadoop
COPY hadoop-conf/ $HADOOP_HOME/etc/hadoop/

# --- AGREGAR ESTAS LÍNEAS ---
# Copia los scripts de inicio al contenedor y dales permisos de ejecución
# COPY scripts/ /scripts/
# RUN chmod +x /scripts/*.sh
# -----------------------------

# Permiso para scripts de inicio (ya no es estrictamente necesario si los copiamos a /scripts/)
# RUN chmod +x $HADOOP_HOME/sbin/*

EXPOSE 9000 9870 9864 
# Puertos para HDFS NameNode, DataNode y UI