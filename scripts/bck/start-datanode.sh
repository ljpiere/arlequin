#!/bin/bash
# Script para iniciar un HDFS DataNode

# Configura SSH (opcional, para propósitos de depuración o si los scripts de Hadoop lo requieren)
/usr/sbin/sshd-keygen
echo "root:docker" | chpasswd
mkdir -p /var/run/sshd
chmod 0755 /var/run/sshd
/usr/sbin/sshd -D & # Inicia SSH

echo "Iniciando HDFS DataNode..."
hdfs --daemon start datanode

# Mantener el contenedor en ejecución
tail -f /dev/null