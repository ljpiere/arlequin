#!/bin/bash
# Script para iniciar el HDFS NameNode

echo "Configurando SSH..."
# Remove sshd-keygen and use ssh-keygen directly for host keys
# We will use ssh-keygen for user keys and assume host keys are handled by sshd startup or are not strictly required for this setup.
# The previous ssh-keygen -t rsa -f /root/.ssh/id_rsa -N '' generates user keys.
# Let's ensure host keys are also generated if sshd needs them. Often, sshd generates them on first run if missing.

# Ensure /var/run/sshd exists
mkdir -p /var/run/sshd
chmod 0755 /var/run/sshd

# Ensure password for root (for sshd)
echo "root:docker" | chpasswd

# Start SSH service in the background
/usr/sbin/sshd & # Just run sshd directly

echo "Formateando HDFS NameNode..."
if [ ! -d "/tmp/hadoop-namenode/current" ]; then
  hdfs namenode -format -force
else
  echo "NameNode ya formateado. Saltando el formato."
fi

echo "Iniciando HDFS NameNode..."
hdfs --daemon start namenode

# Mantener el contenedor en ejecución
tail -f /dev/null