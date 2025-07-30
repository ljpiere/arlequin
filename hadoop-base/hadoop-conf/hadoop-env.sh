# Set Hadoop-specific environment variables here.

# The java implementation to use.
export JAVA_HOME=/usr/local/openjdk-11 # <--- UPDATE THIS LINE
export HADOOP_HEAPSIZE_MAX=1024
export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true"
export HADOOP_LOG_DIR=${HADOOP_HOME}/logs
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop