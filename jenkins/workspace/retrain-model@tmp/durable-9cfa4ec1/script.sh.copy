
          set -eu
          CID_PY=$(cat .cid_py)
          CMD="export MLFLOW_TRACKING_URI=${MLFLOW_URL};                spark-submit --master ${SPARK_MASTER_URL}                  --conf spark.hadoop.fs.defaultFS=${HDFS_URI}                  ${TRAIN_SCRIPT}"
          docker exec -i "$CID_PY" sh -lc "$CMD"
        