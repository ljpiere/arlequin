
          set -eu
          CID_PY=$(cat .cid_py)
          METRICS_DIR="$WORKSPACE/metrics"
          mkdir -p "$METRICS_DIR"
          RUNS_CSV="$METRICS_DIR/jenkins_runs.csv"
          if [ ! -f "$RUNS_CSV" ]; then echo "timestamp,build,stage,cpu_perc,mem_perc,mem_used_mb,mem_limit_mb,duration_s" > "$RUNS_CSV"; fi

          # Pre-stats
          start_ts=$(date +%s)
          stats=$(docker stats --no-stream --format '{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}' "$CID_PY" || echo ",,")
          cpu=$(echo "$stats" | cut -d, -f1 | tr -d '%')
          mem_perc=$(echo "$stats" | cut -d, -f3 | tr -d '%')
          mem_pair=$(echo "$stats" | cut -d, -f2 | tr -d ' ' )
          used=$(echo "$mem_pair" | cut -d/ -f1 | sed 's/[^0-9.]*//g')
          limit=$(echo "$mem_pair" | cut -d/ -f2 | sed 's/[^0-9.]*//g')
          echo "$(date -Iseconds),$BUILD_NUMBER,pre,$cpu,$mem_perc,$used,$limit,0" >> "$RUNS_CSV"

          # Training
          docker exec -i "$CID_PY" sh -lc "            export MLFLOW_TRACKING_URI=${MLFLOW_URL};             export EXPERIMENT_LOG_FILE=/tmp/arlequin-logs/training_log.csv;             mkdir -p /tmp/arlequin-logs;             spark-submit --master ${SPARK_MASTER_URL}               --conf spark.hadoop.fs.defaultFS=${HDFS_URI}               ${TRAIN_SCRIPT}"

          # Post-stats
          end_ts=$(date +%s)
          dur=$((end_ts - start_ts))
          stats=$(docker stats --no-stream --format '{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}' "$CID_PY" || echo ",,")
          cpu=$(echo "$stats" | cut -d, -f1 | tr -d '%')
          mem_perc=$(echo "$stats" | cut -d, -f3 | tr -d '%')
          mem_pair=$(echo "$stats" | cut -d, -f2 | tr -d ' ' )
          used=$(echo "$mem_pair" | cut -d/ -f1 | tr -cd '0-9.
')
          limit=$(echo "$mem_pair" | cut -d/ -f2 | tr -cd '0-9.
')
          echo "$(date -Iseconds),$BUILD_NUMBER,post,$cpu,$mem_perc,$used,$limit,$dur" >> "$RUNS_CSV"

          # Pull training CSV from container (optional)
          docker cp "$CID_PY":/tmp/arlequin-logs/training_log.csv "$METRICS_DIR/" 2>/dev/null || true
          docker cp "$CID_PY":/tmp/arlequin-logs/drift_log.csv "$METRICS_DIR/" 2>/dev/null || true
        