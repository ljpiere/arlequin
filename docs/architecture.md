# Arquitectura de Arlequin

```mermaid
flowchart LR
  subgraph Ingesta
    A[generate_data_session.py] -- spark-submit --> B[(HDFS Parquet: bank_transactions)]
  end

  subgraph Monitoreo
    B --> C[drift_watch.py]
    C -->|Prometheus metrics| D{{Prometheus}}
    C -->|CSV PSI/p-valores| E[(metrics/drift_log.csv)]
    C -->|HTTP Trigger| J[(Jenkins retrain-model)]
  end

  subgraph Entrenamiento
    J --> F[spark-submit train_model.py]
    F -->|MLflow| G[(Tracking Server)]
    F -->|CSV F1| H[(metrics/training_log.csv)]
  end

  subgraph Evaluación
    H --> I[eval_stats.py]
    E --> I
    I --> K[(metrics/mannwhitney_results.csv)]
  end
```

Notas
- El watcher compara ventanas móviles por `ingest_ts` (por defecto 5 minutos) y dispara Jenkins si detecta drift por KS/Chi² o PSI.
- El pipeline de Jenkins ejecuta el entrenamiento y luego la evaluación estadística, dejando artefactos CSV en `metrics/`.

