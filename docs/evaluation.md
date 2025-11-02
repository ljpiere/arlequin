# Evaluación estadística

Este documento resume cómo interpretar y reportar las pruebas usadas:

- Prueba no paramétrica Mann–Whitney U por pares de escenarios (E1/E2/E3).
- Tamaño de efecto de Cliff (δ). Guía de interpretación:
  - |δ| < 0.147: trivial
  - < 0.33: pequeño
  - < 0.474: mediano
  - ≥ 0.474: grande
- Corrección por comparaciones múltiples mediante FDR (Benjamini–Hochberg) opcional.

## Reproducir tabla E1/E2/E3

Con los CSV generados por el pipeline (metrics/training_log.csv y metrics/drift_log.csv):

```bash
python3 scripts/eval_stats.py \
  --input metrics/training_log.csv \
  --drift-input metrics/drift_log.csv \
  --out metrics/mannwhitney_results.csv --fdr
```

El archivo `metrics/mannwhitney_results.csv` contendrá filas con:
`metric, A, B, nA, nB, pvalue, cliffs_delta[, pvalue_fdr]`.

