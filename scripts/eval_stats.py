#!/usr/bin/env python3
"""
eval_stats.py — Calcula pruebas no paramétricas (Mann–Whitney U) y tamaño
de efecto de Cliff (δ) a partir de un CSV de corridas/escenarios.

Entrada esperada (CSV): columnas mínimas
  - scenario: etiqueta del escenario (p.ej., E1, E2, E3)
  - f1: métrica principal en test (si existe)
Opcionales (si están presentes): psi, ttfd, ttr

Ejemplo de uso:
  python3 scripts/eval_stats.py --input metrics/training_log.csv \
      --out metrics/mannwhitney_results.csv

La salida imprime un resumen y guarda una tabla con:
  metric, A, B, nA, nB, pvalue, cliffs_delta

Si no existen al menos dos escenarios se genera un resumen global con
estadísticos descriptivos (media, std, cuantiles) que se concatena en el
mismo CSV. Para comparaciones, se aplica corrección FDR de
Benjamini–Hochberg sobre todos los p-values calculados, y se añade la
columna pvalue_fdr si se usa --fdr.
"""

from __future__ import annotations
import argparse
import math
from typing import List, Dict

import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu


def cliffs_delta(x: np.ndarray, y: np.ndarray) -> float:
    """Cliff's delta (δ) for two samples.

    δ = ( #pairs(x>y) - #pairs(x<y) ) / (n_x * n_y)
    """
    x = np.asarray(x)
    y = np.asarray(y)
    nx, ny = len(x), len(y)
    if nx == 0 or ny == 0:
        return float("nan")
    # Efficient computation using sorting and ranks
    # Fall back to pairwise if very small
    if nx * ny <= 10_000:
        gt = 0
        lt = 0
        for xi in x:
            gt += np.sum(xi > y)
            lt += np.sum(xi < y)
        return (gt - lt) / float(nx * ny)
    # Rank-based approximation
    xy = np.concatenate([x, y])
    order = np.argsort(xy, kind="mergesort")
    ranks = np.empty_like(order)
    ranks[order] = np.arange(len(xy))
    rx = ranks[:nx]
    ry = ranks[nx:]
    # Approximate delta using mean rank diff (scaled)
    return (np.mean(rx) - np.mean(ry)) * 2.0 / (nx + ny - 1)


def fdr_bh(pvals: List[float]) -> List[float]:
    """Benjamini–Hochberg FDR correction.
    Returns adjusted p-values in the original order.
    """
    p = np.asarray(pvals, dtype=float)
    n = p.size
    if n == 0:
        return []
    order = np.argsort(p)
    ranked = p[order]
    adj = np.empty_like(ranked)
    prev = 1.0
    for i in range(n - 1, -1, -1):
        rank = i + 1
        val = ranked[i] * n / rank
        prev = min(prev, val)
        adj[i] = min(prev, 1.0)
    out = np.empty_like(adj)
    out[order] = adj
    return out.tolist()


def compute_tests(df: pd.DataFrame, metrics: List[str], scenarios: List[str]) -> pd.DataFrame:
    rows = []
    pairs = []
    if len(scenarios) >= 2:
        for idx, a in enumerate(scenarios[:-1]):
            for b in scenarios[idx + 1:]:
                pairs.append((a, b))
    for m in metrics:
        if m not in df.columns:
            continue
        for a, b in pairs:
            va = df.loc[df["scenario"] == a, m].dropna().values
            vb = df.loc[df["scenario"] == b, m].dropna().values
            if len(va) == 0 or len(vb) == 0:
                pval = float("nan")
                delta = float("nan")
            else:
                try:
                    pval = float(mannwhitneyu(va, vb, alternative="two-sided").pvalue)
                except Exception:
                    pval = float("nan")
                delta = float(cliffs_delta(va, vb))
            rows.append({
                "metric": m,
                "A": a,
                "B": b,
                "nA": int(len(va)),
                "nB": int(len(vb)),
                "pvalue": pval,
                "cliffs_delta": delta,
                "row_type": "mannwhitney",
            })
    return pd.DataFrame(rows)


def summarize_metrics(df: pd.DataFrame, metrics: List[str], scenarios: List[str]) -> pd.DataFrame:
    """Return descriptive stats for global data + each scenario."""
    rows = []
    scopes = [("global", df)]
    for sc in scenarios:
        scoped = df[df["scenario"] == sc]
        scopes.append((f"scenario:{sc}", scoped))

    for scope_name, scoped_df in scopes:
        for m in metrics:
            series = pd.to_numeric(scoped_df[m], errors="coerce").dropna()
            n = len(series)
            if n == 0:
                continue
            rows.append({
                "metric": m,
                "A": scope_name,
                "B": "",
                "nA": int(n),
                "nB": 0,
                "pvalue": float("nan"),
                "cliffs_delta": float("nan"),
                "row_type": "summary",
                "summary_scope": scope_name,
                "summary_n": int(n),
                "summary_mean": float(series.mean()),
                "summary_std": float(series.std(ddof=0)),
                "summary_min": float(series.min()),
                "summary_p25": float(series.quantile(0.25)),
                "summary_median": float(series.median()),
                "summary_p75": float(series.quantile(0.75)),
                "summary_max": float(series.max()),
            })
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="CSV con columnas scenario y métricas (ej. f1, psi, ttfd, ttr)")
    ap.add_argument("--drift-input", default=None, help="CSV adicional (p.ej., generado por DriftWatch) para combinar metricas como psi")
    ap.add_argument("--out", default=None, help="Ruta de salida CSV con resultados")
    ap.add_argument("--fdr", action="store_true", help="Aplicar corrección FDR Benjamini–Hochberg")
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    if args.drift_input:
        try:
            df2 = pd.read_csv(args.drift_input)
            if "scenario" not in df2.columns and "SCENARIO" in df2.columns:
                df2["scenario"] = df2["SCENARIO"]
            df = pd.concat([df, df2], ignore_index=True, sort=False)
        except Exception as e:
            print("[WARN] No se pudo combinar drift-input:", e)
    if "scenario" not in df.columns:
        raise SystemExit("El CSV debe contener la columna 'scenario'.")

    df["scenario"] = df["scenario"].fillna("").astype(str)
    if not df["scenario"].str.strip().any():
        # Fallback so we can at least emit resumen global
        df["scenario"] = "default"
    else:
        df.loc[df["scenario"].str.strip() == "", "scenario"] = "default"

    metrics = [c for c in ("f1", "psi", "ttfd", "ttr") if c in df.columns]
    if not metrics:
        raise SystemExit("No se encontraron columnas de métricas esperadas (f1, psi, ttfd, ttr).")

    # Preserve order of appearance for scenarios to keep comparisons legible
    scenarios = []
    for val in df["scenario"]:
        if val not in scenarios:
            scenarios.append(val)

    res = compute_tests(df, metrics, scenarios)
    summaries = summarize_metrics(df, metrics, scenarios)

    if args.fdr and len(res) and res["pvalue"].notna().any():
        res["pvalue_fdr"] = fdr_bh(res["pvalue"].fillna(1.0).tolist())

    # Pretty print summary
    if len(res):
        print("Mann–Whitney por métrica y comparación:")
        for m in metrics:
            sub = res[res["metric"] == m]
            if not len(sub):
                continue
            print(f"- {m}:")
            for _, r in sub.iterrows():
                pv = r["pvalue"]
                cd = r["cliffs_delta"]
                pv_str = f"{pv:.4g}" if math.isfinite(pv) else "nan"
                cd_str = f"{cd:.3f}" if math.isfinite(cd) else "nan"
                print(f"  {r['A']} vs {r['B']}: p={pv_str}; δ={cd_str}")
    else:
        print("[INFO] No hay al menos dos escenarios etiquetados; se mostrará resumen global.")

    if len(summaries):
        global_rows = summaries[summaries["summary_scope"] == "global"]
        if len(global_rows):
            print("Resumen global por métrica:")
            for _, row in global_rows.iterrows():
                mean = row["summary_mean"]
                std = row["summary_std"]
                n = row["summary_n"]
                print(f"- {row['metric']}: media={mean:.4f}, std={std:.4f}, n={int(n)}")

    if args.out:
        out_path = args.out
        if len(res):
            out_df = res
        else:
            out_df = pd.DataFrame(columns=["metric", "A", "B", "nA", "nB", "pvalue", "cliffs_delta"])
        if len(summaries):
            out_df = pd.concat([out_df, summaries], ignore_index=True, sort=False)
        out_df.to_csv(out_path, index=False)
        print("Guardado:", out_path)


if __name__ == "__main__":
    main()
