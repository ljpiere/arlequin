import math
import numpy as np
import pandas as pd

from scripts.eval_stats import cliffs_delta, fdr_bh, compute_tests


def test_cliffs_delta_sign_and_scale():
    x = np.array([1, 1, 1, 1, 1])
    y = np.array([2, 2, 2, 2, 2])
    # x < y -> delta should be negative and close to -1
    d = cliffs_delta(x, y)
    assert d < -0.9

    # symmetric case
    d2 = cliffs_delta(y, x)
    assert d2 > 0.9


def test_fdr_bh_is_within_bounds_and_monotone():
    p = [0.001, 0.02, 0.04, 0.2, 0.8]
    adj = fdr_bh(p)
    assert all(0.0 <= v <= 1.0 for v in adj)
    # After sorting by raw p, adjusted must be non-decreasing
    order = np.argsort(p)
    sorted_adj = [adj[i] for i in order]
    assert sorted_adj == sorted(sorted_adj)


def test_compute_tests_detects_difference():
    # Build toy dataframe with clear separation: E2 has lower values
    df = pd.DataFrame({
        'scenario': ['E1']*30 + ['E2']*30 + ['E3']*30,
        'f1':       list(np.random.normal(0.80, 0.02, 30))
                    + list(np.random.normal(0.60, 0.02, 30))
                    + list(np.random.normal(0.78, 0.02, 30)),
    })

    res = compute_tests(df, metrics=['f1'], scenarios=['E1','E2','E3'])
    assert len(res) == 3  # three pairwise comparisons
    # E1 vs E2 should be significant
    p12 = float(res[(res.metric=='f1') & (res.A=='E1') & (res.B=='E2')]['pvalue'].iloc[0])
    assert p12 < 0.001
    # E2 vs E3 should be significant in the other direction
    p23 = float(res[(res.metric=='f1') & (res.A=='E2') & (res.B=='E3')]['pvalue'].iloc[0])
    assert p23 < 0.001

