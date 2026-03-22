# Walkthrough - 30-Day Option Pricing Alpha Finder

I have implemented a 30-day (calendar) return distribution tracker for major Chinese indices (SZ50, HS300, ZZ500, ZZ1000). This tool identifies the probability of significant price moves over a 30-day window, which can be compared against option pricing to find alpha.

## Key Implementation Details

1.  **30-Day Calendar Lookback**: The script calculates the forward return from each trading day to exactly 30 calendar days later (or the next available trading day if it falls on a weekend/holiday).
2.  **Dynamic Bucketing**:
    *   **50 points** for SZ50 and HS300.
    *   **100 points** for ZZ500 and ZZ1000.
3.  **Visualization**: Automatically generates a comparative histogram showing the distribution of moves.

## Summary Results (Since 2010-04-16)

| Index | 30d Avg Move (pts) | Prob Move < -Unit (%) | Prob Move > +Unit (%) |
| :--- | :--- | :--- | :--- |
| **SZ50** (SSE 50) | 3.84 | **48.26%** (<-50pt) | 37.43% (>+50pt) |
| **HS300** (CSI 300) | 5.96 | **49.77%** (<-50pt) | 40.15% (>+50pt) |
| **ZZ500** (CSI 500) | 17.33 | **47.14%** (<-100pt) | 40.55% (>+100pt) |
| **ZZ1000** (CSI 1000) | 3.68 | **50.25%** (<-100pt) | 41.66% (>+100pt) |

> [!NOTE]
> There is a consistent downward skew in the 30-day moves; the probability of dropping 1 unit (50 or 100 pts) is significantly higher than the probability of rising by the same amount, despite a slightly positive average point move.

## Visual Distribution

![30-Day Forward Point Move Distribution](/home/hallo/.gemini/antigravity/brain/8b328f6a-e492-4c45-a4b7-4fc9120faea9/alpha_30d_distribution.png)

## Files Created
- [alpha_finder_30d.py](file:///home/hallo/Documents/display/alpha_finder_30d.py): The main analysis script.
- `SZ50_30d_dist.csv`, `HS300_30d_dist.csv`, etc.: Detailed distribution data.
- `alpha_30d_distribution.png`: Comparative visualization of risk distributions.
