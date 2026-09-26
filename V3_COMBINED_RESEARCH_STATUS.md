# V3 Combined Application + Research Status
## Integration
The V3 alpha-first core is integrated into the existing application backend as `v3_core.py`, with V3 configuration/status exposed through `/api/engine-v3/status` and V3 metadata attached to candidate records. Existing UI/routes and execution logic remain in place. Live authorization remains OFF.
## Verified data baseline
61 sessions, 225 symbols, 1,029,375 verified 5-minute rows from 2026-06-24 through 2026-09-18. No synthetic 1-minute history was used.
## Simulation results after the V3 foundation
- **Path/rule discovery:** No robust positive OOS rule; selected-rule tests remained negative.- **Joint entry + ATR path simulation:** Rank-1 selected rules: mean about -0.104% to -0.179% across ranks; no positive development rule qualified for holdout.- **Side-specific persistent symbol/phase prior:** 21 selected OOS trades; mean -0.1715%; PF 0.0527; 1/4 folds positive.- **Barrier/path LightGBM side regression:** 184 selected OOS observations; mean -0.1928%; 0 positive folds.- **Market/sector residual + VWAP rules:** Validation leaders were at best near zero in individual folds; no rule met the multi-fold promotion gate.- **Volume-expansion family:** Best recurring OOS rule around -0.161% mean; 0 positive folds for recurring candidates.- **Time-of-day/market-state prior:** 164,475 observations; mean -0.1514%; 0 positive folds.- **Logistic barrier classifier:** LONG -0.1551% mean / 0 positive folds; SHORT -0.1435% mean / 0 positive folds.- **Meta portfolio frequency test:** 48 OOS trades; mean -0.3701%; 0 positive folds.- **Cross-sectional ranking experiment:** Single-fold prototype selected mean -0.1512% on test; full walk-forward run was computationally stopped before completion and was not treated as a result.
## Existing V2/V2.3 baseline
- V2.3 broad portfolio: 120 trades, gross +₹2,845.61, mandatory fees -₹8,044.88, 5 bps stress -₹7,569.06, final -₹12,768.33.
- V2.3 classifier portfolio: 110 trades, gross +₹656.56, mandatory fees -₹7,272.87, 5 bps stress -₹6,843.33, final -₹13,459.64.
- V2.2 deep portfolio: 145 trades, gross +₹2,159.17, mandatory fees -₹7,478.50, 5 bps stress -₹7,125.61, final -₹12,444.94.

## Governance decision
The current data has not demonstrated a robust positive net edge. No model is marked proven or production-authorized. Repeated tuning against the same holdout would manufacture apparent edge, so V3 keeps the final authorization gate locked. The next genuine upgrade requires either stronger historical data (especially genuine 1-minute execution data and point-in-time market/universe context) or a genuinely new alpha hypothesis tested on untouched chronology.
