# V2.3 Edge-First Research Upgrade

This build uses a completed-5m signal layer and a next-executable-bar execution reference. When real 1m history is present, 1m is retained for execution and resampled into completed 5m signal bars; when it is absent, the run is explicitly marked 5m compatibility mode.

Production is disabled until a model proves positive out-of-sample net edge after mandatory charges and a 5 bps slippage stress across expanding purged walk-forward folds.

See `research/v2_3/V23_FINAL_REPORT.md` for the complete tested results.
