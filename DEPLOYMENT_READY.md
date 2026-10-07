# MarketPredictor — deployable build

This package is a complete application build based on the uploaded `marketpredictor-main (4).zip`, with the validated UltraScalp research surface merged into the newest application code.

## Render

The repository root contains:

- `requirements.txt`
- `.python-version` (`3.11.9`)
- `render.yaml`
- `Procfile`
- `app.py`
- `templates/`
- all application modules, replay/strategy code, and research artifacts

Use the existing MarketPredictor PostgreSQL database by setting `DATABASE_URL` in Render. The supplied `render.yaml` deliberately does **not** create a new database, so an existing production database is not replaced by a Blueprint deployment.

Recommended Render commands if configured manually:

```text
Build: pip install -r requirements.txt
Start: gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --worker-class gthread --timeout 120
Health: /healthz
```

The build pins Python through `.python-version` and the Blueprint environment to 3.11.9.

## Safety defaults

The deploy manifest defaults new Blueprint deployments to paper/research mode with live trading disabled. Existing service environment variables remain authoritative when redeploying an already-configured service.

## UltraScalp evidence

The priority Scalper Engine page contains the complete packaged research ledger, including synthetic L2 experiments, real NSE OHLCV tests, late-entry rules, and downloadable artifacts. Synthetic performance is clearly marked as research-only and does not authorize live execution.

The raw 1-minute historical archive is intentionally **not** bundled into the web application image. It remains a separate research data asset so a Render deploy does not clone an unnecessary 90+ MB historical ZIP.
