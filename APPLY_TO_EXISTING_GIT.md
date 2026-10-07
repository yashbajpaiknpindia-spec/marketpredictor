# Apply MarketPredictor v3.4.5 to an existing Git checkout

Copy these files over the matching paths in your repo:

- `app.py` → `app.py`
- `templates_index.html` → `templates/index.html`
- `market_data.py` → `market_data.py`
- `indstocks_client.py` → `indstocks_client.py`
- `strategies.py` → `strategies.py`
- `STRATEGY_PF_SOURCE_LEDGER.md` → `STRATEGY_PF_SOURCE_LEDGER.md`
- `V3.4.5_SCRIP_AND_TOURNAMENT.md` → `V3.4.5_SCRIP_AND_TOURNAMENT.md`

Then:

```bash
git status
git add app.py templates/index.html market_data.py indstocks_client.py strategies.py STRATEGY_PF_SOURCE_LEDGER.md V3.4.5_SCRIP_AND_TOURNAMENT.md
git commit -m "MarketPredictor v3.4.5: harden scrip errors and add independent strategy tournament"
git push
```

The ZIP itself does not update GitHub automatically; the files must be copied into the checkout and committed/pushed.
