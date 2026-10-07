# UltraScalp Engine — MarketPredictor research surface

This build packages the event-driven scalper research engine and its evidence ledger into the priority Intraday workspace.

## Engine contract

- Causal OHLCV features only when running against the real 1-minute archive.
- Optional genuine L2 fields (OFI, L10 imbalance, microprice edge) when a real depth feed exists.
- Symmetric long/short scoring.
- Freshness decay and remaining-edge gate to reject late/chased entries.
- Explicit round-trip friction and entry slippage reserve.
- First-touch target/stop semantics with conservative same-bar collision handling.
- Maximum holding time and timeout exits.

## Evidence boundary

The packaged synthetic V8 result was generated in a controlled market-like L2 environment with 132 sessions and 11 regimes. It demonstrates that the architecture can extract edge when informative microstructure variables exist. It is **not historical NSE performance**.

The real archive is 1-minute OHLCV plus market context (NIFTY/VIX) for 83 sessions and 226 stocks. The latest frozen real-data tests on 25 September 2026 remained negative after costs; therefore this build does not authorize live deployment of the UltraScalp research layer.

## UI

The Scalper Engine is the first/priority workspace. AI Copilot and the older utility/research pages remain accessible through the eye menu rather than competing with the scalper workspace.
