# V3 Alpha-First Build

This is the first engineering layer for the new research architecture.

It intentionally does **not** claim a profitable strategy and does not authorize live trading.
The core separates:

- data-quality validation;
- next-executable-open labels;
- MFE/MAE/path labels;
- configurable cost accounting;
- economic gating;
- candidate records;
- experiment/research outputs.

No synthetic 1-minute history is created.

## Current baseline

The verified historical research corpus available for the current project is 5-minute data. Genuine historical 1-minute data is still a separate acquisition requirement for proving 1-minute execution behavior.

## Run a basic core check

```bash
python -m py_compile v3_core.py
python -c "import v3_core; print(v3_core.ENGINE_VERSION)"
```

The next research stage should connect this core to the verified market dataset, generate labels, and evaluate candidate families independently under walk-forward validation.
