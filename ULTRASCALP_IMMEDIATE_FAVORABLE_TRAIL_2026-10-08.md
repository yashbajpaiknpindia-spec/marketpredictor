# UltraScalp V12 — Immediate Favorable-Move Trail

This build preserves the positive-blind direction + edge policy and changes only the profit-protection activation behavior.

## Change
The V12 trailing protection now arms on the first movement that is net-positive after configured round-trip friction and entry slippage. It no longer waits for the +0.20% net economic target before arming the trailing layer.

The configured +0.20% net economic target remains unchanged as the economic execution objective. The +0.60% gross execution target, -0.18% hard stop, 0.1363% friction, 0.015% slippage, 70% model probability gate, direction margin, and 30-minute safety timeout are unchanged.

## Rationale
Earlier forensic results showed several trades reaching favorable MFE and subsequently losing or timing out. The trailing layer should protect favorable movement as soon as it is genuinely net-positive rather than waiting for the larger economic target.

## Validation
- Python compilation: passed for modified live-paper and replay modules.
- Immediate favorable-trail regression tests: 5/5 passed.
