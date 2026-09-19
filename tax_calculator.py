"""
India equity trading-cost and capital-gains-tax ESTIMATOR.

Two distinct things live here, kept separate on purpose:

  1. Trading costs (brokerage, STT, exchange fees, SEBI fee, stamp duty, GST,
     DP charges) — money that leaves your account on every trade, regardless
     of whether it's a profit or a loss. This is what makes gross P&L != net
     P&L on a single trade.

  2. Income tax on the resulting profit (STCG/LTCG for delivery, speculative
     business income for intraday) — this is NOT a per-trade number in
     reality: LTCG has a ₹1.25L/year exemption, STCG/LTCG/business-income are
     each pools you net across the whole financial year, and your actual
     slab rate depends on your total income (which this app has no way to
     know). So per-trade tax numbers here are illustrative; the /api/tax-
     report endpoint that aggregates by financial year is the meaningful one.

Every rate below is a default sourced from indmoney.com/pricing and current
public STT/stamp-duty/exchange-fee schedules as of mid-2026 — broker pricing
and government levies both change, and multiple secondary sources quoted
conflicting numbers for INDstocks' own brokerage. Override any of them via
env vars; nothing here is a substitute for your actual contract note or a
chartered accountant's sign-off on your filing.
"""
import os
import datetime


def _env_float(name: str, default: str) -> float:
    return float(os.environ.get(name, default))


# ---------------------------------------------------------------------------
# Trading-cost rates. INDIA_INTRADAY_* names match the pre-existing constants
# in app.py (kept as-is, used by the intraday engine's cost gate). The
# DELIVERY_* set below is new — same shape, different (delivery) rates.
# ---------------------------------------------------------------------------

# Brokerage: INDmoney's published equity-delivery rate is "0.05% or ₹20,
# whichever is lower, min ₹2 per executed order" (indmoney.com/pricing,
# corroborated by chittorgarh.com). Some comparison sites instead show a flat
# ₹20/order for delivery — if that matches your actual account, override
# DELIVERY_BROKERAGE_PCT to 0.
DELIVERY_BROKERAGE_PCT = _env_float('DELIVERY_BROKERAGE_PCT', '0.05')
DELIVERY_BROKERAGE_CAP_INR = _env_float('DELIVERY_BROKERAGE_CAP_INR', '20')
DELIVERY_BROKERAGE_MIN_INR = _env_float('DELIVERY_BROKERAGE_MIN_INR', '2')

# STT — the biggest single cost on a delivery trade. Official INDmoney
# pricing page: 0.1% of order value on BOTH buy and sell for delivery.
DELIVERY_STT_PCT = _env_float('DELIVERY_STT_PCT', '0.1')  # both sides

# Stamp duty (buy-side only) — uniform market-wide rate under the 2020
# stamp-duty reforms, not broker-specific.
DELIVERY_STAMP_DUTY_BUY_PCT = _env_float('DELIVERY_STAMP_DUTY_BUY_PCT', '0.015')

# Exchange transaction charges (NSE cash segment slab effective 2026), both sides.
DELIVERY_EXCHANGE_TXN_PCT = _env_float('DELIVERY_EXCHANGE_TXN_PCT', '0.00297')

# SEBI turnover fee — ₹10/crore, both sides. Identical across intraday/delivery.
DELIVERY_SEBI_PCT = _env_float('DELIVERY_SEBI_PCT', '0.0001')

# GST — 18% on (brokerage + exchange txn charges + SEBI fee) only. Not on STT
# or stamp duty (those are statutory levies, not a taxable service).
DELIVERY_GST_PCT = _env_float('DELIVERY_GST_PCT', '18')

# DP (depository participant) charge — official INDmoney rate: ₹18.50 + GST
# per unique ISIN sold as delivery in a session (not per order, not per
# share) — CDSL's own ₹3.25-3.50 plus INDstocks' own component. Applied once
# per (ticker, exit date) in the day-level aggregation, not per trade, since
# that's how it's actually billed.
DELIVERY_DP_CHARGE_PER_ISIN_INR = _env_float('DELIVERY_DP_CHARGE_PER_ISIN_INR', '18.50')
DELIVERY_DP_CHARGE_GST_PCT = _env_float('DELIVERY_DP_CHARGE_GST_PCT', '18')


def _pct(value: float, pct: float) -> float:
    return float(value or 0) * float(pct or 0) / 100.0


def _round2(value: float) -> float:
    return round(float(value or 0), 2)


def delivery_brokerage(turnover: float) -> float:
    if turnover <= 0:
        return 0.0
    pct_cost = _pct(turnover, DELIVERY_BROKERAGE_PCT)
    fee = min(pct_cost, DELIVERY_BROKERAGE_CAP_INR) if DELIVERY_BROKERAGE_CAP_INR > 0 else pct_cost
    return max(fee, DELIVERY_BROKERAGE_MIN_INR)


def delivery_charges_breakdown(entry_price: float, exit_price: float, quantity: int,
                                include_dp_charge: bool = True) -> dict:
    """Itemized trading costs for one delivery (CNC) round trip. Does NOT
    include income tax — see estimate_income_tax() / classify_holding()
    below for that."""
    qty = max(0, int(quantity or 0))
    entry = float(entry_price or 0)
    exit_px = float(exit_price or entry or 0)
    buy_turnover = max(entry * qty, 0.0)
    sell_turnover = max(exit_px * qty, 0.0)
    turnover = buy_turnover + sell_turnover
    if qty <= 0 or entry <= 0 or turnover <= 0:
        return {'total_cost': 0.0, 'currency_symbol': '₹'}

    brokerage = delivery_brokerage(buy_turnover) + delivery_brokerage(sell_turnover)
    stt = _pct(turnover, DELIVERY_STT_PCT)  # both sides for delivery
    exchange = _pct(turnover, DELIVERY_EXCHANGE_TXN_PCT)
    sebi = _pct(turnover, DELIVERY_SEBI_PCT)
    stamp = _pct(buy_turnover, DELIVERY_STAMP_DUTY_BUY_PCT)
    gst = _pct(brokerage + exchange + sebi, DELIVERY_GST_PCT)
    dp_charge = 0.0
    if include_dp_charge:
        dp_charge = DELIVERY_DP_CHARGE_PER_ISIN_INR * (1 + DELIVERY_DP_CHARGE_GST_PCT / 100.0)
    total = brokerage + stt + exchange + sebi + stamp + gst + dp_charge
    return {
        'currency_symbol': '₹',
        'buy_turnover': _round2(buy_turnover),
        'sell_turnover': _round2(sell_turnover),
        'turnover': _round2(turnover),
        'brokerage': _round2(brokerage),
        'stt': _round2(stt),
        'exchange_txn': _round2(exchange),
        'sebi': _round2(sebi),
        'stamp_duty': _round2(stamp),
        'gst': _round2(gst),
        'dp_charge': _round2(dp_charge),
        'total_cost': _round2(total),
    }


def delivery_net_pnl(entry_price: float, exit_price: float, quantity: int) -> dict:
    entry = float(entry_price or 0)
    exit_px = float(exit_price or entry or 0)
    qty = int(quantity or 0)
    gross = _round2((exit_px - entry) * qty)
    cost = delivery_charges_breakdown(entry, exit_px, qty)
    total_cost = float(cost.get('total_cost') or 0)
    net = _round2(gross - total_cost)
    buy_value = entry * qty
    return {
        'gross_pnl': gross,
        'gross_pnl_pct': _round2((gross / buy_value) * 100) if buy_value else 0.0,
        'cost_amount': _round2(total_cost),
        'net_pnl': net,
        'net_pnl_pct': _round2((net / buy_value) * 100) if buy_value else 0.0,
        'price_move_pct': _round2(((exit_px - entry) / entry) * 100) if entry else 0.0,
        'cost_breakdown': cost,
    }


# ---------------------------------------------------------------------------
# Income tax classification & estimate.
#
# Rates as of FY 2026-27 (Union Budget 2026 made no change to these):
#   - Speculative business income (intraday equity): taxed at your income
#     SLAB rate, same as salary/other income — NOT a flat rate. This app
#     doesn't know your total income, so INTRADAY_TAX_SLAB_RATE_PCT is a
#     configurable assumption (defaults to 30%, the top slab, as a
#     conservative/worst-case estimate).
#   - STCG on delivery held ≤12 months (Section 111A): flat 20%, provided
#     STT was paid (true for all normal delivery trades on-exchange).
#   - LTCG on delivery held >12 months (Section 112A): flat 12.5% on gains
#     above ₹1.25 lakh PER FINANCIAL YEAR (the exemption is an annual
#     aggregate, not per-trade — see estimate_financial_year_tax()).
# Source: multiple corroborating tax-guide summaries of Finance (No. 2) Act
# 2024 / Budget 2026, cross-checked mid-2026. Verify against the Income Tax
# Department portal / a CA before filing — this is not tax advice.
# ---------------------------------------------------------------------------

INTRADAY_TAX_SLAB_RATE_PCT = _env_float('INTRADAY_TAX_SLAB_RATE_PCT', '30')
STCG_EQUITY_RATE_PCT = _env_float('STCG_EQUITY_RATE_PCT', '20')
LTCG_EQUITY_RATE_PCT = _env_float('LTCG_EQUITY_RATE_PCT', '12.5')
LTCG_EQUITY_EXEMPTION_PER_FY_INR = _env_float('LTCG_EQUITY_EXEMPTION_PER_FY_INR', '125000')
LTCG_HOLDING_DAYS_THRESHOLD = int(_env_float('LTCG_HOLDING_DAYS_THRESHOLD', '365'))  # ~12 months, approximated


def classify_holding(product: str, opened_at, closed_at) -> str:
    """product: 'intraday' or 'delivery'. Returns one of:
    'speculative_business_income', 'stcg', 'ltcg'."""
    if str(product or '').lower() == 'intraday':
        return 'speculative_business_income'
    try:
        held_days = (closed_at.date() - opened_at.date()).days if opened_at and closed_at else 0
    except Exception:
        held_days = 0
    return 'ltcg' if held_days > LTCG_HOLDING_DAYS_THRESHOLD else 'stcg'


def financial_year_label(dt: datetime.date) -> str:
    """India's financial year runs 1 April - 31 March."""
    if dt.month >= 4:
        return f"FY{dt.year}-{str(dt.year + 1)[2:]}"
    return f"FY{dt.year - 1}-{str(dt.year)[2:]}"


def estimate_financial_year_tax(speculative_net: float, stcg_net: float, ltcg_net: float) -> dict:
    """Aggregate a financial year's three P&L pools into an estimated tax
    bill. Losses in a pool net against gains in the SAME pool only (this
    ignores set-off/carry-forward across pool types and prior-year losses,
    which real tax computation allows in specific, rule-bound ways a
    generic calculator can't safely assume) — treat this as a rough,
    conservative ceiling, not a filing-ready number."""
    speculative_taxable = max(float(speculative_net or 0), 0.0)
    stcg_taxable = max(float(stcg_net or 0), 0.0)
    ltcg_gross_taxable = max(float(ltcg_net or 0), 0.0)
    ltcg_after_exemption = max(ltcg_gross_taxable - LTCG_EQUITY_EXEMPTION_PER_FY_INR, 0.0)

    speculative_tax = _pct(speculative_taxable, INTRADAY_TAX_SLAB_RATE_PCT)
    stcg_tax = _pct(stcg_taxable, STCG_EQUITY_RATE_PCT)
    ltcg_tax = _pct(ltcg_after_exemption, LTCG_EQUITY_RATE_PCT)
    total_tax = speculative_tax + stcg_tax + ltcg_tax

    return {
        'speculative_business_income': {
            'net_pnl': _round2(speculative_net), 'taxable_amount': _round2(speculative_taxable),
            'rate_pct': INTRADAY_TAX_SLAB_RATE_PCT, 'estimated_tax': _round2(speculative_tax),
            'note': 'Taxed at your income slab rate, not a flat rate — this uses an assumed slab (override INTRADAY_TAX_SLAB_RATE_PCT).',
        },
        'stcg': {
            'net_pnl': _round2(stcg_net), 'taxable_amount': _round2(stcg_taxable),
            'rate_pct': STCG_EQUITY_RATE_PCT, 'estimated_tax': _round2(stcg_tax),
            'note': 'Delivery, held ≤12 months. Flat 20% under Section 111A, no exemption threshold.',
        },
        'ltcg': {
            'net_pnl': _round2(ltcg_net), 'exemption_applied': LTCG_EQUITY_EXEMPTION_PER_FY_INR,
            'taxable_amount': _round2(ltcg_after_exemption),
            'rate_pct': LTCG_EQUITY_RATE_PCT, 'estimated_tax': _round2(ltcg_tax),
            'note': 'Delivery, held >12 months. Flat 12.5% under Section 112A on gains above ₹1.25L/FY, no indexation.',
        },
        'total_estimated_tax': _round2(total_tax),
        'disclaimer': (
            'Estimate only, not tax advice. Ignores loss set-off/carry-forward across years, '
            'surcharge/cess, other income, and deductions. Confirm with a CA before filing.'
        ),
    }
