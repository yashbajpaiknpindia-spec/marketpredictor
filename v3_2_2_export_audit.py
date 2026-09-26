from pathlib import Path
import py_compile, json, re, sys
ROOT=Path(__file__).resolve().parent
checks=[]
def check(name, cond, detail=''):
    checks.append({'check':name,'status':'PASS' if cond else 'FAIL','detail':detail})

# compile root Python
ok=True; errs=[]
for p in ROOT.glob('*.py'):
    if p.name==Path(__file__).name: continue
    try: py_compile.compile(str(p), doraise=True)
    except Exception as e: ok=False; errs.append(f'{p.name}: {e}')
check('python_compile_all',ok,'; '.join(errs) if errs else 'all root Python files compile')

he=(ROOT/'historical_export.py').read_text()
hf=(ROOT/'historical_fallbacks.py').read_text()
hx=(ROOT/'historical_extras.py').read_text()
app=(ROOT/'app.py').read_text()
readme=(ROOT/'README.md').read_text()

check('indstocks_documented_code_shape_only', 'c.get("variant") == "EXCH_ID"' in he and 'Do not send undocumented IDX_/INDEX_/bare token shapes' in he)
check('nifty_financial_alias_fixed', '"NIFTY FINANCIAL"' in he)
check('upstox_v3_historical', '/v3/historical-candle/{key}/{unit}/{iv}' in hf)
check('upstox_india_vix_documented_key', 'NSE_INDEX|India VIX' in hf)
check('upstox_equity_isin_fallback', 'NSE_EQ|{isin}' in hf and 'Trying Upstox fallback for missing stock-days' in he)
check('dhan_public_master', 'api-scrip-master-detailed.csv' in hf)
check('dhan_index_contract', '"exchangeSegment": "IDX_I"' in hf and '"instrument": "INDEX"' in hf)
check('dhan_no_manual_id_map', 'DHAN_INDEX_SECURITY_MAP_JSON' not in he)
check('upstox_corporate_actions_fallback', 'corporate_actions_upstox_fallback' in hx)
check('upstox_fii_dii_fallback', 'fii_dii_cash_upstox_fallback' in hx)
check('upstox_gift_nifty', 'gift_nifty_overnight_upstox' in hx and 'GIFT NIFTY' in hx)
check('upstox_global_fallback', 'UPSTOX_GLOBAL_ALIASES' in hx and 'Global Instruments + Historical Candle V3' in hx)
check('recent_news_marked_partial', 'news_upstox_recent_partial' in hx and 'past 7 days' in hx)
check('current_profiles_not_pit', 'NO — current profile only' in hx)
check('financials_not_causal_without_timestamp', 'do not expose values to historical decisions without a verified publication timestamp' in hx)
check('credential_missing_status', 'CREDENTIAL_MISSING' in he and 'CREDENTIAL_MISSING' in hx)
check('provider_attempt_log', 'reference/fallback_provider_attempts.csv' in he)
check('provider_capabilities_log', 'reference/provider_capabilities.csv' in he)
check('date_completeness', 'quality/date_completeness.csv' in he and 'HISTORICAL_MIN_DATE_COVERAGE' in he)
check('negative_volume_hygiene', 'NEGATIVE_VOLUME' in he and 'SET_TO_ZERO' in he)
check('duplicate_1530_fix', '15, 29' in he and '15, 30' in he and 'duplicate_close_rows_dropped' in he)
check('manifest_exporter_version', 'v3.2.2-multisource-data-export' in he)
check('estimate_exposes_provider_status', 'data_providers' in app and 'provider_note' in app)
check('readme_documents_fallback_chain', 'V3.2.2 Multi-Source Historical Data Export' in readme)
check('no_historical_microstructure_fabrication', 'Historical best bid/ask' in hx and 'Historical order-book depth' in hx)

passed=sum(x['status']=='PASS' for x in checks); failed=len(checks)-passed
out={'exporter_version':'v3.2.2-multisource-data-export','audit_type':'static + contract wiring audit','simulation_executed':False,'checks':len(checks),'passed':passed,'failed':failed,'ready_for_authenticated_export_test':failed==0,'results':checks}
(ROOT/'V3.2.2_EXPORT_AUDIT.json').write_text(json.dumps(out,indent=2),encoding='utf-8')
md=['# V3.2.2 Exporter Post-Change Audit','',f'- Checks: **{len(checks)}**',f'- Passed: **{passed}**',f'- Failed: **{failed}**','- Trading simulation executed: **NO**','- Authenticated export test ready: **YES**' if failed==0 else '- Authenticated export test ready: **NO**','', '| Check | Result | Detail |','|---|---|---|']
for r in checks: md.append(f"| {r['check']} | {r['status']} | {r['detail'].replace('|','/')} |")
(ROOT/'V3.2.2_EXPORT_AUDIT.md').write_text('\n'.join(md)+'\n',encoding='utf-8')
print(json.dumps(out,indent=2))
sys.exit(1 if failed else 0)
