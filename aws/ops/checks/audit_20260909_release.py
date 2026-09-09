"""Metadata-only release gate; AWS access exists only behind injected clients.

Source parity is independent of output provenance. No payload/position/note text
is returned. Unknown outputs and absent source-version markers stay explicit.
"""
from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / 'aws/shared'), str(ROOT / 'aws/ops/checks'), str(ROOT / 'scripts')]
from donor_contract import numeric, parse_timestamp
from private_artifact import is_private_source
from public_brain_projection import source_map_public, fleet_errors_public
from release_package_evidence import check_packages, release_config, shared_imports
from scheduler_payload import scheduler_payload
from audit_20260909_accounting import inspect_payload
from audit_20260909_risk import check_artifact

BASE = '125a68a89268c5ff8303dfba4177026dcf1e3c21'
BUCKET = 'justhodl-dashboard-live'
REGION = 'us-east-1'
ACCOUNT = '857687956942'
PRIMARY = {
    'justhodl-backtest-engine':['backtest/results.json'],
    'justhodl-research-backtest':['analytics/backtest_results.json'],
    'justhodl-calibration-snapshotter':['calibration/model-latest.json'],
    'justhodl-calibrator':['calibration/latest.json'],
    'justhodl-options-flow-scanner':['data/options-flow-scanner.json'],
    'justhodl-options-flow':['flow-data.json'],
    'justhodl-bloomberg-v8':['data/bloomberg-report.json'],
    'justhodl-daily-report-v3':['data/report.json'],
    'justhodl-ecb-derived':['data/ecb-derived.json'],
    'justhodl-whats-changed':['data/whats-changed.json','data/snapshots-index.json'],
    'justhodl-portfolio-snapshot':['portfolio/snapshot.json'],
    'justhodl-fleet-freshness-monitor':['data/_freshness-monitor.json'],
    'justhodl-fleet-error-monitor':['data/_fleet-monitor.json'],
    'justhodl-source-map':['data/source-map.json'],
    'justhodl-ka-metrics':['data/ka-metrics.json','data/ka-analysis.json'],
    'justhodl-khalid-metrics':['data/khalid-metrics.json','data/khalid-analysis.json'],
    'justhodl-stock-screener':['screener/data.json'],
    'justhodl-conviction-engine':['data/conviction.json'],
    'justhodl-sizing-engine':['data/sizing.json'],
    'justhodl-etf-constituents':['etf-flows/constituent-pressure.json'],
    'justhodl-risk-sizer':['data/risk-sizer.json','risk/recommendations.json'],
}
# Reviewed regular handlers publish research/data only and have no notification
# path. No generic discovery-based invocation is permitted. Portfolio snapshot's
# ordinary watchlist synchronization and ECB-derived's dated research-signal
# recording are part of their authorized normal refresh; neither submits trades.
QUIET_STAGES = (
    ('tradingview','short-interest','liquidity-profile','etf-true-flows','calibration-snapshotter','source-map'),
    ('factor-risk','liquidity-capacity','conviction-engine'),
    ('risk-gate',),
    ('engine-fusion',),
    ('khalid-risk',),
    ('portfolio-snapshot',),
    ('katlin','risk-sizer','squeeze-fuel','trade-tickets','crypto-basis','firm-risk-board'),
    ('sizing-engine',),
    ('backtest-engine','research-backtest','options-flow-scanner','bloomberg-v8','ecb-derived'),
    ('whats-changed','public-archive-index','fleet-freshness-monitor','fleet-error-monitor'),
)
# These two reviewed handler branches suppress all notification/history side effects.
# The default event is never used for these notification-capable engines.
APPROVED_REFRESH_MODES = {
    'justhodl-fleet-freshness-monitor': {'mode':'quiet_refresh'},
    'justhodl-fleet-error-monitor': {'mode':'audit_refresh'},
}
QUIET_FUNCTIONS = {'justhodl-'+name for stage in QUIET_STAGES for name in stage}
# Calibrator changes live SSM weights and emits an EventBridge event. Observe its
# normal publication only; its report must replace any old colliding model file.
OBSERVED_FUNCTIONS = {'justhodl-calibrator','justhodl-ka-metrics','justhodl-khalid-metrics'}
FUNCTION_URL_BINDINGS = (
    ('fmp.html','fmp-fundamentals-agent','nwjtcrf4xwkc6n5r6u3vw7ub6m0wgpiv.lambda-url.us-east-1.on.aws'),
    ('census.html','fedliquidityapi','mjqyipzzwjcmx44irtvijecswm0nkikf.lambda-url.us-east-1.on.aws'),
)
# The retired Census URL was not found in the complete metadata scan5241.
# Page now consumes fedliquidityapi's source-defined summary/catalog/series schema.
UNRESOLVED_FUNCTION_URLS = ()
REFRESH_DEPENDENCIES = {'justhodl-backtest-engine': ('justhodl-calibration-snapshotter',)}
OUTPUT_CONTRACTS = {
    'calibration/model-latest.json': 'immutable_model_snapshot_with_availability',
    'calibration/latest.json': 'calibrator_horizon_and_accuracy_report',
    'data/source-map.json': 'public-source-map.v1',
    'data/_freshness-monitor.json': 'fleet-freshness-monitor.v3',
    'data/_fleet-monitor.json': 'fleet-errors-metadata-20260909-v1',
    'data/ka-analysis.json': 'macro-analysis.v1_ka_owned',
    'data/khalid-analysis.json': 'macro-analysis.v1_khalid_owned',
    'data/options-flow-scanner.json': 'options_flow_scanner_v1_ranked_equities',
    'flow-data.json': 'options_flow_and_sentiment_v3_nested_market_data',
    'data/bloomberg-report.json': 'bloomberg_v8_market_report',
    'data/report.json': 'daily_report_v10_market_report',
    'data/ecb-derived.json': 'ecb_derived_3.4_indicators',
    'data/snapshots-index.json': 'daily-snapshot-index.v1_source_owned_mutable_copies',
}
DAILY_SNAPSHOT_SOURCES = frozenset((
    'data/morning-intel.json','data/macro-surprise.json','data/yield-curve.json','data/correlation-surface.json',
    'data/historical-analogs.json','data/event-study.json','data/ab-test-results.json',
    'portfolio/signal-portfolio-state.json','data/13f-positions.json','data/short-interest.json',
    'data/earnings-tracker.json','data/best-setups.json','data/opportunities.json','data/signal-board.json',
))
RISK_KINDS = {'katlin','risk-sizer','khalid-risk','risk-gate','engine-fusion'}
ACCOUNTING_KEYS = {'portfolio/snapshot.json','backtest/results.json','analytics/backtest_results.json',
                   'calibration/model-latest.json','data/_freshness-monitor.json'}
MAX_AGE_H = {'justhodl-engine-fusion':2,'justhodl-khalid-risk':2,'justhodl-risk-sizer':2,
             'justhodl-risk-gate':48,'justhodl-katlin':36,'justhodl-portfolio-snapshot':3,
             'justhodl-crypto-funding':2,'justhodl-crypto-basis':2,'justhodl-factor-risk':48,
             'justhodl-short-interest':72,'justhodl-calibration-snapshotter':192,'justhodl-calibrator':192,
             'justhodl-backtest-engine':8,'justhodl-research-backtest':30}
MAX_AGE_H['justhodl-whats-changed']=30
MAX_AGE_H['justhodl-public-archive-index']=1
SAFE_STATE = re.compile(r'^[A-Za-z0-9_.-]{1,64}$')


def utcnow(): return datetime.now(timezone.utc)


def safe_label(value):
    return value if isinstance(value,str) and SAFE_STATE.fullmatch(value) else None


def git(root, *args):
    return subprocess.check_output(['git',*args],cwd=root,text=True,stderr=subprocess.DEVNULL).strip()


def changed_scope(root, base=BASE):
    """Exact source/config changes and transitive shared importers; archived excluded."""
    git(root,'cat-file','-e',base+'^{commit}')  # Missing history is a gate failure.
    changed=git(root,'diff','--name-only',base,'HEAD','--','aws/lambdas','aws/shared').splitlines()
    shared_changed={Path(p).stem for p in changed if p.startswith('aws/shared/') and len(Path(p).parts)==3 and p.endswith('.py')}
    reasons={}
    for path in changed:
        parts=Path(path).parts
        if len(parts)>=4 and parts[:2]==('aws','lambdas') and parts[2]!='_archived' and (parts[3]=='source' or parts[3]=='config.json'):
            reasons.setdefault(parts[2],set()).add('source_or_config_changed')
    for directory in sorted((root/'aws/lambdas').iterdir()):
        if not directory.is_dir() or directory.name=='_archived': continue
        source=directory/'source'
        if not source.exists(): continue
        files=list(source.rglob('*.py'))
        # Shared imports are inspected transitively; source text is never reported.
        try:
            imported={p.stem for p in shared_imports(root,files)}
        except (SyntaxError,UnicodeError):
            if any(any(name in p.read_text(errors='replace') for name in shared_changed) for p in files):
                reasons.setdefault(directory.name,set()).add('shared_import_analysis_failed')
            continue
        hits=imported & shared_changed
        if hits: reasons.setdefault(directory.name,set()).update('shared:'+name for name in hits)
    for name in QUIET_FUNCTIONS:
        reasons.setdefault(name,set()).add('reviewed_dependency_refresh')
    for name in OBSERVED_FUNCTIONS:
        reasons.setdefault(name,set()).add('scheduled_report_ownership_verification')
    for _,name,_ in FUNCTION_URL_BINDINGS:
        reasons.setdefault(name,set()).add('function_url_identity_verification')
    return {name:sorted(why) for name,why in sorted(reasons.items()) if (root/'aws/lambdas'/name/'source').exists()}


def public_archive_registry(root):
    """Read literal membership from the exact source release, without importing it."""
    source=root/'aws/lambdas/justhodl-public-archive-index/source/lambda_function.py'
    tree=ast.parse(source.read_text())
    assignments=[node.value for node in tree.body if isinstance(node,ast.Assign)
                 and any(isinstance(target,ast.Name) and target.id=='REGISTRY' for target in node.targets)]
    if len(assignments)!=1:raise ValueError('archive_registry_missing_or_duplicate')
    rows=ast.literal_eval(assignments[0])
    if not isinstance(rows,(tuple,list)) or not rows:raise ValueError('archive_registry_empty')
    result={}
    for row in rows:
        if not isinstance(row,(tuple,list)) or len(row)!=2:raise ValueError('archive_registry_row_invalid')
        engine,pattern=row
        if (not safe_label(engine) or engine in result or not isinstance(pattern,str)
                or not re.fullmatch(r'[A-Za-z0-9_/-]+/[A-Za-z0-9_-]*\*[A-Za-z0-9_.-]*\.json',pattern)
                or pattern.count('*')!=1 or '..' in pattern or is_private_source(pattern)
                or is_private_source(pattern.rsplit('/',1)[0]+'/')):
            raise ValueError('archive_registry_family_invalid')
        result[engine]=pattern
    if len(set(result.values()))!=len(result):raise ValueError('archive_registry_ownership_collision')
    return result


def artifact_map(root, functions):
    manifest=json.loads((root/'engine-manifest.json').read_text())
    engines={row['engine']:row for row in manifest['engines']}
    pages=json.loads((root/'config/page-data-contracts.json').read_text()).get('pages',{})
    result={}
    for function in functions:
        row=engines.get(function,{})
        keys=row.get('keys',[])
        conventional='data/'+function.removeprefix('justhodl-')+'.json'
        chosen=PRIMARY.get(function) or ([conventional] if conventional in keys else keys if len(keys)==1 else [])
        if function=='justhodl-public-archive-index':
            chosen=['data/archive-indexes/catalog.json']+['data/archive-indexes/'+engine+'.json' for engine in public_archive_registry(root)]
        result[function]={'primary_keys':chosen,'other_source_bound_keys':[key for key in keys if key not in chosen],
                          'expected_output_contracts':{key:OUTPUT_CONTRACTS[key] for key in chosen if key in OUTPUT_CONTRACTS},
                          'unresolved_write_count':len(row.get('unresolved_writes',[])),
                          'primary_resolution':'EXPLICIT_OR_SOURCE_BOUND' if chosen else 'API_OR_PRIMARY_UNRESOLVED',
                          'pages':sorted(page for page,contract in pages.items() if function in contract.get('producers',[])),
                          'page_mapping_scope':'checked-in ownership contract; browser rendering verified separately'}
        if function=='justhodl-public-archive-index':
            result[function]['expected_output_contracts']={key:'public-engine-archive-catalog.v1' if key.endswith('/catalog.json') else 'public-engine-archive-index.v1' for key in chosen}
    return result


def strict_document(raw):
    def reject(value): raise ValueError('non_finite_json')
    doc=json.loads(raw,parse_constant=reject)
    if not isinstance(doc,dict): raise ValueError('primary_output_not_object')
    json.dumps(doc,allow_nan=False)
    return doc


def donor_checks(function, doc, key=None, root=ROOT):
    """D27-D30 semantic checks, returning only fixed codes and aggregate counts."""
    name=function.removeprefix('justhodl-');errors=[];requirements=[];counts={}
    if name=='source-map':
        if (doc.get('schema_version')!='public-source-map.v1' or doc.get('engine')!='justhodl-source-map'
                or doc != source_map_public(doc)):errors.append('PUBLIC_SOURCE_MAP_PROJECTION_INVALID')
        if doc.get('input_status')!='AVAILABLE':requirements.append('PRIVATE_ATTRIBUTION_INPUT_UNAVAILABLE')
        requirements.append('RAW_SOURCE_TEXT_AND_DIAGNOSTICS_REQUIRE_AUTHENTICATED_ACCESS')
    elif name=='fleet-error-monitor':
        if (doc.get('version')!='1.1.0' or doc.get('privacy_version')!='fleet-errors-metadata-20260909-v1'
                or doc.get('alerts_scope')!='ALL_CURRENT_ALARMS' or doc != fleet_errors_public(doc)):
            errors.append('FLEET_ERROR_PUBLIC_CONTRACT_INVALID')
        if not isinstance(doc.get('alerts'),list) or type(doc.get('n_alerts_detected')) is not int or doc.get('n_alerts_detected')!=len(doc.get('alerts',[])):
            errors.append('FLEET_ERROR_ALARM_COVERAGE_INVALID')
        if doc.get('notification_mode')=='suppressed_for_audit' and (doc.get('telegram_sent') or doc.get('sns_sent')):
            errors.append('QUIET_MONITOR_SENT_NOTIFICATION')
        if doc.get('dlq_status',{}).get('available') is not True:requirements.append('DLQ_METADATA_UNAVAILABLE')
    elif name in ('ka-metrics','khalid-metrics'):
        analysis=bool(key and key.endswith('-analysis.json'))
        if doc.get('engine')!=function or doc.get('schema_version')!=('macro-analysis.v1' if analysis else 'macro-metrics.v1'):
            errors.append('METRIC_OUTPUT_OWNER_OR_SCHEMA_INVALID')
        if analysis:
            if doc.get('input_artifact')!='data/'+name+'.json':errors.append('METRIC_ANALYSIS_INPUT_OWNER_INVALID')
            if doc.get('llm_status')!='available':requirements.append('CURRENT_MODEL_ANALYSIS_UNAVAILABLE')
    elif name=='short-interest':
        if doc.get('measurement_contract')!='short-positioning.v2': errors.append('SHORT_MEASUREMENT_CONTRACT_MISSING')
        rows=doc.get('by_ticker',{})
        if not isinstance(rows,dict): errors.append('SHORT_ROWS_INVALID');rows={}
        for row in rows.values():
            if not isinstance(row,dict): errors.append('SHORT_ROW_INVALID');continue
            if row.get('latest_short_pct')!=row.get('daily_short_volume_pct'):errors.append('DAILY_VOLUME_ALIAS_MISMATCH')
            if row.get('days_to_cover_source')=='Finviz short ratio' and row.get('days_to_cover_as_of') is not None: errors.append('UNDATED_FALLBACK_FALSE_DATE')
            if row.get('short_interest') is not None and (row.get('short_interest_source')!='FINRA consolidated short interest' or row.get('short_interest_as_of')!=row.get('settlement_date')):errors.append('SHORT_INVENTORY_PROVENANCE_INVALID')
        counts['position_rows']=sum(isinstance(r,dict) and r.get('short_interest') is not None for r in rows.values())
        if not counts['position_rows']:requirements.append('DATED_FINRA_INVENTORY_UNAVAILABLE')
    elif name=='crypto-funding':
        rows=doc.get('by_coin',{})
        if not isinstance(rows,dict):errors.append('FUNDING_ROWS_INVALID');rows={}
        for row in rows.values():
            if not isinstance(row,dict):errors.append('FUNDING_ROW_INVALID');continue
            rate=numeric(row.get('current_funding_rate'));hours=numeric(row.get('funding_interval_hours'))
            if row.get('funding_rate_units')!='fraction_per_observed_interval':errors.append('FUNDING_UNITS_INVALID')
            if hours is None or row.get('interval_source')=='unknown':
                if row.get('annualized_pct') is not None:errors.append('UNKNOWN_INTERVAL_ANNUALIZED')
                requirements.append('OBSERVED_FUNDING_INTERVAL_UNAVAILABLE')
            elif rate is None or hours<=0 or row.get('interval_source') not in ('venue_schedule','observed_settlement_interval'):errors.append('FUNDING_INTERVAL_INVALID')
            elif numeric(row.get('annualized_pct')) is None or abs(row['annualized_pct']-rate*24/hours*365*100)>.011:errors.append('FUNDING_ANNUALIZATION_MISMATCH')
            if not row.get('venue_observed_at'):requirements.append('VENUE_OBSERVATION_TIMESTAMP_UNAVAILABLE')
    elif name in ('squeeze-fuel','trade-tickets'):
        rows=doc.get('board' if name=='squeeze-fuel' else 'tickets',[])
        if 'donor_health' not in doc:errors.append('DONOR_HEALTH_MISSING')
        if not isinstance(rows,list):errors.append('RECIPIENT_ROWS_INVALID');rows=[]
        counts['rows']=len(rows)
        for row in rows:
            if row.get('execution_eligible') is not False:errors.append('RESEARCH_EXECUTION_GUARD_MISSING')
            short=row.get('short_positioning') or {}
            if not short.get('positioning_usable'):requirements.append('DATED_POSITIONING_UNAVAILABLE')
            if short.get('locate_verified') is True or short.get('borrow_fee') is not None:errors.append('PUBLIC_SI_INVENTED_BORROW_TERMS')
            if name=='squeeze-fuel':
                if row.get('pct_of_float') is not None and (row.get('float_evidence') or {}).get('status')!='DATED':errors.append('UNDATED_FLOAT_USED')
            else:
                options=row.get('options_context') or {}
                if options.get('executable_option_quote') is not False:errors.append('OPTION_EXECUTION_GUARD_MISSING')
                if not options.get('usable'):requirements.append('CURRENT_OPTION_ANALYTICS_UNAVAILABLE')
                elif numeric((options.get('record') or {}).get('atm_iv_front')) is None or numeric(options.get('atm_iv_annualized_pct')) is None:errors.append('OPTION_VOLATILITY_UNITS_MISSING')
                elif abs(options['atm_iv_annualized_pct']-options['record']['atm_iv_front']*100)>1e-6:errors.append('OPTION_IV_SCALE_MISMATCH')
    elif name=='crypto-basis':
        if 'donor_health' not in doc:errors.append('DONOR_HEALTH_MISSING')
        for coin in ('btc','eth'):
            row=doc.get(coin) or {};ctx=row.get('perpetual_funding_context') or {};comparison=row.get('funding_basis_comparison') or {}
            if row.get('execution_eligible') is not False:errors.append('CARRY_EXECUTION_GUARD_MISSING')
            if comparison.get('locked_carry') is True:errors.append('FLOATING_FUNDING_TREATED_AS_LOCKED')
            if not ctx.get('usable'):requirements.append('OBSERVED_PERPETUAL_FUNDING_UNAVAILABLE')
            else:
                source=ctx.get('record') or {};rate=numeric(source.get('current_funding_rate'));hours=numeric(source.get('funding_interval_hours'));ann=numeric(ctx.get('annualized_from_observed_interval_pct'))
                if rate is None or hours is None or hours<=0 or ann is None or abs(ann-rate*24/hours*365*100)>1e-6:errors.append('CARRY_FUNDING_INTERVAL_MISMATCH')
    elif name=='sizing-engine':
        if not all(key in doc for key in ('donor_inputs','donor_constraint_summary')):errors.append('SIZING_DONOR_CONTRACT_MISSING')
        if any(r.get('execution_eligible') is not False for r in doc.get('recommendations',[])):errors.append('SIZING_EXECUTION_GUARD_MISSING')
    elif name=='firm-risk-board':
        if not all(key in doc for key in ('data_contract_status','factor_model_detail','liquidity_detail','book_linkage')):errors.append('FIRM_RISK_CONTRACT_MISSING')
        if doc.get('allows_new_entries') is not False:errors.append('MODELED_BOOK_GRANTED_EXECUTION')
    elif name=='liquidity-profile':
        if doc.get('method')!='liquidity_profile_v2_volume_capacity':errors.append('LIQUIDITY_PROFILE_OLD_METHOD')
    elif name=='calibrator':
        for field in ('weights','accuracy_by_type','window_accuracy','window_weights','recommended_horizon','khalid_component_weights'):
            if not isinstance(doc.get(field),dict):errors.append('CALIBRATOR_REPORT_SCHEMA_INVALID')
        for field in ('total_outcomes','signal_types_tracked'):
            value=numeric(doc.get(field))
            if value is None or value < 0 or not value.is_integer():errors.append('CALIBRATOR_REPORT_COUNTS_INVALID')
        if not isinstance(doc.get('recommendations'),list):errors.append('CALIBRATOR_REPORT_SCHEMA_INVALID')
        if doc.get('snapshot_id') or doc.get('model_version') or not doc.get('generated_at'):
            errors.append('CALIBRATOR_REPORT_REPLACED_BY_MODEL_SNAPSHOT')
    elif name=='options-flow-scanner':
        if doc.get('schema_version')!=1 or doc.get('method')!='options_flow_scanner_v1' or not isinstance(doc.get('all_qualifying'),list) or not isinstance(doc.get('summary'),dict) or not isinstance(doc.get('stats'),dict):
            errors.append('OPTIONS_SCANNER_OUTPUT_OWNERSHIP_INVALID')
    elif name=='options-flow':
        data=doc.get('data')
        if doc.get('engine')!='JustHodl Options Flow & Sentiment Engine v3.0' or not isinstance(data,dict) or not all(field in data for field in ('vix_complex','put_call','gamma_exposure','trading_signals')):
            errors.append('OPTIONS_FLOW_OUTPUT_OWNERSHIP_INVALID')
    elif name=='bloomberg-v8':
        if not doc.get('utc') or not all(isinstance(doc.get(field),dict) for field in ('fred','stocks','stats','signals')) or not isinstance(doc.get('yield_curve'),list):
            errors.append('BLOOMBERG_V8_OUTPUT_OWNERSHIP_INVALID')
    elif name=='daily-report-v3':
        if doc.get('version')!='V10' or not all(field in doc for field in ('risk_dashboard','net_liquidity','liquidity_credit_engine','tenor_signals','global_business_cycle')):
            errors.append('DAILY_REPORT_V10_OUTPUT_OWNERSHIP_INVALID')
    elif name=='ecb-derived':
        if doc.get('engine')!='ecb-derived' or doc.get('version')!='3.4.0' or not isinstance(doc.get('indicators'),dict):
            errors.append('ECB_DERIVED_OUTPUT_OWNERSHIP_INVALID')
    elif name=='whats-changed' and key=='data/snapshots-index.json':
        rows=doc.get('snapshots')
        if (doc.get('schema_version')!='daily-snapshot-index.v1' or doc.get('complete') is not True
                or doc.get('coverage_scope')!='all listed source-owned public daily copies'
                or doc.get('semantics')!='MUTABLE_DAILY_COPY; filename date is not certified decision-time availability'
                or not isinstance(rows,list)):
            errors.append('DAILY_SNAPSHOT_INDEX_SCHEMA_INVALID')
        rows=rows if isinstance(rows,list) else []
        dates=[];keys=[]
        for row in rows:
            if not isinstance(row,dict):errors.append('DAILY_SNAPSHOT_ROW_INVALID');continue
            source=row.get('source_key');day=row.get('capture_date');size=numeric(row.get('size_bytes'))
            if not isinstance(source,str) or source not in DAILY_SNAPSHOT_SOURCES or is_private_source(source):
                errors.append('DAILY_SNAPSHOT_SOURCE_UNREVIEWED');continue
            try:
                valid_day=isinstance(day,str) and datetime.strptime(day,'%Y-%m-%d').date().isoformat()==day
            except ValueError:valid_day=False
            expected='data/snapshots/'+source.replace('/','_').replace('.json','')+'-'+str(day)+'.json'
            if not valid_day or row.get('key')!=expected:errors.append('DAILY_SNAPSHOT_KEY_DATE_INVALID')
            if not parse_timestamp(row.get('object_last_modified')) or size is None or size<0 or not size.is_integer():
                errors.append('DAILY_SNAPSHOT_OBJECT_METADATA_INVALID')
            if row.get('immutable') is not False or row.get('point_in_time_certified') is not False or row.get('content_status')!='LISTED_NOT_CONTENT_VALIDATED':
                errors.append('DAILY_SNAPSHOT_AVAILABILITY_OVERCLAIMED')
            if valid_day:dates.append(day)
            keys.append(row.get('key') if isinstance(row.get('key'),str) else None)
        if not isinstance(doc.get('n_snapshots'),int) or isinstance(doc.get('n_snapshots'),bool) or doc.get('n_snapshots')!=len(rows) or doc.get('dates')!=sorted(set(dates)) or len(set(keys))!=len(keys):
            errors.append('DAILY_SNAPSHOT_INDEX_COUNTS_INVALID')
        counts.update(snapshots=len(rows),dates=len(set(dates)))
        requirements.append('MUTABLE_DAILY_COPIES_NOT_POINT_IN_TIME_CERTIFIED')
    elif name=='public-archive-index':
        registry=public_archive_registry(root)
        if doc.get('publisher_engine')!='justhodl-public-archive-index':errors.append('ARCHIVE_INDEX_PUBLISHER_INVALID')
        if key=='data/archive-indexes/catalog.json':
            rows=doc.get('indexes')
            if (doc.get('schema_version')!='public-engine-archive-catalog.v1'
                    or doc.get('completeness_scope')!='reviewed family metadata listings only' or not isinstance(rows,list)):
                errors.append('ARCHIVE_CATALOG_SCHEMA_INVALID')
            rows=rows if isinstance(rows,list) else []
            engines=[]
            for row in rows:
                if not isinstance(row,dict):errors.append('ARCHIVE_CATALOG_ROW_INVALID');continue
                engine=row.get('engine')
                if not isinstance(engine,str) or engine not in registry:errors.append('ARCHIVE_CATALOG_MEMBERSHIP_INVALID');continue
                engines.append(engine)
                if row.get('key')!='data/archive-indexes/'+engine+'.json':errors.append('ARCHIVE_CATALOG_KEY_INVALID')
                if (type(row.get('complete')) is not bool or type(row.get('n_snapshots')) is not int
                        or row['n_snapshots']<0 or not parse_timestamp(row.get('index_observed_at')) or not isinstance(row.get('errors'),list)):
                    errors.append('ARCHIVE_CATALOG_ROW_INVALID')
                if row.get('complete') is False and (row.get('n_snapshots')!=0 or not row.get('errors')):errors.append('ARCHIVE_PARTIAL_LISTING_EXPOSED')
                if row.get('complete') is True and row.get('errors'):errors.append('ARCHIVE_COMPLETE_LISTING_HAS_ERRORS')
            if sorted(engines)!=sorted(registry):errors.append('ARCHIVE_CATALOG_MEMBERSHIP_INVALID')
            if type(doc.get('complete')) is not bool or doc.get('complete')!=all(isinstance(row,dict) and row.get('complete') is True for row in rows):errors.append('ARCHIVE_CATALOG_COMPLETENESS_INVALID')
            counts['families']=len(rows)
        else:
            engine=doc.get('engine');pattern=registry.get(engine) if isinstance(engine,str) else None
            rows=doc.get('snapshots')
            if (doc.get('schema_version')!='public-engine-archive-index.v1' or not pattern
                    or key!='data/archive-indexes/'+str(engine)+'.json' or doc.get('families')!=[pattern]
                    or doc.get('completeness_scope')!='paginated object listing only'
                    or doc.get('listing_is_atomic') is not False or type(doc.get('complete')) is not bool
                    or not isinstance(rows,list) or not isinstance(doc.get('errors'),list)
                    or not parse_timestamp(doc.get('index_observed_at')) or not parse_timestamp(doc.get('listing_started_at'))):
                errors.append('ARCHIVE_INDEX_SCHEMA_INVALID')
            rows=rows if isinstance(rows,list) else []
            if type(doc.get('n_snapshots')) is not int or doc.get('n_snapshots')!=len(rows) or type(doc.get('listing_pages')) is not int or doc.get('listing_pages',-1)<0:
                errors.append('ARCHIVE_INDEX_COUNTS_INVALID')
            complete=doc.get('complete') is True
            if doc.get('status')!=('INDEX_AVAILABLE' if complete else 'INDEX_UNAVAILABLE'):errors.append('ARCHIVE_INDEX_STATUS_INVALID')
            if complete and (doc.get('errors') or not doc.get('listing_pages')):errors.append('ARCHIVE_COMPLETE_LISTING_HAS_ERRORS')
            if not complete and (rows or not doc.get('errors')):errors.append('ARCHIVE_PARTIAL_LISTING_EXPOSED')
            matcher=re.compile(re.escape(pattern).replace(r'\*',r'[^/]+')) if pattern else None
            seen=set()
            for row in rows:
                if not isinstance(row,dict):errors.append('ARCHIVE_METADATA_ROW_INVALID');continue
                archive_key=row.get('key')
                if (not isinstance(archive_key,str) or not matcher or not matcher.fullmatch(archive_key)
                        or '..' in archive_key or is_private_source(archive_key) or archive_key in seen):
                    errors.append('ARCHIVE_METADATA_KEY_INVALID')
                elif isinstance(archive_key,str):seen.add(archive_key)
                if not parse_timestamp(row.get('last_modified')) or type(row.get('size_bytes')) is not int or row.get('size_bytes',-1)<0:errors.append('ARCHIVE_OBJECT_METADATA_INVALID')
                if (row.get('immutable') is not False or row.get('point_in_time_certified') is not False
                        or row.get('availability_basis')!='listed metadata only' or row.get('content_status')!='NOT_READ'):
                    errors.append('ARCHIVE_AVAILABILITY_OVERCLAIMED')
            counts['snapshots']=len(rows)
        if doc.get('complete') is not True:requirements.append('REVIEWED_ARCHIVE_LISTING_UNAVAILABLE')
        requirements.append('ARCHIVE_METADATA_DOES_NOT_CERTIFY_CONTENT_OR_POINT_IN_TIME_AVAILABILITY')
    return {'errors':sorted(set(errors)),'requirements':sorted(set(requirements)),'counts':counts}


def inspect_output(s3, function, key, code, bucket=BUCKET, now=None, not_before=None, root=ROOT):
    now=now or utcnow();result={'key':key,'status':'PENDING_OUTPUT','errors':[],'requirements':[]}
    if key in OUTPUT_CONTRACTS:
        result.update(expected_output_contract=OUTPUT_CONTRACTS[key],expected_producer=function)
    try:
        response=s3.get_object(Bucket=bucket,Key=key);raw=response['Body'].read();doc=strict_document(raw)
        modified=response.get('LastModified');modified=parse_timestamp(modified.isoformat() if isinstance(modified,datetime) else modified)
        deployment=parse_timestamp(code.get('last_modified'))
        generation_field='generated_at' if doc.get('generated_at') else 'updated_at' if doc.get('updated_at') else 'as_of' if function in ('justhodl-risk-sizer','justhodl-calibration-snapshotter','justhodl-whats-changed') else 'utc' if function=='justhodl-bloomberg-v8' else 'timestamp' if function=='justhodl-options-flow' else None
        generated=parse_timestamp(doc.get(generation_field)) if generation_field else None
        result.update(bytes=len(raw),last_modified=modified.isoformat() if modified else None,
                      generated_at=generated.isoformat() if generated else None,version_id=response.get('VersionId'),
                      schema_version=safe_label(doc.get('schema_version')),producer_version=safe_label(doc.get('version') or doc.get('audit_version')),
                      producer=safe_label(doc.get('engine')),source_code_version_marker_present=bool(doc.get('source_commit') or doc.get('code_sha256')),
                      attribution='publication timestamp after verified deployed package; no embedded source SHA asserted')
        if not deployment or not modified or modified<deployment or (now-modified).total_seconds() < -300:
            result['requirements'].append('FIRST_POST_RELEASE_PUBLICATION_PENDING');return result
        if not generated or generated < deployment-timedelta(seconds=5):
            result['requirements'].append('POST_RELEASE_GENERATION_TIMESTAMP_UNPROVEN');return result
        if not_before is not None:
            # A pre-existing post-deployment artifact cannot prove this refresh,
            # or a recomputation after a newly published upstream model. S3 time
            # may be rounded to seconds; generation must satisfy the exact bound.
            result['required_generation_not_before']=not_before.isoformat()
            if generated < not_before or modified < not_before.replace(microsecond=0):
                result['requirements'].append('POST_REFRESH_GENERATION_PENDING');return result
        if (now-generated).total_seconds()>MAX_AGE_H.get(function,72)*3600:
            result['requirements'].append('FRESH_GENERATION_PENDING');return result
        if generated and ((now-generated).total_seconds() < -300 or generated>modified.replace(microsecond=0)+timedelta(minutes=5)):
            result['errors'].append('GENERATION_TIMESTAMP_INVALID')
        if key in ACCOUNTING_KEYS:
            result['errors'].extend(inspect_payload(key,doc))
            if key=='portfolio/snapshot.json':result['requirements'].append('BROKER_RECONCILED_CAPITAL_LEDGER_REQUIRED')
            if key=='backtest/results.json':result['requirements'].append('TRADABLE_PORTFOLIO_NAV_AND_VINTAGES_REQUIRED')
        kind=function.removeprefix('justhodl-')
        if kind in RISK_KINDS and key=='data/'+kind+'.json':
            try:
                checked=check_artifact(kind,doc)
                result['risk_contract_status']=safe_label(checked.get('status'))
                if checked.get('status') in ('DATA_HOLD','BLOCKED'):result['requirements'].append('CAPITAL_OR_SOURCE_REQUIREMENTS_BLOCK_PERMISSION')
            except Exception as exc:
                result['errors'].append('RISK_CONTRACT_'+type(exc).__name__)
        checks=donor_checks(function,doc,key,root)
        result['errors'].extend(checks['errors']);result['requirements'].extend(checks['requirements']);result['counts']=checks['counts']
        if doc.get('ok') is False or doc.get('error') or doc.get('_err'):result['requirements'].append('PRODUCER_REPORTED_DATA_UNAVAILABLE')
        result['status']='CONTRACT_FAILED' if result['errors'] else 'VERIFIED_BLOCKED_REQUIREMENTS' if result['requirements'] else 'VERIFIED'
    except Exception as exc:
        result.update(status='PENDING_OUTPUT' if type(exc).__name__ in ('NoSuchKey','ClientError') else 'CONTRACT_FAILED',error_type=type(exc).__name__)
    return result


def configure_katlin_refresh(scheduler, root):
    config=release_config(root,'justhodl-katlin');primary=config['eventbridge_scheduler']
    extras=config.get('eventbridge_schedulers_extra',[])
    matches=[spec for spec in extras if spec.get('schedule_name')=='justhodl-katlin-permission-refresh']
    if len(matches)!=1:raise ValueError('refresh_schedule_spec_missing_or_duplicate')
    spec={**matches[0],'cron':matches[0].get('expression') or matches[0].get('cron'),
          'role_arn':matches[0].get('role_arn') or primary['role_arn'],'state':'ENABLED'}
    if spec['cron']!='rate(15 minutes)' or spec.get('input')!={'mode':'permission_refresh'} or spec.get('target_alias')!='live':raise ValueError('refresh_schedule_contract_invalid')
    group=spec.get('group_name','default');name=spec['schedule_name']
    try:current=scheduler.get_schedule(Name=name,GroupName=group)
    except scheduler.exceptions.ResourceNotFoundException:current={}
    weekly_spec=config.get('eventbridge_scheduler_extra') or {}
    weekly=None
    if weekly_spec.get('schedule_name'):
        try:weekly=scheduler.get_schedule(Name=weekly_spec['schedule_name'],GroupName=weekly_spec.get('group_name','default'))
        except scheduler.exceptions.ResourceNotFoundException:pass
    target=f'arn:aws:lambda:{REGION}:{ACCOUNT}:function:justhodl-katlin:live'
    payload=scheduler_payload({'eventbridge_scheduler':spec},current,target)
    if current:scheduler.update_schedule(**payload)
    else:scheduler.create_schedule(**payload)
    observed=scheduler.get_schedule(Name=name,GroupName=group)
    if observed.get('State')!='ENABLED' or observed.get('Target',{}).get('Arn')!=target or json.loads(observed.get('Target',{}).get('Input','null'))!={'mode':'permission_refresh'} or observed.get('ScheduleExpression')!='rate(15 minutes)':raise ValueError('refresh_schedule_readback_failed')
    weekly_preserved=None
    if weekly is not None:
        after=scheduler.get_schedule(Name=weekly_spec['schedule_name'],GroupName=weekly_spec.get('group_name','default'))
        fields=('ScheduleExpression','ScheduleExpressionTimezone','State','Target','FlexibleTimeWindow','KmsKeyArn','StartDate','EndDate')
        weekly_preserved=all(weekly.get(k)==after.get(k) for k in fields)
        if not weekly_preserved:raise ValueError('weekly_backtest_schedule_changed')
    return {'name':name,'group':group,'expression':observed['ScheduleExpression'],'target_alias':'live',
            'state':observed.get('State'),'input_mode':'permission_refresh','weekly_schedule_present':weekly is not None,
            'weekly_backtest_preserved':weekly_preserved,'schedule_input_bodies_reported':False}


def input_matches(actual, expected):
    try:
        return json.loads(actual) == (json.loads(expected) if isinstance(expected,str) else expected)
    except (ValueError,TypeError):return actual==expected


def privacy_receipt_summary(root, receipt):
    """Reuse ops5230 evidence; never copy mirror contents or secret values."""
    result={'verified':False,'privacy_ops':5230,'reason':'SUCCESSFUL_PRIVACY_MIGRATION_RECEIPT_REQUIRED'}
    if not isinstance(receipt,dict) or receipt.get('ops')!=5230 or receipt.get('ok') is not True:return result
    from audit_20260909_privacy_migration import READINESS, PUBLISHERS, desired_members, digest, encoded
    checks=receipt.get('checks',[])
    configured={row.get('function') for row in checks if row.get('check')=='private_publisher_config'}
    code=[row for row in checks if row.get('check')=='exact_deployed_code']
    mismatched=[]
    for short in READINESS:
        name='justhodl-'+short
        try:
            expected=digest(encoded({key:digest(value) for key,value in desired_members(root,short).items()}))
            rows=[row for row in code if row.get('function')==name]
            if not rows or any(row.get('source_sha256')!=expected for row in rows):mismatched.append(name)
        except Exception:mismatched.append(name)
    missing_config=sorted({'justhodl-'+name for name in PUBLISHERS}-configured)
    policies=[row for row in checks if row.get('check')=='bucket_policy']
    policy=policies[-1] if policies else {}
    restored=policy.get('private_deny') is True and policy.get('historical_deny') is True and policy.get('temporary_current_deny') is False
    verified=not mismatched and not missing_config and restored
    result.update(verified=verified,reason='VERIFIED' if verified else 'PRIVACY_RECEIPT_INCOMPLETE_OR_DIFFERENT_SOURCE_RELEASE',
                  expected_readiness_functions=len(READINESS),expected_private_publishers=len(PUBLISHERS),
                  source_mismatches=mismatched,missing_publisher_config=missing_config,final_policy_restored=restored,
                  receipt_checkout_sha=receipt.get('checkout_sha'),privacy_epoch=receipt.get('epoch'))
    return result


def observe_metric_rule_bindings(events, root, functions):
    """Report the source-declared KA/Khalid rule collision without rebinding it."""
    owners=('justhodl-ka-metrics','justhodl-khalid-metrics')
    declared={name:release_config(root,name).get('eventbridge_rules',[]) for name in owners}
    rules=sorted({rule for rows in declared.values() for rule in rows if isinstance(rule,str)})
    results=[]
    for rule in rules:
        claimants=[name for name in owners if rule in declared[name]]
        try:
            current=events.describe_rule(Name=rule);targets=[];token=None;seen=set()
            while True:
                response=events.list_targets_by_rule(Rule=rule,**({'NextToken':token} if token else {}))
                targets.extend(response.get('Targets',[]));token=response.get('NextToken')
                if not token:break
                if token in seen:raise ValueError('repeated_rule_target_page')
                seen.add(token)
            arns=[target['Arn'] for target in targets if isinstance(target.get('Arn'),str)
                  and re.fullmatch(r'arn:aws:lambda:[a-z0-9-]+:[0-9]+:function:[A-Za-z0-9_-]+(?::[A-Za-z0-9_$-]+)?',target['Arn'])]
            for function in claimants:
                if function not in functions:continue
                matching=[arn for arn in arns if arn.split(':function:')[1].split(':')[0]==function]
                shared=len(claimants)>1
                row={'function':function,'service':'events','name':rule,'status':'PENDING_CONFIGURATION',
                     'reason':'SHARED_RULE_OWNERSHIP_REVIEW_REQUIRED' if shared else 'TARGET_BINDING_UNPROVEN',
                     'shared_declared_functions':claimants,'observed_lambda_target_arns':arns,
                     'matching_target_arns':matching,'target_count':len(targets),
                     'expression':current.get('ScheduleExpression'),'state':current.get('State'),
                     'dedicated_cadence_verified':bool(not shared and matching and current.get('State')=='ENABLED'),
                     'mutation_requested':False,'scope':'exact source-declared EventBridge rules; Input bodies withheld'}
                if not shared and matching and current.get('State')=='ENABLED':row.update(status='VERIFIED',reason='DEDICATED_TARGET_VERIFIED')
                elif not matching:row['reason']='DEDICATED_METRIC_TARGET_MISSING'
                results.append(row)
        except Exception as exc:
            for function in claimants:
                if function in functions:results.append({'function':function,'service':'events','name':rule,
                    'status':'PENDING_CONFIGURATION','reason':'METRIC_RULE_BINDING_UNAVAILABLE',
                    'shared_declared_functions':claimants,'dedicated_cadence_verified':False,
                    'mutation_requested':False,'error_type':type(exc).__name__})
    return results


def observe_schedules(clients, root, functions):
    """Read configured schedules; report target metadata without Input bodies."""
    metric_functions=set(functions)&{'justhodl-ka-metrics','justhodl-khalid-metrics'}
    result=observe_metric_rule_bindings(clients['events'],root,metric_functions) if metric_functions else []
    for function in functions:
        if function in metric_functions:continue
        config=release_config(root,function);specs=[]
        if function in {'justhodl-liquidity-profile','justhodl-retail-sentiment'} and isinstance(config.get('schedule'),str):
            result.extend(observe_recorded_cadence(clients['events'],root,function,config['schedule']))
        for field in ('eventbridge_scheduler','eventbridge_scheduler_extra'):
            if isinstance(config.get(field),dict):specs.append(('scheduler',config[field]))
        specs.extend(('scheduler',spec) for spec in config.get('eventbridge_schedulers_extra',[]) if isinstance(spec,dict))
        if isinstance(config.get('schedule'),dict):specs.append(('events',config['schedule']))
        specs.extend(('events',{'name':name}) for name in config.get('eventbridge_rules',[]) if isinstance(name,str))
        seen=set()
        for kind,spec in specs:
            name=spec.get('schedule_name') or spec.get('name')
            if not name or (kind,name) in seen:continue
            seen.add((kind,name));row={'function':function,'service':kind,'name':name,'status':'PENDING_CONFIGURATION'}
            try:
                if kind=='scheduler':
                    current=clients['scheduler'].get_schedule(Name=name,GroupName=spec.get('group_name','default'))
                    targets=[current.get('Target',{})]
                else:
                    current=clients['events'].describe_rule(Name=name)
                    targets=clients['events'].list_targets_by_rule(Rule=name).get('Targets',[])
                expected=spec.get('cron') or spec.get('expression')
                matching=[target for target in targets if target.get('Arn','').split(':function:')[-1].split(':')[0]==function]
                governed=bool(config.get('release_validation')) or function in ('justhodl-engine-fusion','justhodl-khalid-risk')
                qualified=all(':' in target['Arn'].split(':function:')[-1] and not target['Arn'].endswith(':$LATEST') for target in matching)
                row.update(expression=current.get('ScheduleExpression'),expected_expression=expected,state=current.get('State'),
                           target_count=len(targets),matching_target_arns=[target['Arn'] for target in matching],
                           governed_target_qualified=qualified if governed else None,
                           input_configured='input' in spec,input_preserved_or_matches=all(input_matches(target.get('Input'),spec['input']) for target in matching) if 'input' in spec else None)
                if matching and current.get('State')=='ENABLED' and (not expected or expected==current.get('ScheduleExpression')) and (not governed or qualified) and row['input_preserved_or_matches'] is not False:row['status']='VERIFIED'
            except Exception as exc:row['error_type']=type(exc).__name__
            result.append(row)
    return result


def observe_recorded_cadence(events, root, function, configured_cadence):
    """Read back recorded identities; a cadence-only declaration changes no rule."""
    base={'function':function,'service':'events','status':'OBSERVED_CADENCE_ONLY',
          'configured_cadence':configured_cadence,'mutation_requested':False,
          'identity_reference':'config/schedule-manifest.json',
          'discovery_scope':'source-recorded rule identities only; not live fleet enumeration'}
    rows=[]
    try:
        manifest=json.loads((root/'config/schedule-manifest.json').read_text())
        recorded=[rule for rule in manifest.get('rules',[]) if rule.get('kind')=='events'
                  and any(target.get('arn','').split(':function:')[-1].split(':')[0]==function for target in rule.get('targets',[]))]
        for rule in recorded:
            row={**base,'name':rule['name'],'identity_reference_generated_at':manifest.get('generated_at')}
            try:
                current=events.describe_rule(Name=rule['name']);targets=[];token=None;seen=set()
                while True:
                    response=events.list_targets_by_rule(Rule=rule['name'],**({'NextToken':token} if token else {}))
                    targets.extend(response.get('Targets',[]));token=response.get('NextToken')
                    if not token:break
                    if token in seen:raise ValueError('repeated_rule_target_page')
                    seen.add(token)
                matching=[target['Arn'] for target in targets if target.get('Arn','').split(':function:')[-1].split(':')[0]==function]
                row.update(observation_status='VERIFIED_IDENTITY' if matching else 'UNPROVEN_IDENTITY',
                           expression=current.get('ScheduleExpression'),state=current.get('State'),
                           cadence_matches=current.get('ScheduleExpression')==configured_cadence,
                           matching_target_arns=matching,target_count=len(targets))
            except Exception as exc:
                row.update(observation_status='UNPROVEN_IDENTITY',error_type=type(exc).__name__)
            rows.append(row)
        if not rows:rows.append({**base,'name':None,'observation_status':'NO_SOURCE_RECORDED_RULE'})
    except Exception as exc:
        rows.append({**base,'name':None,'observation_status':'SOURCE_REFERENCE_UNAVAILABLE','error_type':type(exc).__name__})
    return rows


def observe_function_urls(lam, root):
    """Prove source-reviewed identities; unknown URL owners remain explicit."""
    rows=[]
    for page,function,expected_host in FUNCTION_URL_BINDINGS:
        row={'page':page,'function':function,'page_hostname':expected_host,
             'status':'PENDING_IDENTITY','scope':'URL identity only; response contract not invoked'}
        try:
            text=(root/page).read_text()
            hosts={urlsplit(url).hostname for url in re.findall(r'https://[^\s\'"<>]+',text)}
            if expected_host not in hosts:
                row['reason']='PAGE_URL_CHANGED_SINCE_REVIEW'
            else:
                config=lam.get_function_url_config(FunctionName=function)
                actual=urlsplit(config.get('FunctionUrl',''))
                row.update(deployed_hostname=actual.hostname,auth_type=safe_label(config.get('AuthType')))
                if actual.scheme=='https' and actual.hostname==expected_host:
                    row['status']='VERIFIED'
                else:row['reason']='FUNCTION_URL_HOSTNAME_MISMATCH'
        except Exception as exc:
            row.update(reason='FUNCTION_URL_IDENTITY_UNPROVEN',error_type=type(exc).__name__)
        rows.append(row)
    for page,host in UNRESOLVED_FUNCTION_URLS:
        rows.append({'page':page,'function':None,'page_hostname':host,'status':'PENDING_IDENTITY',
                     'reason':'FUNCTION_URL_OWNER_UNRESOLVED_AFTER_SCHEMA_MISMATCH',
                     'rejected_candidate':'fedliquidityapi','scope':'metadata owner discovery required; no guessed endpoint invocation'})
    return rows


class ReleaseVerifier:
    def __init__(self, root, clients, *, parity_seconds=1800, publication_seconds=2100,
                 sleeper=time.sleep, clock=time.monotonic, package_check=check_packages, privacy_receipt=None):
        self.root=root;self.clients=clients;self.parity_seconds=min(parity_seconds,1800)
        self.publication_seconds=min(publication_seconds,2100);self.sleep=sleeper;self.clock=clock;self.package_check=package_check
        self.privacy_receipt=privacy_receipt
        self.requested_after={}
        self.report={'ops':5231,'ok':False,'status':'INITIALIZING','code':{},'outputs':{},'invocations':[],
                     'requirements':[],'private_payloads_reported':0,'raw_lambda_responses_reported':0}

    def checkpoint(self):
        path=self.root/'aws/ops/reports/5231_audit_release_verify.json';path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text(json.dumps(self.report,indent=2,allow_nan=False)+'\n')

    def parity(self, functions):
        deadline=self.clock()+self.parity_seconds;pending=set(functions)
        while pending:
            with ThreadPoolExecutor(max_workers=6) as pool:
                rows=list(pool.map(lambda name:self.package_check(self.clients['lambda'],self.root,[name])[0],sorted(pending)))
            for row in rows:self.report['code'][row['function']]=row
            pending={name for name in pending if not self.report['code'][name].get('pass')}
            self.report['source_parity_pending']=sorted(pending);self.checkpoint()
            print(json.dumps({'ops':5231,'phase':'package_parity','matched':len(functions)-len(pending),'pending':len(pending)}),flush=True)
            if not pending:return True
            if self.clock()>=deadline:return False
            self.sleep(min(30,max(0,deadline-self.clock())))
        return True

    def inspect_function(self, name):
        outputs=self.report['artifact_scope'][name]['primary_keys'];code=self.report['code'][name]
        floors=[self.requested_after[name]] if name in self.requested_after else []
        for dependency in REFRESH_DEPENDENCIES.get(name,()):
            for row in self.report['outputs'].get(dependency,[]):
                stamp=parse_timestamp(row.get('last_modified'))
                if stamp and row['status'].startswith('VERIFIED'):floors.append(stamp)
        rows=[inspect_output(self.clients['s3'],name,key,code,not_before=max(floors) if floors else None,root=self.root) for key in outputs]
        self.report['outputs'][name]=rows
        return bool(rows) and all(row['status'].startswith('VERIFIED') for row in rows)

    def invoke_once(self, name):
        if name not in QUIET_FUNCTIONS:raise ValueError('unreviewed_manual_invocation')
        payload=APPROVED_REFRESH_MODES.get(name,{})
        source_files=(self.root/'aws/lambdas'/name/'source').rglob('*.py')
        if not payload and any(re.search(r'api\.telegram\.org|sendMessage|send_telegram|send_email|\.invoke\(',path.read_text(errors='replace')) for path in source_files):
            self.report['requirements'].append({'function':name,'reason':'NOTIFICATION_OR_CASCADE_PATH_REQUIRES_NORMAL_SCHEDULE'})
            return False
        # A fresh package check immediately precedes every invocation. Pin the
        # verified numbered version where the governed alias provides one.
        code=self.package_check(self.clients['lambda'],self.root,[name])[0]
        self.report['code'][name]=code
        if not code.get('pass'):raise ValueError('code_changed_before_invocation')
        args={'FunctionName':name,'InvocationType':'Event','Payload':json.dumps(payload,separators=(',',':')).encode()}
        if code.get('qualifier')=='live':args['Qualifier']=code['version']
        requested_at=utcnow()
        response=self.clients['lambda'].invoke(**args)
        self.report['invocations'].append({'function':name,'qualifier':args.get('Qualifier','$LATEST'),
            'request_status':response.get('StatusCode'),'requested_at':requested_at.isoformat(),'mode':payload.get('mode','regular_quiet_publish')})
        if response.get('StatusCode')!=202:raise ValueError('async_invocation_not_accepted')
        self.requested_after[name]=requested_at
        self.checkpoint();return True

    def run(self):
        self.report.update(checkout_sha=git(self.root,'rev-parse','HEAD'),baseline_sha=BASE,started_at=utcnow().isoformat())
        scope=changed_scope(self.root);self.report['engine_scope']=scope
        self.report['artifact_scope']=artifact_map(self.root,scope)
        self.report['status']='WAITING_FOR_SOURCE_PARITY'
        if not self.parity(scope):
            self.report['status']='SOURCE_PARITY_FAILED';self.checkpoint();return self.report
        # No schedule or invocation mutation has happened before this gate.
        self.report['status']='SOURCE_PARITY_VERIFIED';self.checkpoint()
        self.report['privacy_prerequisite']=privacy_receipt_summary(self.root,self.privacy_receipt)
        if not self.report['privacy_prerequisite']['verified']:
            self.report['status']='PRIVACY_PREREQUISITE_BLOCKED';self.checkpoint();return self.report
        katlin=self.package_check(self.clients['lambda'],self.root,['justhodl-katlin'])[0]
        if not katlin.get('pass'):raise ValueError('katlin_changed_before_schedule_update')
        self.report['katlin_refresh_schedule']=configure_katlin_refresh(self.clients['scheduler'],self.root)
        deadline=self.clock()+self.publication_seconds
        for stage in QUIET_STAGES:
            stage_timeout=max(release_config(self.root,'justhodl-'+short).get('timeout',300) for short in stage)+120
            stage_deadline=min(deadline,self.clock()+stage_timeout)
            waiting=[]
            for short in stage:
                name='justhodl-'+short
                if self.inspect_function(name):continue
                if self.clock()>=deadline:break
                if self.invoke_once(name):waiting.append(name)
            while waiting and self.clock()<stage_deadline:
                waiting=[name for name in waiting if not self.inspect_function(name) and not any(row['status']=='CONTRACT_FAILED' for row in self.report['outputs'][name])]
                self.checkpoint()
                if waiting:
                    print(json.dumps({'ops':5231,'phase':'publication','stage':list(stage),'pending':len(waiting)}),flush=True)
                    self.sleep(min(20,max(0,stage_deadline-self.clock())))
            # Later engines may safely publish explicit DATA_HOLD if an upstream
            # source remains unavailable; no donor failure is recast as neutral.
        for name in scope:self.inspect_function(name)
        self.report['schedules']=observe_schedules(self.clients,self.root,scope)
        self.report['function_urls']=observe_function_urls(self.clients['lambda'],self.root)
        self.report['schedule_discovery_scope']='declarative config schedules; no claim that unconfigured fleet rules were exhaustively discovered'
        pending=[];failed=[];blocked=[]
        pending.extend({'function':row['function'],'reason':'SCHEDULE_CONFIGURATION_PENDING','schedule':row['name']} for row in self.report['schedules'] if row['status'] not in ('VERIFIED','OBSERVED_CADENCE_ONLY'))
        pending.extend({'function':row['function'],'page':row['page'],'reason':row.get('reason','FUNCTION_URL_IDENTITY_UNPROVEN')} for row in self.report['function_urls'] if row['status']!='VERIFIED')
        if self.report['katlin_refresh_schedule'].get('weekly_schedule_present') is False:
            pending.append({'function':'justhodl-katlin','reason':'WEEKLY_BACKTEST_SCHEDULE_MISSING'})
        for name,rows in self.report['outputs'].items():
            if not rows:pending.append({'function':name,'reason':'API_REQUEST_FIXTURE_OR_PRIMARY_OUTPUT_MAPPING_REQUIRED'})
            for row in rows:
                if row['status']=='PENDING_OUTPUT':pending.append({'function':name,'key':row['key'],'reason':'FIRST_SCHEDULED_RUN_PENDING' if name not in QUIET_FUNCTIONS else 'POST_RELEASE_OUTPUT_PENDING'})
                elif row['status']=='CONTRACT_FAILED':failed.append({'function':name,'key':row['key'],'errors':row.get('errors',[]),'error_type':row.get('error_type')})
                elif row['requirements']:blocked.append({'function':name,'key':row['key'],'requirements':row['requirements']})
        self.report.update(pending_outputs=pending,contract_failures=failed,blocked_requirements=blocked,
                           finished_at=utcnow().isoformat(),ok=not pending and not failed,
                           status='CONTRACT_FAILURE' if failed else 'PENDING_SCHEDULED_OUTPUTS' if pending else 'VERIFIED_WITH_BLOCKED_REQUIREMENTS' if blocked else 'VERIFIED')
        self.checkpoint();return self.report
