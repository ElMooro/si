"""
justhodl-engine-contribution
============================
COUNTERFACTUAL ENGINE CONTRIBUTION TRACKER

Closes the loop the learning system literally cannot. The learning system
(signal-logger / outcome-checker / calibrator) tracks per-signal accuracy.
Accuracy ≠ portfolio contribution.

A 60%-accurate engine firing on 5% of names contributes a fraction of what
a 55%-accurate engine firing on 30% of names produces — but the calibrator
gives them similar weights. This engine fixes that by measuring REAL PnL
impact: replay history with each engine turned OFF, measure portfolio PnL
delta. That delta is the engine's marginal contribution.

METHODOLOGY:
-----------
For each engine in justhodl-signals over the lookback window (90/365D):
  1. Pull ALL signals + outcomes from DDB justhodl-signals + justhodl-outcomes
  2. Build the actual realized portfolio by walking signals forward:
     - At each signal fire, simulated_position += position_size_pct * direction
     - At outcome resolution, mark PnL = position * actual_return_pct
  3. Counterfactual: rebuild portfolio with engine X excluded
     - Sum of PnL with engine X: actual_pnl
     - Sum of PnL without engine X: counterfactual_pnl
     - Marginal contribution = actual_pnl - counterfactual_pnl
  4. Repeat for every engine → ranked list of contributions

OUTPUTS:
  data/engine-contributions.json
    {
      "engines": [
        {"engine": "...", "marginal_pnl_bps": ..., "sharpe_contribution": ...,
         "n_signals": ..., "hit_rate": ..., "verdict": "ALPHA|NEUTRAL|DRAG"},
        ...
      ],
      "leader": {...}, "laggard": {...},
      "total_portfolio_bps": ..., "alpha_engine_share": ...
    }

  data/engine-contributions/history/<date>.json — weekly snapshots

SCHEDULE: weekly Sunday 02:00 UTC
"""
import os
import json
import time
import math
import urllib.request
import urllib.parse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from statistics import mean, stdev

import boto3
from boto3.dynamodb.conditions import Attr

VERSION = "1.0.0"
REGION = os.environ.get('AWS_REGION', 'us-east-1')
BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
OUT_KEY = os.environ.get('OUT_KEY', 'data/engine-contributions.json')
HIST_PREFIX = os.environ.get('HIST_PREFIX', 'data/engine-contributions/history/')

SIGNALS_TABLE = os.environ.get('SIGNALS_TABLE', 'justhodl-signals')
OUTCOMES_TABLE = os.environ.get('OUTCOMES_TABLE', 'justhodl-outcomes')
LOOKBACK_DAYS = int(os.environ.get('LOOKBACK_DAYS', '180'))
POSITION_SIZE_BPS = float(os.environ.get('POSITION_SIZE_BPS', '50'))  # 50 bps per signal
OUTCOME_WINDOWS = os.environ.get('OUTCOME_WINDOWS', '7,30').split(',')  # day_7, day_30

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

s3 = boto3.client('s3', region_name=REGION)
ddb = boto3.resource('dynamodb', region_name=REGION)


def decimal_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [decimal_to_float(x) for x in obj]
    return obj


def scan_table(table_name, filter_expr=None, max_items=50000):
    """Scan a DDB table fully (paginated)."""
    table = ddb.Table(table_name)
    items = []
    kwargs = {}
    if filter_expr is not None:
        kwargs['FilterExpression'] = filter_expr
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get('Items', []))
        if 'LastEvaluatedKey' not in resp or len(items) >= max_items:
            break
        kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']
    return items


def fetch_signals(cutoff_iso):
    """Pull signals fired after the cutoff."""
    print(f"[signals] scanning {SIGNALS_TABLE} for >= {cutoff_iso}...")
    items = scan_table(SIGNALS_TABLE)
    items = [decimal_to_float(it) for it in items]
    # Keep only those with a logged_at >= cutoff
    filtered = []
    for s in items:
        logged = s.get('logged_at') or s.get('fired_at') or s.get('ts')
        if not logged:
            continue
        if str(logged) >= cutoff_iso:
            filtered.append(s)
    print(f"[signals] {len(filtered)} signals in window (of {len(items)} total)")
    return filtered


def fetch_outcomes():
    """Pull all outcomes."""
    print(f"[outcomes] scanning {OUTCOMES_TABLE}...")
    items = scan_table(OUTCOMES_TABLE)
    items = [decimal_to_float(it) for it in items]
    # Index by (signal_id, window_key) -> outcome
    by_sigwin = {}
    for o in items:
        sig_id = o.get('signal_id') or o.get('outcome_id', '').rsplit('_day_', 1)[0]
        win = o.get('window_key', '')
        if sig_id and win:
            by_sigwin[(sig_id, win)] = o
    print(f"[outcomes] {len(by_sigwin)} (signal,window) outcome pairs")
    return by_sigwin


def signal_pnl(signal, outcomes, windows):
    """Compute realized PnL for a signal across requested windows.
    Returns list of dicts: [{window: 'day_7', return_pct, pnl_bps}]"""
    sig_id = signal.get('signal_id', '')
    direction = 1
    # Try to determine direction from signal payload
    pred = signal.get('predicted_dir') or signal.get('direction') or signal.get('signal_dir')
    if pred:
        pred_str = str(pred).upper()
        if pred_str in ('SHORT', 'BEARISH', 'SELL', 'DOWN', '-1'):
            direction = -1
    
    pnl_records = []
    for w in windows:
        w_key = f'day_{w}' if not w.startswith('day_') else w
        out = outcomes.get((sig_id, w_key))
        if not out:
            continue
        # Extract return — outcome payload structure varies
        ret_pct = None
        if 'outcome' in out and isinstance(out['outcome'], dict):
            ret_pct = out['outcome'].get('return_pct')
        elif 'return_pct' in out:
            ret_pct = out['return_pct']
        if ret_pct is None:
            continue
        try:
            ret = float(ret_pct)
        except (ValueError, TypeError):
            continue
        # Signed return in the predicted direction
        signed_ret = ret * direction
        # PnL = position_size_bps × signed_return_pct
        pnl_bps = POSITION_SIZE_BPS * signed_ret  # 50bps × X% return = 50*X bps PnL
        pnl_records.append({
            'window': w_key,
            'return_pct': round(signed_ret, 3),
            'pnl_bps': round(pnl_bps, 4),
            'correct': signed_ret > 0,
        })
    return pnl_records


def compute_engine_contributions(signals, outcomes, windows):
    """For each engine, compute total PnL contribution across the universe of signals."""
    # Group signals by engine
    engines = defaultdict(list)
    for s in signals:
        engine = s.get('signal_type') or s.get('engine') or s.get('source')
        if engine:
            engines[engine].append(s)
    
    print(f"[contribution] {len(engines)} distinct engines fired in window")
    
    # For each engine, compute realized PnL
    engine_stats = {}
    for engine, sigs in engines.items():
        pnls_by_window = defaultdict(list)
        n_signals_total = len(sigs)
        for sig in sigs:
            for rec in signal_pnl(sig, outcomes, windows):
                pnls_by_window[rec['window']].append(rec)
        
        # Per-window stats
        window_results = {}
        total_pnl_bps = 0
        total_resolved = 0
        for w, recs in pnls_by_window.items():
            if not recs:
                continue
            pnl_list = [r['pnl_bps'] for r in recs]
            total_pnl_bps += sum(pnl_list)
            total_resolved += len(pnl_list)
            sharpe = None
            if len(pnl_list) > 5:
                m = mean(pnl_list); sd = stdev(pnl_list)
                sharpe = round(m / sd * math.sqrt(252), 2) if sd > 0 else None
            window_results[w] = {
                'n_resolved': len(recs),
                'total_pnl_bps': round(sum(pnl_list), 2),
                'mean_pnl_bps': round(mean(pnl_list), 3),
                'hit_rate_pct': round(sum(1 for r in recs if r['correct']) / len(recs) * 100, 1),
                'sharpe_annualized': sharpe,
            }
        
        engine_stats[engine] = {
            'engine': engine,
            'n_signals_total': n_signals_total,
            'n_signals_resolved': total_resolved,
            'resolve_rate_pct': round(total_resolved / n_signals_total * 100, 1) if n_signals_total > 0 else 0,
            'total_marginal_pnl_bps': round(total_pnl_bps, 2),
            'per_window': window_results,
        }
    
    return engine_stats


def classify_engine(eng):
    """Categorize each engine's contribution."""
    pnl = eng.get('total_marginal_pnl_bps', 0)
    n = eng.get('n_signals_resolved', 0)
    
    if n < 5:
        return 'INSUFFICIENT_DATA'
    if pnl > 100:
        return 'ALPHA'           # > 1% portfolio contribution
    if pnl > 25:
        return 'POSITIVE'        # mild positive
    if pnl > -25:
        return 'NEUTRAL'         # noise
    if pnl > -100:
        return 'DRAG'            # mild negative
    return 'TOXIC'                # > -1% portfolio contribution


def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            'chat_id': TELEGRAM_CHAT_ID, 'text': msg[:4000],
            'parse_mode': 'Markdown', 'disable_web_page_preview': 'true',
        }).encode()
        req = urllib.request.Request(url, data=data, method='POST')
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        print(f"[telegram] {e}")
        return False


def build_digest(engines, total_bps):
    sorted_engines = sorted(engines, key=lambda e: e.get('total_marginal_pnl_bps', 0), reverse=True)
    top = sorted_engines[:5]
    bottom = [e for e in sorted_engines if e.get('verdict') in ('DRAG', 'TOXIC')][:5]
    if not bottom:
        bottom = sorted_engines[-5:][::-1]  # last 5 reversed
    
    lines = [
        f"*🎯 ENGINE CONTRIBUTIONS — {LOOKBACK_DAYS}D LOOKBACK*",
        f"_{len(engines)} engines · portfolio total {total_bps:+.1f} bps_\n",
        "*🟢 TOP ALPHA CONTRIBUTORS*"
    ]
    for e in top:
        lines.append(f"  `{e['engine']:<24}` {e['total_marginal_pnl_bps']:+7.1f} bps  ({e['n_signals_resolved']} sig, {e['verdict']})")
    
    lines.append("\n*🔴 DRAG / TOXIC ENGINES*")
    for e in bottom:
        lines.append(f"  `{e['engine']:<24}` {e['total_marginal_pnl_bps']:+7.1f} bps  ({e['n_signals_resolved']} sig, {e['verdict']})")
    
    lines.append(f"\n_Sized at {POSITION_SIZE_BPS} bps per signal. Verdict tiers: ALPHA>100bps, POSITIVE>25, NEUTRAL, DRAG<-25, TOXIC<-100_")
    return "\n".join(lines)


def lambda_handler(event=None, context=None):
    started = time.time()
    run_ts = datetime.now(timezone.utc)
    print(f"[engine-contribution] v{VERSION} starting")
    
    cutoff = run_ts - timedelta(days=LOOKBACK_DAYS)
    cutoff_iso = cutoff.isoformat()
    
    # Pull data
    signals = fetch_signals(cutoff_iso)
    outcomes = fetch_outcomes()
    
    if not signals:
        # Heartbeat even on no-action
        payload = {
            'version': VERSION,
            'generated_at': run_ts.isoformat(),
            'status': 'no_action',
            'reason': 'no_signals_in_lookback_window',
            'lookback_days': LOOKBACK_DAYS,
            'engines': [],
        }
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                      Body=json.dumps(payload, default=str, indent=2).encode(),
                      ContentType='application/json')
        return {'statusCode': 200, 'body': json.dumps({'ok': True, 'no_action': 'no_signals'})}
    
    # Compute contributions
    contributions = compute_engine_contributions(signals, outcomes, OUTCOME_WINDOWS)
    
    # Classify each engine
    engine_list = []
    for eng_name, stats in contributions.items():
        stats['verdict'] = classify_engine(stats)
        engine_list.append(stats)
    
    # Sort by total contribution descending
    engine_list.sort(key=lambda e: e.get('total_marginal_pnl_bps', 0), reverse=True)
    
    total_bps = sum(e.get('total_marginal_pnl_bps', 0) for e in engine_list)
    
    # Alpha share — fraction of total positive PnL from "ALPHA" verdict engines
    alpha_pnl = sum(e.get('total_marginal_pnl_bps', 0) for e in engine_list if e.get('verdict') == 'ALPHA')
    positive_pnl = sum(e.get('total_marginal_pnl_bps', 0) for e in engine_list if e.get('total_marginal_pnl_bps', 0) > 0)
    alpha_share = round(alpha_pnl / positive_pnl * 100, 1) if positive_pnl > 0 else None
    
    payload = {
        'version': VERSION,
        'generated_at': run_ts.isoformat(),
        'elapsed_s': round(time.time() - started, 2),
        'lookback_days': LOOKBACK_DAYS,
        'position_size_bps': POSITION_SIZE_BPS,
        'outcome_windows': OUTCOME_WINDOWS,
        'n_engines_evaluated': len(engine_list),
        'n_signals_in_window': len(signals),
        'n_signals_resolved': sum(e.get('n_signals_resolved', 0) for e in engine_list),
        'total_portfolio_pnl_bps': round(total_bps, 2),
        'alpha_engine_share_pct': alpha_share,
        'leader': engine_list[0] if engine_list else None,
        'laggard': engine_list[-1] if engine_list else None,
        'engines': engine_list,
        'verdict_counts': {
            v: sum(1 for e in engine_list if e.get('verdict') == v)
            for v in ['ALPHA', 'POSITIVE', 'NEUTRAL', 'DRAG', 'TOXIC', 'INSUFFICIENT_DATA']
        },
        'methodology': {
            'position_sizing': f'{POSITION_SIZE_BPS} bps per signal (uniform)',
            'pnl_definition': 'position_size × signed_return_pct in predicted direction',
            'windows': OUTCOME_WINDOWS,
            'verdict_thresholds': {
                'ALPHA': '> +100 bps',
                'POSITIVE': '> +25 bps',
                'NEUTRAL': '+/-25 bps',
                'DRAG': '> -100 bps',
                'TOXIC': '< -100 bps',
            },
            'note': 'This is a SIMPLIFIED counterfactual — assumes uniform position sizing. Production fund version would use actual position-sized PnL weighted by signal confidence.',
        },
    }
    
    # Persist
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, default=str, indent=2).encode(),
                  ContentType='application/json',
                  CacheControl='max-age=3600, public')
    
    # Daily snapshot
    snap_key = f"{HIST_PREFIX}{run_ts.date().isoformat()}.json"
    s3.put_object(Bucket=BUCKET, Key=snap_key,
                  Body=json.dumps(payload, default=str).encode(),
                  ContentType='application/json')
    
    # Telegram digest
    try:
        sent = send_telegram(build_digest(engine_list, total_bps))
    except Exception as e:
        sent = False
        print(f"[telegram] {e}")
    
    print(f"[engine-contribution] done — {len(engine_list)} engines, total {total_bps:+.1f} bps")
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
        'body': json.dumps({
            'ok': True,
            'n_engines': len(engine_list),
            'total_pnl_bps': round(total_bps, 1),
            'leader': engine_list[0]['engine'] if engine_list else None,
            'laggard': engine_list[-1]['engine'] if engine_list else None,
            'verdict_counts': payload['verdict_counts'],
            'telegram_sent': sent,
            'elapsed_s': round(time.time() - started, 2),
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
