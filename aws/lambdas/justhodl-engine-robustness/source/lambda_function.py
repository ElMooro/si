"""
justhodl-engine-robustness
==========================
ENGINE HEALTH CT-SCAN — Adversarial Robustness via Empirical Stability

WHAT
====
For every signal-generating engine in the platform, compute its empirical
rank stability across historical snapshots. The methodology used by quant
risk teams at Renaissance / Two Sigma / AQR — measure how much an engine's
output rankings flip day-over-day for inputs that are similar.

WHY EXPONENTIAL
===============
You have 33+ retail edges + N firm engines. After 90 days of running, you
should know which are real edge vs which are overfit to recent data. Without
this engine, every "decaying" verdict from signal-halflife is ambiguous:
  - Is the engine REALLY decaying (alpha exhausted)?
  - Or is it FRAGILE (always was overfit, just happened to look good for a while)?

Robustness CT-scan answers this. An engine with low stability + decaying
signal-halflife = OVERFIT (kill it). High stability + decaying = ARBED AWAY
(retire gracefully). High stability + fresh edge = KEEP RUNNING. Low
stability + fresh = SUSPECT, INSPECT.

METHODOLOGY
===========
For each tracked engine (auto-discovered from data/history/* prefixes):

  1. Pull last N=30 historical snapshots
  2. From each snapshot, extract the ranked output list:
     - {ticker, score} pairs OR {ticker, signal} pairs OR
       ranked list of names from a 'top_N' field
  3. For each consecutive pair (T, T-1), compute Spearman rank correlation
     on the intersection of names
  4. Average correlation = stability score [-1, +1]
  5. Compute "input churn proxy" — % of names in/out of top 20 day-over-day.
     Engines whose universe shifts wildly day-over-day are CONFOUNDED,
     not necessarily fragile.

CLASSIFICATION
==============
  ROBUST     stability > 0.85  + universe_churn < 30%
  STABLE     stability 0.65–0.85
  VOLATILE   stability 0.40–0.65  (could be by design — e.g., short-horizon)
  FRAGILE    stability < 0.40  + universe_churn < 30%  (likely overfit)
  CHAOTIC    universe_churn > 70%  (whole universe rotating — not measurable)

OUTPUT
======
data/engine-robustness.json — per-engine classifications + drill-down
data/engine-robustness/history/<date>.json — weekly snapshots

ALERTS
======
Telegram digest on:
  - Engines newly classified FRAGILE (regression alert)
  - Engines newly classified ROBUST (graduation alert)

SCHEDULE
========
Weekly Tuesday 04:00 UTC — gives meta-improver fresh signal Wednesday.
"""
import os, json, time, math, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import boto3

VERSION = "1.0.0"
REGION = os.environ.get('AWS_REGION', 'us-east-1')
BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
OUT_KEY = "data/engine-robustness.json"
HIST_PREFIX = "data/engine-robustness/history/"

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

SNAPSHOTS_PER_ENGINE = int(os.environ.get('SNAPSHOTS_PER_ENGINE', '30'))
MIN_SNAPSHOTS = int(os.environ.get('MIN_SNAPSHOTS', '10'))
TOP_N_FOR_RANK = int(os.environ.get('TOP_N_FOR_RANK', '20'))

s3 = boto3.client('s3', region_name=REGION)


# ============================================================================
# Engine discovery — auto-find anything that produces history snapshots
# ============================================================================
def discover_engines():
    """
    Walk data/history/* prefixes. Each prefix that contains JSON files is
    a candidate engine. Returns {engine_name: prefix_uri}.
    """
    engines = {}
    paginator = s3.get_paginator('list_objects_v2')
    
    # First level under data/history/
    try:
        resp = s3.list_objects_v2(
            Bucket=BUCKET, Prefix='data/history/', Delimiter='/'
        )
        for cp in resp.get('CommonPrefixes', []):
            prefix = cp['Prefix']
            # Skip if not engine snapshot pattern (need at least 5 JSON files)
            files = []
            for p2 in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
                for obj in p2.get('Contents', []):
                    if obj['Key'].endswith('.json') or obj['Key'].endswith('.jsonl'):
                        files.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'],
                        })
            if len(files) >= MIN_SNAPSHOTS:
                # Engine name = prefix sans data/history/ and trailing slash
                name = prefix.replace('data/history/', '').rstrip('/')
                engines[name] = {'prefix': prefix, 'files': sorted(files, key=lambda f: f['last_modified'], reverse=True)}
    except Exception as e:
        print(f"[discover] error: {e}")
    
    # Also check for engines that write history elsewhere (e.g. data/snapshots/<engine>/)
    try:
        resp = s3.list_objects_v2(
            Bucket=BUCKET, Prefix='data/snapshots/', Delimiter='/'
        )
        for cp in resp.get('CommonPrefixes', []):
            prefix = cp['Prefix']
            files = []
            for p2 in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
                for obj in p2.get('Contents', []):
                    if obj['Key'].endswith('.json'):
                        files.append({
                            'key': obj['Key'], 'size': obj['Size'],
                            'last_modified': obj['LastModified'],
                        })
            if len(files) >= MIN_SNAPSHOTS:
                name = 'snapshots-' + prefix.replace('data/snapshots/', '').rstrip('/')
                engines[name] = {'prefix': prefix, 'files': sorted(files, key=lambda f: f['last_modified'], reverse=True)}
    except Exception as e:
        print(f"[discover snapshots] error: {e}")
    
    return engines


# ============================================================================
# Snapshot ranking extraction — handle the schema diversity
# ============================================================================
def extract_ranking(obj_body):
    """
    From a JSON snapshot, extract a ranked list of {name, score} for the
    top N entities. Engines use many schemas — try common ones.
    """
    try:
        d = json.loads(obj_body)
    except Exception:
        return None
    
    if not isinstance(d, dict):
        return None
    
    # Common patterns:
    # 1. "rankings": [{symbol/ticker/name, score/rank}, ...]
    # 2. "top_names": [{symbol, score}, ...]
    # 3. "results": [{ticker, score}, ...]
    # 4. "signals": [...]  / "discoveries": [...]
    # 5. nested under asset_classes.{X}.assets[]
    # 6. {symbol: {score: ...}}  dict form
    
    candidate_keys = [
        'rankings', 'top_names', 'results', 'signals',
        'discoveries', 'leaders', 'top_picks', 'baggers',
        'ranked', 'flagged', 'theses', 'positions', 'scored',
        'top_assets', 'global_top_carry_to_vol', 'cross_asset_ranking',
    ]
    
    for k in candidate_keys:
        v = d.get(k)
        if isinstance(v, list) and v:
            # Find name + score fields
            ranked = []
            for item in v[:TOP_N_FOR_RANK]:
                if not isinstance(item, dict):
                    continue
                name = (item.get('symbol') or item.get('ticker') or
                        item.get('name') or item.get('asset') or
                        item.get('engine') or item.get('id') or
                        item.get('key') or item.get('coin'))
                if not name:
                    continue
                score = (item.get('score') or item.get('rank') or
                         item.get('carry_to_vol') or item.get('signal') or
                         item.get('edge_score') or item.get('composite_score') or
                         item.get('asymmetric_score'))
                ranked.append({'name': str(name), 'score': score})
            if ranked:
                return ranked
    
    # Try dict-of-objects form (e.g. {AAPL: {score: 0.5}, ...})
    obj_form = []
    for k, v in d.items():
        if isinstance(v, dict) and ('score' in v or 'edge_score' in v or 'signal' in v):
            obj_form.append({
                'name': k,
                'score': v.get('score') or v.get('edge_score') or v.get('signal'),
            })
    if len(obj_form) >= 5:
        obj_form.sort(key=lambda x: (x['score'] or 0), reverse=True)
        return obj_form[:TOP_N_FOR_RANK]
    
    return None


def spearman_rank_corr(ranking_a, ranking_b):
    """
    Spearman rank correlation on the intersection of names.
    Returns correlation in [-1, +1], or None if too little overlap.
    """
    if not ranking_a or not ranking_b:
        return None
    
    # Build rank-by-name dicts
    rank_a = {item['name']: idx for idx, item in enumerate(ranking_a)}
    rank_b = {item['name']: idx for idx, item in enumerate(ranking_b)}
    
    common = set(rank_a.keys()) & set(rank_b.keys())
    if len(common) < 3:
        return None
    
    # Compute spearman
    n = len(common)
    sum_d2 = 0
    for name in common:
        d = rank_a[name] - rank_b[name]
        sum_d2 += d * d
    return 1.0 - (6.0 * sum_d2) / (n * (n * n - 1))


def universe_churn(rankings):
    """
    % of names that fall out of top-N between consecutive snapshots,
    averaged across the series.
    """
    if len(rankings) < 2:
        return None
    
    churns = []
    for i in range(1, len(rankings)):
        a_names = set([r['name'] for r in rankings[i-1][:TOP_N_FOR_RANK]])
        b_names = set([r['name'] for r in rankings[i][:TOP_N_FOR_RANK]])
        if not a_names:
            continue
        churn = len(a_names - b_names) / len(a_names)
        churns.append(churn)
    return (sum(churns) / len(churns)) if churns else None


# ============================================================================
# Engine analysis
# ============================================================================
def analyze_engine(engine_name, engine_meta):
    """
    Pull last N snapshots, compute Spearman + universe churn.
    """
    files = engine_meta['files'][:SNAPSHOTS_PER_ENGINE]
    if len(files) < MIN_SNAPSHOTS:
        return {
            'engine': engine_name,
            'classification': 'INSUFFICIENT_HISTORY',
            'n_snapshots': len(files),
        }
    
    # Pull rankings from each snapshot
    rankings = []
    for f in files:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=f['key'])
            body = obj['Body'].read().decode()
            r = extract_ranking(body)
            if r:
                rankings.append(r)
        except Exception as e:
            print(f"[analyze {engine_name}] {f['key']}: {e}")
            continue
    
    if len(rankings) < MIN_SNAPSHOTS // 2:
        return {
            'engine': engine_name,
            'classification': 'UNREADABLE',
            'n_snapshots_pulled': len(rankings),
            'n_with_extractable_rankings': len(rankings),
        }
    
    # Spearman correlations between consecutive snapshots
    correlations = []
    for i in range(1, len(rankings)):
        c = spearman_rank_corr(rankings[i-1], rankings[i])
        if c is not None:
            correlations.append(c)
    
    if not correlations:
        return {
            'engine': engine_name,
            'classification': 'NO_OVERLAP',
            'n_snapshots': len(rankings),
        }
    
    # Stability metrics
    mean_corr = sum(correlations) / len(correlations)
    sorted_corr = sorted(correlations)
    median_corr = sorted_corr[len(sorted_corr) // 2]
    min_corr = min(correlations)
    
    churn = universe_churn(rankings)
    
    # Classify
    if churn is not None and churn > 0.70:
        classification = 'CHAOTIC'
    elif mean_corr > 0.85 and (churn or 0) < 0.30:
        classification = 'ROBUST'
    elif mean_corr > 0.65:
        classification = 'STABLE'
    elif mean_corr > 0.40:
        classification = 'VOLATILE'
    else:
        classification = 'FRAGILE'
    
    return {
        'engine': engine_name,
        'classification': classification,
        'mean_rank_correlation': round(mean_corr, 3),
        'median_rank_correlation': round(median_corr, 3),
        'min_rank_correlation': round(min_corr, 3),
        'universe_churn': round(churn, 3) if churn is not None else None,
        'n_snapshots_analyzed': len(rankings),
        'n_correlations': len(correlations),
        'avg_universe_size': round(sum(len(r) for r in rankings) / len(rankings), 1),
        'verdict_explanation': verdict_explanation(classification, mean_corr, churn),
    }


def verdict_explanation(classification, mean_corr, churn):
    if classification == 'ROBUST':
        return f"Day-over-day rank stability {mean_corr:.2f} (>0.85) + low churn ({(churn or 0):.0%}) — engine produces consistent outputs, real edge."
    elif classification == 'STABLE':
        return f"Moderate stability {mean_corr:.2f} — some sensitivity, acceptable for medium-horizon engines."
    elif classification == 'VOLATILE':
        return f"Low stability {mean_corr:.2f} — outputs shift significantly day-over-day. Could be by design for short-horizon engines but inspect."
    elif classification == 'FRAGILE':
        return f"Very low stability {mean_corr:.2f} — outputs flip dramatically. Likely overfit to recent noise. Candidate for retirement or redesign."
    elif classification == 'CHAOTIC':
        return f"Universe churning {(churn or 0):.0%} — top-N rotates too quickly to measure stability reliably. Engine universe may be too broad."
    return classification


# ============================================================================
# Compare to last week for regression / graduation alerts
# ============================================================================
def diff_vs_last(current, last):
    """Identify newly-FRAGILE and newly-ROBUST engines."""
    if not last:
        return {'regressions': [], 'graduations': []}
    
    last_by_engine = {e['engine']: e for e in last.get('engines', [])}
    regressions = []
    graduations = []
    
    for cur in current:
        prev = last_by_engine.get(cur['engine'])
        if not prev:
            continue
        prev_cls = prev.get('classification')
        cur_cls = cur.get('classification')
        if prev_cls == cur_cls:
            continue
        # Regression: ROBUST/STABLE → FRAGILE/CHAOTIC
        if prev_cls in ('ROBUST', 'STABLE') and cur_cls in ('FRAGILE', 'CHAOTIC'):
            regressions.append({
                'engine': cur['engine'],
                'was': prev_cls, 'now': cur_cls,
                'corr_was': prev.get('mean_rank_correlation'),
                'corr_now': cur.get('mean_rank_correlation'),
            })
        # Graduation: FRAGILE/VOLATILE → ROBUST/STABLE
        if prev_cls in ('FRAGILE', 'VOLATILE') and cur_cls in ('ROBUST', 'STABLE'):
            graduations.append({
                'engine': cur['engine'],
                'was': prev_cls, 'now': cur_cls,
                'corr_was': prev.get('mean_rank_correlation'),
                'corr_now': cur.get('mean_rank_correlation'),
            })
    
    return {'regressions': regressions, 'graduations': graduations}


# ============================================================================
# Telegram
# ============================================================================
def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            'chat_id': TELEGRAM_CHAT_ID,
            'text': msg[:4000],
            'parse_mode': 'Markdown',
            'disable_web_page_preview': 'true',
        }).encode()
        urllib.request.urlopen(urllib.request.Request(url, data=data, method='POST'), timeout=10)
        return True
    except Exception as e:
        print(f"[telegram] {e}")
        return False


def build_digest(report, diff):
    by_class = defaultdict(list)
    for e in report['engines']:
        by_class[e['classification']].append(e)
    
    lines = [f"*🔬 ENGINE HEALTH CT-SCAN* — {len(report['engines'])} engines analyzed"]
    counts = []
    for cls in ('ROBUST', 'STABLE', 'VOLATILE', 'FRAGILE', 'CHAOTIC',
                'INSUFFICIENT_HISTORY', 'UNREADABLE'):
        n = len(by_class.get(cls, []))
        if n:
            counts.append(f"{cls}={n}")
    lines.append("  " + "  ".join(counts))
    
    if diff.get('regressions'):
        lines.append(f"\n*🔴 NEW FRAGILE ({len(diff['regressions'])})*")
        for r in diff['regressions'][:5]:
            lines.append(f"  • `{r['engine']}`: {r['was']} → {r['now']} "
                         f"(corr {r['corr_was']:.2f} → {r['corr_now']:.2f})")
    
    if diff.get('graduations'):
        lines.append(f"\n*🟢 NEW ROBUST ({len(diff['graduations'])})*")
        for g in diff['graduations'][:5]:
            lines.append(f"  • `{g['engine']}`: {g['was']} → {g['now']} "
                         f"(corr {g['corr_was']:.2f} → {g['corr_now']:.2f})")
    
    if by_class.get('FRAGILE'):
        lines.append(f"\n*Top FRAGILE engines (kill candidates)*")
        sorted_frag = sorted(by_class['FRAGILE'],
                             key=lambda e: e.get('mean_rank_correlation', 0))
        for e in sorted_frag[:5]:
            lines.append(f"  • `{e['engine']}`: corr={e['mean_rank_correlation']}")
    
    return "\n".join(lines)


# ============================================================================
# Main handler
# ============================================================================
def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[engine-robustness] v{VERSION} starting")
    
    # 1. Discover engines
    engines = discover_engines()
    print(f"[engine-robustness] discovered {len(engines)} candidate engines")
    
    if not engines:
        return {'statusCode': 200, 'body': json.dumps({
            'ok': True, 'no_action': 'no_engines_with_sufficient_history',
            'min_snapshots_required': MIN_SNAPSHOTS,
        })}
    
    # 2. Analyze each in parallel
    results = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(analyze_engine, name, meta): name
                   for name, meta in engines.items()}
        for fut in as_completed(futures):
            try:
                r = fut.result()
                results.append(r)
                cls = r.get('classification')
                if cls not in ('INSUFFICIENT_HISTORY', 'UNREADABLE', 'NO_OVERLAP'):
                    print(f"  {cls:18s} {r['engine']:40s} corr={r.get('mean_rank_correlation','?')}")
            except Exception as e:
                print(f"[analyze err] {e}")
    
    # 3. Load previous run for diff
    try:
        prev_obj = s3.get_object(Bucket=BUCKET, Key=OUT_KEY)
        prev = json.loads(prev_obj['Body'].read().decode())
    except Exception:
        prev = None
    
    # 4. Compute diff
    diff = diff_vs_last(results, prev)
    
    # 5. Build report
    report = {
        'version': VERSION,
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'n_engines_discovered': len(engines),
        'n_engines_analyzed': len(results),
        'classification_counts': dict(defaultdict(int, {
            cls: sum(1 for r in results if r.get('classification') == cls)
            for cls in ('ROBUST', 'STABLE', 'VOLATILE', 'FRAGILE', 'CHAOTIC',
                        'INSUFFICIENT_HISTORY', 'UNREADABLE', 'NO_OVERLAP')
        })),
        'engines': sorted(results, key=lambda r: r.get('mean_rank_correlation', -2), reverse=True),
        'diff_vs_last_run': diff,
        'elapsed_s': round(time.time() - started, 2),
        'thresholds': {
            'top_n_for_rank': TOP_N_FOR_RANK,
            'snapshots_per_engine': SNAPSHOTS_PER_ENGINE,
            'min_snapshots': MIN_SNAPSHOTS,
        },
    }
    
    # 6. Write S3
    s3.put_object(
        Bucket=BUCKET, Key=OUT_KEY,
        Body=json.dumps(report, default=str, indent=2).encode(),
        ContentType='application/json',
        CacheControl='max-age=3600, public',
    )
    # Daily history snapshot
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    s3.put_object(
        Bucket=BUCKET, Key=f"{HIST_PREFIX}{today}.json",
        Body=json.dumps(report, default=str).encode(),
        ContentType='application/json',
        CacheControl='max-age=86400, public',
    )
    
    # 7. Telegram digest if regressions or graduations
    if diff.get('regressions') or diff.get('graduations') or report['classification_counts'].get('FRAGILE', 0) > 0:
        try:
            send_telegram(build_digest(report, diff))
        except Exception as e:
            print(f"[telegram] {e}")
    
    print(f"[engine-robustness] done · {report['elapsed_s']}s · "
          f"{report['classification_counts']}")
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
        'body': json.dumps({
            'ok': True,
            'n_engines': len(results),
            'counts': report['classification_counts'],
            'n_regressions': len(diff.get('regressions', [])),
            'n_graduations': len(diff.get('graduations', [])),
            'elapsed': report['elapsed_s'],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
