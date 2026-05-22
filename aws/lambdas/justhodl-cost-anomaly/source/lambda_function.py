"""
justhodl-cost-anomaly — AWS + Anthropic Cost Anomaly Detection
================================================================

What it does
============
Daily watchdog against runaway compute / API burn. Hedge funds run this
because one bad loop deploy or a recursive Claude call can burn $1000s
before anyone notices.

Components:
  1. AWS Cost Explorer  — last 30d daily spend by service
                          → detect anomalies (>25% above 7d MA)
                          → project month-end spend vs prior month
  2. Per-Lambda invocations — CloudWatch metrics for every Lambda
                          → flag any function invoking >3× its 7d baseline
                          → catches recursive bug deploys
  3. Anthropic API spend — best-effort from Anthropic billing endpoint
                          → tracks daily tokens, projects month-end
  4. Top spenders surface — top 10 services + top 10 Lambdas by spend
  5. Telegram alert       — only when an anomaly fires (no daily spam)

Output: data/cost-anomaly.json + data/cost-anomaly/history/<date>.json

Schedule: cron(0 9 * * ? *)  — daily 09:00 UTC (after AWS cost data
                                refreshes at midnight UTC)
"""
import os, json, time, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

VERSION = "1.0.0"
REGION = os.environ.get('AWS_REGION', 'us-east-1')
BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
OUT_KEY = "data/cost-anomaly.json"
HIST_PREFIX = "data/cost-anomaly/history/"

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
ANOMALY_THRESHOLD_PCT = float(os.environ.get('ANOMALY_THRESHOLD_PCT', '25'))
LAMBDA_INVOCATION_MULTIPLIER = float(os.environ.get('LAMBDA_INV_MULT', '3.0'))
MONTHLY_BUDGET_USD = float(os.environ.get('MONTHLY_BUDGET_USD', '300'))

ce = boto3.client('ce', region_name='us-east-1')  # cost explorer is global, lives in us-east-1
cw = boto3.client('cloudwatch', region_name=REGION)
lam = boto3.client('lambda', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)


# ============================================================================
# 1. AWS Cost Explorer — last 30 days, daily, by service
# ============================================================================
def fetch_aws_spend():
    """Pull 30 days of cost data + project month-end."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=30)
    
    try:
        resp = ce.get_cost_and_usage(
            TimePeriod={'Start': start.isoformat(), 'End': end.isoformat()},
            Granularity='DAILY',
            Metrics=['UnblendedCost'],
            GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}],
        )
    except Exception as e:
        return {'error': f"Cost Explorer access denied or err: {str(e)[:200]}"}
    
    # Reshape: {date: {service: cost}}
    by_date = {}
    services_total = {}
    for day in resp.get('ResultsByTime', []):
        d = day['TimePeriod']['Start']
        by_date[d] = {}
        for g in day.get('Groups', []):
            svc = g['Keys'][0]
            cost = float(g['Metrics']['UnblendedCost']['Amount'])
            by_date[d][svc] = cost
            services_total[svc] = services_total.get(svc, 0) + cost
    
    # Daily totals
    daily_totals = [(d, sum(svc_costs.values())) for d, svc_costs in sorted(by_date.items())]
    
    # 7-day MA + anomaly detection (last day vs 7d avg of prior days)
    anomalies = []
    if len(daily_totals) >= 8:
        last_date, last_cost = daily_totals[-1]
        prior_7 = [c for _, c in daily_totals[-8:-1]]
        avg_7d = sum(prior_7) / len(prior_7) if prior_7 else 0
        pct_diff = ((last_cost - avg_7d) / avg_7d * 100) if avg_7d > 0 else 0
        if abs(pct_diff) > ANOMALY_THRESHOLD_PCT:
            anomalies.append({
                'type': 'daily_total',
                'date': last_date,
                'cost_usd': round(last_cost, 2),
                'avg_7d': round(avg_7d, 2),
                'pct_diff': round(pct_diff, 1),
                'direction': 'SPIKE' if pct_diff > 0 else 'DROP',
            })
        
        # Per-service anomalies
        for svc in services_total:
            last_svc = by_date.get(last_date, {}).get(svc, 0)
            prior_svc = []
            for d, costs in sorted(by_date.items()):
                if d != last_date and svc in costs:
                    prior_svc.append(costs[svc])
            prior_svc = prior_svc[-7:]
            avg_svc = sum(prior_svc) / len(prior_svc) if prior_svc else 0
            if avg_svc > 0.5:  # only flag services costing >$0.50/day on average
                pct = ((last_svc - avg_svc) / avg_svc * 100) if avg_svc > 0 else 0
                if abs(pct) > 50:  # higher threshold per service
                    anomalies.append({
                        'type': 'service',
                        'service': svc,
                        'date': last_date,
                        'cost_usd': round(last_svc, 2),
                        'avg_7d': round(avg_svc, 2),
                        'pct_diff': round(pct, 1),
                    })
    
    # Month-to-date projection
    today = datetime.now(timezone.utc).date()
    mtd_start = today.replace(day=1)
    mtd_days = (today - mtd_start).days + 1
    days_in_month = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    days_in_month = days_in_month.day
    
    mtd_total = sum(c for d, c in daily_totals if d >= mtd_start.isoformat())
    projected_month_end = (mtd_total / mtd_days) * days_in_month if mtd_days > 0 else 0
    
    # Top spenders
    top_services = sorted(services_total.items(), key=lambda x: x[1], reverse=True)[:10]
    
    return {
        'daily_totals_last30': [(d, round(c, 2)) for d, c in daily_totals[-30:]],
        'last_day': daily_totals[-1] if daily_totals else None,
        'mtd_usd': round(mtd_total, 2),
        'projected_month_end_usd': round(projected_month_end, 2),
        'projected_vs_budget_pct': round(projected_month_end / MONTHLY_BUDGET_USD * 100, 1),
        'top_services': [{'service': s, 'cost_30d_usd': round(c, 2)} for s, c in top_services],
        'anomalies': anomalies,
    }


# ============================================================================
# 2. Per-Lambda invocation anomaly detection
# ============================================================================
def fetch_one_lambda_metrics(fn_name):
    """24h invocations vs 7d daily avg."""
    end = datetime.now(timezone.utc)
    start_24h = end - timedelta(hours=24)
    start_7d = end - timedelta(days=8)
    end_7d = end - timedelta(days=1)
    
    try:
        # 24h invocations
        r24 = cw.get_metric_statistics(
            Namespace='AWS/Lambda', MetricName='Invocations',
            Dimensions=[{'Name': 'FunctionName', 'Value': fn_name}],
            StartTime=start_24h, EndTime=end, Period=86400, Statistics=['Sum'],
        )
        inv_24h = sum(p['Sum'] for p in r24.get('Datapoints', []))
        
        # 7d daily avg (excluding last 24h)
        r7 = cw.get_metric_statistics(
            Namespace='AWS/Lambda', MetricName='Invocations',
            Dimensions=[{'Name': 'FunctionName', 'Value': fn_name}],
            StartTime=start_7d, EndTime=end_7d, Period=86400, Statistics=['Sum'],
        )
        total_7d = sum(p['Sum'] for p in r7.get('Datapoints', []))
        avg_daily_7d = total_7d / 7 if total_7d > 0 else 0
        
        return {
            'fn': fn_name,
            'inv_24h': int(inv_24h),
            'avg_daily_7d': round(avg_daily_7d, 1),
            'multiplier': round(inv_24h / avg_daily_7d, 2) if avg_daily_7d > 0 else None,
        }
    except Exception as e:
        return {'fn': fn_name, 'error': str(e)[:100]}


def detect_lambda_invocation_anomalies():
    """List Lambdas + scan for invocation spikes."""
    all_fns = []
    for page in lam.get_paginator('list_functions').paginate():
        all_fns.extend([fn['FunctionName'] for fn in page['Functions']])
    
    results = []
    with ThreadPoolExecutor(max_workers=15) as ex:
        for f in as_completed([ex.submit(fetch_one_lambda_metrics, fn) for fn in all_fns]):
            results.append(f.result())
    
    # Filter to anomalies
    anomalies = []
    high_invokers = []
    for r in results:
        if r.get('error'):
            continue
        mult = r.get('multiplier')
        if mult is None or r.get('avg_daily_7d', 0) < 5:
            # ignore low-volume functions where small ratios are noise
            continue
        if mult >= LAMBDA_INVOCATION_MULTIPLIER:
            anomalies.append(r)
        if r.get('inv_24h', 0) > 100:
            high_invokers.append(r)
    
    high_invokers.sort(key=lambda r: r.get('inv_24h', 0), reverse=True)
    anomalies.sort(key=lambda r: r.get('multiplier', 0), reverse=True)
    
    return {
        'n_scanned': len(results),
        'anomalies': anomalies[:10],
        'top_10_by_invocations_24h': high_invokers[:10],
    }


# ============================================================================
# 3. Anthropic spend — best-effort
# ============================================================================
def fetch_anthropic_spend():
    """Anthropic doesn't have a public billing API (yet). Track our token use
    via internal accounting: read each Lambda's invocation count × estimated
    Claude tokens. This is approximate but useful for trend."""
    # Conservative: count invocations of known Claude-using Lambdas
    claude_users = [
        'justhodl-morning-intelligence', 'justhodl-ai-chat',
        'justhodl-bloomberg-v8', 'justhodl-investor-agents',
        'justhodl-premortem-engine', 'justhodl-chart-vision',
        'justhodl-earnings-nlp', 'justhodl-meta-improver',
        'justhodl-coffee-can', 'justhodl-bagger-engine',
    ]
    
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=7)
    total_invocations_7d = 0
    breakdown = []
    for fn in claude_users:
        try:
            r = cw.get_metric_statistics(
                Namespace='AWS/Lambda', MetricName='Invocations',
                Dimensions=[{'Name': 'FunctionName', 'Value': fn}],
                StartTime=start, EndTime=end, Period=86400 * 7, Statistics=['Sum'],
            )
            inv = sum(p['Sum'] for p in r.get('Datapoints', []))
            total_invocations_7d += inv
            breakdown.append({'fn': fn, 'invocations_7d': int(inv)})
        except Exception:
            continue
    
    # Rough estimate: avg Claude call ~5K tokens, $0.005 per 1K tokens for Haiku
    est_tokens_7d = total_invocations_7d * 5000
    est_cost_7d_usd = est_tokens_7d / 1000 * 0.005
    
    return {
        'claude_invocations_7d': int(total_invocations_7d),
        'est_tokens_7d': int(est_tokens_7d),
        'est_cost_7d_usd': round(est_cost_7d_usd, 2),
        'est_cost_monthly_usd': round(est_cost_7d_usd * 30 / 7, 2),
        'breakdown': sorted(breakdown, key=lambda x: x['invocations_7d'], reverse=True)[:10],
        'note': 'Estimate based on avg 5k tokens per Claude-using Lambda invocation @ $0.005/1k Haiku rate. Actual depends on prompt size.',
    }


# ============================================================================
# Telegram
# ============================================================================
def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            'chat_id': TELEGRAM_CHAT_ID, 'text': msg[:4000],
            'parse_mode': 'Markdown',
        }).encode()
        urllib.request.urlopen(urllib.request.Request(url, data=data, method='POST'), timeout=10)
        return True
    except Exception:
        return False


def build_alert(payload):
    aws = payload['aws_spend']
    lams = payload['lambda_invocations']
    anth = payload['anthropic_spend']
    
    lines = [f"*💰 COST ANOMALY DETECTOR*"]
    lines.append(f"MTD: ${aws.get('mtd_usd', 0)} · Projected: ${aws.get('projected_month_end_usd', 0)} "
                 f"({aws.get('projected_vs_budget_pct', 0)}% of ${MONTHLY_BUDGET_USD} budget)")
    
    if aws.get('anomalies'):
        lines.append(f"\n*🔴 AWS spend anomalies ({len(aws['anomalies'])})*")
        for a in aws['anomalies'][:5]:
            if a['type'] == 'daily_total':
                lines.append(f"  • Daily total {a['direction']}: ${a['cost_usd']} vs ${a['avg_7d']} avg ({a['pct_diff']:+.1f}%)")
            else:
                lines.append(f"  • {a['service']}: ${a['cost_usd']} vs ${a['avg_7d']} avg ({a['pct_diff']:+.1f}%)")
    
    if lams.get('anomalies'):
        lines.append(f"\n*🔴 Lambda invocation spikes ({len(lams['anomalies'])})*")
        for l in lams['anomalies'][:5]:
            lines.append(f"  • `{l['fn']}`: {l['inv_24h']} inv/24h ({l['multiplier']}× baseline {l['avg_daily_7d']})")
    
    lines.append(f"\nClaude est: {anth.get('claude_invocations_7d', 0)} inv/7d, "
                 f"~${anth.get('est_cost_monthly_usd', 0)}/mo")
    
    return "\n".join(lines)


# ============================================================================
# Main
# ============================================================================
def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[cost-anomaly] v{VERSION} starting")
    
    aws_spend = fetch_aws_spend()
    lambda_inv = detect_lambda_invocation_anomalies()
    anthropic = fetch_anthropic_spend()
    
    payload = {
        'version': VERSION,
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'aws_spend': aws_spend,
        'lambda_invocations': lambda_inv,
        'anthropic_spend': anthropic,
        'monthly_budget_usd': MONTHLY_BUDGET_USD,
        'elapsed_s': round(time.time() - started, 1),
    }
    
    # Determine if we should alert
    should_alert = False
    n_anomalies = 0
    if aws_spend.get('anomalies'):
        n_anomalies += len(aws_spend['anomalies'])
        should_alert = True
    if lambda_inv.get('anomalies'):
        n_anomalies += len(lambda_inv['anomalies'])
        should_alert = True
    if aws_spend.get('projected_vs_budget_pct', 0) > 110:
        should_alert = True
    
    # Write outputs
    s3.put_object(
        Bucket=BUCKET, Key=OUT_KEY,
        Body=json.dumps(payload, default=str, indent=2).encode(),
        ContentType='application/json',
        CacheControl='max-age=3600, public',
    )
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    s3.put_object(
        Bucket=BUCKET, Key=f"{HIST_PREFIX}{today}.json",
        Body=json.dumps(payload, default=str).encode(),
        ContentType='application/json',
    )
    
    # Alert
    if should_alert:
        try:
            send_telegram(build_alert(payload))
        except Exception as e:
            print(f"[telegram] {e}")
    
    print(f"[cost-anomaly] done in {payload['elapsed_s']}s · "
          f"MTD=${aws_spend.get('mtd_usd', 0)} · "
          f"anomalies={n_anomalies}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'ok': True,
            'mtd_usd': aws_spend.get('mtd_usd'),
            'projected_month_end_usd': aws_spend.get('projected_month_end_usd'),
            'aws_anomalies': len(aws_spend.get('anomalies', [])),
            'lambda_anomalies': len(lambda_inv.get('anomalies', [])),
            'elapsed_s': payload['elapsed_s'],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
