"""
justhodl-macro-calendar — Economic Events Calendar with Portfolio Sensitivity
==============================================================================

Pulls upcoming high-impact macro events (FOMC + BLS/BEA/Census releases via
FMP /stable/economic-calendar), computes 20d realized vol context, attaches
portfolio sensitivity from data/portfolio.json. Telegram T-1 alert for
high-impact events.

Output: data/macro-calendar.json
Schedule: cron(0 11 * * ? *)  — daily 11 UTC
"""
import os, json, time, urllib.request, urllib.parse, math
from datetime import datetime, timezone, timedelta
import boto3

VERSION = "1.0.0"
REGION = os.environ.get('AWS_REGION', 'us-east-1')
BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
OUT_KEY = "data/macro-calendar.json"
FMP_KEY = os.environ.get('FMP_KEY', '')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

s3 = boto3.client('s3', region_name=REGION)

# Hardcoded FOMC 2026 schedule (always verify against
# https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm)
FOMC_2026 = [
    ('2026-01-28', 'FOMC + SEP + Dot Plot'),
    ('2026-03-18', 'FOMC'),
    ('2026-04-29', 'FOMC + SEP'),
    ('2026-06-17', 'FOMC'),
    ('2026-07-29', 'FOMC + SEP'),
    ('2026-09-16', 'FOMC'),
    ('2026-10-28', 'FOMC + SEP'),
    ('2026-12-16', 'FOMC + SEP'),
]

HIGH_IMPACT_TERMS = [
    'CPI', 'Nonfarm Payroll', 'NFP', 'Payroll', 'GDP', 'PCE', 'PPI',
    'JOLT', 'Retail Sales', 'ISM', 'PMI', 'FOMC', 'Unemployment Rate',
    'Consumer Confidence', 'Industrial Production', 'Powell', 'Fed Chair',
    'Initial Jobless', 'Continuing Claims',
]


def http_get_json(url, timeout=15):
    req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/MacroCal'})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


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


def get_fomc_upcoming(days_horizon=30):
    """FOMC dates with days-until in horizon."""
    now = datetime.now(timezone.utc).date()
    out = []
    for date_str, label in FOMC_2026:
        d = datetime.strptime(date_str, '%Y-%m-%d').date()
        days_until = (d - now).days
        if 0 <= days_until <= days_horizon:
            out.append({
                'date': f"{date_str}T18:00:00Z",
                'event': label,
                'currency': 'USD',
                'impact': 'HIGH',
                'days_until': days_until,
                'type': 'FOMC',
                'source': 'fed_calendar',
            })
    return out


def get_economic_calendar(days_horizon=21):
    """FMP /stable/economic-calendar filtered to US high-impact."""
    if not FMP_KEY:
        return []
    today = datetime.now(timezone.utc).date()
    end = today + timedelta(days=days_horizon)
    url = (f"https://financialmodelingprep.com/stable/economic-calendar"
           f"?from={today.isoformat()}&to={end.isoformat()}&apikey={FMP_KEY}")
    try:
        events = http_get_json(url, timeout=15)
        if not isinstance(events, list):
            return []
        out = []
        for e in events:
            country = e.get('country', '')
            name = e.get('event', '')
            if country not in ('US', 'United States'):
                continue
            if not any(t.lower() in name.lower() for t in HIGH_IMPACT_TERMS):
                continue
            try:
                dt = datetime.fromisoformat(e['date'].replace('Z', '+00:00'))
            except Exception:
                continue
            days_until = (dt.date() - today).days
            if days_until < 0 or days_until > days_horizon:
                continue
            out.append({
                'date': e.get('date'),
                'event': name,
                'currency': e.get('currency', 'USD'),
                'impact': e.get('impact', 'High'),
                'previous': e.get('previous'),
                'estimate': e.get('estimate'),
                'actual': e.get('actual'),
                'days_until': days_until,
                'type': 'macro_release',
                'source': 'fmp',
            })
        return out
    except Exception as e:
        print(f"[economic-cal] {e}")
        return []


def get_market_context():
    """SPY 20d realized vol from FMP for current regime context."""
    if not FMP_KEY:
        return None
    try:
        url = f"https://financialmodelingprep.com/stable/historical-price-eod/light?symbol=SPY&apikey={FMP_KEY}"
        data = http_get_json(url, timeout=10)
        if isinstance(data, dict):
            data = data.get('historical', [])
        if not isinstance(data, list) or len(data) < 21:
            return None
        prices = []
        for d in data[:25]:
            p = d.get('price') or d.get('close') or d.get('adjClose')
            if p is not None:
                prices.append(float(p))
        if len(prices) < 21:
            return None
        rets = [math.log(prices[i] / prices[i+1]) for i in range(20)]
        m = sum(rets) / len(rets)
        v = sum((r - m)**2 for r in rets) / max(len(rets)-1, 1)
        rv = math.sqrt(v * 252) * 100
        return {
            'spy_last': round(prices[0], 2),
            'spy_5d_return_pct': round((prices[0] / prices[5] - 1) * 100, 2) if len(prices) > 5 else None,
            'realized_vol_20d_pct': round(rv, 2),
            'regime': 'LOW_VOL' if rv < 12 else 'NORMAL' if rv < 20 else 'HIGH_VOL' if rv < 30 else 'STRESS',
        }
    except Exception as e:
        print(f"[market-ctx] {e}")
        return None


def get_portfolio_sensitivity():
    """Read portfolio.json, compute equity exposure + risk hint."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key='data/portfolio.json')
        raw = json.loads(obj['Body'].read())
    except Exception:
        return None
    positions = raw if isinstance(raw, list) else raw.get('positions', [])
    if not positions:
        return None
    rows = [p for p in positions if isinstance(p, dict)]
    total_nav = sum(float(p.get('market_value') or p.get('mv') or p.get('value') or 0) for p in rows)
    if total_nav == 0:
        return None
    EQUITY_TICKERS = {'SPY','QQQ','IWM','DIA','VOO','IVV','XLK','XLF','XLE','XLV','XLY','XLI','XLP','XLU','XLB','XLRE','SMH'}
    equity_exposure = sum(
        float(p.get('market_value') or p.get('mv') or p.get('value') or 0)
        for p in rows
        if (p.get('asset_class','') or '').lower() in ('equity','stock','etf')
        or (p.get('symbol') or '').upper() in EQUITY_TICKERS
    )
    pct = equity_exposure / total_nav * 100
    return {
        'n_positions': len(rows),
        'total_nav': round(total_nav, 2),
        'equity_exposure_pct': round(pct, 1),
        'risk_assessment': 'HIGH' if pct > 80 else 'MODERATE' if pct > 40 else 'LOW',
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[macro-calendar] v{VERSION} starting")
    
    fomc = get_fomc_upcoming()
    econ = get_economic_calendar()
    market_ctx = get_market_context()
    portfolio_sens = get_portfolio_sensitivity()
    
    # Dedupe FOMC (FMP may list it too)
    seen = {f"{e['date'][:10]}:{e['event'][:25].upper()}" for e in fomc}
    all_events = list(fomc)
    for e in econ:
        key = f"{e['date'][:10]}:{e['event'][:25].upper()}"
        if 'FOMC' in e['event'].upper() and any('FOMC' in s for s in seen):
            continue
        if key in seen:
            continue
        all_events.append(e)
        seen.add(key)
    all_events.sort(key=lambda e: e.get('days_until', 999))
    
    tomorrow_hi = [e for e in all_events
                   if e.get('days_until') in (0, 1)
                   and str(e.get('impact','')).upper() in ('HIGH', 'HIGH_IMPACT')]
    next_7d_hi = [e for e in all_events
                  if e.get('days_until', 999) <= 7
                  and str(e.get('impact','')).upper() in ('HIGH', 'HIGH_IMPACT')]
    
    payload = {
        'version': VERSION,
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'horizon_days': 30,
        'n_total_events': len(all_events),
        'n_tomorrow_high_impact': len(tomorrow_hi),
        'n_next_7d_high_impact': len(next_7d_hi),
        'tomorrow_high_impact': tomorrow_hi,
        'next_7d_high_impact': next_7d_hi,
        'all_events': all_events,
        'market_context': market_ctx,
        'portfolio_sensitivity': portfolio_sens,
        'elapsed_s': round(time.time() - started, 1),
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, default=str, indent=2).encode(),
                  ContentType='application/json',
                  CacheControl='max-age=3600')
    
    if tomorrow_hi:
        lines = [f"*📅 TOMORROW: high-impact macro*"]
        for e in tomorrow_hi:
            lines.append(f"\n• *{e['event']}* ({e.get('date','')[:10]})")
            if e.get('estimate') is not None:
                lines.append(f"  est: {e.get('estimate')}  prev: {e.get('previous')}")
        if portfolio_sens:
            lines.append(f"\nPortfolio: {portfolio_sens['equity_exposure_pct']}% equity ({portfolio_sens['risk_assessment']})")
        if market_ctx:
            lines.append(f"SPY 20d RV: {market_ctx['realized_vol_20d_pct']}% ({market_ctx['regime']})")
        send_telegram("\n".join(lines))
    
    print(f"[macro-calendar] done · {len(all_events)} events · T-1: {len(tomorrow_hi)} · 7d: {len(next_7d_hi)}")
    return {
        'statusCode': 200,
        'body': json.dumps({
            'ok': True,
            'n_events': len(all_events),
            'tomorrow_high_impact': len(tomorrow_hi),
            'next_7d_high_impact': len(next_7d_hi),
            'elapsed_s': payload['elapsed_s'],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
