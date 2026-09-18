"""Private holdings monitor; legacy Kelly allocation is retired.

The historical helper functions remain for inspection of the former method.
The production handler never uses score-to-return constants, default NAV,
default volatility or Telegram. It publishes WAIT and null sizing targets
until a separately validated protocol and capital book exist.
"""
from private_artifact import publish_private, private_http_denied
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.0.0"

S3_BUCKET = "justhodl-dashboard-live"
SIZING_KEY = "portfolio/sizing.json"
SNAPSHOT_KEY = "portfolio/snapshot.json"
RISK_KEY = "portfolio/risk.json"
ANOMALIES_KEY = "signals/anomalies.json"
ALPHA_KEY = "screener/alpha-score.json"
CONFLUENCE_KEY = "signals/confluence.json"
ALERT_HISTORY_KEY = "portfolio/sizing-alert-history.json"

DDB_TABLE = "justhodl-portfolio"

POLY_KEY = os.environ.get("POLY_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Sizing parameters
KELLY_FRACTION = 0.25                 # quarter-Kelly
MAX_SINGLE_POSITION_PCT = 8.0         # cap any one position at 8% of NAV
MAX_SECTOR_PCT = 30.0                 # cap any sector at 30%
DEFAULT_NAV_IF_NO_POSITIONS = 100_000.0  # fallback notional for empty books
ENTRY_CANDIDATE_TIER_S_TOP_N = 5      # how many new TIER S to suggest
ENTRY_CANDIDATE_TIER_A_TOP_N = 5      # how many new TIER A to suggest

# Alert thresholds
GAP_ALERT_PCT = 3.0     # |current_weight - kelly_weight| > 3% triggers alert candidacy
DEDUPE_HOURS = 12

s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")
table = ddb.Table(DDB_TABLE)
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════
# DATA LOADERS
# ═══════════════════════════════════════════════════════════════════════

def load_s3_json(key):
    try:
        body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        return json.loads(body)
    except Exception as e:
        print(f"  load {key} err: {str(e)[:120]}")
        return None


def scan_ddb_positions():
    """Returns dict of {symbol: position_dict}."""
    out = {}
    last = None
    while True:
        kwargs = {"FilterExpression": "pk = :p",
                   "ExpressionAttributeValues": {":p": "POSITION"}}
        if last: kwargs["ExclusiveStartKey"] = last
        resp = table.scan(**kwargs)
        for item in resp.get("Items") or []:
            sym = item.get("symbol")
            if sym: out[sym] = _decimal_to_float(item)
        last = resp.get("LastEvaluatedKey")
        if not last: break
    return out


def _decimal_to_float(obj):
    if isinstance(obj, Decimal): return float(obj)
    if isinstance(obj, dict): return {k: _decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, list): return [_decimal_to_float(v) for v in obj]
    return obj


def fetch_polygon_price(symbol):
    """Latest close + day change."""
    if not POLY_KEY: return None, None
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev?adjusted=true&apiKey={POLY_KEY}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Sizer/1.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode("utf-8"))
        if data.get("results"):
            r0 = data["results"][0]
            close = r0["c"]
            open_ = r0["o"]
            day_chg_pct = ((close - open_) / open_ * 100) if open_ else 0
            return close, day_chg_pct
    except Exception as e:
        print(f"  poly:{symbol} {str(e)[:80]}")
    return None, None


def batch_fetch_prices(symbols, max_workers=10):
    """Parallel price fetch."""
    out = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_polygon_price, s): s for s in symbols}
        for f in as_completed(futures):
            sym = futures[f]
            try: out[sym] = f.result()
            except Exception: out[sym] = (None, None)
    return out


# ═══════════════════════════════════════════════════════════════════════
# SIZING LOGIC
# ═══════════════════════════════════════════════════════════════════════

def expected_alpha_return(alpha_score):
    """Mapping from alpha score to 60-day expected return (as percentage)."""
    if alpha_score is None: return 0
    if alpha_score >= 90: return 12.0
    if alpha_score >= 80: return 8.0
    if alpha_score >= 70: return 5.0
    if alpha_score >= 60: return 3.0
    if alpha_score >= 50: return 1.5
    return -1.0


def confidence_factor(confluence_tier):
    """Conviction multiplier from confluence tier."""
    return {"S": 1.20, "A": 1.00, "B": 0.70}.get(confluence_tier, 0.50)


def regime_multiplier(macro_stress_score):
    """Risk multiplier from Macro Stress Score."""
    if macro_stress_score is None: return 0.80
    if macro_stress_score < 20: return 1.00
    if macro_stress_score < 40: return 0.85
    if macro_stress_score < 60: return 0.65
    if macro_stress_score < 80: return 0.40
    return 0.20


def regime_label(macro_stress_score):
    if macro_stress_score is None: return "UNKNOWN"
    if macro_stress_score < 20: return "GOLDILOCKS"
    if macro_stress_score < 40: return "NORMAL"
    if macro_stress_score < 60: return "ELEVATED"
    if macro_stress_score < 80: return "HIGH_STRESS"
    return "CRISIS"


def drawdown_multiplier(portfolio_pnl_pct):
    """Drawdown circuit breaker."""
    if portfolio_pnl_pct is None: return 1.00
    if portfolio_pnl_pct >= -3: return 1.00
    if portfolio_pnl_pct >= -7: return 0.75
    if portfolio_pnl_pct >= -12: return 0.50
    return 0.25


def kelly_weight_pct(alpha_score, confluence_tier, annual_vol_pct,
                       regime_mult, drawdown_mult):
    """Compute Kelly-optimal portfolio weight (as % of NAV)."""
    edge_pct = expected_alpha_return(alpha_score) * confidence_factor(confluence_tier)
    if edge_pct <= 0: return 0.0  # negative edge — don't size
    edge = edge_pct / 100.0
    vol = (annual_vol_pct or 25) / 100.0  # default 25% vol if unknown
    if vol < 0.05: vol = 0.05  # floor at 5% to avoid blow-up
    kelly_full = edge / (vol ** 2)
    kelly_q = kelly_full * KELLY_FRACTION
    kelly_regime = kelly_q * regime_mult * drawdown_mult
    kelly_capped = min(kelly_regime * 100.0, MAX_SINGLE_POSITION_PCT)
    return max(0.0, round(kelly_capped, 2))


def action_label(current_pct, kelly_pct):
    if kelly_pct == 0 and current_pct > 0: return "TRIM"
    if current_pct == 0 and kelly_pct > 0: return "NEW"
    gap = kelly_pct - current_pct
    if abs(gap) < 1.0: return "HOLD"
    return "ADD" if gap > 0 else "TRIM"


# ═══════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════

def get_chat_id():
    if TELEGRAM_CHAT_ID: return TELEGRAM_CHAT_ID
    try:
        return ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                  WithDecryption=True)["Parameter"]["Value"]
    except Exception: return None


def send_telegram(text, chat_id):
    if not TELEGRAM_TOKEN or not chat_id: return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    body = urllib.parse.urlencode({
        "chat_id": chat_id, "text": text[:4000],
        "parse_mode": "Markdown", "disable_web_page_preview": "true",
    }).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode("utf-8")).get("ok", False)
    except Exception as e:
        print(f"  telegram err: {str(e)[:200]}")
        return False


def load_alert_history():
    try:
        history = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=ALERT_HISTORY_KEY)["Body"].read())
        if not isinstance(history, dict):
            raise ValueError("invalid owner history document")
        return history
    except Exception as error:
        if str(getattr(error, "response", {}).get("Error", {}).get("Code", "")) in {"404", "NoSuchKey", "NotFound"}:
            return {}
        raise


def save_alert_history(h):
    s3.put_object(Bucket=S3_BUCKET, Key=ALERT_HISTORY_KEY,
        Body=json.dumps(h, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json", CacheControl="private, no-store")
    publish_private("portfolio-sizing-history", h)


def should_alert(history, key):
    last = history.get(key)
    if not last: return True
    try:
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - last_dt) >= timedelta(hours=DEDUPE_HOURS)
    except Exception: return True


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def _run_private(event, context):
    """Legacy score-to-return constants have no validated forecasting authority.

    Do not substitute holdings value or a default dollar amount for account NAV.
    Existing capital-contract/proposed-book-risk engines remain the sizing path.
    """
    import hashlib
    from capital_contract import capital_book_view
    snapshot = load_s3_json(SNAPSHOT_KEY) or {}
    risk = load_s3_json(RISK_KEY) or {}
    book = capital_book_view(snapshot)
    reasons = ['UNVALIDATED_SCORE_TO_RETURN_MODEL', 'NO_APPROVED_SIZING_PROTOCOL']
    if book['status'] != 'READY': reasons.append('RECONCILED_CAPITAL_BOOK_UNAVAILABLE')
    metrics = risk.get('position_metrics') or {}
    rows = []
    for position in snapshot.get('positions') or []:
        symbol = position.get('symbol')
        rows.append({'symbol': symbol, 'current_shares': position.get('qty'),
            'current_value': position.get('market_value'), 'current_price': position.get('current_price'),
            'current_weight_pct': None, 'annual_vol_pct': (metrics.get(symbol) or {}).get('annual_vol_pct'),
            'alpha_score': position.get('alpha_score'), 'alpha_tier': position.get('tier'),
            'sector': position.get('sector'), 'kelly_weight_pct': None, 'weight_gap_pct': None,
            'target_value': None, 'target_shares': None, 'shares_delta': None, 'dollar_delta': None,
            'action': 'WAIT', 'rationale': 'No validated return forecast or sizing protocol; no trade instruction.'})
    payload = {'engine': 'justhodl-portfolio-sizer', 'version': '2.0.0',
        'generated_at': datetime.now(timezone.utc).isoformat(), 'status': 'RESEARCH_ONLY',
        'method': 'Legacy Kelly allocation suspended until forecast and capital validation',
        'permissions': {'sizing_eligible': False, 'may_recommend_trades': False}, 'reason_codes': reasons,
        'input_binding': {'snapshot_sha256': hashlib.sha256(json.dumps(snapshot,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
                         'risk_generated_at': risk.get('generated_at'), 'risk_replay': risk.get('replay')},
        'summary': {'nav': None, 'current_invested_pct': None, 'kelly_invested_pct': None,
                    'kelly_cash_pct': None, 'actionable_count': 0, 'entry_candidates_count': 0,
                    'regime': 'RESEARCH_ONLY', 'macro_stress_score': None, 'regime_multiplier': None,
                    'drawdown_multiplier': None},
        'positions': rows, 'entry_candidates': [], 'alerts_sent': 0, 'alerts_skipped_dedupe': 0,
        'legacy_model': {'status': 'UNVALIDATED', 'score_to_return_calibration': False,
                         'note': 'Prior constants are not empirical expected returns; absent risk is not a default volatility.'}}
    s3.put_object(Bucket=S3_BUCKET, Key=SIZING_KEY, Body=json.dumps(payload,separators=(',',':')).encode(),
                  ContentType='application/json', CacheControl='private, no-store')
    publish_private('portfolio-sizing', payload)
    return {'statusCode':200, 'body':json.dumps({'success':True,'version':'2.0.0','status':'RESEARCH_ONLY','alerts_sent':0})}


def lambda_handler(event, context):
    denied = private_http_denied(event)
    if denied is not None:
        return denied
    response = _run_private(event, context)
    if isinstance(response, dict) and "statusCode" in response:
        response["headers"] = {**response.get("headers", {}), "Cache-Control": "private, no-store", "Vary": "Authorization"}
    return response
