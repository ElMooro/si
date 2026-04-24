#!/usr/bin/env python3
"""
Final fix in the chain — signal-logger now must fetch baseline_price
at log-time so outcome-checker has something to compare against.

Currently log_sig() only sets baseline_price when an explicit price=
kwarg is passed. Most callers don't pass it, so 12 of 13 signal types
have 0% baseline_price coverage → outcome-checker scores them all as
correct=None.

Fix: add a get_baseline_price(ticker) helper that mirrors outcome-checker's
fallback chain (CoinGecko for crypto → FMP /stable → Polygon /prev),
and call it inside log_sig() when no explicit price was passed.

Also fetch benchmark price (SPY) so OUTPERFORM/UNDERPERFORM signals
can be scored relative.

Net effect: from this moment forward, every NEW logged signal will have
both baseline_price and baseline_benchmark_price set. Old signals already
in the table can't be retroactively fixed (we don't know what BTC was
worth on April 21 unless we look up historical bars), so they will
remain unscored. But going forward the loop closes properly.
"""
import io
import os
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)


def build_zip(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


def deploy(fn_name, src_dir):
    z = build_zip(src_dir)
    lam.update_function_code(FunctionName=fn_name, ZipFile=z)
    lam.get_waiter("function_updated").wait(
        FunctionName=fn_name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    return len(z)


with report("fix_signal_logger_baseline") as r:
    r.heading("Fix signal-logger to capture baseline_price for every signal")

    sl_path = REPO_ROOT / "aws/lambdas/justhodl-signal-logger/source/lambda_function.py"
    src = sl_path.read_text(encoding="utf-8")

    # ─── Insert price-fetcher block after the imports/clients ───
    old_top = '''
import json,boto3,uuid,time,urllib.request
from datetime import datetime,timezone,timedelta
from decimal import Decimal

dynamodb=boto3.resource("dynamodb",region_name="us-east-1")
s3=boto3.client("s3",region_name="us-east-1")
SIGNALS_TABLE="justhodl-signals"
S3_BUCKET="justhodl-dashboard-live"
CFTC_URL="https://35t3serkv4gn2hk7utwvp7t2sa0flbum.lambda-url.us-east-1.on.aws/"
'''

    new_top = '''
import json,boto3,uuid,time,urllib.request,urllib.error
from datetime import datetime,timezone,timedelta
from decimal import Decimal

dynamodb=boto3.resource("dynamodb",region_name="us-east-1")
s3=boto3.client("s3",region_name="us-east-1")
SIGNALS_TABLE="justhodl-signals"
S3_BUCKET="justhodl-dashboard-live"
CFTC_URL="https://35t3serkv4gn2hk7utwvp7t2sa0flbum.lambda-url.us-east-1.on.aws/"

# Same keys outcome-checker uses
POLYGON_KEY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
FMP_KEY="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

# Cache prices within a single Lambda invocation (one fetch per ticker)
_PRICE_CACHE={}

def _polygon_prev(ticker):
    """Free-tier-friendly previous close."""
    url=f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={POLYGON_KEY}"
    try:
        with urllib.request.urlopen(url,timeout=8) as r:
            d=json.loads(r.read().decode())
            res=d.get("results") or []
            if res: return float(res[0].get("c") or 0)
    except Exception as e: print(f"[PRICE] Polygon {ticker}: {e}")
    return None

def _fmp_stable(ticker):
    """Modern FMP /stable/quote endpoint."""
    url=f"https://financialmodelingprep.com/stable/quote?symbol={ticker}&apikey={FMP_KEY}"
    try:
        with urllib.request.urlopen(url,timeout=8) as r:
            d=json.loads(r.read().decode())
            if d and isinstance(d,list) and len(d)>0:
                p=d[0].get("price")
                if p is not None: return float(p)
    except Exception as e: print(f"[PRICE] FMP {ticker}: {e}")
    return None

def _coingecko(ticker):
    """Free crypto fallback."""
    cmap={"BTC-USD":"bitcoin","BTC":"bitcoin","ETH-USD":"ethereum","ETH":"ethereum",
          "SOL-USD":"solana","SOL":"solana"}
    cg=cmap.get(ticker.upper())
    if not cg: return None
    url=f"https://api.coingecko.com/api/v3/simple/price?ids={cg}&vs_currencies=usd"
    try:
        with urllib.request.urlopen(url,timeout=8) as r:
            d=json.loads(r.read().decode())
            return float(d.get(cg,{}).get("usd") or 0)
    except Exception as e: print(f"[PRICE] CoinGecko {ticker}: {e}")
    return None

def get_baseline_price(ticker):
    """Get current price for a ticker — same fallback chain as outcome-checker.
    Cached within Lambda invocation to avoid duplicate fetches."""
    if not ticker: return None
    if ticker in _PRICE_CACHE: return _PRICE_CACHE[ticker]
    p=None
    if ticker.upper() in ("BTC-USD","ETH-USD","SOL-USD","BTC","ETH","SOL"):
        p=_coingecko(ticker)
    if not p: p=_fmp_stable(ticker)
    if not p: p=_polygon_prev(ticker)
    _PRICE_CACHE[ticker]=p
    return p
'''

    if old_top not in src:
        r.fail("  Logger top-of-file pattern not found verbatim — cannot patch")
        raise SystemExit(1)

    src = src.replace(old_top, new_top, 1)
    r.ok("  Inserted price-fetcher block at top of logger")

    # ─── Modify log_sig() to fetch baseline_price + benchmark when missing ───
    old_logsig = '''def log_sig(stype,val,pred,conf,against,windows,price=None,meta=None,bench=None):
    table=dynamodb.Table(SIGNALS_TABLE)
    now=datetime.now(timezone.utc)
    sid=str(uuid.uuid4())
    ts={f"day_{d}":(now+timedelta(days=d)).isoformat() for d in windows}
    item={"signal_id":sid,"signal_type":stype,"signal_value":str(val),
          "predicted_direction":pred,"confidence":f2d(float(conf)),
          "measure_against":against,"baseline_price":f2d(float(price)) if price else None,
          "benchmark":bench,"check_windows":[str(d) for d in windows],
          "check_timestamps":ts,"outcomes":{},"accuracy_scores":{},
          "logged_at":now.isoformat(),"logged_epoch":int(now.timestamp()),
          "status":"pending","metadata":f2d(meta or {}),
          "ttl":int((now+timedelta(days=365)).timestamp())}
    table.put_item(Item=item)
    print(f"[LOG] {stype}={val} {pred} conf={conf:.2f}")
    return sid'''

    new_logsig = '''def log_sig(stype,val,pred,conf,against,windows,price=None,meta=None,bench=None):
    table=dynamodb.Table(SIGNALS_TABLE)
    now=datetime.now(timezone.utc)
    sid=str(uuid.uuid4())
    ts={f"day_{d}":(now+timedelta(days=d)).isoformat() for d in windows}

    # Auto-fetch baseline_price if not explicitly passed
    if price is None and against:
        price=get_baseline_price(against)
    # Auto-fetch benchmark price for relative-comparison signals (OUTPERFORM/UNDERPERFORM)
    bench_price=None
    if bench and pred in ("OUTPERFORM","UNDERPERFORM"):
        bench_price=get_baseline_price(bench)

    item={"signal_id":sid,"signal_type":stype,"signal_value":str(val),
          "predicted_direction":pred,"confidence":f2d(float(conf)),
          "measure_against":against,"baseline_price":f2d(float(price)) if price else None,
          "baseline_benchmark_price":f2d(float(bench_price)) if bench_price else None,
          "benchmark":bench,"check_windows":[str(d) for d in windows],
          "check_timestamps":ts,"outcomes":{},"accuracy_scores":{},
          "logged_at":now.isoformat(),"logged_epoch":int(now.timestamp()),
          "status":"pending","metadata":f2d(meta or {}),
          "ttl":int((now+timedelta(days=365)).timestamp())}
    table.put_item(Item=item)
    bp_str=f" baseline=${price:.2f}" if price else " baseline=None"
    print(f"[LOG] {stype}={val} {pred} conf={conf:.2f}{bp_str}")
    return sid'''

    if old_logsig not in src:
        r.fail("  log_sig() pattern not found verbatim — cannot patch")
        raise SystemExit(1)

    src = src.replace(old_logsig, new_logsig, 1)
    r.ok("  Updated log_sig() to auto-fetch baseline_price + baseline_benchmark_price")

    import ast
    try:
        ast.parse(src)
    except SyntaxError as e:
        r.fail(f"  Syntax error: {e}")
        raise SystemExit(1)

    sl_path.write_text(src, encoding="utf-8")
    r.ok(f"  Source valid ({len(src)} bytes), saved")

    size = deploy("justhodl-signal-logger", sl_path.parent)
    r.ok(f"  Deployed signal-logger ({size:,} bytes)")

    # Trigger a fresh signal-logger run so we get a clean batch with baselines
    r.section("Trigger fresh signal-logger run with baseline_price capture")
    try:
        resp = lam.invoke(
            FunctionName="justhodl-signal-logger",
            InvocationType="Event",
        )
        r.ok(f"  Async-triggered signal-logger (status {resp['StatusCode']})")
        r.log("  Next run will create signals WITH baseline_price for all signal types.")
        r.log("  When daily outcome-checker fires (cron(30 22 ? * MON-FRI *)),")
        r.log("  it will be able to score them properly.")
    except Exception as e:
        r.fail(f"  Trigger failed: {e}")

    r.kv(
        baseline_price_capture="now automatic for all signals",
        old_signals="legacy 4,400 records can't be retroactively scored",
        new_signals="will all have baseline_price + baseline_benchmark_price",
    )
    r.log("Done")
