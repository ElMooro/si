"""
PHASE D — Hook all 5 hunter systems into the existing signal-logger Lambda
so that ALL signals get logged into the calibration system, allowing per-system
calibration weights to emerge over time.

Current state: only Layer 4 (asymmetric-hunter) signals get logged via the
nobrainer-tracker. The other 4 systems output JSON but their predictions never
get tracked → no calibration → no learning.

Fix: extend the signal-logger to read all 5 system outputs and log entries
into DynamoDB justhodl-signals with proper category tags.
"""
import io, json, os, time, base64, zipfile
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-system-signal-logger"
SCHEDULE_NAME = "justhodl-system-signal-logger-6h"
SCHEDULE_EXPR = "rate(6 hours)"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"

L = boto3.client("lambda", region_name=REGION)
EB = boto3.client("events", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


LAMBDA_SOURCE = '''"""
justhodl-system-signal-logger — logs hunter signals into DynamoDB justhodl-signals
for downstream calibration by justhodl-outcome-checker + justhodl-calibrator.

Reads all 5 system feeds, picks top picks (score >= MIN_SCORE), logs each
as a directional prediction with horizon, strike price, and origin system.

Schema in justhodl-signals table:
  PK: signal_id (uuid)
  ts_iso: when logged
  source: "insider_cluster" | "smart_money" | "deep_value" | "eps_velocity"
  ticker: SYMBOL
  direction: "UP"
  conviction: score / 100
  baseline_price: price at log-time (from FMP /quote)
  horizon_days_primary: 60-180 depending on system
  metadata: full record from source feed

The existing outcome-checker (Sun 22:30 UTC) will price-check these names
forward and the calibrator (Sun 9 UTC) will produce per-source weights.
"""
import json
import os
import time
import urllib.request
import uuid
import boto3
from decimal import Decimal

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
TABLE = os.environ.get("DDB_TABLE", "justhodl-signals")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
MIN_SCORE = float(os.environ.get("MIN_SCORE", "65"))

S3 = boto3.client("s3", region_name=REGION)
DDB = boto3.resource("dynamodb", region_name=REGION).Table(TABLE)


def _quote_price(symbol):
    """Fetch latest price for a symbol from FMP. Returns float or None."""
    url = f"https://financialmodelingprep.com/stable/quote?symbol={symbol}&apikey={FMP_KEY}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            d = json.loads(r.read())
            if isinstance(d, list) and d:
                return float(d[0].get("price", 0)) or None
    except Exception:
        pass
    return None


def _to_decimal(v):
    """Convert float to Decimal for DynamoDB. Recurse through dicts/lists."""
    if isinstance(v, float):
        return Decimal(str(round(v, 6)))
    if isinstance(v, dict):
        return {k: _to_decimal(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_to_decimal(x) for x in v]
    return v


def log_signal(signal_id, ts_iso, source, ticker, direction, conviction,
                baseline_price, horizon_days, metadata):
    item = {
        "signal_id": signal_id,
        "ts_iso": ts_iso,
        "source": source,
        "ticker": ticker,
        "direction": direction,
        "conviction": Decimal(str(round(conviction, 4))),
        "horizon_days_primary": int(horizon_days),
        "check_windows": [int(horizon_days), int(horizon_days * 2)],
        "logged_at": int(time.time()),
        "metadata": _to_decimal(metadata),
    }
    if baseline_price is not None:
        item["baseline_price"] = Decimal(str(round(float(baseline_price), 4)))
    DDB.put_item(Item=item)


def process_insider_clusters(ts_iso):
    """Top insider-cluster signals (score >= 70)."""
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key="data/insider-clusters.json")["Body"].read())
    except Exception as e:
        print(f"[signal-logger] insider-clusters skipped: {e}")
        return 0
    n = 0
    for c in d.get("clusters", []):
        score = c.get("score") or 0
        if score < MIN_SCORE:
            continue
        ticker = c.get("ticker")
        if not ticker:
            continue
        avg_price = c.get("avg_price") or _quote_price(ticker)
        sid = f"insider_{ticker}_{int(time.time())}"
        try:
            log_signal(
                signal_id=sid, ts_iso=ts_iso,
                source="insider_cluster", ticker=ticker,
                direction="UP", conviction=score / 100,
                baseline_price=avg_price, horizon_days=90,
                metadata={
                    "signal_type": c.get("signal_type"),
                    "n_insiders": c.get("n_insiders"),
                    "total_value": float(c.get("total_value") or 0),
                    "has_ceo": bool(c.get("has_ceo")),
                    "has_cfo": bool(c.get("has_cfo")),
                },
            )
            n += 1
        except Exception as e:
            print(f"[signal-logger] insider {ticker} put_item: {e}")
    print(f"[signal-logger] logged {n} insider-cluster signals")
    return n


def process_smart_money(ts_iso):
    """Top 13F smart-money clusters (score >= 70)."""
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key="data/smart-money-clusters.json")["Body"].read())
    except Exception as e:
        print(f"[signal-logger] smart-money skipped: {e}")
        return 0
    n = 0
    for c in d.get("clusters", []):
        score = c.get("score") or 0
        if score < MIN_SCORE:
            continue
        ticker = c.get("ticker")
        if not ticker:
            continue
        price = _quote_price(ticker)
        sid = f"smartmoney_{ticker}_{int(time.time())}"
        try:
            log_signal(
                signal_id=sid, ts_iso=ts_iso,
                source="smart_money", ticker=ticker,
                direction="UP", conviction=score / 100,
                baseline_price=price, horizon_days=180,
                metadata={
                    "signal_types": c.get("signal_types") or [],
                    "n_buyers": c.get("n_buyers"),
                    "legend_buyers": c.get("legend_buyers") or [],
                },
            )
            n += 1
        except Exception as e:
            print(f"[signal-logger] smart-money {ticker} put_item: {e}")
    print(f"[signal-logger] logged {n} smart-money signals")
    return n


def process_deep_value(ts_iso):
    """Deep-value tier-A and tier-B (score >= 70)."""
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key="data/deep-value.json")["Body"].read())
    except Exception as e:
        print(f"[signal-logger] deep-value skipped: {e}")
        return 0
    n = 0
    for c in d.get("all_qualifying", []):
        score = c.get("score") or 0
        if score < MIN_SCORE:
            continue
        if c.get("flag") in ("FINANCIAL_BOOK_EXCLUDED", "REIT_EXCLUDED"):
            continue
        ticker = c.get("symbol")
        if not ticker:
            continue
        f = c.get("fundamentals") or {}
        price = f.get("price") or _quote_price(ticker)
        sid = f"deepvalue_{ticker}_{int(time.time())}"
        try:
            log_signal(
                signal_id=sid, ts_iso=ts_iso,
                source="deep_value", ticker=ticker,
                direction="UP", conviction=score / 100,
                baseline_price=price, horizon_days=180,
                metadata={
                    "flag": c.get("flag"),
                    "net_cash_pct": float(f.get("net_cash_pct_of_mcap") or 0),
                    "rev_yield": float(f.get("revenue_yield_of_mcap") or 0),
                    "mcap_to_rev": float(f.get("mcap_to_rev") or 0),
                },
            )
            n += 1
        except Exception as e:
            print(f"[signal-logger] deep-value {ticker} put_item: {e}")
    print(f"[signal-logger] logged {n} deep-value signals")
    return n


def process_eps_velocity(ts_iso):
    """EPS velocity tier-A and tier-B (score >= 70)."""
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key="data/eps-revision-velocity.json")["Body"].read())
    except Exception as e:
        print(f"[signal-logger] eps-velocity skipped: {e}")
        return 0
    n = 0
    for c in d.get("all_qualifying", []):
        score = c.get("score") or 0
        if score < MIN_SCORE:
            continue
        ticker = c.get("symbol")
        if not ticker:
            continue
        f = c.get("fundamentals") or {}
        price = f.get("price") or _quote_price(ticker)
        sid = f"epsvelocity_{ticker}_{int(time.time())}"
        try:
            log_signal(
                signal_id=sid, ts_iso=ts_iso,
                source="eps_velocity", ticker=ticker,
                direction="UP", conviction=score / 100,
                baseline_price=price, horizon_days=120,
                metadata={
                    "flag": c.get("flag"),
                    "fy2_lift_pct": float((c.get("estimates") or {}).get("fy2_lift_pct") or 0),
                    "fwd_rev_growth_pct": float((c.get("estimates") or {}).get("fwd_rev_growth_pct") or 0),
                },
            )
            n += 1
        except Exception as e:
            print(f"[signal-logger] eps-velocity {ticker} put_item: {e}")
    print(f"[signal-logger] logged {n} eps-velocity signals")
    return n


def process_compound(ts_iso):
    """Top compound signals (>= 2 systems agreeing). High-conviction."""
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key="data/compound-signals.json")["Body"].read())
    except Exception as e:
        print(f"[signal-logger] compound skipped: {e}")
        return 0
    n = 0
    for c in d.get("compound", []):
        compound_score = c.get("compound_score") or 0
        if compound_score < 150:  # only log high-conviction compounds
            continue
        ticker = c.get("symbol")
        if not ticker:
            continue
        price = _quote_price(ticker)
        sid = f"compound_{ticker}_{int(time.time())}"
        try:
            log_signal(
                signal_id=sid, ts_iso=ts_iso,
                source="compound", ticker=ticker,
                direction="UP",
                conviction=min(compound_score / 300, 1.0),  # 300 is high
                baseline_price=price, horizon_days=120,
                metadata={
                    "n_systems": c.get("n_systems"),
                    "systems": c.get("systems") or [],
                },
            )
            n += 1
        except Exception as e:
            print(f"[signal-logger] compound {ticker} put_item: {e}")
    print(f"[signal-logger] logged {n} compound signals")
    return n


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[signal-logger] starting v1.0, MIN_SCORE={MIN_SCORE}")
    ts_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())

    counts = {
        "insider_cluster": process_insider_clusters(ts_iso),
        "smart_money": process_smart_money(ts_iso),
        "deep_value": process_deep_value(ts_iso),
        "eps_velocity": process_eps_velocity(ts_iso),
        "compound": process_compound(ts_iso),
    }
    total = sum(counts.values())
    print(f"[signal-logger] total logged: {total}, breakdown: {counts}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "total": total,
            "by_source": counts,
            "duration_s": round(time.time() - started, 2),
        }),
    }
'''


def main():
    section("0) Verify justhodl-signals DDB table exists")
    ddb = boto3.client("dynamodb", region_name=REGION)
    try:
        info = ddb.describe_table(TableName="justhodl-signals")
        log(f"  ✓ table exists, status={info['Table']['TableStatus']}")
    except Exception as e:
        log(f"  ❌ {e} — won't continue without DDB table")
        return

    section("1) Write Lambda source")
    src_dir = "aws/lambdas/justhodl-system-signal-logger/source"
    os.makedirs(src_dir, exist_ok=True)
    src_path = f"{src_dir}/lambda_function.py"
    with open(src_path, "w", encoding="utf-8") as f:
        f.write(LAMBDA_SOURCE)
    log(f"  wrote {src_path}: {len(LAMBDA_SOURCE)} chars")

    import ast
    try:
        ast.parse(LAMBDA_SOURCE)
        log("  ✓ valid python")
    except SyntaxError as e:
        log(f"  ❌ {e}")
        return

    section("2) Build zip + deploy")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, LAMBDA_SOURCE)
    zb = buf.getvalue()
    log(f"  zip: {len(zb):,}b")

    exists = False
    try:
        L.get_function(FunctionName=LAMBDA_NAME)
        exists = True
        log("  exists — updating")
    except L.exceptions.ResourceNotFoundException:
        log("  creating")

    env = {
        "S3_BUCKET": "justhodl-dashboard-live",
        "DDB_TABLE": "justhodl-signals",
        "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
        "MIN_SCORE": "65",
    }
    if exists:
        L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
        for _ in range(30):
            c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
        L.update_function_configuration(
            FunctionName=LAMBDA_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=512, Timeout=300,
            Environment={"Variables": env},
        )
    else:
        L.create_function(
            FunctionName=LAMBDA_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN, Code={"ZipFile": zb},
            Timeout=300, MemorySize=512,
            Environment={"Variables": env},
        )

    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ ready")

    section("3) Schedule rate(6 hours)")
    rule_arn = EB.put_rule(Name=SCHEDULE_NAME, ScheduleExpression=SCHEDULE_EXPR, State="ENABLED")["RuleArn"]
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{LAMBDA_NAME}"
    EB.put_targets(Rule=SCHEDULE_NAME, Targets=[{"Id": "1", "Arn": fn_arn}])
    try:
        L.add_permission(FunctionName=LAMBDA_NAME, StatementId=f"{SCHEDULE_NAME}-eb",
                          Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                          SourceArn=rule_arn)
        log("  ✓ permission added")
    except L.exceptions.ResourceConflictException:
        log("  ✓ permission already exists")

    section("4) Smoke invoke")
    t0 = time.time()
    r = L.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                  LogType="Tail", Payload=b"{}")
    log(f"  status: {r['StatusCode']}, dur: {time.time()-t0:.1f}s")
    body = json.loads(r["Payload"].read())
    log(f"  body: {json.dumps(body)[:400]}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-15:]:
            log(f"    {ln.rstrip()}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_d_system_signal_logger.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
