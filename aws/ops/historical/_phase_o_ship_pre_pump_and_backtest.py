"""Phase O — Ship pre-pump detector + run historical backtest against the user's
pump list. The KEY question: would this signal have fired while the names were
still COILING (before the breakout)?
"""
import io, json, os, time, base64, zipfile, urllib.request, statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-pre-pump-detector"
SCHEDULE_NAME = "justhodl-pre-pump-detector-daily"
SCHEDULE_EXPR = "cron(15 13 * * ? *)"  # 13:15 UTC daily
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

L = boto3.client("lambda", region_name=REGION)
EB = boto3.client("events", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    src = open("aws/lambdas/justhodl-pre-pump-detector/source/lambda_function.py").read()
    log(f"  source: {len(src)} chars")

    section("1) Create + deploy pre-pump-detector Lambda")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    zb = buf.getvalue()
    log(f"  zip: {len(zb):,}b")

    env = {
        "S3_BUCKET": "justhodl-dashboard-live",
        "FMP_KEY": FMP_KEY,
        "MAX_TICKERS": "600",
        "TIMEOUT_BUDGET_S": "260",
        "MIN_DOLLAR_VOL": "1000000",
        "N_WORKERS": "12",
    }
    exists = False
    try:
        L.get_function(FunctionName=LAMBDA_NAME)
        exists = True
    except L.exceptions.ResourceNotFoundException:
        pass

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
            MemorySize=1024, Timeout=300,
            Environment={"Variables": env},
        )
    else:
        L.create_function(
            FunctionName=LAMBDA_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN, Code={"ZipFile": zb},
            Timeout=300, MemorySize=1024,
            Environment={"Variables": env},
        )

    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ deployed at {c['LastModified']}")

    section("2) Schedule daily 13:15 UTC")
    rule_arn = EB.put_rule(Name=SCHEDULE_NAME, ScheduleExpression=SCHEDULE_EXPR, State="ENABLED")["RuleArn"]
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{LAMBDA_NAME}"
    EB.put_targets(Rule=SCHEDULE_NAME, Targets=[{"Id": "1", "Arn": fn_arn}])
    try:
        L.add_permission(FunctionName=LAMBDA_NAME, StatementId=f"{SCHEDULE_NAME}-eb",
                          Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                          SourceArn=rule_arn)
        log("  ✓ permission added")
    except L.exceptions.ResourceConflictException:
        log("  ✓ permission exists")

    section("3) Smoke invoke")
    from botocore.config import Config
    L2 = boto3.client("lambda", region_name=REGION,
                       config=Config(read_timeout=600, connect_timeout=10))
    t0 = time.time()
    r = L2.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                   LogType="Tail", Payload=b"{}")
    dur = time.time() - t0
    log(f"  status: {r['StatusCode']}, dur: {dur:.1f}s")
    body = json.loads(r["Payload"].read())
    log(f"  body: {body.get('body','')[:300]}")

    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-10:]:
            log(f"    {ln.rstrip()}")

    section("4) Today's top 15 pre-pump signals")
    obj = S3.get_object(Bucket=BUCKET, Key="data/pre-pump-signals.json")
    d = json.loads(obj["Body"].read())
    log(f"  generated_at: {d.get('generated_at')}")
    log(f"  stats: {json.dumps(d.get('stats', {}))}")
    log("")
    log(f"  ── TIER_A_COILED candidates ──")
    for c in d.get("summary", {}).get("top_25_overall", [])[:15]:
        log(f"    {c['symbol']:<6} {c['score']:>5.1f} {c['tier']:<18}  range_pos={c['range_pos']}%  vol_comp={c['vol_compression']}  obv={c['obv_slope']}  liq={c['liq_expansion']}")

    section("5) HISTORICAL BACKTEST — would pre-pump have caught these EARLY?")
    log("  At various dates, simulate the pre-pump scoring using only data available")
    log("  AT THAT DATE. Track when each pump-list name would have been flagged.")
    log("")
    
    PUMP_LIST = [
        ("AXTI",  464),
        ("LWLG",  408),
        ("AAOI",  353),
        ("AEHR",  277),
        ("ICHR",  138),
        ("MRVL",  130),
        ("INTC",  122),
        ("LITE",  116),
        ("CRDO",  101),
    ]

    # Pull 270d history for each name
    def fetch_h(sym):
        url = f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={sym}&apikey={FMP_KEY}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Backtest"})
            with urllib.request.urlopen(req, timeout=20) as r:
                d = json.loads(r.read())
                out = []
                for x in d[:270]:
                    if x.get("close") and x.get("date"):
                        out.append({
                            "date": x.get("date"),
                            "close": float(x.get("close")),
                            "high": float(x.get("high") or x.get("close")),
                            "low": float(x.get("low") or x.get("close")),
                            "volume": float(x.get("volume") or 0),
                        })
                out.sort(key=lambda x: x["date"])
                return out
        except Exception:
            return None

    # Pre-pump scoring (mirrors the Lambda's logic)
    def score_at(history_window):
        if not history_window or len(history_window) < 120:
            return None
        closes = [h["close"] for h in history_window]
        volumes = [h["volume"] for h in history_window]
        n = len(closes)
        today = closes[-1]
        avg_dollar_vol = sum(c * v for c, v in zip(closes[-30:], volumes[-30:])) / min(30, n)
        if avg_dollar_vol < 1000000:
            return None

        lookback = min(180, n)
        range_high = max(closes[-lookback:])
        range_low = min(closes[-lookback:])
        if range_high == range_low:
            return None
        range_position = (today - range_low) / (range_high - range_low) * 100

        returns_60 = []
        for i in range(max(0, n-61), n-1):
            if closes[i] > 0:
                returns_60.append((closes[i+1]/closes[i] - 1) * 100)
        returns_180 = []
        for i in range(max(0, n-181), n-1):
            if closes[i] > 0:
                returns_180.append((closes[i+1]/closes[i] - 1) * 100)
        stdev_60 = statistics.stdev(returns_60) if len(returns_60) > 1 else 0
        stdev_180 = statistics.stdev(returns_180) if len(returns_180) > 1 else 0
        vol_compression = stdev_180 / stdev_60 if stdev_60 > 0 else 0

        obv = [0]
        for i in range(1, n):
            if closes[i] > closes[i-1]: obv.append(obv[-1] + volumes[i])
            elif closes[i] < closes[i-1]: obv.append(obv[-1] - volumes[i])
            else: obv.append(obv[-1])
        if len(obv) >= 60:
            obv_recent = obv[-60:]
            x_mean = (len(obv_recent) - 1) / 2
            y_mean = sum(obv_recent) / len(obv_recent)
            num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(obv_recent))
            den = sum((i - x_mean) ** 2 for i in range(len(obv_recent)))
            obv_slope = num / den if den > 0 else 0
            avg_vol = sum(volumes[-60:]) / 60
            obv_slope_norm = obv_slope / avg_vol if avg_vol > 0 else 0
        else:
            obv_slope_norm = 0

        avg_vol_20 = sum(volumes[-20:]) / min(20, n)
        avg_vol_60 = sum(volumes[-60:]) / min(60, n) if n >= 60 else avg_vol_20
        avg_vol_120 = sum(volumes[-120:]) / min(120, n) if n >= 120 else avg_vol_60
        liq_expansion_30v120 = avg_vol_20 / avg_vol_120 if avg_vol_120 > 0 else 1.0

        ret_60d = (today / closes[-61] - 1) * 100 if n >= 61 else 0
        ret_120d = (today / closes[-121] - 1) * 100 if n >= 121 else 0
        ret_5d = (today / closes[-6] - 1) * 100 if n >= 6 else 0

        has_disaster = False
        for i in range(max(0, n-60), n-1):
            if closes[i] > 0 and (closes[i+1]/closes[i] - 1) * 100 < -15:
                has_disaster = True; break

        score = 0
        if range_position < 20: score += 25
        elif range_position < 35: score += 18
        elif range_position < 50: score += 8
        if vol_compression > 1.6: score += 18
        elif vol_compression > 1.25: score += 12
        elif vol_compression > 1.10: score += 5
        if obv_slope_norm > 0.5: score += 20
        elif obv_slope_norm > 0.2: score += 12
        elif obv_slope_norm > 0.05: score += 5
        if liq_expansion_30v120 > 1.6: score += 15
        elif liq_expansion_30v120 > 1.25: score += 10
        elif liq_expansion_30v120 > 1.05: score += 4
        if abs(ret_60d) < 8 and abs(ret_120d) < 12: score += 10
        elif abs(ret_60d) < 15: score += 5
        if 1 <= ret_5d <= 8: score += 12
        elif 0 <= ret_5d <= 12: score += 5
        if has_disaster: score = score * 0.4
        return min(score, 100)

    log("  Walking history to find earliest pre-pump fire date for each name:")
    log("")
    for sym, total in PUMP_LIST:
        h = fetch_h(sym)
        if not h or len(h) < 150:
            log(f"  {sym:<6} no history")
            continue
        baseline = h[0]["close"]
        peak = max(x["close"] for x in h)
        # When does score first cross 60?
        first_60 = None
        first_75 = None
        for i in range(120, len(h)):
            window = h[:i+1]
            score = score_at(window)
            if score is None:
                continue
            current = window[-1]["close"]
            gain_so_far = (current / baseline - 1) * 100
            future_peak_gain = (peak / current - 1) * 100
            if score >= 60 and first_60 is None:
                first_60 = (window[-1]["date"], score, gain_so_far, future_peak_gain)
            if score >= 75 and first_75 is None:
                first_75 = (window[-1]["date"], score, gain_so_far, future_peak_gain)
        log(f"  {sym} (final +{total:.0f}%):")
        if first_60:
            d_, sc, sf, fg = first_60
            log(f"    PRE-PUMP score≥60 first: {d_}  score={sc:.0f}  gain_so_far=+{sf:.0f}%  future_peak_gain=+{fg:.0f}%")
        else:
            log(f"    PRE-PUMP never crossed 60 (silent name — needs different signal)")
        if first_75:
            d_, sc, sf, fg = first_75
            log(f"    PRE-PUMP score≥75 first: {d_}  score={sc:.0f}  gain_so_far=+{sf:.0f}%  future_peak_gain=+{fg:.0f}%")
        log("")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_o_pre_pump_backtest.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
