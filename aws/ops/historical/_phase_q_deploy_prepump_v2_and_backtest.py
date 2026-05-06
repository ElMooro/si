"""
PHASE Q — Deploy pre-pump v2 (calibrated) + run historical backtest.
"""
import io, json, os, time, base64, zipfile, urllib.request, statistics
import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-pre-pump-detector"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

L = boto3.client("lambda", region_name=REGION)
L2 = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=600, connect_timeout=10))
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S")
    print("- `" + ts + "`   " + m)
    REPORT.append("- `" + ts + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


def fetch_h(sym):
    url = "https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=" + sym + "&apikey=" + FMP_KEY
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-BT-v2"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read())
            out = []
            for x in d[:300]:
                if x.get("close") and x.get("date"):
                    out.append({
                        "date": x.get("date"),
                        "close": float(x.get("close")),
                        "volume": float(x.get("volume") or 0),
                    })
            out.sort(key=lambda x: x["date"])
            return out
    except Exception:
        return None


def find_breakout_idx(h):
    for i in range(20, len(h)):
        if (h[i]["close"] / h[i-5]["close"] - 1) * 100 > 25:
            return i
    return None


def v2_score_at(window):
    if not window or len(window) < 120:
        return None, []
    closes = [h["close"] for h in window]
    volumes = [h["volume"] for h in window]
    n = len(closes)
    today = closes[-1]

    avg_dol = sum(c * v for c, v in zip(closes[-30:], volumes[-30:])) / min(30, n)
    if avg_dol < 1000000:
        return None, []

    rh = max(closes[-min(180, n):])
    rl = min(closes[-min(180, n):])
    range_pos = (today - rl) / (rh - rl) * 100 if rh > rl else 50

    r60 = []
    for i in range(max(0, n-61), n-1):
        if closes[i] > 0:
            r60.append((closes[i+1]/closes[i] - 1) * 100)
    r180 = []
    for i in range(max(0, n-181), n-1):
        if closes[i] > 0:
            r180.append((closes[i+1]/closes[i] - 1) * 100)
    s60 = statistics.stdev(r60) if len(r60) > 1 else 0
    s180 = statistics.stdev(r180) if len(r180) > 1 else 0
    vc = s180 / s60 if s60 > 0 else 0

    obv = [0]
    for i in range(1, n):
        if closes[i] > closes[i-1]: obv.append(obv[-1] + volumes[i])
        elif closes[i] < closes[i-1]: obv.append(obv[-1] - volumes[i])
        else: obv.append(obv[-1])
    if len(obv) >= 60:
        o = obv[-60:]
        xm = (len(o)-1)/2; ym = sum(o)/len(o)
        num = sum((i-xm)*(v-ym) for i, v in enumerate(o))
        den = sum((i-xm)**2 for i in range(len(o)))
        slope = num/den if den > 0 else 0
        avg_v = sum(volumes[-60:])/60
        obv_n = slope/avg_v if avg_v > 0 else 0
    else:
        obv_n = 0

    av20 = sum(volumes[-20:])/min(20, n)
    av120 = sum(volumes[-120:])/min(120, n) if n >= 120 else av20
    liq = av20/av120 if av120 > 0 else 1.0

    r5 = (today/closes[-6]-1)*100 if n >= 6 else 0
    r30 = (today/closes[-31]-1)*100 if n >= 31 else 0
    r60d = (today/closes[-61]-1)*100 if n >= 61 else 0

    disaster = False
    for i in range(max(0, n-60), n-1):
        if closes[i] > 0 and (closes[i+1]/closes[i]-1)*100 < -15:
            disaster = True; break

    sc = 0.0
    flags = []
    if obv_n >= 0.40: sc += 30; flags.append("OBV_STRONG")
    elif obv_n >= 0.20: sc += 22; flags.append("OBV_ACCUM")
    elif obv_n >= 0.13: sc += 14; flags.append("OBV_RISING")
    elif obv_n >= 0.05: sc += 6
    if vc >= 1.55: sc += 20; flags.append("VC_STRONG")
    elif vc >= 1.35: sc += 14; flags.append("VC_MOD")
    elif vc >= 1.22: sc += 8
    if liq >= 1.40: sc += 15; flags.append("LIQ_FAST")
    elif liq >= 1.15: sc += 10; flags.append("LIQ_EXP")
    elif liq >= 0.95: sc += 5
    if 5 <= r60d <= 60: sc += 20; flags.append("UP_NOT_PAR")
    elif 60 < r60d <= 100: sc += 10
    elif 0 <= r60d < 5: sc += 12
    elif -15 <= r60d < 0: sc += 8
    if -3 <= r5 <= 8: sc += 10
    elif 8 < r5 <= 15: sc += 6
    if 20 <= range_pos <= 85: sc += 5
    if disaster: sc *= 0.4
    if r60d > 100 or (r30 > 60 and r5 > 15): sc *= 0.5
    return min(sc, 100), flags


def main():
    section("1) Force-deploy pre-pump v2")
    src = open("aws/lambdas/justhodl-pre-pump-detector/source/lambda_function.py").read()
    log("  source: " + str(len(src)) + " chars")

    markers = [
        "v2.0 (calibrated thresholds)",
        "TIER_A_BREAKING",
        "OBV_STRONG_ACCUM",
        "UPTREND_NOT_PARABOLIC",
    ]
    for m in markers:
        log("    " + ("✓" if m in src else "❌") + " " + m)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=buf.getvalue())
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log("  ✓ deployed at " + str(c["LastModified"]))

    section("2) Smoke invoke")
    t0 = time.time()
    r = L2.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                   LogType="Tail", Payload=b"{}")
    log("  status: " + str(r["StatusCode"]) + ", dur: " + "{:.1f}".format(time.time()-t0) + "s")
    body = json.loads(r["Payload"].read())
    log("  body: " + json.dumps(body)[:300])
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-10:]:
            log("    " + ln.rstrip())

    section("3) Today's top 15 v2 signals")
    obj = S3.get_object(Bucket=BUCKET, Key="data/pre-pump-signals.json")
    d = json.loads(obj["Body"].read())
    log("  generated_at: " + str(d.get("generated_at")))
    log("  stats: " + json.dumps(d.get("stats", {})))
    log("")
    log("  ── top 15 ──")
    for c in d.get("summary", {}).get("top_25_overall", [])[:15]:
        log("    {:<6} {:>5.1f} {:<22}  obv={:>+5.2f}  vc={:>4.2f}  liq={:>4.2f}  r60d={:>+5.0f}%  r30d={:>+5.0f}%".format(
            c["symbol"], c["score"], c["tier"],
            c["obv_slope"], c["vol_comp"], c["liq_expand"],
            c["ret_60d"], c["ret_30d"]))

    section("4) HISTORICAL BACKTEST v2 — pump-list catch capability")
    log("  At various days BEFORE each name's breakout, what would v2 score?")
    log("")

    PUMP_LIST = [("ICHR", 138), ("INTC", 122), ("LITE", 116), ("CRDO", 101), ("MRVL", 130)]

    for sym, total in PUMP_LIST:
        h = fetch_h(sym)
        if not h or len(h) < 200:
            log("  " + sym + " — no history")
            continue
        bi = find_breakout_idx(h)
        if bi is None:
            log("  " + sym + " — no clear breakout (smooth uptrend)")
            continue
        bd = h[bi]["date"]
        log("  " + sym + " (final +" + str(total) + "%, breakout " + bd + "):")
        caught = False
        for offset_days in [60, 45, 30, 20, 10, 5]:
            idx = bi - offset_days
            if idx < 120:
                continue
            window = h[:idx+1]
            score, flags = v2_score_at(window)
            if score is None:
                continue
            current = window[-1]["close"]
            baseline = h[bi]["close"]
            future_pump = (baseline / current - 1) * 100
            tier = "TIER_A_BREAKING" if score >= 70 else ("TIER_B_BUILDING" if score >= 55 else ("WATCH" if score >= 40 else "MARGINAL"))
            mark = "🎯" if score >= 55 else "  "
            if score >= 55:
                caught = True
            log("    {} {} ({} days before breakout): {:>5.1f} {:<18} future_to_breakout=+{:>5.0f}% flags={}".format(
                mark, window[-1]["date"], offset_days, score, tier, future_pump, ",".join(flags[:4])))
        log("    {}".format("✓ CAUGHT (>= TIER_B at some point)" if caught else "❌ MISSED"))
        log("")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log("FATAL: " + str(e))
        for ln in traceback.format_exc().splitlines():
            log("    " + ln)
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_q_prepump_v2_backtest.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
