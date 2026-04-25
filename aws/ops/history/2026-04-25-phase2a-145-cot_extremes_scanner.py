#!/usr/bin/env python3
"""
Step 145 — Phase 2A: COT positioning extremes scanner.

Phase 2 of the hedge-fund-grade system, opportunity ranking pillar.
Reads CFTC Commitment of Traders data on 29 contracts, percentile-
ranks current positioning vs the 5-year range, flags >95th or <5th
percentile extremes. Historically these extremes mark turning points
60-70% of the time within 30-60 days.

KEY DESIGN DECISIONS:

  1. 5-YEAR LOOKBACK, NOT 8 WEEKS. The existing CFTC agent fetches
     8 weeks per contract — that's enough for slope analysis but
     useless for percentile ranking. We need at least 5 years (260
     weekly reports) to know what's actually 'extreme.'

  2. INCREMENTAL PERSISTENCE. First run fetches 5 years per contract
     (29 × 260 = 7,540 records); subsequent weekly runs only fetch
     the last 2 weeks and merge. Stored at:
       cot/history/{contract}.json — full 5-year time series
       cot/extremes/current.json — today\\'s percentile rankings + flags

  3. WHICH POSITIONING METRIC TO RANK. CFTC publishes 3 report types:
     TFF (financials): Asset Manager + Leveraged Funds
     Disagg (commodities): Managed Money + Producer/Merchant
     Legacy: Non-commercial + Commercial
     We use a UNIFIED 'speculator net positioning' metric:
       speculator_net = long - short (in their respective category)
     Then percentile-rank this single value per contract.

  4. NORMALIZE BY OPEN INTEREST. Raw positioning numbers grow over
     time as markets expand. We rank against 'spec_net / open_int'
     ratio, which is comparable across decades.

  5. ALERT ONLY ON EXTREMES + DIRECTIONAL CONSENSUS. Single-contract
     extremes are noise. Multi-contract extremes within a CATEGORY
     (e.g., gold + silver + copper all >90th pct) is a signal worth
     a Telegram alert. Single extremes go to dashboard only.

OUTPUT:
  s3://justhodl-dashboard-live/cot/history/{contract}.json (5y series, per contract)
  s3://justhodl-dashboard-live/cot/extremes/current.json (today\\'s scan)
  Telegram alert on category-wide extreme consensus

LAMBDA: justhodl-cot-extremes-scanner (512MB arm64, 300s)
SCHEDULE: cron(0 19 ? * FRI *) — Friday 19:00 UTC, ~1h after CFTC
publishes weekly COT report
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)

BUCKET = "justhodl-dashboard-live"


SCANNER_SRC = '''"""
justhodl-cot-extremes-scanner — Phase 2A COT positioning percentile scanner.

Reads/maintains 5-year CFTC COT history per contract, computes percentile
rank of current speculator net positioning, flags >95th / <5th percentile
extremes. Sends Telegram alert when ≥3 contracts in same category cluster
at extreme.
"""
import json
import os
import statistics
import urllib.request
import urllib.parse
import ssl
from datetime import datetime, timezone, timedelta
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
TG_TOKEN_PARAM = "/justhodl/telegram/bot_token"
TG_CHAT_ID_PARAM = "/justhodl/telegram/chat_id"

s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

TFF_URL = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
DISAGG_URL = "https://publicreporting.cftc.gov/resource/72hh-3qpy.json"
LEGACY_URL = "https://publicreporting.cftc.gov/resource/6dca-aqww.json"

# Same contract list as the existing agent; categorized for cluster detection
COT_CONTRACTS = {
    "ES": {"name": "S&P 500 E-Mini",     "cftc_code": "13874A", "category": "equity_index"},
    "NQ": {"name": "NASDAQ 100 E-Mini",  "cftc_code": "209742", "category": "equity_index"},
    "YM": {"name": "Dow Jones E-Mini",   "cftc_code": "124603", "category": "equity_index"},
    "RTY":{"name": "Russell 2000 E-Mini","cftc_code": "239742", "category": "equity_index"},
    "VX": {"name": "VIX Futures",        "cftc_code": "1170E1", "category": "volatility"},
    "ZB": {"name": "30-Year T-Bond",     "cftc_code": "020601", "category": "treasury"},
    "ZN": {"name": "10-Year T-Note",     "cftc_code": "043602", "category": "treasury"},
    "ZF": {"name": "5-Year T-Note",      "cftc_code": "044601", "category": "treasury"},
    "ZT": {"name": "2-Year T-Note",      "cftc_code": "042601", "category": "treasury"},
    "6E": {"name": "Euro FX",            "cftc_code": "099741", "category": "currency"},
    "6J": {"name": "Japanese Yen",       "cftc_code": "097741", "category": "currency"},
    "6B": {"name": "British Pound",      "cftc_code": "096742", "category": "currency"},
    "6C": {"name": "Canadian Dollar",    "cftc_code": "090741", "category": "currency"},
    "6S": {"name": "Swiss Franc",        "cftc_code": "092741", "category": "currency"},
    "DX": {"name": "US Dollar Index",    "cftc_code": "098662", "category": "currency"},
    "CL": {"name": "Crude Oil WTI",      "cftc_code": "067651", "category": "energy"},
    "NG": {"name": "Natural Gas",        "cftc_code": "023651", "category": "energy"},
    "RB": {"name": "RBOB Gasoline",      "cftc_code": "111659", "category": "energy"},
    "HO": {"name": "Heating Oil",        "cftc_code": "022651", "category": "energy"},
    "GC": {"name": "Gold",               "cftc_code": "088691", "category": "metals"},
    "SI": {"name": "Silver",             "cftc_code": "084691", "category": "metals"},
    "HG": {"name": "Copper",             "cftc_code": "085692", "category": "metals"},
    "PL": {"name": "Platinum",           "cftc_code": "076651", "category": "metals"},
    "ZC": {"name": "Corn",               "cftc_code": "002602", "category": "agriculture"},
    "ZS": {"name": "Soybeans",           "cftc_code": "005602", "category": "agriculture"},
    "ZW": {"name": "Wheat",              "cftc_code": "001602", "category": "agriculture"},
    "CT": {"name": "Cotton",             "cftc_code": "033661", "category": "agriculture"},
    "KC": {"name": "Coffee",             "cftc_code": "083731", "category": "agriculture"},
    "SB": {"name": "Sugar",              "cftc_code": "080732", "category": "agriculture"},
}

FINANCIAL_CATS = {"equity_index", "treasury", "currency", "volatility"}

# Percentile thresholds
EXTREME_HIGH_PCT = 95.0   # above this = bullish positioning extreme (often = sell signal)
EXTREME_LOW_PCT = 5.0     # below this = bearish positioning extreme (often = buy signal)
# Min consensus for category alert
MIN_CATEGORY_CLUSTER = 3


def fetch_url(url, timeout=30):
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "JustHodl-COT-Scanner/1.0"}
        )
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.read().decode("utf-8")
    except Exception as e:
        print(f"[FETCH] {url[:100]}: {e}")
        return None


def safe_int(v):
    try:
        if v is None or v == "":
            return 0
        return int(float(str(v).replace(",", "")))
    except Exception:
        return 0


def fetch_history(contract_key, contract_info, weeks=260):
    """Fetch up to N weeks of weekly COT for one contract.
    Returns list of {report_date, spec_net, open_int, ratio} sorted oldest→newest."""
    cftc_code = contract_info["cftc_code"]
    category = contract_info["category"]
    is_financial = category in FINANCIAL_CATS

    # CFTC publishes weekly; pull the last `weeks` reports
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(weeks=weeks + 4)).strftime("%Y-%m-%dT00:00:00.000")

    where = f"cftc_contract_market_code='{cftc_code}' AND report_date_as_yyyy_mm_dd > '{cutoff_iso}'"
    params = urllib.parse.urlencode({
        "$where": where,
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": weeks + 8,
    })

    # Try TFF first for financial contracts, disagg for physicals, legacy as fallback
    sources = ([TFF_URL, DISAGG_URL, LEGACY_URL] if is_financial
               else [DISAGG_URL, LEGACY_URL, TFF_URL])

    records = None
    report_type = None
    for url in sources:
        data = fetch_url(f"{url}?{params}")
        if data:
            try:
                parsed = json.loads(data)
                if parsed and len(parsed) > 5:  # need meaningful sample
                    records = parsed
                    if url == TFF_URL: report_type = "tff"
                    elif url == DISAGG_URL: report_type = "disagg"
                    else: report_type = "legacy"
                    break
            except json.JSONDecodeError:
                continue

    if not records:
        return [], None

    history = []
    for rec in records:
        rd = rec.get("report_date_as_yyyy_mm_dd", "")[:10]
        if not rd:
            continue
        # Spec net positioning by report type
        if report_type == "tff":
            # Asset Manager + Leveraged Funds combined as speculators
            am_long  = safe_int(rec.get("asset_mgr_positions_long_all"))
            am_short = safe_int(rec.get("asset_mgr_positions_short_all"))
            lf_long  = safe_int(rec.get("lev_money_positions_long_all"))
            lf_short = safe_int(rec.get("lev_money_positions_short_all"))
            spec_net = (am_long + lf_long) - (am_short + lf_short)
        elif report_type == "disagg":
            # Managed Money is the standard speculator
            mm_long = safe_int(rec.get("m_money_positions_long_all"))
            mm_short = safe_int(rec.get("m_money_positions_short_all"))
            spec_net = mm_long - mm_short
        else:  # legacy
            nc_long = safe_int(rec.get("noncomm_positions_long_all"))
            nc_short = safe_int(rec.get("noncomm_positions_short_all"))
            spec_net = nc_long - nc_short

        oi = safe_int(rec.get("open_interest_all"))
        ratio = (spec_net / oi) if oi > 0 else 0.0
        history.append({
            "date": rd,
            "spec_net": spec_net,
            "open_int": oi,
            "ratio": round(ratio, 5),
        })

    # Sort oldest → newest, dedupe by date
    seen = set()
    history = sorted(history, key=lambda x: x["date"])
    deduped = []
    for h in history:
        if h["date"] not in seen:
            seen.add(h["date"])
            deduped.append(h)
    return deduped, report_type


def percentile_rank(values, x):
    """What percentile is `x` at, given the distribution `values`?
    Returns 0-100. Uses sample-rank percentile (not interpolation)."""
    if not values:
        return None
    n = len(values)
    below = sum(1 for v in values if v < x)
    equal = sum(1 for v in values if v == x)
    pct = (below + 0.5 * equal) / n * 100
    return round(pct, 1)


def get_telegram_creds():
    try:
        token = ssm.get_parameter(Name=TG_TOKEN_PARAM, WithDecryption=True)["Parameter"]["Value"]
        chat_id = ssm.get_parameter(Name=TG_CHAT_ID_PARAM)["Parameter"]["Value"]
        return token, chat_id
    except Exception as e:
        print(f"[TG-CREDS] {e}")
        return None, None


def send_telegram(message):
    token, chat_id = get_telegram_creds()
    if not token or not chat_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": chat_id, "text": message, "parse_mode": "Markdown",
        }).encode("utf-8")
        req = urllib.request.Request(url, data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception as e:
        print(f"[TG] {e}")
        return False


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return default


def put_s3_json(key, body, cache="public, max-age=3600"):
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(body, default=str).encode("utf-8"),
        ContentType="application/json", CacheControl=cache,
    )


def maintain_history(contract_key, contract_info, full_refresh=False):
    """Read existing history, fetch updates, merge, save."""
    key = f"cot/history/{contract_key}.json"
    existing = get_s3_json(key)

    if full_refresh or not existing or not existing.get("history"):
        # Initial full fetch
        print(f"  {contract_key}: full 5-year fetch")
        history, rt = fetch_history(contract_key, contract_info, weeks=260)
        if not history:
            return None
        out = {
            "contract": contract_key,
            "name": contract_info["name"],
            "category": contract_info["category"],
            "report_type": rt,
            "history": history,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        put_s3_json(key, out, cache="public, max-age=3600")
        return out

    # Incremental update: fetch last 4 weeks and merge
    new_recent, rt = fetch_history(contract_key, contract_info, weeks=4)
    if not new_recent:
        return existing
    by_date = {h["date"]: h for h in existing["history"]}
    for h in new_recent:
        by_date[h["date"]] = h  # overwrite if existing
    merged = sorted(by_date.values(), key=lambda x: x["date"])
    # Trim to ~5 years (260 weeks plus buffer)
    if len(merged) > 280:
        merged = merged[-280:]
    existing["history"] = merged
    existing["report_type"] = rt or existing.get("report_type")
    existing["last_updated"] = datetime.now(timezone.utc).isoformat()
    put_s3_json(key, existing, cache="public, max-age=3600")
    return existing


def lambda_handler(event, context):
    print("=== COT EXTREMES SCANNER v1 ===")
    now = datetime.now(timezone.utc)

    # Allow forced full refresh via event payload (for first run)
    full_refresh = bool(event and event.get("full_refresh"))
    only_contract = (event or {}).get("contract")

    contracts = COT_CONTRACTS
    if only_contract:
        contracts = {only_contract: COT_CONTRACTS[only_contract]} if only_contract in COT_CONTRACTS else {}

    snapshots = []
    fetch_errors = 0
    for ck, info in contracts.items():
        try:
            data = maintain_history(ck, info, full_refresh=full_refresh)
            if not data or not data.get("history"):
                fetch_errors += 1
                continue
            history = data["history"]
            n = len(history)
            if n < 26:  # need ≥6 months for any meaningful percentile
                snapshots.append({
                    "contract": ck, "name": info["name"], "category": info["category"],
                    "status": "insufficient_history", "n_weeks": n,
                })
                continue

            # Use the ratio (spec_net / open_int) for percentile ranking —
            # this normalizes for market growth over time
            ratios = [h["ratio"] for h in history if h.get("ratio") is not None]
            current = history[-1]
            cur_ratio = current["ratio"]
            cur_spec_net = current["spec_net"]
            cur_oi = current["open_int"]

            pct = percentile_rank(ratios[:-1], cur_ratio)  # exclude self
            if pct is None:
                snapshots.append({"contract": ck, "name": info["name"],
                                  "status": "rank_failed"})
                continue

            extreme = "high" if pct >= EXTREME_HIGH_PCT else (
                      "low" if pct <= EXTREME_LOW_PCT else None)

            # Trend indicator: ratio change over last 4 weeks
            if n >= 5:
                prev_ratio = history[-5]["ratio"]
                trend_4w = round(cur_ratio - prev_ratio, 5)
            else:
                trend_4w = None

            snapshots.append({
                "contract": ck,
                "name": info["name"],
                "category": info["category"],
                "report_type": data.get("report_type"),
                "status": "ok",
                "current_ratio": cur_ratio,
                "spec_net": cur_spec_net,
                "open_int": cur_oi,
                "report_date": current.get("date"),
                "percentile": pct,
                "extreme": extreme,
                "trend_4w": trend_4w,
                "n_weeks_history": n,
            })

            print(f"  {ck:5} pct={pct:5.1f} ratio={cur_ratio:+.4f} extreme={extreme}")
        except Exception as e:
            print(f"  {ck}: ERROR {e}")
            fetch_errors += 1

    # Cluster detection: any category with ≥3 extremes in same direction?
    clusters = {}
    for s in snapshots:
        if s.get("extreme") not in ("high", "low"):
            continue
        cat = s.get("category")
        key = f"{cat}:{s['extreme']}"
        clusters.setdefault(key, []).append(s["contract"])

    cluster_alerts = []
    for key, contracts_in_cluster in clusters.items():
        cat, direction = key.split(":")
        if len(contracts_in_cluster) >= MIN_CATEGORY_CLUSTER:
            cluster_alerts.append({
                "category": cat,
                "direction": direction,
                "contracts": contracts_in_cluster,
                "n": len(contracts_in_cluster),
            })

    # Build summary snapshot
    sorted_by_extreme = sorted(
        [s for s in snapshots if s.get("status") == "ok"],
        key=lambda x: abs(x.get("percentile", 50) - 50),
        reverse=True,
    )

    n_extreme = sum(1 for s in snapshots if s.get("extreme") in ("high", "low"))

    snapshot = {
        "as_of": now.isoformat(),
        "v": "1.0",
        "summary": {
            "n_contracts_total": len(COT_CONTRACTS),
            "n_processed": len([s for s in snapshots if s.get("status") == "ok"]),
            "n_errors": fetch_errors,
            "n_extreme": n_extreme,
            "n_cluster_alerts": len(cluster_alerts),
        },
        "cluster_alerts": cluster_alerts,
        "contracts": sorted_by_extreme,
        "thresholds": {
            "extreme_high_pct": EXTREME_HIGH_PCT,
            "extreme_low_pct": EXTREME_LOW_PCT,
            "min_category_cluster": MIN_CATEGORY_CLUSTER,
        },
    }

    put_s3_json("cot/extremes/current.json", snapshot, cache="public, max-age=900")

    # Telegram only on cluster alerts (rare events)
    if cluster_alerts:
        lines = ["📊 *CFTC COT Cluster Extreme*\\n"]
        for a in cluster_alerts:
            direction_text = "BULLISH (>95th pct)" if a["direction"] == "high" else "BEARISH (<5th pct)"
            lines.append(
                f"• *{a['category']}*: {a['n']} contracts at {direction_text}\\n"
                f"  Contracts: {', '.join(a['contracts'])}\\n"
            )
        lines.append("\\n_Speculator positioning extremes typically mark turning points within 30-60 days_")
        message = "\\n".join(lines)
        sent = send_telegram(message)
        snapshot["alert_sent"] = sent
        print(f"  Cluster alert sent: {sent}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "n_processed": snapshot["summary"]["n_processed"],
            "n_extreme": n_extreme,
            "n_cluster_alerts": len(cluster_alerts),
            "top_extremes": [
                {"contract": s["contract"], "pct": s.get("percentile"),
                 "extreme": s.get("extreme")}
                for s in sorted_by_extreme[:5]
                if s.get("extreme") in ("high", "low")
            ],
        }),
    }
'''


with report("build_cot_extremes_scanner") as r:
    r.heading("Phase 2A — COT Positioning Extremes Scanner")

    # ─── 1. Set up Lambda ───────────────────────────────────────────────
    r.section("1. Set up justhodl-cot-extremes-scanner Lambda")
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-cot-extremes-scanner/source"
    src_dir.mkdir(parents=True, exist_ok=True)
    (src_dir / "lambda_function.py").write_text(SCANNER_SRC)

    import ast
    try:
        ast.parse(SCANNER_SRC)
        r.ok(f"  Wrote source: {len(SCANNER_SRC):,}B, {SCANNER_SRC.count(chr(10))} LOC")
    except SyntaxError as e:
        r.fail(f"  Syntax: {e}")
        if hasattr(e, "lineno"):
            lines = SCANNER_SRC.split("\n")
            for i in range(max(0, e.lineno - 3), min(len(lines), e.lineno + 3)):
                marker = " >>> " if i == e.lineno - 1 else "     "
                r.log(f"  {marker}L{i+1}: {lines[i][:200]}")
        raise SystemExit(1)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        info = zipfile.ZipInfo("lambda_function.py")
        info.external_attr = 0o644 << 16
        zout.writestr(info, SCANNER_SRC)
    zbytes = buf.getvalue()

    fname = "justhodl-cot-extremes-scanner"
    role_arn = "arn:aws:iam::857687956942:role/lambda-execution-role"

    try:
        lam.get_function(FunctionName=fname)
        lam.update_function_code(
            FunctionName=fname, ZipFile=zbytes, Architectures=["arm64"],
        )
        lam.get_waiter("function_updated").wait(
            FunctionName=fname, WaiterConfig={"Delay": 3, "MaxAttempts": 30},
        )
        r.ok(f"  Updated existing {fname}")
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zbytes},
            Description="Phase 2A — COT positioning percentile extremes scanner",
            Timeout=300,  # 5 years × 29 contracts is slow; first run could be 2-3 min
            MemorySize=512,
            Architectures=["arm64"],
            Environment={"Variables": {}},
        )
        lam.get_waiter("function_active_v2").wait(
            FunctionName=fname, WaiterConfig={"Delay": 3, "MaxAttempts": 30},
        )
        r.ok(f"  Created {fname}")

    # ─── 2. First run — full 5-year fetch ───────────────────────────────
    r.section("2. First run — full 5-year fetch (this takes 2-3 minutes)")
    time.sleep(3)
    invoke_start = time.time()
    resp = lam.invoke(
        FunctionName=fname,
        InvocationType="RequestResponse",
        Payload=json.dumps({"full_refresh": True}).encode("utf-8"),
    )
    elapsed = time.time() - invoke_start
    payload = resp.get("Payload").read().decode()
    if resp.get("FunctionError"):
        r.fail(f"  FunctionError ({elapsed:.1f}s): {payload[:1000]}")
        raise SystemExit(1)
    r.ok(f"  Initial 5-year fetch completed in {elapsed:.1f}s")

    try:
        outer = json.loads(payload)
        body = json.loads(outer.get("body", "{}"))
        r.log(f"\n  Response body:")
        for k, v in body.items():
            if isinstance(v, list):
                r.log(f"    {k}: {len(v)} items")
                for item in v[:5]:
                    r.log(f"      {item}")
            else:
                r.log(f"    {k:25} {v}")
    except Exception:
        r.log(f"  Raw payload: {payload[:600]}")

    # ─── 3. Read cot/extremes/current.json ──────────────────────────────
    r.section("3. Read cot/extremes/current.json — full report")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="cot/extremes/current.json")
        snap = json.loads(obj["Body"].read().decode("utf-8"))
        s = snap.get("summary", {})
        r.log(f"  Processed: {s.get('n_processed')}/{s.get('n_contracts_total')}")
        r.log(f"  Errors: {s.get('n_errors')}")
        r.log(f"  At >95th or <5th percentile: {s.get('n_extreme')}")
        r.log(f"  Category cluster alerts: {s.get('n_cluster_alerts')}")

        if snap.get("cluster_alerts"):
            r.log(f"\n  Category clusters:")
            for ca in snap["cluster_alerts"]:
                r.log(f"    {ca['category']:15} {ca['direction']:5} ({ca['n']}): {', '.join(ca['contracts'])}")

        r.log(f"\n  Top 10 most-extreme contracts (by deviation from 50th pct):")
        for c in snap.get("contracts", [])[:10]:
            if c.get("status") == "ok":
                pct = c.get("percentile", 50)
                ext = c.get("extreme") or "—"
                ext_marker = f" ← {ext.upper()} EXTREME" if ext in ("high", "low") else ""
                trend = c.get("trend_4w") or 0
                trend_arrow = "↑" if trend > 0.005 else ("↓" if trend < -0.005 else "→")
                r.log(f"    {c['contract']:5} {c['name']:25} pct={pct:5.1f} "
                      f"ratio={c['current_ratio']:+.4f} 4w={trend_arrow}{ext_marker}")
    except Exception as e:
        r.warn(f"  Read extremes: {e}")

    # ─── 4. Verify history files were created ───────────────────────────
    r.section("4. Verify cot/history/ — sample 3 contracts")
    for ck in ("ES", "GC", "CL"):
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=f"cot/history/{ck}.json")
            data = json.loads(obj["Body"].read().decode("utf-8"))
            history = data.get("history", [])
            if history:
                r.log(f"    {ck}: {len(history)} weekly bars, {history[0]['date']} → {history[-1]['date']}")
        except Exception as e:
            r.warn(f"    {ck}: {e}")

    # ─── 5. Schedule weekly Friday 19:00 UTC ────────────────────────────
    r.section("5. Schedule cron(0 19 ? * FRI *) — Friday post-CFTC publish")
    rule_name = "justhodl-cot-extremes-scanner-weekly"
    try:
        try:
            existing = events.describe_rule(Name=rule_name)
            r.log(f"  Rule exists: {existing['State']} {existing.get('ScheduleExpression')}")
        except events.exceptions.ResourceNotFoundException:
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="cron(0 19 ? * FRI *)",
                State="ENABLED",
                Description="Phase 2A — Friday 19:00 UTC after CFTC weekly COT publish",
            )
            r.ok(f"  Created rule cron(0 19 ? * FRI *)")
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "1",
                      "Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{fname}"}],
        )
        try:
            lam.add_permission(
                FunctionName=fname,
                StatementId=f"{rule_name}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rule_name}",
            )
            r.ok(f"  Added invoke permission")
        except lam.exceptions.ResourceConflictException:
            r.log(f"  Permission already exists")
    except Exception as e:
        r.fail(f"  Schedule: {e}")

    r.kv(
        zip_size=len(zbytes),
        invoke_s=f"{elapsed:.1f}",
        n_processed=body.get("n_processed"),
        n_extreme=body.get("n_extreme"),
        n_cluster_alerts=body.get("n_cluster_alerts"),
    )
    r.log("Done")
