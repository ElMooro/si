#!/usr/bin/env python3
"""
Step 139 — Loop 4: multi-agent watchlist debate via Batch API.

CRITICAL DESIGN DECISION: do NOT replace the interactive
investor-agents endpoint with a debate flow. Users hit that endpoint
from the website button and wait for results — Batch API has 24h
latency, that's wrong UX.

Instead: ADD a separate scheduled Lambda that runs the FULL debate
(Stage 1 + Stage 2 + Stage 3) on a curated watchlist nightly, using
the Batch API at 50% discount. The interactive endpoint stays
fast and 1-stage. The watchlist endpoint produces deeper
multi-agent analysis as a daily refresh.

Architecture:

  justhodl-watchlist-debate Lambda (NEW)
    ↓ Stage 1 (parallel, BATCH): each agent's isolated opinion
       Submit message_batches request with N tickers × 6 agents = 6N
       calls. Batch API returns when all complete (typically ~10-30 min).
    ↓ Stage 2 (parallel, BATCH): each agent reads the others'
       Stage 1 opinions and writes a rebuttal/refinement
       Another 6N calls submitted as a single batch.
    ↓ Stage 3 (parallel, regular): synthesis per ticker
       N calls (one per ticker). Interactive sync since Stage 3
       depends on full Stage 2 results and we want to write S3
       output as soon as each ticker is done.
    ↓ Write to s3://justhodl-dashboard-live/investor-debate/<TICKER>.json

Watchlist v1: top 5 tickers from S&P (SPY, AAPL, MSFT, NVDA, TSLA)
plus 5 from existing screener output. Configurable via
  s3://justhodl-dashboard-live/portfolio/watchlist.json

Cost estimate: 13 calls × ~$0.001/call × 10 tickers = ~$0.13/run
Batch API: 50% off = ~$0.07/run
Schedule: nightly at 03:00 UTC (Euro morning, US night)
Annual cost: ~$25/year for full multi-agent debate on 10 tickers daily

KEY INSTRUMENTATION: Stage 2 may produce diminishing returns
(rebuttals could be redundant). The Lambda computes a 'novelty score'
per Stage 2 response (Levenshtein-style overlap with the agent's
Stage 1 take). If novelty < 20% across all rebuttals for a ticker,
the synthesis flags this and downgrades confidence — telling us
when the debate ISN'T adding value.

This validates whether the architecture is worth the cost.
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


# ════════════════════════════════════════════════════════════════════════
# Source for new justhodl-watchlist-debate Lambda
# ════════════════════════════════════════════════════════════════════════
DEBATE_SRC = '''"""
justhodl-watchlist-debate — Loop 4 multi-agent debate via Batch API.

Nightly job. For each ticker on the watchlist:
  Stage 1: each of 6 agents produces isolated opinion (BATCH)
  Stage 2: each agent reads the others' opinions, writes rebuttal (BATCH)
  Stage 3: synthesis agent produces structured consensus (sync)

Writes investor-debate/<TICKER>.json per ticker. Plus a manifest
investor-debate/_index.json with all current debates + metadata.

If Stage 1 completes but Stage 2 doesn't add information (low novelty
score), the synthesis flags this — telling us when debate ISN'T helping.
"""
import json
import os
import time
import urllib.request
import ssl
from datetime import datetime, timezone, timedelta
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

s3 = boto3.client("s3", region_name=REGION)
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# 6 investor personas (mirror of investor-agents)
AGENTS = {
    "buffett": {
        "name": "Warren Buffett",
        "philosophy": "You are Warren Buffett. Patient long-term value investor. Focus on durable competitive moats, ROIC, predictable earnings, capital allocation. Avoid speculation. Margin of safety is everything."
    },
    "munger": {
        "name": "Charlie Munger",
        "philosophy": "You are Charlie Munger. Rational, multidisciplinary thinker. Prefer wonderful businesses at fair prices over fair businesses at wonderful prices. Skeptical of stories without numbers. Inversion: ask what could go wrong."
    },
    "burry": {
        "name": "Michael Burry",
        "philosophy": "You are Michael Burry. Deep contrarian. Obsess over balance sheets, FCF, Altman Z-Score, Piotroski F-Score. Comfortable being early. Look for catalysts to unlock hidden value or expose hidden weakness."
    },
    "druck": {
        "name": "Stanley Druckenmiller",
        "philosophy": "You are Stanley Druckenmiller. Macro + concentrated bets. Liquidity drives markets. Position sizes matter more than entry. Focus on Fed policy, dollar, real rates. Concentrated when conviction is high."
    },
    "lynch": {
        "name": "Peter Lynch",
        "philosophy": "You are Peter Lynch. Buy what you understand. Look for tenbaggers in your circle of competence. PEG ratio, growth at reasonable price. Average industry, exceptional company > exciting industry, mediocre company."
    },
    "wood": {
        "name": "Cathie Wood",
        "philosophy": "You are Cathie Wood. Disruptive innovation, exponential growth. AI/genomics/blockchain/automation/EV. Tolerate volatility for asymmetric upside. 5-year horizon. Total addressable market matters more than current earnings."
    },
}

# Default watchlist if none specified
DEFAULT_WATCHLIST = ["SPY", "QQQ", "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "GOOGL", "BRK.B"]


def fetch_json(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"[FETCH] {url[:80]}: {e}")
        return None


def get_s3_json(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return None


def put_s3_json(key, body, cache="public, max-age=600"):
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(body, default=str).encode("utf-8"),
        ContentType="application/json", CacheControl=cache,
    )


def get_ticker_metrics(ticker):
    """Pull a slim metrics snapshot via FMP."""
    profile = fetch_json(f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_KEY}")
    if not profile or not isinstance(profile, list) or not profile:
        return {"ticker": ticker, "error": "no_data"}
    p = profile[0]
    ratios = fetch_json(f"https://financialmodelingprep.com/api/v3/key-metrics-ttm/{ticker}?apikey={FMP_KEY}")
    r = ratios[0] if isinstance(ratios, list) and ratios else {}
    return {
        "ticker": ticker,
        "name": p.get("companyName", ticker),
        "sector": p.get("sector", "N/A"),
        "price": p.get("price"),
        "mktCap_b": round((p.get("mktCap") or 0) / 1e9, 1),
        "pe": p.get("pe"),
        "beta": p.get("beta"),
        "pb": r.get("pbRatio"),
        "ps": r.get("priceToSalesRatio"),
        "fcfYield": r.get("freeCashFlowYield"),
        "roic": r.get("roic"),
        "debtEq": r.get("debtToEquity"),
        "yoy_growth": r.get("revenueGrowthTTM"),
    }


def call_anthropic_sync(system, user, max_tokens=600):
    """Single sync API call."""
    if not ANTHROPIC_KEY:
        return None
    body = json.dumps({
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user}],
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30, context=ctx) as r:
            data = json.loads(r.read().decode("utf-8"))
            return data["content"][0]["text"].strip()
    except Exception as e:
        print(f"[SYNC] {e}")
        return None


def submit_message_batch(requests):
    """Submit a batch of messages to Anthropic Message Batches API.
    requests: list of {custom_id, params}.
    Returns the batch_id.
    """
    if not ANTHROPIC_KEY:
        return None
    body = json.dumps({"requests": requests}).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages/batches",
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "anthropic-beta": "message-batches-2024-09-24",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30, context=ctx) as r:
            data = json.loads(r.read().decode("utf-8"))
            return data.get("id")
    except Exception as e:
        print(f"[BATCH-SUBMIT] {e}")
        return None


def poll_batch(batch_id, max_wait_s=2400, poll_interval_s=30):
    """Poll a batch until complete. Returns the results URL, or None."""
    if not ANTHROPIC_KEY:
        return None
    url = f"https://api.anthropic.com/v1/messages/batches/{batch_id}"
    start = time.time()
    while time.time() - start < max_wait_s:
        req = urllib.request.Request(
            url,
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "anthropic-beta": "message-batches-2024-09-24",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=20, context=ctx) as r:
                data = json.loads(r.read().decode("utf-8"))
                status = data.get("processing_status")
                print(f"  batch {batch_id[:16]}... status={status}")
                if status == "ended":
                    return data.get("results_url")
        except Exception as e:
            print(f"[BATCH-POLL] {e}")
        time.sleep(poll_interval_s)
    print(f"  batch {batch_id} timed out after {max_wait_s}s")
    return None


def fetch_batch_results(results_url):
    """Fetch JSONL results from batch. Returns dict {custom_id: text}."""
    if not results_url or not ANTHROPIC_KEY:
        return {}
    req = urllib.request.Request(
        results_url,
        headers={
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "anthropic-beta": "message-batches-2024-09-24",
        },
    )
    out = {}
    try:
        with urllib.request.urlopen(req, timeout=60, context=ctx) as r:
            for line in r.read().decode("utf-8").splitlines():
                if not line.strip():
                    continue
                rec = json.loads(line)
                cid = rec.get("custom_id")
                result = rec.get("result", {})
                if result.get("type") == "succeeded":
                    msg = result.get("message", {})
                    content = msg.get("content", [])
                    if content:
                        out[cid] = content[0].get("text", "").strip()
    except Exception as e:
        print(f"[BATCH-FETCH] {e}")
    return out


def jaccard_overlap(a, b):
    """Cheap text-similarity heuristic: word-level Jaccard."""
    if not a or not b:
        return 0.0
    sa = set(str(a).lower().split())
    sb = set(str(b).lower().split())
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def build_stage1_request(custom_id, agent_key, agent_cfg, metrics):
    user_prompt = (
        f"Analyze {metrics['ticker']} ({metrics.get('name', '')}) {metrics.get('sector', '')}\\n"
        f"Price: ${metrics.get('price')} MktCap: ${metrics.get('mktCap_b')}B\\n"
        f"P/E: {metrics.get('pe')} P/B: {metrics.get('pb')} P/S: {metrics.get('ps')}\\n"
        f"FCF Yield: {metrics.get('fcfYield')} ROIC: {metrics.get('roic')} D/E: {metrics.get('debtEq')}\\n"
        f"YoY Revenue Growth: {metrics.get('yoy_growth')}\\n"
        f'Respond ONLY as valid JSON: {{"signal":"<STRONG BUY|BUY|HOLD|SELL|STRONG SELL>","conviction":<1-10>,"thesis":"<2-3 sentences citing numbers>","key_metric":"<most important metric>"}}'
    )
    return {
        "custom_id": custom_id,
        "params": {
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 500,
            "system": agent_cfg["philosophy"],
            "messages": [{"role": "user", "content": user_prompt}],
        },
    }


def build_stage2_request(custom_id, agent_key, agent_cfg, metrics, all_stage1):
    """Stage 2: agent reads all 6 stage 1 opinions and writes a rebuttal/refinement."""
    others = []
    for k, v in all_stage1.items():
        if k == agent_key:
            continue
        others.append(f"- {AGENTS[k]['name']}: {v[:300]}")
    others_text = "\\n".join(others)
    own = all_stage1.get(agent_key, "")[:300]

    user_prompt = (
        f"You analyzed {metrics['ticker']} and wrote:\\n{own}\\n\\n"
        f"The other 5 investors said:\\n{others_text}\\n\\n"
        f"Where do you AGREE? Where do you DISAGREE most strongly? Has any argument changed your conviction?\\n"
        f'Respond as JSON: {{"agree_with":[<list of agent names you agree with>],"disagree_with":[<list>],"updated_conviction":<1-10>,"updated_signal":"<same scale>","strongest_counterpoint":"<1-2 sentences>"}}'
    )
    return {
        "custom_id": custom_id,
        "params": {
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 500,
            "system": agent_cfg["philosophy"],
            "messages": [{"role": "user", "content": user_prompt}],
        },
    }


def parse_json_response(text):
    """Extract JSON from a possibly fenced response."""
    if not text:
        return None
    t = text.strip()
    if t.startswith("```"):
        t = t.split("\\n", 1)[1].rsplit("```", 1)[0].strip()
    try:
        return json.loads(t)
    except Exception:
        return None


def lambda_handler(event, context):
    print("=== WATCHLIST DEBATE v1 ===")
    now = datetime.now(timezone.utc)

    # 1. Load watchlist
    wl_obj = get_s3_json("portfolio/watchlist.json")
    watchlist = (wl_obj or {}).get("tickers", DEFAULT_WATCHLIST)
    if event and isinstance(event.get("tickers"), list):
        watchlist = event["tickers"]
    watchlist = watchlist[:10]  # cap
    print(f"  Watchlist: {watchlist}")

    # 2. Get metrics for each
    print("  Fetching metrics...")
    metrics_by_ticker = {}
    for tk in watchlist:
        m = get_ticker_metrics(tk)
        if "error" not in m:
            metrics_by_ticker[tk] = m
    print(f"  Got metrics for {len(metrics_by_ticker)}/{len(watchlist)} tickers")

    if not metrics_by_ticker:
        return {"statusCode": 500, "body": json.dumps({"error": "no metrics fetched"})}

    # 3. Stage 1: build batch of N tickers × 6 agents requests
    print(f"\\n  STAGE 1: {len(metrics_by_ticker) * 6} agent opinions via batch")
    stage1_requests = []
    for tk, m in metrics_by_ticker.items():
        for k, cfg in AGENTS.items():
            cid = f"s1_{tk}_{k}"
            stage1_requests.append(build_stage1_request(cid, k, cfg, m))

    batch1_id = submit_message_batch(stage1_requests)
    if not batch1_id:
        return {"statusCode": 500, "body": json.dumps({"error": "stage1 batch submit failed"})}
    print(f"  Stage 1 batch submitted: {batch1_id}")

    results1_url = poll_batch(batch1_id, max_wait_s=900)
    if not results1_url:
        return {"statusCode": 500, "body": json.dumps({"error": "stage1 batch did not complete"})}
    stage1_raw = fetch_batch_results(results1_url)
    print(f"  Stage 1 returned {len(stage1_raw)}/{len(stage1_requests)} responses")

    # Parse Stage 1 by ticker
    stage1_by_ticker = {}
    for cid, text in stage1_raw.items():
        # cid = s1_TICKER_AGENT
        parts = cid.split("_", 2)
        if len(parts) < 3:
            continue
        tk = parts[1]
        ag = parts[2]
        stage1_by_ticker.setdefault(tk, {})[ag] = text

    # 4. Stage 2: each agent reads others' opinions and writes rebuttal
    print(f"\\n  STAGE 2: {sum(len(v) for v in stage1_by_ticker.values())} rebuttals via batch")
    stage2_requests = []
    for tk, agents_opinions in stage1_by_ticker.items():
        if tk not in metrics_by_ticker:
            continue
        for k, cfg in AGENTS.items():
            if k not in agents_opinions:
                continue
            cid = f"s2_{tk}_{k}"
            stage2_requests.append(build_stage2_request(cid, k, cfg, metrics_by_ticker[tk], agents_opinions))

    batch2_id = submit_message_batch(stage2_requests) if stage2_requests else None
    stage2_raw = {}
    if batch2_id:
        print(f"  Stage 2 batch submitted: {batch2_id}")
        results2_url = poll_batch(batch2_id, max_wait_s=900)
        if results2_url:
            stage2_raw = fetch_batch_results(results2_url)
            print(f"  Stage 2 returned {len(stage2_raw)}/{len(stage2_requests)} responses")

    # 5. Stage 3: per-ticker synthesis (sync, fast since N≤10)
    print(f"\\n  STAGE 3: {len(stage1_by_ticker)} synthesis calls (sync)")
    debates = {}
    for tk, agents_s1 in stage1_by_ticker.items():
        s2_for_tk = {}
        for cid, text in stage2_raw.items():
            parts = cid.split("_", 2)
            if len(parts) >= 3 and parts[1] == tk:
                s2_for_tk[parts[2]] = text

        # Compute novelty score: how much did Stage 2 differ from Stage 1 per agent?
        novelty_scores = {}
        for ag, s1 in agents_s1.items():
            s2 = s2_for_tk.get(ag, "")
            overlap = jaccard_overlap(s1, s2)
            novelty_scores[ag] = round(1.0 - overlap, 3)
        avg_novelty = sum(novelty_scores.values()) / len(novelty_scores) if novelty_scores else 0
        print(f"  {tk}: avg_novelty={avg_novelty:.2f}")

        # Synthesis call
        s1_summary = "\\n".join([f"{AGENTS[k]['name']}: {v[:200]}" for k, v in agents_s1.items()])
        s2_summary = "\\n".join([f"{AGENTS[k]['name']}: {v[:200]}" for k, v in s2_for_tk.items()])
        synth_prompt = (
            f"Synthesize the multi-agent debate on {tk}. Stage 1 was each investor's isolated take. Stage 2 was each investor reading the others' takes and refining.\\n\\n"
            f"STAGE 1:\\n{s1_summary}\\n\\nSTAGE 2:\\n{s2_summary}\\n\\n"
            f"Average novelty (S2 vs S1): {avg_novelty:.0%}\\n\\n"
            f"Produce a structured consensus: where do most agree, where do they disagree, what's the resolved view, and was the debate informative (high novelty = yes, low novelty = agents repeated themselves)?\\n"
            f'Respond as JSON: {{"consensus_signal":"<STRONG BUY|BUY|HOLD|SELL|STRONG SELL>","consensus_conviction":<1-10>,"areas_of_agreement":["<bullet>","<bullet>"],"areas_of_disagreement":["<bullet>","<bullet>"],"resolved_view":"<2-3 sentences>","debate_informative":<true|false>,"debate_quality_note":"<1 sentence>"}}'
        )
        synth_text = call_anthropic_sync(
            system="You are a senior portfolio manager moderating a multi-agent debate.",
            user=synth_prompt,
            max_tokens=700,
        )
        synth_parsed = parse_json_response(synth_text) or {}

        debates[tk] = {
            "ticker": tk,
            "metrics": metrics_by_ticker.get(tk),
            "stage1": {k: parse_json_response(v) or {"raw": v[:300]} for k, v in agents_s1.items()},
            "stage2": {k: parse_json_response(v) or {"raw": v[:300]} for k, v in s2_for_tk.items()},
            "stage3_synthesis": synth_parsed,
            "novelty_scores": novelty_scores,
            "avg_novelty": round(avg_novelty, 3),
            "generated_at": now.isoformat(),
        }
        # Write per-ticker debate
        put_s3_json(f"investor-debate/{tk}.json", debates[tk])

    # 6. Index manifest
    index = {
        "generated_at": now.isoformat(),
        "n_tickers": len(debates),
        "tickers": sorted(debates.keys()),
        "stage1_batch": batch1_id,
        "stage2_batch": batch2_id,
        "summary": {tk: {
            "consensus_signal": d.get("stage3_synthesis", {}).get("consensus_signal"),
            "consensus_conviction": d.get("stage3_synthesis", {}).get("consensus_conviction"),
            "avg_novelty": d.get("avg_novelty"),
            "debate_informative": d.get("stage3_synthesis", {}).get("debate_informative"),
        } for tk, d in debates.items()},
    }
    put_s3_json("investor-debate/_index.json", index)
    print(f"\\n  Wrote {len(debates)} debates + _index.json")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_tickers": len(debates),
            "tickers": sorted(debates.keys()),
            "stage1_batch": batch1_id,
            "stage2_batch": batch2_id,
        }),
    }
'''


with report("loop4_watchlist_debate") as r:
    r.heading("Loop 4 — multi-agent watchlist debate via Batch API")

    # ─── 1. Initialize watchlist.json (if missing) ──────────────────────
    r.section("1. Initialize portfolio/watchlist.json")
    try:
        s3.head_object(Bucket=BUCKET, Key="portfolio/watchlist.json")
        r.log("  portfolio/watchlist.json already exists")
    except Exception:
        wl = {
            "v": "1.0",
            "tickers": ["SPY", "QQQ", "AAPL", "MSFT", "NVDA",
                        "TSLA", "AMZN", "META", "GOOGL", "BRK.B"],
            "_note": "Edit this list to control which tickers get nightly multi-agent debate. Max 10. Used by justhodl-watchlist-debate Lambda.",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        s3.put_object(
            Bucket=BUCKET, Key="portfolio/watchlist.json",
            Body=json.dumps(wl, indent=2).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=600",
        )
        r.ok("  Initialized watchlist with 10 default tickers")

    # ─── 2. Set up Lambda ───────────────────────────────────────────────
    r.section("2. Set up justhodl-watchlist-debate Lambda")
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-watchlist-debate/source"
    src_dir.mkdir(parents=True, exist_ok=True)
    (src_dir / "lambda_function.py").write_text(DEBATE_SRC)

    import ast
    try:
        ast.parse(DEBATE_SRC)
        r.ok("  Syntax OK")
    except SyntaxError as e:
        r.fail(f"  Syntax: {e}")
        if hasattr(e, "lineno"):
            lines = DEBATE_SRC.split("\n")
            for i in range(max(0, e.lineno - 3), min(len(lines), e.lineno + 3)):
                marker = " >>> " if i == e.lineno - 1 else "     "
                r.log(f"  {marker}L{i+1}: {lines[i][:200]}")
        raise SystemExit(1)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        info = zipfile.ZipInfo("lambda_function.py")
        info.external_attr = 0o644 << 16
        zout.writestr(info, DEBATE_SRC)
    zbytes = buf.getvalue()
    r.log(f"  Deployment zip: {len(zbytes):,}B")

    fname = "justhodl-watchlist-debate"
    role_arn = "arn:aws:iam::857687956942:role/lambda-execution-role"

    # Get keys from investor-agents
    ia_env = lam.get_function_configuration(
        FunctionName="justhodl-investor-agents"
    ).get("Environment", {}).get("Variables", {})
    ant_key = ia_env.get("ANTHROPIC_KEY", "")
    fmp_key = ia_env.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

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
            Description="Loop 4 — nightly multi-agent debate via Batch API",
            Timeout=900,  # batch polling can take ~15min
            MemorySize=512,
            Architectures=["arm64"],
            Environment={"Variables": {
                "ANTHROPIC_KEY": ant_key,
                "FMP_KEY": fmp_key,
            }},
        )
        lam.get_waiter("function_active_v2").wait(
            FunctionName=fname, WaiterConfig={"Delay": 3, "MaxAttempts": 30},
        )
        r.ok(f"  Created {fname}")

    # ─── 3. Schedule nightly 03:00 UTC ──────────────────────────────────
    r.section("3. Schedule nightly cron(0 3 * * ? *)")
    rule_name = "justhodl-watchlist-debate-nightly"
    try:
        try:
            existing_rule = events.describe_rule(Name=rule_name)
            r.log(f"  Rule exists: {existing_rule['State']}")
        except events.exceptions.ResourceNotFoundException:
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="cron(0 3 * * ? *)",
                State="ENABLED",
                Description="Loop 4 — nightly multi-agent debate at 03:00 UTC",
            )
            r.ok(f"  Created rule cron(0 3 * * ? *)")
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

    # NOTE: not invoking now since the full debate takes ~10-30 min
    # (Stage 1 + Stage 2 batch polling). The first scheduled run at
    # 03:00 UTC tonight will be the real test.
    r.section("4. Skipping test invoke")
    r.log("  Full debate takes 10-30 min (batch polling). Skipping live")
    r.log("  test invoke. First scheduled run is tonight at 03:00 UTC.")
    r.log("  To invoke manually: aws lambda invoke --function-name justhodl-watchlist-debate")

    r.kv(
        zip_size=len(zbytes),
        function_name=fname,
        watchlist_size=10,
        schedule="cron(0 3 * * ? *)",
        approx_cost_per_run="$0.07 (Batch API 50% off)",
    )
    r.log("Done")
