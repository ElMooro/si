"""
justhodl-meta-improver — Exponential Idea #8 (the moonshot)

The closest thing to making the platform autonomous in improvement.

Algorithm:
  1. Read data/signal-halflife.json (from idea #2)
  2. Identify the WORST engine by edge_status — DECAYED, then DECAYING with
     lowest decay_trend_90d
  3. Skip engines listed in PROTECTED_ENGINES (Khalid's protected list)
  4. Skip engines patched in the last 30 days (cooldown)
  5. Read that engine's source code from GitHub via API
  6. Read its last 100 outcomes from DynamoDB
  7. Claude analyses code + outcomes and proposes ONE specific patch:
       - The hypothesis (why the engine is degrading)
       - The patch (full updated source code)
       - The expected improvement
  8. Create a new branch + commit + PR via GitHub API
  9. Telegram alert with PR link → Khalid reviews

Checkpoints:
  - Only engines with edge_status in {DECAYING, DECAYED} get touched
  - Patched engine name appended to data/meta-improver-state.json
    (30-day cooldown per engine)
  - All patches go through PR — never auto-merged
  - Protected engines (stock-screener, behavior-mirror, calibrators, learning-system)
    NEVER get touched

After 6 months: platform has evolved itself in ways Khalid didn't write.

Schedule: weekly Sunday 22 UTC (after causality-scanner, before week start).
"""
import json, os, logging, urllib.request, urllib.parse, base64, time
import boto3
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
STATE_KEY = "data/meta-improver-state.json"
HIST_KEY = "data/history/meta-improver-history.json"

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO = "ElMooro/si"
GITHUB_API = "https://api.github.com"

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY") or os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

OUTCOMES_TABLE = "justhodl-outcomes"

# Engines that NEVER get touched by the meta-improver
PROTECTED_ENGINES = {
    "justhodl-stock-screener",
    "justhodl-behavior-mirror",
    "justhodl-signal-halflife",
    "justhodl-meta-improver",
    "justhodl-outcome-checker",
    "justhodl-calibrator",
    "justhodl-alpha-calibrator",
    "justhodl-gsi-calibrator",
    "justhodl-opportunity-calibrator",
    "justhodl-calibration-fleet",
    "justhodl-calibration-snapshot",
    "justhodl-signal-logger",
    "justhodl-ab-test",
    "justhodl-prompt-iterator",
    "justhodl-portfolio-manager",
    "justhodl-portfolio-catalysts",
    "justhodl-fleet-error-monitor",
    "justhodl-fleet-freshness-monitor",
    "justhodl-telegram-bot",
    "justhodl-ai-chat",
    "justhodl-api-auth",
    # Don't modify the recently-shipped exponential engines for 90 days
    "justhodl-premortem-engine",
    "justhodl-failure-library",
    "justhodl-causality-scanner",
    "justhodl-convexity-scorer",
    "justhodl-chart-vision",
}

COOLDOWN_DAYS = 30

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)


PATCH_PROMPT = """You are a senior Python engineer fixing a degrading signal in a quantitative trading system.

ENGINE: `{engine_name}`
LAMBDA FUNCTION: `{lambda_name}`

CURRENT PERFORMANCE:
- Peak hit rate: {peak_hit_rate:.1%} (at horizon {peak_horizon_days}d)
- Peak edge above random: {peak_edge:+.1%}
- Status: {edge_status}
- 90-day decay trend: {decay_trend_90d:+.1%}
- Half-life days: {half_life_days}
- Number of signals tracked: {n_signals}

The decay trend tells us the edge has been shrinking over the last 90 days vs the prior period. This signal is being arbed away or the market regime has shifted away from what this engine detects.

RECENT OUTCOMES (last 30 signals from this engine):
{recent_outcomes}

CURRENT SOURCE CODE (full file):
```python
{source_code}
```

YOUR TASK: Propose ONE specific patch to revive this engine's edge.

Common reasons engines decay:
1. Threshold drift — the threshold that was alpha 2 years ago is consensus now
2. Regime change — the engine was tuned for a different macro regime
3. Crowd arb — the same signal is now being acted on by too many funds
4. Upstream data drift — the input distribution has changed
5. Parameter staleness — lookback windows / percentile cutoffs are mistuned

CRITICAL REQUIREMENTS:
- The patch must be SPECIFIC and MINIMAL — under 50 lines changed.
- The patch must explain its causal hypothesis.
- The patch must NOT change the output schema (downstream consumers depend on it).
- Preserve all logging, error handling, and S3 write logic.
- If the engine has hardcoded thresholds, consider making them adaptive (e.g. percentile-based instead of fixed numbers).

OUTPUT JSON ONLY (no markdown):
{{
  "hypothesis": "<1-2 sentences naming the root cause>",
  "patch_summary": "<1 sentence on what changes>",
  "expected_improvement": "<concrete prediction: e.g. 'hit rate should recover to ~58% from current 52%'>",
  "risk_if_wrong": "<what could happen if the patch is misguided>",
  "lines_changed_estimate": <integer>,
  "patched_source_code": "<full new Python file content, ready to commit>"
}}"""


def deci(v):
    if isinstance(v, Decimal): return float(v)
    if isinstance(v, dict):
        if "N" in v: return float(v["N"])
        if "S" in v: return v["S"]
        if "BOOL" in v: return v["BOOL"]
    return v


def github_get(path):
    url = GITHUB_API + path
    req = urllib.request.Request(url, headers={
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "justhodl-meta-improver/1.0",
    })
    try:
        r = urllib.request.urlopen(req, timeout=30)
        return json.loads(r.read())
    except Exception as e:
        logger.warning(f"github_get_fail {path}: {str(e)[:200]}")
        return None


def github_post(path, data, method="POST"):
    url = GITHUB_API + path
    body = json.dumps(data).encode()
    req = urllib.request.Request(
        url, data=body, method=method,
        headers={
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json",
            "User-Agent": "justhodl-meta-improver/1.0",
        })
    try:
        r = urllib.request.urlopen(req, timeout=30)
        return json.loads(r.read())
    except Exception as e:
        logger.warning(f"github_post_fail {path}: {str(e)[:200]}")
        return None


def list_lambdas_in_repo():
    """List all Lambda directories in aws/lambdas/."""
    r = github_get(f"/repos/{GITHUB_REPO}/contents/aws/lambdas")
    if not isinstance(r, list):
        return []
    return [item["name"] for item in r if item.get("type") == "dir"]


def get_lambda_source(lambda_name):
    """Fetch the lambda_function.py source from GitHub."""
    path = f"/repos/{GITHUB_REPO}/contents/aws/lambdas/{lambda_name}/source/lambda_function.py"
    r = github_get(path)
    if not r or "content" not in r:
        # Try alternate path layouts
        path = f"/repos/{GITHUB_REPO}/contents/aws/lambdas/{lambda_name}/lambda_function.py"
        r = github_get(path)
        if not r or "content" not in r:
            return None, None
    try:
        content = base64.b64decode(r["content"]).decode("utf-8")
        return content, r["path"]
    except Exception as e:
        logger.warning(f"decode_fail: {e}")
        return None, None


def get_recent_outcomes_for_signal_type(signal_type, limit=30):
    """Scan outcomes table for last N matching a signal_type."""
    items = []
    last_key = None
    seen = 0
    while seen < 5000:  # cap scan
        kwargs = {"TableName": OUTCOMES_TABLE, "Limit": 1000,
                  "FilterExpression": "signal_type = :s",
                  "ExpressionAttributeValues": {":s": {"S": signal_type}}}
        if last_key: kwargs["ExclusiveStartKey"] = last_key
        try:
            r = ddb.scan(**kwargs)
            for raw in r.get("Items", []):
                items.append({k: deci(v) for k, v in raw.items() if not isinstance(deci(v), dict)})
            last_key = r.get("LastEvaluatedKey")
            seen += 1000
            if not last_key or len(items) >= limit * 3:
                break
        except Exception as e:
            logger.warning(f"outcomes_scan_fail: {e}")
            break
    # Sort by checked_at desc, take latest
    items.sort(key=lambda x: str(x.get("checked_at", "")), reverse=True)
    return items[:limit]


def lambda_name_from_engine(engine):
    """Map an engine signal_type to a Lambda directory name."""
    # Heuristic mappings — most engines named justhodl-<engine_name>
    candidates = [
        f"justhodl-{engine}",
        f"justhodl-{engine.replace('_', '-')}",
        engine,
        engine.replace("_", "-"),
    ]
    repo_lambdas = list_lambdas_in_repo()
    for c in candidates:
        if c in repo_lambdas:
            return c
    # Partial match
    for lam in repo_lambdas:
        if engine.replace("_", "").replace("-", "") in lam.replace("_", "").replace("-", ""):
            return lam
    return None


def load_state():
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read())
    except Exception:
        return {"recent_patches": {}, "all_proposals": []}


def save_state(state):
    s3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                  Body=json.dumps(state, indent=2, default=str).encode(),
                  ContentType="application/json")


def pick_target_engine(state):
    """Read signal-halflife.json, pick worst engine that isn't on cooldown or protected."""
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/signal-halflife.json")["Body"].read())
    except Exception as e:
        logger.warning(f"no_halflife_data: {e}")
        return None
    engines = d.get("engines", {})
    now = datetime.now(timezone.utc)
    cooldown_cutoff = now - timedelta(days=COOLDOWN_DAYS)
    recent_patches = state.get("recent_patches", {})

    # Build candidate list: engines in DECAYED or DECAYING status
    candidates = []
    for name, e in engines.items():
        if e.get("edge_status") not in ("DECAYED", "DECAYING"):
            continue
        # Cooldown check
        last_patched_str = recent_patches.get(name)
        if last_patched_str:
            try:
                last_patched = datetime.fromisoformat(last_patched_str)
                if last_patched > cooldown_cutoff:
                    continue
            except Exception:
                pass
        candidates.append((name, e))

    if not candidates:
        return None

    # Sort by most negative decay trend
    candidates.sort(key=lambda x: x[1].get("decay_trend_90d") or 0)
    target = candidates[0]
    return {"engine": target[0], **target[1]}


def call_claude(prompt, max_tokens=8000):
    if not ANTHROPIC_KEY:
        return None, "no_api_key"
    body = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }
    try:
        req = urllib.request.Request(
            ANTHROPIC_URL, data=json.dumps(body).encode(),
            headers={"x-api-key": ANTHROPIC_KEY,
                     "anthropic-version": "2023-06-01",
                     "Content-Type": "application/json"})
        r = urllib.request.urlopen(req, timeout=120)
        response = json.loads(r.read())
        return response.get("content", [{}])[0].get("text", ""), None
    except Exception as e:
        return None, str(e)[:200]


def extract_json(text):
    if not text: return None
    t = text.strip()
    if t.startswith("```"):
        first_nl = t.find("\n")
        if first_nl > 0: t = t[first_nl+1:]
        last_fence = t.rfind("```")
        if last_fence > 0: t = t[:last_fence]
    start = t.find("{")
    end = t.rfind("}")
    if start < 0 or end <= start: return None
    try: return json.loads(t[start:end+1])
    except Exception: return None


def create_branch_and_pr(lambda_name, source_path, patched_code, hypothesis,
                        patch_summary, expected_improvement, target_engine_data):
    """Create a new branch, commit the patch, open a PR. Returns PR URL or None."""
    if not GITHUB_TOKEN:
        return None, "no_github_token"

    # 1. Get current main SHA
    main_ref = github_get(f"/repos/{GITHUB_REPO}/git/refs/heads/main")
    if not main_ref or "object" not in main_ref:
        return None, "no_main_ref"
    main_sha = main_ref["object"]["sha"]

    # 2. Create new branch
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    branch_name = f"meta-improver/{lambda_name}-{timestamp}"
    branch_payload = {
        "ref": f"refs/heads/{branch_name}",
        "sha": main_sha,
    }
    branch_resp = github_post(f"/repos/{GITHUB_REPO}/git/refs", branch_payload)
    if not branch_resp:
        return None, "branch_create_fail"

    # 3. Get current file SHA (for update)
    file_info = github_get(f"/repos/{GITHUB_REPO}/contents/{source_path}?ref=main")
    if not file_info or "sha" not in file_info:
        return None, "file_sha_fetch_fail"

    # 4. Commit patched file to new branch
    commit_payload = {
        "message": f"meta-improver: patch {lambda_name}\n\n"
                   f"Hypothesis: {hypothesis}\n"
                   f"Patch: {patch_summary}\n"
                   f"Expected: {expected_improvement}",
        "content": base64.b64encode(patched_code.encode()).decode(),
        "sha": file_info["sha"],
        "branch": branch_name,
        "committer": {"name": "justhodl-meta-improver", "email": "meta@justhodl.ai"},
    }
    commit_resp = github_post(f"/repos/{GITHUB_REPO}/contents/{source_path}",
                              commit_payload, method="PUT")
    if not commit_resp:
        return None, "commit_fail"

    # 5. Open PR
    pr_payload = {
        "title": f"meta-improver: patch {lambda_name} (DECAYING edge)",
        "head": branch_name,
        "base": "main",
        "body": f"""## Self-Modifying Engine — Patch Proposal

**Target engine:** `{lambda_name}`

**Current state:**
- Edge status: `{target_engine_data.get('edge_status')}`
- Peak edge: {(target_engine_data.get('peak_edge') or 0) * 100:.1f}%
- 90-day decay trend: {(target_engine_data.get('decay_trend_90d') or 0) * 100:+.1f}%
- Half-life: {target_engine_data.get('half_life_days')} days

**Claude's analysis:**

**Hypothesis:** {hypothesis}

**Patch:** {patch_summary}

**Expected improvement:** {expected_improvement}

---

### ⚠️ Review checklist before merging

- [ ] The patch preserves the output schema (downstream consumers won't break)
- [ ] The hypothesis is plausible given recent market regime
- [ ] No hardcoded credentials or destructive operations introduced
- [ ] The patch is genuinely minimal (under 50 lines changed)
- [ ] If merged, the engine will run side-by-side with v1 for 30 days before promotion

---

_Auto-generated by `justhodl-meta-improver` Lambda (Exponential Idea #8).  
Patched engines are added to cooldown for 30 days regardless of merge decision._
""",
    }
    pr_resp = github_post(f"/repos/{GITHUB_REPO}/pulls", pr_payload)
    if not pr_resp or "html_url" not in pr_resp:
        return None, "pr_open_fail"

    return pr_resp["html_url"], None


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": text,
                           "parse_mode": "Markdown",
                           "disable_web_page_preview": True}).encode()
        req = urllib.request.Request(url, data=data,
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=15)
    except Exception as e:
        logger.error(f"telegram_fail: {e}")


def lambda_handler(event, context):
    started = datetime.now(timezone.utc)
    logger.info("meta-improver starting")

    state = load_state()

    # 1. Pick worst-decaying engine
    target = pick_target_engine(state)
    if not target:
        # Heartbeat so freshness monitor sees the Lambda ran
        state["last_run"] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "status": "no_action",
            "reason": "no_decaying_engines_outside_cooldown",
        }
        save_state(state)
        return {"statusCode": 200,
                "body": json.dumps({"ok": True, "no_action": "no_decaying_engines_outside_cooldown"})}
    target_engine = target["engine"]
    logger.info(f"target: {target_engine} status={target['edge_status']} "
                f"trend={target.get('decay_trend_90d')}")

    # 2. Map to Lambda directory
    lambda_name = lambda_name_from_engine(target_engine)
    if not lambda_name:
        # Update state to skip this engine for cooldown period anyway
        state["recent_patches"][target_engine] = datetime.now(timezone.utc).isoformat()
        save_state(state)
        return {"statusCode": 200, "body": json.dumps({
            "ok": True, "no_action": f"could_not_map_engine_{target_engine}_to_lambda",
        })}
    # 3. Protected check
    if lambda_name in PROTECTED_ENGINES:
        state["recent_patches"][target_engine] = datetime.now(timezone.utc).isoformat()
        save_state(state)
        return {"statusCode": 200, "body": json.dumps({
            "ok": True, "no_action": f"protected_lambda_{lambda_name}",
        })}

    # 4. Read source
    source, source_path = get_lambda_source(lambda_name)
    if not source:
        state["recent_patches"][target_engine] = datetime.now(timezone.utc).isoformat()
        save_state(state)
        return {"statusCode": 200, "body": json.dumps({
            "ok": True, "no_action": f"could_not_read_source_{lambda_name}",
        })}

    # Truncate source if absurdly long (>30k chars)
    if len(source) > 30000:
        logger.warning(f"source too long ({len(source)}), truncating")
        source = source[:30000] + "\n# ... (truncated)"

    # 5. Read recent outcomes
    outcomes = get_recent_outcomes_for_signal_type(target_engine, limit=30)
    recent_summary = "\n".join([
        f"  - {o.get('checked_at', '?')[:10]} "
        f"signal={o.get('signal_value', '?')} "
        f"predicted={o.get('predicted_dir', '?')} "
        f"actual={(o.get('outcome', {}).get('actual_direction') if isinstance(o.get('outcome'), dict) else '?')} "
        f"correct={o.get('correct')}"
        for o in outcomes[:30]
    ]) if outcomes else "  (no recent outcomes found)"

    # 6. Ask Claude for patch
    prompt = PATCH_PROMPT.format(
        engine_name=target_engine,
        lambda_name=lambda_name,
        peak_hit_rate=target.get("peak_hit_rate", 0),
        peak_horizon_days=target.get("peak_horizon_days", 0),
        peak_edge=target.get("peak_edge", 0),
        edge_status=target.get("edge_status", "UNKNOWN"),
        decay_trend_90d=target.get("decay_trend_90d") or 0,
        half_life_days=target.get("half_life_days", "unknown"),
        n_signals=target.get("n_signals", 0),
        recent_outcomes=recent_summary,
        source_code=source,
    )
    response, error = call_claude(prompt)
    if error:
        return {"statusCode": 500, "body": json.dumps({
            "error": "claude_fail", "details": error, "target": target_engine,
        })}
    parsed = extract_json(response)
    if not parsed or "patched_source_code" not in parsed:
        return {"statusCode": 500, "body": json.dumps({
            "error": "parse_fail", "head": (response or "")[:300],
        })}

    # 7. Sanity check: patched code must be Python, not empty, not too different in size
    patched = parsed["patched_source_code"]
    if (not patched or
        "def lambda_handler" not in patched or
        len(patched) < len(source) * 0.3 or
        len(patched) > len(source) * 3):
        return {"statusCode": 500, "body": json.dumps({
            "error": "patch_sanity_check_failed",
            "original_len": len(source),
            "patched_len": len(patched),
        })}

    # 8. Create PR
    pr_url, pr_error = create_branch_and_pr(
        lambda_name=lambda_name,
        source_path=source_path,
        patched_code=patched,
        hypothesis=parsed.get("hypothesis", "unknown"),
        patch_summary=parsed.get("patch_summary", "unknown"),
        expected_improvement=parsed.get("expected_improvement", "unknown"),
        target_engine_data=target,
    )

    # 9. Update state regardless of PR success — to enforce cooldown
    now_iso = datetime.now(timezone.utc).isoformat()
    state["recent_patches"][target_engine] = now_iso
    state.setdefault("all_proposals", []).append({
        "ts": now_iso,
        "engine": target_engine,
        "lambda_name": lambda_name,
        "hypothesis": parsed.get("hypothesis"),
        "patch_summary": parsed.get("patch_summary"),
        "expected_improvement": parsed.get("expected_improvement"),
        "pr_url": pr_url,
        "pr_error": pr_error,
        "source_metrics": {
            "edge_status": target.get("edge_status"),
            "peak_edge": target.get("peak_edge"),
            "decay_trend_90d": target.get("decay_trend_90d"),
        },
    })
    state["all_proposals"] = state["all_proposals"][-200:]
    save_state(state)
    logger.info(f"saved state. pr_url={pr_url} pr_error={pr_error}")

    # 10. Telegram alert
    if pr_url:
        msg = (f"🧬 *Meta-Improver Patch Proposal*\n\n"
               f"Engine: `{target_engine}` (`{lambda_name}`)\n"
               f"Edge status: {target.get('edge_status')}\n"
               f"Decay trend: {(target.get('decay_trend_90d') or 0)*100:+.1f}%\n\n"
               f"*Hypothesis:* {parsed.get('hypothesis', '')[:200]}\n\n"
               f"*Patch:* {parsed.get('patch_summary', '')[:200]}\n\n"
               f"*Expected:* {parsed.get('expected_improvement', '')[:200]}\n\n"
               f"👉 [Review PR]({pr_url})")
    else:
        msg = (f"🧬 *Meta-Improver — Proposal Generated (no PR)*\n\n"
               f"Engine: `{target_engine}`\n"
               f"Reason no PR: {pr_error}\n\n"
               f"*Hypothesis:* {parsed.get('hypothesis', '')[:200]}\n"
               f"*Patch:* {parsed.get('patch_summary', '')[:200]}\n\n"
               f"Full proposal in `data/meta-improver-state.json`")
    try: send_telegram(msg)
    except Exception as e: logger.error(f"telegram_fail: {e}")

    return {"statusCode": 200,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({
                "ok": True,
                "target_engine": target_engine,
                "lambda_name": lambda_name,
                "patch_generated": True,
                "pr_url": pr_url,
                "pr_error": pr_error,
                "hypothesis": parsed.get("hypothesis"),
                "elapsed": round((datetime.now(timezone.utc) - started).total_seconds(), 2),
            })}
