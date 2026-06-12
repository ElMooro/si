"""
justhodl-ask-desk v1.0 — RAG over the entire desk, with citations
=================================================================
One question in -> two-stage agentic retrieval:
  1) ROUTER (haiku): sees the engine manifest (433 feeds) + special sources,
     picks up to 6 data keys relevant to the question.
  2) FETCH: each key is loaded from S3 and slimmed (scalars kept, arrays
     truncated) to a budgeted context.
  3) ANSWER (sonnet): answers ONLY from the provided sources; every factual
     claim must carry a [data/...json] citation; absent data is said plainly.
Exposed via Lambda Function URL (CORS) with a soft x-desk-key header.
Every Q/A is archived to data/_askdesk/ for audit.
"""
import json, os, time, urllib.request
from datetime import datetime, timezone
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DESK_KEY = os.environ.get("DESK_KEY", "")
MODELS_FAST = ["claude-haiku-4-5-20251001", "claude-sonnet-4-6"]
MODELS_SMART = ["claude-sonnet-4-6", "claude-haiku-4-5-20251001"]
EXTRA_SOURCES = [
    ("data/signal-board.json", "the 80-engine signal board (one-line read per engine)"),
    ("data/research-papers.json", "AI research-paper library index (debated theses)"),
    ("data/stock-valuations.json", "S&P valuations + HP scores + underlooked boards"),
    ("data/backtest-harness.json", "walk-forward backtests + live signal grades"),
    ("data/meta-labeler.json", "gatekeeper model + pending TAKE/SKIP verdicts"),
    ("data/crisis-canaries.json", "30-canary crisis composite"),
    ("data/crisis-knowledge-base.json", "1091-rule crisis knowledge base"),
    ("data/transcripts-index.json", "index of past work-session transcripts"),
]
ROUTER_SYS = (
  "You are the retrieval router for a financial-intelligence desk. Given a question "
  "and a catalog of data feeds, pick the MINIMAL set (1-6) of feed keys most likely "
  "to contain the answer. Respond ONLY with JSON: {\"keys\": [\"data/...\"], "
  "\"why\": \"one line\"}. Prefer specific engine feeds over broad ones.")
ANSWER_SYS = (
  "You are the senior analyst of the JustHodl desk answering an internal question. "
  "STRICT RULES: use ONLY the provided sources; after EVERY factual claim cite the "
  "source key in square brackets like [data/crisis-canaries.json]; if the sources "
  "don't contain something, say 'not in desk data' rather than guessing; numbers "
  "must appear verbatim in a source; be concise and direct; this is research, not "
  "advice.")
VERSION = "1.0.0"


def claude(system, user, models, max_tokens=1100):
    last = None
    for model in models:
        try:
            payload = json.dumps({"model": model, "max_tokens": max_tokens,
                                   "system": system,
                                   "messages": [{"role": "user", "content": user}]}).encode()
            req = urllib.request.Request(
                "https://api.anthropic.com/v1/messages", data=payload,
                headers={"Content-Type": "application/json", "x-api-key": ANTHROPIC_KEY,
                          "anthropic-version": "2023-06-01"})
            r = json.loads(urllib.request.urlopen(req, timeout=100).read())
            return ("".join(b.get("text", "") for b in r.get("content", [])
                             if b.get("type") == "text").strip(), model)
        except Exception as e:
            last = e
    raise RuntimeError(f"claude failed: {str(last)[:80]}")


def slim(obj, depth=0):
    if isinstance(obj, dict):
        out = {}
        for k, v in list(obj.items())[:40]:
            if isinstance(v, (str, int, float, bool)) or v is None:
                out[k] = v if not isinstance(v, str) else v[:300]
            elif depth < 2:
                out[k] = slim(v, depth + 1)
        return out
    if isinstance(obj, list):
        return [slim(x, depth + 1) for x in obj[:8]]
    return str(obj)[:200]


def fetch_slim(key):
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        return json.dumps(slim(d), default=str)[:5200]
    except Exception as e:
        return json.dumps({"error": f"feed unavailable: {str(e)[:60]}"})


def catalog():
    lines = [f"{k} — {desc}" for k, desc in EXTRA_SOURCES]
    try:
        man = json.loads(S3.get_object(Bucket=BUCKET,
                          Key="data/engine-manifest.json")["Body"].read())
        for e in (man.get("engines") or [])[:430]:
            ks = e.get("keys") or []
            if ks:
                lines.append(f"{ks[0]} — {e['engine'].replace('justhodl-', '')}")
    except Exception:
        pass
    seen, out = set(), []
    for l_ in lines:
        k = l_.split(" — ")[0]
        if k not in seen:
            seen.add(k)
            out.append(l_)
    return out


def answer_question(q):
    t0 = time.time()
    cat = catalog()
    rt, rmodel = claude(ROUTER_SYS,
                         "QUESTION: " + q[:600] + "\n\nFEED CATALOG:\n" + "\n".join(cat),
                         MODELS_FAST, 500)
    if rt.startswith("```"):
        rt = rt.strip("`")
        if rt.lower().startswith("json"):
            rt = rt[4:]
    try:
        route = json.loads(rt.strip())
    except Exception:
        route = {"keys": ["data/signal-board.json"], "why": "router parse fallback"}
    keys = [k for k in (route.get("keys") or []) if str(k).startswith("data/")][:6]
    if not keys:
        keys = ["data/signal-board.json"]
    src_blobs = []
    for k in keys:
        src_blobs.append(f"=== SOURCE {k} ===\n{fetch_slim(k)}")
    ans, amodel = claude(ANSWER_SYS,
                          "QUESTION: " + q[:600] + "\n\n" + "\n\n".join(src_blobs)
                          + "\n\nAnswer with citations now.",
                          MODELS_SMART, 1100)
    out = {"answer": ans, "sources_used": keys, "router_why": route.get("why"),
            "models": {"router": rmodel, "answer": amodel},
            "duration_s": round(time.time() - t0, 1), "version": VERSION}
    try:
        S3.put_object(Bucket=BUCKET,
                      Key=f"data/_askdesk/{int(time.time())}.json",
                      Body=json.dumps({"q": q[:600], **out}).encode(),
                      ContentType="application/json")
    except Exception:
        pass
    return out


def lambda_handler(event=None, context=None):
    event = event or {}
    hdrs = {"Access-Control-Allow-Origin": "*",
             "Access-Control-Allow-Headers": "content-type,x-desk-key",
             "Content-Type": "application/json"}
    if event.get("test_question"):                       # direct-invoke test path
        return {"statusCode": 200, "headers": hdrs,
                 "body": json.dumps(answer_question(event["test_question"]), default=str)}
    method = ((event.get("requestContext") or {}).get("http") or {}).get("method", "")
    if method == "OPTIONS":
        return {"statusCode": 200, "headers": hdrs, "body": ""}
    req_hdrs = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    if DESK_KEY and req_hdrs.get("x-desk-key") != DESK_KEY:
        return {"statusCode": 403, "headers": hdrs,
                 "body": json.dumps({"error": "bad desk key"})}
    try:
        body = json.loads(event.get("body") or "{}")
    except Exception:
        body = {}
    q = (body.get("question") or "").strip()
    if not q:
        return {"statusCode": 400, "headers": hdrs,
                 "body": json.dumps({"error": "question required"})}
    try:
        return {"statusCode": 200, "headers": hdrs,
                 "body": json.dumps(answer_question(q), default=str)}
    except Exception as e:
        return {"statusCode": 500, "headers": hdrs,
                 "body": json.dumps({"error": str(e)[:160]})}
