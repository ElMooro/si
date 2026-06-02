"""1224 — Redeploy justhodl-page-ai-commentary with 10 page configs + invoke + verify."""
import json
import os
import time
import zipfile
import io
import urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1224_ai_pages_10_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-page-ai-commentary"
SOURCE_DIR = "aws/lambdas/justhodl-page-ai-commentary/source"
REGION = "us-east-1"
ALL_PAGES = [
    # Original 5
    "pre-pump-radar", "signal-board", "risk-desk", "liquidity", "fundamentals",
    # New 5
    "crisis", "signals", "portfolio", "13f", "screener",
]

cfg = Config(read_timeout=900, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(SOURCE_DIR):
            for f in files:
                if f.startswith("__") or f.endswith(".pyc"):
                    continue
                fpath = os.path.join(root, f)
                rel = os.path.relpath(fpath, SOURCE_DIR)
                zf.write(fpath, arcname=rel)
    return buf.getvalue()


# 1. Update Lambda code
print(f"[1224] 1. Update {LAMBDA} with 10-page config")
try:
    zip_bytes = build_zip()
    lam.update_function_code(FunctionName=LAMBDA, ZipFile=zip_bytes)
    for _ in range(15):
        time.sleep(2)
        c = lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("LastUpdateStatus") == "Successful":
            break
    lam.update_function_configuration(FunctionName=LAMBDA, Timeout=420, MemorySize=512)
    out["update"] = "ok"
    print(f"  ✓ updated")
except Exception as e:
    out["update_err"] = str(e)[:300]
    print(f"  ❌ {e}")

# 2. Invoke (generates commentary for all 10 pages)
print(f"\n[1224] 2. Invoke (will generate commentary for 10 pages)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["invoke"] = {"elapsed_s": elapsed, "status": resp.get("StatusCode"),
                      "function_error": resp.get("FunctionError"),
                      "body": payload[:2500]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:600]}")
    else:
        try:
            inner = json.loads(json.loads(payload).get("body", "{}"))
            print(f"  pages_generated: {inner.get('pages_generated')}")
            for p, r in (inner.get("results") or {}).items():
                status = "✓" if r.get("has_content") else "⚠"
                print(f"    {status} {p:18s}: keys={r.get('keys')}")
        except Exception as e:
            print(f"  parse_err: {e}")
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}

# 3. Read all 10 commentaries
print(f"\n[1224] 3. Read all 10 page commentaries")
out["commentaries"] = {}
for page in ALL_PAGES:
    try:
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=f"data/ai-commentary/{page}.json")["Body"].read())
        comm = doc.get("commentary", {})
        score_keys = ["confidence_score", "regime_score", "risk_score", "liquidity_score",
                       "value_score", "crisis_score", "signal_score", "portfolio_score",
                       "conviction_score", "screener_score"]
        score = next((comm.get(k) for k in score_keys if comm.get(k) is not None), None)
        out["commentaries"][page] = {
            "generated_at": doc.get("generated_at"),
            "has_content": "error" not in comm,
            "headline": (comm.get("headline") or "")[:250],
            "score": score,
        }
        ok = "✓" if out["commentaries"][page]["has_content"] else "⚠"
        print(f"  {ok} {page:18s} score={score:>3} {out['commentaries'][page]['headline'][:90]}")
    except Exception as e:
        out["commentaries"][page] = {"error": str(e)[:120]}
        print(f"  ✗ {page:18s} {e}")

# 4. Verify GitHub Pages deployment (try fetching each)
print(f"\n[1224] 4. Verify 10 pages deployed with panels")
out["deployments"] = {}
for page in ALL_PAGES:
    page_file = "pre-pump-radar.html" if page == "pre-pump-radar" else \
                ("screener/" if page == "screener" else f"{page}.html")
    url = f"https://justhodl.ai/{page_file}"
    try:
        req = urllib.request.Request(url, headers={"Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=12) as r:
            html = r.read().decode()
        has_panel = "ai-brief-panel" in html
        has_page_var = f'__AI_BRIEF_PAGE__ = "{page}"' in html
        out["deployments"][page] = {
            "url": url, "size_kb": round(len(html) / 1024, 1),
            "has_panel": has_panel, "has_page_var": has_page_var,
        }
        status = "✓" if (has_panel and has_page_var) else "⚠"
        print(f"  {status} {page:18s} {out['deployments'][page]['size_kb']:>6.1f} KB  panel={has_panel} var={has_page_var}")
    except Exception as e:
        out["deployments"][page] = {"error": str(e)[:120]}
        print(f"  ⚠ {page:18s} {e}")

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1224] DONE")
