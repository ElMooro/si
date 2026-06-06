"""justhodl-my-brief — 'What changed for ME today.' Filters the whole system
through the user's Brain directive: surfaces ONLY what matters given their
themes, sector tilts, watchlist, and rules — everything generic suppressed.

Reads best-setups, brain (directive), bond-vol, funding-plumbing, crypto-risk,
catalyst-calendar. Asks Claude to write a tight, personalized brief in the
user's own frame. OUTPUT: data/my-brief.json · SCHEDULE: daily 13:30 UTC.
"""
import json, time, os
import urllib.request
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/my-brief.json"
MODEL = "claude-haiku-4-5-20251001"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "") or os.environ.get("ANTHROPIC_API_KEY", "")
s3 = boto3.client("s3", region_name=REGION)


def rj(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default


def claude(system, prompt, mx=1100):
    if not ANTHROPIC_KEY: return None
    try:
        body = {"model": MODEL, "max_tokens": mx, "system": system, "messages": [{"role": "user", "content": prompt}]}
        req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01"})
        r = json.loads(urllib.request.urlopen(req, timeout=40).read().decode())
        return "".join(b.get("text", "") for b in r.get("content", []) if b.get("type") == "text")
    except Exception as e:
        print(f"[my-brief] err {str(e)[:70]}"); return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    brain = rj("data/brain.json") or {}
    directive = brain.get("directive")
    if not directive:
        out = {"engine": "my-brief", "generated_at": datetime.now(timezone.utc).isoformat(),
               "brief": None, "note": "Add notes to your Brain to get a personalized brief."}
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(), ContentType="application/json")
        return {"statusCode": 200, "body": "no directive"}

    bs = rj("data/best-setups.json") or {}
    ctx = {
        "your_directive": directive,
        "your_aligned_setups": (bs.get("brain_aligned") or [])[:10],
        "top_setups": [{"t": s.get("ticker"), "verdict": s.get("verdict"), "conv": s.get("conviction"),
                        "signals": s.get("signal_keys")} for s in (bs.get("top_setups") or [])[:15]],
        "bond_vol": (rj("data/bond-vol.json") or {}).get("regime"),
        "funding_plumbing": {k: (rj("data/funding-plumbing.json") or {}).get(k) for k in ["regime", "balance_sheet_direction"]},
        "crypto_dump_risk": {k: (rj("data/crypto-cycle-risk.json") or {}).get(k) for k in ["risk_level", "dump_risk_score"]},
    }
    system = (
        "You write a SHORT personalized market brief for ONE investor, seen entirely through THEIR "
        "worldview (provided as 'your_directive': their themes, sector tilts, rules, risk posture). "
        "Rules: (1) Surface ONLY what's relevant to THEM — their themes, their watched names, their "
        "tilts. Suppress generic noise. (2) Lead with the single most important thing for them today. "
        "(3) If the regime/plumbing conflicts with their posture, say so. (4) Reference their own rules "
        "where they apply. (5) Markdown, tight, scannable, <250 words. (6) Research, not advice. "
        "Write in second person ('you')."
    )
    brief = claude(system, "Write my brief from this:\n" + json.dumps(ctx, default=str)[:11000])
    out = {"engine": "my-brief", "version": "1.0",
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "brief": brief, "context_used": list(ctx.keys())}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=600")
    print(f"[my-brief] done, brief={'yes' if brief else 'no'}")
    return {"statusCode": 200, "body": "ok"}
