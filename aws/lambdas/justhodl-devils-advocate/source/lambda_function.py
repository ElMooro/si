"""justhodl-devils-advocate — argues the BEAR case against the top setups, using
the user's OWN avoid-list and hard-learned lessons from the Brain. Protects
against confirmation bias and the user's known blind spots.

Reads data/best-setups.json (the conviction board) + data/brain.json (the user's
directive: avoid-list, hard_rules, lessons). For each top setup, asks Claude to
make the strongest disciplined bear case — citing the actual signal data AND
flagging when a setup violates one of the user's own stated rules.

OUTPUT: data/devils-advocate.json  ·  SCHEDULE: every 6h (after best-setups).
"""
import json, time, os
import urllib.request
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/devils-advocate.json"
MODEL = "claude-haiku-4-5-20251001"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "") or os.environ.get("ANTHROPIC_API_KEY", "")
s3 = boto3.client("s3", region_name=REGION)


def read_json(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default


def call_claude(system, prompt, max_tokens=1400):
    if not ANTHROPIC_KEY:
        return None
    try:
        body = {"model": MODEL, "max_tokens": max_tokens, "system": system,
                "messages": [{"role": "user", "content": prompt}]}
        req = urllib.request.Request("https://api.anthropic.com/v1/messages",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01"})
        r = json.loads(urllib.request.urlopen(req, timeout=40).read().decode())
        return "".join(b.get("text", "") for b in r.get("content", []) if b.get("type") == "text")
    except Exception as e:
        print(f"[devils] claude err: {str(e)[:80]}")
        return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    bs = read_json("data/best-setups.json") or {}
    brain = read_json("data/brain.json") or {}
    directive = brain.get("directive") or {}

    # take the strongest setups (where over-confidence is most dangerous)
    setups = (bs.get("top_setups") or [])[:12]
    if not setups:
        out = {"engine": "devils-advocate", "generated_at": datetime.now(timezone.utc).isoformat(), "cases": [], "note": "no setups"}
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(), ContentType="application/json")
        return {"statusCode": 200, "body": "no setups"}

    slim = [{"ticker": s.get("ticker"), "verdict": s.get("verdict"), "conviction": s.get("conviction"),
             "signals": s.get("signal_keys"), "thesis": (s.get("thesis") or "")[:200]} for s in setups]

    system = (
        "You are the DEVIL'S ADVOCATE on an investing desk. For each setup the system is bullish on, "
        "make the strongest DISCIPLINED bear case — the reasons it could fail or is a trap. Be "
        "specific and fair, not reflexively negative. Use the signal data given. CRUCIALLY, the "
        "investor's own avoid-list, hard rules, and lessons are provided — if a setup VIOLATES one "
        "of their own rules, flag it explicitly (this is the highest-value warning). "
        "Return STRICT JSON only: {\"cases\":[{\"ticker\":\"X\",\"bear_case\":\"<2-3 sentence strongest "
        "counter-argument citing data>\",\"violates_your_rule\":\"<the user's own rule it breaks, or null>\","
        "\"risk_level\":\"<low|medium|high — how much this should temper conviction>\"}]}"
    )
    prompt = (f"INVESTOR'S OWN RULES/AVOID-LIST/LESSONS:\n{json.dumps({'hard_rules': directive.get('hard_rules'), 'avoid': directive.get('avoid'), 'risk_posture': directive.get('risk_posture')}, default=str)}\n\n"
              f"SETUPS THE SYSTEM IS BULLISH ON:\n{json.dumps(slim, default=str)}\n\n"
              "Make the bear case for each. Return the JSON.")
    raw = call_claude(system, prompt)
    cases = []
    if raw:
        import re
        cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip())
        try:
            cases = (json.loads(cleaned) or {}).get("cases", [])
        except Exception as e:
            print(f"[devils] parse err: {str(e)[:60]}")

    by_ticker = {c.get("ticker"): c for c in cases if c.get("ticker")}
    out = {"engine": "devils-advocate", "version": "1.0",
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "cases": cases, "by_ticker": by_ticker,
           "n_rule_violations": sum(1 for c in cases if c.get("violates_your_rule")),
           "note": "Bear case for the top setups, checked against your own rules."}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[devils] {len(cases)} bear cases, {out['n_rule_violations']} rule violations")
    return {"statusCode": 200, "body": json.dumps({"n_cases": len(cases)})}
