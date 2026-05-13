#!/usr/bin/env python3
"""520 — Probe FMP earnings transcripts + verify Anthropic NLP scoring works from Lambda."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/520_earnings_probe.json"
lam = boto3.client("lambda", region_name="us-east-1")

# Try probing with multiple FMP endpoints + Anthropic call
PROBE_CODE = r"""
import json, os, urllib.request, time

FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"  # premium
ANTH_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

def http_get(url, timeout=20):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            return {"status": r.status, "bytes": len(body),
                     "body": body[:3000].decode('utf-8','replace'),
                     "elapsed_ms": int((time.time()-t0)*1000)}
    except urllib.error.HTTPError as e:
        body = e.read()[:500].decode('utf-8','replace') if hasattr(e, 'read') else ''
        return {"status": e.code, "body_preview": body, "elapsed_ms": int((time.time()-t0)*1000)}
    except Exception as e:
        return {"status": "EXC", "err": str(e)[:200]}

def anthropic_score(text):
    if not ANTH_KEY: return {"err": "no ANTHROPIC_API_KEY in env"}
    t0 = time.time()
    prompt = f"""Score the management tone of this earnings call excerpt on a scale of -100 (extremely bearish/cautious/declining) to +100 (extremely bullish/confident/accelerating). Output ONLY valid JSON, no other text.

Excerpt:
{text[:5000]}

Output schema:
{{"management_tone": -100..100, "guidance_direction": "RAISED"|"MAINTAINED"|"LOWERED"|"NONE", "confidence": "LOW"|"MEDIUM"|"HIGH", "key_themes": ["theme1", "theme2", "theme3"], "summary": "one-sentence summary"}}"""
    try:
        body = json.dumps({
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 600,
            "messages": [{"role":"user","content":prompt}]
        }).encode()
        req = urllib.request.Request("https://api.anthropic.com/v1/messages",
            data=body, headers={
                "x-api-key": ANTH_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            })
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.loads(r.read().decode("utf-8"))
            txt = resp.get("content", [{}])[0].get("text", "")
            try:
                # Strip code fences if present
                if "```" in txt:
                    parts = txt.split("```")
                    for p in parts:
                        if p.strip().startswith("{"):
                            txt = p.strip()
                            break
                if txt.startswith("json"): txt = txt[4:].strip()
                parsed = json.loads(txt)
                return {"ok": True, "score": parsed, "elapsed_ms": int((time.time()-t0)*1000),
                          "usage": resp.get("usage")}
            except Exception as e:
                return {"err": f"parse: {e}", "raw": txt[:500],
                          "elapsed_ms": int((time.time()-t0)*1000)}
    except urllib.error.HTTPError as e:
        body = e.read()[:500].decode("utf-8","replace") if hasattr(e,'read') else ''
        return {"err": f"http {e.code}", "body": body}
    except Exception as e:
        return {"err": str(e)[:300]}

def lambda_handler(event, context):
    out = {}

    # Probe 1: AAPL latest transcript via v3 endpoint
    out["aapl_v3_q4_2025"] = http_get(
        f"https://financialmodelingprep.com/api/v3/earning_call_transcript/AAPL?quarter=4&year=2025&apikey={FMP_KEY}")

    # Probe 2: latest transcripts API
    out["latest_transcripts_v4"] = http_get(
        f"https://financialmodelingprep.com/api/v4/earning-call-transcript?symbol=AAPL&apikey={FMP_KEY}")

    # Probe 3: batch transcripts metadata for a year
    out["batch_aapl_2025"] = http_get(
        f"https://financialmodelingprep.com/api/v4/batch_earning_call_transcript/AAPL?year=2025&apikey={FMP_KEY}")

    # Probe 4: earnings calendar (which tickers have upcoming earnings)
    out["earnings_calendar"] = http_get(
        f"https://financialmodelingprep.com/api/v3/earning_calendar?from=2026-05-01&to=2026-05-15&apikey={FMP_KEY}")

    # Probe 5: if transcript came back, try to NLP score a slice of it
    aapl_body = out.get("aapl_v3_q4_2025", {}).get("body", "")
    if aapl_body and len(aapl_body) > 200:
        # Parse the JSON response to get just the content
        try:
            data = json.loads(aapl_body[:2900] + "]") if not aapl_body.endswith("]") else json.loads(aapl_body[:2999])
        except: data = None
        if isinstance(data, list) and data:
            transcript_text = data[0].get("content", "")[:5000]
            out["anthropic_score_test"] = anthropic_score(transcript_text)
        else:
            out["anthropic_score_test"] = {"note": "couldn't parse transcript JSON, trying raw"}
            out["anthropic_score_test"]["result"] = anthropic_score(aapl_body[:5000])

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
"""


def zip_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()


def inherit_env_with_anthropic():
    """Get ANTHROPIC_API_KEY from a known Lambda."""
    for src in ("justhodl-ai-chat", "justhodl-morning-intelligence", "justhodl-investor-agents"):
        try:
            cfg = lam.get_function_configuration(FunctionName=src)
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            if env.get("ANTHROPIC_API_KEY"):
                return {"ANTHROPIC_API_KEY": env["ANTHROPIC_API_KEY"]}
        except Exception: pass
    return {}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    NAME = "justhodl-tmp-earnings-probe"
    env = inherit_env_with_anthropic()
    out["env_inherited"] = bool(env.get("ANTHROPIC_API_KEY"))

    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role="arn:aws:iam::857687956942:role/lambda-execution-role",
            MemorySize=512, Timeout=120, Code={"ZipFile": zip_str(PROBE_CODE)},
            Environment={"Variables": env})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except lam.exceptions.ResourceConflictException:
        lam.update_function_code(FunctionName=NAME, ZipFile=zip_str(PROBE_CODE))
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        lam.update_function_configuration(FunctionName=NAME, Environment={"Variables": env}, Timeout=120)

    _time.sleep(2)
    try:
        r = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                        LogType="Tail", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["result"] = json.loads(p["body"]) if p.get("body") else p
        except: out["raw"] = body[:2500]
        if r.get("LogResult"):
            out["log_tail"] = base64.b64decode(r["LogResult"]).decode("utf-8","replace")[-1500:]
    except Exception as e:
        out["err"] = str(e)[:300]

    try: lam.delete_function(FunctionName=NAME)
    except: pass

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
# trigger workflow
