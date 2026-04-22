#!/usr/bin/env python3
"""
Fix chat-api: trim system prompt below 200k tokens + fix khalid_index
shape access.

Rewrites chat-api's system prompt to include ONLY the current scalar
values for FRED series and stocks (not full nested history), trims
gainers/losers/ath/plays, and extracts khalid_index.score / .regime
properly since the shape changed from scalar to dict.

Deploys directly + re-invokes to verify.
"""

import io
import json
import os
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)


# New chat-api source — completely rewritten to be concise + correct
NEW_CHAT_API_SOURCE = '''import os
import json, urllib.request, urllib.error, ssl

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
BUCKET = "justhodl-dashboard-live"


def _compact(md: dict) -> str:
    """Build a compact context string well under 200k tokens."""
    # Khalid index — dict shape from data/report.json
    ki = md.get("khalid_index")
    if isinstance(ki, dict):
        khalid_score = ki.get("score", "N/A")
        khalid_regime = ki.get("regime", "N/A")
        ki_signals = ki.get("signals", [])[:8]
    else:
        khalid_score = ki if ki is not None else "N/A"
        khalid_regime = md.get("regime", "N/A")
        ki_signals = md.get("signals", [])[:8]

    # FRED — current scalar values only
    fred_items = []
    for k, v in list(md.get("fred", {}).items())[:20]:
        if isinstance(v, dict):
            cur = v.get("current") or v.get("value")
            if cur is not None:
                fred_items.append(f"{k}={cur}")
        else:
            fred_items.append(f"{k}={v}")

    # Stocks — top 15 with price + change only
    stock_items = []
    for k, v in list(md.get("stocks", {}).items())[:15]:
        if isinstance(v, dict):
            px = v.get("price") or v.get("close")
            chg = v.get("chg_pct") or v.get("pct_change")
            if px is not None:
                stock_items.append(f"{k}=${px:.2f}" + (f" ({chg:+.2f}%)" if chg is not None else ""))
        else:
            stock_items.append(f"{k}={v}")

    # Gainers/losers — top 5
    def _fmt_mover(m):
        if isinstance(m, dict):
            sym = m.get("symbol") or m.get("ticker") or "?"
            chg = m.get("chg_pct") or m.get("pct_change") or m.get("change")
            return f"{sym} {chg:+.2f}%" if isinstance(chg, (int, float)) else str(m)
        return str(m)

    gainers = [_fmt_mover(g) for g in (md.get("gainers") or [])[:5]]
    losers  = [_fmt_mover(l) for l in (md.get("losers")  or [])[:5]]

    # Risk + regime
    risk = md.get("risk_dashboard") or {}
    net_liq = md.get("net_liquidity") or {}

    parts = [
        f"KHALID INDEX: {khalid_score}/100 ({khalid_regime})",
        f"Signals: {json.dumps(ki_signals)}",
        f"FRED (current): {', '.join(fred_items)}",
        f"Stocks: {', '.join(stock_items)}",
        f"Gainers: {', '.join(gainers)}",
        f"Losers: {', '.join(losers)}",
        f"Risk: {json.dumps(risk)[:400]}",
        f"Net liquidity: {json.dumps(net_liq)[:200]}",
        f"Generated: {md.get('generated_at', md.get('generated', 'Unknown'))}",
    ]
    return "\\n".join(parts)


def lambda_handler(event, context):
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "POST,OPTIONS",
    }
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": headers, "body": ""}

    try:
        body = json.loads(event.get("body", "{}"))
        messages = body.get("messages", [])

        # Load current market snapshot
        import boto3
        s3 = boto3.client("s3", region_name="us-east-1")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/report.json")
            market_data = json.loads(obj["Body"].read())
        except Exception:
            market_data = {}

        system_prompt = (
            "You are Khalid's personal AI financial advisor for the JustHodl.AI dashboard. "
            "You have access to his live market data (updated every 5 minutes). "
            "Answer in direct, data-driven markdown. Use actual numbers from the context below.\\n\\n"
            "CURRENT MARKET CONTEXT:\\n"
            + _compact(market_data)
        )

        payload = json.dumps({
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 2000,
            "system": system_prompt,
            "messages": messages,
        }).encode()
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
            },
        )
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=30, context=ctx) as r:
            resp = json.loads(r.read())
        reply = resp.get("content", [{}])[0].get("text", "No response")
        return {"statusCode": 200, "headers": headers, "body": json.dumps({"reply": reply})}

    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8", errors="ignore")[:600]
        except Exception:
            pass
        return {"statusCode": 500, "headers": headers, "body": json.dumps({
            "error": f"HTTP {e.code}: {e.reason}",
            "anthropic_detail": detail,
        })}
    except Exception as e:
        return {"statusCode": 500, "headers": headers, "body": json.dumps({"error": str(e)})}
'''


def build_zip(src_dir: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


with report("fix_chat_api_prompt") as r:
    r.heading("Fix chat-api: trim prompt + khalid_index shape")

    path = REPO_ROOT / "aws/lambdas/justhodl-chat-api/source/lambda_function.py"

    r.section("Step 1: replace with trimmed implementation")
    path.write_text(NEW_CHAT_API_SOURCE, encoding="utf-8")
    r.ok(f"  Wrote {len(NEW_CHAT_API_SOURCE)} bytes to {path.relative_to(REPO_ROOT)}")

    r.section("Step 2: deploy")
    zbytes = build_zip(path.parent)
    lam.update_function_code(FunctionName="justhodl-chat-api", ZipFile=zbytes)
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-chat-api", WaiterConfig={"Delay": 3, "MaxAttempts": 20}
    )
    r.ok(f"  Deployed ({len(zbytes)} bytes)")

    r.section("Step 3: re-invoke with real question")
    resp = lam.invoke(
        FunctionName="justhodl-chat-api",
        InvocationType="RequestResponse",
        Payload=json.dumps({
            "httpMethod": "POST",
            "body": json.dumps({
                "messages": [{"role": "user", "content": "What is the current Khalid Index and regime?"}]
            }),
        }).encode(),
    )
    body = resp["Payload"].read().decode("utf-8", errors="ignore")
    fn_err = resp.get("FunctionError")

    if fn_err:
        r.fail(f"  FunctionError: {fn_err}")
        r.log(f"  Body: {body[:400]}")
        r.kv(verdict="FUNCTION_ERROR")
    else:
        try:
            outer = json.loads(body)
            if outer.get("statusCode", 200) >= 400:
                inner = json.loads(outer["body"])
                r.fail(f"  Inner error: {inner}")
                r.kv(verdict="INNER_ERROR", detail=str(inner)[:200])
            else:
                inner = json.loads(outer["body"])
                reply = inner.get("reply", "")
                r.ok(f"  Got reply ({len(reply)} chars)")
                r.log(f"  Reply preview:")
                for line in reply.splitlines()[:15]:
                    r.log(f"    {line[:200]}")
                r.kv(verdict="OK", reply_chars=len(reply))
        except Exception as e:
            r.warn(f"  Couldn't parse response: {e}")
            r.log(f"  Raw: {body[:400]}")

    r.log("Done")
