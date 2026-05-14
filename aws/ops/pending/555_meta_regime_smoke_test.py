#!/usr/bin/env python3
"""555 — Final smoke test: meta-regime stack end-to-end.
   - Sidecar fresh + valid
   - Page renders correctly from justhodl.ai
   - ai-chat returns META-REGIME framing context
   - Homepage has working 🧭 META-REGIME link
"""
import io, json, os, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/555_meta_regime_smoke_test.json"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # 1. Sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/regime-composite.json")
        p = json.loads(obj["Body"].read())
        age = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
        out["sidecar"] = {
            "version": p.get("version"),
            "age_min": round(age, 1),
            "meta_regime": p.get("meta_regime"),
            "composite_score": p.get("composite_score"),
            "n_with_data": p.get("n_modules_with_data"),
            "n_missing": p.get("n_modules_missing"),
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    # 2. Page
    try:
        req = urllib.request.Request("https://justhodl.ai/composite/",
                                       headers={"User-Agent": "JustHodl.AI ops/555"})
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode("utf-8", "replace")
        out["page"] = {
            "size": len(html),
            "loads_composite_json": "regime-composite.json" in html,
            "loads_history_json": "regime-composite-history.json" in html,
            "title": "Meta-Regime" in html,
            "has_dim_grid": "dims-grid" in html,
            "has_mods_grid": "mods-grid" in html,
            "has_methodology": "7-dimension model" in html.lower() or "7 Dimensions" in html,
        }
    except Exception as e:
        out["page_err"] = str(e)[:200]

    # 3. Homepage link
    try:
        req = urllib.request.Request("https://justhodl.ai/",
                                       headers={"User-Agent": "JustHodl.AI ops/555"})
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode("utf-8", "replace")
        out["homepage"] = {
            "size": len(html),
            "has_meta_regime_link": "/composite/" in html and "META-REGIME" in html,
            "has_compass_emoji": "🧭" in html,
        }
    except Exception as e:
        out["homepage_err"] = str(e)[:200]

    # 4. ai-chat META framing
    try:
        # Invoke ai-chat with a market state question; check META block fires
        ssm = boto3.client("ssm", region_name="us-east-1")
        try:
            token = ssm.get_parameter(Name="/justhodl/ai-chat/auth-token",
                                       WithDecryption=True)["Parameter"]["Value"]
        except Exception:
            token = None
        payload = {
            "messages": [
                {"role": "user", "content": "what's the current market state — give me one paragraph"}
            ]
        }
        resp = lam.invoke(FunctionName="justhodl-ai-chat",
                           InvocationType="RequestResponse",
                           Payload=json.dumps(payload).encode("utf-8"))
        body = resp["Payload"].read().decode("utf-8")
        out["ai_chat_invoke_status"] = resp.get("StatusCode")
        out["ai_chat_fn_error"] = resp.get("FunctionError")
        try:
            p = json.loads(body)
            text = p.get("body") if isinstance(p, dict) else None
            if isinstance(text, str):
                try: parsed = json.loads(text)
                except: parsed = {"raw": text}
            else:
                parsed = p
            # Look for evidence the META context was injected
            content = str(parsed)
            out["ai_chat"] = {
                "response_len": len(content),
                "mentions_late_cycle": "late" in content.lower() and "cycle" in content.lower(),
                "mentions_composite": "composite" in content.lower(),
                "mentions_15_modules": "15" in content,
                "response_sample": content[:1200],
            }
        except Exception as e:
            out["ai_chat_parse_err"] = str(e)[:200]
            out["ai_chat_raw"] = body[:1500]
    except Exception as e:
        out["ai_chat_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
