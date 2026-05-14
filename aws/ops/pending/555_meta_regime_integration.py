#!/usr/bin/env python3
"""555 — Verify [META-REGIME] block appears in ai-chat live response and
meta_* metrics appear in morning-intel extract_metrics output."""
import io, json, os, time as _time, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/555_meta_regime_integration.json"

lam = boto3.client("lambda", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Wait for ai-chat to settle if mid-update
    for i in range(8):
        try:
            cfg = lam.get_function(FunctionName="justhodl-ai-chat")["Configuration"]
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                out["ai_chat_state"] = cfg.get("State")
                out["ai_chat_last_modified"] = cfg.get("LastModified")
                break
            out[f"ai_wait_{i}"] = {"state": cfg.get("State"), "lus": cfg.get("LastUpdateStatus")}
        except Exception as e:
            out[f"ai_wait_err_{i}"] = str(e)[:120]
        _time.sleep(5)

    # Wait for morning-intel
    for i in range(8):
        try:
            cfg = lam.get_function(FunctionName="justhodl-morning-intelligence")["Configuration"]
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                out["morning_state"] = cfg.get("State")
                out["morning_last_modified"] = cfg.get("LastModified")
                break
            out[f"morn_wait_{i}"] = {"state": cfg.get("State"), "lus": cfg.get("LastUpdateStatus")}
        except Exception as e:
            out[f"morn_wait_err_{i}"] = str(e)[:120]
        _time.sleep(5)

    # Invoke ai-chat with a context probe message that returns context
    try:
        # Invoke with debug=true if supported, else just check the response
        payload = json.dumps({
            "message": "What is the current meta-regime and 7-dimension breakdown?",
            "include_context_debug": True,
        }).encode("utf-8")
        resp = lam.invoke(FunctionName="justhodl-ai-chat", InvocationType="RequestResponse",
                           LogType="Tail", Payload=payload)
        out["ai_chat_invoke_status"] = resp.get("StatusCode")
        out["ai_chat_fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        out["ai_chat_response_size"] = len(body)
        # Extract just the text response to see if meta-regime knowledge surfaced
        try:
            p = json.loads(body)
            inner = json.loads(p.get("body", "{}")) if isinstance(p.get("body"), str) else p
            text = inner.get("response", "") or inner.get("text", "") or ""
            out["ai_chat_response_excerpt"] = text[:1200]
            # Check if META-REGIME appears in response (Claude saw the context)
            out["ai_chat_mentions_meta_regime"] = any(
                tok in text.upper() for tok in ["LATE_CYCLE", "LATE-CYCLE", "META-REGIME", "META REGIME"]
            )
            out["ai_chat_mentions_composite_score"] = "+62" in text or "62." in text or "composite" in text.lower()
        except Exception as e:
            out["ai_chat_parse_err"] = str(e)[:150]
            out["ai_chat_raw"] = body[:800]
        if resp.get("LogResult") and resp.get("FunctionError"):
            out["ai_chat_log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-1500:]
    except Exception as e:
        out["ai_chat_invoke_err"] = str(e)[:200]

    # Invoke morning-intel and capture the extracted metrics
    try:
        resp = lam.invoke(FunctionName="justhodl-morning-intelligence",
                           InvocationType="RequestResponse", LogType="Tail",
                           Payload=json.dumps({"dry_run": True, "return_metrics": True}).encode("utf-8"))
        out["morning_invoke_status"] = resp.get("StatusCode")
        out["morning_fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        out["morning_response_size"] = len(body)
        try:
            p = json.loads(body)
            metrics = None
            if isinstance(p, dict):
                if "metrics" in p: metrics = p["metrics"]
                elif p.get("body"):
                    try: metrics = (json.loads(p["body"]) or {}).get("metrics")
                    except: pass
            if metrics and isinstance(metrics, dict):
                meta_keys = {k: v for k, v in metrics.items() if k.startswith("meta_")}
                out["morning_meta_keys_found"] = meta_keys
            else:
                # Just check the raw response for meta_regime token
                out["morning_response_excerpt"] = body[:800]
                out["morning_has_meta_token"] = "meta_regime" in body
        except Exception as e:
            out["morning_parse_err"] = str(e)[:150]
            out["morning_raw"] = body[:600]
        if resp.get("LogResult") and resp.get("FunctionError"):
            out["morning_log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-1500:]
    except Exception as e:
        out["morning_invoke_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
