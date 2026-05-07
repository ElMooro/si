#!/usr/bin/env python3
"""Step 315 — Verify morning-brief-tg now includes Sector Tilt + Pairs sections."""
import base64
import json
import os
import time
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-morning-brief-tg"
REPORT = "aws/ops/reports/315_morning_brief_phase_d.json"

lam = boto3.client("lambda", region_name=REGION)


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # Wait for deploy-lambdas to finish
        time.sleep(60)

        cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
        out["lambda_last_modified"] = cfg.get("LastModified")

        resp = lam.invoke(
            FunctionName=LAMBDA_NAME,
            InvocationType="RequestResponse",
            Payload=json.dumps({"dryrun": True}).encode(),
            LogType="Tail",
        )
        out["status_code"] = resp.get("StatusCode")
        out["function_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            out["response"] = json.loads(body)
        except Exception:
            out["response_raw"] = body[:500]

        # Decode log tail for the actual brief content
        log_b64 = resp.get("LogResult")
        if log_b64:
            try:
                logs = base64.b64decode(log_b64).decode("utf-8", errors="replace")
                # Look for our new section markers
                indicators = {
                    "sector_tilt_section": "Sector Tilt" in logs,
                    "pairs_section": "Pairs Trading" in logs,
                    "misaligned_buy": "MISALIGNED BUY" in logs,
                    "divergence_section": "Divergence v2" in logs,
                    "claude_call": "💡 Claude" in logs or "Claude:" in logs,
                }
                out["section_markers"] = indicators
                # Capture the relevant log lines
                relevant = [
                    line for line in logs.split("\n")
                    if any(m in line for m in [
                        "Sector Tilt", "Pairs Trading", "MISALIGNED",
                        "Divergence", "Strong OW", "Strong UW", "Trade:",
                        "fetch_json", "💡", "📐", "🎯", "🚨"
                    ])
                ][:25]
                out["relevant_log_lines"] = relevant
            except Exception as e:
                out["log_decode_err"] = str(e)[:200]

        out["duration_s"] = round(time.time() - started, 1)

    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)

    if out.get("fatal_error"):
        print(f"❌ FATAL: {out['fatal_error']}")
        print(out.get("traceback","")[-1500:])
        return

    print()
    print("═" * 70)
    print("  MORNING-BRIEF PHASE D VERIFICATION")
    print("═" * 70)
    print(f"  Lambda last modified: {out.get('lambda_last_modified')}")
    print(f"  Invoke status:        {out.get('status_code')} · err={out.get('function_error')}")
    r = out.get("response", {})
    if isinstance(r, dict):
        body = r.get("body", "")
        if isinstance(body, str):
            try:
                bd = json.loads(body)
                print(f"  Telegram delivered:   ok={bd.get('ok')} · message_id seen in info")
            except Exception:
                print(f"  Body: {body[:200]}")
    markers = out.get("section_markers", {})
    print()
    print("  ── SECTION MARKERS IN BRIEF (if True, section appeared) ──")
    for k, v in markers.items():
        icon = "✅" if v else "❌"
        print(f"    {icon} {k}: {v}")
    print()
    print("  ── RELEVANT LOG LINES (CloudWatch tail, first 15) ──")
    for line in (out.get("relevant_log_lines") or [])[:15]:
        print(f"    {line[:140]}")
    print(f"\n  Total duration: {out.get('duration_s')}s")


if __name__ == "__main__":
    main()
