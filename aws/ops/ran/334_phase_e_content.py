#!/usr/bin/env python3
"""Step 334 — Capture actual Phase E content (Fed Speak + Global Macro) in brief."""
import json
import os
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
REPORT = "aws/ops/reports/334_phase_e_content.json"
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/morning-brief-latest.json")
    data = json.loads(obj["Body"].read())

    out["s3_keys"] = list(data.keys())
    text = data.get("text") or data.get("message") or ""
    out["text_length"] = len(text)
    out["has_fed_section"]    = "🏦 Fed Speak" in text
    out["has_global_section"] = "🌍 Global Macro" in text

    # Extract excerpts
    for marker, key in (("🏦 Fed Speak", "fed_section"),
                          ("🌍 Global Macro", "global_section")):
        idx = text.find(marker)
        if idx >= 0:
            # Find end of section (next bold marker or end of text)
            end_markers = ["💼 Paper Portfolio", "🧪 A/B Test", "🔄 What Changed", "Auto-generated"]
            end_idx = len(text)
            for em in end_markers:
                eidx = text.find(em, idx + 1)
                if 0 < eidx < end_idx:
                    end_idx = eidx
            out[key] = text[idx:end_idx].rstrip()
        else:
            out[key] = "NOT FOUND"

    # Pull telemetry
    info_str = data.get("info", "{}")
    try:
        if isinstance(info_str, str):
            info = json.loads(info_str)
        else:
            info = info_str
        out["telegram_send"] = {
            "ok": info.get("ok"),
            "message_id": info.get("result", {}).get("message_id"),
            "date_utc": info.get("result", {}).get("date"),
        }
    except Exception:
        out["telegram_info_raw"] = str(info_str)[:300]

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str, ensure_ascii=False)
    print(json.dumps(out, indent=2, default=str, ensure_ascii=False)[:6000])


if __name__ == "__main__":
    main()
