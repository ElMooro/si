"""ops/714 — verify leading-markets is wired into the REAL Khalid Index
(daily-report-v3 -> data/report.json)."""
import json, os, time
import boto3
from datetime import datetime, timezone

BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    report = {"started": datetime.now(timezone.utc).isoformat()}

    # invoke daily-report-v3 (the real Khalid Index writer)
    try:
        r = lam.invoke(FunctionName="justhodl-daily-report-v3",
                        InvocationType="RequestResponse", Payload=b"{}")
        report["invoke"] = {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                            "response": (r["Payload"].read().decode("utf-8", "replace")[:400]
                                         if r.get("Payload") else "")}
    except Exception as e:
        report["invoke"] = {"status": "error", "err": str(e)[:250]}

    time.sleep(5)
    try:
        rep = json.loads(s3.get_object(Bucket=BUCKET, Key="data/report.json")["Body"].read())
        ki = rep.get("khalid_index") or rep.get("ka_index") or {}
        sigs = ki.get("signals") or []
        lead_sig = next((s for s in sigs if isinstance(s, (list, tuple))
                         and "Leading Markets" in str(s[0])), None)
        report["khalid_index"] = {
            "score": ki.get("score"),
            "regime": ki.get("regime"),
            "leading_markets_signal": ki.get("leading_markets_signal"),
            "n_signals": len(sigs),
            "leading_markets_in_signals": lead_sig,
            "wired": (ki.get("leading_markets_signal") is not None) or (lead_sig is not None),
            "other_overlays": {
                "lce_state": ki.get("lce_state"),
                "gbc_global_phase": ki.get("gbc_global_phase"),
                "tenor_state": ki.get("tenor_state"),
            },
        }
    except Exception as e:
        report["khalid_index"] = {"err": str(e)[:250]}

    report["summary"] = {"khalid_index_wired": report.get("khalid_index", {}).get("wired", False)}
    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/714_khalid_index_verify.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 714_khalid_index_verify.json :: " + json.dumps(report["summary"]))


if __name__ == "__main__":
    main()
