"""ops 1138 — Additional sniffer seeding for the history chart.

The first seeding pass (ops 1137) wrote 1 snapshot. This op does 5 more
invocations spaced 25s apart, retrying once on Claude-parse failures, so
the chart has ~6 datapoints to render a meaningful trend line.
"""
import json, os, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-ai-brief-router"
BUCKET = "justhodl-dashboard-live"

_cfg = Config(connect_timeout=10, read_timeout=300, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=_cfg)
s3 = boto3.client("s3", region_name=REGION)


def invoke_once():
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                     Payload=json.dumps({"contexts": ["frontrun-sniffer"]}).encode())
    body_resp = json.loads(inv["Payload"].read() or b"{}")
    if isinstance(body_resp, dict) and "body" in body_resp:
        try: body_resp = json.loads(body_resp["body"])
        except Exception: pass
    n_ok = body_resp.get("n_ok") if isinstance(body_resp, dict) else 0
    dur = body_resp.get("duration_s") if isinstance(body_resp, dict) else None
    return n_ok, dur, inv.get("FunctionError")


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat(), "runs": []}
    try:
        # Read history-before
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/frontrun-sniffer-history.json")
            before = json.loads(obj["Body"].read())
            rpt["history_before"] = {"n_snapshots": len(before.get("snapshots") or [])}
        except Exception:
            rpt["history_before"] = {"n_snapshots": 0}

        # 5 invocations, 25s apart, with one retry on n_ok=0
        for i in range(5):
            n_ok, dur, err = invoke_once()
            print(f"[1138] run {i+1}/5: n_ok={n_ok} dur={dur}s err={err}")
            if n_ok == 0 and not err:
                # Retry once with shorter pause
                time.sleep(8)
                n_ok2, dur2, err2 = invoke_once()
                print(f"[1138]   retry: n_ok={n_ok2} dur={dur2}s")
                rpt["runs"].append({"idx": i+1, "n_ok": n_ok, "retry_n_ok": n_ok2, "dur": dur, "retry_dur": dur2})
            else:
                rpt["runs"].append({"idx": i+1, "n_ok": n_ok, "dur": dur})
            if i < 4:
                time.sleep(25)

        # Verify history-after
        time.sleep(3)
        obj = s3.get_object(Bucket=BUCKET, Key="data/frontrun-sniffer-history.json")
        after = json.loads(obj["Body"].read())
        snaps = after.get("snapshots") or []
        stats = after.get("stats_7d") or {}
        rpt["history_after"] = {
            "n_snapshots": len(snaps),
            "snapshots_added": len(snaps) - rpt["history_before"]["n_snapshots"],
            "scores":   [s.get("score") for s in snaps],
            "regimes":  [s.get("regime") for s in snaps],
            "targets":  [s.get("top_setup_asset") for s in snaps],
            "headlines": [(s.get("headline") or "")[:80] for s in snaps],
            "stats_7d": stats,
            "n_events": len(after.get("events") or []),
        }
    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1138.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items() if k != "traceback"}, indent=2, default=str)[:3500])


if __name__ == "__main__":
    main()
