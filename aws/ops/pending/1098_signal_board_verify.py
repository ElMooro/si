"""1098 — invoke signal-board + confirm Auction Crisis is in the unified output."""
import json, pathlib, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1098_signal_board.json"
lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=180))
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Invoke
    print("[1098] invoking signal-board…")
    t0 = time.time()
    r = lam.invoke(FunctionName="justhodl-signal-board",
                    InvocationType="RequestResponse", Payload=b"{}")
    out["elapsed_s"] = round(time.time() - t0, 1)
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        out["invoke_status_code"] = p.get("statusCode")
        if isinstance(p.get("body"), str):
            try:
                inner = json.loads(p["body"])
                out["invoke_summary"] = inner
            except Exception:
                out["body_preview"] = p["body"][:300]
    except Exception:
        out["raw"] = body[:500]
    
    # Read signal-board.json
    print("[1098] reading data/signal-board.json…")
    time.sleep(2)
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/signal-board.json")
    d = json.loads(obj["Body"].read())
    out["board_meta"] = {
        "size_kb":       round(obj["ContentLength"]/1024, 1),
        "last_modified": obj["LastModified"].isoformat(),
        "generated_at":  d.get("generated_at"),
        "n_engines":     len(d.get("engines") or []),
    }
    
    # Find the auction-crisis entry
    engines = d.get("engines") or []
    auction_entry = None
    for e in engines:
        nm = (e.get("name") or "").lower()
        if "auction crisis" in nm:
            auction_entry = e
            break
    if auction_entry:
        out["auction_crisis_entry"] = auction_entry
        out["auction_crisis_found"] = True
    else:
        out["auction_crisis_found"] = False
        out["all_engine_names"] = [e.get("name") for e in engines]
    
    # Show overall composite posture
    out["board_composite"]  = d.get("composite")
    out["board_posture"]    = d.get("posture")
    out["board_signal_sum"] = d.get("signal_sum")
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1098] DONE")


if __name__ == "__main__":
    main()
