"""ops 5711 -- wall-post rehearsal only (Claude, 2026-09-18). Direct lane. No new prepare: settles the owned answers
already submitted for the staged week and rehearses the post against Monday 09:31 ET. Nothing written to the wall."""
import json, sys, time
from pathlib import Path
import boto3
from botocore.config import Config
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402


def main():
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=910, connect_timeout=10, retries={"max_attempts": 0}))
    with report("5711_wall_post_rehearse_only") as r:
        r.heading("ops 5711 -- wall-post rehearsal (no new prepare)")
        receipt = {}
        for i in range(8):
            resp = lam.invoke(FunctionName="justhodl-ai", InvocationType="RequestResponse", Payload=json.dumps({"mode": "wall-post", "body": {"rehearse": True}}).encode())
            receipt = json.loads(resp["Payload"].read())
            owned = [e for e in (receipt.get("entries") or []) if e.get("agent") == "student-owned"]
            pending = [s for s in (receipt.get("skipped") or []) if any(w in str(s.get("reason")) for w in ("queued", "running", "unknown"))]
            r.log("try %d: entries=%d owned=%d skipped=%d pending=%d" % (i + 1, len(receipt.get("entries") or []), len(owned), len(receipt.get("skipped") or []), len(pending)))
            if owned or not pending:
                break
            time.sleep(45)
        for e in receipt.get("entries") or []:
            r.log("  %-14s %-4s %-5s %-10s crisis=%.2f %s" % (e["agent"], e["symbol"], e["direction"], e["regime"], e["crisis_probability"], e["status"]))
        for s_ in receipt.get("skipped") or []:
            r.warn("  skipped %s %s: %s" % (s_["agent"], s_["symbol"], s_["reason"]))
        n_owned = len([e for e in (receipt.get("entries") or []) if e.get("agent") == "student-owned" and e.get("status") == "rehearsed"])
        (r.ok if n_owned else r.fail)("%d owned-model entries pass the door" % n_owned)
        if not n_owned:
            sys.exit(1)


if __name__ == "__main__":
    main()
