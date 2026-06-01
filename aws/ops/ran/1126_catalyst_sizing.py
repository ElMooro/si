"""1126 — verify catalyst-aware sizing.

Invoke positioning Lambda (which now uses catalysts + clusters),
then read the aggressive basket and show:
  - catalyst grade per position
  - catalyst multiplier applied
  - cluster multiplier applied
  - base_combined vs final combined_score
  - suggested_additions
"""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1126_catalyst_sizing.json"
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=350))
s3  = boto3.client("s3", region_name="us-east-1")


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
    except Exception as e:
        out["phases"].append({"name": name, "status": "ERROR", "error": str(e)[:300],
                                "traceback": traceback.format_exc()[:1200]})


def invoke(fn_name: str) -> dict:
    t0 = time.time()
    r = lam.invoke(FunctionName=fn_name, InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        if isinstance(p.get("body"), str):
            try: return {"elapsed_s": elapsed, "summary": json.loads(p["body"])}
            except Exception: return {"elapsed_s": elapsed, "body": p["body"][:600]}
        return {"elapsed_s": elapsed, "p": str(p)[:600]}
    except Exception: return {"elapsed_s": elapsed, "raw": body[:800]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}

    phase(out, "invoke_positioning", lambda: invoke("justhodl-pump-positioning"))

    def read_basket():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/pump-positioning.json")
        d = json.loads(obj["Body"].read())
        pathlib.Path("aws/ops/reports/1126_positioning_full.json").write_text(
            json.dumps(d, indent=2, default=str))
        agg = d.get("aggressive_basket") or {}
        return {
            "size_kb":        round(obj["ContentLength"]/1024, 1),
            "n_positions":    agg.get("n_positions"),
            "total_exposure": agg.get("total_exposure"),
            "cash_pct":       agg.get("cash_pct"),
            "positions":      agg.get("positions", []),
            "suggested_additions": agg.get("suggested_additions", []),
            "philosophy":     (agg.get("philosophy", "")[:300]),
            "construction_rules": agg.get("construction_rules"),
        }
    phase(out, "read_basket", read_basket)

    # Also invoke the brief to make sure downstream works
    phase(out, "invoke_brief", lambda: invoke("justhodl-pump-radar-brief"))

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1126] DONE")


if __name__ == "__main__":
    main()
