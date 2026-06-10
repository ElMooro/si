# ops 1570 — read-only acceptance: print the 8 new alpha-stack rows from signal-board.engines
import json, boto3
s3 = boto3.client("s3", region_name="us-east-1")
d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/signal-board.json")["Body"].read())
eng = d.get("engines") or []
out = {"ops": 1570, "n_engines": d.get("n_engines"), "n_live": d.get("n_live"),
       "n_stale": d.get("n_stale"), "composite_signal": d.get("composite_signal"),
       "composite_posture": d.get("composite_posture")}
NEW = ["Ignition", "Bottleneck", "Crisis Canaries", "Liquidity Inflection",
       "Confluence", "Crisis-KB", "EU Dump", "Inclusion"]
rows = {}
for n in NEW:
    for e in eng:
        if isinstance(e, dict) and n.lower() in str(e.get("name", "")).lower():
            rows[n] = e
            break
    else:
        rows[n] = "MISSING"
out["new_rows"] = rows
out["stale_names"] = [e.get("name") for e in eng if isinstance(e, dict) and
                      (e.get("stale") or e.get("fresh") is False)][:14]
open("aws/ops/reports/1570_board_accept.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps(out, indent=1, default=str)[:1600])
