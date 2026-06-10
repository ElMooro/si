# ops 1571 — corrected acceptance: row schema is {engine, category, signal, signal_label, read, as_of, stale}
import json, boto3
s3 = boto3.client("s3", region_name="us-east-1")
d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/signal-board.json")["Body"].read())
eng = d.get("engines") or []
NEW = ["Ignition Pre-Pump", "Bottleneck Boom", "Crisis Canaries", "Liquidity Inflection",
       "Confluence Net-Breadth", "Crisis-KB Match", "EU Dump Radar", "S&P Inclusion Watch"]
rows = {}
for n in NEW:
    rows[n] = next(({k: e.get(k) for k in ("engine","category","signal","signal_label","read","stale")}
                    for e in eng if isinstance(e, dict) and e.get("engine") == n), "MISSING")
out = {"ops": 1571, "n_engines": d.get("n_engines"), "n_live": d.get("n_live"),
       "n_stale": d.get("n_stale"), "composite_signal": d.get("composite_signal"),
       "composite_posture": d.get("composite_posture"), "categories": d.get("categories"),
       "new_rows": rows,
       "stale_engines": [e.get("engine") for e in eng if isinstance(e, dict) and e.get("stale")]}
open("aws/ops/reports/1571_board_accept2.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"new": rows, "stale": out["stale_engines"][:12]}, default=str)[:1500])
