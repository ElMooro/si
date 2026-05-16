"""ops/721 — probe the justhodl-outcomes table to see how predicted_dir,
the stored `correct` flag, and the realised `outcome` actually relate.

720 showed many signal types at EXACTLY 0.0 hit-rate over large directional
samples (market_phase 0/302, corr_break_top_pair 0/72). That is impossible
for a merely-bad signal — it is the fingerprint of a broken/inverted
`correct` flag. This probe dumps full sample records for both a KNOWN-GOOD
signal (screener_top_pick, 75%) and several KNOWN-ZERO signals so we can see
the exact field encoding and design a correctness recompute.
"""
import json, os
from collections import defaultdict
import boto3

ddb = boto3.resource("dynamodb", region_name="us-east-1")
table = ddb.Table("justhodl-outcomes")

WANT = ["screener_top_pick", "macro_composite_z", "market_phase",
        "corr_break_top_pair", "crisis_broad_dollar_vs_spy", "momentum_tlt",
        "cot_extreme", "edge_regime"]

report = {"ops": 721, "subject": "outcomes table record-structure probe"}

# scan, collecting up to 4 samples per wanted signal_type
samples = defaultdict(list)
all_keys = set()
kwargs = {}
scanned = 0
while True:
    resp = table.scan(**kwargs)
    for it in resp.get("Items", []):
        scanned += 1
        all_keys.update(it.keys())
        st = it.get("signal_type")
        if st in WANT and len(samples[st]) < 4:
            samples[st].append(it)
    if all(len(samples[s]) >= 4 for s in WANT) or scanned > 60000:
        break
    lek = resp.get("LastEvaluatedKey")
    if not lek:
        break
    kwargs["ExclusiveStartKey"] = lek

report["records_scanned"] = scanned
report["all_top_level_keys_seen"] = sorted(all_keys)

# dump samples (stringify Decimals etc.)
def clean(v):
    if isinstance(v, dict):
        return {k: clean(x) for k, x in v.items()}
    if isinstance(v, list):
        return [clean(x) for x in v]
    try:
        f = float(v)
        return int(f) if f == int(f) else round(f, 4)
    except (TypeError, ValueError):
        return str(v)

report["samples"] = {st: [clean(r) for r in recs] for st, recs in samples.items()}

# for each wanted signal, tabulate predicted_dir vs stored correct vs sign(return)
xtab = {}
for st, recs in samples.items():
    rows = []
    for r in recs:
        oc = r.get("outcome") or {}
        ret = oc.get("return_pct", oc.get("excess_return"))
        try:
            ret = float(ret) if ret is not None else None
        except (TypeError, ValueError):
            ret = None
        rows.append({
            "predicted_dir": str(r.get("predicted_dir") or r.get("predicted_direction")),
            "stored_correct": bool(r.get("correct")),
            "actual_direction": str(oc.get("actual_direction")),
            "return_pct": ret,
            "return_sign": (None if ret is None else ("UP" if ret > 0 else "DOWN")),
        })
    xtab[st] = rows
report["predicted_vs_actual"] = xtab

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/721_outcomes_probe.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/721_outcomes_probe.json")
