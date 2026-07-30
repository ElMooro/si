"""ops_4130 — artifact anatomy: is ECONOMICS in there at all?"""
import json
import sys
from collections import Counter
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def main():
    with report("4130_artifact_anatomy") as rep:
        rep.heading("ops 4130 — artifact anatomy")
        v = json.loads(s3.get_object(Bucket=BUCKET,
                                     Key="data/tradingview.json")["Body"].read())
        rows = v.get("symbols") or []
        rep.kv(marker=str(v.get("marker"))[:50],
               generated_at=v.get("generated_at"), n=len(rows))
        econ = [r for r in rows
                if str(r.get("symbol", "")).startswith("ECONOMICS")]
        fam = [r for r in rows
               if str(r.get("adapter", "")).startswith("family:")]
        live = sum(1 for r in rows if r.get("status") == "LIVE")
        st = Counter(r.get("status") for r in rows)
        rep.kv(econ_rows=len(econ), family_rows=len(fam), live=live)
        rep.log("  statuses: " + json.dumps(dict(st.most_common(6))))
        for r in econ[:4]:
            rep.log("  ECON " + json.dumps(r)[:180])
        for r in fam[:4]:
            rep.log("  FAM  " + json.dumps(r)[:180])
        pref = Counter(str(r.get("symbol", "")).split(":")[0]
                       for r in rows if ":" in str(r.get("symbol", "")))
        rep.log("  prefixes: " + json.dumps(dict(pref.most_common(10))))
        bare = [r for r in rows if ":" not in str(r.get("symbol", ""))][:3]
        for r in bare:
            rep.log("  BARE " + json.dumps(r)[:150])
        rep.ok(f"ANATOMY — n={len(rows)} econ={len(econ)} fam={len(fam)} "
               f"live={live}")


if __name__ == "__main__":
    main()
