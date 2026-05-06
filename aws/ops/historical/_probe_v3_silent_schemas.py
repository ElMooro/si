"""Probe schemas of divergence/current.json, cot-extremes, eurodollar-stress to see why translators fire 0."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def load(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        return {"_err": str(e)}


def main():
    with report("probe_v3_silent_schemas") as r:
        # ---- divergence/current.json ----
        r.heading("1) divergence/current.json")
        d = load("divergence/current.json")
        if "_err" in d:
            r.log(f"  ✗ {d['_err']}")
        else:
            r.log(f"  top-level keys: {list(d.keys())}")
            divs = d.get("divergences") or d.get("active_divergences") or []
            r.log(f"  divergences count: {len(divs)}")
            for i, div in enumerate(divs[:5]):
                r.log(f"  [{i}] keys: {list(div.keys())}")
                r.log(f"      values: { {k: str(v)[:50] for k,v in div.items()} }")
            # Check if any have |z| >= 2.5
            zs = []
            for div in divs:
                z = div.get("residual_z") or div.get("z_score") or div.get("z")
                if z is not None:
                    zs.append((div.get("pair") or div.get("name") or "?", z))
            zs.sort(key=lambda x: -abs(x[1]))
            r.log(f"  z-scores ranked: {zs[:8]}")

        # ---- cot-extremes ----
        r.heading("2) cot-extremes (try multiple paths)")
        for path in ["data/cot-extremes.json", "cot/extremes.json", "data/cot/extremes.json"]:
            d = load(path)
            if "_err" not in d:
                r.log(f"  ✓ found at {path}")
                r.log(f"  keys: {list(d.keys())}")
                ext = d.get("extremes") or d.get("contracts") or []
                r.log(f"  extremes count: {len(ext)}")
                for i, e in enumerate(ext[:5]):
                    r.log(f"  [{i}] keys: {list(e.keys())}")
                    r.log(f"      contract={e.get('contract') or e.get('name')}  pct={e.get('percentile_rank') or e.get('pct_rank')}")
                # ranked
                ranked = []
                for e in ext:
                    pct = e.get("percentile_rank") or e.get("pct_rank")
                    if pct is not None:
                        ranked.append((e.get("contract") or e.get("name"), pct))
                ranked.sort(key=lambda x: x[1])
                r.log(f"  lowest pct: {ranked[:3]}")
                r.log(f"  highest pct: {ranked[-3:]}")
                break
            else:
                r.log(f"  ✗ {path}: {d['_err']}")

        # ---- eurodollar-stress ----
        r.heading("3) data/eurodollar-stress.json")
        d = load("data/eurodollar-stress.json")
        if "_err" in d:
            r.log(f"  ✗ {d['_err']}")
        else:
            r.log(f"  top-level keys: {list(d.keys())[:20]}")
            for k in ["composite_stress_score", "composite_score", "composite", "stress_score", "score"]:
                if k in d:
                    r.log(f"  ✓ {k}: {d[k]}")
            # show full top-level for non-dict scalars
            for k, v in d.items():
                if isinstance(v, (int, float, str, bool)):
                    r.log(f"  {k:35s} = {v}")


if __name__ == "__main__":
    main()
