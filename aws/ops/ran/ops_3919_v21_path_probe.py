"""
ops_3919 — PROBE for v2.1: (1) recursive path hunt for the 3 MISSING Leg-1
fields in their real live docs; (2) sovereign-distress real key (vanished
from 3917's report — list the prefix, don't guess); (3) CISS top keys/scale;
(4) HYG/LQD presence in etf-true-flows by_etf; (5) FMP commodities: does it
expose MULTIPLE CL contract months (oil-backwardation feasibility — build
only if a real curve source exists). Writes no code.
"""
import json, os, sys, urllib.request
from pathlib import Path
import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
FMP = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")


def get(k):
    return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())


def find_paths(obj, name, path="", hits=None, cap=6):
    if hits is None: hits = []
    if len(hits) >= cap: return hits
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == name and not isinstance(v, (dict, list)):
                hits.append(f"{path}.{k} = {str(v)[:80]}")
            find_paths(v, name, f"{path}.{k}", hits, cap)
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:12]):
            find_paths(v, name, f"{path}[{i}]", hits, cap)
    return hits


def main():
    with report("3919_v21_path_probe") as rep:
        rep.heading("ops 3919 — v2.1 unknowns, all at once")

        rep.section("1. real nested paths for the 3 MISSING Leg-1 fields")
        for key, fields in (("data/nyfed-primary-dealer.json", ["corp_net_bonds_b", "net_treasury_total_b"]),
                            ("data/ofr-stfm.json", ["fails_cross"]),
                            ("data/crisis-plumbing.json", ["composite_stress_score", "composite_score", "repo_stress_score"])):
            doc = get(key)
            for f in fields:
                rep.log(f"  {key} :: {f}: " + (" | ".join(find_paths(doc, f)) or "NOT FOUND ANYWHERE"))

        rep.section("2. sovereign-distress — list the real prefix")
        resp = s3.list_objects_v2(Bucket=B, Prefix="data/sovereign")
        keys = [o["Key"] for o in resp.get("Contents", [])]
        rep.log(f"  keys: {keys}")
        if keys:
            d = get(keys[0])
            rep.log(f"  top keys: {sorted(d.keys())[:15]}")
            rep.log(f"  composite hunt: " + " | ".join(find_paths(d, "composite") +
                    find_paths(d, "composite_score") + find_paths(d, "distress_score")))

        rep.section("3. CISS scale")
        c = get("data/ciss-stress.json")
        rep.log(f"  top keys: {sorted(c.keys())[:15]}")
        for f in ("ciss", "level", "composite", "us_ciss", "country_ciss"):
            h = find_paths(c, f)
            if h: rep.log(f"  {f}: " + " | ".join(h[:3]))

        rep.section("4. HYG/LQD in etf-true-flows by_etf")
        tf = get("data/etf-true-flows.json")
        be = tf.get("by_etf")
        if isinstance(be, dict):
            rep.log(f"  by_etf has HYG={('HYG' in be)} LQD={('LQD' in be)}; "
                    f"HYG sample: {json.dumps(be.get('HYG'), default=str)[:200]}")
        elif isinstance(be, list):
            tick = {r.get('ticker') or r.get('symbol') for r in be if isinstance(r, dict)}
            rep.log(f"  by_etf list; HYG={'HYG' in tick} LQD={'LQD' in tick}")
        else:
            rep.log(f"  by_etf type={type(be).__name__}")

        rep.section("5. FMP commodities — CL curve months? (backwardation feasibility)")
        try:
            url = f"https://financialmodelingprep.com/stable/quote?symbol=CLUSD&apikey={FMP}"
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "jh/1"}), timeout=20) as r:
                rep.log(f"  CLUSD quote: {r.read().decode()[:300]}")
        except Exception as e:
            rep.log(f"  CLUSD: {str(e)[:120]}")
        try:
            url2 = f"https://financialmodelingprep.com/stable/commodities-list?apikey={FMP}"
            with urllib.request.urlopen(urllib.request.Request(url2, headers={"User-Agent": "jh/1"}), timeout=20) as r:
                data = json.loads(r.read())
            cl = [x for x in data if isinstance(x, dict) and "CL" in str(x.get("symbol", ""))][:12]
            rep.log(f"  commodities-list n={len(data) if isinstance(data, list) else '?'}; "
                    f"CL-ish symbols: {[x.get('symbol') for x in cl]}")
        except Exception as e:
            rep.log(f"  commodities-list: {str(e)[:150]}")

        rep.ok("PROBE COMPLETE")
        if False: sys.exit(1)


if __name__ == "__main__":
    main()
