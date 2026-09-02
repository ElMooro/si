"""ops_5109 -- probe the dead FRED ids / NY Fed endpoint behind the daily 400s
(plumbing-aggregator, repo-monitor, manufacturing-global-agent), read-only.

Real-data doctrine: a series id is only wired if FRED answers 200 with an
observation in 2026. Every candidate is probed here first; the code patch
follows from the evidence, never from memory of an id.
"""
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

FRED_KEY = "2f057499936072679d8843d7fce99989"
PROBES = {
    # engine: {dead_id: [candidates]}
    "plumbing-aggregator": {
        "WCBSL": ["SWPT", "WCBSL"],
        "DRTSCLM": ["DRTSCLM", "DRTSCILM", "DRTSCLCC", "SUBLPDRCSM", "DRTSPM"],
        "WGRESUS": ["WGRESUS", "WMTSECL1", "WSHOMCB", "FDHBFIN"],
    },
    "repo-monitor": {
        "OTHL1690": ["OTHL1690", "WLCFLPCL", "WORAL"],
        "SOFR25": ["SOFR25", "SOFR1", "SOFR99"],
        "SOFR75": ["SOFR75", "SOFR99"],
        "USD3MTD156N": ["USD3MTD156N", "SOFR90DAYAVG", "TSFR3M"],
        "WDTGAL": ["WDTGAL", "WTREGEN", "WDTGAL"],
        "WLCFLPCL": ["WLCFLPCL"],
        "SWPT": ["SWPT"],
    },
    "manufacturing-global-agent": {
        "DALLASFEDFAB": ["DALLASFEDFAB", "BACTSAMFRBDAL", "PROSAMFRBDAL"],
        "KCLFEDFAB": ["KCLFEDFAB", "KCFMCI", "COMPRMTSAMFRBKC"],
        "CHEFMNM156N": ["CHEFMNM156N", "CHNPRMNTO01IXOBM"],
        "GAFDIMSA": ["GAFDIMSA", "GACDISA066MSFRBNY"],
        "GAPHDFBA": ["GAPHDFBA", "GACDFSA066MSFRBPHI"],
        "RMTSPL": ["RMTSPL"],
        "NAPMEI": ["NAPMEI"], "NAPMII": ["NAPMII"],
        "EA19PRMNTO01IXOBM": ["EA19PRMNTO01IXOBM", "EA20PRMNTO01IXOBM"],
        "JPNPRMNTO01IXOBM": ["JPNPRMNTO01IXOBM"], "GBRPRMNTO01IXOBM": ["GBRPRMNTO01IXOBM"], "DEUPRMNTO01IXOBM": ["DEUPRMNTO01IXOBM"],
    },
}
NYFED = ["https://markets.newyorkfed.org/api/rp/srf/results/latest.json",
         "https://markets.newyorkfed.org/api/rp/all/all/results/latest.json",
         "https://markets.newyorkfed.org/api/rp/repo/all/results/latest.json",
         "https://markets.newyorkfed.org/api/rp/repo/all/results/last/1.json",
         "https://markets.newyorkfed.org/api/rp/all/all/results/last/3.json"]


def fred_probe(sid):
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=3"
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "ops5109"}), timeout=20) as r:
            d = json.loads(r.read().decode())
        obs = [(o["date"], o["value"]) for o in d.get("observations") or [] if o.get("value") not in (".", "", None)]
        meta = {}
        try:
            with urllib.request.urlopen(f"https://api.stlouisfed.org/fred/series?series_id={sid}&api_key={FRED_KEY}&file_type=json", timeout=20) as r2:
                meta = (json.loads(r2.read().decode()).get("seriess") or [{}])[0]
        except Exception:  # noqa: BLE001
            pass
        return {"status": 200, "latest": obs[0] if obs else None, "title": (meta.get("title") or "")[:80], "freq": meta.get("frequency_short"), "obs_end": meta.get("observation_end")}
    except urllib.error.HTTPError as e:
        return {"status": e.code, "error": e.read()[:120].decode("utf-8", "replace")}
    except Exception as e:  # noqa: BLE001
        return {"status": None, "error": str(e)[:100]}


def main():
    with report("5109-fred-id-probe") as r:
        r.heading("ops 5109 -- probe replacements for the dead FRED ids / NY Fed endpoint (read-only)")
        out = {}
        for engine, m in PROBES.items():
            r.section(engine)
            out[engine] = {}
            for dead, cands in m.items():
                res = {}
                for c in cands:
                    res[c] = fred_probe(c)
                    time.sleep(0.4)
                ok = [c for c, v in res.items() if v.get("status") == 200 and v.get("latest") and str(v["latest"][0]) >= "2026"]
                r.log(f"{dead}: " + " | ".join(f"{c}->{v.get('status')} {v.get('latest')} {v.get('title', '')[:50]}" for c, v in res.items()))
                r.kv(engine=engine, dead=dead, live_2026=",".join(ok) or "NONE")
                out[engine][dead] = {"results": res, "live_2026": ok}
        r.section("nyfed endpoints")
        out["nyfed"] = {}
        for u in NYFED:
            try:
                with urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "ops5109"}), timeout=20) as rr:
                    body = rr.read()
                head = body[:220].decode("utf-8", "replace")
                r.log(f"{u}: 200 {len(body)}B {head}")
                out["nyfed"][u] = {"status": 200, "head": head}
            except urllib.error.HTTPError as e:
                r.log(f"{u}: HTTP {e.code}")
                out["nyfed"][u] = {"status": e.code}
            except Exception as e:  # noqa: BLE001
                r.log(f"{u}: {str(e)[:80]}")
                out["nyfed"][u] = {"status": None, "error": str(e)[:80]}
        boto3.client("s3", region_name="us-east-1").put_object(Bucket="justhodl-dashboard-live", Key="data/audit/fred-id-probe-5109.json",
                                                              Body=json.dumps(out, default=str).encode(), ContentType="application/json")
        r.ok("probe written to data/audit/fred-id-probe-5109.json")
        if not any(v.get("status") == 200 for eng in PROBES for v in (out.get(eng) or {}).values() for v in v["results"].values()):
            r.fail("FRED unreachable from the runner")
            sys.exit(1)


if __name__ == "__main__":
    main()
