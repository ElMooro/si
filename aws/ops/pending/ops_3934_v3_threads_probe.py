"""
ops_3934 — PROBE the three open v3.0 threads, no code changes:
(1) ECB adapter: did EU03Y/DE02Y/EUCA resolve via ecb:? If not, hit the ECB
    Data Portal directly from the runner for the exact YC 2Y + BP6 keys and
    print the raw structure heads — diagnose the parse in one shot.
(2) Cache math: from the artifact, counts by (cadence, cached) and fetched
    rows by source family — explain run2's 395 FRED calls vs run1's 270.
(3) NFS composition: the 112 by resolution-note class.
Also dumps EU02Y-TVC presence (expected absent post-hyphen-fix).
"""
import json, sys, urllib.request
from collections import Counter
from pathlib import Path
import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("3934_v3_threads_probe") as rep:
        rep.heading("ops 3934 — v3.0 open-threads probe")
        doc = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                       Key="data/tradingview.json")["Body"].read())
        idx = {r["symbol"]: r for r in doc.get("symbols") or []}

        rep.section("1. ECB-aliased symbols in the live artifact")
        for s_ in ("EU03Y", "DE02Y", "EUCA", "EU02Y-TVC", "EU30Y-TVC"):
            r = idx.get(s_)
            rep.log(f"  {s_}: " + ("ABSENT from registry" if r is None else
                    f"{r.get('status')} value={r.get('value')} src={r.get('source')} "
                    f"note={str(r.get('resolution_note'))[:60]}"))

        ecb_worked = any("ecb:" in str((idx.get(s_) or {}).get("source"))
                         and (idx.get(s_) or {}).get("status") == "LIVE"
                         for s_ in ("EU03Y", "DE02Y", "EUCA"))
        if not ecb_worked:
            rep.section("1b. direct ECB Data Portal test from the runner")
            for key in ("YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y",
                        "BP6/M.N.I8.W1.S1.S1.T.B.CA._Z._Z._Z.EUR._T._X.N"):
                url = (f"https://data-api.ecb.europa.eu/service/data/{key}"
                       f"?format=jsondata&lastNObservations=1")
                try:
                    req = urllib.request.Request(url, headers={
                        "User-Agent": "JH/1", "Accept": "application/json"})
                    with urllib.request.urlopen(req, timeout=20) as r_:
                        body = r_.read()
                    rep.log(f"  {key}: HTTP {r_.status}, {len(body)}b")
                    try:
                        d = json.loads(body)
                        series = d["dataSets"][0]["series"]
                        sk = list(series.keys())[0]
                        obs = series[sk]["observations"]
                        rep.log(f"    parse: series_key={sk} first_obs={list(obs.items())[0]}")
                    except Exception as pe:
                        rep.log(f"    PARSE FAIL: {str(pe)[:100]}; head={body[:200]!r}")
                except Exception as e:
                    rep.log(f"  {key}: FETCH FAIL {str(e)[:140]}")

        rep.section("2. cache math (from artifact rows)")
        cc = Counter((r.get("cadence"), bool(r.get("cached"))) for r in doc.get("symbols") or [])
        for (cad, cached), n in sorted(cc.items()):
            rep.log(f"  cadence={cad} cached={cached}: {n}")
        src_fetched = Counter()
        for r in doc.get("symbols") or []:
            if not r.get("cached"):
                fam = str(r.get("source", "")).split(":")[0]
                src_fetched[fam] += 1
        rep.log(f"  fetched-this-run by source family: {dict(src_fetched.most_common(12))}")
        rep.kv(fred_calls_this_run=doc.get("fred_calls_this_run"),
               n_cached=doc.get("n_cached_this_run"), n_live=doc.get("n_live"))

        rep.section("3. NFS composition (112)")
        nc = Counter(str((r.get("resolution_note") or "no note"))[:55]
                     for r in doc.get("symbols") or []
                     if r.get("status") == "NO_FREE_SOURCE")
        for note, n in nc.most_common(20):
            rep.log(f"  {n:3d}  {note}")

        rep.ok("PROBE COMPLETE")
        if False: sys.exit(1)


if __name__ == "__main__":
    main()
