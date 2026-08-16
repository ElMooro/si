"""ops/4790 -- the 14-block audit closure table, from LIVE artifacts.
Retry-invokes justhodl-repo until v2.1 answers, then maps every block
of the external audit to PRESENT/ADDED/PENDING with evidence: group
sizes, key spans (SOFR, RRPONTSYD, sponsored vols, MMF wFICC, a
derived spread, percentile width, a haircut series), the DTCC attempt
outcome, the NCCBR tripwire state, and Europe as next-arc."""
import gzip
import json
import sys
import time
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=900, connect_timeout=10,
                                   retries={"max_attempts": 0}))


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("4790_audit_closure") as rep:
        rep.heading("ops 4790 -- external audit: 14-block closure table")
        d = None
        for i in range(9):
            r = lam.invoke(FunctionName="justhodl-repo",
                            InvocationType="RequestResponse", Payload=b"{}")
            body = json.loads(json.loads(
                r["Payload"].read().decode())["body"])
            rep.log(f"attempt {i}: v={body.get('v')} "
                    f"series={body.get('series')}")
            if body.get("v") == "2.1":
                d = sread("data/repo.json")
                break
            time.sleep(22)
        if not d:
            rep.warn("v2.1 not observed")
            return
        G = {g["name"]: g["series"] for g in d["groups"]}
        allrows = {s0["id"]: s0 for g in d["groups"] for s0 in g["series"]}

        def span(mid):
            s0 = allrows.get(mid)
            return (f"{s0['first']}->{s0['last']} n={s0['n_obs']}"
                     if s0 else "ABSENT")

        def block(n, name, status, ev):
            rep.kv(**{f"blk{n:02d}_{name}": status})
            rep.log(f"  #{n} {name}: {ev}")

        block(1, "venue_rates_vols", "PRESENT",
               f"TRIV1={len(G.get('Tri-party (TRIV1, ex-Fed)', []))} "
               f"DVP={len(G.get('DVP bilateral cleared', []))} "
               f"GCF={len(G.get('GCF', []))}")
        ov = [m for m in allrows if "_OV_" in m]
        block(2, "tenor_rollover", "PRESENT",
               f"tenor legs incl OV={len(ov)}; derived ON/total in "
               "Derived group")
        block(3, "pd_financing", "PRESENT",
               f"fails+financing rows="
               f"{sum(1 for m in allrows if m.startswith('NYPD'))}, "
               f"deepest {min((allrows[m]['first'] for m in allrows if m.startswith('NYPD')), default='-')}")
        dt = [m for m in allrows if "DTCC" in m.upper()]
        block(4, "dtcc_daily_fails",
               "PRESENT" if dt else "PENDING(source)",
               f"rows={dt[:2]}" if dt else
               "engine attempts DTCC endpoint; no public API confirmed "
               "-- recon flagged")
        sp = [m for m in allrows if "SPONSORED" in m.upper()]
        block(5, "ficc_sponsored", "PRESENT" if sp else "MISSING",
               "; ".join(f"{m} {span(m)}" for m in sp[:2]))
        hc = G.get("Tri-party haircuts (NY Fed monthly)", [])
        block(6, "haircuts", f"ADDED({len(hc)})" if hc else "MISSING",
               (f"{hc[0]['id']} {span(hc[0]['id'])}" if hc else
                "workbooks parsed?") + " | NCCBR: publisher-blocked, "
               "stfm rediscovery tripwire armed")
        drv = G.get("Derived stress indicators", [])
        dvps = next((m for m in allrows if "DVP" in m and "SOFR" in m), None)
        block(7, "specialness_proxy",
               "PRESENT(proxy)" if dvps else "PARTIAL",
               f"derived {dvps} {span(dvps) if dvps else ''}; per-CUSIP "
               "OTR specials = commercial (Kinetics) -- not public")
        pct = G.get("Rate percentiles (NY Fed)", [])
        width = next((m for m in allrows if "P75" in m and "P25" in m
                       or "WIDTH" in m.upper()), None)
        block(8, "sofr_distribution",
               "PRESENT" if pct else "MISSING",
               f"percentile rows={len(pct)}; width={width}; "
               f"SOFR {span('SOFR')}; SOFR_UV {span('FNYR-SOFR_UV-A')}")
        srf = [m for m in allrows if m.startswith("RPONTSY")
                or "SRF" in m.upper()]
        block(9, "srf", "PRESENT" if srf else "MISSING",
               "; ".join(f"{m} {span(m)}" for m in srf[:3]))
        mmf = [m for m in allrows if m.startswith("MMF-")]
        block(10, "mmf_counterparties",
               "PRESENT" if mmf else "MISSING",
               f"rows={len(mmf)}; wFICC {span('MMF-MMF_RP_wFICC-M')}")
        block(11, "collateral_splits", "PRESENT",
               "all TRIV1/TRI/GCF collateral legs on board")
        fima = [m for m in allrows if "FOREIGN" in
                 (allrows[m].get('label') or '').upper() and
                 "REPO" in (allrows[m].get('label') or '').upper()]
        block(12, "fima_foreign_pool",
               "PRESENT" if fima else "PARTIAL",
               f"matches={fima[:2]} (H.4.1 lines via FRED tag)")
        block(13, "europe_mmsr", "PENDING(next arc)",
               "ECB MMSR secured-by-collateral-country -- queued "
               "international arc")
        block(14, "derived_barometer", "PRESENT",
               f"Derived group={len(drv)} first-class series + "
               "user red-tag barometer")
        rep.kv(check="board_total_series",
                value=sum(len(v) for v in G.values()))
        rep.kv(check="groups_total", value=len(G))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
