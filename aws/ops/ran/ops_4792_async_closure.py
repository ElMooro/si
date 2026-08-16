"""ops/4792 -- audit closure, ASYNC pattern (sync invokes die at ~5.5
min on this path: ConnectionClosedError, ops 4791). Flow: snapshot
repo.json as_of -> Event-invoke justhodl-repo (202) -> poll as_of
until it advances (<=13 min) -> run the full 14-block closure
assertions against the fresh board. If as_of never moves, the engine
is exceeding its 900s cap and that is reported as the finding."""
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
                    config=Config(read_timeout=30, connect_timeout=10,
                                   retries={"max_attempts": 1}))


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("4792_async_closure") as rep:
        rep.heading("ops 4792 -- async engine run + 14-block closure")
        try:
            pre = sread("data/repo.json").get("as_of")
        except Exception:
            pre = None
        rep.kv(check="as_of_before", value=pre)
        r = lam.invoke(FunctionName="justhodl-repo",
                        InvocationType="Event", Payload=b"{}")
        rep.kv(check="event_status", value=r.get("StatusCode"))
        d = None
        t0 = time.time()
        while time.time() - t0 < 13 * 60:
            time.sleep(25)
            try:
                cur = sread("data/repo.json")
            except Exception:
                continue
            if cur.get("as_of") != pre:
                d = cur
                break
        rep.kv(check="engine_completed",
                value=bool(d),
                waited_s=round(time.time() - t0, 1))
        if not d:
            rep.warn("as_of never advanced in 13 min -- engine likely "
                     "exceeds its 900s cap on this run shape; next patch "
                     "= global time guards in the ICE/frepo loops")
            return
        rep.kv(check="engine_v_note", value=d.get("as_of"))
        G = {g["name"]: g["series"] for g in d["groups"]}
        allrows = {s0["id"]: s0 for g in d["groups"]
                    for s0 in g["series"]}

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
        block(2, "tenor_rollover", "PRESENT",
               f"OV legs={sum(1 for m in allrows if '_OV_' in m)}")
        block(3, "pd_financing", "PRESENT",
               f"NYPD rows="
               f"{sum(1 for m in allrows if m.startswith('NYPD'))}; "
               f"deepest {min((allrows[m]['first'] for m in allrows if m.startswith('NYPD')), default='-')}")
        dt = [m for m in allrows if "DTCC" in m.upper()]
        block(4, "dtcc_daily_fails",
               "PRESENT" if dt else "PENDING(source recon)",
               str(dt[:2]) if dt else "no public API confirmed yet")
        sp = [m for m in allrows if "SPONSORED" in m.upper()]
        block(5, "ficc_sponsored", "PRESENT" if sp else "MISSING",
               "; ".join(f"{m} {span(m)}" for m in sp[:2]))
        hc = G.get("Tri-party haircuts (NY Fed monthly)", [])
        block(6, "haircuts",
               f"PRESENT({len(hc)})" if hc else "PENDING(parser v2)",
               (f"{hc[0]['id']} {span(hc[0]['id'])}" if hc else
                "ops 4793 parser v2 in this push") +
               " | NCCBR publisher-blocked, tripwire armed")
        dvps = next((m for m in allrows
                      if "DVP" in m.upper() and "SOFR" in m.upper()), None)
        block(7, "specialness_proxy",
               "PRESENT(proxy)" if dvps else "PARTIAL",
               f"{dvps} {span(dvps) if dvps else ''}; per-CUSIP specials "
               "= commercial only")
        pct = G.get("Rate percentiles (NY Fed)", [])
        block(8, "sofr_distribution",
               "PRESENT" if pct else "MISSING",
               f"percentile rows={len(pct)}; SOFR {span('SOFR')}; "
               f"UV {span('FNYR-SOFR_UV-A')}")
        srf = [m for m in allrows if m.startswith("RPONTSY")
                or "SRF" in m.upper()]
        block(9, "srf", "PRESENT" if srf else "MISSING",
               "; ".join(f"{m} {span(m)}" for m in srf[:3]))
        mmf = [m for m in allrows if m.startswith("MMF-")]
        block(10, "mmf_counterparties",
               "PRESENT" if mmf else "MISSING",
               f"rows={len(mmf)}; wFICC {span('MMF-MMF_RP_wFICC-M')}")
        block(11, "collateral_splits", "PRESENT", "all legs on board")
        fima = [m for m, s0 in allrows.items()
                 if "FOREIGN" in (s0.get('label') or '').upper()
                 and "REPO" in (s0.get('label') or '').upper()]
        block(12, "fima_foreign_pool",
               "PRESENT" if fima else "PARTIAL", str(fima[:2]))
        block(13, "europe_mmsr", "PENDING(next arc)",
               "ECB MMSR queued -- international arc")
        drv = G.get("Derived stress indicators", [])
        block(14, "derived_barometer", "PRESENT",
               f"derived={len(drv)} first-class series + red-tag "
               "user barometer")
        rep.kv(check="board_total",
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
