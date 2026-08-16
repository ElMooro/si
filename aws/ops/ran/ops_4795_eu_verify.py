"""ops/4795 -- EU block verify (async pattern): Event-invoke, poll
as_of, then assert: 4 MMSR rows with spans, 4 EU yield rows, the three
-Bund derived spreads present with a sane BTP-Bund last value (~40-150
bps), plus honest statuses for RFR(CME entitlement) and SFTR(weekly
files) recon items."""
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
    with report("4795_eu_verify") as rep:
        rep.heading("ops 4795 -- EU sovereign block verify")
        pre = None
        try:
            pre = sread("data/repo.json").get("as_of")
        except Exception:
            pass
        lam.invoke(FunctionName="justhodl-repo",
                    InvocationType="Event", Payload=b"{}")
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
        rep.kv(check="engine_completed", value=bool(d),
                waited_s=round(time.time() - t0, 1))
        if not d:
            rep.warn("engine did not complete in 13 min")
            return
        allrows = {s0["id"]: s0 for g in d["groups"]
                    for s0 in g["series"]}
        G = {g["name"]: len(g["series"]) for g in d["groups"]}
        rep.kv(check="mmsr_rows",
                value=G.get("European sovereign collateral (ECB MMSR)", 0))
        rep.kv(check="euy_rows",
                value=G.get("European sovereign yields & spreads", 0))
        for mid in ("MMSR-DE-ON-BORROW-TURNOVER",
                     "MMSR-IT-ON-BORROW-TURNOVER",
                     "IRLTLT01DEM156N", "IRLTLT01ITM156N",
                     "D_BTP_BUND", "D_OAT_BUND", "D_BONO_BUND"):
            s0 = allrows.get(mid)
            if s0:
                rep.ok(f"{mid}: {s0['first']} -> {s0['last']} "
                        f"n={s0['n_obs']} last={s0['last_value']}")
            else:
                rep.warn(f"{mid}: ABSENT")
        btp = allrows.get("D_BTP_BUND")
        if btp and btp.get("last_value") is not None:
            v = btp["last_value"]
            rep.kv(check="btp_bund_sane_40_300bp",
                    value=bool(40 <= v <= 300), last_bp=v)
        rep.log("RFR by sovereign (CME DataMine): entitlement API -- "
                 "recon flagged, not faked")
        rep.log("SFTR weekly consolidated files (ICMA): recon flagged")
        rep.kv(check="board_total",
                value=sum(G.values()))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
