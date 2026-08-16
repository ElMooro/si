"""ops/4798 -- v2.4 verify (async): Event-invoke, poll as_of, assert
engine_v == 2.4 in repo.json, then: TE daily rows (DE10Y_TE/IT10Y_TE),
D_BTP_BUND_D sane 40-300bp, WREPOFOR present, sftr_rows > 0 with a
named sample, board_total."""
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
                    config=Config(read_timeout=30,
                                   retries={"max_attempts": 1}))


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("4798_v24_verify") as rep:
        rep.heading("ops 4798 -- engine v2.4 verify")
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
            rep.warn("no as_of advance in 13 min")
            return
        rep.kv(check="engine_v", value=d.get("engine_v"))
        allrows = {s0["id"]: s0 for g in d["groups"]
                    for s0 in g["series"]}
        G = {g["name"]: len(g["series"]) for g in d["groups"]}
        for mid in ("DE10Y_TE", "IT10Y_TE", "D_BTP_BUND_D",
                     "WREPOFOR"):
            s0 = allrows.get(mid)
            if s0:
                rep.ok(f"{mid}: {s0['first']} -> {s0['last']} "
                        f"n={s0['n_obs']} last={s0['last_value']}")
            else:
                rep.warn(f"{mid}: ABSENT")
        lv = (allrows.get("D_BTP_BUND_D") or {}).get("last_value")
        rep.kv(check="daily_btp_sane",
                value=bool(lv and 40 <= lv <= 300), last_bp=lv)
        sf = G.get("SFTR EU/UK repo (ICMA weekly)", 0)
        rep.kv(check="sftr_rows", value=sf)
        if sf:
            samp = next((m for m in allrows
                          if m.startswith("SFTR-EU-")), None)
            rep.ok(f"sftr sample: {samp}")
        rep.kv(check="board_total", value=sum(G.values()))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
