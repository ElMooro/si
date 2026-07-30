"""ops_4114 — family verify-only: artifact truth or the vault's own logs."""
import json
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
MARK = "tradingview-vault v3.15.0 ops4112 family-adapters"


def main():
    with report("4114_family_verify2") as rep:
        rep.heading("ops 4113 — family verify-only")
        v = json.loads(s3.get_object(Bucket=BUCKET,
                                     Key="data/tradingview.json")["Body"].read())
        mk = str(v.get("marker"))
        rep.kv(marker=mk[:60], generated_at=v.get("generated_at"))

        if mk == MARK:
            fams = Counter()
            idx = {}
            live = 0
            for r in v.get("symbols") or []:
                idx[r.get("symbol")] = r
                if r.get("status") == "LIVE":
                    live += 1
                ad = str(r.get("adapter") or "")
                if ad.startswith("family:"):
                    fams[ad] += 1
            rep.kv(total_live=live, **{k: n for k, n in fams.most_common()})
            checks = [("INTR >=25", fams.get("family:INTR", 0) >= 25),
                      ("FER >=80", fams.get("family:FER", 0) >= 80),
                      ("WB trio >=250",
                       sum(fams.get("family:" + f, 0)
                           for f in ("GDPYY", "IRYY", "UR")) >= 250)]
            for sym, want, tol in (("ECONOMICS:BRINTR", 14.25, 0.06),
                                   ("ECONOMICS:PEINTR", 4.25, 0.06),
                                   ("ECONOMICS:BRFER", 368899, 12000)):
                got = (idx.get(sym) or {}).get("value")
                rep.log(f"  spot {sym}: got={got} want~{want} "
                        f"src={(idx.get(sym) or {}).get('source')}")
                checks.append((f"spot {sym}",
                               got is not None and
                               abs(float(got) - want) <= tol))
            failed = [l for l, k in checks if not k]
            for l, k in checks:
                (rep.ok if k else rep.fail)(f"  {l}")
            if failed:
                rep.fail(f"FAILED: {failed}")
                sys.exit(1)
            rep.ok(f"PASS_ALL — families {dict(fams)}, total LIVE {live}")
            return

        rep.section("marker not v3.15.0 — the vault's own story")
        try:
            ev = logs.filter_log_events(
                logGroupName=f"/aws/lambda/justhodl-tradingview",
                startTime=int((datetime.now(timezone.utc) -
                               timedelta(minutes=50)).timestamp() * 1000),
                filterPattern='?REPORT ?timed ?Traceback ?family ?Error',
                limit=40)
            for e in (ev.get("events") or [])[-25:]:
                ts = datetime.fromtimestamp(e["timestamp"] / 1000,
                                            tz=timezone.utc).strftime("%H:%M:%S")
                rep.log(f"  [{ts}] {e['message'].strip()[:170]}")
        except Exception as e2:
            rep.log(f"  logs: {type(e2).__name__}")
        rep.fail("ARTIFACT NOT v3.15.0 — evidence above")
        sys.exit(1)


if __name__ == "__main__":
    main()
