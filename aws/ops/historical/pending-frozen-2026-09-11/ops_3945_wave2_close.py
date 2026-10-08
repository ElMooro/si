"""
ops_3945 — wave-2 close: gate what's proven (JP02Y via MOF-proxy — proxy
serving verified 3944; CH02Y/CH03Y max-date fix; n_live), and DIAGNOSE IMF
instead of gating it: dump the full 3.3KB response (likely an empty dataset
- 200 + structure header, zero <Obs>), try sdmx-json Accept + two alternate
key orders. No engine change in this push.
"""
import json, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300, retries={"max_attempts": 0}))
FN = "justhodl-tradingview"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"}


def get_doc():
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/tradingview.json")["Body"].read())


def main():
    with report("3945_wave2_close") as rep:
        rep.heading("ops 3945 — wave-2 close + IMF diagnosis")
        checks = []

        rep.section("IMF diagnosis — full body + format/key variants")
        base = "https://api.imf.org/external/sdmx/2.1/data/IRFCL/"
        variants = [
            ("orig", base + "M.US.RAF_USD?lastNObservations=1", {}),
            ("json-accept", base + "M.US.RAF_USD?lastNObservations=1",
             {"Accept": "application/vnd.sdmx.data+json"}),
            ("key-swap", base + "US.RAF_USD.M?lastNObservations=1", {}),
            ("all-dims-wild", base + "M.US.?lastNObservations=1", {}),
        ]
        for label, u, extra in variants:
            try:
                req = urllib.request.Request(u, headers={**UA, **extra})
                with urllib.request.urlopen(req, timeout=25) as r:
                    body = r.read().decode("utf-8", "ignore")
                has_obs = "<Obs" in body or '"observations"' in body
                rep.log(f"  [{label}] {len(body)}b has_obs={has_obs}")
                if label == "orig":
                    rep.log("  FULL BODY: " + body[:2400].replace("\n", " "))
                elif has_obs:
                    rep.log("  head: " + body[:400].replace("\n", " "))
            except Exception as e:
                rep.log(f"  [{label}] {str(e)[:100]}")

        rep.section("engine gates (v3.5.1 already deployed; force run)")
        t_mark = datetime.now(timezone.utc).isoformat()
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=json.dumps({"force": True}).encode())
        doc = None
        for i in range(60):
            time.sleep(15)
            d = get_doc()
            if d.get("generated_at", "") > t_mark:
                doc = d; rep.ok(f"  refreshed ~{(i+1)*15}s"); break
        checks.append(("force run wrote", doc is not None))
        if not doc: rep.fail("never wrote"); sys.exit(1)
        st_ = doc.get("status_counts") or {}
        idx = {r["symbol"]: r for r in doc.get("symbols") or []}
        for s_ in ("JP02Y", "CH02Y", "CH03Y", "PETOT", "NO03Y", "US02MY"):
            rw = idx.get(s_) or {}
            rep.log(f"  {s_}: {rw.get('status')} value={rw.get('value')} "
                    f"src={rw.get('source')} asof={rw.get('asof')}")
        checks.append(("JP02Y LIVE via mof-japan (proxy path)",
                       (idx.get("JP02Y") or {}).get("status") == "LIVE"
                       and "mof" in str((idx.get("JP02Y") or {}).get("source"))))
        ch = idx.get("CH02Y") or {}
        checks.append(("CH02Y LIVE + 2026 asof (max-date fix)",
                       ch.get("status") == "LIVE" and "2026" in str(ch.get("asof"))))
        rep.kv(n_live=doc.get("n_live"), coverage_pct=doc.get("coverage_pct"),
               statuses=str(st_))
        checks.append(("n_live >= 455", (doc.get("n_live") or 0) >= 455))
        checks.append(("zero bare UNRESOLVED", st_.get("UNRESOLVED", 0) == 0))
        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — {doc.get('n_live')} LIVE ({doc.get('coverage_pct')}%); "
               f"JP02Y {(idx.get('JP02Y') or {}).get('value')} · CH02Y "
               f"{ch.get('value')} @ {ch.get('asof')}")


if __name__ == "__main__":
    main()
