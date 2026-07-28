"""
ops_3998 — v2.2 verify: the families that were structurally impossible
before (vault size-capped out) must now exist, each holding its flagship.
"""
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
MARK = "data-census v2.2 ops3997 cap-honoured"


def main():
    with report("3998_v22_verify") as rep:
        rep.heading("ops 3992 — v2.2 vault-complete verify")
        checks = []
        doc = None
        for i in range(26):
            doc = json.loads(s3.get_object(Bucket=BUCKET,
                                           Key="data/data-census.json")["Body"].read())
            if doc.get("marker") == MARK:
                rep.ok(f"  v2.2 artifact after ~{i*30}s")
                break
            rep.log(f"  [{i}] {str(doc.get('marker'))[:40]}")
            time.sleep(30)
        checks.append(("artifact is v2.2", doc.get("marker") == MARK))
        if doc.get("marker") != MARK:
            for l, ok in checks:
                (rep.ok if ok else rep.fail)(f"  {l}")
            rep.fail("v2.2 never wrote in the window")
            sys.exit(1)

        bs = doc.get("by_source") or {}
        per = {k: (v or {}).get("n") for k, v in bs.items()}
        tot = sum(v or 0 for v in per.values())
        rep.kv(total=tot, families=len(bs), per_source=json.dumps(per))

        arts = {a.get("key"): a for a in doc.get("artifacts") or []}
        va = arts.get("data/tradingview.json") or {}
        rep.kv(vault_walked=json.dumps({k: va.get(k) for k in
                                        ("n_paths", "skipped", "size")}))
        checks.append(("vault actually walked (>=3000 paths, not skipped)",
                       (va.get("n_paths") or 0) >= 3000 and not va.get("skipped")))

        def flag(fam, sym, srckey=None):
            # v2.2: membership only — the substring pin produced the "bo" not
            # in "bank-of-japan" false-negative. Family assignment already
            # required the source to match; pinning it twice added only bugs.
            ms = (bs.get(fam) or {}).get("metrics") or []
            m = next((x for x in ms if str(x.get("name")).upper() == sym), None)
            rep.log(f"  {fam:16s} {sym:7s} -> "
                    f"{json.dumps(m)[:150] if m else 'ABSENT'}")
            return bool(m)

        checks += [
            ("BOJ family holds JPLG", flag("BOJ", "JPLG", "bo")),
            ("MOF-JAPAN holds JP02Y", flag("MOF-JAPAN", "JP02Y", "mof")),
            ("NORGES holds NO03Y", flag("NORGES", "NO03Y", "norges")),
            ("BCRP-PERU holds PETOT", flag("BCRP-PERU", "PETOT", "bcrp")),
            ("ECB holds EUBUND", flag("ECB", "EUBUND", "ecb")),
            ("OTHER shrunk below 800", (per.get("OTHER") or 0) < 800),
        ]
        fred = ((bs.get("FRED") or {}).get("metrics")) or []
        us10 = next((m for m in fred
                     if str(m.get("name")).upper() == "US10Y"), None)
        rep.log(f"  FRED US10Y -> {json.dumps(us10)[:170]}")
        checks.append(("US10Y credited to the vault engine (v1.9 canonical)",
                       bool(us10) and "tradingview" in str(us10.get("engine"))))

        got, MARKS = 0, ["v4-ops3988", "Data points pulled",
                         "what it is supposed to do"]
        try:
            req = urllib.request.Request(
                f"https://justhodl.ai/data-census.html?cb={int(time.time())}",
                headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
            h = urllib.request.urlopen(req, timeout=25).read().decode("utf8", "ignore")
            got = sum(1 for m in MARKS if m in h)
        except Exception:
            pass
        checks.append(("page v4 still at edge", got == len(MARKS)))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — v2.2 complete: {tot} points / {len(bs)} families; "
               f"vault {va.get('n_paths')} paths; OTHER {per.get('OTHER')}")


if __name__ == "__main__":
    main()
