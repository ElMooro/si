"""ops_4144 — two truths: (A) real MFS series keys via serieskeysonly;
(B) tv-sources harvest audit vs the 10,319 union."""
import json
import re
import sys
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
BASE = "https://api.imf.org/external/sdmx/2.1"
UA = {"User-Agent": "Mozilla/5.0"}


def fetch(url, timeout=75):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except Exception as e:
        return -1, str(e)[:180]


def main():
    with report("4144_keys_and_harvest") as rep:
        rep.heading("ops 4144 — real MFS keys + harvest audit")

        rep.section("A. real series keys (serieskeysonly)")
        for flow in ("MFS_CBS", "MFS_DC"):
            for probe in (f"{BASE}/data/{flow}/all"
                          "?detail=serieskeysonly",
                          f"{BASE}/data/{flow}/all/all"
                          "?detail=serieskeysonly"):
                st, x = fetch(probe)
                ns = x.count("<Series")
                rep.log(f"  {flow} {probe.split('?')[0][-14:]} -> {st} "
                        f"series={ns} ({len(x)}B)")
                if ns:
                    for m in re.finditer(r"<Series[^>]*>", x):
                        rep.log("    " + m.group(0)[:220])
                        if x[:m.start()].count("<Series") >= 4:
                            break
                    break
                rep.log("    body: " + x[:160])

        rep.section("B. tv-sources harvest audit")
        try:
            d = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/tv-sources.json")["Body"].read())
        except Exception as e:
            rep.fail(f"tv-sources.json unreadable: {type(e).__name__}")
            sys.exit(1)
        rows = (d.get("sources") or d.get("symbols")
                or d.get("by_symbol") or d)
        if isinstance(rows, dict):
            items = list(rows.items())
        else:
            items = [(r.get("symbol"), r) for r in rows]
        n = len(items)
        with_src = 0
        econ_null = 0
        srcs = Counter()
        for sym, r in items:
            v = r if isinstance(r, dict) else {"source": r}
            src = (v.get("source") or v.get("source_description")
                   or v.get("src") or "")
            if src:
                with_src += 1
                srcs[str(src)[:40]] += 1
            elif str(sym).startswith("ECONOMICS"):
                econ_null += 1
        rep.kv(generated_at=d.get("generated_at") or d.get("updated_at"),
               entries=n, with_source=with_src,
               pct=round(100 * with_src / max(1, n), 1),
               econ_null=econ_null)
        try:
            wl = json.loads(s3.get_object(
                Bucket=BUCKET,
                Key="data/tv-watchlists.json")["Body"].read())
            uni = set()
            for l in (wl.get("lists") or []):
                uni.update(map(str, l.get("symbols") or []))
            rep.kv(union=len(uni),
                   harvest_coverage_pct=round(100 * n / max(1, len(uni)),
                                              1))
        except Exception:
            pass
        rep.log("  top sources: " + json.dumps(
            dict(srcs.most_common(10)), ensure_ascii=False)[:400])
        rep.ok(f"AUDIT DONE — harvest {n} entries, {with_src} sourced")


if __name__ == "__main__":
    main()
