"""ops_4166 — fresh QUEUED class-table + econ residual suffixes (post-wave)
+ DBnomics OECD/MEI probe + IMF CPI census in one pass."""
import json
import re
import sys
import urllib.request
from collections import Counter
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
UA = {"User-Agent": "Mozilla/5.0"}


def fetch(url, timeout=60):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except Exception as e:
        return -1, str(e)[:160]


def main():
    with report("4166_next_frontiers") as rep:
        rep.heading("ops 4166 — queued table + CPI census + DBnomics")

        v = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        pend = Counter()
        sfx = Counter()
        for r in v.get("symbols") or []:
            if r.get("status") == "PENDING_RESOLUTION":
                sy = str(r.get("symbol"))
                m = re.match(r"^([A-Z]{2})([A-Z0-9]{1,12})$", sy)
                if m and not any(ch.isdigit() for ch in sy[:2]):
                    pend["econ-bare"] += 1
                    sfx[m.group(2)] += 1
                elif re.match(r"^\d{6}_", sy):
                    pend["cot"] += 1
                elif sy.endswith("!"):
                    pend["futures"] += 1
                else:
                    pend["other-bare"] += 1
        rep.log("  pending classes: " + json.dumps(
            dict(pend.most_common()))[:300])
        rep.log("  econ residual suffixes: " + json.dumps(
            dict(sfx.most_common(24)))[:520])

        rep.section("IMF CPI flow — indicator census")
        st, x = fetch("https://api.imf.org/external/sdmx/2.1/data/CPI/all"
                      "?detail=serieskeysonly", timeout=120)
        inds = Counter(re.findall(
            r'INDICATOR="([A-Za-z0-9_]+)"[^>]*FREQUENCY="M"', x))
        rep.kv(cpi_status=st, monthly_indicators=len(inds))
        for cid, n in inds.most_common(12):
            rep.log(f"    {n:4d}  {cid}")

        rep.section("DBnomics OECD gateway probe")
        for url in (
            "https://api.db.nomics.world/v22/series/OECD/MEI/"
            "JPN.PRINTO01.IXOBSA.M?observations=1",
            "https://api.db.nomics.world/v22/series?provider_code=OECD"
            "&q=industrial%20production&limit=3",
        ):
            st2, x2 = fetch(url, timeout=45)
            rep.log(f"  {url[:74]}... -> {st2} ({len(x2)}B)")
            if st2 == 200:
                rep.log("    " + x2[:220].replace("\n", " "))
        rep.ok("FRONTIERS MAPPED")


if __name__ == "__main__":
    main()
