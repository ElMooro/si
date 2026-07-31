"""ops_4209 — SYSTEMATIC FORENSICS: five discovery-machine hypotheses
tested empirically (TE unmapped ledger + dead slugs, description corpus,
bare-futures class, EODHD macro API, NFS-ladder freeze)."""
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
ssm = boto3.client("ssm", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def fetch(url, timeout=45):
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except Exception as e:
        return -1, str(e)[:120]


def main():
    with report("4209_systematic") as rep:
        rep.heading("ops 4209 — systematic discovery forensics")
        te = ssm.get_parameter(Name="/justhodl/te_api",
                               WithDecryption=True)["Parameter"]["Value"]
        eo = ssm.get_parameter(Name="/justhodl/eodhd_api",
                               WithDecryption=True)["Parameter"]["Value"]

        rep.section("H1a. TE categories we NEVER mapped (US dump)")
        fs = (ROOT / "lambdas" / "justhodl-te-feed" / "source" /
              "lambda_function.py").read_text()
        mapped = set(re.findall(r'"([^"]+)": "[A-Z0-9]+"', fs))
        st, x = fetch("https://api.tradingeconomics.com/country/"
                      "united%20states?c=" + te + "&f=json", 60)
        cats = []
        try:
            for r in json.loads(x):
                c = str(r.get("Category") or "")
                if c and c not in mapped and \
                        r.get("LatestValue") is not None:
                    cats.append(c)
        except Exception:
            pass
        rep.kv(us_categories_unmapped=len(set(cats)))
        rep.log("  unmapped sample: " + json.dumps(
            sorted(set(cats))[:36])[:800])

        rep.section("H1b. dead country slugs (silent 404s)")
        for cty in ("czech republic", "ivory coast", "east timor",
                    "macau", "swaziland", "cape verde"):
            st2, x2 = fetch("https://api.tradingeconomics.com/country/"
                            + urllib.request.quote(cty) + "?c=" + te
                            + "&f=json", 30)
            try:
                n2 = len(json.loads(x2))
            except Exception:
                n2 = -1
            rep.log(f"  '{cty}': status={st2} rows={n2}")

        rep.section("H4. description corpus over NFS bares")
        tv = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tv-sources.json")["Body"].read())
        rows = (tv.get("sources") or tv.get("by_symbol") or {})
        v = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        nfs = [str(r.get("symbol")) for r in v.get("symbols") or []
               if r.get("status") == "NO_FREE_SOURCE"]
        with_desc = 0
        samples = []
        for sy in nfs:
            rec = rows.get(sy) or rows.get("ECONOMICS:" + sy)
            if isinstance(rec, dict):
                d = (rec.get("description") or rec.get("desc")
                     or rec.get("name"))
                if d:
                    with_desc += 1
                    if len(samples) < 6:
                        samples.append([sy, str(d)[:48]])
        rep.kv(nfs_total=len(nfs), nfs_with_description=with_desc)
        rep.log("  desc samples: " + json.dumps(samples)[:400])

        rep.section("H2. bare futures-root class in NFS")
        roots = [sy for sy in nfs
                 if re.match(r"^[A-Z0-9]{1,4}1!$", sy)]
        rep.kv(bare_futures_roots=len(roots))
        rep.log("  roots sample: " + json.dumps(roots[:16]))

        rep.section("H6. EODHD macro-indicator API")
        for cc3, ind in (("CHE", "government_bond_10y"),
                         ("CHE", ""), ("MCO", "gdp_current_usd")):
            u = ("https://eodhd.com/api/macro-indicator/" + cc3
                 + "?api_token=" + eo + "&fmt=json"
                 + ("&indicator=" + ind if ind else ""))
            st3, x3 = fetch(u, 30)
            rep.log(f"  {cc3}/{ind or 'ALL'}: {st3} {x3[:130]}")

        rep.section("H3. NFS-ladder freeze size (yield class)")
        yields = [sy for sy in nfs
                  if re.match(r"^[A-Z]{2}\d{2}Y?$", sy)]
        rep.kv(nfs_yield_class=len(yields))
        rep.log("  yields sample: " + json.dumps(yields[:14]))
        rep.ok("SYSTEMATIC FORENSICS COMPLETE")


if __name__ == "__main__":
    main()
