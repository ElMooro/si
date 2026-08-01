"""ops_4223 — premium param-hunt + SSR transient recheck + feed refire."""
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=150,
                                 retries={"max_attempts": 1}))
BUCKET = "justhodl-dashboard-live"

CANDS = [("btc/fund-data/coinbase-premium-index",
          "&exchange=coinbase"),
         ("btc/fund-data/coinbase-premium-index",
          "&market=coinbase"),
         ("btc/fund-data/coinbase-premium-index",
          "&exchange=coinbase_pro"),
         ("btc/fund-data/coinbase-premium-gap",
          "&exchange=coinbase"),
         ("btc/fund-data/market-premium", "&exchange=coinbase"),
         ("btc/fund-data/korea-premium-index", "&exchange=upbit")]


def main():
    with report("4223_premium_hunt") as rep:
        rep.heading("ops 4223 — premium param hunt")
        key = ssm.get_parameter(Name="/justhodl/cryptoquant_api",
                                WithDecryption=True)["Parameter"]["Value"]
        cat = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/cq-catalog.json")["Body"].read())
        catalog = cat.get("catalog") or {}
        won = []
        for path, extra in CANDS:
            url = ("https://api.cryptoquant.com/v1/" + path
                   + "?window=day&limit=1" + extra)
            try:
                req = urllib.request.Request(
                    url, headers={"Authorization": "Bearer " + key,
                                  "User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=12) as r:
                    rows = ((json.loads(r.read().decode())
                             .get("result") or {}).get("data")) or []
                if rows:
                    row = rows[0]
                    flds = {k: row[k] for k in row
                            if k not in ("date", "datetime")}
                    catalog[path] = {
                        "fields": list(flds)[:6],
                        "sample": {k: flds[k]
                                   for k in list(flds)[:3]},
                        "asof": str(row.get("date"))[:10],
                        "extra": extra}
                    won.append(path)
                    rep.log(f"  WON {path}{extra}: "
                            + json.dumps(catalog[path]["sample"])[:100])
                    break
            except Exception as e:
                rep.log(f"  miss {path}{extra}: {str(e)[:40]}")
            time.sleep(0.2)
        cat["catalog"] = catalog
        cat["n"] = len(catalog)
        s3.put_object(Bucket=BUCKET, Key="data/cq-catalog.json",
                      Body=json.dumps(cat).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=3600")
        r0 = lam.invoke(FunctionName="justhodl-cq-feed",
                        InvocationType="RequestResponse", Payload=b"{}")
        cd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/cq-feed.json")["Body"].read())
        ssr = ((cd.get("metrics") or {}).get(
            "btc_market-indicator_stablecoin-supply-ratio") or {}
        ).get("fields") or {}
        rep.kv(catalog_n=len(catalog),
               feed_metrics=cd.get("n_metrics"),
               ssr_now=ssr.get("stablecoin_supply_ratio"),
               premium_won=bool(won))
        lam.invoke(FunctionName="justhodl-altseason",
                   InvocationType="Event", Payload=b"{}")
        lam.invoke(FunctionName="justhodl-coinbase-premium",
                   InvocationType="Event", Payload=b"{}")
        rep.ok(f"HUNT DONE — won={won} catalog={len(catalog)}")


if __name__ == "__main__":
    main()
