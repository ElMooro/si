"""ops_4220 — CQ CATALOG HARVEST: probe the endpoint universe, bank what
answers with fields+samples. The $109 stops being wasted here."""
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
ssm = boto3.client("ssm", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

PATHS = []
for coin in ("btc", "eth"):
    for fam, mets in (
        ("exchange-flows", ["reserve", "netflow", "inflow", "outflow",
                            "transactions-count", "in-house-flow"]),
        ("flow-indicator", ["mpi", "exchange-whale-ratio",
                            "fund-flow-ratio", "stablecoins-ratio",
                            "exchange-shutdown-index"]),
        ("market-indicator", ["estimated-leverage-ratio", "sopr",
                              "sopr-ratio", "mvrv", "realized-price",
                              "nvt", "nvt-golden-cross",
                              "stablecoin-supply-ratio",
                              "puell-multiple"]),
        ("network-indicator", ["nupl", "nvm"]),
        ("miner-flows", ["reserve", "netflow", "outflow", "inflow"]),
        ("fund-data", ["coinbase-premium-index",
                       "korea-premium-index", "fund-volume",
                       "market-premium"]),
        ("market-data", ["price-ohlcv", "open-interest",
                         "funding-rates", "taker-buy-sell-stats",
                         "liquidations", "estimated-leverage-ratio"]),
        ("network-data", ["hashrate", "supply", "difficulty",
                          "fees", "blockreward", "transactions-count",
                          "addresses-count", "utxo-count"]),
    ):
        for m in mets:
            PATHS.append(f"{coin}/{fam}/{m}")


def main():
    with report("4220_cq_catalog") as rep:
        rep.heading("ops 4220 — CryptoQuant catalog harvest")
        key = ssm.get_parameter(Name="/justhodl/cryptoquant_api",
                                WithDecryption=True)["Parameter"]["Value"]
        cat = {}
        dead = []
        for path in PATHS:
            extra = ("&exchange=all_exchange"
                     if "flow" in path or "reserve" in path
                     or "transactions" in path else "")
            extra2 = ("&miner=all_miner"
                      if path.split("/")[1] == "miner-flows" else "")
            extra3 = ("&market=all_market"
                      if path.split("/")[1] == "market-data"
                      and "price" not in path else "")
            url = ("https://api.cryptoquant.com/v1/" + path
                   + "?window=day&limit=1" + extra + extra2 + extra3)
            try:
                req = urllib.request.Request(
                    url, headers={"Authorization": "Bearer " + key,
                                  "User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=12) as r:
                    j = json.loads(r.read().decode())
                rows = ((j.get("result") or {}).get("data")) or []
                if rows:
                    row = rows[0]
                    flds = {k: row[k] for k in row
                            if k not in ("date", "datetime")}
                    cat[path] = {"fields": list(flds)[:6],
                                 "sample": {k: flds[k] for k in
                                            list(flds)[:3]},
                                 "asof": str(row.get("date"))[:10],
                                 "extra": extra + extra2 + extra3}
                else:
                    dead.append(path)
            except Exception as e:
                dead.append(path + ":" + str(e)[:24])
            time.sleep(0.15)
        rep.kv(probed=len(PATHS), live=len(cat), dead=len(dead))
        rep.section("LIVE catalog")
        for p, r2 in sorted(cat.items()):
            rep.log(f"  {p}: {json.dumps(r2['sample'])[:90]}")
        rep.log("  dead sample: " + json.dumps(dead[:10])[:300])
        s3.put_object(Bucket=BUCKET, Key="data/cq-catalog.json",
                      Body=json.dumps(
                          {"marker": "cq-catalog v1 ops4220",
                           "n": len(cat), "catalog": cat,
                           "dead": dead}).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=3600")
        if len(cat) < 20:
            rep.fail(f"only {len(cat)} live paths")
            sys.exit(1)
        rep.ok(f"CQ CATALOG — {len(cat)} live metrics banked")


if __name__ == "__main__":
    main()
