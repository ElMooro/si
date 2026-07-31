"""ops_4197 — PAID WAVE keys to SSM + discovery probes: TE country dump
shape (Ticker mapping!), EODHD real-time, CryptoQuant auth."""
import json
import re
import sys
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

ssm = boto3.client("ssm", region_name="us-east-1")
KEYS = {"/justhodl/te_api": "BF59C0362D564C5:4DF6F2AA99FB40F",
        "/justhodl/eodhd_api": "6a543beea9ebe2.87551566",
        "/justhodl/cryptoquant_api":
        "iibYiFubTVOVm9JgXcXE1haQ3YHR8wSv9V9oX7Qv"}
UA = {"User-Agent": "Mozilla/5.0"}


def fetch(url, hdrs=None, timeout=45):
    try:
        req = urllib.request.Request(url, headers=hdrs or UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except Exception as e:
        return -1, str(e)[:200]


def main():
    with report("4197_paid_keys") as rep:
        rep.heading("ops 4197 — paid keys + discovery")
        for k, v in KEYS.items():
            ssm.put_parameter(Name=k, Value=v, Type="SecureString",
                              Overwrite=True)
            rep.ok(f"  SSM {k} = ...{v[-4:]}")

        rep.section("A. Trading Economics — country dump shape")
        te = KEYS["/justhodl/te_api"]
        st, x = fetch("https://api.tradingeconomics.com/country/"
                      f"mexico?c={te}&f=json")
        rep.kv(te_status=st, te_bytes=len(x))
        try:
            rows = json.loads(x)
            rep.kv(te_rows=len(rows))
            r0 = rows[0]
            rep.log("  keys: " + json.dumps(list(r0))[:300])
            for r2 in rows[:4]:
                rep.log("  row: " + json.dumps(
                    {k2: r2.get(k2) for k2 in
                     ("Country", "Category", "Ticker", "LatestValue",
                      "LatestValueDate", "Unit")})[:220])
            tickers = [str(r2.get("Ticker") or "") for r2 in rows]
            tv_like = sum(1 for t in tickers
                          if re.match(r"^[A-Z]{2,3}[A-Z0-9]{2,14}$",
                                      t.replace(" ", "")))
            rep.kv(tickers_tv_like=tv_like)
        except Exception as e2:
            rep.log(f"  TE parse: {type(e2).__name__} {x[:200]}")

        rep.section("B. EODHD — real-time + LSE bare")
        eo = KEYS["/justhodl/eodhd_api"]
        for sym in ("AAPL.US", "IEFM.LSE", "0P0000WA0M.LSE"):
            st2, x2 = fetch("https://eodhd.com/api/real-time/"
                            f"{sym}?api_token={eo}&fmt=json")
            rep.log(f"  {sym}: {st2} {x2[:140]}")

        rep.section("C. CryptoQuant — auth + one metric")
        cq = KEYS["/justhodl/cryptoquant_api"]
        for url in ("https://api.cryptoquant.com/v1/btc/market-data/"
                    "price-ohlcv?window=day&limit=1",
                    "https://api.cryptoquant.com/v1/btc/network-data/"
                    "supply?window=day&limit=1"):
            st3, x3 = fetch(url, {"Authorization": f"Bearer {cq}",
                                  "User-Agent": "Mozilla/5.0"})
            rep.log(f"  {url.split('v1/')[1][:40]}: {st3} {x3[:160]}")

        rep.ok("PAID DISCOVERY DONE")


if __name__ == "__main__":
    main()
