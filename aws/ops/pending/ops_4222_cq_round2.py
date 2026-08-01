"""ops_4222 — CQ round-2: ETH/derivatives/premium probes, catalog merge,
altseason+premium wires, bus+vault fire."""
import base64
import io
import json
import sys
import time
import urllib.request
import zipfile as zf
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

PROBES = [
    ("eth/exchange-flows/reserve", "&exchange=all_exchange"),
    ("eth/exchange-flows/netflow", "&exchange=all_exchange"),
    ("eth/market-indicator/mvrv", ""),
    ("eth/network-indicator/nupl", ""),
    ("eth/network-data/supply", ""),
    ("eth/flow-indicator/exchange-whale-ratio",
     "&exchange=all_exchange"),
    ("btc/market-data/funding-rates", "&exchange=all_exchange"),
    ("btc/market-data/open-interest", "&exchange=all_exchange"),
    ("btc/market-data/liquidations", "&exchange=all_exchange"),
    ("btc/market-data/taker-buy-sell-stats",
     "&exchange=all_exchange"),
    ("btc/fund-data/coinbase-premium-index", ""),
    ("btc/fund-data/coinbase-premium-gap", ""),
    ("btc/inter-entity-flows/exchange-to-exchange",
     "&from_exchange=all_exchange&to_exchange=all_exchange"),
]


def main():
    with report("4222_cq_round2") as rep:
        rep.heading("ops 4222 — CQ round-2 probes + wires")
        key = ssm.get_parameter(Name="/justhodl/cryptoquant_api",
                                WithDecryption=True)["Parameter"]["Value"]
        cat = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/cq-catalog.json")["Body"].read())
        catalog = cat.get("catalog") or {}
        new = []
        for path, extra in PROBES:
            if path in catalog:
                continue
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
                    catalog[path] = {"fields": list(flds)[:6],
                                     "sample": {k: flds[k] for k in
                                                list(flds)[:3]},
                                     "asof": str(row.get("date"))[:10],
                                     "extra": extra}
                    new.append(path)
                    rep.log(f"  NEW {path}: "
                            + json.dumps(catalog[path]["sample"])[:90])
                else:
                    rep.log(f"  dead {path}")
            except Exception as e:
                rep.log(f"  dead {path}: {str(e)[:40]}")
            time.sleep(0.2)
        cat["catalog"] = catalog
        cat["n"] = len(catalog)
        s3.put_object(Bucket=BUCKET, Key="data/cq-catalog.json",
                      Body=json.dumps(cat).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=3600")
        rep.kv(new_paths=len(new), catalog_n=len(catalog))

        r0 = lam.invoke(FunctionName="justhodl-cq-feed",
                        InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(cqfeed=r0["Payload"].read().decode()[:60])

        checks = []
        for name, blk in (("justhodl-altseason", "cq_ssr"),
                          ("justhodl-coinbase-premium", "cq_premium")):
            src = (ROOT / "lambdas" / name / "source" /
                   "lambda_function.py").read_text()
            assert blk in src
            import re as _re
            m2 = _re.search(
                r'(OUT_KEY|KEY|S3_KEY)\s*=\s*'
                r'"(data/[a-z0-9\-_]+\.json)"', src)
            outk = m2.group(2)
            buf = io.BytesIO()
            with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
                z.writestr("lambda_function.py", src)
                for sh in sorted((ROOT / "shared").glob("*.py")):
                    z.writestr(sh.name, sh.read_text())
            for att in range(5):
                try:
                    lam.update_function_code(FunctionName=name,
                                             ZipFile=buf.getvalue(),
                                             Publish=True)
                    break
                except Exception:
                    time.sleep(8)
            time.sleep(12)
            r = lam.invoke(FunctionName=name,
                           InvocationType="RequestResponse",
                           Payload=b"{}", LogType="Tail")
            tail = base64.b64decode(
                r.get("LogResult") or b"").decode("utf-8", "ignore")
            for ln in tail.splitlines():
                if blk in ln:
                    rep.log("  " + ln.strip()[:130])
            d = json.loads(s3.get_object(
                Bucket=BUCKET, Key=outk)["Body"].read())
            has = blk in d
            checks.append((f"{name} {blk} emitted", has))
            if blk == "cq_ssr" and has:
                sv = d[blk].get("ssr")
                checks.append(("ssr plausible 3-40",
                               isinstance(sv, (int, float))
                               and 3 < sv < 40))

        lam.invoke(FunctionName="justhodl-indicator-bus",
                   InvocationType="Event", Payload=b"{}")
        time.sleep(15)
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        rep.ok("  bus + vault fired — alias thaws tonight")

        failed = [l for l, k2 in checks if not k2]
        for l, k2 in checks:
            (rep.ok if k2 else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"CQ ROUND-2 — +{len(new)} paths, altseason+premium wired")


if __name__ == "__main__":
    main()
