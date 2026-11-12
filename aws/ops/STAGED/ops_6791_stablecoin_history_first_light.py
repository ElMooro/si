"""ops 6791 - first light for justhodl-stablecoin-flow 1.1 (commit 74326d82): invokes the engine ONCE synchronously
so data/stablecoin-supply-history.json exists before the widened Inflows & outflows desk (953795ff) reaches visitors,
then verifies the feed from S3: daily rows since 2017-11-29, total / USDT / USDC populated, latest day = today or
yesterday (DefiLlama's chart closes the UTC day), the regime output untouched. Read-only part afterwards, for the
Korea / Japan follow-up: the JPX investor-type listing's file links (the engine's stock_val regex finds none since
the page changed) and Naver's daily investor endpoints for the KOSPI (what history, if any, they expose)."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import re
import sys
import time

import requests

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-stablecoin-flow"
KEY = "data/stablecoin-supply-history.json"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36", "Accept-Language": "en-US,en;q=0.9"}


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    cfg = Config(retries={"max_attempts": 2, "mode": "standard"}, connect_timeout=10, read_timeout=180)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    facts = {}
    fails = []
    with report("ops_6791_stablecoin_history_first_light") as r:
        r.heading("ops 6791 - stablecoin supply history first light %s" % now.isoformat(timespec="minutes"))
        c = lam.get_function_configuration(FunctionName=FN)
        r.log("function: code last modified %s, sha %s, timeout %ss" % (c.get("LastModified"), str(c.get("CodeSha256"))[:12], c.get("Timeout")))
        try:
            before = s3.head_object(Bucket=LIVE, Key=KEY)["LastModified"]
            r.log("before: %s LastModified %s" % (KEY, before.isoformat(timespec="minutes")))
        except Exception:  # noqa: BLE001
            before = None
            r.log("before: %s absent (expected on first light)" % KEY)
        regime_before = s3.head_object(Bucket=LIVE, Key="data/stablecoin-flow.json")["LastModified"]
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        payload = json.loads(resp["Payload"].read() or b"{}")
        body = payload.get("body")
        try:
            body = json.loads(body) if isinstance(body, str) else body
        except Exception:  # noqa: BLE001
            pass
        r.log("invoke: status %s, function error %s, %.0fs, body %s" % (resp.get("StatusCode"), resp.get("FunctionError"), time.time() - t0, json.dumps(body, default=str)[:500]))
        hist = (body or {}).get("supply_history") if isinstance(body, dict) else None
        facts["supply_history"] = json.dumps(hist)[:200]
        if not (isinstance(hist, dict) and hist.get("ok")):
            fails.append("engine did not publish the history")
        doc = json.loads(s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read())
        series = doc.get("series") or []
        latest = series[-1] if series else {}
        r.log("feed: generated %s | %d days | first %s | latest %s | units %s | definitions %s" % (doc.get("generated_at"), doc.get("count"), doc.get("first_date"), json.dumps(latest), doc.get("units"), list((doc.get("definitions") or {}).keys())))
        recent = series[-400:]
        filled = {k: sum(1 for row in recent if row.get(k) is not None) for k in ("total", "usdt", "usdc", "minted", "bridged")}
        r.log("last 400 days filled: %s" % filled)
        if not (doc.get("count", 0) >= 3000 and doc.get("first_date") == "2017-11-29"):
            fails.append("history incomplete: %s days from %s" % (doc.get("count"), doc.get("first_date")))
        if not (latest.get("date") in (now.date().isoformat(), (now - timedelta(days=1)).date().isoformat())):
            fails.append("latest day %s is not today/yesterday" % latest.get("date"))
        if not (latest.get("total") and latest.get("total") > 1e11 and latest.get("usdt") and latest.get("usdc")):
            fails.append("latest totals not populated: %s" % json.dumps(latest))
        if filled["total"] < 395 or filled["usdt"] < 395 or filled["usdc"] < 395:
            fails.append("gaps in the last 400 days: %s" % filled)
        regime_after = s3.head_object(Bucket=LIVE, Key="data/stablecoin-flow.json")
        regime = json.loads(s3.get_object(Bucket=LIVE, Key="data/stablecoin-flow.json")["Body"].read())
        r.log("regime output: LastModified %s -> %s | version %s | state %s | total %s" % (regime_before.isoformat(timespec="minutes"), regime_after["LastModified"].isoformat(timespec="minutes"), regime.get("version"), regime.get("state"), (regime.get("aggregate") or {}).get("total_usd")))
        if regime.get("version") != "1.1":
            fails.append("regime output version %s" % regime.get("version"))
        facts["days"] = doc.get("count")
        facts["latest"] = "%s total %s usdt %s usdc %s" % (latest.get("date"), latest.get("total"), latest.get("usdt"), latest.get("usdc"))

        r.section("Korea / Japan sources from the runner (read-only)")
        try:
            page = requests.get("https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/index.html", headers=UA, timeout=30)
            links = sorted(set(re.findall(r'href="([^"]+\.(?:xls|xlsx|pdf|csv))"', page.text, re.I)))
            r.log("JPX investor-type listing: HTTP %s, %d bytes, %d file links: %s" % (page.status_code, len(page.content), len(links), links[:12]))
            weekly = [u for u in links if re.search(r"stock|week|wk|val", u, re.I)]
            if weekly:
                u = weekly[0] if weekly[0].startswith("http") else "https://www.jpx.co.jp" + weekly[0]
                x = requests.get(u, headers=UA, timeout=45)
                r.log("newest candidate %s: HTTP %s, %d KB, %s" % (u[-60:], x.status_code, len(x.content) // 1024, x.headers.get("content-type", "")[:40]))
            sub = re.findall(r'href="(/english/markets/statistics-equities/investor-type/[^"]+)"', page.text)
            r.log("sub-pages: %s" % sorted(set(sub))[:12])
            facts["jpx_links"] = len(links)
        except Exception as exc:  # noqa: BLE001
            r.warn("JPX: %s %s" % (type(exc).__name__, str(exc)[:120]))
        for label, url in (("Naver KOSPI trend", "https://m.stock.naver.com/api/index/KOSPI/trend"), ("Naver KOSPI investor daily (pageSize 60)", "https://m.stock.naver.com/api/index/KOSPI/investorDaily?pageSize=60&page=1"),
                           ("Naver KOSPI investor (alt path)", "https://m.stock.naver.com/api/index/KOSPI/investor?pageSize=60&page=1"), ("Naver KOSDAQ trend", "https://m.stock.naver.com/api/index/KOSDAQ/trend"),
                           ("Naver sise investor daily (classic)", "https://finance.naver.com/sise/investorDealTrendDay.naver?bizdate=20261112&sosok=01")):
            try:
                x = requests.get(url, headers={**UA, "Referer": "https://m.stock.naver.com/"}, timeout=30)
                txt = x.text
                shape = ""
                try:
                    j = x.json()
                    shape = "list[%d] first %s" % (len(j), json.dumps(j[0])[:200]) if isinstance(j, list) and j else "dict keys %s" % list(j.keys())[:10] if isinstance(j, dict) else str(j)[:120]
                except Exception:  # noqa: BLE001
                    rows = re.findall(r"<td[^>]*>([^<]{1,40})</td>", txt)
                    shape = "html, %d td cells, first %s" % (len(rows), rows[:12])
                r.log("%s: HTTP %s, %d bytes | %s" % (label, x.status_code, len(x.content), shape[:400]))
            except Exception as exc:  # noqa: BLE001
                r.warn("%s: %s" % (label, type(exc).__name__))
        r.kv(**{k: str(v) for k, v in facts.items()}, fails=str(fails))
        if fails:
            r.fail("first light failed: %s" % fails)
        else:
            r.ok("stablecoin supply history live: %s days, latest %s" % (doc.get("count"), latest.get("date")))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
