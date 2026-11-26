"""ops 6963 - every ETF's flow for every day of a month, in one question to the provider: probe, check, bank.

The bank asks the provider fund by fund, 180 funds a run, so most of its 5,789 flow files are days or weeks old
(ops 6962: 1,628 funds at the newest day, 3,258 files older than eight days). The same provider answers a whole month
for every fund at once when asked by date (about 27 pages). This op

  1. asks for 2026-10 and 2026-11 by processed date, pages through, counts funds and days;
  2. checks the answer against the bank's own per-fund files, row for row, on every fund that has both;
  3. only if the check holds, banks the two months where the bank's new dataset `etf-global-fund-flows-by-date`
     (kind window, added to polygon-bank-spec.json in the same change) will keep them from its next daily run on:
     data/warm/polygon-bank/etf-global-fund-flows-by-date/<YYYY-MM>.json.gz, written as the bank writes
     (the rows as answered, gzip, sha256 in the metadata, nothing overwritten that is already the same).

A one-off fetch (the data-plane rule: pullers and one-off ops fetch; engines read the warehouse). The provider key is
read from the parameter store, sent in a header, and never printed. No engine is invoked. Run with event WRITE=0 in
the environment to probe without banking.
"""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import gzip
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
NAME = "ops_6963_etf_flows_by_date_prime"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
HOST = "https://api.polygon.io"
PATH = "/etf-global/v1/fund-flows"
BANK = "data/warm/polygon-bank/"
DATASET = "etf-global-fund-flows-by-date"
MONTHS = (("2026-10", "2026-10-01", "2026-10-31"), ("2026-11", "2026-11-01", "2026-11-30"))
PARAMS = {"limit": 5000, "sort": "processed_date.desc"}
WRITE = os.environ.get("WRITE", "1") != "0"          # two probe runs came first (rows equal the per-fund files)


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=90, max_pool_connections=48)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)

    with report(NAME) as r:
        r.heading("ops 6963 - every ETF's flows by date: probe, check, bank (%s)" % now.isoformat(timespec="minutes"))
        try:
            key = boto3.client("ssm", region_name=REGION, config=cfg).get_parameter(Name="/justhodl/polygon/api-key", WithDecryption=True)["Parameter"]["Value"]
        except Exception as exc:  # noqa: BLE001
            r.fail("the provider key could not be read: %s" % type(exc).__name__)
            sys.exit(1)
        headers = {"Authorization": "Bearer " + key, "User-Agent": "JustHodl Research ops/6963", "Accept-Encoding": "gzip"}

        def ask(url):
            """One page. The address is never logged: only the status and the provider's own short message."""
            if urllib.parse.urlsplit(url).netloc != "api.polygon.io":
                return 0, {"error": "refused: not the provider's host"}
            for attempt in range(5):
                try:
                    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=90) as resp:
                        raw = resp.read()
                        if resp.headers.get("Content-Encoding") == "gzip" or raw[:2] == b"\x1f\x8b":
                            raw = gzip.decompress(raw)
                        return resp.status, json.loads(raw)
                except urllib.error.HTTPError as e:
                    body = e.read()[:300].decode("utf-8", "replace")
                    if e.code == 429 and attempt < 4:
                        time.sleep(8 * (attempt + 1))
                        continue
                    try:
                        msg = json.loads(body)
                        msg = {k: str(msg.get(k))[:160] for k in ("status", "error", "message") if k in msg}
                    except Exception:  # noqa: BLE001
                        msg = {"error": "http %d" % e.code}
                    return e.code, msg
                except Exception as exc:  # noqa: BLE001
                    if attempt < 4:
                        time.sleep(4)
                        continue
                    return 0, {"error": type(exc).__name__}
            return 0, {"error": "gave up"}

        # ------------------------------------------------------------------ 1. the per-fund files are the truth to check against
        r.section("1. The bank's per-fund files (their newest 45 rows each)")
        objs = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix=BANK + "etf-global-fund-flows/"):
            objs += page.get("Contents", [])

        def read(o):
            try:
                raw = s3.get_object(Bucket=LIVE, Key=o["Key"])["Body"].read()
                rows = json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)
                rows = rows if isinstance(rows, list) else rows.get("results") or []
                return o["Key"].split("/")[-1].split(".")[0], sorted(rows, key=lambda x: str(x.get("effective_date")), reverse=True)[:45]
            except Exception:  # noqa: BLE001
                return None, []
        with ThreadPoolExecutor(max_workers=40) as pool:
            per = {t: rows for t, rows in pool.map(read, objs) if t}
        truth = {}
        for t, rows in per.items():
            for x in rows:
                if x.get("composite_ticker") == t:
                    truth[(t, str(x.get("effective_date"))[:10])] = x
        r.log("%d funds, %d fund-days" % (len(per), len(truth)))

        def pull(q, cap=80):
            url = HOST + PATH + "?" + urllib.parse.urlencode(q)
            rows, pages, last = [], 0, None
            while url and pages < cap:
                st, data = ask(url)
                last = st
                if st != 200 or not isinstance(data.get("results"), list):
                    r.log("    page %d: status %s %s" % (pages + 1, st, json.dumps({k: v for k, v in data.items() if k in ("status", "error", "message")})[:200]))
                    break
                rows.extend(data["results"])
                pages += 1
                url = data.get("next_url")
                time.sleep(0.25)
            return rows, pages, (last == 200 and not url)

        def judge(tag, rows, lo, hi):
            best = {}
            for x in rows:
                k = (x.get("composite_ticker"), str(x.get("effective_date"))[:10])
                if k not in best or str(x.get("processed_date")) > str(best[k].get("processed_date")):
                    best[k] = x
            want = [k for k in truth if lo <= k[1] <= hi]
            got = [k for k in want if k in best]
            diff = [k for k in got if any(best[k].get(f) != truth[k].get(f) for f in ("shares_outstanding", "fund_flow")) or abs(float(best[k].get("nav") or 0) - float(truth[k].get("nav") or 0)) > 0.01]
            days = Counter(k[1] for k in best)
            r.log("%s: %d rows, %d funds, %d days | per day (last 4) %s | of the per-fund files' %d fund-days it has %d (%.1f%%), different %d | first and last fund %s .. %s" % (
                tag, len(rows), len({k[0] for k in best}), len(days), json.dumps(dict(sorted(days.items())[-4:])), len(want), len(got), 100.0 * len(got) / max(1, len(want)), len(diff),
                min(k[0] for k in best) if best else None, max(k[0] for k in best) if best else None))
            return best, len(got) == len(want) and not diff

        # ------------------------------------------------------------------ 2. ask by date, funds A to Z, and check
        r.section("2. Each month asked by processed date, funds A to Z (the order that returns every row), checked against the per-fund files")
        months = {}
        for tag, lo, hi in MONTHS:
            t0 = time.time()
            rows, pages, complete = pull({"processed_date.gte": lo, "processed_date.lte": hi, "limit": 5000, "sort": "composite_ticker.asc"})
            best = {}
            for x in rows:
                k = (x.get("composite_ticker"), str(x.get("effective_date"))[:10])
                if k not in best or str(x.get("processed_date")) > str(best[k].get("processed_date")):
                    best[k] = x
            want = [k for k in truth if lo <= str(truth[k].get("processed_date"))[:10] <= hi]
            got = [k for k in want if k in best]
            diff = [k for k in got if any(best[k].get(f) != truth[k].get(f) for f in ("shares_outstanding", "fund_flow")) or abs(float(best[k].get("nav") or 0) - float(truth[k].get("nav") or 0)) > 0.01]
            days = Counter(k[1] for k in best)
            outside = sum(1 for x in rows if not (lo <= str(x.get("processed_date"))[:10] <= hi))
            good = complete and outside == 0 and len({k[0] for k in best}) >= 5000 and len(got) >= 0.999 * len(want) and len(diff) <= 0.001 * max(1, len(got))
            r.log("%s: %d rows in %d pages, %.0f s, complete %s | %d funds, %d days, per day (last 4) %s | of the per-fund files' %d fund-days it has %d, different %d %s | outside the month %d | usable %s" % (
                tag, len(rows), pages, time.time() - t0, complete, len({k[0] for k in best}), len(days), json.dumps(dict(sorted(days.items())[-4:])), len(want), len(got), len(diff),
                json.dumps([(k, best[k].get("fund_flow"), truth[k].get("fund_flow"), best[k].get("processed_date"), truth[k].get("processed_date")) for k in diff[:3]])[:300], outside, good))
            months[tag] = {"rows": rows, "pages": pages, "best": best, "good": good, "lo": lo, "hi": hi}
        if not all(m["good"] for m in months.values()):
            r.fail("the by-date answer did not pass the check; nothing was banked")
            sys.exit(1)

        # ------------------------------------------------------------------ 3. bank the two months as the bank will
        r.section("3. Banked where the bank's new dataset keeps them")
        written = 0
        for tag, m in months.items():
            keyname = "%s%s/%s.json.gz" % (BANK, DATASET, tag)
            body = json.dumps(m["rows"], separators=(",", ":"), ensure_ascii=False, default=str).encode("utf-8")
            sha = hashlib.sha256(body).hexdigest()
            try:
                have = (s3.head_object(Bucket=LIVE, Key=keyname).get("Metadata") or {})
            except Exception:  # noqa: BLE001
                have = None
            if have and have.get("sha256") == sha:
                r.log("%s: already banked with the same content" % keyname)
                continue
            if not WRITE:
                r.log("%s: would be written (%d rows, %.1f MB) - probe only" % (keyname, len(m["rows"]), len(body) / 1e6))
                continue
            gz = gzip.compress(body, compresslevel=6)
            s3.put_object(Bucket=LIVE, Key=keyname, Body=gz, ContentType="application/json", ContentEncoding="gzip",
                          Metadata={"path": PATH, "from": m["lo"], "to": m["hi"], "pages": str(m["pages"]), "sha256": sha, "content-sha256": sha, "engine": "justhodl-polygon-bank",
                                    "primed-by": "ops-6963"})
            back = json.loads(gzip.decompress(s3.get_object(Bucket=LIVE, Key=keyname)["Body"].read()))
            r.log("%s: written, %d rows, %.1f MB stored (%.1f MB as text); read back %d rows, equal %s" % (keyname, len(m["rows"]), len(gz) / 1e6, len(body) / 1e6, len(back), back == m["rows"]))
            written += 1
        nov = months["2026-11"]["best"]
        newest = max(k[1] for k in nov)
        on_newest = sum(1 for k in nov if k[1] == newest)
        r.kv(months=len(months), written=written, write=WRITE, newest_day=newest, funds_on_newest_day=on_newest, funds_november=len({k[0] for k in nov}), rows_november=len(months["2026-11"]["rows"]),
             per_fund_files_on_newest_day=sum(1 for k in truth if k[1] == newest))
        r.ok("%s: November has %d funds, %d of them on the newest day %s (the per-fund files: %d)" % ("BANKED" if written else "CHECKED", len({k[0] for k in nov}), on_newest, newest,
                                                                                                       sum(1 for k in truth if k[1] == newest)))


if __name__ == "__main__":
    main()
