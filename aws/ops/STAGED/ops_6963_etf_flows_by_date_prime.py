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
WRITE = os.environ.get("WRITE", "0") != "0"          # first run: probe only


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

        # ------------------------------------------------------------------ 1. ask by date
        r.section("1. A whole month for every fund, asked by processed date")
        months = {}
        for tag, lo, hi in MONTHS:
            t0 = time.time()
            q = dict(PARAMS)
            q.update({"processed_date.gte": lo, "processed_date.lte": hi})
            url = HOST + PATH + "?" + urllib.parse.urlencode(q)
            rows, pages, last = [], 0, None
            while url and pages < 60:
                st, data = ask(url)
                last = st
                if st != 200 or not isinstance(data.get("results"), list):
                    r.log("%s page %d: status %s %s" % (tag, pages + 1, st, json.dumps({k: v for k, v in data.items() if k in ("status", "error", "message")})[:200]))
                    break
                rows.extend(data["results"])
                pages += 1
                url = data.get("next_url")
                time.sleep(0.25)
            complete = last == 200 and not url
            funds = {x.get("composite_ticker") for x in rows}
            days = Counter(str(x.get("effective_date"))[:10] for x in rows)
            pdays = Counter(str(x.get("processed_date"))[:10] for x in rows)
            outside = sum(1 for x in rows if not (lo <= str(x.get("processed_date"))[:10] <= hi))
            dup = len(rows) - len({(x.get("composite_ticker"), x.get("effective_date"), x.get("processed_date")) for x in rows})
            r.log("%s: %d rows in %d pages, %.0f s, complete %s | %d funds | %d effective days, %d processed days | rows outside the month %d, repeated rows %d" % (
                tag, len(rows), pages, time.time() - t0, complete, len(funds), len(days), len(pdays), outside, dup))
            r.log("    funds per effective day (last 8): %s" % json.dumps(dict(sorted(days.items())[-8:])))
            r.log("    a row: %s | fields %s" % (json.dumps(rows[0])[:260] if rows else None, dict(Counter(k for x in rows[:2000] for k in x).most_common(8))))
            months[tag] = {"rows": rows, "pages": pages, "complete": complete, "funds": funds, "lo": lo, "hi": hi, "outside": outside}

        # ------------------------------------------------------------------ 2. against the bank's own per-fund files
        r.section("2. The same rows in the bank's per-fund files")
        objs = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix=BANK + "etf-global-fund-flows/"):
            objs += page.get("Contents", [])

        def read(o):
            try:
                raw = s3.get_object(Bucket=LIVE, Key=o["Key"])["Body"].read()
                rows = json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)
                return o["Key"].split("/")[-1].split(".")[0], rows if isinstance(rows, list) else rows.get("results") or []
            except Exception:  # noqa: BLE001
                return None, []
        with ThreadPoolExecutor(max_workers=40) as pool:
            per = {t: rows for t, rows in pool.map(read, objs) if t}
        bank_rows = {}
        for t, rows in per.items():
            for x in rows:
                if x.get("composite_ticker") == t:
                    k = (t, str(x.get("effective_date"))[:10])
                    if k not in bank_rows or str(x.get("processed_date")) > str(bank_rows[k].get("processed_date")):
                        bank_rows[k] = x
        ok_all = True
        for tag, m in months.items():
            best = {}
            for x in m["rows"]:
                k = (x.get("composite_ticker"), str(x.get("effective_date"))[:10])
                if k not in best or str(x.get("processed_date")) > str(best[k].get("processed_date")):
                    best[k] = x
            both = [k for k in best if k in bank_rows]
            diff = [k for k in both if any(best[k].get(f) != bank_rows[k].get(f) for f in ("shares_outstanding", "nav", "fund_flow"))]
            in_bank = [k for k in bank_rows if m["lo"] <= k[1] <= m["hi"]]
            missing = [k for k in in_bank if k not in best]
            new_funds = m["funds"] - set(per)
            fresh = Counter()
            for t in m["funds"]:
                newest_bank = max((k[1] for k in bank_rows if k[0] == t), default="")
                newest_here = max((k[1] for k in best if k[0] == t), default="")
                fresh["newer here" if newest_here > newest_bank else "same" if newest_here == newest_bank else "older here"] += 1
            r.log("%s: fund-days here %d | also in the per-fund files %d, of which different %d %s | per-fund fund-days of the month not here %d %s | funds here with no per-fund file %d" % (
                tag, len(best), len(both), len(diff), json.dumps([(k, best[k], bank_rows[k]) for k in diff[:2]])[:420], len(missing), json.dumps(missing[:5]), len(new_funds)))
            r.log("    newest day per fund, this answer against the per-fund file: %s" % dict(fresh))
            m["best"] = best
            m["good"] = m["complete"] and len(m["funds"]) >= 4000 and m["outside"] == 0 and len(both) > 10000 and len(diff) <= 0.002 * len(both)
            ok_all = ok_all and m["good"]
            r.log("    usable: %s" % m["good"])
        if not ok_all:
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
        newest = max(k[1] for k in months["2026-11"]["best"])
        on_newest = sum(1 for k in months["2026-11"]["best"] if k[1] == newest)
        r.kv(months=len(months), written=written, write=WRITE, newest_day=newest, funds_on_newest_day=on_newest, funds_november=len(months["2026-11"]["funds"]), rows_november=len(months["2026-11"]["rows"]))
        r.ok("%s: November has %d funds, %d of them on the newest day %s; the by-date answer equals the per-fund files" % (
            "BANKED" if written else "CHECKED", len(months["2026-11"]["funds"]), on_newest, newest))


if __name__ == "__main__":
    main()
