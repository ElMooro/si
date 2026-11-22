"""ops 6900 - READ-ONLY: what the NY Fed dealer API accepts today, and DTCC's moved files without a browser.

ops 6899 found (1) justhodl-nyfed-pd has published nothing since 2026-11-06 because its one request for all 70
series (`/api/pd/get/<70 ids joined by _>.json`) answers 422 since 2026-11-12, and (2) DTCC's daily fails file moved
from www.dtcc.com/data/failsdata.csv to cms-prod.dtcc.com/data/failsdata.csv (a real browser showed the request).

Measured here, nothing written:
  1. the 422's own words; how many series one request may name (1 .. 70); whether the answers of split requests,
     put together, hold every series through the newest week;
  2. cms-prod.dtcc.com/data/failsdata.csv and gcfindex.csv fetched the way a Lambda would (plain urllib, the engine's
     own header): status, rows, first and last day, the trailer line.
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
BASE = "https://markets.newyorkfed.org/api/pd/get/"


def http(url, headers, timeout=45):
    t0 = time.time()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=timeout) as resp:
            return resp.status, resp.read(), time.time() - t0, {k.lower(): v for k, v in resp.headers.items()}
    except urllib.error.HTTPError as e:
        body = b""
        try:
            body = e.read()
        except Exception:  # noqa: BLE001
            pass
        return e.code, body, time.time() - t0, {k.lower(): v for k, v in (e.headers or {}).items()}
    except Exception as e:  # noqa: BLE001
        return type(e).__name__, b"", time.time() - t0, {}


def chunks(keys, size):
    return [keys[i:i + size] for i in range(0, len(keys), size)]


def selftest():
    assert chunks(list("abcde"), 2) == [["a", "b"], ["c", "d"], ["e"]] and chunks([], 3) == []


def main():
    selftest()
    from ops_report import report
    sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-nyfed-pd" / "source"))
    import dealer_original as native
    keys = list(native.KEYS)
    head = {"User-Agent": "JustHodl source research", "Accept": "*/*"}
    with report("ops_6900_pd_api_and_dtcc_probe") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6900 - the dealer API's limit and DTCC's moved files (read-only), %s" % now.isoformat(timespec="minutes"))
        facts = {}

        r.section("1. NY Fed primary dealer API")
        code, raw, secs, hdr = http(native.DATA_URL, head)
        r.log("all %d ids in one request (%d characters of URL): %s, %d bytes | body: %s | headers %s" % (
            len(keys), len(native.DATA_URL), code, len(raw), raw[:400].decode("utf-8", "replace").replace("\n", " "), {k: v for k, v in hdr.items() if k in ("content-type", "x-amzn-errortype", "server", "via")}))
        accepted = 0
        for n in (1, 2, 5, 10, 15, 20, 25, 30, 35, 40, 50, 60, 69, 70):
            url = BASE + "_".join(keys[:n]) + ".json"
            code, raw, secs, _ = http(url, head)
            rows = 0
            try:
                rows = len(json.loads(raw).get("pd", {}).get("timeseries", []))
            except Exception:  # noqa: BLE001
                pass
            r.log("  first %2d ids, URL %4d characters: %s, %7d bytes, %6d rows, %.1f s%s" % (n, len(url), code, len(raw), rows, secs, "" if code == 200 else " | " + raw[:160].decode("utf-8", "replace").replace("\n", " ")))
            if code == 200 and rows:
                accepted = max(accepted, n)
            time.sleep(0.4)
        facts["ids_one_request_accepts"] = accepted
        # the last ids alone, to tell a count limit from one identity the API no longer knows
        for label, part in (("last 10 ids", keys[-10:]), ("ids 31-40", keys[30:40]), ("ids 41-50", keys[40:50]), ("ids 51-60", keys[50:60])):
            code, raw, secs, _ = http(BASE + "_".join(part) + ".json", head)
            r.log("  %s: %s, %d bytes%s" % (label, code, len(raw), "" if code == 200 else " | " + raw[:160].decode("utf-8", "replace")))
            time.sleep(0.4)
        if accepted < len(keys):
            bad = []
            for k in keys:
                code, raw, secs, _ = http(BASE + k + ".json", head)
                if code != 200:
                    bad.append((k, code, raw[:120].decode("utf-8", "replace")))
                time.sleep(0.15)
            r.log("  each id alone: %d of %d answer 200 | not 200: %s" % (len(keys) - len(bad), len(keys), bad[:10]))
            facts["ids_alone_refused"] = len(bad)
        for size in (10, 14, 20, 24):
            merged, ok, total, t0 = {}, True, 0, time.time()
            for part in chunks(keys, size):
                code, raw, secs, _ = http(BASE + "_".join(part) + ".json", head)
                if code != 200:
                    ok = False
                    r.log("  parts of %d: %s for %s..." % (size, code, part[0]))
                    break
                total += len(raw)
                for row in json.loads(raw).get("pd", {}).get("timeseries", []):
                    merged.setdefault(row.get("keyid"), {})[row.get("asofdate")] = row.get("value")
                time.sleep(0.3)
            newest = Counter(max(v) for v in merged.values()) if merged else Counter()
            r.log("  parts of %d ids (%d requests): ok %s | %d series | %d bytes | %.1f s | newest weeks %s | ids not answered %s" % (
                size, len(chunks(keys, size)), ok, len(merged), total, time.time() - t0, dict(newest.most_common(3)), sorted(set(keys) - set(merged))[:6]))
            if ok and len(merged) == len(keys):
                facts.setdefault("split_size_that_works", size)
                facts["newest_week"] = max(newest)
        code, raw, secs, _ = http("https://markets.newyorkfed.org/api/pd/get/all/timeseries.csv", head, timeout=90)
        r.log("  /api/pd/get/all/timeseries.csv: %s, %d bytes, %.1f s | %s" % (code, len(raw), secs, raw[:200].decode("utf-8", "replace").replace("\n", " | ")))
        for path in ("latest/SBN2024.json", "get/SBN2024/timeseries/" + "_".join(keys[:3]) + ".json", "get/SBN2024/timeseries/" + "_".join(keys) + ".json"):
            code, raw, secs, _ = http("https://markets.newyorkfed.org/api/pd/" + path, head, timeout=90)
            r.log("  /api/pd/%s: %s, %d bytes, %.1f s | %s" % (path[:70], code, len(raw), secs, raw[:160].decode("utf-8", "replace").replace("\n", " ")))

        r.section("2. DTCC's moved files, fetched without a browser")
        for name in ("failsdata.csv", "gcfindex.csv"):
            for label, headers in (("engine header", {"User-Agent": "Mozilla/5.0"}), ("plain urllib", {}), ("browser header", {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36", "Referer": "https://www.dtcc.com/"})):
                code, raw, secs, hdr = http("https://cms-prod.dtcc.com/data/" + name, headers)
                lines = raw.decode("utf-8", "replace").splitlines()
                data = [x for x in lines if x[:2].isdigit()]
                r.log("%s, %s: %s, %d bytes, type %s, last-modified %s | %d lines, %d dated | head %r | first %r | last %r | tail %r" % (
                    name, label, code, len(raw), hdr.get("content-type"), hdr.get("last-modified"), len(lines), len(data), lines[0][:110] if lines else None,
                    data[0][:70] if data else None, data[-1][:70] if data else None, lines[-1][:80] if lines else None))
                if name == "failsdata.csv" and code == 200:
                    facts["dtcc_fails_rows"] = len(data)
                    facts["dtcc_fails_last"] = data[-1].split(",")[0] if data else None
                time.sleep(0.3)
        r.kv(**facts)
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
