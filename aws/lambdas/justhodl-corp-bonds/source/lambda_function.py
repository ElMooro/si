"""justhodl-corp-bonds — company holdings, TRACE tape, ECB book, ICE sleeves.

Owns data/corp-bonds.json. Measurement only. A failed feed is recorded in
sources and left out of the page; it is never filled with a zero.
"""
import json
import os
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

import boto3

import corp_bonds_core as core
import corp_bonds_issues as issues
import finra_trace

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
PACKET_KEY = os.environ.get("PACKET_KEY", "data/corp-bonds.json")
CDS_KEY = "data/cds-desk.json"
ECB_INDEX = "https://www.ecb.europa.eu/mopo/implement/app/html/index.en.html"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
FRED = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=%s"

s3 = boto3.client("s3")


def _get(url, timeout=40):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "text/csv,text/html,*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


def _get_bytes(url, timeout=25):
    # urlopen's timeout resets on every socket read, so a trickled file can
    # outlive it. The monotonic deadline is the real cap, plus one blocked read.
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": "text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
    })
    deadline = time.monotonic() + timeout
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        chunks, total = [], 0
        while True:
            if time.monotonic() > deadline:
                raise TimeoutError(url)
            block = resp.read(1 << 16)
            if not block:
                break
            total += len(block)
            if total > 25000000:
                raise TimeoutError("oversized")
            chunks.append(block)
        return b"".join(chunks)


def _one_fund(fund):
    try:
        blob = _get_bytes(fund["url"])
    except Exception as exc:
        return fund, [], type(exc).__name__, None
    try:
        if fund["kind"] == "ssga":
            rows, as_of = issues.parse_ssga(blob, fund)
        else:
            rows, as_of = issues.parse_ishares(blob.decode("utf-8", "replace"), fund)
    except Exception as exc:
        return fund, [], "parse %s" % type(exc).__name__, None
    return fund, rows, "live" if rows else "empty", as_of


def fetch_issues(prior_bonds):
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(_one_fund, issues.FUNDS))
    return issues.assemble(results, prior_bonds)


def _get_json(key):
    try:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except Exception as exc:
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code in ("NoSuchKey", "404") or type(exc).__name__ == "NoSuchKey":
            return None
        return None
    return json.loads(body)


def fetch_fred(spec):
    try:
        text = _get(FRED % spec["fred"], timeout=30)
    except Exception as exc:
        return None, "%s %s" % (type(exc).__name__, exc)
    points = core.parse_fred_csv(text)
    if len(points) < 5:
        return None, "no observations"
    return points, "live"


def fetch_ecb():
    """Prefer the filename the ECB index page is currently linking."""
    errors = []
    names = []
    try:
        page = _get(ECB_INDEX, timeout=30)
        names = []
        for token in page.replace("'", '"').split('"'):
            base = token.split("?")[0]
            if "CSPP_PEPP_corporate_bond_holdings_" in base and base.endswith(".csv"):
                names.append(base)
    except Exception as exc:
        errors.append("index %s" % type(exc).__name__)
    urls = []
    for name in names:
        urls.append(name if name.startswith("http") else "https://www.ecb.europa.eu" + name.split("?")[0])
    day = date.today()
    for offset in range(0, 35):
        stamp = (day - timedelta(days=offset)).strftime("%Y%m%d")
        urls.append("https://www.ecb.europa.eu/mopo/pdf/CSPP_PEPP_corporate_bond_holdings_%s.csv" % stamp)
    seen = set()
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        try:
            text = _get(url, timeout=40)
        except urllib.error.HTTPError as exc:
            if exc.code not in (403, 404):
                errors.append("HTTP %s" % exc.code)
            continue
        except Exception as exc:
            errors.append(type(exc).__name__)
            continue
        bonds = core.parse_ecb_holdings(text)
        if len(bonds) >= 50:
            stamp = url.rsplit("_", 1)[-1].replace(".csv", "")
            return bonds, {"url": url.split("?")[0], "file_date": stamp, "status": "live"}
    return [], {"url": None, "file_date": None, "status": "unavailable", "errors": errors[:6]}


def _finra(dataset):
    start = (date.today() - timedelta(days=12)).isoformat()
    end = date.today().isoformat()
    rows = finra_trace.query(dataset, date_range_filters=[{
        "fieldName": "tradeReportDate", "startDate": start, "endDate": end,
    }], limit=4000)
    if rows is None:
        rows = finra_trace.query(dataset, date_range_filters=[{
            "fieldName": "tradeDate", "startDate": start, "endDate": end,
        }], limit=4000)
    return rows


def fetch_trace():
    datasets = {
        "corporate_breadth": "corporateMarketBreadth",
        "corporate_sentiment": "corporateMarketSentiment",
        "m144a_breadth": "corporate144AMarketBreadth",
        "m144a_sentiment": "corporate144AMarketSentiment",
        "capped": "corporatesAndAgenciesCappedVolume",
    }
    raw, status = {}, {}
    for name, dataset in datasets.items():
        try:
            rows = _finra(dataset)
        except Exception as exc:
            rows = None
            status[name] = "error %s" % type(exc).__name__
            continue
        raw[name] = rows
        status[name] = "live %s rows" % len(rows) if isinstance(rows, list) else "unavailable"
    trace = {
        "corporate": {
            "breadth": core.compact_breadth(core.latest_slice(raw.get("corporate_breadth"))),
            "sentiment": core.compact_sentiment(core.latest_slice(raw.get("corporate_sentiment"))),
        },
        "rule_144a": {
            "breadth": core.compact_breadth(core.latest_slice(raw.get("m144a_breadth"))),
            "sentiment": core.compact_sentiment(core.latest_slice(raw.get("m144a_sentiment"))),
        },
        "capped_volume": core.compact_capped(core.latest_slice(raw.get("capped"))),
        "status": status,
        "note": "Aggregates of the disseminated TRACE corporate tape. Not executable quotes and not a CUSIP blotter.",
    }
    return trace


def publish():
    generated_at, today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), date.today().isoformat()
    sources = {"fred": {}, "ecb": {}, "trace": {}, "cds": "not joined", "issues": {}}
    prior = _get_json(PACKET_KEY) or {}
    prior_bonds = ((prior.get("issues") or {}).get("bonds")) or []
    # Holdings files overlap FRED, the euro book and TRACE. They were sequential
    # before, and that sum crossed the 300s task limit before the packet was written.
    issue_pool = ThreadPoolExecutor(max_workers=1)
    issue_future = issue_pool.submit(fetch_issues, prior_bonds)
    sleeves = []
    for spec in core.SLEEVES:
        points, status = fetch_fred(spec)
        sources["fred"][spec["fred"]] = status
        if points:
            row = core.sleeve_stats(spec, points, today_iso)
            if row:
                sleeves.append(row)
    bonds, ecb_meta = fetch_ecb()
    sources["ecb"] = ecb_meta
    euro = core.euro_book(bonds, today_iso) if bonds else {
        "n_bonds": 0, "n_issuers": 0, "n_countries": 0,
        "by_country": [], "maturity_wall": [], "issuers": [], "bonds": [],
        "status": ecb_meta.get("status"),
    }
    euro["file_date"] = ecb_meta.get("file_date")
    euro["source_url"] = ecb_meta.get("url")
    cds_matched = 0
    cds = _get_json(CDS_KEY)
    if cds:
        cds_matched = core.attach_cds(euro.get("issuers") or [], cds)
        sources["cds"] = "joined %s issuers from data/cds-desk.json" % cds_matched
    trace = fetch_trace()
    sources["trace"] = trace.get("status")
    try:
        issue_book = issue_future.result()
        sources["issues"] = {row["id"]: "%s %s" % (row["status"], row["n"]) for row in issue_book.get("funds") or []}
    except Exception as exc:
        issue_book = None
        sources["issues"] = "error %s" % type(exc).__name__
    finally:
        issue_pool.shutdown(wait=False, cancel_futures=True)
    packet = core.build_packet(generated_at, today_iso, sleeves, trace, euro, cds_matched, sources, issue_book)
    body = json.dumps(packet, separators=(",", ":"), default=str).encode()
    s3.put_object(
        Bucket=BUCKET, Key=PACKET_KEY, Body=body,
        ContentType="application/json", CacheControl="no-cache",
    )
    return {
        "ok": True,
        "key": PACKET_KEY,
        "bytes": len(body),
        "as_of": packet["as_of"],
        "sleeves": len(packet["sleeves"]),
        "euro_bonds": euro.get("n_bonds"),
        "euro_issuers": euro.get("n_issuers"),
        "issue_bonds": (issue_book or {}).get("n_bonds"),
        "issue_issuers": (issue_book or {}).get("n_issuers"),
        "issue_changes": (issue_book or {}).get("n_with_change"),
        "cds_matched": cds_matched,
        "trace": sources["trace"],
    }


def lambda_handler(event, context):
    event = event or {}
    action = event.get("action") or "publish"
    if action not in ("publish", "daily"):
        return {"ok": False, "error": "unknown action"}
    return publish()
