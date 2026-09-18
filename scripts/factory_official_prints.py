#!/usr/bin/env python3
"""Official prints for the factory wall (runner-owned; the student cannot write this prefix).

The grader (justhodl-factory-grader.grade_market) grades a week only when
``factory/official-prints/<week>/<symbol>.json`` exists in the private bucket with
``verified_by == "owner_runner"``, a ``window`` identical to the accepted entry's,
one close per session, a boolean ``corporate_action`` and a 64-hex ``raw_sha256``.

Sources -- nothing is fabricated, a missing input leaves the symbol unwritten:
  * SPY QQQ IWM TLT GLD: the warehouse's Polygon grouped-daily session files
    (data/warm/polygon-full/grouped/YYYY/YYYY-MM-DD.json.gz, adjusted=true = split
    adjusted; the same basis every graded engine in the fleet uses). Opening = the
    first session's open print, closes = each session's official close.
  * BTC: Coinbase Exchange (the season's pinned venue) minute candles at the New
    York session boundaries -- 09:30 ET open on the first session, 16:00 ET (or the
    season's early close) on every session. A UTC daily bar is never substituted.
    Raw candles are banked to data/warm/coinbase/BTC-USD/1m/<week>/ first so the
    print's provenance is a warehouse key.
  * Corporate actions: Polygon reference splits for the week (ETFs), banked to
    data/warm/polygon-full/reference/<week>/. Splits void the week; dividends are
    reported (dividends_in_window) and do not void -- season-1 price_definition is
    unadjusted OHLC and SPY/QQQ/IWM go ex-div in week 1, TLT monthly.

Usage: factory_official_prints.py [--week 2026-09-14] [--dry-run] [--symbols SPY,BTC]
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "aws" / "shared"))
from factory_core import NY, SYMBOLS, canonical, digest, iso, week_window  # noqa: E402

PRIVATE = os.environ.get("FACTORY_PRIVATE_BUCKET", "justhodl-ai-857687956942")
PUBLIC = os.environ.get("FACTORY_PUBLIC_BUCKET", "justhodl-dashboard-live")
REGION = os.environ.get("AWS_REGION", "us-east-1")
GROUPED = "data/warm/polygon-full/grouped/"
ETF_HISTORY = "data/warm/etf-history/grouped/"      # scripts/backfill_etf_history.py: FMP daily bars for the drill symbols, pre-entitlement years
COINBASE = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
POLYGON = "https://api.polygon.io/v3/reference/"
SCHEMA = "factory-official-print.v1"
UA = {"User-Agent": "JustHodl-FactoryPrints/1.0 (+https://justhodl.ai/ai.html)"}
ETFS = tuple(s for s in SYMBOLS if s != "BTC")


class Missing(RuntimeError):
    """A required input does not exist; the print is not written."""


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def git_sha():
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True, timeout=10).stdout.strip()[:12]
    except Exception:  # noqa: BLE001
        return "unknown"


# ------------------------------------------------------------------ sources
class Warehouse:
    """Bounded S3 reads/writes; every read returns (doc, raw_sha256, key)."""

    def __init__(self, s3, private=PRIVATE, public=PUBLIC):
        self.s3, self.private, self.public = s3, private, public

    def get(self, bucket, key, limit=64 * 1024 * 1024):
        try:
            body = self.s3.get_object(Bucket=bucket, Key=key)["Body"].read(limit + 1)
        except Exception as exc:  # noqa: BLE001
            if getattr(exc, "response", {}).get("Error", {}).get("Code") in ("NoSuchKey", "404", "NotFound"):
                return None, None
            raise
        if len(body) > limit:
            raise Missing("object_too_large:" + key)
        return body, sha(body)

    def put_if_absent(self, bucket, key, body):
        try:
            self.s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json", IfNoneMatch="*")
            return "written"
        except Exception as exc:  # noqa: BLE001
            code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if code in ("PreconditionFailed", "412"):
                return "exists"
            raise


def grouped_row(wh, day, symbol):
    """(row, raw_sha, key) for one session's grouped-daily file, or raise Missing."""
    # 2026-09-18: Polygon grouped-daily is entitled on a rolling ~5-year window, so the COVID-2020 drill block cannot be
    # banked from it; sessions before the boundary come from the source-tagged ETF history prefix (six drill symbols only).
    for key in (GROUPED + "%s/%s.json.gz" % (day[:4], day), GROUPED + "%s/%s.json.gz" % (day[:4], day.replace("-", "")),
                ETF_HISTORY + "%s/%s.json.gz" % (day[:4], day)):
        raw, raw_sha = wh.get(wh.public, key)
        if raw is None:
            continue
        try:
            doc = json.loads(gzip.decompress(raw))
        except OSError:
            doc = json.loads(raw)
        for r in doc.get("results") or []:
            if r.get("T") == symbol and all(isinstance(r.get(k), (int, float)) for k in ("o", "c")) and r["o"] > 0 and r["c"] > 0:
                return {"o": float(r["o"]), "c": float(r["c"]), "h": float(r.get("h") or 0), "l": float(r.get("l") or 0),
                        "v": float(r.get("v") or 0)}, raw_sha, key
        raise Missing("symbol_absent_in_session:%s:%s" % (symbol, day))
    raise Missing("session_file_missing:" + day)


def fetch_json(url, timeout=20):
    req = urllib.request.Request(url, headers={**UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read(8 * 1024 * 1024)
    return raw, json.loads(raw)


def coinbase_candles(start, end, fetch=fetch_json):
    q = urllib.parse.urlencode({"granularity": 60, "start": start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                                "end": end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")})
    raw, rows = fetch(COINBASE + "?" + q)
    if not isinstance(rows, list):
        raise Missing("coinbase_candles_shape")
    return raw, rows


def boundary_price(rows, at, field):
    """Price from the minute candle that starts at ``at`` (field 'open' or 'close'). Candle: [time, low, high, open, close, volume]."""
    epoch = int(at.timestamp())
    for row in rows:
        if isinstance(row, list) and len(row) >= 5 and int(row[0]) == epoch:
            value = float(row[3] if field == "open" else row[4])
            if value > 0:
                return value
    raise Missing("coinbase_boundary_candle_missing:%s" % iso(at))


def polygon_reference(kind, symbol, first, last, key, fetch=fetch_json):
    field = "execution_date" if kind == "splits" else "ex_dividend_date"
    url = POLYGON + "%s?ticker=%s&%s.gte=%s&%s.lte=%s&limit=50&apiKey=%s" % (kind, symbol, field, first, field, last, key)
    raw, doc = fetch(url)
    return raw, [r for r in (doc.get("results") or []) if isinstance(r, dict)]


def polygon_key():
    key = os.environ.get("POLYGON_API_KEY") or os.environ.get("POLYGON_KEY")
    if key:
        return key
    try:
        import boto3
        return boto3.client("ssm", region_name=REGION).get_parameter(Name="/justhodl/polygon/api-key", WithDecryption=True)["Parameter"]["Value"]
    except Exception:  # noqa: BLE001
        return None


# ------------------------------------------------------------------ prints
def print_base(season, week, symbol, window, now):
    return {"schema_version": SCHEMA, "week": week, "symbol": symbol, "season": season["id"],
            "policy_hash": season["policy_hash"], "source": season["price_sources"][symbol],
            "verified_by": "owner_runner", "window": window, "sessions": list(window["sessions"]),
            "available_at": iso(now), "written_by": "scripts/factory_official_prints.py@" + git_sha()}


def etf_print(wh, season, week, symbol, window, now, *, poly_key, fetch=fetch_json, bank=True, dry_run=False):
    rows, hashes, keys = [], [], []
    for day in window["sessions"]:
        row, raw_sha, key = grouped_row(wh, day, symbol)
        rows.append(row); hashes.append(raw_sha); keys.append(key)
    doc = print_base(season, week, symbol, window, now)
    doc.update(opening=rows[0]["o"], closes=[r["c"] for r in rows], highs=[r["h"] for r in rows], lows=[r["l"] for r in rows],
               volumes=[r["v"] for r in rows], source_url="https://justhodl.ai/" + keys[0],
               raw_sha256=sha("".join(hashes).encode()), raw_sha256_basis="sha256 of the session files' sha256 hex digests concatenated in session order",
               provenance={"keys": [{"bucket": "public", "key": k, "sha256": h} for k, h in zip(keys, hashes)]},
               adjustment="polygon grouped adjusted=true (split-adjusted); split weeks void; dividends reported, not adjusted (season-1 unadjusted OHLC)")
    if not poly_key:
        raise Missing("polygon_key_unavailable_for_corporate_action_check")
    first, last = window["sessions"][0], window["sessions"][-1]
    actions = {}
    for kind in ("splits", "dividends"):
        raw, results = polygon_reference(kind, symbol, first, last, poly_key, fetch=fetch)
        redacted = json.dumps({"kind": kind, "ticker": symbol, "gte": first, "lte": last, "results": results}, sort_keys=True).encode()
        key = "data/warm/polygon-full/reference/%s/%s-%s.json" % (week, symbol, kind)
        if bank and not dry_run:
            wh.put_if_absent(wh.public, key, redacted)
        actions[kind] = {"results": results, "key": key, "sha256": sha(redacted)}
    splits = actions["splits"]["results"]
    doc.update(corporate_action=bool(splits),
               corporate_actions=[{"kind": "split", "execution_date": r.get("execution_date"), "from": r.get("split_from"), "to": r.get("split_to")} for r in splits],
               dividends_in_window=[{"ex_dividend_date": r.get("ex_dividend_date"), "cash_amount": r.get("cash_amount"), "pay_date": r.get("pay_date")}
                                    for r in actions["dividends"]["results"]],
               corporate_action_basis="polygon reference splits in [first session, last session]; dividends do not void",
               corporate_action_provenance={k: {"key": v["key"], "sha256": v["sha256"]} for k, v in actions.items()})
    return doc


def btc_print(wh, season, week, symbol, window, now, *, fetch=fetch_json, bank=True, dry_run=False):
    sessions = window["sessions"]
    boundaries = [("open", datetime.combine(date.fromisoformat(sessions[0]), time(9, 30), NY))]
    for day in sessions:
        end_time = time.fromisoformat(season.get("early_closes", {}).get(day, "16:00"))
        boundaries.append(("close:" + day, datetime.combine(date.fromisoformat(day), end_time, NY)))
    banked, closes, opening = [], [], None
    for label, at in boundaries:
        query_at = at if label == "open" else at - timedelta(minutes=1)     # the close is the last minute candle's close
        raw, rows = coinbase_candles(query_at - timedelta(minutes=2), query_at + timedelta(minutes=3), fetch=fetch)
        key = "data/warm/coinbase/BTC-USD/1m/%s/%s.json" % (week, label.replace(":", "-"))
        if bank and not dry_run:
            wh.put_if_absent(wh.public, key, raw)
        banked.append({"bucket": "public", "key": key, "sha256": sha(raw), "boundary": iso(at)})
        if label == "open":
            opening = boundary_price(rows, query_at, "open")
        else:
            closes.append(boundary_price(rows, query_at, "close"))
    doc = print_base(season, week, symbol, window, now)
    doc.update(opening=opening, closes=closes, source_url="https://justhodl.ai/" + banked[0]["key"], venue_url=COINBASE,
               raw_sha256=sha("".join(b["sha256"] for b in banked).encode()),
               raw_sha256_basis="sha256 of the banked candle files' sha256 hex digests concatenated in boundary order",
               provenance={"keys": banked}, corporate_action=False, corporate_actions=[], dividends_in_window=[],
               corporate_action_basis="spot asset on the pinned venue; not applicable",
               adjustment="Coinbase BTC-USD minute candles at New York session boundaries; no daily UTC bar substituted")
    return doc


def grader_shape_ok(doc):
    """Mirror of the grader's acceptance checks, so a bad print is never written."""
    required = {"source", "source_url", "raw_sha256", "verified_by", "window", "opening", "closes", "sessions", "corporate_action", "available_at"}
    if not required <= doc.keys() or doc["verified_by"] != "owner_runner" or not doc["source_url"].startswith("https://"):
        return False
    if len(doc["raw_sha256"]) != 64 or doc["sessions"] != doc["window"]["sessions"] or len(doc["closes"]) != len(doc["sessions"]):
        return False
    if not isinstance(doc["corporate_action"], bool) or len(doc["closes"]) < 3:
        return False
    return doc["opening"] > 0 and all(c > 0 for c in doc["closes"])


def default_week(season, now):
    """Most recent season week whose window has closed."""
    first = date.fromisoformat(season["starts_on"])
    local = now.astimezone(NY).date()
    monday = local - timedelta(days=local.weekday())
    while monday >= first:
        window = week_window(monday.isoformat(), season)
        if datetime.fromisoformat(window["closes_at"]) <= now:
            return monday.isoformat()
        monday -= timedelta(days=7)
    raise Missing("no_closed_week_in_season")


def load_season(wh):
    raw, _ = wh.get(wh.private, "factory/control/season.json")
    if raw is None:
        raw, _ = wh.get(wh.public, "factory/salon/season.json")
    if raw is None:
        raise Missing("season_missing")
    season = json.loads(raw)
    clean = dict(season); frozen = clean.pop("policy_hash", None)
    if digest(clean) != frozen:
        raise Missing("frozen_season_hash_mismatch")
    return season


def run(wh, *, week=None, symbols=SYMBOLS, dry_run=False, now=None, fetch=fetch_json, poly_key=None, bank=True):
    now = now or datetime.now(timezone.utc)
    season = load_season(wh)
    week = week or default_week(season, now)
    window = week_window(week, season)
    if datetime.fromisoformat(window["closes_at"]) > now:
        raise Missing("week_not_closed:" + week)
    poly_key = poly_key or polygon_key()
    report = {"week": week, "window": window, "dry_run": dry_run, "at": iso(now), "symbols": {}}
    for symbol in symbols:
        try:
            doc = btc_print(wh, season, week, symbol, window, now, fetch=fetch, bank=bank, dry_run=dry_run) if symbol == "BTC" \
                else etf_print(wh, season, week, symbol, window, now, poly_key=poly_key, fetch=fetch, bank=bank, dry_run=dry_run)
            if not grader_shape_ok(doc):
                raise Missing("print_failed_grader_shape")
            doc["print_hash"] = digest(doc)
            key = "factory/official-prints/%s/%s.json" % (week, symbol)
            status = "dry_run" if dry_run else wh.put_if_absent(wh.private, key, canonical(doc))
            report["symbols"][symbol] = {"status": status, "key": key, "opening": doc["opening"], "closes": doc["closes"],
                                         "corporate_action": doc["corporate_action"], "dividends": len(doc["dividends_in_window"]),
                                         "print_hash": doc["print_hash"]}
        except Missing as exc:
            report["symbols"][symbol] = {"status": "incomplete", "reason": str(exc)[:200]}
        except Exception as exc:  # noqa: BLE001 -- unexpected: report, keep the others going
            report["symbols"][symbol] = {"status": "error", "reason": type(exc).__name__ + ":" + str(exc)[:160]}
    report["complete"] = all(r["status"] in ("written", "exists", "dry_run") for r in report["symbols"].values())
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--week", help="Monday ISO date of the season week (default: latest closed week)")
    parser.add_argument("--symbols", default=",".join(SYMBOLS))
    parser.add_argument("--dry-run", action="store_true", help="compute and validate, write nothing")
    parser.add_argument("--strict", action="store_true", help="exit 1 unless every symbol is complete")
    parser.add_argument("--report", help="write the JSON report to this path")
    args = parser.parse_args(argv)
    import boto3
    wh = Warehouse(boto3.client("s3", region_name=REGION))
    try:
        report = run(wh, week=args.week, symbols=tuple(s.strip().upper() for s in args.symbols.split(",") if s.strip()), dry_run=args.dry_run)
    except Missing as exc:
        report = {"complete": False, "error": str(exc), "week": args.week, "dry_run": args.dry_run}
    text = json.dumps(report, indent=2, default=str)
    print(text)
    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(text + "\n")
    if args.strict and not report.get("complete"):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
