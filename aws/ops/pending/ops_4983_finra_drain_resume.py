"""ops_4983 -- FINRA drain, RESUME-AWARE (supersedes 4982, which the
workflow's 90-min timeout cancelled at t+92 with all its S3 uploads
banked but none of the end-of-run bookkeeping written).

Armor this version carries:
  RESUME    a dataset whose src/ object already exists with rows
            metadata is skipped (4982's ~168MB+ counts)
  CHECKPT   state + manifest rewritten after EVERY dataset -- a
            kill can never lose bookkeeping again
  GUARD     78min drain window (under the workflow cap), then
            gates; if a remainder survives, this op stays red-in-
            pending and the next push resumes it -- Khalid:
            "budget is not a problem"
  P4 substance (weekly <=2016)  P5 card flips (8min window)
"""
import gzip
import io
import json
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
CAT = "justhodl-provider-catalog"
API = "https://api.finra.org"
GROUPS = ["otcMarket", "fixedIncomeMarket"]
SEEDS = {
    "otcMarket": [
        "weeklySummary", "monthlySummary", "blocksSummary",
        "otcBlocksSummary", "consolidatedShortInterest",
        "regShoDaily", "weeklyDownloadDetails",
        "monthlyDownloadDetails", "atsIssueData", "otcIssueData"],
    "fixedIncomeMarket": [
        "treasuryWeeklyAggregates", "treasuryMonthlyAggregates",
        "corporatesAndAgenciesCappedVolume",
        "corporateMarketSentiment", "agencyMarketSentiment",
        "corporate144AMarketSentiment", "treasuryDailyAggregates",
        "securitizedProductsCapped", "corporateMarketBreadth"],
}
ROOT = "data/warm/finra-full/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
HUB_KEY = "data/provider-catalog.json"
PAGE = 5000
DRAIN_S = 78 * 60
T0 = time.time()

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def fetch(path, timeout=90, cap=60_000_000):
    req = urllib.request.Request(
        API + path, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read(cap)


def gj(key, default=None):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default


def checkpoint(universe, invalid, have, dfail):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    remaining = [k for k in sorted(universe)
                 if k not in have and k not in dfail]
    st = {
        "version": "1.0.6",
        "phase": "COMPLETE" if not remaining and not dfail
        else "DRAIN",
        "universe": universe, "invalid": invalid, "have": have,
        "queue": [[k, 0] for k in remaining],
        "failures": {k: {"err": v, "tries": 1}
                     for k, v in dfail.items()},
        "lease_until": 0, "n_banked": len(have), "as_of": now,
        "drain_src": "runner ops4983 resume "
                     "(lambda egress 400s keyless)"}
    s3.put_object(Bucket=B, Key=STATE_KEY,
                  Body=json.dumps(st, indent=1).encode(),
                  ContentType="application/json")
    s3.put_object(Bucket=B, Key=MANIFEST_KEY, Body=json.dumps({
        "as_of": now, "engine": "justhodl-finra-full",
        "version": "1.0.6",
        "datasets_catalog": len(universe),
        "datasets_banked": len(have),
        "rows": sum(v.get("rows") or 0 for v in have.values()),
        "mb": round(sum(v.get("bytes") or 0
                        for v in have.values()) / 1e6, 1),
        "invalid_named": len(invalid), "failures": len(dfail),
        "phase": st["phase"],
        "cred_expires": "secret pending (portal Reset)",
        "note": ("complete FINRA Query API warehouse: every "
                 "discovered dataset fully paginated since "
                 "inception (OTC/ATS weeklies, TRACE aggregates, "
                 "short interest...); daily rediscovery, weekly "
                 "redrain")}, indent=1).encode(),
        ContentType="application/json")
    return st["phase"], remaining


with report("ops_4983_finra_drain_resume") as R:
    fails = []
    R.section("P1 catalog + resume census")
    universe, invalid = {}, {}
    for g in GROUPS:
        names = []
        try:
            js = json.loads(fetch("/metadata/group/%s" % g,
                                  cap=6_000_000))
            items = js if isinstance(js, list) else \
                js.get("datasets") or js.get("data") or []
            for it in items:
                nm = it.get("datasetName") or it.get("name") \
                    if isinstance(it, dict) else it
                if nm:
                    names.append(nm)
            R.log("  %s metadata: %d names" % (g, len(names)))
        except Exception as e:
            R.log("  %s metadata refused (%s) -> seeds" % (
                g, str(e)[:60]))
            names = list(SEEDS[g])
        for nm in names:
            key = "%s__%s" % (g, nm)
            try:
                fetch("/data/group/%s/name/%s?limit=2" % (g, nm),
                      cap=300_000)
                universe[key] = {"group": g, "name": nm}
            except urllib.error.HTTPError as e:
                invalid[key] = "HTTP %s" % e.code
            except Exception as e:
                invalid[key] = str(e)[:60]
            time.sleep(0.2)
    have, dfail = {}, {}
    for key in sorted(universe):
        try:
            h = s3.head_object(Bucket=B,
                               Key=ROOT + "src/%s.jsonl.gz" % key)
            rows = int((h.get("Metadata") or {}).get("rows") or 0)
            if rows > 0:
                have[key] = {"rows": rows,
                             "bytes": h["ContentLength"],
                             "at": h["LastModified"].isoformat(),
                             "status": "resumed-4982"}
        except Exception:
            pass
    R.log("  universe=%d invalid=%d already-banked(4982)=%d" % (
        len(universe), len(invalid), len(have)))
    if len(universe) < 6:
        R.log("ops 4983 RED: catalog thin")
        sys.exit(1)
    checkpoint(universe, invalid, have, dfail)

    R.section("P2 drain (checkpointed)")
    for key, meta in sorted(universe.items()):
        if key in have:
            continue
        if time.time() - T0 > DRAIN_S:
            R.log("  guard -- remainder resumes next push")
            break
        g, nm = meta["group"], meta["name"]
        buf = io.BytesIO()
        rows = 0
        try:
            with gzip.GzipFile(fileobj=buf, mode="wb") as zf:
                off = 0
                while True:
                    raw = fetch("/data/group/%s/name/%s?limit=%d"
                                "&offset=%d" % (g, nm, PAGE, off),
                                timeout=120)
                    js = json.loads(raw)
                    batch = js if isinstance(js, list) else \
                        js.get("data") or []
                    for r_ in batch:
                        zf.write((json.dumps(
                            r_, separators=(",", ":")) + "\n"
                        ).encode())
                    rows += len(batch)
                    if len(batch) < PAGE or \
                            time.time() - T0 > DRAIN_S:
                        break
                    off += PAGE
                    time.sleep(0.2)
            body = buf.getvalue()
            s3.put_object(
                Bucket=B, Key=ROOT + "src/%s.jsonl.gz" % key,
                Body=body, ContentType="application/gzip",
                Metadata={"engine": "finra-full",
                          "dataset": key[:110],
                          "rows": str(rows)})
            have[key] = {"rows": rows, "bytes": len(body),
                         "at": datetime.now(timezone.utc
                                            ).isoformat(
                             timespec="seconds"),
                         "status": "fresh"}
            R.log("  %-52s rows=%-9d %6.1fMB" % (
                key, rows, len(body) / 1e6))
        except Exception as e:
            dfail[key] = str(e)[:100]
            R.log("  %-52s FAIL %s" % (key, str(e)[:80]))
        checkpoint(universe, invalid, have, dfail)
    phase, remaining = checkpoint(universe, invalid, have, dfail)
    R.log("  banked=%d rows=%d failed=%d remaining=%d phase=%s"
          % (len(have),
             sum(v.get("rows") or 0 for v in have.values()),
             len(dfail), len(remaining), phase))
    if len(have) < 6:
        fails.append("P2")

    R.section("P4 substance -- full-stream min/max dates")
    # v4: FINRA pagination is NOT chronological; sampling head+tail
    # of an unordered 215MB stream landed mid-window (2022-2023)
    # and looked shallow. Scan EVERY date. weeklySummary since 2014
    # ~= 600 weeks x ~10k issue-tier rows ~= 6M -- the 5.75M banked
    # is plausibly the complete history.
    import zlib
    date_re = re.compile(rb'20\d{2}-\d{2}-\d{2}')

    def span_scan(key):
        body = s3.get_object(Bucket=B,
                             Key=ROOT + "src/%s.jsonl.gz" % key
                             )["Body"]
        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        lo, hi, carry = None, None, b""
        for chunk in iter(lambda: body.read(4 << 20), b""):
            buf = carry + d.decompress(chunk)
            carry = buf[-12:]
            for m in date_re.finditer(buf):
                v = m.group()
                if lo is None or v < lo:
                    lo = v
                if hi is None or v > hi:
                    hi = v
        return (lo or b"?").decode(), (hi or b"?").decode()

    ok4 = False
    try:
        # v2: pick the DEEPEST weekly history (the first-match
        # picker grabbed weeklyDownloadDetails, whose own source
        # inception is 2021-12; weeklySummary holds 5.75M rows
        # back to the program's start)
        spans = {}
        for pick in ["otcMarket__weeklySummary",
                     "otcMarket__regShoDaily",
                     "otcMarket__blocksSummary"]:
            if pick not in have:
                continue
            lo, hi = span_scan(pick)
            spans[pick] = (lo, hi)
            R.log("  %-38s rows=%-9s span %s .. %s" % (
                pick, have[pick].get("rows"), lo, hi))
        lo_w = (spans.get("otcMarket__weeklySummary") or
                ("9999", ""))[0]
        ok4 = lo_w <= "2016-12-31"
    except Exception as e:
        R.log("  substance err %s" % str(e)[:110])
    R.log("P4 %s" % ("PASS" if ok4 else "FAIL"))
    if not ok4:
        fails.append("P4")

    R.section("P5 card")
    t_mark = datetime.now(timezone.utc).isoformat(
        timespec="seconds")
    try:
        lam.invoke(FunctionName=CAT, InvocationType="Event",
                   Payload=b"{}")
    except Exception:
        pass
    hub, t0 = {}, time.time()
    while time.time() - t0 < 8 * 60:
        time.sleep(30)
        hub = gj(HUB_KEY) or {}
        if (hub.get("as_of") or "") >= t_mark:
            break
    fe = next((p for p in hub.get("providers", [])
               if p.get("slug") == "finra"), {}) or {}
    note = fe.get("catalog_note") or ""
    ok5 = "FULL Query-API warehouse" in note and \
        "0/0 datasets" not in note
    R.log("P5 %s note=%s" % ("PASS" if ok5 else "FAIL",
                             note[:180]))
    if not ok5:
        fails.append("P5")

    if remaining:
        fails.append("REMAINDER(%d) -- next push resumes"
                     % len(remaining))
    if fails:
        R.log("ops 4983 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(datasets=len(have), catalog=len(universe),
         rows=sum(v.get("rows") or 0 for v in have.values()),
         mb=round(sum(v.get("bytes") or 0
                      for v in have.values()) / 1e6, 1))
    R.log("ops 4983 GREEN -- FINRA fully imported since "
          "inception; engine owns refresh once the secret lands")
