"""ops_4984 -- FINRA since-inception via DATE PARTITIONS (keyless).

Full-stream scans proved the keyless tier serves bounded windows:
regShoDaily = rolling 1yr, weeklySummary = ~2022-01..2024-04 slice,
blocksSummary = its complete (discontinued) life. Plan:

  P1 field census: sample rows -> the date field per dataset
  P2 filter probes (historical week 2015-06-29):
       a) GET equality   ?weekStartDate=2015-06-29
       b) POST compareFilters / dateRangeFilters
     -> first shape that returns historical rows WINS
  P3 partition drain with the winning shape, checkpointed per
     partition into have{} as sibling keys
     (otcMarket__weeklySummary__Y2014 ...):
       weeklySummary  Mondays 2014-01-06 .. window_min
       regShoDaily    weekdays, years desc from window_min-1 until
                      an empty year names inception
     guard 70min; remainder resumes next push
  P4 spans re-scan on the new partitions  P5 card
If no filter shape works keyless -> RED with the verdict named:
full depth then rides Khalid's secret (auth tier).
"""
import gzip
import io
import json
import re
import sys
import time
import urllib.error
import urllib.request
import zlib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
CAT = "justhodl-provider-catalog"
API = "https://api.finra.org"
ROOT = "data/warm/finra-full/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
HUB_KEY = "data/provider-catalog.json"
PAGE = 5000
GUARD_S = 70 * 60
T0 = time.time()

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def http(path, method="GET", body=None, timeout=90,
         cap=60_000_000):
    h = {"Accept": "application/json"}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        h["Content-Type"] = "application/json"
    req = urllib.request.Request(API + path, data=data,
                                 headers=h, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read(cap)


def rows_of(raw):
    js = json.loads(raw)
    return js if isinstance(js, list) else js.get("data") or []


def gj(key, default=None):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default


with report("ops_4984_finra_partition_drain") as R:
    fails = []
    st = gj(STATE_KEY) or {}
    have = dict(st.get("have") or {})
    universe = dict(st.get("universe") or {})

    R.section("P1 field census (ALL keys) + banked-truth spans")
    # v2: the 8-key slice hid weekStartDate; and the all-dates
    # regex span mixed lastUpdateDate noise. Target the REAL
    # business-date field per dataset and measure the banked
    # files' true spans on that field alone.
    fields = {}
    for g, nm in [("otcMarket", "weeklySummary"),
                  ("otcMarket", "regShoDaily")]:
        try:
            sample = rows_of(http(
                "/data/group/%s/name/%s?limit=2" % (g, nm)))
            r0 = sample[0] if sample else {}
            dks = [k for k, v in r0.items()
                   if isinstance(v, str) and
                   re.fullmatch(r"20\d\d-\d\d-\d\d", v)]
            pref = next((k for k in dks if k in (
                "weekStartDate", "tradeReportDate",
                "monthStartDate", "summaryStartDate")), None)
            fields["%s__%s" % (g, nm)] = pref or (
                dks[0] if dks else None)
            R.log("  %s.%s date-fields=%s -> using %s" % (
                g, nm, dks, fields["%s__%s" % (g, nm)]))
            R.log("    all-keys=%s" % sorted(r0))
        except Exception as e:
            R.log("  %s.%s sample err %s" % (g, nm, str(e)[:80]))
    wk_f = fields.get("otcMarket__weeklySummary")
    rs_f = fields.get("otcMarket__regShoDaily")
    if not (wk_f and rs_f):
        R.log("ops 4984 RED: field census failed")
        sys.exit(1)

    def field_span(key, fname):
        pat = re.compile(
            ('"%s":"(20\\d\\d-\\d\\d-\\d\\d)"'
             % fname).encode())
        body = s3.get_object(Bucket=B,
                             Key=ROOT + "src/%s.jsonl.gz" % key
                             )["Body"]
        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        lo, hi, carry = None, None, b""
        for chunk in iter(lambda: body.read(4 << 20), b""):
            buf = carry + d.decompress(chunk)
            carry = buf[-64:]
            for m in pat.finditer(buf):
                v = m.group(1)
                if lo is None or v < lo:
                    lo = v
                if hi is None or v > hi:
                    hi = v
        return (lo or b"?").decode(), (hi or b"?").decode()

    wk_lo, wk_hi = field_span("otcMarket__weeklySummary", wk_f)
    rs_lo, rs_hi = field_span("otcMarket__regShoDaily", rs_f)
    R.log("  BANKED weeklySummary.%s span %s .. %s" % (
        wk_f, wk_lo, wk_hi))
    R.log("  BANKED regShoDaily.%s   span %s .. %s" % (
        rs_f, rs_lo, rs_hi))
    wk_complete = wk_lo != "?" and wk_lo <= "2015-12-31"
    rs_complete = rs_lo != "?" and rs_lo <= "2011-12-31"

    if wk_complete and rs_complete:
        R.log("  BOTH banked files already reach inception -- no "
              "partitions owed; earlier shallow reads were "
              "lastUpdateDate noise")
    R.section("P2 filter probes (historical 2015-06-29)")
    shape = None
    probe_d = "2015-06-29"
    try:
        rs = rows_of(http(
            "/data/group/otcMarket/name/weeklySummary?limit=5"
            "&%s=%s" % (wk_f, probe_d)))
        hist = [r_ for r_ in rs if r_.get(wk_f) == probe_d]
        R.log("  GET-equality -> %d rows (%d historical)" % (
            len(rs), len(hist)))
        if hist:
            shape = "get-eq"
    except Exception as e:
        R.log("  GET-equality -> %s" % str(e)[:90])
    if not shape:
        for lbl, body in [
            ("POST-compare", {
                "limit": 5, "compareFilters": [{
                    "fieldName": wk_f, "compareType": "equal",
                    "fieldValue": probe_d}]}),
            ("POST-dateRange", {
                "limit": 5, "dateRangeFilters": [{
                    "fieldName": wk_f, "startDate": probe_d,
                    "endDate": probe_d}]}),
        ]:
            try:
                rs = rows_of(http(
                    "/data/group/otcMarket/name/weeklySummary",
                    method="POST", body=body))
                hist = [r_ for r_ in rs
                        if r_.get(wk_f) == probe_d]
                R.log("  %s -> %d rows (%d historical)" % (
                    lbl, len(rs), len(hist)))
                if hist:
                    shape = lbl
                    break
            except Exception as e:
                R.log("  %s -> %s" % (lbl, str(e)[:90]))
    if not shape:
        try:
            rs = rows_of(http(
                "/data/group/otcMarket/name/regShoDaily?limit=5"
                "&%s=%s" % (rs_f, "2023-06-29")))
            hist = [r_ for r_ in rs
                    if r_.get(rs_f) == "2023-06-29"]
            R.log("  GET-eq(regSho 2023) -> %d rows (%d match)"
                  % (len(rs), len(hist)))
            if hist:
                shape = "get-eq"
        except Exception as e:
            R.log("  GET-eq(regSho) -> %s" % str(e)[:80])
    R.log("  WINNING SHAPE: %s" % shape)
    if not shape and not (wk_complete and rs_complete):
        R.log("ops 4984 RED: keyless tier refuses historical "
              "filters AND banked spans are shallow -- full "
              "depth rides the auth secret")
        sys.exit(1)
    if not shape:
        shape = "none-needed"

    def part_fetch(g, nm, f, dval, off):
        if shape == "get-eq":
            return rows_of(http(
                "/data/group/%s/name/%s?limit=%d&offset=%d&%s=%s"
                % (g, nm, PAGE, off, f, dval), timeout=120))
        body = {"limit": PAGE, "offset": off}
        if shape == "POST-compare":
            body["compareFilters"] = [{
                "fieldName": f, "compareType": "equal",
                "fieldValue": dval}]
        else:
            body["dateRangeFilters"] = [{
                "fieldName": f, "startDate": dval,
                "endDate": dval}]
        return rows_of(http("/data/group/%s/name/%s" % (g, nm),
                            method="POST", body=body,
                            timeout=120))

    def checkpoint():
        now = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        st2 = gj(STATE_KEY) or {}
        st2["have"] = have
        st2["n_banked"] = len(have)
        st2["as_of"] = now
        st2["lease_until"] = 0
        s3.put_object(Bucket=B, Key=STATE_KEY,
                      Body=json.dumps(st2, indent=1).encode(),
                      ContentType="application/json")
        man = gj(MANIFEST_KEY) or {}
        man.update({
            "as_of": now,
            "datasets_banked": len(have),
            "rows": sum(v.get("rows") or 0
                        for v in have.values()),
            "mb": round(sum(v.get("bytes") or 0
                            for v in have.values()) / 1e6, 1)})
        s3.put_object(Bucket=B, Key=MANIFEST_KEY,
                      Body=json.dumps(man, indent=1).encode(),
                      ContentType="application/json")

    def drain_partition(g, nm, f, key, dvals):
        buf = io.BytesIO()
        rows = 0
        with gzip.GzipFile(fileobj=buf, mode="wb") as zf:
            for dval in dvals:
                off = 0
                while True:
                    batch = part_fetch(g, nm, f, dval, off)
                    for r_ in batch:
                        zf.write((json.dumps(
                            r_, separators=(",", ":")) + "\n"
                        ).encode())
                    rows += len(batch)
                    if len(batch) < PAGE:
                        break
                    off += PAGE
                if time.time() - T0 > GUARD_S:
                    break
                time.sleep(0.12)
        body = buf.getvalue()
        if rows:
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
                         "status": "partition"}
        return rows

    R.section("P3 partition drain")
    # weeklySummary: Mondays 2014-01-06 .. 2021-12-27, by year
    d0 = date(2014, 1, 6)
    for yr in ([] if (wk_complete or shape == "none-needed")
               else range(2014, 2022)):
        if time.time() - T0 > GUARD_S:
            R.log("  guard -- remainder resumes next push")
            break
        key = "otcMarket__weeklySummary__Y%d" % yr
        if key in have:
            continue
        mondays = []
        d = d0 if yr == 2014 else date(yr, 1, 1)
        while d.weekday() != 0:
            d += timedelta(days=1)
        while d.year == yr:
            mondays.append(d.isoformat())
            d += timedelta(days=7)
        n = drain_partition("otcMarket", "weeklySummary", wk_f,
                            key, mondays)
        R.log("  %-40s weeks=%d rows=%d" % (key, len(mondays), n))
        checkpoint()
    # regShoDaily: years desc from 2024 until an empty year
    for yr in ([] if (rs_complete or shape == "none-needed")
               else range(2024, 2004, -1)):
        if time.time() - T0 > GUARD_S:
            R.log("  guard -- remainder resumes next push")
            break
        key = "otcMarket__regShoDaily__Y%d" % yr
        if key in have:
            continue
        days = []
        d = date(yr, 1, 1)
        end = min(date(yr, 12, 31), date(2025, 8, 24))
        while d <= end:
            if d.weekday() < 5:
                days.append(d.isoformat())
            d += timedelta(days=1)
        n = drain_partition("otcMarket", "regShoDaily", rs_f,
                            key, days)
        R.log("  %-40s days=%d rows=%d" % (key, len(days), n))
        checkpoint()
        if n == 0:
            R.log("  regShoDaily inception reached above Y%d" % yr)
            break

    R.section("P4 spans on new partitions")
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

    ok4 = wk_complete
    try:
        k14 = "otcMarket__weeklySummary__Y2014"
        if k14 in have:
            lo, hi = span_scan(k14)
            R.log("  %s span %s .. %s rows=%s" % (
                k14, lo, hi, have[k14].get("rows")))
            ok4 = ok4 or lo <= "2014-12-31"
        if wk_complete:
            R.log("  weeklySummary banked-span already reaches "
                  "%s -- inception ok" % wk_lo)
    except Exception as e:
        R.log("  span err %s" % str(e)[:90])
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
    R.log("P5 note=%s" % note[:180])
    if "FULL Query-API warehouse" not in note:
        fails.append("P5")

    total_rows = sum(v.get("rows") or 0 for v in have.values())
    if fails:
        R.log("ops 4984 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(shape=shape, datasets=len(have), rows=total_rows,
         mb=round(sum(v.get("bytes") or 0
                      for v in have.values()) / 1e6, 1))
    R.log("ops 4984 GREEN -- since-inception depth via %s "
          "partitions; remainder (if any) resumes next push"
          % shape)
