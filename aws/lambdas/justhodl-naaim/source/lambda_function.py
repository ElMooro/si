"""justhodl-naaim — NAAIM Exposure Index (weekly active-manager equity exposure).

The NAAIM number (~-200..+200, typically 10..110) is the average reported equity
exposure of active managers. Institutional read is CONTRARIAN at extremes:
washed-out (<=30) has preceded strong forward returns; euphoric/levered (>=97)
marks crowding. Signal convention here: +2 max-bullish (washed out) .. -2
max-bearish (levered euphoria).

SOURCE naaim.org exposure-index page (latest print) + best-effort discovery of
their published history file (.xlsx/.csv parsed with stdlib only). We ALWAYS
accumulate our own S3 history so stats harden even if the file link changes.
OUTPUT data/naaim.json -> consumed by put-call-extreme sentiment composite
(6th component, weekly, flipped) and signal-board ("NAAIM Positioning").
PROVISIONAL until history_n >= 52. Weekly cron THU+FRI (upsert-idempotent).
"""
import io, json, re, time, zipfile, statistics, urllib.request
from datetime import datetime, timezone, timedelta
import boto3

BUCKET, KEY = "justhodl-dashboard-live", "data/naaim.json"
s3 = boto3.client("s3", region_name="us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (compatible; justhodl/1.0)"}
PAGE = "https://naaim.org/programs/naaim-exposure-index/"


def _get(url, timeout=25, binary=False):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        b = r.read()
    return b if binary else b.decode("utf-8", "ignore")


def _parse_date(txt):
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(txt.strip(), fmt).date().isoformat()
        except Exception:
            pass
    return None


def _xlsx_rows(blob):
    """Minimal stdlib .xlsx reader -> list[list[str]] from sheet1."""
    z = zipfile.ZipFile(io.BytesIO(blob))
    shared = []
    if "xl/sharedStrings.xml" in z.namelist():
        sx = z.read("xl/sharedStrings.xml").decode("utf-8", "ignore")
        shared = [re.sub(r"<[^>]+>", "", m) for m in re.findall(r"<si>(.*?)</si>", sx, re.S)]
    sheet = next((n for n in z.namelist() if re.match(r"xl/worksheets/sheet1\.xml", n)), None)
    if not sheet:
        return []
    xml = z.read(sheet).decode("utf-8", "ignore")
    rows = []
    for rxml in re.findall(r"<row[^>]*>(.*?)</row>", xml, re.S):
        row = []
        for ctype, cv in re.findall(r'<c[^>]*?(?:t="(\w+)")?[^>]*>.*?<v>(.*?)</v>', rxml, re.S):
            if ctype == "s":
                try:
                    cv = shared[int(cv)]
                except Exception:
                    pass
            row.append(cv)
        if row:
            rows.append(row)
    return rows


def _excel_serial_date(v):
    try:
        n = float(v)
        if 20000 < n < 60000:
            return (datetime(1899, 12, 30) + timedelta(days=n)).date().isoformat()
    except Exception:
        pass
    return None


def _history_from_file(page_html):
    """Discover + parse NAAIM's published data file. Returns {date: value}."""
    out = {}
    links = re.findall(r'href="([^"]+\.(?:xlsx|xls|csv)[^"]*)"', page_html, re.I)
    links = [l for l in links if re.search(r"exposure|naaim|use", l, re.I)] or links[:2]
    for link in links[:3]:
        url = link if link.startswith("http") else "https://naaim.org" + link
        try:
            blob = _get(url, binary=True, timeout=30)
        except Exception as e:
            print("  file fetch fail %s %s" % (url[:70], str(e)[:60]))
            continue
        rows = []
        if url.lower().endswith(".csv"):
            rows = [ln.split(",") for ln in blob.decode("utf-8", "ignore").splitlines() if ln.strip()]
        elif url.lower().endswith(".xlsx"):
            try:
                rows = _xlsx_rows(blob)
            except Exception as e:
                print("  xlsx parse fail:", str(e)[:70])
        for row in rows:
            if len(row) < 2:
                continue
            d = _parse_date(str(row[0])) or _excel_serial_date(row[0])
            try:
                v = float(str(row[1]).replace("%", "").strip())
            except Exception:
                continue
            if d and -220 <= v <= 220:
                out[d] = round(v, 2)
        if out:
            print("  history file parsed: %d rows from %s" % (len(out), url[:80]))
            break
    return out


def lambda_handler(event=None, context=None):
    html = _get(PAGE)
    today = datetime.now(timezone.utc).date()
    # Page scrape is a FALLBACK only, hard-gated: the raw page carries stray
    # numbers and event-calendar dates (v1 grabbed "1.0 @ 2026-08-01"). Require
    # the "number is" phrase, a plausible value, and a date within the last
    # two weeks (never the future). The published history FILE is primary.
    latest_val = latest_date = None
    m = re.search(r"number\s+is[^0-9\-]{0,60}?(-?\d{1,3}(?:\.\d{1,2})?)", html, re.I | re.S)
    if m:
        _v = float(m.group(1))
        if 0 <= _v <= 200:
            latest_val = _v
    for dtxt in re.findall(r"([A-Z][a-z]+ \d{1,2}, \d{4})", html)[:6]:
        _d = _parse_date(dtxt)
        if _d and timedelta(days=-14) <= (datetime.fromisoformat(_d).date() - today) <= timedelta(days=1):
            latest_date = _d
            break
    print("  page latest (gated): %s = %s" % (latest_date, latest_val))

    prior_doc = {}
    try:
        prior_doc = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
    except Exception:
        pass
    hist = {h["date"]: h["value"] for h in prior_doc.get("history", []) if h.get("date")}
    hist.update(_history_from_file(html))
    # scrub any contaminated rows (future dates, absurd values) from prior runs
    cutoff = (today + timedelta(days=2)).isoformat()
    hist = {d: v for d, v in hist.items() if d <= cutoff and 0 <= v <= 200}
    file_max = max(hist) if hist else None
    if latest_val is not None and latest_date and (file_max is None or latest_date >= file_max):
        hist[latest_date] = latest_val
    series = sorted(hist.items())
    if not series:
        raise RuntimeError("no NAAIM data parsed at all")
    dates = [d for d, _ in series]
    vals = [v for _, v in series]
    cur_date, cur = dates[-1], vals[-1]
    prev = vals[-2] if len(vals) > 1 else None
    look = vals[-260:]
    mean = statistics.fmean(look)
    sd = statistics.pstdev(look) if len(look) > 5 else 0
    z = round((cur - mean) / sd, 2) if sd else None
    pct = round(100.0 * sum(1 for v in look if v <= cur) / len(look), 1)
    n = len(vals)
    provisional = n < 52
    if cur <= 25 or (z is not None and z <= -2.0):
        sig, state = 2, "WASHED_OUT"
    elif cur <= 40:
        sig, state = 1, "DEFENSIVE"
    elif cur >= 105 or (z is not None and z >= 2.2):
        sig, state = -2, "LEVERED_EUPHORIA"
    elif cur >= 95:
        sig, state = -1, "EUPHORIC"
    else:
        sig, state = 0, "NEUTRAL"
    doc = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "source": "naaim.org exposure index (page + published history file)",
           "latest": {"date": cur_date, "value": cur}, "prior": prev,
           "change_w": round(cur - prev, 2) if prev is not None else None,
           "avg_4w": round(statistics.fmean(vals[-4:]), 2),
           "pctile": pct, "z": z, "signal": sig, "state": state,
           "provisional": provisional, "history_n": n,
           "history": [{"date": d, "value": v} for d, v in series[-520:]]}
    s3.put_object(Bucket=BUCKET, Key=KEY, Body=json.dumps(doc, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print("  wrote %s n=%d cur=%.1f z=%s pct=%s state=%s%s"
          % (KEY, n, cur, z, pct, state, " PROVISIONAL" if provisional else ""))
    return {"ok": True, "value": cur, "n": n, "state": state, "provisional": provisional}
