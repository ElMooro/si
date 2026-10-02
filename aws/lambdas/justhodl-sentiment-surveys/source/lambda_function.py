"""justhodl-sentiment-surveys — weekly investor-sentiment composite for justhodl.ai.

SOURCES (all free, no API key):
  1. AAII Investor Sentiment Survey (weekly, since Jul 1987):
     https://www.aaii.com/files/surveys/sentiment.xls
     Parsed with a pure-stdlib BIFF8 (.xls) reader (no xlrd/openpyxl in Lambda).
  2. NAAIM Exposure Index (weekly):
     https://index.naaim.org/embeddable/chart  (Chart.js JSON embedded in page)
     NOTE: NAAIM moved to a subscription model effective 2026-08-01, so the
     free history is frozen at 2026-06-24. The pipeline keeps serving the last
     known history and flags staleness.

OUTPUT: data/sentiment-surveys.json
  { generated_at, engine,
    aaii:   { latest: {date,bullish,neutral,bearish,bull_bear_spread},
              history: [{date,bullish,neutral,bearish}] },
    naaim:  { latest: {date,exposure}, history: [{date,exposure}],
              number_page_value, stale_note },
    composite: { fear_greed_tilt, label, components } }

FAIL-SOFT: every fetch/parse is wrapped; the handler never raises and always
writes the output file, even if partially empty.
SCHEDULE: weekly Friday 14:00 UTC (AAII publishes Thursdays).
"""
import json
import re
import struct
import html as _html
import urllib.request
from datetime import datetime, timezone, date, timedelta
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/sentiment-surveys.json"
ENGINE = "sentiment-surveys"

AAII_XLS_URL = "https://www.aaii.com/files/surveys/sentiment.xls"
NAAIM_CHART_URL = "https://index.naaim.org/embeddable/chart"
NAAIM_NUMBER_URL = "https://index.naaim.org/embeddable/number"

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
      "Accept": "*/*"}

s3 = boto3.client("s3", region_name=REGION)


def http_get(url, timeout=60, retries=1):
    last = None
    for _ in range(max(1, retries)):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception as e:
            last = e
    raise last


# ---------------------------------------------------------------- BIFF8 .xls
def _rk_to_num(rk):
    if rk & 2:  # 30-bit signed int in bits 31..2
        v = rk >> 2
        n = v - 0x40000000 if (v & 0x20000000) else v
    else:  # high 32 bits of an IEEE-754 double (flag bits = low 2 bits)
        n = struct.unpack("<d", struct.pack("<Q", (rk & 0xFFFFFFFC) << 32))[0]
    if rk & 1:  # fX100
        n /= 100.0
    return n


def _ole_workbook_stream(blob):
    """Extract the 'Workbook' stream from an OLE2 compound document."""
    if blob[:8] != b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1":
        raise ValueError("not an OLE2 document")
    sector_shift, mini_shift = struct.unpack("<HH", blob[30:34])
    first_dir = struct.unpack("<I", blob[48:52])[0]
    first_difat = struct.unpack("<I", blob[68:72])[0]
    sec_size = 1 << sector_shift

    def sector(i):
        return 512 + i * sec_size

    difat = list(struct.unpack("<109I", blob[76:512]))
    cur = first_difat
    while cur not in (0xFFFFFFFE, 0xFFFFFFFF):
        difat += list(struct.unpack("<128I", blob[sector(cur):sector(cur) + sec_size]))
        cur = struct.unpack("<I", blob[sector(cur) + sec_size - 4:sector(cur) + sec_size])[0]
    fat = []
    for s in difat:
        if s in (0xFFFFFFFE, 0xFFFFFFFF):
            break
        fat += list(struct.unpack("<%dI" % (sec_size // 4), blob[sector(s):sector(s) + sec_size]))

    def chain(start):
        out = bytearray()
        s = start
        while s not in (0xFFFFFFFE, 0xFFFFFFFF, 0xFFFFFFFD):
            out += blob[sector(s):sector(s) + sec_size]
            s = fat[s] if s < len(fat) else 0xFFFFFFFF
        return bytes(out)

    dirdata = chain(first_dir)
    for off in range(0, len(dirdata), 128):
        e = dirdata[off:off + 128]
        if len(e) < 128:
            break
        namelen = struct.unpack("<H", e[64:66])[0]
        name = e[:namelen - 2].decode("utf-16-le", "ignore") if namelen else ""
        if name == "Workbook" and e[66] == 2:
            start = struct.unpack("<I", e[116:120])[0]
            size = struct.unpack("<Q", e[120:128])[0]
            return chain(start)[:size]
    raise KeyError("Workbook stream not found")


def _parse_biff_sheets(wb):
    """Walk BIFF records; return list of {(row,col): value} per sheet + SST."""
    sheets, sst, cur = [], [], None
    pos, n = 0, len(wb)
    while pos + 4 <= n:
        rtype, rlen = struct.unpack("<HH", wb[pos:pos + 4])
        rec = wb[pos + 4:pos + 4 + rlen]
        pos += 4 + rlen
        try:
            if rtype == 0x0809:  # BOF -> new sheet substream
                cur = {}
                sheets.append(cur)
            elif rtype == 0x00FC:  # SST
                p = 8
                unique = struct.unpack("<I", rec[4:8])[0]
                for _ in range(unique):
                    if p + 3 > len(rec):
                        break
                    ln = struct.unpack("<H", rec[p:p + 2])[0]
                    flags = rec[p + 2]
                    p += 3
                    if flags & 8:
                        p += 2 + struct.unpack("<H", rec[p:p + 2])[0] * 4
                    if flags & 4:
                        p += 4
                    w = 2 if flags & 1 else 1
                    sst.append(rec[p:p + ln * w].decode("utf-16-le" if w == 2 else "latin-1", "ignore"))
                    p += ln * w
            elif cur is not None and rtype == 0x0203 and len(rec) >= 14:  # NUMBER
                r, c = struct.unpack("<HH", rec[:4])
                cur[(r, c)] = struct.unpack("<d", rec[6:14])[0]
            elif cur is not None and rtype == 0x00FD and len(rec) >= 10:  # LABELSST
                r, c = struct.unpack("<HH", rec[:4])
                idx = struct.unpack("<I", rec[6:10])[0]
                cur[(r, c)] = sst[idx] if idx < len(sst) else ""
            elif cur is not None and rtype == 0x027E and len(rec) >= 10:  # RK
                r, c = struct.unpack("<HH", rec[:4])
                cur[(r, c)] = _rk_to_num(struct.unpack("<I", rec[6:10])[0])
            elif cur is not None and rtype == 0x00BD and len(rec) >= 8:  # MULRK
                r = struct.unpack("<H", rec[:2])[0]
                c1 = struct.unpack("<H", rec[2:4])[0]
                c2 = struct.unpack("<H", rec[-2:])[0]
                for i in range(c2 - c1 + 1):
                    cur[(r, c1 + i)] = _rk_to_num(struct.unpack("<I", rec[6 + 6 * i:10 + 6 * i])[0])
            elif cur is not None and rtype == 0x0006 and len(rec) >= 14:  # FORMULA
                r, c = struct.unpack("<HH", rec[:4])
                if rec[6] == 0 and rec[7:14] != b"\xff" * 7:
                    cur[(r, c)] = struct.unpack("<d", rec[6:14])[0]
        except Exception:
            continue
    return sheets


def _xldate_to_iso(x):
    try:
        return (date(1899, 12, 30) + timedelta(days=int(x))).isoformat()
    except Exception:
        return None


def fetch_aaii():
    """Return (history, latest, error). history = [{date,bullish,neutral,bearish}] in %."""
    blob = http_get(AAII_XLS_URL, timeout=90, retries=2)
    wb = _ole_workbook_stream(blob)
    sheets = _parse_biff_sheets(wb)
    # the survey history sheet = the one with the most valid data rows
    best, best_rows = None, []
    for sh in sheets:
        rows = []
        if not sh:
            continue
        maxr = max(r for r, _ in sh)
        for r in range(maxr + 1):
            v0, v1, v2, v3 = (sh.get((r, c)) for c in range(4))
            if (isinstance(v0, (int, float)) and 25000 < v0 < 60000
                    and all(isinstance(v, (int, float)) and 0 <= v <= 1 for v in (v1, v2, v3))
                    and abs(v1 + v2 + v3 - 1.0) < 0.02):
                d = _xldate_to_iso(v0)
                if d:
                    rows.append({"date": d,
                                 "bullish": round(v1 * 100, 1),
                                 "neutral": round(v2 * 100, 1),
                                 "bearish": round(v3 * 100, 1)})
        if len(rows) > len(best_rows):
            best, best_rows = sh, rows
    if not best_rows:
        raise ValueError("AAII: no valid survey rows found in workbook")
    best_rows.sort(key=lambda x: x["date"])
    # de-dupe by date, keep last
    seen, hist = {}, []
    for row in best_rows:
        seen[row["date"]] = row
    hist = [seen[d] for d in sorted(seen)]
    latest = dict(hist[-1])
    latest["bull_bear_spread"] = round(latest["bullish"] - latest["bearish"], 1)
    return hist, latest, None


# ---------------------------------------------------------------- NAAIM
def fetch_naaim():
    """Return (history, latest, number_page_value, error)."""
    page = http_get(NAAIM_CHART_URL, timeout=30).decode("utf-8", "ignore")
    m = re.search(r'data-symfony--ux-chartjs--chart-view-value="([^"]*)"', page)
    if not m:
        raise ValueError("NAAIM: chart JSON not found in embeddable page")
    data = json.loads(_html.unescape(m.group(1)))
    labels = data["data"]["labels"]
    ds = None
    for d in data["data"]["datasets"]:
        if "NAAIM" in str(d.get("label", "")):
            ds = d
            break
    if ds is None:
        ds = data["data"]["datasets"][0]
    hist = []
    for lab, val in zip(labels, ds["data"]):
        try:
            v = float(val)
        except (TypeError, ValueError):
            continue
        d = str(lab)[:10]
        if re.match(r"^\d{4}-\d{2}-\d{2}$", d):
            hist.append({"date": d, "exposure": round(v, 2)})
    if not hist:
        raise ValueError("NAAIM: empty history in chart JSON")
    hist.sort(key=lambda x: x["date"])
    latest = dict(hist[-1])
    # the "number" page sometimes carries a newer (undated) reading
    number_val = None
    try:
        npage = http_get(NAAIM_NUMBER_URL, timeout=30).decode("utf-8", "ignore")
        txt = re.sub(r"<script.*?</script>", " ", npage, flags=re.S)
        txt = re.sub(r"<style.*?</style>", " ", txt, flags=re.S)
        txt = re.sub(r"<[^>]+>", " ", txt)
        nums = re.findall(r"(\d{2,3}\.\d{1,2})", txt)
        if nums:
            number_val = float(nums[0])
    except Exception:
        pass
    return hist, latest, number_val


# ---------------------------------------------------------------- composite
def _clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, x))


def build_composite(aaii_latest, naaim_latest):
    parts, weights = [], []
    if aaii_latest:
        spread = aaii_latest.get("bull_bear_spread")
        if spread is not None:
            parts.append(("aaii_bull_bear", _clamp(50 + spread, 0, 100)))
            weights.append(0.5)
    if naaim_latest:
        exp = naaim_latest.get("exposure")
        if exp is not None:
            parts.append(("naaim_exposure", _clamp(exp, 0, 100)))
            weights.append(0.5)
    if not parts:
        return {"fear_greed_tilt": None, "label": "no data",
                "components": {}, "note": "no sentiment inputs available"}
    tilt = round(sum(p[1] * w for p, w in zip(parts, weights)) / sum(weights), 1)
    if tilt <= 20:
        label = "extreme fear"
    elif tilt <= 40:
        label = "fear"
    elif tilt <= 60:
        label = "neutral"
    elif tilt <= 80:
        label = "greed"
    else:
        label = "extreme greed"
    return {"fear_greed_tilt": tilt, "label": label,
            "components": {k: round(v, 1) for k, v in parts},
            "note": "0=extreme fear, 100=extreme greed; "
                    "AAII maps bull-bear spread (pp) to 50+spread; "
                    "NAAIM maps equity exposure % (0-100 clamped)"}


def _put(out):
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out).encode("utf-8"),
                  ContentType="application/json")


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc).isoformat()
    errors, notes = [], []
    aaii_hist, aaii_latest, naaim_hist, naaim_latest, naaim_number = [], None, [], None, None

    try:
        aaii_hist, aaii_latest, _ = fetch_aaii()
    except Exception as e:
        errors.append("aaii: %s" % str(e)[:200])

    try:
        naaim_hist, naaim_latest, naaim_number = fetch_naaim()
        notes.append("NAAIM moved to subscription access 2026-08-01; "
                     "free history frozen at %s" % (naaim_latest["date"] if naaim_latest else "n/a"))
    except Exception as e:
        errors.append("naaim: %s" % str(e)[:200])

    out = {
        "generated_at": now,
        "engine": ENGINE,
        "aaii": {"latest": aaii_latest, "history": aaii_hist,
                 "count": len(aaii_hist), "source": AAII_XLS_URL},
        "naaim": {"latest": naaim_latest, "history": naaim_hist,
                  "count": len(naaim_hist),
                  "number_page_value": naaim_number,
                  "source": NAAIM_CHART_URL},
        "composite": build_composite(aaii_latest, naaim_latest),
        "notes": notes,
    }
    if errors:
        out["errors"] = errors
    try:
        _put(out)
        status = "ok"
    except Exception as e:
        status = "s3_error: %s" % str(e)[:200]
    return {"statusCode": 200, "body": json.dumps({
        "status": status, "engine": ENGINE,
        "aaii_points": len(aaii_hist), "naaim_points": len(naaim_hist),
        "errors": errors})}
