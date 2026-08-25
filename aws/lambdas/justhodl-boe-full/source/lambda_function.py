"""justhodl-boe-full v1.0.0 -- Bank of England full-history warehouse.

Queue #4 (no dedicated engine existed; 26 keys came from shared
pullers with 2015-/current-month windows). Two lanes:

  ZIPS   the yield-curve ARCHIVES -- daily full nominal/real/
         inflation curves back to 1970 -- harvested live from the
         /statistics/yield-curves page (every .zip href), mirrored
         verbatim with Last-Modified conditionals
  IADB   flagship series at FULL window (Datefrom=01/Jan/1694 --
         the API clamps to each series' true start), one verbatim
         CSV.gz per code; unknown codes fail NAMED (probe-validated
         universe, house rule)
Refresh rate(12 hours). data/warm/boe-full/; manifest -> boe-note-v2.
"""
import gzip
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

ENGINE_VERSION = "justhodl-boe-full v1.0.0 ops4977 iadb+curves"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
SITE = "https://www.bankofengland.co.uk"
YC_PAGE = SITE + "/statistics/yield-curves"
IADB = (SITE + "/boeapps/iadb/fromshowcolumns.asp?csv.x=yes"
        "&SeriesCodes=%s&CSVF=TN&UsingCodes=Y"
        "&Datefrom=01/Jan/1694")
ROOT = "data/warm/boe-full/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
UA = {"User-Agent": "Mozilla/5.0 JustHodl Research "
      "(raafouis@gmail.com)"}
BUDGET_S = 640
SPACING = 0.5

CODES = [
    "IUDSOIA", "IUDBEDR",                       # SONIA, Bank Rate
    "XUDLUSS", "XUDLERS", "XUDLJYS", "XUDLSFS",  # FX spot dailies
    "XUDLCDS", "XUDLADS", "XUDLBK89", "XUDLBK97",
    "XUDLGBG",                                   # ERI
    "LPMAUYN", "LPMVWYT", "LPMB3VJ",             # money aggregates
    "LPMVQKW", "LPMBL22", "LPMBI2O",
    "IUMTLMV", "IUMBV34", "IUMBV42",             # lending rates
    "IUMCCTL", "IUMB482", "IUMSOIA",
    "CFMBI59", "CFMBJ84",                        # capital issues
    "RPMTBIA", "RPMB56C",                        # repo/other
    "XUDLDKS", "XUDLNKS", "XUDLSKS",             # more FX
    "XUDLHDS", "XUDLSGS", "XUDLTWS",
    "IUDMNZC", "IUDLNZC", "IUDSNZC",             # gilt zc anchors
]

s3 = boto3.client("s3", region_name="us-east-1")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _j(key, default=None):
    try:
        return json.loads(
            s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def _put_json(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, indent=1).encode(),
                  ContentType="application/json")


def fetch(url, timeout=120, cap=80_000_000, head_only=False):
    req = urllib.request.Request(
        url, headers=UA, method="HEAD" if head_only else "GET")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        lm = r.headers.get("Last-Modified") or ""
        ln = int(r.headers.get("Content-Length") or 0)
        return (lm, ln, b"" if head_only else r.read(cap))


def zips_lane(state, t0):
    st = state.setdefault("zips", {})
    try:
        _, _, html_b = fetch(YC_PAGE, cap=4_000_000)
        html = html_b.decode("utf-8", "replace")
        hrefs = sorted(set(re.findall(
            r'href="(/-/media/[^"]+?\.zip)[^"]*"', html, re.I)))
        state["zip_universe"] = len(hrefs)
    except Exception as e:
        state["failures"]["_yc_page"] = str(e)[:100]
        hrefs = ["/-/media" + (v.get("path") or "")
                 for v in st.values() if v.get("path")]
    for h in hrefs:
        if time.time() - t0 > BUDGET_S - 120:
            break
        name = re.sub(r"[^A-Za-z0-9._-]+", "_",
                      h.rsplit("/", 1)[-1])[:120]
        url = SITE + h
        prev = st.get(name) or {}
        try:
            lm, ln, _ = fetch(url, head_only=True, timeout=60)
            if lm and prev.get("lm") == lm and \
                    prev.get("bytes") == ln:
                st[name] = dict(prev, status="unchanged",
                                path=h)
                continue
            lm2, ln2, body = fetch(url, timeout=240)
            s3.put_object(
                Bucket=BUCKET, Key=ROOT + "curves/" + name,
                Body=body, ContentType="application/zip",
                Metadata={"engine": "boe-full",
                          "src_lm": (lm2 or lm)[:60]})
            st[name] = {"bytes": len(body), "lm": lm2 or lm,
                        "status": "fresh", "path": h}
            state["failures"].pop("zip:" + name, None)
        except Exception as e:
            state["failures"]["zip:" + name] = str(e)[:90]
        time.sleep(SPACING)


def iadb_lane(state, t0):
    st = state.setdefault("iadb", {})
    for code in CODES:
        if time.time() - t0 > BUDGET_S - 40:
            break
        prev = st.get(code) or {}
        try:
            age = time.time() - float(prev.get("epoch") or 0)
        except Exception:
            age = 10**9
        if prev.get("ok") and age < 11 * 3600:
            continue
        try:
            _, _, body = fetch(IADB % code, timeout=120,
                               cap=30_000_000)
            txt = body.decode("utf-8", "replace")
            rows = txt.count("\n")
            if rows < 5 or "<html" in txt[:400].lower():
                raise RuntimeError(
                    "non-csv/thin (%d rows) head=%r"
                    % (rows, txt[:80]))
            s3.put_object(
                Bucket=BUCKET,
                Key=ROOT + "iadb/%s.csv.gz" % code,
                Body=gzip.compress(body),
                ContentType="application/gzip",
                Metadata={"engine": "boe-full", "code": code,
                          "rows": str(rows)})
            first = txt.split("\n", 2)[1][:24] if rows > 1 else ""
            st[code] = {"ok": True, "rows": rows,
                        "bytes": len(body), "first": first,
                        "epoch": time.time(), "at": _now()}
            state["failures"].pop("iadb:" + code, None)
        except Exception as e:
            st[code] = {"ok": False, "epoch": time.time()}
            state["failures"]["iadb:" + code] = str(e)[:110]
        time.sleep(SPACING)


def lambda_handler(event, ctx=None):
    t0 = time.time()
    state = _j(STATE_KEY, None) or {"version": "1.0.0",
                                    "failures": {}}
    if float(state.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held"}
    state["lease_until"] = time.time() + BUDGET_S + 120
    _put_json(STATE_KEY, state)

    zips_lane(state, t0)
    iadb_lane(state, t0)

    state["lease_until"] = 0
    state["as_of"] = _now()
    z = state.get("zips") or {}
    ia = state.get("iadb") or {}
    _put_json(STATE_KEY, state)
    _put_json(MANIFEST_KEY, {
        "as_of": state["as_of"], "engine": "justhodl-boe-full",
        "version": "1.0.0",
        "curve_zips": len(z),
        "curve_mb": round(sum(v.get("bytes") or 0
                              for v in z.values()) / 1e6, 1),
        "iadb_codes_ok": sum(1 for v in ia.values()
                             if v.get("ok")),
        "iadb_rows": sum(v.get("rows") or 0 for v in ia.values()),
        "failures": len(state["failures"]),
        "note": ("full BoE warehouse: yield-curve zip archives "
                 "(daily curves since 1970) verbatim-conditional + "
                 "flagship IADB series at full window since series "
                 "inception; unknown codes fail named")})
    return {"ok": True, "zips": len(z),
            "iadb_ok": sum(1 for v in ia.values() if v.get("ok")),
            "failures": len(state["failures"]),
            "elapsed_s": round(time.time() - t0, 1)}
