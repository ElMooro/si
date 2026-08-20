"""ops/4939 -- PROBE: find the CIKs that actually file 13F-HR today.

READ-ONLY. Changes no roster, no engine state, no S3 data. Its whole job
is to replace a guess with evidence.

Standing problem the 4936-4938 arc surfaced but did not fix: four of the
eighteen tracked CIKs are wrong or dead, and no amount of parser work can
fix a roster pointing at entities that stopped filing.

  GREENLIGHT 0001079114 -> last 13F-HR 2023-12-31, ten quarters stale
  ELLIOTT    0001286922 -> no 13F-HR at all (the 18th "missing" fund)
  SCION      0001649339 -> last 13F-HR 2025-09-30
  PERSHING   0001336528 -> last 13F-HR 2026-03-31

A manager that re-registers keeps the NAME and gets a NEW CIK. The old
one simply goes quiet -- which is why this looked like a parser bug for
months. So: ask EDGAR which CIKs file 13F-HR under each name, and rank
them by their most recent filing.

Method per fund:
  1. EDGAR company search, atom output, type=13F-HR, matched on name
  2. for every candidate CIK, read data.sec.gov submissions
  3. keep those with a real 13F-HR, sorted by latest reportDate
  4. classify: CURRENT / LAGGING / DARK / NO_13F

Output is a recommendation table only. Nothing is applied here -- the
roster edit is a separate, reviewable commit, because silently swapping
a CIK is exactly the kind of change that should never happen invisibly.

TRAP ALREADY PAID FOR: sec.gov 403s any request without a real
User-Agent (ops 4914, and I re-broke it earlier today by probing with
bare curl and mistaking the 403 for "no network access").
"""
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

UA = "JustHodl Research raafouis@gmail.com"
ROSTER = {
    "BERKSHIRE": ("0001067983", "Berkshire Hathaway"),
    "BRIDGEWATER": ("0001350694", "Bridgewater Associates"),
    "RENAISSANCE": ("0001037389", "Renaissance Technologies"),
    "AQR": ("0001167557", "AQR Capital Management"),
    "TWO_SIGMA": ("0001179392", "Two Sigma Investments"),
    "CITADEL": ("0001423053", "Citadel Advisors"),
    "MILLENNIUM": ("0001273087", "Millennium Management"),
    "PERSHING": ("0001336528", "Pershing Square Capital"),
    "GREENLIGHT": ("0001079114", "Greenlight Capital"),
    "SOROS": ("0001029160", "Soros Fund Management"),
    "TIGER_GLOBAL": ("0001167483", "Tiger Global Management"),
    "COATUE": ("0001135730", "Coatue Management"),
    "BAUPOST": ("0001061165", "Baupost Group"),
    "ELLIOTT": ("0001286922", "Elliott Investment Management"),
    "SCION": ("0001649339", "Scion Asset Management"),
    "DURATION": ("0001582202", "Duration Capital"),
    "POINT72": ("0001603466", "Point72 Asset Management"),
    "LONE_PINE": ("0001061768", "Lone Pine Capital"),
}


def _get(url, timeout=25):
    req = urllib.request.Request(url, headers={
        "User-Agent": UA, "Accept-Encoding": "gzip, deflate",
        "Accept": "application/json,application/atom+xml,*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            import gzip
            raw = gzip.decompress(raw)
        return raw.decode("utf-8", "replace")


def latest_13f(cik):
    """Most recent 13F-HR reportDate for a CIK, or None."""
    try:
        d = json.loads(_get(
            "https://data.sec.gov/submissions/CIK%s.json" % cik.zfill(10)))
    except Exception as e:
        return None, None, "ERR %s" % str(e)[:60]
    rec = d.get("filings", {}).get("recent", {})
    forms, per, filed = (rec.get("form", []), rec.get("reportDate", []),
                         rec.get("filingDate", []))
    hits = [(per[i], filed[i]) for i, f in enumerate(forms)
            if f in ("13F-HR", "13F-HR/A")]
    if not hits:
        return d.get("name"), None, "NO_13F"
    hits.sort(reverse=True)
    return d.get("name"), hits[0][0], None


def candidates(name):
    """EDGAR company search -> CIKs filing 13F-HR under this name."""
    url = ("https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"
           "&company=%s&type=13F-HR&dateb=&owner=include&count=40"
           "&output=atom" % urllib.parse.quote(name))
    try:
        xml = _get(url)
    except Exception as e:
        return [], "search failed: %s" % str(e)[:70]
    ciks = re.findall(r"CIK=(\d{10})", xml) or re.findall(
        r"<cik>(\d+)</cik>", xml)
    seen, out = set(), []
    for c in ciks:
        c = c.zfill(10)
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out[:8], None


def expected_quarter(now=None):
    now = now or datetime.now(timezone.utc)
    best = None
    for y, mth, d in ((now.year, 3, 31), (now.year, 6, 30),
                      (now.year, 9, 30), (now.year, 12, 31),
                      (now.year - 1, 12, 31), (now.year - 1, 9, 30)):
        qe = datetime(y, mth, d, tzinfo=timezone.utc)
        if (now - qe).days >= 45 and (best is None or qe > best):
            best = qe
    return best.date().isoformat()


with report("ops_4939_13f_roster_probe") as R:
    exp = expected_quarter()
    R.log("expected latest reportable quarter: %s" % exp)
    rows, recommend = [], []

    for key, (cik, name) in ROSTER.items():
        secname, cur, err = latest_13f(cik)
        time.sleep(0.15)
        status = ("NO_13F" if err == "NO_13F" else
                  "ERR" if err else
                  "CURRENT" if cur == exp else "LAGGING")
        row = {"fund": key, "cik": cik, "sec_name": secname,
               "latest_13f": cur, "status": status}

        if status in ("NO_13F", "ERR") or (
                status == "LAGGING" and cur and cur < exp):
            cands, cerr = candidates(name)
            time.sleep(0.3)
            scored = []
            for c in cands:
                if c.lstrip("0") == cik.lstrip("0"):
                    continue
                nm, lat, e2 = latest_13f(c)
                time.sleep(0.15)
                if lat:
                    scored.append({"cik": c, "name": nm, "latest": lat,
                                   "current": lat == exp})
            scored.sort(key=lambda r: r["latest"], reverse=True)
            row["candidates"] = scored[:4]
            row["search_error"] = cerr
            better = [c for c in scored
                      if not cur or c["latest"] > cur]
            if better:
                top = better[0]
                row["RECOMMEND"] = top["cik"]
                recommend.append({
                    "fund": key, "from": cik, "to": top["cik"],
                    "to_name": top["name"], "gains": "%s -> %s"
                    % (cur or "none", top["latest"]),
                    "unambiguous": len(better) == 1 or (
                        top["current"] and not better[1]["current"])})
        rows.append(row)
        R.log("%-13s %s  %-10s %-9s %s"
              % (key, cik, cur or "-", status,
                 (secname or "")[:42]))

    R.log("")
    R.log("=== ROSTER RECOMMENDATIONS (not applied) ===")
    if not recommend:
        R.log("none -- every lagging filer is simply late or terminated, "
              "no better CIK exists")
    for r in recommend:
        R.log("%-12s %s -> %s  (%s)  %s  [%s]"
              % (r["fund"], r["from"], r["to"], r["gains"],
                 (r["to_name"] or "")[:40],
                 "UNAMBIGUOUS" if r["unambiguous"] else "REVIEW"))

    dark = [r["fund"] for r in rows
            if r["status"] in ("NO_13F",)
            or (r["latest_13f"] and r["latest_13f"] < exp
                and not r.get("RECOMMEND"))]
    R.log("")
    R.log("TERMINATED-or-DARK (no better CIK found, mark honestly): %s"
          % dark)
    # A probe still needs a real failure condition, or a dead SEC endpoint
    # reads as "nothing to fix". Errors on >3 funds means the source is
    # down and this report is worthless -- fail loudly rather than bank a
    # false all-clear. (This is the 4936 lesson: a gate must be able to
    # fail, and I must be able to name the data that makes it fail.)
    errs = [r["fund"] for r in rows if r["status"] == "ERR"]
    if len(errs) > 3:
        R.log("PROBE INVALID: SEC unreachable for %s" % errs)
        sys.exit(1)
    if errs:
        R.log("partial: SEC errored for %s (report still usable)" % errs)
    R.log("probe complete -- %d funds checked, %d recommendations, "
          "0 changes applied" % (len(rows), len(recommend)))
