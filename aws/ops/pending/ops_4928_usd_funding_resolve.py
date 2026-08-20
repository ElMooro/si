"""ops/4928 -- offshore-USD funding: identifier resolution (read-only).

4927 proved NY Fed TGCR/BGCR live with volumes+percentiles, and proved
BIS WS_LBS_D_PUB is ALREADY banked in the lake and read by nothing.
Two legs came back blocked and must be resolved before any engine is
written, because binding to a guessed FRED id is exactly how a board
starts lying quietly:

  * FRED returned 429 on every call -- the house rate-limit trap. This
    run is SINGLE-FLIGHT and PACED (1.2s floor between calls, 429 ->
    5/15/40/90s backoff, hard stop on 403) exactly like the scoped
    importer, and walks the H.8 and Commercial Paper RELEASES rather
    than guessing ids.
  * BIS LBS dimensions were read off a 3MB range slice; the currency
    column was misread because UNIT_MEASURE is also "USD" (the unit
    every BIS figure is reported in), while the real denomination dim
    is L_DENOM where USD is a CODE. This run gunzips the WHOLE banked
    object, parses by column NAME, and reports the true L_DENOM /
    L_POSITION / L_MEASURE code distribution -- plus the object's
    `truncated` metadata, because a truncated walk could have cut the
    USD rows off entirely.

Nothing is deployed and nothing is written to the bucket.
"""
import gzip
import io
import json
import re
import sys
import time
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
UA = {"User-Agent": "JustHodl research ops4928 admin@justhodl.ai"}
LBS_KEY = "data/warm/bis/data/WS_LBS_D_PUB.dat.gz"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
OUT = {"generated": datetime.now(timezone.utc).isoformat(), "op": 4928}


# ───────────────────────── paced FRED client ─────────────────────────
class FredBlocked(Exception):
    pass


_last = [0.0]
_calls = [0]


def fred_get(path, key, cap=4000000, r=None):
    """Single-flight, paced, 429-backoff FRED GET. 403 -> hard stop."""
    url = "https://api.stlouisfed.org/fred/%s&api_key=%s&file_type=json" % (
        path, key)
    for attempt, wait in enumerate((0, 5, 15, 40, 90)):
        if wait:
            time.sleep(wait)
        gap = time.time() - _last[0]
        if gap < 1.2:
            time.sleep(1.2 - gap)
        _last[0] = time.time()
        _calls[0] += 1
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=45) as resp:
                return json.loads(resp.read(cap).decode("utf-8", "replace"))
        except urllib.error.HTTPError as e:
            if e.code == 403:
                raise FredBlocked("403 on call %d" % _calls[0])
            if e.code == 429:
                if r:
                    r.log("      429 -> backoff %ss (attempt %d)"
                          % ((5, 15, 40, 90)[min(attempt, 3)], attempt + 1))
                continue
            return {"_error": "HTTP %s" % e.code}
        except Exception as e:  # noqa: BLE001
            return {"_error": "%s:%s" % (type(e).__name__, str(e)[:60])}
    return {"_error": "429 exhausted"}


def fred_key():
    for fn in ("dollar-strength-agent", "justhodl-eurodollar-plumbing",
               "justhodl-sp500", "justhodl-repo-monitor"):
        try:
            env = (lam.get_function_configuration(FunctionName=fn)
                   .get("Environment") or {}).get("Variables") or {}
            if env.get("FRED_API_KEY"):
                return env["FRED_API_KEY"], fn
        except Exception:  # noqa: BLE001
            continue
    return None, None


# ───────────────────────── A. BIS LBS truth ──────────────────────────
def probe_bis(r):
    r.section("A. BIS WS_LBS_D_PUB — the banked object, parsed properly")
    res = {}
    try:
        h = s3.head_object(Bucket=B, Key=LBS_KEY)
        res["size_mb"] = round(h["ContentLength"] / 1e6, 2)
        res["modified"] = h["LastModified"].isoformat()
        res["metadata"] = h.get("Metadata", {})
        r.kv(size_mb=res["size_mb"], modified=res["modified"][:10],
             truncated=res["metadata"].get("truncated", "?"))
    except Exception as e:  # noqa: BLE001
        r.fail("head_object failed: %s" % str(e)[:90])
        OUT["A"] = {"ok": False}
        return

    try:
        raw = s3.get_object(Bucket=B, Key=LBS_KEY)["Body"].read()
        txt = gzip.GzipFile(fileobj=io.BytesIO(raw)).read().decode(
            "utf-8", "replace")
    except Exception as e:  # noqa: BLE001
        r.fail("gunzip failed: %s" % str(e)[:90])
        OUT["A"] = {"ok": False}
        return

    lines = txt.splitlines()
    cols = [c.strip().strip('"') for c in lines[0].split(",")]
    idx = {c: i for i, c in enumerate(cols)}
    res["columns"] = cols
    res["rows_total"] = len(lines) - 1
    r.ok("gunzipped: %d rows, %d columns" % (len(lines) - 1, len(cols)))

    need = ("L_DENOM", "L_POSITION", "L_MEASURE", "L_INSTR", "L_REP_CTY",
            "L_CP_SECTOR", "L_CP_COUNTRY", "L_PARENT_CTY", "TIME_PERIOD",
            "OBS_VALUE", "UNIT_MULT")
    missing = [c for c in need if c not in idx]
    res["missing_cols"] = missing
    if missing:
        r.warn("columns absent: %s" % missing)

    counts = {c: Counter() for c in ("L_DENOM", "L_POSITION", "L_MEASURE",
                                     "L_INSTR", "L_CP_SECTOR", "L_REP_CTY")}
    periods = Counter()
    usd_rows = 0
    for ln in lines[1:]:
        f = ln.split(",")
        if len(f) < len(cols) - 4:
            continue
        for c in counts:
            if c in idx and idx[c] < len(f):
                counts[c][f[idx[c]]] += 1
        if "L_DENOM" in idx and idx["L_DENOM"] < len(f) \
                and f[idx["L_DENOM"]] == "USD":
            usd_rows += 1
            if "TIME_PERIOD" in idx and idx["TIME_PERIOD"] < len(f):
                periods[f[idx["TIME_PERIOD"]]] += 1

    res["usd_rows"] = usd_rows
    res["code_counts"] = {c: v.most_common(12) for c, v in counts.items()}
    res["latest_usd_periods"] = sorted(periods)[-6:] if periods else []
    r.kv(usd_denominated_rows=usd_rows,
         latest_usd_period=(res["latest_usd_periods"][-1]
                            if res["latest_usd_periods"] else "NONE"))
    for c, top in res["code_counts"].items():
        r.log("  %-12s %s" % (c, ", ".join("%s:%d" % (k or "-", n)
                                           for k, n in top[:8])))

    # sample real USD claim/liability rows so the engine binds to shape
    samples = []
    for ln in lines[1:]:
        f = ln.split(",")
        if "L_DENOM" in idx and idx["L_DENOM"] < len(f) \
                and f[idx["L_DENOM"]] == "USD":
            samples.append(ln[:260])
            if len(samples) >= 4:
                break
    res["usd_samples"] = samples
    for s in samples:
        r.log("  USD row: %s" % s)

    # allowed codes straight from BIS
    for dim in ("L_DENOM", "L_POSITION", "L_MEASURE"):
        try:
            u = ("https://stats.bis.org/api/v1/availableconstraint/"
                 "WS_LBS_D_PUB/all/all/%s" % dim)
            req = urllib.request.Request(u, headers=UA)
            with urllib.request.urlopen(req, timeout=40) as resp:
                xml = resp.read(400000).decode("utf-8", "replace")
            codes = re.findall(r'Value>([^<]{1,12})</', xml)[:40]
            res.setdefault("bis_codes", {})[dim] = codes
            r.log("  BIS %s allowed: %s" % (dim, ",".join(codes[:20])))
        except Exception as e:  # noqa: BLE001
            res.setdefault("bis_codes", {})[dim] = "err:%s" % str(e)[:50]

    OUT["A"] = res


# ───────────────────────── B. paced FRED walk ────────────────────────
KW_H8 = ("large time deposit", "borrowings", "net due to related",
         "deposits, all commercial", "other deposits", "total deposits")
KW_CP = ("outstanding", "asset-backed", "asset backed", "a2/p2",
         "1-month", "3-month", "overnight", "aa financial",
         "aa nonfinancial", "aa asset")
SOFR_IDS = ("SOFR30DAYAVG", "SOFR90DAYAVG", "SOFR180DAYAVG", "SOFRINDEX",
            "SOFRVOL", "SOFR1", "SOFR25", "SOFR75", "SOFR99",
            "BGCR", "TGCR", "BGCRVOL", "TGCRVOL", "OBFRVOL", "EFFRVOL")


def probe_fred(r):
    r.section("B. FRED — paced release walk (H.8 + Commercial Paper)")
    key, donor = fred_key()
    if not key:
        r.fail("no FRED_API_KEY on any donor")
        OUT["B"] = {"ok": False}
        return
    r.kv(fred_donor=donor)
    out = {"releases": {}, "series": {}, "sofr_term": {}}
    try:
        j = fred_get("releases?limit=1000", key, r=r)
        if j.get("_error"):
            r.fail("/releases -> %s" % j["_error"])
            OUT["B"] = {"ok": False, "error": j["_error"]}
            return
        rels = j.get("releases", [])
        want = {}
        for rel in rels:
            n = (rel.get("name") or "").lower()
            if "assets and liabilities of commercial banks" in n:
                want["H8"] = rel
            elif n.strip() == "commercial paper":
                want["CP"] = rel
        out["releases"] = {k: {"id": v["id"], "name": v["name"]}
                           for k, v in want.items()}
        r.ok("releases=%d  H.8=%s  CP=%s"
             % (len(rels), want.get("H8", {}).get("id"),
                want.get("CP", {}).get("id")))

        for tag, kws in (("H8", KW_H8), ("CP", KW_CP)):
            rel = want.get(tag)
            if not rel:
                r.warn("%s release not resolved" % tag)
                continue
            j2 = fred_get("release/series?release_id=%s&limit=1000"
                          "&order_by=popularity&sort_order=desc" % rel["id"],
                          key, r=r)
            if j2.get("_error"):
                r.warn("%s release/series -> %s" % (tag, j2["_error"]))
                continue
            ser = j2.get("seriess", [])
            hits = []
            for s in ser:
                t = (s.get("title") or "").lower()
                if any(k in t for k in kws):
                    hits.append({"id": s["id"], "title": s.get("title"),
                                 "freq": s.get("frequency_short"),
                                 "units": s.get("units_short"),
                                 "end": s.get("observation_end"),
                                 "updated": s.get("last_updated"),
                                 "pop": s.get("popularity")})
            hits.sort(key=lambda x: -(x.get("pop") or 0))
            out["series"][tag] = {"release_series": len(ser),
                                  "matched": hits[:45]}
            r.ok("%s release %s: %d series -> %d matches"
                 % (tag, rel["id"], len(ser), len(hits)))
            for m in hits[:22]:
                r.log("    %-24s %-3s %-9s end=%-10s %s"
                      % (m["id"], m["freq"] or "", m["units"] or "",
                         m["end"], (m["title"] or "")[:62]))

        r.section("C. SOFR term structure + rate-volume series")
        for sid in SOFR_IDS:
            j3 = fred_get("series?series_id=%s" % sid, key, r=r)
            if j3.get("_error"):
                out["sofr_term"][sid] = {"alive": False,
                                         "err": j3["_error"]}
                r.log("  %-14s DEAD  %s" % (sid, j3["_error"][:50]))
                continue
            try:
                s = j3["seriess"][0]
                out["sofr_term"][sid] = {
                    "alive": True, "title": s.get("title"),
                    "freq": s.get("frequency_short"),
                    "units": s.get("units_short"),
                    "end": s.get("observation_end"),
                    "updated": s.get("last_updated")}
                r.log("  %-14s LIVE  end=%-11s %-8s %s"
                      % (sid, s.get("observation_end"),
                         s.get("units_short") or "",
                         (s.get("title") or "")[:56]))
            except Exception:  # noqa: BLE001
                out["sofr_term"][sid] = {"alive": False, "err": "shape"}
                r.log("  %-14s DEAD  unexpected shape" % sid)
    except FredBlocked as e:
        r.fail("FRED HARD-BLOCKED (403): %s — stopping, no retry storm" % e)
        out["blocked"] = str(e)
    out["fred_calls"] = _calls[0]
    r.kv(fred_calls=_calls[0])
    OUT["B"] = out


def main():
    t0 = datetime.now(timezone.utc)
    with report("4928-usd-funding-resolve") as r:
        r.heading("ops 4928 — offshore-USD funding: identifier "
                  "resolution (read-only)")
        probe_bis(r)
        probe_fred(r)
        (ROOT / "aws" / "ops" / "reports" / "4928.json").write_text(
            json.dumps(OUT, indent=1, default=str))
        r.kv(status="RESOLVED",
             duration_s=int((datetime.now(timezone.utc) - t0)
                            .total_seconds()))


if __name__ == "__main__":
    main()
    sys.exit(0)
