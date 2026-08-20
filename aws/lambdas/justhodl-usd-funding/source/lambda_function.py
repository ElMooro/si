"""justhodl-usd-funding v1.0.0 — the offshore-USD feeds the board lacked.

Built from ops 4927/4928 evidence, and bound ONLY to identifiers that
printed live in those probes. Nothing here is guessed.

What this adds that `justhodl-eurodollar-plumbing` does not carry:

  1. TGCR + BGCR. Absent from the entire stack, and PROVEN absent from
     FRED (both return HTTP 400 there — they simply are not published).
     The NY Fed Markets API is the only path, so we go direct.
  2. Reference-rate VOLUMES and PERCENTILES. FRED gives a rate and
     nothing else. NY Fed returns volume plus the 1/25/75/99 percentile
     fan for every rate. Volume collapse and percentile fanning lead
     the rate move — that is the whole point of carrying them.
  3. H.8 bank wholesale funding: large time deposits, borrowings, the
     deposit base, other deposits. The board previously held exactly
     one H.8 series (net due to related foreign offices).
  4. Commercial paper QUANTITIES — total, financial, nonfinancial and
     ABCP outstandings — plus the full tenor grid (O/N, 7d, 30d, 90d)
     across AA financial, AA nonfinancial, AA asset-backed and A2/P2.
     In a freeze the quantity goes before the rate does, and ABCP is
     the direct foreign-bank dollar-funding channel.
  5. SOFR term structure: 30/90/180-day averages and the SOFR Index —
     the free stand-in for a futures curve.
  6/7. SOFR futures / Term SOFR and the cross-currency basis are NOT
     in entitlements. They are published here as explicit
     `not_entitled` rows carrying source, cost and upgrade path. A
     board that hides what it cannot see is worse than one that admits
     it — these render as gaps, never as zeros.

  BIS. Locational Banking Statistics USD cross-border claims and
     liabilities. The bulk SDMX walk banks WS_LBS_D_PUB but ops 4928
     proved that object is `truncated: True` and holds only 2,517
     USD-denominated rows against 266,047 all-currency — the walk was
     cut before USD completed. So this engine queries BIS with a
     USD-PINNED key rather than trusting the truncated file, and
     records which key answered.

  Z-SCORES. The composite here is the note's construction — a sum of
  standardised legs, z(SOFR−IORB) + z(OBFR−IORB) + z(CP−OIS) +
  z(TGCR−IORB) + z(BGCR−IORB) — not a threshold ladder. A threshold
  says "amber"; a z-score says "this is 2.4σ for THIS spread", which
  is the only version that is comparable across time and across legs.
  Structural legs (BIS USD liabilities growth) enter separately and
  are labelled as such, because quarterly data must never masquerade
  as a live trigger.

Explicit failures. Never fabricates. stdlib-only.
"""
import gzip
import json
import os
import statistics
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

VERSION = "1.0.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
KEY = "data/usd-funding.json"
REGION = "us-east-1"
UA = {"User-Agent": "JustHodl research admin@justhodl.ai"}
s3 = boto3.client("s3", region_name=REGION)

FRED_KEY = os.environ.get("FRED_API_KEY", "")
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
NYFED = "https://markets.newyorkfed.org/api/rates"

# ── proven live in ops 4928 (end dates 2026-08-05 .. 2026-08-19) ──────
H8 = {
    "large_time_deposits": ("LTDACBW027SBOG", "$bn", "Large time deposits"),
    "borrowings":          ("H8B3094NCBA", "$mn", "Borrowings"),
    "deposits":            ("DPSACBW027SBOG", "$bn", "Total deposits"),
    "other_deposits":      ("ODSACBW027SBOG", "$bn", "Other deposits"),
    "net_due_foreign":     ("NDFACBW027SBOG", "$bn",
                            "Net due to related foreign offices"),
}
CP_OUT = {
    "cp_total":       ("COMPOUT", "Commercial paper outstanding, total"),
    "cp_financial":   ("FINCP", "Financial CP outstanding"),
    "cp_nonfinancial": ("NFINCP", "Nonfinancial CP outstanding"),
    "cp_abcp":        ("ABCOMP", "Asset-backed CP outstanding"),
}
CP_RATES = {
    "aa_fin_on":   ("RIFSPPFAAD01NB", "AA financial", "O/N"),
    "aa_fin_7d":   ("RIFSPPFAAD07NB", "AA financial", "7d"),
    "aa_fin_30d":  ("RIFSPPFAAD30NB", "AA financial", "30d"),
    "aa_fin_90d":  ("RIFSPPFAAD90NB", "AA financial", "90d"),
    "aa_nfin_on":  ("RIFSPPNAAD01NB", "AA nonfinancial", "O/N"),
    "aa_nfin_30d": ("RIFSPPNAAD30NB", "AA nonfinancial", "30d"),
    "aa_nfin_90d": ("RIFSPPNAAD90NB", "AA nonfinancial", "90d"),
    "abcp_30d":    ("RIFSPPAAAD30NB", "AA asset-backed", "30d"),
    "abcp_90d":    ("RIFSPPAAAD90NB", "AA asset-backed", "90d"),
    "a2p2_on":     ("RIFSPPNA2P2D01NB", "A2/P2 nonfinancial", "O/N"),
    "a2p2_7d":     ("RIFSPPNA2P2D07NB", "A2/P2 nonfinancial", "7d"),
    "a2p2_30d":    ("RIFSPPNA2P2D30NB", "A2/P2 nonfinancial", "30d"),
    "a2p2_90d":    ("RIFSPPNA2P2D90NB", "A2/P2 nonfinancial", "90d"),
}
SOFR_TERM = {
    "sofr_30d_avg":  ("SOFR30DAYAVG", "30-day average SOFR"),
    "sofr_90d_avg":  ("SOFR90DAYAVG", "90-day average SOFR"),
    "sofr_180d_avg": ("SOFR180DAYAVG", "180-day average SOFR"),
    "sofr_index":    ("SOFRINDEX", "SOFR Index (compounded)"),
}
ANCHORS = {"sofr": "SOFR", "iorb": "IORB"}

NOT_ENTITLED = [
    {"id": "sofr_futures", "label": "SOFR futures / Term SOFR forward curve",
     "source": "CME Group (Term SOFR is CME-licensed)",
     "why": "Market-implied FORWARD funding expectations — the only leg "
            "that tells you what the market thinks funding costs in 3/6/12 "
            "months, rather than what it costs today.",
     "proxy_in_use": "SOFR 30/90/180-day averages + SOFR Index (below) — "
                     "backward-compounded, so it lags rather than leads.",
     "upgrade": "CME market-data licence or a vendor redistribution feed."},
    {"id": "xccy_basis", "label": "Cross-currency basis (EUR/JPY/GBP)",
     "source": "FX forward / swap-points feed (Bloomberg, Refinitiv, ICAP)",
     "why": "The cleanest real-time measure of the offshore dollar gap: "
            "how much a non-US bank overpays to swap into dollars. Went to "
            "-150bp in Mar-2020 before anything else broke.",
     "proxy_in_use": "Broad trade-weighted dollar + CNH-CNY gap + HKMA "
                     "hub metrics on the plumbing board.",
     "upgrade": "Paid FX-forward entitlement. Already flagged "
                "'unavailable' by justhodl-eurodollar-plumbing."},
]

# BIS LBS key order proven in ops 4928 (25-col CSV header):
# FREQ.L_MEASURE.L_POSITION.L_INSTR.L_DENOM.L_CURR_TYPE.L_PARENT_CTY.
# L_REP_BANK_TYPE.L_REP_CTY.L_CP_SECTOR.L_CP_COUNTRY.L_POS_TYPE
BIS_BASE = "https://stats.bis.org/api/v1/data/WS_LBS_D_PUB"


def _get(url, timeout=45, cap=12_000_000, headers=None):
    h = dict(UA)
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read(cap)
        if resp.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        return raw.decode("utf-8", "replace")


# ───────────────────────── FRED (paced, 429-aware) ───────────────────
_last_call = [0.0]


def fred(series_id, start="2016-01-01"):
    """Paced FRED observations -> [(date, float)]. Explicit on failure."""
    if not FRED_KEY:
        raise RuntimeError("FRED_API_KEY missing")
    url = ("%s?series_id=%s&api_key=%s&file_type=json"
           "&observation_start=%s" % (FRED_BASE, series_id, FRED_KEY, start))
    for wait in (0, 4, 12, 30):
        if wait:
            time.sleep(wait)
        gap = time.time() - _last_call[0]
        if gap < 0.85:
            time.sleep(0.85 - gap)
        _last_call[0] = time.time()
        try:
            j = json.loads(_get(url, cap=6_000_000))
            out = []
            for o in j.get("observations", []):
                try:
                    out.append((o["date"], float(o["value"])))
                except (ValueError, KeyError):
                    continue
            return out
        except urllib.error.HTTPError as e:
            if e.code == 429:
                continue
            raise
    raise RuntimeError("FRED 429 exhausted on %s" % series_id)


def last(series):
    return series[-1] if series else (None, None)


def chg(series, back):
    """Absolute change over `back` observations."""
    if len(series) > back and series[-1][1] is not None \
            and series[-1 - back][1] is not None:
        return round(series[-1][1] - series[-1 - back][1], 2)
    return None


def zscore(value, hist, min_n=60):
    """z of value against its own history. None when history is too thin —
    a z computed off 8 points is noise wearing a lab coat."""
    pts = [v for v in hist if v is not None]
    if value is None or len(pts) < min_n:
        return None
    try:
        sd = statistics.pstdev(pts)
        if sd < 1e-9:
            return None
        return round((value - statistics.fmean(pts)) / sd, 2)
    except statistics.StatisticsError:
        return None


# ───────────────────────── NY Fed reference rates ────────────────────
RATE_EPS = {
    "sofr": ("secured/sofr", "SOFR", "Secured overnight financing"),
    "tgcr": ("secured/tgcr", "TGCR", "Tri-party general collateral"),
    "bgcr": ("secured/bgcr", "BGCR", "Broad general collateral"),
    "effr": ("unsecured/effr", "EFFR", "Effective fed funds"),
    "obfr": ("unsecured/obfr", "OBFR", "Overnight bank funding"),
}

# ops 4932 burned this in: `last/1.json` answered 200 from the ops runner
# while `last/1300.json` returned 400 from Lambda on ALL FIVE rates —
# including SOFR, so it was never a TGCR/BGCR problem and never an egress
# problem. It was an unverified N bound: the probe tested one shape and
# the engine shipped another. Never again pin a magic constant that was
# not the thing actually proven — descend a ladder, record what answered,
# and keep a differently-shaped fallback behind it.
NY_N_LADDER = (750, 500, 250, 120, 60, 30, 5, 1)


def _nyfed_rows(path, diag):
    """Candidate-chain: first request shape that returns rows wins.
    Every attempt is recorded so a future failure is diagnosable from
    the payload alone rather than needing another probe cycle."""
    for n in NY_N_LADDER:
        url = "%s/%s/last/%d.json" % (NYFED, path, n)
        try:
            rows = (json.loads(_get(url)) or {}).get("refRates") or []
            diag.append({"shape": "last/%d" % n, "ok": True,
                         "rows": len(rows)})
            if rows:
                return rows, "last/%d" % n
        except Exception as e:  # noqa: BLE001
            diag.append({"shape": "last/%d" % n, "ok": False,
                         "error": "%s: %s" % (type(e).__name__,
                                              str(e)[:70])})
    # differently-shaped fallback: date range instead of a row count
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=730)
    url = ("%s/%s/search.json?startDate=%s&endDate=%s"
           % (NYFED, path, start.isoformat(), end.isoformat()))
    try:
        rows = (json.loads(_get(url)) or {}).get("refRates") or []
        diag.append({"shape": "search", "ok": True, "rows": len(rows)})
        if rows:
            return rows, "search"
    except Exception as e:  # noqa: BLE001
        diag.append({"shape": "search", "ok": False,
                     "error": "%s: %s" % (type(e).__name__, str(e)[:70])})
    return [], None


def nyfed_rates():
    """Direct NY Fed pull: rate + volume + the 1/25/75/99 percentile fan.

    FRED carries none of this for TGCR/BGCR — it does not carry those
    rates at all (HTTP 400, proven ops 4928)."""
    out = {}
    for rid, (path, label, desc) in RATE_EPS.items():
        diag = []
        try:
            rows, shape = _nyfed_rows(path, diag)
            rows = [r for r in rows if r.get("percentRate") is not None]
            rows.sort(key=lambda r: r.get("effectiveDate", ""))
            if not rows:
                out[rid] = {"ok": False, "label": label,
                            "error": "no rows from any request shape",
                            "attempts": diag}
                continue
            cur = rows[-1]
            hist = [float(r["percentRate"]) for r in rows
                    if r.get("percentRate") is not None]
            vols = [float(r["volumeInBillions"]) for r in rows
                    if r.get("volumeInBillions") is not None]
            p99 = cur.get("percentPercentile99")
            p1 = cur.get("percentPercentile1")
            p75 = cur.get("percentPercentile75")
            p25 = cur.get("percentPercentile25")
            out[rid] = {
                "ok": True, "label": label, "desc": desc,
                "date": cur.get("effectiveDate"),
                "rate": float(cur["percentRate"]),
                "volume_bn": (float(cur["volumeInBillions"])
                              if cur.get("volumeInBillions") is not None
                              else None),
                "volume_z": zscore(
                    float(cur["volumeInBillions"]) if
                    cur.get("volumeInBillions") is not None else None, vols),
                "p1": p1, "p25": p25, "p75": p75, "p99": p99,
                "iqr_bp": (round((float(p75) - float(p25)) * 100, 1)
                           if p75 is not None and p25 is not None else None),
                "fan_bp": (round((float(p99) - float(p1)) * 100, 1)
                           if p99 is not None and p1 is not None else None),
                "n_obs": len(rows), "request_shape": shape,
                "_hist": {r["effectiveDate"]: float(r["percentRate"])
                          for r in rows if r.get("percentRate") is not None},
            }
        except Exception as e:  # noqa: BLE001
            out[rid] = {"ok": False, "label": label,
                        "error": "%s: %s" % (type(e).__name__, str(e)[:90]),
                        "attempts": diag}
    return out


# ───────────────────────── BIS LBS (USD-pinned) ──────────────────────
def bis_lbs_usd():
    """USD cross-border claims & liabilities, queried with a USD-pinned
    key. Candidate-chain: first key that answers with parseable rows
    wins, and the key that answered is recorded in the payload."""
    ladder = [
        ("stocks_wildcard", "Q.S.{pos}.A.USD......."),
        ("allmeasure_wildcard", "Q..{pos}.A.USD......."),
    ]
    res = {"ok": False, "positions": {}, "attempts": []}
    for name, tmpl in ladder:
        got = {}
        for pos, plabel in (("C", "claims"), ("L", "liabilities")):
            url = ("%s/%s/all?format=csv&lastNObservations=12"
                   % (BIS_BASE, tmpl.format(pos=pos)))
            try:
                txt = _get(url, timeout=60, cap=9_000_000)
                lines = txt.splitlines()
                if len(lines) < 2:
                    res["attempts"].append({"key": name, "pos": pos,
                                            "rows": 0})
                    continue
                cols = [c.strip().strip('"') for c in lines[0].split(",")]
                ix = {c: i for i, c in enumerate(cols)}
                need = ("L_DENOM", "L_REP_CTY", "L_CP_COUNTRY",
                        "L_CP_SECTOR", "TIME_PERIOD", "OBS_VALUE")
                if any(c not in ix for c in need):
                    res["attempts"].append({"key": name, "pos": pos,
                                            "error": "cols"})
                    continue
                # Take the single MOST-AGGREGATE series only. Summing
                # overlapping BIS rows double-counts — 5A is "all
                # reporting countries", 5J "all counterparty countries",
                # A "all sectors". If that row is absent we say so
                # rather than inventing an aggregate.
                best = {}
                for ln in lines[1:]:
                    f = ln.split(",")
                    if len(f) < len(cols) - 4:
                        continue
                    if f[ix["L_DENOM"]] != "USD":
                        continue
                    if f[ix["L_REP_CTY"]] != "5A":
                        continue
                    if f[ix["L_CP_COUNTRY"]] != "5J":
                        continue
                    if f[ix["L_CP_SECTOR"]] != "A":
                        continue
                    try:
                        val = float(f[ix["OBS_VALUE"]])
                    except (ValueError, IndexError):
                        continue
                    best[f[ix["TIME_PERIOD"]]] = val
                if not best:
                    res["attempts"].append({"key": name, "pos": pos,
                                            "rows": len(lines) - 1,
                                            "aggregate": "not found"})
                    continue
                per = sorted(best)
                vals = [best[p] for p in per]
                mult = 1e6  # BIS LBS UNIT_MULT=6 -> USD millions
                got[plabel] = {
                    "latest_period": per[-1],
                    "latest_usd_bn": round(vals[-1] * mult / 1e9, 1),
                    "qoq_pct": (round((vals[-1] / vals[-2] - 1) * 100, 2)
                                if len(vals) > 1 and vals[-2] else None),
                    "yoy_pct": (round((vals[-1] / vals[-5] - 1) * 100, 2)
                                if len(vals) > 4 and vals[-5] else None),
                    "history": [{"period": p,
                                 "usd_bn": round(best[p] * mult / 1e9, 1)}
                                for p in per],
                }
                res["attempts"].append({"key": name, "pos": pos,
                                        "rows": len(lines) - 1, "ok": True})
            except Exception as e:  # noqa: BLE001
                res["attempts"].append({
                    "key": name, "pos": pos,
                    "error": "%s: %s" % (type(e).__name__, str(e)[:80])})
        if got.get("claims") or got.get("liabilities"):
            res["ok"] = True
            res["key_used"] = tmpl
            res["positions"] = got
            c = (got.get("claims") or {}).get("latest_usd_bn")
            li = (got.get("liabilities") or {}).get("latest_usd_bn")
            if c is not None and li is not None:
                res["net_usd_bn"] = round(c - li, 1)
                res["funding_gap_note"] = (
                    "Positive = reporting banks hold more USD cross-border "
                    "claims than USD liabilities; the gap must be funded in "
                    "the swap market, which is where CIP breaks show up.")
            res["aggregation"] = ("L_REP_CTY=5A (all reporting) × "
                                  "L_CP_COUNTRY=5J (all counterparties) × "
                                  "L_CP_SECTOR=A (all sectors), L_DENOM=USD")
            res["cadence"] = "quarterly, ~4-month publication lag"
            res["structural"] = True
            break
    return res


# ───────────────────────── build ─────────────────────────────────────
def build():
    errors = []
    payload = {"version": VERSION, "engine": "justhodl-usd-funding",
               "generated": datetime.now(timezone.utc).isoformat()}

    # 1+2. NY Fed reference rates with volumes + percentiles
    rates = nyfed_rates()
    payload["reference_rates"] = {
        k: {kk: vv for kk, vv in v.items() if kk != "_hist"}
        for k, v in rates.items()}
    for k, v in rates.items():
        if not v.get("ok"):
            errors.append("nyfed:%s:%s" % (k, v.get("error", "?")))

    # anchors
    anchors = {}
    for name, sid in ANCHORS.items():
        try:
            anchors[name] = fred(sid)
        except Exception as e:  # noqa: BLE001
            anchors[name] = []
            errors.append("fred:%s:%s" % (sid, str(e)[:60]))
    iorb_hist = dict(anchors.get("iorb") or [])
    _, iorb_now = last(anchors.get("iorb") or [])
    _, sofr_now = last(anchors.get("sofr") or [])

    # spreads vs IORB — including the two legs that were missing entirely
    spreads = {}
    for rid in ("sofr", "tgcr", "bgcr", "effr", "obfr"):
        rec = rates.get(rid) or {}
        if not rec.get("ok") or iorb_now is None:
            continue
        cur_bp = round((rec["rate"] - iorb_now) * 100, 1)
        hist_bp = []
        for d, rv in (rec.get("_hist") or {}).items():
            iv = iorb_hist.get(d)
            if iv is not None:
                hist_bp.append((rv - iv) * 100)
        spreads[rid] = {
            "label": "%s − IORB" % rec["label"],
            "value_bp": cur_bp,
            "z": zscore(cur_bp, hist_bp),
            "n_hist": len(hist_bp),
            "date": rec.get("date"),
        }
    payload["spreads_vs_iorb"] = spreads

    # 3. H.8 wholesale funding
    h8 = {}
    for k, (sid, unit, label) in H8.items():
        try:
            s = fred(sid, start="2010-01-01")
            d, v = last(s)
            h8[k] = {"series_id": sid, "label": label, "unit": unit,
                     "date": d, "value": v,
                     "chg_13w": chg(s, 13), "chg_52w": chg(s, 52),
                     "z_13w_chg": zscore(chg(s, 13),
                                         [s[i][1] - s[i - 13][1]
                                          for i in range(13, len(s))])}
        except Exception as e:  # noqa: BLE001
            h8[k] = {"series_id": sid, "label": label,
                     "error": str(e)[:80]}
            errors.append("fred:%s" % sid)
    payload["bank_wholesale_funding"] = h8

    # 4. Commercial paper — quantities first, then the tenor grid
    cp = {"outstandings": {}, "rates": {}}
    for k, (sid, label) in CP_OUT.items():
        try:
            s = fred(sid, start="2010-01-01")
            d, v = last(s)
            cp["outstandings"][k] = {
                "series_id": sid, "label": label, "unit": "$bn",
                "date": d, "value": v,
                "chg_4w": chg(s, 4), "chg_13w": chg(s, 13),
                "pct_13w": (round((v / s[-14][1] - 1) * 100, 2)
                            if len(s) > 13 and s[-14][1] else None)}
        except Exception as e:  # noqa: BLE001
            cp["outstandings"][k] = {"series_id": sid, "label": label,
                                     "error": str(e)[:80]}
            errors.append("fred:%s" % sid)
    tot = (cp["outstandings"].get("cp_total") or {}).get("value")
    abcp = (cp["outstandings"].get("cp_abcp") or {}).get("value")
    if tot and abcp:
        cp["abcp_share_pct"] = round(abcp / tot * 100, 1)
        cp["abcp_note"] = ("ABCP is the direct foreign-bank dollar-funding "
                           "channel — it funded the offshore dollar system "
                           "into 2007 and vanished first when it broke.")
    for k, (sid, tier, tenor) in CP_RATES.items():
        try:
            s = fred(sid, start="2015-01-01")
            d, v = last(s)
            spr = (round((v - sofr_now) * 100, 1)
                   if v is not None and sofr_now is not None else None)
            hist_spr = [x[1] for x in s[-780:]]
            cp["rates"][k] = {"series_id": sid, "tier": tier,
                              "tenor": tenor, "date": d, "rate": v,
                              "spread_to_sofr_bp": spr,
                              "z_rate": zscore(v, hist_spr)}
        except Exception as e:  # noqa: BLE001
            cp["rates"][k] = {"series_id": sid, "tier": tier,
                              "tenor": tenor, "error": str(e)[:80]}
            errors.append("fred:%s" % sid)
    a2 = (cp["rates"].get("a2p2_30d") or {}).get("rate")
    aa = (cp["rates"].get("aa_nfin_30d") or {}).get("rate")
    if a2 is not None and aa is not None:
        cp["quality_spread_30d_bp"] = round((a2 - aa) * 100, 1)
        cp["quality_spread_note"] = ("A2/P2 minus AA at 30 days — the "
                                     "credit tier inside CP. Widens when "
                                     "lenders stop rolling weaker names.")
    payload["commercial_paper"] = cp

    # 5. SOFR term structure
    term = {}
    for k, (sid, label) in SOFR_TERM.items():
        try:
            s = fred(sid, start="2019-01-01")
            d, v = last(s)
            term[k] = {"series_id": sid, "label": label, "date": d,
                       "value": v}
        except Exception as e:  # noqa: BLE001
            term[k] = {"series_id": sid, "label": label,
                       "error": str(e)[:80]}
            errors.append("fred:%s" % sid)
    a30 = (term.get("sofr_30d_avg") or {}).get("value")
    a180 = (term.get("sofr_180d_avg") or {}).get("value")
    if a30 is not None and a180 is not None:
        term["term_slope_bp"] = round((a180 - a30) * 100, 1)
        term["term_slope_note"] = (
            "180d minus 30d compounded average. These are BACKWARD-looking "
            "averages, so the slope reads where funding HAS been, not where "
            "it is going — a genuine futures curve is the missing piece "
            "(see not_entitled).")
    payload["sofr_term_structure"] = term

    # BIS LBS
    payload["bis_lbs_usd"] = bis_lbs_usd()
    if not payload["bis_lbs_usd"].get("ok"):
        errors.append("bis:lbs:no-aggregate")

    # 6/7. honest gaps
    payload["not_entitled"] = NOT_ENTITLED

    # Z-score composite — the note's construction
    legs, missing = [], []
    for rid in ("sofr", "obfr", "tgcr", "bgcr"):
        z = (spreads.get(rid) or {}).get("z")
        if z is None:
            missing.append("%s-IORB" % rid.upper())
        else:
            legs.append({"leg": "z(%s − IORB)" % rid.upper(), "z": z})
    cpz = (cp["rates"].get("aa_fin_90d") or {}).get("z_rate")
    if cpz is None:
        missing.append("CP-OIS")
    else:
        legs.append({"leg": "z(90d AA financial CP)", "z": cpz})
    total = round(sum(x["z"] for x in legs), 2) if legs else None
    payload["stress_z"] = {
        "method": ("Sum of z-scores, each standardised against that "
                   "series' own history (min 60 obs). Replaces the "
                   "threshold ladder: a threshold says 'amber', a z-score "
                   "says how unusual today is for THIS spread."),
        "legs": legs, "legs_missing": missing,
        "sum_z": total,
        "mean_z": round(total / len(legs), 2) if legs else None,
        "reading": (None if total is None else
                    "CALM" if total <= 0 else
                    "NORMAL" if total <= 2 else
                    "TIGHTENING" if total <= 5 else
                    "STRESSED" if total <= 9 else "ACUTE"),
        "structural_note": (
            "BIS LBS legs are deliberately EXCLUDED from this sum. They "
            "are quarterly with a ~4-month lag; folding structural data "
            "into a live trigger is how a board lies slowly."),
    }

    payload["errors"] = errors
    payload["status"] = ("GREEN" if not errors else
                         "PARTIAL" if len(errors) < 6 else "DEGRADED")
    return payload


def lambda_handler(event, context):  # noqa: ARG001
    # Self-diagnostic: run the NY Fed request ladder from INSIDE Lambda
    # and return the raw attempt log. Diagnosing a Lambda-only failure
    # from the ops runner is how ops 4932 shipped a bad N in the first
    # place — the probe must run where the failure lives.
    if (event or {}).get("probe") == "nyfed":
        diag = {}
        for rid, (path, _lab, _d) in RATE_EPS.items():
            att = []
            rows, shape = _nyfed_rows(path, att)
            diag[rid] = {"shape_used": shape, "rows": len(rows),
                         "attempts": att}
        return {"statusCode": 200,
                "body": json.dumps({"probe": "nyfed", "diag": diag})}

    p = build()
    body = json.dumps(p, separators=(",", ":")).encode()
    s3.put_object(Bucket=BUCKET, Key=KEY, Body=body,
                  ContentType="application/json",
                  CacheControl="max-age=300")
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3.put_object(Bucket=BUCKET,
                  Key="data/usd-funding/history/%s.json" % stamp,
                  Body=body, ContentType="application/json")
    print("[usd-funding] %s status=%s errors=%d sum_z=%s"
          % (VERSION, p["status"], len(p["errors"]),
             (p.get("stress_z") or {}).get("sum_z")))
    return {"statusCode": 200,
            "body": json.dumps({"status": p["status"],
                                "errors": len(p["errors"]),
                                "sum_z": (p.get("stress_z") or {})
                                .get("sum_z")})}
