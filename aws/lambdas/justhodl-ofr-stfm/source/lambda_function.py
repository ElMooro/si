"""justhodl-ofr-stfm — OFR SHORT-TERM FUNDING MONITOR extractor (ops 3302).

Khalid: financialresearch.gov carries primary-dealer data plus a lot more
that should feed the system. This engine pulls the STFM v1 API by whole
DATASET (one call each — proven shape from the tyld example:
  GET data.financialresearch.gov/v1/series/dataset?dataset=X
  -> {"timeseries": {MNEMONIC: {"timeseries": {"aggregation": [[date,v],..]}}}}
) and publishes:
  data/ofr-stfm.json            curated blocks + FULL mnemonic catalog so
                                any future engine can see what's available
  data/history/ofr-stfm.json    headline series history (capped)

DATASETS
  repo  — market-wide repo: DVP / GCF / Triparty volumes + rates (daily)
  mmf   — money market funds: net assets + holdings mix (monthly)
  nypd  — FR2004 mirror; we extract only the FAILS cross-check totals
          (positions come from our direct NY Fed engine justhodl-nyfed-pd)

CONSUMERS  primary-dealers.html (funding context strip), eurodollar/
           plumbing + liquidity engines (wiring queued), repo desk.
"""
import gzip
import json
import os
import re
import statistics
import time
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = "justhodl-dashboard-live"
OUT = "data/ofr-stfm.json"
HIST = "data/history/ofr-stfm.json"
BASE = "https://data.financialresearch.gov/v1"
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com",
      "Accept": "application/json"}
s3 = boto3.client("s3", region_name="us-east-1")


def _get(url, timeout=60):
    last = None
    for ua in (UA, {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
                    "JustHodl/1.0", "Accept": "application/json"}):
        try:
            req = urllib.request.Request(url, headers=ua)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
            if raw[:2] == b"\x1f\x8b":  # OFR gzips dataset responses
                raw = gzip.decompress(raw)
            return json.loads(raw)
        except Exception as e:
            last = e
            time.sleep(0.6)
    raise RuntimeError("%s -> %s" % (url.split("?")[-1],
                                     str(last)[:140]))


def _j(k, d=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception:
        return d


def _agg_of(node):
    """Find the aggregation list wherever OFR nests it."""
    if isinstance(node, list):
        return node
    if isinstance(node, dict):
        if "aggregation" in node:
            return node["aggregation"]
        for v in node.values():
            got = _agg_of(v)
            if got:
                return got
    return None


def _series_full(mnemonic):
    doc = _get("%s/series/full?mnemonic=%s" % (BASE, mnemonic), 45)
    rows = []
    for it in (_agg_of(doc) or []):
        try:
            if it[1] is not None:
                rows.append((str(it[0]), float(it[1])))
        except Exception:
            continue
    rows.sort()
    return rows


def _dataset(name):
    """-> {mnemonic: [(date, val), ...]} sorted, plus raw mnemonic list."""
    doc = _get("%s/series/dataset?dataset=%s" % (BASE, name), 90)
    ts = doc.get("timeseries") or {}
    out = {}
    for mn, node in ts.items():
        agg = _agg_of(node) or []
        rows = []
        for it in agg:
            try:
                d, v = it[0], it[1]
                if v is None:
                    continue
                rows.append((str(d), float(v)))
            except Exception:
                continue
        if rows:
            rows.sort()
            out[mn] = rows[-900:]
    print("[ofr] dataset %s: %d series" % (name, len(out)))
    return out


def _stat(rows, look=252):
    v = [x[1] for x in rows]
    d = [x[0] for x in rows]
    w = v[-look:]
    z = (round((v[-1] - statistics.mean(w)) / statistics.stdev(w), 2)
         if len(w) >= 30 and statistics.stdev(w) > 0 else None)
    return {"latest": round(v[-1], 3), "as_of": d[-1],
            "d1": round(v[-1] - v[-2], 3) if len(v) >= 2 else None,
            "d20": round(v[-1] - v[-21], 3) if len(v) >= 21 else None,
            "z_1y": z}


def lambda_handler(event=None, context=None):
    hist = _j(HIST, {}) or {}
    doc = {"engine": "justhodl-ofr-stfm", "version": "1.1.0",
           "generated_at": datetime.now(timezone.utc)
           .isoformat(timespec="seconds"),
           "source": "OFR Short-Term Funding Monitor v1 API "
                     "(data.financialresearch.gov) — repo market, money "
                     "market funds, FR2004 mirror. Values as published; "
                     "repo volumes USD millions unless OFR states "
                     "otherwise, rates in percent."}

    # ── REPO: DVP / GCF / TRI volumes + rates ──
    try:
        repo = _dataset("repo")
        venues = {}
        cur = {}
        for mn, rows in repo.items():
            m = re.match(r"^REPO-(DVP|GCF|TRI)_([A-Z0-9]+)_?([A-Z]*)-", mn)
            if not m:
                continue
            ven, meas = m.group(1), m.group(2)
            st = _stat(rows)
            cur[mn] = st
            slot = venues.setdefault(ven, {})
            if meas == "TV" and ("vol" not in slot
                                 or mn.endswith("_TOT-P")
                                 or "TOT" in mn):
                slot["vol_mn"] = st["latest"]
                slot["vol_z_1y"] = st["z_1y"]
                slot["vol_mnemonic"] = mn
                hist.setdefault(mn, {})
                for d2, v2 in rows[-500:]:
                    hist[mn][d2] = round(v2, 1)
            if meas == "AR" and "rate" not in slot:
                slot["rate_pct"] = st["latest"]
                slot["rate_mnemonic"] = mn
            if meas in ("P75", "AR75", "IQR") and "rate_p75_pct" not in slot:
                slot["rate_p75_pct"] = st["latest"]
        doc["repo"] = {"venues": venues,
                       "n_series": len(repo),
                       "series": {k: cur[k] for k in sorted(cur)[:60]},
                       "read": "Market-wide repo: DVP (bilateral cleared), "
                               "GCF (interdealer general collateral), "
                               "Triparty — the funding water level under "
                               "dealer balance sheets."}
        doc.setdefault("catalog", {})["repo"] = sorted(repo.keys())
    except Exception as e:
        print("[ofr] repo failed: %s" % str(e)[:160])
        doc["repo"] = {"error": str(e)[:160]}

    time.sleep(0.5)

    # ── MMF: money market funds ──
    try:
        mmf = _dataset("mmf")
        cur = {}
        for mn, rows in mmf.items():
            cur[mn] = _stat(rows, look=36)
        # v1.1: self-curating — group by the asset token in the mnemonic
        # grammar MMF-MMF_{ASSET}[_{DETAIL}...]-M and pick the broadest
        # series per family (prefers *TOT*, else shortest mnemonic).
        fam = {}
        for mn in mmf:
            m = re.match(r"^MMF-MMF_([A-Z0-9]+)", mn)
            fam.setdefault(m.group(1) if m else "OTHER", []).append(mn)
        LABELS = {"RP": "repo_holdings", "TS": "treasury_holdings",
                  "TB": "treasury_holdings", "A": "agency_holdings",
                  "AG": "agency_holdings", "CP": "cp_holdings",
                  "ABCP": "abcp_holdings", "CD": "cd_holdings",
                  "TD": "time_deposits", "TNA": "total_net_assets",
                  "NA": "total_net_assets", "TOT": "total_net_assets",
                  "VRDN": "vrdn_holdings", "FRN": "frn_holdings",
                  "US": "us_govt_holdings", "OTHR": "other_holdings"}
        families, picks = {}, {}
        for f, lst in sorted(fam.items()):
            tot = [x for x in lst if "TOT" in x.upper()]
            mn = min(tot or lst, key=len)
            families[f] = {"pick": mn, "n_members": len(lst),
                           "members": sorted(lst)[:12], **cur[mn]}
            picks[LABELS.get(f, f.lower())] = {"mnemonic": mn, **cur[mn]}
            hist.setdefault(mn, {})
            for d2, v2 in mmf[mn][-260:]:
                hist[mn][d2] = round(v2, 1)
        doc["mmf"] = {"picks": picks, "families": families,
                      "n_series": len(mmf),
                      "series": {k: cur[k] for k in sorted(cur)[:60]},
                      "read": "Money-market funds are the cash pool on the "
                              "other side of dealer repo — their asset mix "
                              "shows where short-term cash is hiding."}
        doc.setdefault("catalog", {})["mmf"] = sorted(mmf.keys())
    except Exception as e:
        print("[ofr] mmf failed: %s" % str(e)[:160])
        doc["mmf"] = {"error": str(e)[:160]}

    time.sleep(0.5)

    # ── NYPD: fails cross-check via direct mnemonics (small, reliable) ──
    fails_x = {}
    for slot, mn in (("ftd_tot", "NYPD-PD_AFtD_TOT-A"),
                     ("ftr_tot", "NYPD-PD_AFtR_TOT-A")):
        try:
            rows = _series_full(mn)
            if rows:
                fails_x[slot] = {"mnemonic": mn, **_stat(rows, look=104)}
                hist.setdefault(mn, {})
                for d2, v2 in rows[-300:]:
                    hist[mn][d2] = round(v2, 1)
        except Exception as e:
            print("[ofr] %s failed: %s" % (mn, str(e)[:140]))
            fails_x.setdefault("errors", []).append(
                "%s: %s" % (mn, str(e)[:120]))
        time.sleep(0.4)
    doc["nypd_fails_cross"] = fails_x or None

    ok_blocks = sum(1 for k in ("repo", "mmf") if isinstance(doc.get(k), dict)
                    and not doc[k].get("error"))
    doc["health"] = {"ok_blocks": ok_blocks,
                     "errors": {k: doc[k].get("error")
                                for k in ("repo", "mmf")
                                if isinstance(doc.get(k), dict)
                                and doc[k].get("error")}}

    s3.put_object(Bucket=BUCKET, Key=HIST,
                  Body=json.dumps(hist, separators=(",", ":")).encode(),
                  ContentType="application/json")
    s3.put_object(Bucket=BUCKET, Key=OUT,
                  Body=json.dumps(doc, separators=(",", ":")).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=1800")
    return {"ok": ok_blocks >= 1 or bool(fails_x.get("ftd_tot")),
            "health": doc["health"],
            "repo_series": (doc.get("repo") or {}).get("n_series"),
            "mmf_series": (doc.get("mmf") or {}).get("n_series"),
            "fails_cross": bool(doc.get("nypd_fails_cross"))}
