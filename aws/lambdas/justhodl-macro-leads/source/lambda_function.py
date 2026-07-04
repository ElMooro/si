"""
justhodl-macro-leads — the free MacroMicro-style leading indicators missing from
the fleet, pulled straight from their public origins (no MacroMicro paywall):

  1. Copper/Gold ratio  — leads US 10Y yield & the risk/growth regime (FMP metals,
     FRED copper fallback). Also reports the ratio's 1y z-score + level vs DGS10.
  2. Gold/Silver ratio  — risk-off / inflation-regime tell (FMP metals).
  3. Rate-cut diffusion — Net % of central banks whose last move was a cut
     (BIS CBPOL policy-rate dataset, ~38 CBs) → global monetary-regime breadth.
  4. Geopolitical Risk Index (GPR) — Caldara & Iacoviello (matteoiacoviello.com).
  5. US Heavy Truck Sales — FRED HTRUCKSSAAR, the >1y S&P-reversal lead.

Every block is independently try/except'd — partial data still ships. Writes
data/macro-leads.json. Real data only; a block that can't fetch reports null,
never fake.
"""
import os
import json
import time
import io
import statistics
import urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/macro-leads.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "")
FMP_KEY = os.environ.get("FMP_KEY", "") or os.environ.get("FMP_API_KEY", "")
UA = {"User-Agent": "Mozilla/5.0 (JustHodl macro-leads)"}


def _get(url, timeout=30, headers=None):
    req = urllib.request.Request(url, headers=headers or UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def fred_series(sid, obs=800):
    """Return [(date, value)] ascending for a FRED series."""
    url = ("https://api.stlouisfed.org/fred/series/observations"
           "?series_id=%s&api_key=%s&file_type=json&sort_order=asc&limit=%d" % (sid, FRED_KEY, obs))
    d = json.loads(_get(url))
    out = []
    for o in d.get("observations", []):
        v = o.get("value")
        if v not in (".", "", None):
            try:
                out.append((o["date"], float(v)))
            except Exception:
                pass
    return out


def fmp_hist(symbol, days=400):
    """FMP /stable historical daily close for a commodity/metal symbol."""
    url = "https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=%s&apikey=%s" % (symbol, FMP_KEY)
    d = json.loads(_get(url))
    rows = d if isinstance(d, list) else d.get("historical", [])
    out = []
    for r in rows[:days]:
        try:
            out.append((r.get("date"), float(r.get("close") or r.get("price"))))
        except Exception:
            pass
    out.sort()
    return out


def zscore(series_vals):
    if len(series_vals) < 30:
        return None
    window = series_vals[-252:] if len(series_vals) >= 252 else series_vals
    mu = statistics.mean(window)
    sd = statistics.pstdev(window) or 1e-9
    return round((series_vals[-1] - mu) / sd, 2)


def block_metal_ratios():
    """Copper/Gold + Gold/Silver from FMP metals (COMEX-linked)."""
    out = {}
    try:
        gold = fmp_hist("GCUSD")      # gold
        silver = fmp_hist("SIUSD")    # silver
        copper = fmp_hist("HGUSD")    # copper (per lb)
        gmap = dict(gold); smap = dict(silver); cmap = dict(copper)
        # align on common dates
        dates = sorted(set(gmap) & set(cmap))
        cg = [(d, cmap[d] / gmap[d]) for d in dates if gmap[d]]
        dates2 = sorted(set(gmap) & set(smap))
        gs = [(d, gmap[d] / smap[d]) for d in dates2 if smap[d]]
        if cg:
            vals = [v for _, v in cg]
            out["copper_gold"] = {"ratio": round(cg[-1][1], 6), "z_1y": zscore(vals),
                                  "asof": cg[-1][0], "n": len(cg),
                                  "note": "rising copper/gold = pro-cyclical, leads 10Y yields up"}
        if gs:
            vals = [v for _, v in gs]
            out["gold_silver"] = {"ratio": round(gs[-1][1], 3), "z_1y": zscore(vals),
                                  "asof": gs[-1][0], "n": len(gs),
                                  "note": "high gold/silver = risk-off / deflation lean"}
    except Exception as e:
        out["error"] = str(e)[:120]
    # FRED copper fallback for context if FMP metals empty
    if "copper_gold" not in out:
        try:
            cop = fred_series("PCOPPUSDM", 240)
            if cop:
                out["copper_fred_monthly_usd_mt"] = {"value": cop[-1][1], "asof": cop[-1][0]}
        except Exception:
            pass
    return out


def block_rate_cut_diffusion():
    """Net % of central banks whose last policy move was a cut, from BIS CBPOL."""
    try:
        # BIS SDMX v2 — monthly central-bank policy rates, all series, CSV
        url = "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_CBPOL/1.0/M..?format=csv&lastNObservations=18"
        raw = _get(url, timeout=45).decode("utf-8", "ignore")
        lines = [ln for ln in raw.splitlines() if ln.strip()]
        if len(lines) < 2:
            return {"error": "empty BIS response"}
        hdr = lines[0].split(",")
        # find the key column (series ref) and TIME/OBS
        try:
            i_ref = next(i for i, h in enumerate(hdr) if "REF_AREA" in h.upper())
        except StopIteration:
            i_ref = 1
        try:
            i_time = next(i for i, h in enumerate(hdr) if h.upper() in ("TIME_PERIOD", "TIME"))
            i_val = next(i for i, h in enumerate(hdr) if h.upper() in ("OBS_VALUE", "VALUE"))
        except StopIteration:
            return {"error": "BIS columns not found"}
        by_cb = {}
        for ln in lines[1:]:
            p = ln.split(",")
            if len(p) <= max(i_ref, i_time, i_val):
                continue
            cb = p[i_ref]
            try:
                v = float(p[i_val])
            except Exception:
                continue
            by_cb.setdefault(cb, []).append((p[i_time], v))
        cutting = hiking = held = 0
        for cb, obs in by_cb.items():
            obs.sort()
            if len(obs) < 2:
                continue
            last, prev = obs[-1][1], obs[-2][1]
            if last < prev - 0.001:
                cutting += 1
            elif last > prev + 0.001:
                hiking += 1
            else:
                held += 1
        n = cutting + hiking + held
        if not n:
            return {"error": "no CB moves parsed"}
        return {"n_central_banks": n, "cutting": cutting, "hiking": hiking, "holding": held,
                "net_pct_cutting": round(100.0 * (cutting - hiking) / n, 1),
                "regime": ("EASING" if cutting > hiking else "TIGHTENING" if hiking > cutting else "ON_HOLD"),
                "note": "net % of CBs whose last move was a cut minus hike — global monetary breadth"}
    except Exception as e:
        return {"error": str(e)[:150]}


def block_gpr():
    """Geopolitical Risk Index — Caldara & Iacoviello monthly."""
    try:
        raw = _get("https://www.matteoiacoviello.com/gpr_files/data_gpr_export.xls", timeout=45)
        try:
            import xlrd  # vendored in some builds
            wb = xlrd.open_workbook(file_contents=raw)
            sh = wb.sheet_0() if hasattr(wb, "sheet_0") else wb.sheet_by_index(0)
            hdr = [str(sh.cell_value(0, c)).strip().upper() for c in range(sh.ncols)]
            ci = hdr.index("GPR") if "GPR" in hdr else 1
            di = hdr.index("MONTH") if "MONTH" in hdr else 0
            rows = []
            for r in range(1, sh.nrows):
                try:
                    rows.append((sh.cell_value(r, di), float(sh.cell_value(r, ci))))
                except Exception:
                    pass
            if rows:
                vals = [v for _, v in rows]
                return {"gpr": round(rows[-1][1], 1), "z_5y": zscore(vals[-60:] + [vals[-1]]) if len(vals) > 60 else None,
                        "n": len(rows), "note": "Caldara-Iacoviello Geopolitical Risk Index"}
        except ImportError:
            return {"error": "xlrd not bundled — add to this engine's build to enable GPR"}
        return {"error": "GPR parse produced no rows"}
    except Exception as e:
        return {"error": str(e)[:150]}


def block_heavy_truck():
    """US Heavy Truck Sales — >1y lead on S&P reversals (FRED HTRUCKSSAAR)."""
    try:
        s = fred_series("HTRUCKSSAAR", 240)
        if not s:
            return {"error": "no data"}
        last = s[-1]
        yoy = None
        if len(s) >= 13:
            prev = s[-13][1]
            if prev:
                yoy = round(100.0 * (last[1] - prev) / prev, 1)
        vals = [v for _, v in s]
        return {"units_saar_thousands": round(last[1], 1), "asof": last[0], "yoy_pct": yoy,
                "z_1y": zscore(vals), "note": "peaks tend to lead S&P 500 downturns by 12-18 months"}
    except Exception as e:
        return {"error": str(e)[:120]}


def handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    out = {
        "generated_at": now.isoformat(),
        "source": "public origins (FRED / FMP / BIS / matteoiacoviello) — not MacroMicro API",
        "copper_gold_silver": block_metal_ratios(),
        "rate_cut_diffusion": block_rate_cut_diffusion(),
        "geopolitical_risk": block_gpr(),
        "heavy_truck_sales": block_heavy_truck(),
    }
    populated = [k for k in ("copper_gold_silver", "rate_cut_diffusion", "geopolitical_risk", "heavy_truck_sales")
                 if isinstance(out[k], dict) and not out[k].get("error") and out[k]]
    out["_populated"] = populated
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=3600")
    return {"ok": True, "populated": populated}


def lambda_handler(event=None, context=None):
    return handler(event, context)
