"""
justhodl-inventory-drawdown  ·  v1.0  —  THE PRE-SHORTAGE TELL (days-of-inventory falling)
==========================================================================================
Falling days-of-inventory into RISING demand is the cupboard emptying before the
shortage. It is widely misregarded because most screens watch revenue and margins,
not the inventory-days derivative — yet a company drawing down stock while sales
accelerate is physically running out, and the re-rate comes when it must ration or
raise price. This is the Micron tell one layer earlier than spot/PPI.

Two layers:
  SECTOR (FRED, real + monthly) — inventories-to-sales ratios by sector. A FALLING
      I/S ratio (especially historically lean) = the whole sector drawing down.
  STOCK (FMP /stable, quarterly) — days-inventory-outstanding (DIO) trend per name.
      The boom setup = DIO falling AND revenue rising = demand outstripping supply.

  boom_score = sqrt(drawdown x demand) — both must be true.

OUTPUT: data/inventory-drawdown.json   SCHEDULE: weekly.   Research, not advice.
"""
import json, time, boto3, math, os, urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

S3 = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/inventory-drawdown.json"
VERSION = "1.0.0"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
FRED_KEY = os.environ.get("FRED_API_KEY", "")
FRED_BASE = "https://api.stlouisfed.org/fred"

# FRED inventory/sales ratio series  (series_id -> (sector label, theme ETF for scarcity wiring))
FRED_SECTORS = {
    "ISRATIO":      ("Total business", None),
    "MNFCTRIRSA":   ("Manufacturing", "XLI"),
    "RETAILIRSA":   ("Retail", "XRT"),
    "WHLSLRIRSA":   ("Wholesale", None),
    "AISRSA":       ("Autos", None),
    "MRTSIR441USS": ("Motor vehicle & parts", None),
    "MRTSIR444USS": ("Building materials", "XLB"),
    "MRTSIR452USS": ("General merchandise", "XRT"),
    "MRTSIR448USS": ("Clothing", None),
}


def _read(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))


def _get(url, timeout=20):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


# ── SECTOR LAYER ──────────────────────────────────────────────────────────────
def fred_series(sid, n=72):
    u = f"{FRED_BASE}/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={n}"
    j = _get(u)
    if not j:
        return []
    out = []
    for o in j.get("observations", []):
        try:
            out.append(float(o["value"]))
        except (ValueError, TypeError, KeyError):
            continue
    return out  # newest-first


def sector_drawdown():
    rows = []
    for sid, (label, etf) in FRED_SECTORS.items():
        s = fred_series(sid)            # newest-first
        if len(s) < 13:
            continue
        latest = s[0]
        chg3 = (latest / s[3] - 1) * 100 if len(s) > 3 and s[3] else None
        chg6 = (latest / s[6] - 1) * 100 if len(s) > 6 and s[6] else None
        chg12 = (latest / s[12] - 1) * 100 if len(s) > 12 and s[12] else None
        hist = s[:60]
        below = sum(1 for v in hist if v < latest)
        pctl = round(100.0 * below / len(hist), 1)        # low = historically lean
        # drawdown: falling ratio (negative chg) + historically lean (low percentile)
        draw_from_chg = clamp((-(chg6 if chg6 is not None else 0)) * 12.0)   # -1% 6m ≈ 12 pts
        lean = 100 - pctl
        score = round(clamp(0.60 * draw_from_chg + 0.40 * lean), 1)
        flag = ("DRAWING DOWN" if (chg6 or 0) < -0.5 else
                "BUILDING" if (chg6 or 0) > 0.5 else "STABLE")
        rows.append({"series": sid, "sector": label, "theme_etf": etf,
                     "latest_ratio": round(latest, 3), "chg_3m": round(chg3, 2) if chg3 is not None else None,
                     "chg_6m": round(chg6, 2) if chg6 is not None else None,
                     "chg_12m": round(chg12, 2) if chg12 is not None else None,
                     "percentile_5y": pctl, "drawdown_score": score, "flag": flag})
    rows.sort(key=lambda r: -r["drawdown_score"])
    return rows


# ── STOCK LAYER ───────────────────────────────────────────────────────────────
def dio_trend(tk):
    """One FMP call -> DIO trend + revenue-per-share YoY (demand proxy)."""
    km = _get(f"https://financialmodelingprep.com/stable/key-metrics?symbol={tk}&period=quarter&limit=8&apikey={FMP}")
    if not isinstance(km, list) or len(km) < 5:
        return None
    dio = [q.get("daysOfInventoryOutstanding") for q in km]
    rps = [q.get("revenuePerShare") for q in km]
    d0 = dio[0] if dio and isinstance(dio[0], (int, float)) else None
    d4 = dio[4] if len(dio) > 4 and isinstance(dio[4], (int, float)) else None
    if not d0 or not d4 or d0 <= 0 or d4 <= 0:
        return None                       # no meaningful inventory (software/services)
    dio_chg = (d0 / d4 - 1) * 100         # falling (negative) = drawdown
    rev_g = None
    if len(rps) > 4 and isinstance(rps[0], (int, float)) and isinstance(rps[4], (int, float)) and rps[4]:
        rev_g = (rps[0] / rps[4] - 1) * 100
    return {"dio_latest": round(d0, 1), "dio_4q_ago": round(d4, 1),
            "dio_chg_pct": round(dio_chg, 1), "rev_growth_yoy": round(rev_g, 1) if rev_g is not None else None}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    sectors = sector_drawdown()

    # universe: the supply/scarcity-relevant names already in the system
    bb = _read("data/bottleneck-boom.json") or {}
    cp = _read("data/chokepoint.json") or {}
    sr = _read("data/scarcity-radar.json") or {}
    rev_ctx, meta = {}, {}
    for r in (bb.get("ranks") or []):
        tk = r.get("ticker")
        if tk:
            rev_ctx[tk] = {"rev_growth_yoy": r.get("rev_growth_yoy"), "rev_accel_pp": r.get("rev_accel_pp")}
            meta[tk] = {"industry": r.get("industry"), "sector": r.get("sector")}
    uni = set(rev_ctx)
    for r in (cp.get("all_chokepoints") or []):
        tk = r.get("ticker")
        if tk:
            uni.add(tk); meta.setdefault(tk, {"industry": r.get("industry"), "sector": r.get("sector")})
    for r in (sr.get("stealth_shortage_board") or []):
        tk = r.get("ticker")
        if tk:
            uni.add(tk); meta.setdefault(tk, {"industry": r.get("industry"), "sector": None})
    uni = list(uni)[:130]

    results = {}
    with ThreadPoolExecutor(max_workers=10) as pool:
        futs = {pool.submit(dio_trend, tk): tk for tk in uni}
        for f in as_completed(futs):
            tk = futs[f]
            try:
                d = f.result()
            except Exception:
                d = None
            if d:
                results[tk] = d

    board = []
    for tk, d in results.items():
        # demand: prefer the system's computed rev figures, else FMP revenue-per-share YoY
        ctx = rev_ctx.get(tk) or {}
        rev_g = ctx.get("rev_growth_yoy")
        accel = ctx.get("rev_accel_pp") or 0
        if rev_g is None:
            rev_g = d.get("rev_growth_yoy")
        draw_score = clamp((-d["dio_chg_pct"]) * 2.0)               # -25% DIO ≈ 50 pts
        demand_score = clamp((rev_g or 0) * 1.5 + (accel or 0) * 2.0) if rev_g is not None else None
        boom = round(math.sqrt(max(0, draw_score) * max(0, demand_score)), 1) if demand_score is not None else None
        if d["dio_chg_pct"] <= -8 and (demand_score or 0) >= 35:
            cls = "BOOM_SETUP"            # emptying shelves into rising demand
        elif d["dio_chg_pct"] <= -8:
            cls = "DRAWDOWN"             # lean but demand flat/unknown
        elif d["dio_chg_pct"] >= 12:
            cls = "BUILDING"            # inventory piling up — the opposite tell
        else:
            cls = "NEUTRAL"
        m = meta.get(tk) or {}
        board.append({"ticker": tk, "classification": cls, "boom_score": boom,
                      "dio_latest": d["dio_latest"], "dio_4q_ago": d["dio_4q_ago"],
                      "dio_chg_pct": d["dio_chg_pct"], "draw_score": round(draw_score, 1),
                      "rev_growth_yoy": round(rev_g, 1) if rev_g is not None else None,
                      "demand_score": round(demand_score, 1) if demand_score is not None else None,
                      "industry": m.get("industry"), "sector": m.get("sector")})
    # rank: boom setups first (by boom_score), then pure drawdowns (by draw_score)
    board.sort(key=lambda r: (-(1 if r["classification"] == "BOOM_SETUP" else 0),
                              -((r["boom_score"] or 0)), -r["draw_score"]))
    boom_setups = [r for r in board if r["classification"] == "BOOM_SETUP"]

    out = {
        "engine": "inventory-drawdown", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "thesis": ("Days-of-inventory falling into rising demand = the cupboard emptying before the shortage. "
                   "The inventory-days derivative leads pricing power and is widely ignored because screens watch "
                   "revenue and margins, not the rate the shelves are clearing."),
        "method": "sector: FRED inventories/sales ratio (falling + historically lean = drawing down). "
                  "stock: DIO YoY change (FMP key-metrics) x revenue growth; boom = sqrt(drawdown x demand).",
        "sector_drawdown": sectors,
        "stock_drawdown_board": board[:40],
        "boom_setups": boom_setups[:20],
        "counts": {"sectors_drawing": sum(1 for s in sectors if s["flag"] == "DRAWING DOWN"),
                   "names_scanned": len(uni), "names_with_inventory": len(results),
                   "boom_setups": len(boom_setups),
                   "building_inventory": sum(1 for r in board if r["classification"] == "BUILDING")},
        "legend": {"BOOM_SETUP": "DIO falling >=8% YoY AND demand rising — pre-shortage",
                   "DRAWDOWN": "DIO falling but demand flat/unknown",
                   "BUILDING": "DIO rising — inventory piling up (bearish tell)"},
        "sources": ["FRED inventories-to-sales (sector)", "FMP key-metrics DIO (stock)",
                    "bottleneck-boom (demand)", "chokepoint", "scarcity-radar"],
        "disclaimer": "Synthesis of the platform's own data — research, not advice.",
    }

    # closed loop: grade the boom setups forward vs SPY
    try:
        nowt = datetime.now(timezone.utc)
        tbl = boto3.resource("dynamodb", "us-east-1").Table("justhodl-signals")
        logged = 0
        for r in boom_setups[:8]:
            tbl.put_item(Item={
                "signal_id": f"inventory-drawdown#{r['ticker']}#{nowt.date().isoformat()}",
                "signal_type": "inventory_drawdown", "predicted_direction": "UP",
                "signal_value": str(r["boom_score"]), "confidence": Decimal("0.55"),
                "measure_against": "ticker_vs_benchmark", "benchmark": "SPY",
                "check_windows": ["day_5", "day_21", "day_63"], "outcomes": {}, "accuracy_scores": {},
                "status": "pending", "logged_at": nowt.isoformat(), "logged_epoch": int(nowt.timestamp()),
                "horizon_days_primary": 63, "schema_version": "2", "ttl": int(nowt.timestamp()) + 150 * 86400,
                "metadata": {"engine": "inventory-drawdown", "v": VERSION, "dio_chg_pct": r["dio_chg_pct"],
                             "rev_growth_yoy": r["rev_growth_yoy"]},
                "rationale": f"{r['ticker']} DIO {r['dio_chg_pct']}% YoY into rev {r['rev_growth_yoy']}% — drawing down"})
            logged += 1
        out["signals_logged"] = logged
    except Exception as e:
        print(f"[loop] {str(e)[:80]}")

    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[inventory-drawdown] sectors_drawing={out['counts']['sectors_drawing']} "
          f"names_with_inv={len(results)} boom_setups={len(boom_setups)} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps(out["counts"])}
