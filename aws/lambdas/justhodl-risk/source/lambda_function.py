"""justhodl-risk — one composite risk engine over every risk series the system banks.

Reads the long-history risk inputs that already live in the bucket (FRED-scoped
warm store, CDS-desk long sources — OFR FSI, EBP/GZ, ECB CISS/SovCISS — and the
risk-source engines' warm series: CMDI, EPU, WUI, FAO, FHFA, HHDC, ESMA, FDIC)
plus Coin Metrics community data for Bitcoin, and compiles two 0–100 composites
with point-in-time expanding percentiles (see risk_model.py):

  STRESS  — ten pillars (credit, volatility, funding, official stress indices,
            rates, sovereign, banking, uncertainty, equity, crypto)
  FROTH   — complacency / extension / late-cycle gauges

plus 3-year rolling-relative versions, breadth, a Bitcoin froth/stress pair, and
a retrospective scorecard against labelled equity and Bitcoin episodes
(1990 → today) with train (≤2012) / held-out (2013→) threshold statistics.

Everything is descriptive. decision.call None. Weights are frozen constants;
the engine never retunes itself at run time.

Owns data/risk.json + data/warm/risk/.  Page: /risk.html
"""
from __future__ import annotations

import csv
import datetime as dt
import gzip
import io
import json
import os
import time

import risk_model as RM
import risk_sources as RS

SLUG = "risk"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
PUBLIC = f"https://{BUCKET}.s3.amazonaws.com/"
FS = "data/warm/fred-scoped/"

# FRED series id -> fred-scoped folder (full-history JSON banked by justhodl-fred-catalog)
FRED_KEYS = {
    "VIXCLS": "Financial_Indicators", "VXVCLS": "Financial_Indicators", "VXNCLS": "Financial_Indicators",
    "OVXCLS": "Financial_Indicators", "GVZCLS": "Financial_Indicators", "RVXCLS": "Financial_Indicators",
    "STLFSI4": "Financial_Indicators", "NFCI": "Financial_Indicators", "ANFCI": "Financial_Indicators",
    "KCFSI": "Financial_Indicators", "SP500": "Financial_Indicators", "NASDAQCOM": "Financial_Indicators",
    "UMCSENT": "Financial_Indicators", "BAMLHYH0A0HYM2TRIV": "Financial_Indicators",
    "BAMLH0A0HYM2": "Interest_Rates", "BAMLC0A0CM": "Interest_Rates", "BAMLH0A3HYC": "Interest_Rates",
    "BAMLH0A1HYBB": "Interest_Rates", "BAMLEMCBPIOAS": "Interest_Rates", "BAA10Y": "Interest_Rates",
    "T10Y2Y": "Interest_Rates", "T10Y3M": "Interest_Rates", "SOFR": "Interest_Rates", "IORB": "Interest_Rates",
    "EFFR": "Interest_Rates", "DPCREDIT": "Interest_Rates", "T10YIE": "Interest_Rates", "DFII10": "Interest_Rates",
    "TEDRATE": "On_Demand", "DTWEXBGS": "Exchange_Rates", "RRPONTSYD": "Monetary_Data",
    "DRTSCILM": "Banking", "DRALACBS": "Banking", "DRCCLACBS": "Banking", "DRSFRMACBS": "Banking",
    "TOTBKCR": "Banking", "DCOILWTICO": "Prices",
}
CDS_LONG = {  # CDS-desk long-source mirrors (gzipped CSV)
    "ofr_fsi": "data/warm/cds/long-src/ofr_fsi.csv.gz",
    "ebp": "data/warm/cds/long-src/ebp.csv.gz",
    "ecb_ciss": "data/warm/cds/long-src/ecb_ciss.csv.gz",
    "ecb_sovciss": "data/warm/cds/long-src/ecb_sovciss.csv.gz",
    "ecb_sovciss_ea": "data/warm/cds/long-src/ecb_sovciss_ea.csv.gz",
}
WARM_SERIES = {  # risk-source engines -> raw key used by risk_model specs
    "cmdi_market": "data/warm/nyfed-cmdi/series/MARKET.json.gz",
    "epu_us30": "data/warm/epu/series/US_DAILY_30D.json.gz",
    "wui_global": "data/warm/world-uncertainty-index/series/GLOBAL_GLOBAL_GDP_WEIGHTED_AVERAGE.json.gz",
    "fao_ffpi": "data/warm/fao-food-price-index/series/FFPI.json.gz",
    "fhfa_us": "data/warm/fhfa-hpi/series/US_SA.json.gz",
    "hhdc_90": "data/warm/nyfed-hhdc/series/P11_90_DAYS_LATE.json.gz",
    "esma_downgrades": "data/warm/esma-ratings/series/N_DOWNGRADES_M.json.gz",
    "fdic_htm": "data/warm/fdic-bankfind/series/SYS_HTM_LOSS_TO_EQUITY.json.gz",
}
COINMETRICS_CSV = "https://raw.githubusercontent.com/coinmetrics/data/master/csv/btc.csv"
COINMETRICS_API = ("https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
                   "?assets=btc&metrics=PriceUSD,CapMVRVCur&frequency=1d&page_size=10000&start_time={start}")

# Existing risk engines rolled up on the board (slug, page, score path, label path)
BOARD = [
    ("systemic-stress", "/systemic-stress.html", "composite.score_0_100", "composite.regime"),
    ("regime-composite", "/regime.html", "composite_score", "meta_regime"),
    ("khalid-risk", "/khalidrisk.html", "risk_score", "status"),
    ("bank-stress", "/stress.html", "bank_stress_score", "regime"),
    ("canary-grid", "/canaries.html", "early_warning_level", None),
    ("crisis-canaries", "/canaries.html", "composite_score", "level"),
    ("ciss-stress", "/systemic-stress.html", "ea_composite", "ea_regime"),
    ("eurodollar-stress", "/stress.html", "composite_stress_score", "regime"),
    ("vol-regime", "/vol-regime.html", "composite_score", "composite_regime"),
    ("crypto-cycle-risk", "/crypto-risk.html", "dump_risk_score", "risk_level"),
    ("tail-risk", "/tail-risk.html", "score", "tail_regime"),
    ("risk-regime", "/risk-regime.html", "risk_regime_score", "risk_regime"),
    ("crisis-composite", "/defcon.html", "composite_score", "defcon_name"),
    ("global-stress", "/global-stress.html", "global_stress_index", "global_stress_level"),
    ("credit-stress", "/stress.html", None, "composite_regime"),
    ("crisis-plumbing", "/crisis.html", "composite.composite_stress_score", "composite.agreement_signal"),
    ("sovereign-stress", "/sovereign-stress.html", "country_stress_scores", None),
    ("blackswan-watch", "/blackswan-watch.html", "n_with_history", None),
    ("cds-desk", "/cds.html", "history.cdx_ig_vs_2006.pct_rank_since_2006", None),
]


# ──────────────────────────────────────────────────────────────── store ──
class Store:
    """Bucket reader: boto3 inside Lambda, public HTTPS for local dry runs."""

    def __init__(self, http=False):
        self.http = http
        self.s3 = None
        if not http:
            try:
                import boto3
                self.s3 = boto3.client("s3", region_name="us-east-1")
            except Exception:  # noqa: BLE001
                self.http = True
        self.status = []

    def get_bytes(self, key):
        try:
            if self.s3 is not None:
                body = self.s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            else:
                body = RS.http_get(PUBLIC + key, timeout=60, retries=1)
            if key.endswith(".gz") and body[:2] == b"\x1f\x8b":
                body = gzip.decompress(body)
            self.status.append({"key": key, "ok": True, "bytes": len(body)})
            return body
        except Exception as exc:  # noqa: BLE001
            self.status.append({"key": key, "ok": False, "error": type(exc).__name__})
            return None

    def get_json(self, key):
        b = self.get_bytes(key)
        if b is None:
            return None
        try:
            return json.loads(b)
        except ValueError:
            return None


def load_raw(store):
    raw = {}
    for sid, folder in FRED_KEYS.items():
        obj = store.get_json(f"{FS}{folder}/{sid}.json")
        raw[sid] = RM.fred_points(obj) if obj else []
    b = store.get_bytes(CDS_LONG["ofr_fsi"])
    if b:
        raw.update(RM.parse_fsi_csv(b.decode("utf-8", "replace")))
    b = store.get_bytes(CDS_LONG["ebp"])
    if b:
        raw.update(RM.parse_ebp_csv(b.decode("utf-8", "replace")))
    b = store.get_bytes(CDS_LONG["ecb_ciss"])
    if b:
        raw.update(RM.parse_ecb_csv(b.decode("utf-8", "replace"), "ciss_"))
    for k in ("ecb_sovciss", "ecb_sovciss_ea"):
        b = store.get_bytes(CDS_LONG[k])
        if b:
            raw.update(RM.parse_ecb_csv(b.decode("utf-8", "replace"), "sovciss_"))
    for name, key in WARM_SERIES.items():
        obj = store.get_json(key)
        raw[name] = RM.warm_points(obj) if obj else []
    raw.update(load_btc(store))
    return RM.derive_inputs(raw)


def load_btc(store):
    """Coin Metrics: full history CSV (GitHub mirror) + community API for the recent tail."""
    out = {"btc_price": [], "btc_mvrv": []}
    try:
        txt = RS.http_text(COINMETRICS_CSV, timeout=120, retries=1)
        out = RM.parse_coinmetrics_csv(txt)
        store.status.append({"key": COINMETRICS_CSV, "ok": True, "bytes": len(txt)})
    except Exception as exc:  # noqa: BLE001
        store.status.append({"key": COINMETRICS_CSV, "ok": False, "error": type(exc).__name__})
        cached = store.get_json("data/warm/risk/src/btc_coinmetrics.json")
        if cached:
            out = {"btc_price": RM.clean(cached.get("btc_price")), "btc_mvrv": RM.clean(cached.get("btc_mvrv"))}
    last = out["btc_price"][-1][0] if out["btc_price"] else "2010-07-18"
    start = (RM.to_date(last) - dt.timedelta(days=3)).isoformat()
    try:
        js = RS.http_json(COINMETRICS_API.format(start=start), timeout=60, retries=1)
        price = dict(out["btc_price"]); mvrv = dict(out["btc_mvrv"])
        for row in js.get("data", []):
            d = str(row.get("time", ""))[:10]
            if RM.fnum(row.get("PriceUSD")) is not None:
                price[d] = float(row["PriceUSD"])
            if RM.fnum(row.get("CapMVRVCur")) is not None:
                mvrv[d] = float(row["CapMVRVCur"])
        out = {"btc_price": sorted(price.items()), "btc_mvrv": sorted(mvrv.items())}
        store.status.append({"key": "coinmetrics community api", "ok": True, "bytes": len(js.get("data", []))})
    except Exception as exc:  # noqa: BLE001
        store.status.append({"key": "coinmetrics community api", "ok": False, "error": type(exc).__name__})
    out["btc_price"] = [list(p) for p in out["btc_price"]]
    out["btc_mvrv"] = [list(p) for p in out["btc_mvrv"]]
    return out


def _path(obj, path):
    cur = obj
    for part in (path or "").split("."):
        if not part:
            return None
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def system_board(store):
    rows = []
    for slug, page, score_path, label_path in BOARD:
        pk = store.get_json(f"data/{slug}.json")
        if not pk:
            rows.append([slug, page, None, "packet unavailable", None])
            continue
        score = _path(pk, score_path) if score_path else None
        if isinstance(score, dict):
            vals = [v for v in score.values() if isinstance(v, (int, float))]
            score = max(vals) if vals else None
        label = _path(pk, label_path) if label_path else None
        if score is None and not label:
            label = "research-only · no qualified score published"
        elif score is None:
            label = f"{label} · no numeric score"
        gen = pk.get("generated_at") or pk.get("as_of") or pk.get("timestamp")
        rows.append([slug, page, None if score is None else round(float(score), 1), str(label)[:80], str(gen)[:19] if gen else None])
    return rows


# ─────────────────────────────────────────────────────────────── labels ──
def stress_zone(v):
    if v is None:
        return "n/a", "mute"
    if v >= RM.STRESS_EXTREME:
        return "extreme stress zone", "neg"
    if v >= RM.STRESS_ALERT:
        return "high stress", "warning"
    if v >= 45:
        return "elevated", "gold"
    return "calm", "pos"


def froth_zone(v):
    if v is None:
        return "n/a", "mute"
    if v >= RM.FROTH_ALERT:
        return "frothy (top-5 % of history)", "neg"
    if v >= 60:
        return "warm", "warning"
    if v >= 45:
        return "neutral", "info"
    return "cool", "pos"


def _last(series, calendar, back=0):
    i = len(series) - 1 - back
    while i >= 0 and series[i] is None:
        i -= 1
    return (series[i], calendar[i]) if i >= 0 else (None, None)


def _series_points(series, calendar):
    return [[calendar[i], round(v, 2)] for i, v in enumerate(series) if v is not None]


# ──────────────────────────────────────────────────────────────── build ──
def build(event):
    t0 = time.time()
    store = Store(http=bool(event.get("http")))
    raw = load_raw(store)
    end = max([p[-1][0] for p in raw.values() if p] + [dt.date.today().isoformat()])
    cal = RM.business_days(RM.CAL_START, end)

    sc = RM.build_components(raw, RM.STRESS_COMPONENTS, cal)
    fc = RM.build_components(raw, RM.FROTH_COMPONENTS, cal)
    sp, scount = RM.pillar_scores(sc, cal, RM.PILLAR_LABELS)
    fp, fcount = RM.pillar_scores(fc, cal, RM.FROTH_LABELS)
    stress = RM.composite(sp, RM.STRESS_WEIGHTS, cal)
    froth = RM.composite(fp, RM.FROTH_WEIGHTS, cal, min_pillars=2)
    stress_rel = RM.rolling_pct(stress)
    froth_rel = RM.rolling_pct(froth)
    brd = RM.breadth(sc, cal)
    btc_froth = RM.sub_composite(fc, RM.BTC_FROTH_IDS, cal)
    btc_stress = sp["crypto"]

    flags = RM.in_windows(cal, [(e[0], e[1]) for e in RM.EPISODES])
    train = (cal[0], RM.TRAIN_END)
    test = ("2013-01-01", cal[-1])
    thr_train_best, thr_train_rows = RM.best_threshold(stress, flags, cal, *train)
    thr_test_best, thr_test_rows = RM.best_threshold(stress, flags, cal, *test)
    sep_train = RM.separation(stress, flags, cal, *train)
    sep_test = RM.separation(stress, flags, cal, *test)
    eq_rows = RM.episode_scorecard(stress, froth, cal, RM.EPISODES, stress_rel, froth_rel)
    btc_rows = RM.episode_scorecard(btc_stress, btc_froth, cal, RM.BTC_EPISODES,
                                    RM.rolling_pct(btc_stress), RM.rolling_pct(btc_froth), lead_days=180,
                                    alert=RM.STRESS_ALERT, froth_alert=RM.FROTH_ALERT)
    fa_froth = RM.false_alarms(froth, flags, cal, RM.FROTH_ALERT, 365, cal[0], cal[-1])
    fa_froth_rel = RM.false_alarms(froth_rel, flags, cal, RM.REL_ZONE, 365, cal[0], cal[-1])
    fa_stress = RM.false_alarms(stress, flags, cal, RM.STRESS_ALERT, 0, cal[0], cal[-1])

    # ── packet ──
    pk = RS.Packet(
        SLUG, "Risk", "JustHodl composite risk — stress & froth across the whole system",
        "One engine over every long-history risk series the system banks: credit spreads, volatility, funding, "
        "official stress indices (OFR, St. Louis Fed, Chicago Fed, Kansas City Fed, ECB CISS), rates, sovereign, "
        "banking and household, uncertainty, equity and Bitcoin. Each gauge is ranked against its own history up to "
        "that day (no look-ahead), pillars average their gauges, and two weighted composites — STRESS and FROTH — are "
        "measured against every labelled equity and Bitcoin episode since 1990. Weights and thresholds were chosen on "
        "1990–2012 and are reported separately for 2013 onward. Descriptive; nothing here is a call.",
        {"provider": "JustHodl composite over in-system sources (FRED, OFR, Federal Reserve, ECB, NY Fed, EPU, WUI, FAO, "
                     "FHFA, ESMA, FDIC, Coin Metrics community data)",
         "url": "https://justhodl.ai/risk.html", "docs": "https://justhodl.ai/data.html",
         "license": "Underlying sources: public / CC BY (Coin Metrics community data: CC BY-NC 4.0)",
         "cadence": "daily, after the source engines"},
        cadence="daily")
    pk.hot_tail = 300

    s_now, s_date = _last(stress, cal)
    s_1m, _ = _last(stress, cal, 21)
    sr_now, _ = _last(stress_rel, cal)
    f_now, f_date = _last(froth, cal)
    f_1m, _ = _last(froth, cal, 21)
    fr_now, _ = _last(froth_rel, cal)
    b_now, _ = _last(brd, cal)
    bf_now, _ = _last(btc_froth, cal)
    bs_now, _ = _last(btc_stress, cal)
    s_hist = sorted(v for v in stress if v is not None)
    f_hist = sorted(v for v in froth if v is not None)
    s_pct_all = RS.pct_rank(s_hist, s_now)
    f_pct_all = RS.pct_rank(f_hist, f_now)
    sz, stone = stress_zone(s_now)
    fz, ftone = froth_zone(f_now)
    live_s = sum(1 for c in sc.values() if c["pct_aligned"][-1] is not None)
    live_f = sum(1 for c in fc.values() if c["pct_aligned"][-1] is not None)

    pk.kpi("Stress composite", "—" if s_now is None else f"{s_now:.1f}",
           f"{sz} · {RS.ordinal(round(s_pct_all)) if s_pct_all is not None else '—'} pct of 1990→ · 1m {('%+.1f' % (s_now - s_1m)) if (s_now is not None and s_1m is not None) else '—'}", stone)
    pk.kpi("Stress vs last 3 years", "—" if sr_now is None else f"{RS.ordinal(sr_now)} pct",
           "≥90 = stress zone (where troughs clustered)", "neg" if (sr_now or 0) >= RM.REL_ZONE else "info")
    pk.kpi("Froth composite", "—" if f_now is None else f"{f_now:.1f}",
           f"{fz} · {RS.ordinal(round(f_pct_all)) if f_pct_all is not None else '—'} pct of 1990→ · 1m {('%+.1f' % (f_now - f_1m)) if (f_now is not None and f_1m is not None) else '—'}", ftone)
    pk.kpi("Froth vs last 3 years", "—" if fr_now is None else f"{RS.ordinal(fr_now)} pct",
           "≥90 = froth zone (where peaks clustered)", "neg" if (fr_now or 0) >= RM.REL_ZONE else "info")
    pk.kpi("Breadth", "—" if b_now is None else f"{b_now:.0f}%",
           f"of {live_s} live stress gauges above their 80th percentile", "warning" if (b_now or 0) >= 40 else "info")
    pk.kpi("Bitcoin froth / stress", f"{'—' if bf_now is None else '%.0f' % bf_now} / {'—' if bs_now is None else '%.0f' % bs_now}",
           "MVRV, 200d trend, 1y return / drawdown, vol, trend, MVRV-low", "gold")
    top_p = max(((p, v[-1]) for p, v in sp.items() if v[-1] is not None), key=lambda x: x[1], default=(None, None))
    pk.kpi("Hottest pillar", RM.PILLAR_LABELS.get(top_p[0], "—") if top_p[0] else "—",
           f"{top_p[1]:.0f}/100" if top_p[1] is not None else "", "warning")
    pk.kpi("Gauges live", f"{live_s + live_f} / {len(sc) + len(fc)}",
           f"{live_s} stress · {live_f} froth · as of {s_date}", "info")

    # ── series ──
    pk.add_series("STRESS", "Stress composite (0–100)", _series_points(stress, cal), unit="composite percentile", freq="D", group="composite")
    pk.add_series("STRESS_REL3Y", "Stress — percentile within trailing 3 years", _series_points(stress_rel, cal), unit="pct", freq="D", group="composite")
    pk.add_series("FROTH", "Froth composite (0–100)", _series_points(froth, cal), unit="composite percentile", freq="D", group="composite")
    pk.add_series("FROTH_REL3Y", "Froth — percentile within trailing 3 years", _series_points(froth_rel, cal), unit="pct", freq="D", group="composite")
    pk.add_series("BREADTH80", "Share of stress gauges above their 80th percentile", _series_points(brd, cal), unit="%", freq="D", group="composite")
    pk.add_series("BTC_FROTH", "Bitcoin froth (MVRV, 200d trend, 1y return)", _series_points(btc_froth, cal), unit="pct", freq="D", group="bitcoin")
    pk.add_series("BTC_STRESS", "Bitcoin stress (crypto pillar)", _series_points(btc_stress, cal), unit="pct", freq="D", group="bitcoin")
    for p, label in RM.PILLAR_LABELS.items():
        pk.add_series(f"P_{p.upper()}", f"Pillar · {label}", _series_points(sp[p], cal), unit="pct", freq="D", group="pillar",
                      weight=RM.STRESS_WEIGHTS[p], n_components_now=scount[p][-1])
    for p, label in RM.FROTH_LABELS.items():
        pk.add_series(f"F_{p.upper()}", f"Froth pillar · {label}", _series_points(fp[p], cal), unit="pct", freq="D", group="froth-pillar",
                      weight=RM.FROTH_WEIGHTS[p], n_components_now=fcount[p][-1])
    for c in sc.values():
        if c["pct_series"]:
            pk.add_series(f"C_{c['id'].upper()}", f"{c['label']} · percentile", c["pct_series"], unit="pct", freq=c["freq"],
                          group="stress:" + c["pillar"], source_id=c["source"], transform=c["transform"], raw_last=c["raw_last"])
    for c in fc.values():
        if c["pct_series"]:
            pk.add_series(f"X_{c['id'].upper()}", f"{c['label']} · percentile", c["pct_series"], unit="pct", freq=c["freq"],
                          group="froth:" + c["pillar"], source_id=c["source"], transform=c["transform"], raw_last=c["raw_last"])

    # ── tables ──
    def _at(series, back):
        v, _ = _last(series, cal, back)
        return None if v is None else round(v, 1)
    pk.add_table("pillars", ["pillar", "label", "weight", "score", "1m ago", "3m ago", "gauges live", "hottest gauge", "hottest pct"],
                 [[p, RM.PILLAR_LABELS[p], RM.STRESS_WEIGHTS[p], _at(sp[p], 0), _at(sp[p], 21), _at(sp[p], 63), scount[p][-1]]
                  + (lambda m: [m["label"], round(m["pct_aligned"][-1], 1)] if m else [None, None])(
                      max([c for c in sc.values() if c["pillar"] == p and c["pct_aligned"][-1] is not None],
                          key=lambda c: c["pct_aligned"][-1], default=None))
                  for p in RM.PILLAR_LABELS],
                 note="Stress pillars: mean of member percentiles; composite = weight-averaged pillars (weights frozen from the 1990–2012 fit, "
                      "equity 1.0 and crypto 0.5 held fixed to avoid fitting the labels to themselves).", title="Stress pillars")
    pk.add_table("froth_pillars", ["pillar", "label", "weight", "score", "1m ago", "3m ago", "gauges live"],
                 [[p, RM.FROTH_LABELS[p], RM.FROTH_WEIGHTS[p], _at(fp[p], 0), _at(fp[p], 21), _at(fp[p], 63), fcount[p][-1]] for p in RM.FROTH_LABELS],
                 title="Froth pillars")
    comp_rows = []
    for c in list(sc.values()) + list(fc.values()):
        kind = "stress" if c["id"] in sc else "froth"
        comp_rows.append([kind, c["pillar"], c["id"], c["label"], None if c["raw_last"] is None else round(c["raw_last"], 3),
                          None if c["pct_aligned"][-1] is None else round(c["pct_aligned"][-1], 1),
                          None if c["pct_series"] is None or not c["pct_series"] else _at(c["pct_aligned"], 21),
                          c["last"], "live" if c["pct_aligned"][-1] is not None else ("stale" if c["last"] else "missing"),
                          c["source"], c["transform"], c["n_obs"]])
    pk.add_table("components", ["set", "pillar", "id", "gauge", "latest value", "percentile now", "percentile 1m ago", "as of", "status", "source id", "transform", "n obs"],
                 comp_rows, note="Percentile = rank of today's transformed value against that gauge's own history up to today. 'stale' = last "
                                 "observation older than the gauge's freshness window, so it drops out of its pillar until it updates.",
                 title="All gauges")
    sc_cols = ["episode", "peak", "trough", "froth_max_pre_peak", "froth_rel_max_pre_peak", "froth_zone_first", "froth_zone_lead_days",
               "stress_at_peak", "stress_alert_first", "stress_alert_lag_days", "stress_max", "stress_max_date", "stress_rel_max",
               "stress_at_trough", "stress_peak_minus_trough_days", "stress_alert_hit", "stress_zone_hit", "froth_zone_hit"]
    pk.add_table("scorecard_equity", sc_cols, [[r.get(k) for k in sc_cols] for r in eq_rows],
                 note="Retrospective behaviour around labelled equity episodes (peak → trough). froth_* measured in the 365 days before the peak; "
                      "stress_* inside the window. 'stress_peak_minus_trough_days' = days between the stress maximum (±60d) and the actual trough "
                      "(0 = same day). Current-vintage data: revised series can differ from what was visible at the time.",
                 title="Scorecard · equity episodes 1990→")
    pk.add_table("scorecard_bitcoin", sc_cols, [[r.get(k) for k in sc_cols] for r in btc_rows],
                 note="Bitcoin froth (MVRV, 200d trend, 1y return) in the 180 days before each cycle peak; Bitcoin stress (crypto pillar) into each trough.",
                 title="Scorecard · Bitcoin cycles")
    thr_cols = ["window", "threshold", "precision %", "recall %", "F1", "alert days", "episode days"]
    pk.add_table("thresholds", thr_cols,
                 [["train 1990–2012", r["thr"], r["precision"], r["recall"], r["f1"], r["alert_days"], r["episode_days"]] for r in thr_train_rows]
                 + [["held-out 2013→", r["thr"], r["precision"], r["recall"], r["f1"], r["alert_days"], r["episode_days"]] for r in thr_test_rows],
                 note=f"Day-level classification of 'inside a labelled episode' by STRESS ≥ threshold. Frozen alert {RM.STRESS_ALERT:.0f} is the train "
                      f"F1 maximiser; separation (mean inside − mean outside) {sep_train:.1f} train vs {sep_test:.1f} held-out. Post-2012 episodes were "
                      "shallower, which is why the 3-year relative reading (STRESS_REL3Y) is published alongside the level.",
                 title="Threshold statistics · train vs held-out")
    drivers = sorted([c for c in sc.values() if c["pct_aligned"][-1] is not None], key=lambda c: -c["pct_aligned"][-1])[:12]
    movers = sorted([c for c in sc.values() if c["pct_aligned"][-1] is not None and _at(c["pct_aligned"], 21) is not None],
                    key=lambda c: -(c["pct_aligned"][-1] - _at(c["pct_aligned"], 21)))[:8]
    pk.add_table("drivers", ["gauge", "pillar", "percentile now", "1m change", "latest value", "as of"],
                 [[c["label"], c["pillar"], round(c["pct_aligned"][-1], 1), round(c["pct_aligned"][-1] - (_at(c["pct_aligned"], 21) or c["pct_aligned"][-1]), 1),
                   None if c["raw_last"] is None else round(c["raw_last"], 3), c["last"]] for c in drivers]
                 + [[c["label"] + " ↑", c["pillar"], round(c["pct_aligned"][-1], 1), round(c["pct_aligned"][-1] - _at(c["pct_aligned"], 21), 1),
                     None if c["raw_last"] is None else round(c["raw_last"], 3), c["last"]] for c in movers],
                 title="Hottest gauges and biggest 1-month risers")
    pk.add_table("system_board", ["engine", "page", "score", "label", "generated"], system_board(store),
                 note="Current readings published by the system's other risk engines (their own scales). Engines that publish no qualified "
                      "score under the research-only doctrine are listed as such; they are not blended into STRESS/FROTH.",
                 title="System risk engines · board")
    pk.add_table("episodes_vs_today", ["episode", "stress at trough", "stress today", "froth max before peak", "froth today"],
                 [[r["episode"], r["stress_at_trough"], None if s_now is None else round(s_now, 1), r["froth_max_pre_peak"], None if f_now is None else round(f_now, 1)]
                  for r in eq_rows], title="Today against each episode")
    pk.add_table("inputs", ["key", "ok", "bytes / rows", "error"],
                 [[s["key"], s["ok"], s.get("bytes"), s.get("error")] for s in store.status], title="Input reads")

    # ── notes ──
    summ_eq, summ_btc = RM.scorecard_summary(eq_rows), RM.scorecard_summary(btc_rows)
    pk.note(f"Method: {len(sc)} stress gauges in {len(RM.PILLAR_LABELS)} pillars and {len(fc)} froth gauges in {len(RM.FROTH_LABELS)} pillars. "
            "Every gauge is transformed to a risk direction (level, inverted level, 63d/1y change, drawdown, realised vol, distance from 200d average) "
            "and ranked against its own history up to each day — point-in-time percentiles, no look-ahead. Pillars average their live gauges; "
            "composites are weighted pillar means.")
    pk.note(f"Scorecard (equity, {summ_eq.get('episodes')} episodes): stress ≥{RM.STRESS_ALERT:.0f} reached inside {summ_eq.get('stress_alert_hit')} windows, "
            f"≥{RM.STRESS_EXTREME:.0f} in {summ_eq.get('stress_extreme_hit')}, 3-year relative ≥{RM.REL_ZONE:.0f} in {summ_eq.get('stress_zone_hit')}; the stress maximum sat a median "
            f"{summ_eq.get('median_abs_days_stress_peak_vs_trough')} days from the actual trough — a coincident gauge of capitulation, not a lead. "
            f"Froth 3-year relative ≥{RM.REL_ZONE:.0f} appeared in the year before {summ_eq.get('froth_zone_hit')} of the peaks (median lead "
            f"{summ_eq.get('median_froth_zone_lead_days')} days) and missed credit-led or exogenous tops (2007, COVID, SVB).")
    pk.note(f"False alarms: froth ≥{RM.FROTH_ALERT:.0f} on {fa_froth['alert_days']} days, {fa_froth['false_alarm_pct']}% of them not followed by a labelled episode "
            f"within a year (3-year relative ≥{RM.REL_ZONE:.0f}: {fa_froth_rel['false_alarm_pct']}%); stress ≥{RM.STRESS_ALERT:.0f} on {fa_stress['alert_days']} days, "
            f"{fa_stress['false_alarm_pct']}% outside any labelled window. Complacency can persist for years (2004–06); treat froth as a conditions gauge.")
    pk.note(f"Bitcoin ({summ_btc.get('episodes')} cycles): Bitcoin froth ≥{RM.FROTH_ALERT:.0f} before {summ_btc.get('froth_alert_hit')} peaks; crypto-pillar stress ≥{RM.STRESS_ALERT:.0f} "
            f"into {summ_btc.get('stress_alert_hit')} troughs (≥{RM.STRESS_EXTREME:.0f} in {summ_btc.get('stress_extreme_hit')}). MVRV-based froth was muted at the Nov-2021 double top.")
    pk.note("Caveats: current-vintage history (revisions), gauges enter when their history begins (crypto 2013→, CISS 2000→, sovereign CDS/ESMA later), "
            "TED ended in 2022 and SOFR–IORB takes over, Coin Metrics GitHub data lags and is topped up from the community API. Weights were fit on "
            "1990–2012 only; held-out statistics are published, not hidden. Descriptive engine — decision.call null, sizing_eligible false.")

    pk.extra = {
        "model": {"version": RM.MODEL_VERSION, "calendar_start": RM.CAL_START, "train_end": RM.TRAIN_END,
                  "stress_weights": RM.STRESS_WEIGHTS, "froth_weights": RM.FROTH_WEIGHTS,
                  "thresholds": {"stress_alert": RM.STRESS_ALERT, "stress_extreme": RM.STRESS_EXTREME,
                                 "froth_alert": RM.FROTH_ALERT, "rel_zone": RM.REL_ZONE, "rel_window_days": RM.REL_WINDOW},
                  "min_history": RM.MIN_HISTORY, "episodes": RM.EPISODES, "btc_episodes": RM.BTC_EPISODES,
                  "separation": {"train": None if sep_train is None else round(sep_train, 2), "test": None if sep_test is None else round(sep_test, 2)},
                  "best_threshold": {"train": thr_train_best, "test": thr_test_best},
                  "false_alarms": {"froth": fa_froth, "froth_rel": fa_froth_rel, "stress": fa_stress},
                  "scorecard_summary": {"equity": summ_eq, "bitcoin": summ_btc}},
        "latest": {"date": s_date, "stress": None if s_now is None else round(s_now, 1), "stress_zone": sz,
                   "stress_pct_all": s_pct_all, "stress_rel3y": None if sr_now is None else round(sr_now, 1),
                   "froth": None if f_now is None else round(f_now, 1), "froth_zone": fz, "froth_pct_all": f_pct_all,
                   "froth_rel3y": None if fr_now is None else round(fr_now, 1), "breadth80": None if b_now is None else round(b_now, 1),
                   "btc_froth": None if bf_now is None else round(bf_now, 1), "btc_stress": None if bs_now is None else round(bs_now, 1),
                   "pillars": {p: (None if sp[p][-1] is None else round(sp[p][-1], 1)) for p in RM.PILLAR_LABELS},
                   "froth_pillars": {p: (None if fp[p][-1] is None else round(fp[p][-1], 1)) for p in RM.FROTH_LABELS},
                   "gauges_live": live_s + live_f, "gauges_total": len(sc) + len(fc)},
        "scorecard": {"equity": eq_rows, "bitcoin": btc_rows},
        "inputs_ok": sum(1 for s in store.status if s["ok"]), "inputs_failed": sum(1 for s in store.status if not s["ok"]),
        "build_seconds": round(time.time() - t0, 1),
    }
    # mirror the Bitcoin inputs so a GitHub outage does not blank the crypto pillar next run
    if raw.get("btc_price"):
        pk.raw("btc_coinmetrics.json", json.dumps({"btc_price": raw["btc_price"], "btc_mvrv": raw["btc_mvrv"]}, separators=(",", ":")).encode(),
               "application/json", url=COINMETRICS_CSV)
    return pk


def lambda_handler(event, context=None):
    return RS.run(SLUG, build, event or {})


if __name__ == "__main__":  # local dry run: python3 lambda_function.py
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "shared"))
    out = lambda_handler({"dry_run": True, "http": True})
    print(out["body"][:500])
