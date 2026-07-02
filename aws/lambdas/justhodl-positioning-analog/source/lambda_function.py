"""justhodl-positioning-analog — CRISIS ANALOGS & FORWARD MAP (Khalid mandate).

Answers three questions on the Surveillance Desk: which historical weeks did
market CONDITIONS most resemble today, what did SPX do NEXT from those weeks,
and what does the fusion imply forward. Long-history, source-robust dims
(1997->, weekly) so analogs span dot-com, GFC, 2011, 2015, 2018Q4, COVID,
2022 bear, SVB, the 2024 yen-carry unwind:

  DIMS (z over full panel)   VIXCLS · NFCI · HY OAS (BAMLH0A0HYM2) ·
                             curve 10y-3m (T10Y3M) · SPX drawdown-from-high ·
                             SPX 13w momentum · SPX 4w realized vol
  SIMILARITY                 euclidean in z-space -> sim = 100/(1+d);
                             analogs deduped to distinct episodes (>=60d apart),
                             last 8 weeks excluded (self-match guard)
  FORWARD MAP                SPX +4w / +13w from each analog week; medians,
                             hit-rates over the top set
  EPISODE LABELS             named crisis/top windows (LTCM..2024 carry unwind)
  AI OUTLOOK                 llm_router 4-sentence brief, _clean contract,
                             DETERMINISTIC fallback (never empty/truncated)

  OUT   data/positioning-analog.json   (page: institutional-footprint.html)
  CRON  daily 22:40 UTC (after factor-returns, before footprint 23:10)
"""
import json, math, time, urllib.request, urllib.parse, statistics
from datetime import datetime, timezone
import boto3

BUCKET, OUT = "justhodl-dashboard-live", "data/positioning-analog.json"
FRED_KEY = "2f057499936072679d8843d7fce99989"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com"}
s3 = boto3.client("s3", region_name="us-east-1")

EPISODES = [  # (start, end, label)
    ("1998-08-01", "1998-10-15", "LTCM/Russia 1998"),
    ("2000-03-01", "2000-09-30", "Dot-com top 2000"),
    ("2001-09-01", "2001-10-15", "9/11 shock"),
    ("2002-07-01", "2002-10-15", "Dot-com bottom 2002"),
    ("2007-10-01", "2007-12-31", "2007 market top"),
    ("2008-09-01", "2009-01-31", "GFC core 2008"),
    ("2009-03-01", "2009-04-30", "GFC bottom 2009"),
    ("2010-05-01", "2010-07-15", "Flash crash 2010"),
    ("2011-08-01", "2011-10-15", "US downgrade 2011"),
    ("2015-08-01", "2016-02-28", "China deval 2015-16"),
    ("2018-01-15", "2018-02-28", "Volmageddon 2018"),
    ("2018-10-01", "2018-12-31", "2018 Q4 rout"),
    ("2020-02-15", "2020-04-15", "COVID crash 2020"),
    ("2021-11-01", "2022-01-15", "2021/22 market top"),
    ("2022-06-01", "2022-10-31", "2022 bear trough"),
    ("2023-03-01", "2023-03-31", "SVB stress 2023"),
    ("2024-07-15", "2024-08-15", "Yen-carry unwind 2024"),
]

def _get(url, timeout=30):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
        return json.loads(r.read())

def fred(series, start="1996-01-01"):
    u = ("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s"
         "&file_type=json&observation_start=%s") % (series, FRED_KEY, start)
    out = {}
    for o in _get(u, 40).get("observations", []):
        v = o.get("value")
        if v not in (None, ".", ""):
            try: out[o["date"]] = float(v)
            except Exception: pass
    return out

def spx_closes():
    # FMP caps ~5000 rows per call — paginate by decade chunks and merge.
    out = {}
    for frm, to in (("1996-01-01", "2006-06-30"), ("2006-07-01", "2016-06-30"),
                    ("2016-07-01", "2030-01-01")):
        u = ("https://financialmodelingprep.com/stable/historical-price-eod/full"
             "?symbol=%%5EGSPC&from=%s&to=%s&apikey=%s") % (frm, to, FMP_KEY)
        rows = _get(u.replace("%%", "%"), 60)
        if isinstance(rows, dict): rows = rows.get("historical") or rows.get("data") or []
        for r in rows or []:
            d, c = r.get("date"), r.get("close") or r.get("adjClose")
            if d and c:
                try: out[d[:10]] = float(c)
                except Exception: pass
        time.sleep(0.25)
    return dict(sorted(out.items()))

def weekly(daily):  # last obs each ISO week -> {(iso_year, iso_week): (date, v)}
    wk = {}
    for d, v in sorted(daily.items()):
        y, w, _ = datetime.strptime(d, "%Y-%m-%d").isocalendar()
        wk[(y, w)] = (d, v)
    return wk

def z(vals):
    m, sd = statistics.mean(vals), statistics.pstdev(vals)
    return [(v - m) / sd if sd > 1e-9 else 0.0 for v in vals]

def label_for(date):
    for a, b, lab in EPISODES:
        if a <= date <= b: return lab
    return None

def lambda_handler(event=None, context=None):
    print("[analog] pulling panels…")
    spx = spx_closes(); assert len(spx) > 6500, "SPX history thin: %d" % len(spx)
    sdates = list(spx); scl = list(spx.values())
    vix, nfci = fred("VIXCLS"), fred("NFCI")
    hy, curve = fred("BAMLH0A0HYM2"), fred("T10Y3M")
    time.sleep(0.3)
    # derived SPX dims (daily)
    dd, mom, rvol, hi = {}, {}, {}, 0.0
    rets = []
    for i, d in enumerate(sdates):
        c = scl[i]; hi = max(hi, c); dd[d] = c / hi - 1.0
        if i >= 65: mom[d] = c / scl[i - 65] - 1.0
        if i >= 1:
            rets.append(math.log(c / scl[i - 1]))
            if len(rets) >= 20:
                rvol[d] = statistics.pstdev(rets[-20:]) * math.sqrt(252)
    RAW = {"vix": weekly(vix), "nfci": weekly(nfci), "hy_oas": weekly(hy),
           "curve_10y3m": weekly(curve), "spx_drawdown": weekly(dd),
           "spx_mom_13w": weekly(mom), "spx_rvol_4w": weekly(rvol)}
    depth = {k: len(v) for k, v in RAW.items()}
    print("[analog] per-dim weekly depth:", json.dumps(depth))
    DIMS = {k: v for k, v in RAW.items() if len(v) >= 900}   # graceful dim gate
    dropped = sorted(set(RAW) - set(DIMS))
    if dropped: print("[analog] DROPPED short dims:", dropped)
    assert len(DIMS) >= 5, "too few long dims: %s" % json.dumps(depth)
    common = sorted(set.intersection(*(set(v) for v in DIMS.values())))
    assert len(common) > 900, "panel thin after tuple-keying: %d (depth %s)" % (len(common), json.dumps(depth))
    dates_of = {wk: max(DIMS[k][wk][0] for k in DIMS) for wk in common}
    mat = {k: z([DIMS[k][wk][1] for wk in common]) for k in DIMS}
    today_i = len(common) - 1
    cur = {k: round(mat[k][today_i], 2) for k in DIMS}

    # similarity vs all weeks (exclude trailing 8)
    cweeks = [dates_of[wk] for wk in common]   # ISO-dated week labels
    sims = []
    for i, d in enumerate(cweeks[:-8]):
        dist = math.sqrt(sum((mat[k][i] - mat[k][today_i]) ** 2 for k in DIMS))
        sims.append((100.0 / (1.0 + dist), d))
    sims.sort(reverse=True)

    # dedupe to distinct episodes, forward map
    def fwd(d, days):
        j = sdates.index(d) if d in sdates else None
        if j is None:
            j = max(0, min(range(len(sdates)), key=lambda k: abs(datetime.strptime(sdates[k], "%Y-%m-%d")
                                                                 - datetime.strptime(d, "%Y-%m-%d")).days))
        k2 = j + days
        return round((scl[k2] / scl[j] - 1) * 100, 1) if k2 < len(scl) else None
    picked, analogs = [], []
    for sim, d in sims:
        if any(abs((datetime.strptime(d, "%Y-%m-%d") - datetime.strptime(p, "%Y-%m-%d")).days) < 60 for p in picked):
            continue
        picked.append(d)
        analogs.append({"date": d, "label": label_for(d) or "unlabeled regime",
                        "similarity": round(sim, 1),
                        "spx_fwd_1m_pct": fwd(d, 21), "spx_fwd_3m_pct": fwd(d, 65)})
        if len(analogs) >= 6: break
    f1 = [a["spx_fwd_1m_pct"] for a in analogs if a["spx_fwd_1m_pct"] is not None]
    f3 = [a["spx_fwd_3m_pct"] for a in analogs if a["spx_fwd_3m_pct"] is not None]
    stats = {"median_fwd_1m_pct": round(statistics.median(f1), 1) if f1 else None,
             "median_fwd_3m_pct": round(statistics.median(f3), 1) if f3 else None,
             "hit_rate_up_3m": round(100 * sum(1 for x in f3 if x > 0) / len(f3)) if f3 else None}
    crisis_names = [a["label"] for a in analogs if a["label"] != "unlabeled regime"]
    verdict = ("CRISIS-ADJACENT CONDITIONS" if len(crisis_names) >= 3 and analogs[0]["similarity"] >= 55
               else "TOP-LIKE CALM" if cur["vix"] < -0.5 and cur["spx_drawdown"] > -0.2 and cur["spx_mom_13w"] > 0.5
               else "MIXED REGIME")

    # AI outlook (contract + deterministic fallback)
    outlook, src = None, "deterministic"
    try:
        from llm_router import complete
        prompt = ("Respond with ONLY four plain prose sentences — no preamble, no lists, no markdown. "
                  "You are a market historian. Today's condition vector (z): %s. Closest historical analogs: %s. "
                  "Forward stats: %s. Write what regimes today most resembles, what followed then, the key "
                  "difference now, and a probabilistic forward read." %
                  (json.dumps(cur), json.dumps(analogs[:4]), json.dumps(stats)))
        t = complete(prompt, tier="reason", max_tokens=420)
        if t:
            t = str(t).strip().replace("**", "")
            if any(k in t[:200] for k in ("Analyze", "Let me", "First,", "Wait", "Hmm", "Actually", "Okay")):
                ps = [p.strip() for p in t.split("\n\n") if len(p.strip()) >= 80]
                t = ps[-1] if ps else ""
            if len(t) > 700: t = t[:700]
            if t and not t.rstrip().endswith((".", "!", "?")):
                t = t.rsplit(".", 1)[0] + "." if "." in t else ""
            if 90 <= len(t) <= 720: outlook, src = t, "llm"
    except Exception as e:
        print("[analog] llm:", str(e)[:60])
    if not outlook:
        a0 = analogs[0]
        outlook = ("Today's condition vector sits closest to %s (%s, similarity %.0f) with echoes of %s. "
                   "From the top analog weeks SPX ran %+.1f%% over one month and %+.1f%% over three (up %d%% of the time). "
                   "The defining tension now: momentum z %+.1f with drawdown z %+.1f against credit z %+.1f and vol z %+.1f. "
                   "History maps this as a %s tape — respect the forward hedging, but the analog set says dips from here were %s."
                   % (a0["label"], a0["date"], a0["similarity"],
                      ", ".join(dict.fromkeys(crisis_names[1:3])) or "unlabeled regimes",
                      stats["median_fwd_1m_pct"] or 0, stats["median_fwd_3m_pct"] or 0, stats["hit_rate_up_3m"] or 0,
                      cur["spx_mom_13w"], cur["spx_drawdown"], cur["hy_oas"], cur["vix"],
                      verdict.lower(), "bought" if (stats["hit_rate_up_3m"] or 0) >= 60 else "sold"))
    doc = {"engine": "justhodl-positioning-analog", "version": "1.0.0",
           "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "as_of_week": cweeks[-1], "panel_weeks": len(common), "dims": len(DIMS), "dims_dropped": dropped, "dim_depth": depth,
           "vector_today_z": cur, "verdict": verdict, "analogs": analogs,
           "forward_stats": stats, "ai_outlook": outlook, "ai_outlook_src": src,
           "method": ("7-dim weekly condition vector (VIX, NFCI, HY OAS, 10y-3m, SPX drawdown/13w-momentum/"
                      "4w-rvol), z over %d weeks since %s; euclidean similarity, episodes deduped >=60d; "
                      "forward = SPX +21d/+65d from analog week. Positioning-space match, not a price forecast.")
                     % (len(common), cweeks[0])}
    s3.put_object(Bucket=BUCKET, Key=OUT,
                  Body=json.dumps(doc, separators=(",", ":"), allow_nan=False).encode(),
                  ContentType="application/json", CacheControl="public, max-age=60")
    return {"ok": True, "weeks": len(common), "verdict": verdict,
            "top": analogs[0], "stats": stats, "outlook_src": src}
