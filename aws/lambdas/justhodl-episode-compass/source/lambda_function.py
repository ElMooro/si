"""
justhodl-episode-compass v1.0 — Tops · Bottoms · Black Swans
============================================================
Compares TODAY's macro state vector (14 features, z-normalized over their full
histories) against the state at every labeled MARKET TOP, MARKET BOTTOM and
BLACK SWAN since 1973. Outputs class resemblance scores, per-episode matches
with the features that match and diverge, what followed each episode (real
SPX sequels from the deep base), a tail-event checklist — and a server-side
Claude briefing bound to those measured tables.
"""
import anthropic_shim  # resilient LLM fallback (Anthropic->GLM via llm_router)
import json, os, time, re, urllib.request, urllib.parse, bisect
from datetime import datetime, timezone, timedelta
from statistics import mean, pstdev
from decimal import Decimal
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/episode-compass.json"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
VERSION = "1.0.1"

EPISODES = {
  "TOP": [("1973-01", "Nifty-Fifty top"), ("1980-11", "Pre-Volcker-recession top"),
           ("1987-08", "Pre-crash top"), ("2000-03", "Dot-com top"),
           ("2007-10", "GFC top"), ("2020-02", "COVID top"), ("2021-12", "QE-era top")],
  "BOTTOM": [("1974-10", "Nifty bust low"), ("1982-08", "Volcker low"),
              ("1987-12", "Post-crash low"), ("1990-10", "S&L low"),
              ("2002-10", "Dot-com low"), ("2009-03", "GFC low"),
              ("2020-03", "COVID low"), ("2022-10", "Inflation-bear low")],
  "BLACK_SWAN": [("1987-10", "Black Monday"), ("1998-09", "LTCM/Russia"),
                  ("2008-09", "Lehman"), ("2010-05", "Flash crash"),
                  ("2011-08", "US downgrade"), ("2015-08", "Yuan deval"),
                  ("2018-02", "Volmageddon"), ("2018-12", "Powell-pivot purge"),
                  ("2020-03", "COVID crash"), ("2023-03", "SVB/CS")]}


def fred(sid, start="1950-01-01"):
    u = ("https://api.stlouisfed.org/fred/series/observations?"
         + urllib.parse.urlencode({"series_id": sid, "api_key": FRED_KEY,
                                   "file_type": "json", "observation_start": start,
                                   "limit": 100000}))
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=40).read())
        return [(o["date"], float(o["value"])) for o in j.get("observations", [])
                if o.get("value") not in (".", "", None)]
    except Exception as e:
        print(f"[fred] {sid}: {str(e)[:50]}")
        return []


def monthly(pts):
    out = {}
    for d, v in pts:
        out[d[:7]] = v
    return sorted(out.items())


def yoy(mp):
    return [(mp[i][0], (mp[i][1] / mp[i - 12][1] - 1) * 100)
            for i in range(12, len(mp)) if mp[i - 12][1]]


def d6(series):
    return [(series[i][0], series[i][1] - series[i - 6][1])
            for i in range(6, len(series))]


def zmap(series):
    """[(m,v)] → dict m→z (full-history z), plus latest month."""
    vals = [v for _, v in series]
    if len(vals) < 24:
        return {}, None
    mu, sd = mean(vals), pstdev(vals)
    if not sd:
        return {}, None
    return {m: round((v - mu) / sd, 2) for m, v in series}, series[-1][0]


def lambda_handler(event=None, context=None):
    t0 = time.time()
    # ── feature construction (monthly, longest free histories) ──
    gs10 = monthly(fred("GS10")); tb3 = monthly(fred("TB3MS"))
    baa = monthly(fred("BAA")); cpi_idx = monthly(fred("CPIAUCSL"))
    indpro = monthly(fred("INDPRO")); unrate = monthly(fred("UNRATE"))
    m2 = monthly(fred("M2SL")); nfci = monthly(fred("NFCI", "1971-01-01"))
    vix = monthly(fred("VIXCLS", "1990-01-01"))
    tp = monthly(fred("THREEFYTP10", "1990-01-01"))
    sloos = monthly(fred("DRTSCILM", "1990-01-01"))
    cpi_y = yoy(cpi_idx); m2_y = yoy(m2); ip_y = yoy(indpro)
    d10, d3, dbaa, dcpi = dict(gs10), dict(tb3), dict(baa), dict(cpi_y)

    spx_doc = json.loads(S3.get_object(Bucket=BUCKET,
                                        Key="data/spx-history-deep.json")["Body"].read())
    spx_d = [(d, float(v)) for d, v in spx_doc.get("points", []) if v]
    closes = [v for _, v in spx_d]
    dates = [d for d, _ in spx_d]
    # SPX monthly + 200dma distance + 12m ret + drawdown + realized vol (pre-VIX splice)
    sma200, s_ = [], 0.0
    for i, c in enumerate(closes):
        s_ += c
        if i >= 200:
            s_ -= closes[i - 200]
        sma200.append(s_ / 200 if i >= 199 else None)
    spx_m, dist_m, rv_m, dd_m, r12_m = {}, {}, {}, {}, {}
    hi = 0.0
    for i, (d, c) in enumerate(spx_d):
        hi = max(hi, c)
        m = d[:7]
        spx_m[m] = c
        if sma200[i]:
            dist_m[m] = (c / sma200[i] - 1) * 100
        dd_m[m] = (c / hi - 1) * 100
        if i >= 21:
            ch = [closes[j] / closes[j - 1] - 1 for j in range(i - 20, i + 1)]
            mu = mean(ch)
            rv_m[m] = (sum((x - mu) ** 2 for x in ch) / 21) ** 0.5 * (252 ** 0.5) * 100
        if i >= 252:
            r12_m[m] = (c / closes[i - 252] - 1) * 100
    months_spx = sorted(spx_m)
    vixd = dict(vix)
    vix_spliced = [(m, vixd.get(m, rv_m.get(m))) for m in months_spx
                   if (vixd.get(m) or rv_m.get(m)) is not None]

    FEATURES = {
      "curve_10y3m": [(m, d10[m] - d3[m]) for m in sorted(set(d10) & set(d3))],
      "credit_baa10y": [(m, dbaa[m] - d10[m]) for m in sorted(set(dbaa) & set(d10))],
      "real_10y": [(m, d10[m] - dcpi[m]) for m in sorted(set(d10) & set(dcpi))],
      "cpi_6m_mom": d6(cpi_y), "growth_6m_mom": d6(ip_y),
      "unemp_3m_mom": [(unrate[i][0], unrate[i][1] - unrate[i - 3][1])
                        for i in range(3, len(unrate))],
      "real_m2_yoy": [(m, v - dcpi[m]) for m, v in m2_y if m in dcpi],
      "nfci": nfci, "vix_spliced": vix_spliced,
      "spx_dist_200dma": sorted(dist_m.items()),
      "spx_drawdown": sorted(dd_m.items()),
      "spx_12m_ret": sorted(r12_m.items()),
      "term_premium": tp, "sloos": sloos}

    Z, latest = {}, {}
    for k, ser in FEATURES.items():
        zm, last = zmap(ser)
        Z[k] = zm
        if last:
            latest[k] = last
    now_m = max(v for v in latest.values())
    today = {}
    for k, zm in Z.items():
        # use the most recent month available per feature (publication lags differ)
        for back in range(0, 4):
            mm = (datetime.strptime(now_m + "-01", "%Y-%m-%d")
                   - timedelta(days=30 * back)).strftime("%Y-%m")
            if mm in zm:
                today[k] = {"z": zm[mm], "as_of": mm}
                break

    spx_keys = months_spx

    def sequel(m):
        if m not in spx_m:
            return None
        i = spx_keys.index(m)
        out = {}
        for w, lab in ((3, "fwd_3m_pct"), (12, "fwd_12m_pct")):
            if i + w < len(spx_keys):
                out[lab] = round((spx_m[spx_keys[i + w]] / spx_m[m] - 1) * 100, 1)
        return out

    def compare(m):
        ds = []
        for k, zm in Z.items():
            if m in zm and k in today:
                ds.append((k, abs(zm[m] - today[k]["z"]), zm[m]))
        if len(ds) < 8:
            return None
        mad = mean(d for _, d, _ in ds)
        ds.sort(key=lambda x: x[1])
        return {"similarity": round(max(0.0, 100 - 22 * mad), 1),
                "n_features": len(ds),
                "matches": [{"f": k, "ep_z": z, "today_z": today[k]["z"]}
                             for k, _, z in ds[:3]],
                "divergences": [{"f": k, "ep_z": z, "today_z": today[k]["z"]}
                                 for k, _, z in ds[-3:]]}

    classes = {}
    for cls, eps in EPISODES.items():
        rows = []
        for m, label in eps:
            c = compare(m)
            if c:
                rows.append({"date": m, "label": label, **c, "sequel": sequel(m)})
        rows.sort(key=lambda r: -r["similarity"])
        top3 = [r["similarity"] for r in rows[:3]]
        classes[cls] = {"score": round(mean(top3), 1) if top3 else None,
                         "episodes": rows}
    tails = sorted(((k, v["z"]) for k, v in today.items() if abs(v["z"]) >= 1.5),
                   key=lambda x: -abs(x[1]))
    swan_checklist = [{"feature": k, "z": z} for k, z in tails]

    spread = None
    if classes.get("TOP", {}).get("score") is not None and \
       classes.get("BOTTOM", {}).get("score") is not None:
        spread = round(classes["TOP"]["score"] - classes["BOTTOM"]["score"], 1)
    reading = {
        "top_minus_bottom": spread, "tails_n": len(swan_checklist),
        "interpretation": (
            "All class scores run high when today's vector is unstretched — calm "
            "states precede both tops and vol accidents. Read the SPREAD (top-minus-"
            f"bottom {spread:+.1f}) and the tail count ({len(swan_checklist)} features "
            "|z|≥1.5). High swan-likeness with ZERO tails means 'calm-before-accident' "
            "resemblance (2018-type), not active crisis (2008-type).")}

    out = {"engine": "episode-compass", "version": VERSION, "reading": reading,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "today_vector": today, "classes": classes,
           "swan_checklist": swan_checklist,
           "class_scores": {c: classes[c]["score"] for c in classes},
           "methodology": ("14 monthly features (curve, credit, real rates, CPI & growth "
                            "momentum, labor momentum, real M2, NFCI, VIX spliced with "
                            "pre-1990 realized vol, SPX 200dma distance/drawdown/12m, term "
                            "premium, SLOOS), z-normalized over full history. Similarity = "
                            "100 − 22×mean|Δz| vs each labeled episode (≥8 shared features). "
                            "Sequels are real SPX forwards from the deep base.")}

    # ── server-side AI briefing bound to the tables ──
    ai = {"error": None}
    try:
        if ANTHROPIC_KEY:
            compact = {"class_scores": out["class_scores"], "reading": reading,
                        "top_matches": {c: classes[c]["episodes"][:2] for c in classes},
                        "today_extremes": swan_checklist[:6]}
            prompt = (
              "You are a market historian on an institutional desk. Using ONLY the "
              "measured comparison below (z-scores, similarity %, real SPX sequels), "
              "Never claim feature counts beyond the data shown. Write JSON with keys: verdict (<=160 chars), closest_rhyme (which single "
              "historical episode today most resembles and why, citing features), "
              "key_divergences (what today does NOT share with tops/swans), "
              "historical_sequels (what followed the closest matches, with the numbers), "
              "swan_checklist_read (are any tail features flashing), watch_next (array of "
              "3 short strings). Under 380 words total. No markdown, JSON only.\n\nDATA:\n"
              + json.dumps(compact, default=str))
            req = urllib.request.Request(
                "https://api.anthropic.com/v1/messages",
                data=json.dumps({"model": "claude-haiku-4-5-20251001",
                                  "max_tokens": 2200,
                                  "messages": [{"role": "user", "content": prompt}]}).encode(),
                headers={"x-api-key": ANTHROPIC_KEY,
                         "anthropic-version": "2023-06-01",
                         "content-type": "application/json"})
            rj = json.loads(urllib.request.urlopen(req, timeout=90).read())
            txt = "".join(b.get("text", "") for b in rj.get("content", [])
                           if b.get("type") == "text")
            txt = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", txt)
            m1, m2_ = txt.find("{"), txt.rfind("}")
            ai.update(json.loads(txt[m1:m2_ + 1]))
        else:
            ai["error"] = "no ANTHROPIC_API_KEY in env"
    except Exception as e:
        ai["error"] = str(e)[:120]
    out["ai_brief"] = ai

    out["duration_s"] = round(time.time() - t0, 1)
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[compass] scores={out['class_scores']} {out['duration_s']}s ai={ai.get('error')}")
    return {"statusCode": 200, "body": json.dumps(out["class_scores"])}
