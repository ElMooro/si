"""justhodl-fundamental-census v1.0.0 (ops 3527) — the S&P-500-wide
fundamental sweep. Institutional census over EVERY metric the
fundamental-graphs engine computes: periodically (1st + 15th, 06:00
UTC) rebuilds all ~500 docs through the existing verdict/elite
machinery, then aggregates the cache into hedge-fund boards:

  · TOP quality      — 2x elites + greens − severity-weighted reds
                       (fundamental basis only; tech verdicts excluded)
  · WORST quality    — the mirror board
  · METRIC boards    — best/worst 10 per core metric, direction-aware
  · CAREFUL board    — dilution (share_count_yoy: >=4%/yr HEAVY,
                       >=8 SEVERE), sev-3 reds, earnings-integrity /
                       accrual percentile floors from factor_dna
  · SECTOR strip     — per-sector average score + best/worst name

Architecture (extend-don't-rebuild): phase "warm" sync-invokes
justhodl-fundamental-graphs {"warm": batch25, refresh} and async
self-chains the cursor; the final batch chains phase "aggregate",
which reads data/fundgraph/cache/{SYM}_quarter_v21.json for the whole
forensic universe and writes data/fundamental-census.json. Partial
coverage is stated honestly (coverage block + dormant names). All
values REAL from the docs; nulls skipped, never invented.
"""
import json
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

VERSION = "1.6.1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/fundamental-census.json"
MATRIX_KEY = "data/fundamental-census-matrix.json"
MX_EXCLUDE_PRE = ("px_", "rsi_", "vol", "est_")
HIST_KEY = "data/fundamental-census-history.json"
CACHE_TPL = "data/fundgraph/cache/{sym}_quarter_v21.json"
FG_FN = "justhodl-fundamental-graphs"
BATCH = 8  # v1.2.0: small SYNC batches — proven robust; Event-25 dropped silently

S3 = boto3.client("s3", region_name="us-east-1")
LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=420,
                                 connect_timeout=10,
                                 retries={"max_attempts": 0}))

CORE_METRICS = [
    ("gross_margin_pct", "Gross margin %", "H"),
    ("operating_margin_pct", "Operating margin %", "H"),
    ("net_margin_pct", "Net margin %", "H"),
    ("ebitda_margin_pct", "EBITDA margin %", "H"),
    ("fcf_margin_pct", "FCF margin %", "H"),
    ("fcf_yield_pct", "FCF yield %", "H"),
    ("roe_pct", "Return on equity %", "H"),
    ("roa_pct", "Return on assets %", "H"),
    ("roic_pct", "ROIC %", "H"),
    ("revenue_yoy_pct", "Revenue growth YoY %", "H"),
    ("eps_yoy_pct", "EPS growth YoY %", "H"),
    ("fcf_yoy_pct", "FCF growth YoY %", "H"),
    ("interest_coverage_ttm", "Interest coverage (x)", "H"),
    ("current_ratio", "Current ratio", "H"),
    ("piotroski", "Piotroski F-score", "H"),
    ("altman_z", "Altman Z", "H"),
    ("shareholder_yield_pct", "Shareholder yield %", "H"),
    ("buyback_yield_pct", "Buyback yield %", "H"),
    ("debt_to_equity", "Debt / equity", "L"),
    ("dio_days", "Inventory days", "L"),
    ("sbc_to_revenue_pct", "SBC / revenue %", "L"),
    ("share_count_yoy_pct", "Share count YoY %", "L"),
]
TURN_METRICS = [
    ("gross_margin_pct", "gross margin", "H", "pp"),
    ("operating_margin_pct", "op margin", "H", "pp"),
    ("fcf_margin_pct", "FCF margin", "H", "pp"),
    ("roe_pct", "ROE", "H", "pp"),
    ("roic_pct", "ROIC", "H", "pp"),
    ("revenue_yoy_pct", "rev growth", "H", "pp"),
    ("eps_yoy_pct", "EPS growth", "H", "pp"),
    ("interest_coverage_ttm", "int. coverage", "H", "x"),
    ("debt_to_equity", "debt/equity", "L", "x"),
    ("share_count_yoy_pct", "share count YoY", "L", "pp"),
]
MIN_BOARD_N = 150
FLAG_W = {"DILUTION_SEVERE": 3, "DILUTION_HEAVY": 2,
          "EARNINGS_INTEGRITY_LOW": 2, "ACCRUALS_HIGH": 2,
          "HIGH_CONCERN": 2}


def rnd(v, n=2):
    try:
        return round(float(v), n)
    except (TypeError, ValueError):
        return None


def universe():
    d = json.loads(S3.get_object(
        Bucket=BUCKET, Key="data/forensic-screen.json")["Body"].read())
    rows = d.get("all_results") or []
    out = []
    seen = set()
    for r in rows:
        t = r.get("ticker") or r.get("symbol")
        if t and t not in seen:
            seen.add(t)
            out.append({"t": t, "sector": r.get("sector") or "Unknown"})
    return out


def momentum(price, weeks):
    """Price momentum over `weeks` (12-1 style handled by caller):
    pct change last close vs close `weeks` back; needs weekly rows."""
    px = [v for _, v in (price or []) if isinstance(v, (int, float))
          and v > 0]
    if len(px) < weeks + 1:
        return None
    return round((px[-1] / px[-1 - weeks] - 1.0) * 100.0, 2)


def mom_12_1(price):
    px = [v for _, v in (price or []) if isinstance(v, (int, float))
          and v > 0]
    if len(px) < 53:
        return None
    return round((px[-5] / px[-53] - 1.0) * 100.0, 2)


def turn_delta(P, key, w=4):
    """4q-vs-prior-4q mean delta; needs >=2w numeric points; None else."""
    ser = [v for _, v in ((P or {}).get(key) or [])
           if isinstance(v, (int, float))]
    if len(ser) < 2 * w:
        return None
    a = sum(ser[-w:]) / w
    b = sum(ser[-2 * w:-w]) / w
    return round(a - b, 4)


def last_val(P, key):
    s = (P or {}).get(key) or []
    for d, v in reversed(s):
        if isinstance(v, (int, float)):
            return float(v)
    return None


def extract(doc, sector):
    """Pure per-ticker census row from a fundgraph doc."""
    V = doc.get("verdicts") or {}
    greens = [v for v in (V.get("greens") or [])
              if isinstance(v, dict) and v.get("basis") != "tech"]
    reds = [v for v in (V.get("reds") or [])
            if isinstance(v, dict) and v.get("basis") != "tech"]
    elites = [v for v in greens if v.get("elite")]
    sev = sum(int(v.get("sev") or 1) for v in reds)
    score = 2 * len(elites) + len(greens) - sev
    P = doc.get("points") or {}
    met = {k: rnd(last_val(P, k), 2) for k, _, _ in CORE_METRICS}
    dil = met.get("share_count_yoy_pct")
    flags = []
    if dil is not None and dil >= 8:
        flags.append("DILUTION_SEVERE")
    elif dil is not None and dil >= 4:
        flags.append("DILUTION_HEAVY")
    fd = doc.get("factor_dna") or {}
    for a in (fd.get("axes") or []):
        if a.get("k") == "beneish_m" and (a.get("pct") or 100) < 10:
            flags.append("EARNINGS_INTEGRITY_LOW")
        if a.get("k") == "sloan_accruals_pct" and (a.get("pct") or 100) < 10:
            flags.append("ACCRUALS_HIGH")
        if a.get("k") == "concern_score" and (a.get("pct") or 100) < 5:
            flags.append("HIGH_CONCERN")
    red3 = [v["k"] for v in reds if int(v.get("sev") or 1) >= 3]
    flag_w = (sum(FLAG_W.get(f, 1) for f in flags)
              + 3 * len(red3))
    tr = {k: turn_delta(P, k) for k, _, _, _ in TURN_METRICS}
    industry = ((doc.get("profile") or {}).get("industry") or "").strip()
    price = doc.get("price") or []
    moms = {"mom_6m_pct": momentum(price, 26),
            "mom_12_1_pct": mom_12_1(price)}
    return {"t": doc.get("symbol") or doc.get("ticker"),
            "sector": sector, "industry": industry, "score": score,
            "n_elite": len(elites), "n_green": len(greens),
            "n_red": len(reds), "sev_sum": sev,
            "top_elites": [v["k"] for v in elites[:3]],
            "red3": red3[:4], "flags": flags,
            "flag_w": flag_w, "dilution_yoy": dil,
            "metrics": met, "_tr": tr,
            "_lv": {**{mk: mv for mk, mv in moms.items()
                       if mv is not None},
                    **{k: rnd(last_val(P, k), 4) for k in P
                    if not any(k.startswith(pre)
                               for pre in MX_EXCLUDE_PRE)
                    and last_val(P, k) is not None}},
            "vintage_days": doc.get("vintage_days")}


def build_census(rows_by_t, uni):
    rows, dormant = [], []
    for u in uni:
        r = rows_by_t.get(u["t"])
        if r:
            rows.append(r)
        else:
            dormant.append(u["t"])
    rows_scored = [r for r in rows if r["t"]]
    top = sorted(rows_scored, key=lambda r: (-r["score"], r["t"]))
    boards = {}
    for k, label, direction in CORE_METRICS:
        vals = [(r["t"], (r.get("_lv") or {}).get(
            k, r["metrics"].get(k))) for r in rows_scored]
        vals = [(t, v) for t, v in vals if v is not None]
        if len(vals) < MIN_BOARD_N:
            print(f"[census] board {k} skipped — coverage {len(vals)}")
            continue
        srt = sorted(vals, key=lambda x: x[1],
                     reverse=(direction == "H"))
        boards[k] = {"label": label, "dir": direction,
                     "n": len(vals),
                     "best": [{"t": t, "v": v} for t, v in srt[:10]],
                     "worst": [{"t": t, "v": v}
                               for t, v in srt[-10:]][::-1]}
    careful = sorted([r for r in rows_scored
                      if r["flags"] or r["red3"]],
                     key=lambda r: (-r["flag_w"], r["t"]))[:60]
    sectors = {}
    for r in rows_scored:
        s = sectors.setdefault(r["sector"],
                               {"n": 0, "sum": 0, "best": None,
                                "worst": None})
        s["n"] += 1
        s["sum"] += r["score"]
        if not s["best"] or r["score"] > s["best"][1]:
            s["best"] = (r["t"], r["score"])
        if not s["worst"] or r["score"] < s["worst"][1]:
            s["worst"] = (r["t"], r["score"])
    sec_rows = sorted(
        [{"sector": k, "n": v["n"],
          "avg_score": rnd(v["sum"] / v["n"], 1),
          "best": v["best"], "worst": v["worst"]}
         for k, v in sectors.items()],
        key=lambda x: -x["avg_score"])
    # ── TURNAROUNDS: cross-sectional percentile of 4q-vs-4q deltas,
    # direction-adjusted; needs >=5 comparable metrics per name ──
    def _pct_map(kk, low):
        col = [(r["t"], (r.get("_tr") or {}).get(kk))
               for r in rows_scored]
        col = [(t, v) for t, v in col if v is not None]
        col.sort(key=lambda x: x[1])
        n = len(col)
        out = {}
        i = 0
        while i < n:
            j = i
            while j + 1 < n and col[j + 1][1] == col[i][1]:
                j += 1
            p = 100.0 * ((i + j) / 2 + 0.5) / n
            for q2 in range(i, j + 1):
                out[col[q2][0]] = 100.0 - p if low else p
            i = j + 1
        return out, n
    tmaps = {}
    for kk, lbl2, d2, u2 in TURN_METRICS:
        mp, nn = _pct_map(kk, d2 == "L")
        if nn >= MIN_BOARD_N:
            tmaps[kk] = mp
    trows = []
    for r in rows_scored:
        parts = []
        for kk, lbl2, d2, u2 in TURN_METRICS:
            mp = tmaps.get(kk)
            if not mp or r["t"] not in mp:
                continue
            dv = (r.get("_tr") or {}).get(kk)
            parts.append((kk, lbl2, u2, dv, mp[r["t"]]))
        if len(parts) < 5:
            continue
        tscore = rnd(sum(p[4] for p in parts) / len(parts), 1)
        drivers = sorted(parts, key=lambda p: -p[4])[:3]
        laggers = sorted(parts, key=lambda p: p[4])[:3]
        trows.append({"t": r["t"], "sector": r["sector"],
                      "turn_score": tscore,
                      "n_metrics": len(parts),
                      "drivers": [{"k": d[0], "label": d[1],
                                   "u": d[2], "delta": d[3]}
                                  for d in drivers],
                      "laggers": [{"k": d[0], "label": d[1],
                                   "u": d[2], "delta": d[3]}
                                  for d in laggers],
                      "quality_now": r["score"],
                      "flags": r["flags"]})
    trows.sort(key=lambda x: -x["turn_score"])
    _turn_full = {r["t"]: r["turn_score"] for r in trows}
    turnarounds = {"improving": trows[:25],
                   "deteriorating": trows[-15:][::-1],
                   "method": ("mean of direction-adjusted cross-"
                              "sectional percentiles of 4q-vs-prior-"
                              "4q deltas; >=5 comparable metrics")}

    slim = ["t", "sector", "score", "n_elite", "n_green", "n_red",
            "top_elites", "red3", "flags", "dilution_yoy"]
    return {
        "top_quality": [{k: r[k] for k in slim} for r in top[:50]],
        "bottom_quality": [{k: r[k] for k in slim}
                           for r in top[-50:]][::-1],
        "careful": [{k: r[k] for k in slim + ["flag_w"]}
                    for r in careful],
        "_turn_full": _turn_full,
        "metric_boards": boards, "turnarounds": turnarounds,
        "sectors": sec_rows,
        "coverage": {"universe": len(uni), "scored": len(rows_scored),
                     "dormant_n": len(dormant),
                     "dormant_sample": dormant[:12]},
        "summary": {"avg_score": rnd(sum(r["score"] for r in
                                         rows_scored)
                                     / max(1, len(rows_scored)), 1),
                    "n_flagged": sum(1 for r in rows_scored
                                     if r["flags"] or r["red3"]),
                    "n_elite_heavy": sum(1 for r in rows_scored
                                         if r["n_elite"] >= 5)},
    }


FACTORS = {
    "factor_value": [("fcf_yield_pct", 0), ("fcf_ev_yield_pct", 0),
                     ("pe_ttm", 1), ("ps_ttm", 1), ("peg", 1)],
    "factor_quality": [("roic_pct", 0), ("roe_pct", 0),
                       ("gross_margin_pct", 0), ("fcf_margin_pct", 0),
                       ("sloan_accruals_pct", 1), ("beneish_m", 1)],
    "factor_momentum": [("mom_12_1_pct", 0), ("mom_6m_pct", 0)],
    "factor_growth": [("revenue_yoy_pct", 0), ("eps_yoy_pct", 0),
                      ("fcf_yoy_pct", 0)],
    "factor_safety": [("altman_z", 0), ("interest_coverage_ttm", 0),
                      ("debt_to_equity", 1), ("current_ratio", 0)],
}


def cross_pct(col, low=False):
    """Percentile per index with avg-rank ties; None-safe; low flips."""
    idx = [(v, i) for i, v in enumerate(col)
           if isinstance(v, (int, float))]
    idx.sort(key=lambda x: x[0])
    out = [None] * len(col)
    n = len(idx)
    i = 0
    while i < n:
        j = i
        while j + 1 < n and idx[j + 1][0] == idx[i][0]:
            j += 1
        pv = 100.0 * ((i + j) / 2 + 0.5) / n
        for q2 in range(i, j + 1):
            out[idx[q2][1]] = round(100.0 - pv if low else pv, 2)
        i = j + 1
    return out


def add_factors(cols, n):
    for fk, parts in FACTORS.items():
        mats = [cross_pct(cols.get(k) or [None] * n, low)
                for k, low in parts if cols.get(k)]
        if not mats:
            continue
        out = []
        for i in range(n):
            vs = [mm[i] for mm in mats if mm[i] is not None]
            out.append(round(sum(vs) / len(vs), 1) if len(vs) >= 2
                       else None)
        cols[fk] = out
    return cols


def joins(S3c, tickers):
    """Whale 13F $ + earnings-days + book membership columns."""
    import json as _j
    from datetime import datetime as _dt, timezone as _tz
    out = {"whale_net_usd_m": [None] * len(tickers),
           "earnings_in_days": [None] * len(tickers),
           "in_long_book": [0] * len(tickers),
           "in_short_book": [0] * len(tickers)}
    pos = {t: i for i, t in enumerate(tickers)}
    try:
        tf = _j.loads(S3c.get_object(
            Bucket=BUCKET,
            Key="data/13f-flows-by-ticker.json")["Body"].read())
        for t, o in (tf.get("t") or {}).items():
            i = pos.get(str(t).upper())
            if i is None or not isinstance(o, dict):
                continue
            v = o.get("wn") if isinstance(o.get("wn"),
                                          (int, float)) else None
            for kk, vv in (o.items() if v is None else []):
                if "whale" in kk and "usd" in kk and                         isinstance(vv, (int, float)):
                    v = vv; break
            if v is None:
                w = o.get("whales")
                if isinstance(w, dict):
                    for kk, vv in w.items():
                        if "usd" in kk and isinstance(vv, (int, float)):
                            v = vv; break
            if v is not None:
                out["whale_net_usd_m"][i] = round(v / 1e6, 1)
    except Exception as e:  # noqa: BLE001
        print("[joins] 13f:", str(e)[:80])
    try:
        ecal = _j.loads(S3c.get_object(
            Bucket=BUCKET,
            Key="data/benzinga-earnings-calendar.json")["Body"].read())
        ed = {}
        def _ew(o):
            if isinstance(o, dict):
                tk = o.get("ticker") or o.get("symbol")
                d = (o.get("date") or o.get("earnings_date")
                     or o.get("report_date"))
                if tk and d:
                    ed.setdefault(str(tk).upper(), str(d)[:10])
                for v in o.values():
                    _ew(v)
            elif isinstance(o, list):
                for v in o:
                    _ew(v)
        _ew(ecal)
        today = _dt.now(_tz.utc).date()
        for t, d in ed.items():
            i = pos.get(t)
            if i is None:
                continue
            try:
                dd = (_dt.fromisoformat(d).date() - today).days
                if 0 <= dd <= 45:
                    out["earnings_in_days"][i] = dd
            except ValueError:
                continue
    except Exception as e:  # noqa: BLE001
        print("[joins] ecal:", str(e)[:80])
    for key, colname in (("data/proven-portfolio.json", "in_long_book"),
                         ("data/short-book.json", "in_short_book")):
        try:
            d = _j.loads(S3c.get_object(Bucket=BUCKET, Key=key)
                         ["Body"].read())
            for r in d.get("book") or []:
                i = pos.get(str(r.get("ticker") or "").upper())
                if i is not None:
                    out[colname][i] = 1
        except Exception as e:  # noqa: BLE001
            print("[joins]", key, str(e)[:60])
    return out


def build_matrix(rows_by_t, uni, turn_map=None, flag_set=None):
    """Columnar latest-value matrix over EVERY fundamentals metric
    present in >=50%% of scored docs (tech/price/estimate keys
    excluded) — the explorer's sorting substrate. rows_by_t entries
    carry _P (full points) attached by the aggregate loop."""
    scored = [u["t"] for u in uni if u["t"] in rows_by_t]
    sectors = [rows_by_t[t]["sector"] for t in scored]
    industries = [rows_by_t[t].get("industry") or "" for t in scored]
    quality = [rows_by_t[t].get("score") for t in scored]
    turn = [(turn_map or {}).get(t) for t in scored]
    flagged = [1 if t in (flag_set or set()) else 0 for t in scored]
    counts = {}
    latest = {}
    for t in scored:
        lv = rows_by_t[t].get("_lv") or {}
        for k in lv:
            counts[k] = counts.get(k, 0) + 1
        latest[t] = lv
    n = max(1, len(scored))
    keys = sorted([k for k, c in counts.items() if c >= 0.5 * n])[:240]
    cols = {k: [latest[t].get(k) for t in scored] for k in keys}
    add_factors(cols, len(scored))
    try:
        cols.update(joins(S3, scored))
    except Exception as e:  # noqa: BLE001
        print("[matrix] joins:", str(e)[:80])
    keys = sorted(cols.keys())
    return {"generated_at": datetime.now(timezone.utc).isoformat(),
            "n_tickers": len(scored), "n_metrics": len(keys),
            "tickers": scored, "sectors": sectors,
            "industries": industries, "quality": quality,
            "turn": turn, "flagged": flagged,
            "metrics": keys, "cols": cols}


def lambda_handler(event, context):
    event = event or {}
    phase = event.get("phase") or "aggregate"
    uni = universe()
    if phase == "warm":
        # v1.1.2 chain-hardening: fundgraph batches fire-and-forget
        # (Event) with ~35s pacing so a slow batch can NEVER kill the
        # orchestrator; the next chain link is guaranteed by finally.
        cur = int(event.get("cursor") or 0)
        refresh = bool(event.get("refresh"))
        batch = [u["t"] for u in uni[cur:cur + BATCH]]
        try:
            if batch:
                rr = LAM.invoke(FunctionName=FG_FN,
                                Payload=json.dumps(
                                    {"warm": batch,
                                     "periods": ["quarter"],
                                     "refresh": refresh}).encode())
                print(f"[census] warm@{cur} status="
                      f"{rr.get('StatusCode')} err="
                      f"{rr.get('FunctionError')}")
        except Exception as e:  # noqa: BLE001
            print(f"[census] warm batch@{cur}: {str(e)[:90]}")
        finally:
            nxt = cur + BATCH
            payload = ({"phase": "warm", "cursor": nxt,
                        "refresh": refresh}
                       if nxt < len(uni)
                       else {"phase": "aggregate", "settle_s": 240})
            LAM.invoke(FunctionName=context.function_name,
                       InvocationType="Event",
                       Payload=json.dumps(payload).encode())
        return {"ok": True, "phase": "warm", "cursor": cur,
                "n_batch": len(batch)}

    if event.get("settle_s"):
        time.sleep(min(600, int(event["settle_s"])))

    t0 = time.time()
    rows_by_t = {}
    for u in uni:
        try:
            doc = json.loads(S3.get_object(
                Bucket=BUCKET,
                Key=CACHE_TPL.format(sym=u["t"]))["Body"].read())
            doc.setdefault("symbol", u["t"])
            rows_by_t[u["t"]] = extract(doc, u["sector"])
        except Exception:  # noqa: BLE001
            continue
    census = build_census(rows_by_t, uni)
    turn_map = {r["t"]: r["turn_score"]
                for r in (census["turnarounds"]["improving"]
                          + census["turnarounds"]["deteriorating"])}
    # full turn coverage: recompute quick map from all trows? improving/
    # deteriorating are trimmed — rebuild full from rows via the same
    # path is heavy; instead attach every scored trow captured below.
    turn_map = census.pop("_turn_full", turn_map)
    flag_set = {r["t"] for r in census["careful"]}
    matrix = build_matrix(rows_by_t, uni, turn_map, flag_set)
    S3.put_object(Bucket=BUCKET, Key=MATRIX_KEY,
                  Body=json.dumps(matrix, separators=(",", ":"),
                                  default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    doc = {"ok": True, "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": rnd(time.time() - t0, 1),
           "cadence": "1st + 15th monthly, 06:00 UTC",
           **census}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    try:
        h = json.loads(S3.get_object(Bucket=BUCKET, Key=HIST_KEY)
                       ["Body"].read())
    except Exception:  # noqa: BLE001
        h = {"rows": []}
    today = datetime.now(timezone.utc).date().isoformat()
    row = {"date": today, "avg_score": doc["summary"]["avg_score"],
           "scored": doc["coverage"]["scored"],
           "top1": (doc["top_quality"] or [{}])[0].get("t")}
    if h["rows"] and h["rows"][-1].get("date") == today:
        h["rows"][-1] = row
    else:
        h["rows"].append(row)
    h["rows"] = h["rows"][-60:]
    S3.put_object(Bucket=BUCKET, Key=HIST_KEY,
                  Body=json.dumps(h).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    print(json.dumps({"scored": doc["coverage"]["scored"],
                      "top3": [r["t"] for r in doc["top_quality"][:3]],
                      "n_flagged": doc["summary"]["n_flagged"]}))
    return doc
