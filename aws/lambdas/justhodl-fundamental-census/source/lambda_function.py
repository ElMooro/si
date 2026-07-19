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

VERSION = "1.1.1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/fundamental-census.json"
MATRIX_KEY = "data/fundamental-census-matrix.json"
MX_EXCLUDE_PRE = ("px_", "rsi_", "vol", "est_")
HIST_KEY = "data/fundamental-census-history.json"
CACHE_TPL = "data/fundgraph/cache/{sym}_quarter_v21.json"
FG_FN = "justhodl-fundamental-graphs"
BATCH = 25

S3 = boto3.client("s3", region_name="us-east-1")
LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900,
                                 connect_timeout=10,
                                 retries={"max_attempts": 0}))

CORE_METRICS = [
    ("gross_margin_pct", "Gross margin %", "H"),
    ("ebitda_margin_pct", "EBITDA margin %", "H"),
    ("fcf_margin_pct", "FCF margin %", "H"),
    ("fcf_yield_pct", "FCF yield %", "H"),
    ("roe_pct", "Return on equity %", "H"),
    ("roa_pct", "Return on assets %", "H"),
    ("fcf_yoy_pct", "FCF growth YoY %", "H"),
    ("debt_to_equity", "Debt / equity", "L"),
    ("dio_days", "Inventory days", "L"),
    ("share_count_yoy_pct", "Share count YoY %", "L"),
]
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
    return {"t": doc.get("symbol") or doc.get("ticker"),
            "sector": sector, "score": score,
            "n_elite": len(elites), "n_green": len(greens),
            "n_red": len(reds), "sev_sum": sev,
            "top_elites": [v["k"] for v in elites[:3]],
            "red3": red3[:4], "flags": flags,
            "flag_w": flag_w, "dilution_yoy": dil,
            "metrics": met,
            "_lv": {k: rnd(last_val(P, k), 4) for k in P
                    if not any(k.startswith(pre)
                               for pre in MX_EXCLUDE_PRE)
                    and last_val(P, k) is not None},
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
        vals = [(r["t"], r["metrics"].get(k)) for r in rows_scored
                if r["metrics"].get(k) is not None]
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
    slim = ["t", "sector", "score", "n_elite", "n_green", "n_red",
            "top_elites", "red3", "flags", "dilution_yoy"]
    return {
        "top_quality": [{k: r[k] for k in slim} for r in top[:50]],
        "bottom_quality": [{k: r[k] for k in slim}
                           for r in top[-50:]][::-1],
        "careful": [{k: r[k] for k in slim + ["flag_w"]}
                    for r in careful],
        "metric_boards": boards, "sectors": sec_rows,
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


def build_matrix(rows_by_t, uni):
    """Columnar latest-value matrix over EVERY fundamentals metric
    present in >=50%% of scored docs (tech/price/estimate keys
    excluded) — the explorer's sorting substrate. rows_by_t entries
    carry _P (full points) attached by the aggregate loop."""
    scored = [u["t"] for u in uni if u["t"] in rows_by_t]
    sectors = [rows_by_t[t]["sector"] for t in scored]
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
    return {"generated_at": datetime.now(timezone.utc).isoformat(),
            "n_tickers": len(scored), "n_metrics": len(keys),
            "tickers": scored, "sectors": sectors,
            "metrics": keys, "cols": cols}


def lambda_handler(event, context):
    event = event or {}
    phase = event.get("phase") or "aggregate"
    uni = universe()
    if phase == "warm":
        cur = int(event.get("cursor") or 0)
        refresh = bool(event.get("refresh"))
        batch = [u["t"] for u in uni[cur:cur + BATCH]]
        if batch:
            try:
                LAM.invoke(FunctionName=FG_FN,
                           Payload=json.dumps(
                               {"warm": batch, "periods": ["quarter"],
                                "refresh": refresh}).encode())
            except Exception as e:  # noqa: BLE001
                print(f"[census] warm batch@{cur}: {str(e)[:80]}")
        nxt = cur + BATCH
        payload = ({"phase": "warm", "cursor": nxt, "refresh": refresh}
                   if nxt < len(uni) else {"phase": "aggregate"})
        LAM.invoke(FunctionName=context.function_name,
                   InvocationType="Event",
                   Payload=json.dumps(payload).encode())
        return {"ok": True, "phase": "warm", "cursor": cur,
                "n_batch": len(batch), "next": payload["phase"]}

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
    matrix = build_matrix(rows_by_t, uni)
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
