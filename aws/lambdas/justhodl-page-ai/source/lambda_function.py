"""
justhodl-page-ai  ·  v1.1  —  UNIVERSAL PER-PAGE AI (explain + analyze + grounded outlook)
================================================================================
Every page (331) gets an AI panel: (1) EXPLAINS what it is / how it works (cached),
(2) ANALYZES the live data, (3) OUTLOOK grounded in signal-scorecard REAL forward
returns (hit_rate, mean excess vs SPY = honest 'boom %', alpha_status) — never an
LLM-invented number. Threaded GLM calls + cursor + wall-clock budget -> a wave per
invocation; explanations cached; ALPHA_NEGATIVE engines honestly flagged low-confidence.
Writes data/page-ai/{page}.json (rendered by shared /jh-page-ai.js). v1.1: parallel
GLM (6 workers), retry-on-empty, normalized scorecard matching.
"""
import json, time, urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

VERSION = "1.1"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", "us-east-1")
WAVE_BUDGET_S = 240
WAVE_MAX_PAGES = 42
WORKERS = 6


def _read(key):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return None

def _write(key, obj, cache="public, max-age=900"):
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(obj, default=str).encode(),
                  ContentType="application/json", CacheControl=cache)

def _parse_json(txt):
    if not txt: return {}
    import re
    t = txt.strip()
    if "```" in t:
        m = re.search(r"```(?:json)?\s*(.*?)```", t, re.S)
        if m: t = m.group(1).strip()
    a, b = t.find("{"), t.rfind("}")
    if a >= 0 and b > a:
        try: return json.loads(t[a:b + 1])
        except Exception: return {}
    return {}

def _norm(s):
    return str(s).lower().replace("eng:", "").replace("justhodl-", "").replace(".json", "").replace("-", "").replace("_", "").strip()


def build_scorecard_lookup():
    sc = _read("data/signal-scorecard.json") or {}
    rows = sc.get("scorecard") or sc.get("rows") or []
    look, norm = {}, {}
    for r in rows:
        st = r.get("signal_type")
        if not st: continue
        stats = {
            "hit_rate": r.get("hit_rate"), "n": r.get("alpha_n") or r.get("n_scored"),
            "mean_excess_pct": r.get("alpha_mean_excess_pct"), "avg_return_pct": r.get("avg_return_pct"),
            "alpha_status": r.get("alpha_status"), "grade": r.get("grade"),
            "edge_vs_coinflip_pct": r.get("edge_vs_coinflip_pct"),
        }
        look[st] = stats; norm[_norm(st)] = stats
    return look, norm


def grounded_outlook(engines, data_files, look, norm):
    cands = list(engines) + [d.split("/")[-1] for d in data_files] + [f"eng:{d.split('/')[-1]}" for d in data_files]
    # exact first, then normalized
    for c in cands:
        s = look.get(c)
        if s and s.get("mean_excess_pct") is not None: return _fmt_outlook(s, c)
    for c in cands:
        s = norm.get(_norm(c))
        if s and s.get("mean_excess_pct") is not None: return _fmt_outlook(s, c)
    return {"alpha_status": "UNGRADED", "n_graded": 0,
            "confidence": "BUILDING — this engine's picks are still being graded forward",
            "note": "No scorecard track record yet; the outlook is qualitative until forward outcomes accrue."}

def _fmt_outlook(s, matched):
    status = s.get("alpha_status") or "UNGRADED"
    hr = s.get("hit_rate")
    return {"matched_key": matched, "alpha_status": status, "grade": s.get("grade"),
            "n_graded": s.get("n"), "hit_rate_pct": round(hr * 100, 1) if isinstance(hr, (int, float)) else None,
            "mean_excess_vs_spy_pct": s.get("mean_excess_pct"), "edge_vs_coinflip_pct": s.get("edge_vs_coinflip_pct"),
            "confidence": ("HIGH — beat SPY net of cost across graded picks" if status == "ALPHA_PROVEN" else
                           "LOW — historically did NOT beat SPY net of cost" if status == "ALPHA_NEGATIVE" else
                           "BUILDING — not enough graded outcomes yet")}


def gen_page_ai(page, meta, look, norm, explain_existing):
    from llm_router import complete
    title = meta.get("title") or page
    data_files = meta.get("data_files") or []
    engines = meta.get("engines") or []
    sample = ""
    if data_files:
        d = _read("data/" + data_files[0] + ".json")
        if d is not None: sample = json.dumps(d, default=str)[:2600]

    outlook = grounded_outlook(engines, data_files, look, norm)
    need_explain = not (explain_existing or {}).get("what_it_is")
    parts = ['"analysis": "3-4 sentences reading the CURRENT data — the actual top picks/metrics shown and what they signal now"',
             '"pick_read": "2-3 sentences on the specific named picks (tickers) + setup, or \'no single-name picks on this page\'"']
    if need_explain:
        parts = ['"what_it_is": "1-2 sentences: what this page shows"',
                 '"what_it_does": "1-2 sentences: how the engine behind it works / what edge it seeks"'] + parts
    sys = ("You are JustHodl.AI's analyst writing a short per-page AI brief for an institutional dashboard. Be specific; "
           "cite the actual tickers/metrics in the data. No hedging filler, no markdown. Do NOT predict specific return "
           "percentages — a separate grounded system supplies those from the scorecard. Return ONLY a JSON object:\n{"
           + ", ".join(parts) + "}")
    prompt = f"PAGE: {title} ({page})\nLIVE DATA SAMPLE:\n{sample or '(no data feed on this page)'}"

    res = {}
    for attempt in range(2):
        try:
            res = _parse_json(complete(prompt, tier="reason", max_tokens=850, system=sys))
        except Exception:
            res = {}
        if res.get("analysis") or res.get("what_it_is"): break

    out = {"page": page, "title": title, "generated_at": datetime.now(timezone.utc).isoformat(),
           "outlook": outlook, "has_data": bool(sample)}
    upd = None
    if need_explain and res.get("what_it_is"):
        upd = {"what_it_is": res.get("what_it_is"), "what_it_does": res.get("what_it_does")}
    ex = explain_existing or {}
    out["what_it_is"] = ex.get("what_it_is") or res.get("what_it_is")
    out["what_it_does"] = ex.get("what_it_does") or res.get("what_it_does")
    out["analysis"] = res.get("analysis")
    out["pick_read"] = res.get("pick_read")
    return out, upd


def lambda_handler(event, context):
    t0 = time.time()
    manifest = _read("data/page-ai-manifest.json") or {}
    pages = list(manifest.keys())
    if not pages: return {"statusCode": 500, "body": "no manifest"}
    look, norm = build_scorecard_lookup()
    explain_cache = _read("data/_cache/page-ai-explain.json") or {}
    cur = (_read("data/_cache/page-ai-cursor.json") or {}).get("i", 0) % len(pages)

    wave = [pages[(cur + k) % len(pages)] for k in range(min(WAVE_MAX_PAGES, len(pages)))]
    done = with_outlook = errors = 0
    ex = ThreadPoolExecutor(max_workers=WORKERS)
    futs = {ex.submit(gen_page_ai, pg, manifest[pg], look, norm, explain_cache.get(pg)): pg for pg in wave}
    for f in as_completed(futs):
        pg = futs[f]
        try:
            out, upd = f.result()
            _write(f"data/page-ai/{pg}.json", out)
            if upd: explain_cache[pg] = upd
            if out.get("outlook", {}).get("alpha_status") not in ("UNGRADED", None): with_outlook += 1
            done += 1
        except Exception as e:
            errors += 1; print(f"[page-ai] {pg}: {str(e)[:60]}")
        if (time.time() - t0) > WAVE_BUDGET_S: break
    ex.shutdown(wait=False, cancel_futures=True)

    _write("data/_cache/page-ai-explain.json", explain_cache, cache="no-cache")
    new_cur = (cur + done) % len(pages)
    _write("data/_cache/page-ai-cursor.json", {"i": new_cur, "updated": datetime.now(timezone.utc).isoformat()}, cache="no-cache")
    elapsed = round(time.time() - t0, 1)
    print(f"[page-ai v{VERSION}] {done} pages ({with_outlook} graded-outlook, {errors} err) {elapsed}s · cursor {cur}->{new_cur}/{len(pages)} · cached {len(explain_cache)}")
    return {"statusCode": 200, "body": json.dumps({"processed": done, "with_outlook": with_outlook,
            "errors": errors, "cursor": new_cur, "total_pages": len(pages)})}
