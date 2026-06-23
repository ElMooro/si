"""
justhodl-page-ai  ·  v1.0  —  UNIVERSAL PER-PAGE AI (explain + analyze + grounded outlook)
================================================================================
Gives EVERY page (331 of them) an AI panel that:
  1. EXPLAINS what the page is and what it does (cached — rarely changes).
  2. ANALYZES the page's live data (what the current picks/metrics are saying).
  3. OUTLOOK — how the picks are likely to perform, GROUNDED IN THE SCORECARD'S
     REAL FORWARD-RETURN DATA, not an LLM-invented number. For each page we map
     its producing engine -> signal-scorecard -> {hit_rate, alpha_mean_excess_pct
     (mean excess vs SPY = the honest 'boom %'), alpha_status, grade, n}. The LLM
     phrases it; the numbers come from the grading system. A page whose engine is
     ALPHA_NEGATIVE is honestly labeled low-confidence rather than hyped.

Paced: cursor + wall-clock budget, so each invocation advances a wave through the
331-page list and wraps around (schedule every few hours -> full daily coverage).
Explanations cached in S3 so steady-state cost is just the data read. GLM backend
(Anthropic credits are out); fault-tolerant — a page that errors is retried next run.

Writes data/page-ai/{page}.json (NEW key — does not touch the 10 bespoke
ai-commentary pages). Rendered by the shared /jh-page-ai.js on every page.
"""
import json, time, urllib.request
from datetime import datetime, timezone
import boto3

VERSION = "1.0"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", "us-east-1")
WAVE_BUDGET_S = 200
WAVE_MAX_PAGES = 50


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


def build_scorecard_lookup():
    """signal_type -> grounded forward-return stats."""
    sc = _read("data/signal-scorecard.json") or {}
    rows = sc.get("scorecard") or sc.get("rows") or []
    look = {}
    for r in rows:
        st = r.get("signal_type")
        if not st: continue
        look[st] = {
            "hit_rate": r.get("hit_rate"), "n": r.get("alpha_n") or r.get("n_scored"),
            "mean_excess_pct": r.get("alpha_mean_excess_pct"), "avg_return_pct": r.get("avg_return_pct"),
            "alpha_status": r.get("alpha_status"), "grade": r.get("grade"),
            "edge_vs_coinflip_pct": r.get("edge_vs_coinflip_pct"),
        }
    return look


def grounded_outlook(engines, data_files, sc_look):
    """Map a page's engine(s)/feeds to the scorecard -> honest performance expectation."""
    for cand in list(engines) + [f"eng:{d.split('/')[-1]}" for d in data_files] + \
                [d.split('/')[-1] for d in data_files] + [e.replace("justhodl-", "") for e in engines] + \
                [f"eng:{e.replace('justhodl-','')}" for e in engines]:
        s = sc_look.get(cand)
        if s and s.get("mean_excess_pct") is not None:
            status = s.get("alpha_status") or "UNGRADED"
            hr = s.get("hit_rate")
            return {
                "matched_key": cand, "alpha_status": status, "grade": s.get("grade"),
                "n_graded": s.get("n"), "hit_rate_pct": round(hr * 100, 1) if isinstance(hr, (int, float)) else None,
                "mean_excess_vs_spy_pct": s.get("mean_excess_pct"),
                "edge_vs_coinflip_pct": s.get("edge_vs_coinflip_pct"),
                "confidence": ("HIGH" if status == "ALPHA_PROVEN" else
                               "LOW — historically did NOT beat SPY net of cost" if status == "ALPHA_NEGATIVE" else
                               "BUILDING — not enough graded outcomes yet"),
            }
    return {"alpha_status": "UNGRADED", "confidence": "BUILDING — this engine's picks are still being graded forward",
            "n_graded": 0, "note": "No scorecard track record yet; outlook is qualitative only."}


def gen_page_ai(page, meta, sc_look, explain_cache):
    from llm_router import complete
    title = meta.get("title") or page
    data_files = meta.get("data_files") or []
    engines = meta.get("engines") or []
    sample = ""
    if data_files:
        d = _read(data_files[0] if data_files[0].startswith("data/") else "data/" + data_files[0] + ".json") \
            or _read("data/" + data_files[0] + ".json")
        if d is not None:
            sample = json.dumps(d, default=str)[:2600]

    outlook = grounded_outlook(engines, data_files, sc_look)
    cached = explain_cache.get(page) or {}
    need_explain = not cached.get("what_it_is")

    # one GLM call; explanation parts only when not cached
    parts = ['"analysis": "3-4 sentences reading the CURRENT data — the actual top picks/metrics shown, what they signal right now"',
             '"pick_read": "2-3 sentences on the specific named picks (tickers) and the setup, or \'no single-name picks on this page\' if none"']
    if need_explain:
        parts = ['"what_it_is": "1-2 sentences: what this page shows"',
                 '"what_it_does": "1-2 sentences: how the engine behind it works / what edge it looks for"'] + parts
    sys = ("You are JustHodl.AI's analyst writing a short per-page AI brief for an institutional dashboard. "
           "Be specific and concrete; cite the actual tickers/metrics in the data. No hedging filler, no markdown. "
           "Do NOT predict specific return percentages — a separate grounded-outlook system supplies those from the "
           "scorecard. Return ONLY a JSON object with these keys:\n{" + ", ".join(parts) + "}")
    prompt = f"PAGE: {title} ({page})\n"
    if outlook.get("alpha_status") and outlook["alpha_status"] != "UNGRADED":
        prompt += (f"SCORECARD (for your awareness, don't restate numbers): this engine is {outlook['alpha_status']}, "
                   f"hit rate {outlook.get('hit_rate_pct')}%, mean excess vs SPY {outlook.get('mean_excess_vs_spy_pct')}%.\n")
    prompt += f"LIVE DATA SAMPLE:\n{sample or '(no data feed on this page)'}"

    out = {"page": page, "title": title, "generated_at": datetime.now(timezone.utc).isoformat(),
           "outlook": outlook, "has_data": bool(sample)}
    try:
        res = _parse_json(complete(prompt, tier="reason", max_tokens=900, system=sys))
    except Exception as e:
        res = {}
        out["llm_error"] = str(e)[:60]
    if need_explain:
        if res.get("what_it_is"):
            cached = {"what_it_is": res.get("what_it_is"), "what_it_does": res.get("what_it_does")}
            explain_cache[page] = cached
    out["what_it_is"] = cached.get("what_it_is") or res.get("what_it_is")
    out["what_it_does"] = cached.get("what_it_does") or res.get("what_it_does")
    out["analysis"] = res.get("analysis")
    out["pick_read"] = res.get("pick_read")
    return out


def lambda_handler(event, context):
    t0 = time.time()
    manifest = _read("data/page-ai-manifest.json") or {}
    pages = list(manifest.keys())
    if not pages:
        return {"statusCode": 500, "body": "no manifest"}
    sc_look = build_scorecard_lookup()
    explain_cache = _read("data/_cache/page-ai-explain.json") or {}
    cur = (_read("data/_cache/page-ai-cursor.json") or {}).get("i", 0) % len(pages)

    done, errors, with_outlook = 0, 0, 0
    i = cur
    while done < WAVE_MAX_PAGES and (time.time() - t0) < WAVE_BUDGET_S:
        page = pages[i % len(pages)]
        try:
            o = gen_page_ai(page, manifest[page], sc_look, explain_cache)
            _write(f"data/page-ai/{page}.json", o)
            if o.get("outlook", {}).get("alpha_status", "UNGRADED") not in ("UNGRADED",): with_outlook += 1
            if o.get("llm_error"): errors += 1
        except Exception as e:
            errors += 1
            print(f"[page-ai] {page} failed: {str(e)[:60]}")
        done += 1
        i += 1
        if i % len(pages) == cur: break   # wrapped fully

    _write("data/_cache/page-ai-explain.json", explain_cache, cache="no-cache")
    _write("data/_cache/page-ai-cursor.json", {"i": i % len(pages), "updated": datetime.now(timezone.utc).isoformat()}, cache="no-cache")
    elapsed = round(time.time() - t0, 1)
    print(f"[page-ai v{VERSION}] wave: {done} pages ({with_outlook} with grounded outlook, {errors} errors) "
          f"in {elapsed}s · cursor {cur}->{i % len(pages)} of {len(pages)} · cached explanations {len(explain_cache)}")
    return {"statusCode": 200, "body": json.dumps({"processed": done, "errors": errors,
            "with_outlook": with_outlook, "cursor": i % len(pages), "total_pages": len(pages)})}
