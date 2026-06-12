"""
justhodl-research-papers v1.0 — AI research notes on the most underlooked names
===============================================================================
Daily (15:05 UTC, after stock-valuations): takes the top UNDERLOOKED candidates,
assembles a strict numeric dossier from existing desk feeds (HP metrics/cats/
pillars/class, valuation row, FMP profile, sector rank, macro canary line),
and has Claude write a structured buy-side research note — thesis, why it's
underlooked, financials, valuation case, catalysts, risks, bear case, what
breaks it, verdict, conviction 1-10. Numbers ONLY from the dossier (the prompt
forbids invention; absent data must be called 'not in dossier').
Each paper logs research_paper to the graded closed loop -> theses get scored.
Output: data/research/{TICKER}.json + index data/research-papers.json
"""
import json, os, time, urllib.request
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
STATE_KEY = "data/_research/state.json"
INDEX_KEY = "data/research-papers.json"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
MODELS = ["claude-sonnet-4-6", "claude-haiku-4-5-20251001"]
PAPERS_PER_RUN = 3
VERSION = "2.0.2"
DIAG = []

BULL_SYS = (
  "You are the BULL analyst in an internal investment-desk debate on an underlooked "
  "small/mid-cap. Build the STRONGEST affirmative case — but every figure must appear "
  "verbatim in the DOSSIER; absent data must be called 'not in dossier'; never invent "
  "facts. Respond ONLY with a JSON object: bull_thesis (string), strongest_points "
  "(array of 3-5 strings, each citing dossier numbers), what_the_market_misses "
  "(string), upside_framework (string).")

BEAR_SYS = (
  "You are the BEAR analyst in an internal investment-desk debate. You receive the "
  "dossier AND the bull case. Your job is to ATTACK it: find what the bull ignored, "
  "stretched, or got wrong — using ONLY dossier figures; absent data must be called "
  "'not in dossier'. Be ruthless but honest: concede genuinely strong points. Respond "
  "ONLY with a JSON object: bear_rebuttal (string), strongest_attacks (array of 3-5 "
  "strings, each citing dossier numbers), what_the_bull_ignores (string), "
  "kill_conditions (array of 2-4 strings: observable events that would end the thesis).")

JUDGE_SYS = (
  "You are the PORTFOLIO MANAGER judging an internal bull/bear debate and writing "
  "the final research note on an underlooked small/mid-cap. You receive the dossier, "
  "the bull case, and the bear rebuttal. Your conviction MUST reflect the bear's best "
  "surviving attacks - do not award high conviction to a thesis the bear wounded. "
  "internal research note on an UNDERLOOKED small/mid-cap stock. STRICT RULES: "
  "every figure you cite must appear verbatim in the DOSSIER; if a datum you want "
  "is absent, write 'not in dossier' rather than estimating; never invent "
  "customers, contracts, events, or numbers; acknowledge weak spots honestly; "
  "this is research for an internal desk, not investment advice. Respond with "
  "ONLY a single JSON object (no markdown fences, no prose outside JSON) with "
  "exactly these keys: title (string), one_line_thesis, business_overview, "
  "why_underlooked, financial_analysis, valuation_case, catalysts (array of "
  "strings), risks (array of strings), bear_case, what_would_break_the_thesis, "
  "verdict, conviction_1_10 (integer 1-10), debate_verdict (string: who argued "
  "better and why), points_conceded_to_bear (array of strings), "
  "position_stance (one of: BUY-WATCH, RESEARCH-MORE, PASS).")


def jget(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl Research admin@justhodl.ai"})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def s3j(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def call_claude(user_text, system, max_tokens=3500):
    last = None
    for model in MODELS:
        try:
            payload = json.dumps({"model": model, "max_tokens": max_tokens,
                                   "system": system,
                                   "messages": [{"role": "user", "content": user_text}]}).encode()
            req = urllib.request.Request(
                "https://api.anthropic.com/v1/messages", data=payload,
                headers={"Content-Type": "application/json",
                          "x-api-key": ANTHROPIC_KEY,
                          "anthropic-version": "2023-06-01"})
            r = json.loads(urllib.request.urlopen(req, timeout=120).read())
            txt = "".join(b.get("text", "") for b in r.get("content", [])
                           if b.get("type") == "text").strip()
            if txt.startswith("```"):
                txt = txt.strip("`")
                if txt.lower().startswith("json"):
                    txt = txt[4:]
            try:
                return json.loads(txt.strip()), model
            except json.JSONDecodeError as je:
                # one repair round: send the broken output back for correction
                rep = json.dumps({"model": model, "max_tokens": max_tokens,
                                    "system": "You repair invalid JSON. Respond with ONLY "
                                               "the corrected JSON object, nothing else.",
                                    "messages": [{"role": "user", "content":
                                      f"This JSON is invalid ({je}). Fix and return it:\n"
                                      + txt[:8000]}]}).encode()
                rq = urllib.request.Request(
                    "https://api.anthropic.com/v1/messages", data=rep,
                    headers={"Content-Type": "application/json",
                              "x-api-key": ANTHROPIC_KEY,
                              "anthropic-version": "2023-06-01"})
                r2 = json.loads(urllib.request.urlopen(rq, timeout=90).read())
                t2 = "".join(b.get("text", "") for b in r2.get("content", [])
                              if b.get("type") == "text").strip()
                if t2.startswith("```"):
                    t2 = t2.strip("`")
                    if t2.lower().startswith("json"):
                        t2 = t2[4:]
                return json.loads(t2.strip()), model + "+repair"
        except Exception as e:
            last = e
            continue
    raise RuntimeError(f"all models failed: {str(last)[:80]}")


def gather(t, slim_row, sv, sec_rank, macro_line):
    hp = next((x for x in (sv.get("hp") or []) if x.get("t") == t), {})
    sp = next((x for x in (sv.get("sp_table") or []) if x.get("t") == t), None)
    prof = {}
    try:
        j = jget(f"https://financialmodelingprep.com/stable/profile?symbol={t}&apikey={FMP_KEY}")
        if isinstance(j, list) and j:
            p0 = j[0]
            prof = {"name": p0.get("companyName"), "industry": p0.get("industry"),
                     "sector": p0.get("sector"), "price": p0.get("price"),
                     "market_cap": p0.get("marketCap") or p0.get("mktCap"),
                     "employees": p0.get("fullTimeEmployees"),
                     "description": (p0.get("description") or "")[:1200]}
    except Exception:
        pass
    return {"ticker": t, "profile": prof,
             "underlooked_row": slim_row,
             "hp_score": hp.get("score"), "hp_class": hp.get("hp_class"),
             "category_scores_0_10": hp.get("cats"), "pillars": hp.get("pillars"),
             "hard_flags": hp.get("flags"), "soft_flags": hp.get("soft_flags"),
             "key_metrics": hp.get("metrics"), "chart": hp.get("chart_detail"),
             "sp500_valuation_row": sp,
             "sector_1m_rank_of_11": sec_rank.get(slim_row.get("sector")),
             "macro_context": macro_line,
             "methodology_note": ("underlooked score = smallness + low trading "
                                   "attention + fundamental strength + value + "
                                   "basing chart; hp_score = 10x10 rubric")}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    DIAG.clear()
    sv = s3j("data/stock-valuations.json")
    cands = sv.get("underlooked_top") or []
    if not cands:
        DIAG.append("no underlooked candidates yet")
    st = s3j(STATE_KEY) or {}
    done = st.get("done") or {}
    index = st.get("index") or []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).date().isoformat()
    sec_rank = {}
    for g in (s3j("data/sector-groups.json").get("groups") or []):
        sec_rank[g.get("sector")] = g.get("rank_1m")
    cc = s3j("data/crisis-canaries.json")
    macro_line = (f"crisis composite v3 {cc.get('composite_v3')} ({cc.get('level_v3')}), "
                   f"{cc.get('red_count')} red canaries of {cc.get('n_global')}")
    todo = [c for c in cands if (done.get(c["t"]) or "") < cutoff][:PAPERS_PER_RUN]
    written = []
    TIME_BUDGET = 360
    for c in todo:
        if time.time() - t0 > TIME_BUDGET:
            DIAG.append(f"time budget {TIME_BUDGET}s reached — remaining papers roll "
                         "to next daily run")
            break
        t = c["t"]
        try:
            dossier = gather(t, c, sv, sec_rank, macro_line)
            dj = json.dumps(dossier, default=str)
            bull, m1 = call_claude(f"DOSSIER for {t}:\n{dj}\n\nBuild the bull case.",
                                    BULL_SYS, 1400)
            bear, m2 = call_claude(f"DOSSIER for {t}:\n{dj}\n\nBULL CASE:\n"
                                    + json.dumps(bull) + "\n\nAttack it.",
                                    BEAR_SYS, 1400)
            paper, model = call_claude(f"DOSSIER for {t}:\n{dj}\n\nBULL CASE:\n"
                                        + json.dumps(bull) + "\n\nBEAR REBUTTAL:\n"
                                        + json.dumps(bear)
                                        + "\n\nJudge the debate and write the final "
                                          "research note now.",
                                        JUDGE_SYS, 3000)
            paper["debate"] = {"bull": bull, "bear": bear,
                                "models": {"bull": m1, "bear": m2, "judge": model}}
            conv = paper.get("conviction_1_10")
            conv = max(1, min(10, int(conv))) if isinstance(conv, (int, float)) else 5
            paper["conviction_1_10"] = conv
            body = {"ticker": t, "name": (dossier.get("profile") or {}).get("name"),
                     "generated_at": datetime.now(timezone.utc).isoformat(),
                     "model_used": model, "engine_version": VERSION,
                     "dossier": dossier, "paper": paper}
            S3.put_object(Bucket=BUCKET, Key=f"data/research/{t}.json",
                          Body=json.dumps(body, default=str).encode(),
                          ContentType="application/json",
                          CacheControl="public, max-age=3600")
            today = datetime.now(timezone.utc).date().isoformat()
            done[t] = today
            index = [e for e in index if e.get("t") != t]
            index.insert(0, {"t": t, "name": body["name"],
                              "title": paper.get("title"),
                              "one_line_thesis": (paper.get("one_line_thesis") or "")[:180],
                              "conviction": conv, "date": today,
                              "stance": paper.get("position_stance"),
                              "key": f"data/research/{t}.json"})
            written.append(t)
            try:
                px = (dossier.get("profile") or {}).get("price")
                if px:
                    nowt = datetime.now(timezone.utc)
                    DDB.Table("justhodl-signals").put_item(Item={
                        "signal_id": f"research_paper#{t}#{today}",
                        "signal_type": "research_paper", "predicted_direction": "UP",
                        "confidence": Decimal(str(round(min(0.7, 0.4 + conv * 0.03), 2))),
                        "baseline_price": Decimal(str(round(float(px), 4))),
                        "measure_against": "ticker", "ticker": t, "benchmark": "SPY",
                        "horizon_days_primary": 63, "check_windows": [21, 63],
                        "status": "pending", "logged_epoch": int(nowt.timestamp()),
                        "ttl": int(nowt.timestamp()) + 150 * 86400,
                        "rationale": f"AI research note conviction {conv}/10: "
                                      + (paper.get("one_line_thesis") or "")[:160],
                    }, ConditionExpression="attribute_not_exists(signal_id)")
            except Exception as e:
                if "ConditionalCheckFailed" not in str(e):
                    DIAG.append(f"loop {t}: {str(e)[:40]}")
        except Exception as e:
            DIAG.append(f"{t}: {str(e)[:90]}")
    index = index[:60]
    S3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                  Body=json.dumps({"done": done, "index": index}).encode(),
                  ContentType="application/json")
    out = {"engine": "research-papers", "version": VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "duration_s": round(time.time() - t0, 1),
            "n_papers": len(index), "written_this_run": written,
            "papers": index, "diagnostics": list(DIAG),
            "methodology": ("Daily AI research notes on the top underlooked names "
                             "(stock-valuations underlooked_top). Dossier-locked: the "
                             "model may only cite numbers present in the dossier and "
                             "must say 'not in dossier' otherwise. TradingAgents-style bull/bear debate: a bull analyst builds the case, a bear analyst attacks it with the dossier, and a PM judge writes the final note with debate-tempered conviction, points conceded to the bear, and a stance (BUY-WATCH/RESEARCH-MORE/PASS). Conviction 1-10; "
                             "every note logs research_paper (UP, 63d) to the graded "
                             "closed loop so the theses are scored, not just written. "
                             "Research, not advice.")}
    S3.put_object(Bucket=BUCKET, Key=INDEX_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[research] wrote {written} · index {len(index)} · {out['duration_s']}s · {DIAG}")
    return {"statusCode": 200, "body": json.dumps({"written": written})}
