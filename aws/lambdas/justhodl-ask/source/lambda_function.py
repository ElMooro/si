"""justhodl-ask — natural-language query over the entire JustHodl system.

A user asks a plain-English question ("show me cheap small-caps institutions
are buying that aren't falling knives"). This engine:
  1. Loads a compact, current snapshot of the key signal datasets the platform
     already produces (opportunities, dislocations, compounders, capital-flow,
     best-setups, bond-vol regime).
  2. Hands Claude the question + that grounded context.
  3. Returns a direct, CITED answer — specific tickers with the data-backed
     reason for each (the explainability layer, surfaced conversationally).

It answers ONLY from the platform's own data (no hallucinated tickers), and
every claim is tied to a signal/metric. Invoked via Function URL / worker proxy.

Guardrails: research/analytics only, never personalized financial advice.
"""
import anthropic_shim  # resilient LLM fallback (Anthropic->GLM via llm_router)
import json, os, re, time, urllib.request
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
MODEL = "claude-haiku-4-5-20251001"
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def read_json(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default


def anthropic_key():
    if os.environ.get("ANTHROPIC_API_KEY"): return os.environ["ANTHROPIC_API_KEY"]
    for p in ["/justhodl/anthropic/api_key", "/anthropic/api_key"]:
        try: return ssm.get_parameter(Name=p, WithDecryption=True)["Parameter"]["Value"]
        except Exception: continue
    return ""


def call_claude(system, prompt, max_tokens=1100):
    key = anthropic_key()
    if not key: return ""
    body = {"model": MODEL, "max_tokens": max_tokens, "system": system,
            "messages": [{"role": "user", "content": prompt}]}
    req = urllib.request.Request("https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "anthropic-version": "2023-06-01", "x-api-key": key},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=50) as r:
            data = json.loads(r.read().decode())
        return "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text").strip()
    except Exception as e:
        print(f"[ask] claude err: {e}"); return ""


def build_context():
    """Compact, current snapshot of the platform's signal datasets."""
    ctx = {}
    bs = read_json("data/best-setups.json") or {}
    ctx["todays_setups"] = {
        "bond_vol_regime": bs.get("bond_vol_regime"),
        "quad_threats": [_slim(s) for s in (bs.get("quad_threats") or [])[:8]],
        "triple_threats": [_slim(s) for s in (bs.get("triple_threats") or [])[:10]],
        "top": [_slim(s) for s in (bs.get("top_setups") or [])[:25]],
    }
    opp = read_json("data/opportunities.json") or {}
    allr = opp.get("all") or []
    comp = sorted([r for r in allr if (r.get("compounder_score") or 0) >= 70],
                  key=lambda r: -(r.get("compounder_score") or 0))[:25]
    ctx["compounders"] = [{"t": r.get("ticker"), "cap": r.get("cap_bucket"),
                            "compounder": r.get("compounder_score"),
                            "verdict": r.get("verdict"),
                            "exp_growth": (r.get("growth_intel") or {}).get("expected_company_growth_pct"),
                            "fwd_peg": (r.get("growth_intel") or {}).get("peg_forward"),
                            "pe_vs_industry": (r.get("growth_intel") or {}).get("pe_vs_industry_pct"),
                            "cheap_and_inflecting": r.get("cheap_and_inflecting")} for r in comp]
    disl = read_json("data/dislocations.json") or {}
    ctx["dislocations"] = [{"t": d.get("ticker"), "cap": d.get("cap_bucket"),
                             "industry": d.get("industry"), "score": d.get("dislocation_score"),
                             "ev_sales": d.get("ev_sales"), "rule_of_40": d.get("rule_of_40"),
                             "cheap_and_inflecting": d.get("cheap_and_inflecting"),
                             "vs": (d.get("dislocated_vs") or {}).get("ticker"),
                             "vs_premium_pct": (d.get("dislocated_vs") or {}).get("ev_sales_premium_pct"),
                             "caveats": d.get("caveats")} for d in (disl.get("buy_the_laggard") or [])[:25]]
    ctx["cheap_and_inflecting"] = [{"t": d.get("ticker"), "score": d.get("dislocation_score"),
                                     "mom_20d": (d.get("momentum") or {}).get("ret_20d")}
                                    for d in (disl.get("cheap_and_inflecting") or [])[:15]]
    cf = read_json("data/capital-flow.json") or {}
    ctx["capital_accumulating"] = [{"t": x.get("ticker"), "name": x.get("name"),
                                     "sector": x.get("sector"), "flow_score": x.get("flow_score"),
                                     "lenses": x.get("lenses")} for x in (cf.get("accumulating") or [])[:20]]
    ctx["capital_distributing"] = [{"t": x.get("ticker"), "flow_score": x.get("flow_score")}
                                    for x in (cf.get("distributing") or [])[:10]]
    ctx["etf_flows"] = [{"t": e.get("ticker"), "cat": e.get("category"),
                          "net_flow_5d": e.get("net_flow_5d_usd")} for e in (cf.get("etf_flows_in") or [])[:10]]
    bv = read_json("data/bond-vol.json") or {}
    ctx["bond_vol"] = {"regime": bv.get("regime"), "z": bv.get("composite_z_score"),
                        "risk_posture": bv.get("risk_posture"),
                        "term_structure": (bv.get("term_structure") or {}).get("signal"),
                        "playbook": bv.get("playbook")}
    da = read_json("data/dislocation-ai.json") or {}
    ctx["ai_theses"] = {tk: {"verdict": v.get("cheap_verdict"), "summary": v.get("summary"),
                              "pt": v.get("price_target_12m"), "theme": v.get("theme")}
                        for tk, v in list((da.get("by_ticker") or {}).items())[:15]}
    brain = read_json("data/brain.json") or {}
    ctx["_brain"] = {"prompt_block": brain.get("prompt_block"), "tickers": brain.get("mentioned_tickers"),
                     "directive": brain.get("directive")}
    return ctx


def _slim(s):
    return {"t": s.get("ticker"), "conviction": s.get("conviction"), "verdict": s.get("verdict"),
            "signals": s.get("signal_keys"), "thesis": (s.get("thesis") or "")[:160]}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    # parse the question from Function URL / API GW / direct invoke
    q = ""
    if isinstance(event, dict):
        if event.get("body"):
            try: q = (json.loads(event["body"]) or {}).get("q", "")
            except Exception: q = ""
        q = q or (event.get("queryStringParameters") or {}).get("q", "") or event.get("q", "")
    q = (q or "").strip()[:400]
    cors = {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS", "Content-Type": "application/json"}
    if isinstance(event, dict) and event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS":
        return {"statusCode": 200, "headers": cors, "body": "{}"}
    if not q:
        return {"statusCode": 400, "headers": cors, "body": json.dumps({"error": "Ask a question via ?q= or {\"q\":...}"})}

    ctx = build_context()
    brain_block = (ctx.get("_brain") or {}).get("prompt_block")
    brain_directive = ""
    if brain_block:
        brain_directive = (
            " IMPORTANT — THE USER'S OWN INVESTING PHILOSOPHY is provided in the data under "
            "'_brain.prompt_block'. Answer THROUGH THAT LENS: honor their pinned principles, "
            "rules, and stated preferences; frame the answer the way THEY think about markets; "
            "and when their philosophy is relevant to the question, reference which principle "
            "applies. Their rules override generic framing (e.g. if they say 'never confuse QT "
            "ending with QE', don't treat QT-ending as bullish)."
        )
    system = (
        "You are JustHodl AI's analyst assistant. Answer the user's question USING ONLY the "
        "JSON data provided (it is the live output of the platform's signal engines)."
        + brain_directive +
        " RULES: (1) Only name tickers that appear in the data — never invent. (2) For every "
        "ticker you cite, give the specific data-backed reason (the signal + the metric, e.g. "
        "'compounder 88, est revised +3pp, institutions accumulating via 13F'). (3) Respect the "
        "bond-vol risk regime in your framing. (4) Be concise and scannable — lead with the direct "
        "answer, then a short ranked list with reasons. (5) This is research/analytics, NOT "
        "personalized financial advice; do not tell the user to buy/sell — present the data and "
        "let them decide. (6) If the data doesn't contain an answer, say so plainly. "
        "Return STRICT JSON: {\"answer\": \"<markdown prose, 1-3 short paragraphs>\", "
        "\"results\": [{\"ticker\": \"X\", \"reason\": \"<data-cited one-liner>\", \"tags\": [\"signal keys\"]}], "
        "\"caveat\": \"<one-line risk/regime caveat or null>\"}"
    )
    prompt = f"USER QUESTION:\n{q}\n\nLIVE SYSTEM DATA:\n{json.dumps(ctx, default=str)[:14000]}"
    raw = call_claude(system, prompt)
    parsed = None
    if raw:
        cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip())
        cleaned = re.sub(r"\s*```$", "", cleaned).strip()
        m = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if m:
            try: parsed = json.loads(m.group(0))
            except Exception: parsed = None
    if not parsed:
        parsed = {"answer": raw or "I couldn't generate an answer right now — try rephrasing.",
                  "results": [], "caveat": None}
    parsed["question"] = q
    parsed["bond_vol_regime"] = ctx.get("bond_vol", {}).get("regime")
    parsed["generated_at"] = datetime.now(timezone.utc).isoformat()
    parsed["disclaimer"] = "Research & analytics, not investment advice."
    print(f"[ask] '{q[:60]}' -> {len(parsed.get('results', []))} results in {round(time.time()-t0,1)}s")
    return {"statusCode": 200, "headers": cors, "body": json.dumps(parsed, default=str)}
