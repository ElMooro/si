"""
justhodl-kb-matcher v1.0 — Item 3: Crisis-KB Live Pattern Match
================================================================
The platform codified 20 crisis documents into 16 frameworks / 1,091 rules
(data/crisis-knowledge-base.json). This engine scores TODAY'S tape against
every framework, daily:

  "Closest framework: Oct-2018 Quantitative-Tightening (match 78%);
   next un-triggered rule in that playbook: HY OAS +40bp."

Schema-adaptive by design: if rules carry machine-evaluable conditions
({indicator, operator, threshold}) they are evaluated directly (method=
"machine"); otherwise a keyword+direction heuristic scores textual rules
(method="keyword-heuristic-v1") with coverage statistics — never faked
precision. Inputs are the platform's own live briefs + FRED.
"""
import json, os, re, time, urllib.request, urllib.parse
from datetime import datetime, timezone
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/kb-match.json"
KB_KEY = "data/crisis-knowledge-base.json"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
VERSION = "1.0.0"


def s3j(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def fred_last(sid, n=40):
    u = ("https://api.stlouisfed.org/fred/series/observations?"
         + urllib.parse.urlencode({"series_id": sid, "api_key": FRED_KEY, "file_type": "json",
                                   "sort_order": "desc", "limit": n}))
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=30).read())
        obs = [(o["date"], float(o["value"])) for o in j.get("observations", [])
               if o.get("value") not in (".", "", None)]
        obs.reverse()
        return obs
    except Exception as e:
        print(f"[fred] {sid}: {str(e)[:50]}")
        return []


def build_today_state():
    """Live indicator snapshot: value + 20-obs direction sign per indicator."""
    st = {}
    hy = fred_last("BAMLH0A0HYM2")
    if len(hy) > 5:
        st["hy_oas"] = {"value": hy[-1][1], "chg": round(hy[-1][1] - hy[max(0, len(hy) - 21)][1], 2)}
    cv = fred_last("T10Y2Y")
    if len(cv) > 5:
        st["curve_2s10s"] = {"value": cv[-1][1], "chg": round(cv[-1][1] - cv[max(0, len(cv) - 21)][1], 2)}
    dx = fred_last("DTWEXBGS")
    if len(dx) > 5:
        st["dollar"] = {"value": dx[-1][1], "chg": round(dx[-1][1] - dx[max(0, len(dx) - 21)][1], 2)}
    vs = s3j("data/vol-surface.json") or {}
    spot = (((vs.get("term") or {}).get("spot")) or vs.get("vix_spot") or
            ((vs.get("composite") or {}).get("vix")))
    if isinstance(spot, (int, float)):
        st["vix"] = {"value": spot, "chg": None}
    cn = s3j("data/crisis-canaries.json") or {}
    sf = ((cn.get("canaries") or {}).get("sofr_tail") or {})
    if sf.get("tail_bp") is not None:
        st["sofr_tail"] = {"value": sf["tail_bp"], "chg": sf.get("z")}
    ed = s3j("data/eurodollar-stress.json") or {}
    esc = ed.get("composite_score") or ed.get("score")
    if isinstance(esc, (int, float)):
        st["eurodollar_stress"] = {"value": esc, "chg": None}
    li = s3j("data/liquidity-inflection.json") or {}
    lz = (li.get("usd") or {}).get("impulse_z")
    if isinstance(lz, (int, float)):
        st["net_liquidity"] = {"value": lz, "chg": lz}
    au = s3j("data/auction-crisis.json") or {}
    ac = au.get("composite_score") or au.get("score")
    if isinstance(ac, (int, float)):
        st["auction_stress"] = {"value": ac, "chg": None}
    return st


# keyword → (state key, polarity meaning of "rising/widening")
KEYMAP = [
    (r"high.?yield|hy\b|credit spread|oas|junk", "hy_oas"),
    (r"yield curve|2s10s|inver|steepen|term spread", "curve_2s10s"),
    (r"\bvix\b|volatil", "vix"),
    (r"sofr|repo|collateral|funding", "sofr_tail"),
    (r"dollar|dxy|usd\b", "dollar"),
    (r"liquidit|qt\b|balance sheet|reserves", "net_liquidity"),
    (r"auction|treasury supply|bid.?cover|tail", "auction_stress"),
    (r"eurodollar|offshore dollar", "eurodollar_stress"),
]
RISE = r"widen|spike|surge|rise|rising|above|exceed|jump|elevat|stress|blow|break.?out|>"
FALL = r"compress|fall|falling|below|under|drop|declin|collaps|contract|inver|<"


def rule_texts(node, out):
    """Recursively harvest rule entries: dicts w/ condition fields or plain strings."""
    if isinstance(node, dict):
        ind, op, th = node.get("indicator"), node.get("operator") or node.get("op"), node.get("threshold")
        txt = node.get("rule") or node.get("text") or node.get("condition") or node.get("description")
        if ind is not None and op and th is not None:
            out.append({"kind": "machine", "indicator": str(ind), "op": str(op),
                        "threshold": th, "text": txt or f"{ind} {op} {th}"})
        elif isinstance(txt, str) and len(txt) > 12:
            out.append({"kind": "text", "text": txt})
        for v in node.values():
            rule_texts(v, out)
    elif isinstance(node, list):
        for v in node:
            rule_texts(v, out)
    elif isinstance(node, str) and len(node) > 25:
        out.append({"kind": "text", "text": node})


def eval_rule(r, state):
    """→ (status, why) status ∈ matched / unmatched / not_evaluable"""
    if r["kind"] == "machine":
        key = next((k for pat, k in KEYMAP if re.search(pat, r["indicator"].lower())), None)
        cur = (state.get(key) or {}).get("value") if key else None
        if cur is None:
            return "not_evaluable", "no live feed for indicator"
        try:
            th = float(r["threshold"])
        except Exception:
            return "not_evaluable", "non-numeric threshold"
        ok = {"<": cur < th, "<=": cur <= th, ">": cur > th, ">=": cur >= th,
              "==": abs(cur - th) < 1e-9}.get(r["op"].strip())
        if ok is None:
            return "not_evaluable", f"op {r['op']}"
        return ("matched" if ok else "unmatched",
                f"{key}={cur} {r['op']} {th}")
    t = r["text"].lower()
    key = next((k for pat, k in KEYMAP if re.search(pat, t)), None)
    if not key or key not in state:
        return "not_evaluable", "no indicator keyword"
    chg = state[key].get("chg")
    val = state[key].get("value")
    wants_rise, wants_fall = bool(re.search(RISE, t)), bool(re.search(FALL, t))
    if wants_rise == wants_fall:
        return "not_evaluable", "no direction keyword"
    metric = chg if chg is not None else val
    if metric is None:
        return "not_evaluable", "no live value"
    hit = (metric > 0) if wants_rise else (metric < 0)
    return ("matched" if hit else "unmatched",
            f"{key} {'chg' if chg is not None else 'level'}={metric:+.2f} vs wants {'rise' if wants_rise else 'fall'}")


def lambda_handler(event=None, context=None):
    t0 = time.time()
    kb = s3j(KB_KEY)
    if not kb:
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(
            {"engine": "kb-matcher", "version": VERSION, "error": "KB not found",
             "generated_at": datetime.now(timezone.utc).isoformat()}).encode(),
            ContentType="application/json")
        return {"statusCode": 200, "body": "kb missing"}

    fw_node = kb.get("frameworks") or kb.get("playbooks") or kb
    frameworks = []
    if isinstance(fw_node, list):
        frameworks = fw_node
    elif isinstance(fw_node, dict):
        frameworks = [dict(v, _name=k) if isinstance(v, dict) else {"_name": k, "rules": v}
                      for k, v in fw_node.items()
                      if k not in ("version", "generated_at", "meta", "methodology")]
    state = build_today_state()

    scored = []
    tot_rules = tot_eval = tot_machine = 0
    for fw in frameworks:
        if not isinstance(fw, dict):
            continue
        name = fw.get("name") or fw.get("framework") or fw.get("_name") or "unnamed"
        rules = []
        rule_texts(fw, rules)
        seen, dedup = set(), []
        for r in rules:
            k = r["text"][:90]
            if k not in seen:
                seen.add(k); dedup.append(r)
        rules = dedup
        matched, unmatched, why_m, next_triggers = 0, 0, [], []
        n_eval = 0
        for r in rules:
            st_, why = eval_rule(r, state)
            if r["kind"] == "machine":
                tot_machine += 1
            if st_ == "matched":
                matched += 1; n_eval += 1
                if len(why_m) < 3:
                    why_m.append({"rule": r["text"][:140], "why": why})
            elif st_ == "unmatched":
                unmatched += 1; n_eval += 1
                if len(next_triggers) < 3:
                    next_triggers.append({"rule": r["text"][:140], "gap": why})
        tot_rules += len(rules); tot_eval += n_eval
        if n_eval >= 3:
            scored.append({"framework": str(name)[:80], "n_rules": len(rules),
                           "n_evaluable": n_eval,
                           "match_pct": round(100 * matched / n_eval, 1),
                           "matched_rules": why_m, "next_triggers": next_triggers})
    scored.sort(key=lambda x: -x["match_pct"])

    out = {"engine": "kb-matcher", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "method": "machine" if tot_machine > tot_eval / 2 else "keyword-heuristic-v1",
           "kb_stats": {"n_frameworks": len(frameworks), "n_rules_harvested": tot_rules,
                        "n_evaluable_today": tot_eval, "n_machine_rules": tot_machine,
                        "coverage_pct": round(100 * tot_eval / tot_rules, 1) if tot_rules else 0},
           "today_state": state,
           "top_matches": scored[:5],
           "all_frameworks": [{k: f[k] for k in ("framework", "match_pct", "n_evaluable", "n_rules")}
                              for f in scored],
           "methodology": ("Daily live pattern-match of the tape against the codified crisis "
                           "knowledge base. Machine-evaluable rule conditions are computed directly; "
                           "textual rules are scored by indicator-keyword + direction-keyword "
                           "consistency with live 20-obs changes (method flagged, coverage% shown — "
                           "unevaluable rules are excluded, never assumed). 'Next triggers' = the "
                           "closest framework's not-yet-met rules: the playbook's next domino.")}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[kb] fw={len(frameworks)} rules={tot_rules} eval={tot_eval} "
          f"top={scored[0]['framework'] if scored else None} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"frameworks": len(scored)})}
