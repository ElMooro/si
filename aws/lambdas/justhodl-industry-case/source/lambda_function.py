"""justhodl-industry-case -- what role does this stock play?
Marker: industry-case v1.0.0
Every answer is assembled from banked fleet data (universe,
industry boom league, earnings desk, tape-truth) and cites its
numbers.  The value-chain question is HONESTLY deferred until
readthrough's event->edge aggregation is proven (probed 4888:
event-shaped, not a static map).  LLM narrative self-heals.
"""
import gzip
import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/industry-case.json"
AI_CAP = 6

s3 = boto3.client("s3", region_name=REGION)


def _g(key):
    try:
        raw = s3.get_object(Bucket=BUCKET,
                            Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return None


def _put(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=gzip.compress(
                      json.dumps(obj).encode()),
                  ContentType="application/json",
                  ContentEncoding="gzip",
                  CacheControl="no-cache")


def llm_case(key_a, name, facts):
    if not key_a:
        return None, ("rules_only (Anthropic billing down -- "
                      "self-heals on credits)")
    try:
        body = json.dumps({
            "model": "claude-haiku-4-5",
            "max_tokens": 160,
            "messages": [{"role": "user", "content":
                          "Two factual sentences on %s's role "
                          "in its industry, using ONLY these "
                          "facts, no hype: %s"
                          % (name, json.dumps(facts)[:700])}]})
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=body.encode(),
            headers={"x-api-key": key_a,
                     "anthropic-version": "2023-06-01",
                     "content-type": "application/json"})
        with urllib.request.urlopen(req, timeout=40) as r:
            j = json.loads(r.read())
        return j["content"][0]["text"].strip()[:420], "llm"
    except Exception as e:  # noqa: BLE001
        return None, ("rules_only (llm error %s -- "
                      "self-heals)" % str(e)[:40])


def build(event=None):
    now = datetime.now(timezone.utc)
    key_a = os.environ.get("ANTHROPIC_KEY", "")
    doc = {"v": VERSION, "engine": "justhodl-industry-case",
           "as_of": now.date().isoformat(),
           "generated_at": now.isoformat(),
           "status": "LIVE",
           "method": {
               "share": "industry share = ticker mcap / "
                        "sum(universe mcap in industry); "
                        "share of the LISTED-US investable "
                        "cohort, never a claimed product-"
                        "market share",
               "chain": "value-chain edges DEFERRED -- "
                        "readthrough is event-shaped "
                        "(probe 4888); aggregation queued, "
                        "never guessed"}}
    uni = _g("data/universe.json")
    stocks = (uni or {}).get("stocks") or []
    if len(stocks) < 1000:
        doc.update({"status": "MISSING",
                    "why": "universe spine absent/thin "
                           "(%d)" % len(stocks)})
        _put(OUT_KEY, doc)
        return doc
    boom = _g("data/industry-boom.json") or {}
    league = boom.get("league") or []
    boom_by_ind = {}
    for i, r in enumerate(sorted(
            league, key=lambda x: -(x.get("boom_score")
                                    or -1))):
        if r.get("industry"):
            boom_by_ind[r["industry"]] = {
                "score": r.get("boom_score"),
                "rank": i + 1,
                "of": len(league),
                "n_names": r.get("n"),
                "inst_net_bps": (r.get("comp") or {})
                .get("inst_net_bps"),
                "insider_buys_30d": (r.get("comp") or {})
                .get("insider_buys_30d")}
    earn = _g("data/earnings.json") or {}
    beat_by_t = {r["t"]: r for r in
                 (earn.get("beat_league") or [])}
    picks_by_t = {p["t"]: p for p in
                  ((earn.get("growth_calls") or {})
                   .get("picks") or [])}
    tape = _g("data/tape-truth.json") or {}
    tape_by_t = tape.get("symbols") or {}

    inds = {}
    for r in stocks:
        ind = r.get("industry")
        mc = r.get("market_cap")
        if not ind or not isinstance(mc, (int, float)) \
                or mc <= 0:
            continue
        inds.setdefault(ind, {"sector": r.get("sector"),
                              "members": []})
        inds[ind]["members"].append(
            (str(r.get("symbol")), r.get("name"), mc))
    industries = {}
    rank_of = {}
    for ind, blk in inds.items():
        mem = sorted(blk["members"], key=lambda x: -x[2])
        tot = sum(m[2] for m in mem)
        industries[ind] = {
            "sector": blk["sector"], "n": len(mem),
            "total_mcap_b": round(tot / 1e9, 1),
            "boom": boom_by_ind.get(ind),
            "top5": [{"t": m[0], "name": m[1],
                      "mcap_b": round(m[2] / 1e9, 1),
                      "share_pct": round(100.0 * m[2]
                                         / tot, 1)}
                     for m in mem[:5]]}
        for i, m in enumerate(mem):
            rank_of[m[0]] = (ind, i + 1, len(mem),
                             round(100.0 * m[2] / tot, 2),
                             tot)
    cases = {}
    for r in stocks:
        t = str(r.get("symbol") or "")
        if t not in rank_of:
            continue
        ind, rk, n, share, tot = rank_of[t]
        mc = r.get("market_cap") or 0
        c = {"name": r.get("name"), "sector": r.get("sector"),
             "industry": ind,
             "mcap_b": round(mc / 1e9, 2),
             "bucket": r.get("cap_bucket"),
             "ind_rank": rk, "ind_n": n,
             "ind_share_pct": share,
             "boom": boom_by_ind.get(ind)}
        br = beat_by_t.get(t)
        if br:
            c["earn"] = {"beat_rank": br.get("rank"),
                         "beat_score": br.get("beat_score"),
                         "eps_surprise_pct":
                         br.get("eps_surprise_pct")}
        pk = picks_by_t.get(t)
        if pk:
            c.setdefault("earn", {})["growth_pick_score"] = \
                pk.get("pick_score")
        tp = tape_by_t.get(t)
        if tp:
            v = tp.get("verdict") or {}
            c["tape"] = {"call": v.get("call"),
                         "conviction": v.get("conviction")}
        cases[t] = c
    ai_done = 0
    for tname in sorted(cases,
                        key=lambda x: -cases[x]["mcap_b"]):
        if ai_done >= AI_CAP:
            break
        c = cases[tname]
        line, mode = llm_case(
            key_a, c["name"],
            {"industry": c["industry"],
             "rank": "%d of %d" % (c["ind_rank"],
                                   c["ind_n"]),
             "share_pct": c["ind_share_pct"],
             "boom": c.get("boom")})
        c["ai_case"] = line
        c["ai_mode"] = mode
        ai_done += 1
    doc["n_industries"] = len(industries)
    doc["n_cases"] = len(cases)
    doc["industries"] = industries
    doc["cases"] = cases
    _put(OUT_KEY, doc)
    return doc


def lambda_handler(event, context):
    doc = build(event)
    return {"statusCode": 200,
            "body": json.dumps({"v": doc.get("v"),
                                "status": doc.get("status"),
                                "n": doc.get("n_cases")})}
