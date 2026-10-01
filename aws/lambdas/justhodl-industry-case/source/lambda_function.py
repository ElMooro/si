"""justhodl-industry-case -- what role does this stock play?
Marker: industry-case v1.1.0
Every answer is assembled from banked fleet data (universe,
industry boom league, earnings desk, tape-truth) and cites its
numbers.  The value-chain question is HONESTLY deferred until
readthrough's event->edge aggregation is proven (probed 4888:
event-shaped, not a static map).  LLM narrative self-heals.
"""
import gzip
import json
import os
import math
import time
from datetime import datetime, timezone

import boto3
from tape_truth_qualification import project_tape
from publication import CONTRACT, SOURCE_KEYS, prepare, number

VERSION = "1.3.0"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/industry-case.json"
AI_CAP = 6
CLOSES_KEY = "spx-beaters/weekly-closes.json"


def tier_of(share_pct):
    if share_pct >= 25:
        return "LEADER"
    if share_pct >= 10:
        return "MAJOR"
    if share_pct >= 3:
        return "CHALLENGER"
    return "NICHE"

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


def llm_case(enabled, name, facts, context=None, deadline=None):
    fallback = "Recorded cohort: rank %s in %s; %s%% of listed-cohort market value. Source freshness UNKNOWN; no current-market inference." % (facts["rank"], str(facts["industry"])[:120], facts["share_pct"])
    def denied(reason):
        return fallback[:420], "rules_only (" + reason + ")"
    if not enabled:
        return denied("optional narratives disabled pending activation review")
    try:
        remaining = context.get_remaining_time_in_millis() if context else None
        if (type(remaining) not in (int, float) or not math.isfinite(remaining) or remaining < 60000
                or deadline is None or time.monotonic() >= deadline):
            return denied("optional narrative time budget unavailable")
        # Import failure means deterministic fallback. Never construct a provider request here.
        import llm_router
        import llm_cost
        # Require the existing admission controls before entering the router, which checks them again.
        if llm_cost.mode() not in ("normal", "economy") or llm_cost.budget_ok() is not True or llm_cost.within_daily_cap() is not True:
            return denied("router admission denied")
        prompt = ("Explain these dated recorded cohort measurements in two sentences. Freshness is UNKNOWN; "
                  "do not imply current conditions or make investment claims. Name: %s. Facts: %s" %
                  (name[:120] if isinstance(name, str) else "unavailable", json.dumps(facts, allow_nan=False)[:700]))[:1100]
        # Admission reads may themselves consume time; recheck before optional router work.
        remaining = context.get_remaining_time_in_millis()
        if (type(remaining) not in (int, float) or not math.isfinite(remaining) or remaining < 60000
                or time.monotonic() >= deadline):
            return denied("optional narrative time budget exhausted during admission")
        line = llm_router.complete(prompt, tier="bulk", max_tokens=160,
                                   contains_proprietary=False, on_demand=False)
        if not isinstance(line, str) or not line.strip():
            return denied("router unavailable or disallowed")
        return line.strip()[:420], "governed_router (dated facts; freshness UNKNOWN)"
    except Exception:
        return denied("governed narrative unavailable")


def build(event=None, context=None):
    now = datetime.now(timezone.utc)
    narratives_enabled = os.environ.get("INDUSTRY_CASE_NARRATIVES", "off") == "governed"
    narrative_deadline = time.monotonic() + 30
    primary = _g(SOURCE_KEYS[0])
    rows = primary.get("stocks") if isinstance(primary, dict) else None
    read_companions = isinstance(rows, list) and len(rows) >= 1000
    packets = {SOURCE_KEYS[0]: primary}
    packets.update({key: _g(key) if read_companions else None for key in SOURCE_KEYS[1:]})
    inputs, sources = prepare(packets, now)
    if not read_companions:
        for key in SOURCE_KEYS[1:]:
            sources[key]["availability"] = "NOT_READ_PRIMARY_UNAVAILABLE"
    doc = {"v": VERSION, "engine": "justhodl-industry-case",
           "as_of": None,
           "publication_contract": CONTRACT,
           "qualification": {"freshness": "UNKNOWN", "freshness_reason": "NO_AUTHORITATIVE_SOURCE_SLA",
                             "current_eligible": False},
           "sources": sources,
           "narrative_control": {"enabled": narratives_enabled, "router_calls_max": AI_CAP,
                                 "duplicate_protection": "BLOCKED_NO_DURABLE_IDEMPOTENCY",
                                 "activation": "NOT_RESTORED"},
           "generated_at": now.isoformat(),
           "status": "COMPUTED" if all(v["availability"] == "AVAILABLE" for v in sources.values()) else "PARTIAL",
           "method": {
               "share": "industry share = ticker mcap / "
                        "sum(universe mcap in industry); "
                        "share of the LISTED-US investable "
                        "cohort, never a claimed product-"
                        "market share",
               "chain": "value-chain edges DEFERRED -- "
                        "readthrough is event-shaped "
                        "(probe 4888); aggregation queued, "
                        "never guessed",
               "tiers": "LEADER>=25%% / MAJOR>=10%% / "
                        "CHALLENGER>=3%% / NICHE -- "
                        "mcap-cohort-derived labels, not "
                        "product-share claims",
               "growth": "ret_12m from the beaters weekly-"
                         "closes ledger (52w); revenue "
                         "growth DEFERRED -- census matrix "
                         "not proven under probed keys "
                         "(4890), nulled never guessed",
               "hhi": "sum of squared %% shares (0-10000); "
                      ">2500 highly concentrated, 1500-2500 "
                      "moderate, <1500 competitive (DOJ/FTC "
                      "convention)"}}
    uni = inputs["data/universe.json"]
    stocks = (uni or {}).get("stocks") or []
    if not read_companions or not stocks:
        doc.update({"status": "MISSING",
                    "why": "universe spine absent/thin "
                           "(%d)" % len(stocks)})
        _put(OUT_KEY, doc)
        return doc
    boom = inputs["data/industry-boom.json"]
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
    earn = inputs["data/earnings.json"]
    beat_by_t = {r["t"]: r for r in
                 (earn.get("beat_league") or [])}
    picks_by_t = {p["t"]: p for p in
                  ((earn.get("growth_calls") or {})
                   .get("picks") or [])}
    tape = inputs["data/tape-truth.json"]
    tape = tape if isinstance(tape, dict) else {}
    tape_by_t = tape.get("symbols") or {}
    tape_by_t = tape_by_t if isinstance(tape_by_t, dict) else {}
    closes = inputs[CLOSES_KEY].get("closes") or {}

    def ret12(sym):
        arr = closes.get(sym)
        if isinstance(arr, list) and len(arr) >= 53 \
                and arr[-53] and arr[-1]:
            try:
                value = round((arr[-1] / arr[-53] - 1) * 100, 1)
                return value if math.isfinite(value) else None
            except (TypeError, ZeroDivisionError, OverflowError):
                return None
        return None

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
        if (not number(tot) or tot <= 0
                or not all(math.isfinite(100.0 * m[2] / tot) for m in mem)):
            doc["status"] = "PARTIAL"
            doc.setdefault("unavailable_industries", []).append(ind)
            continue
        members = []
        hhi = 0.0
        wtd_num = wtd_den = 0.0
        rets = []
        for i, m in enumerate(mem):
            sh = 100.0 * m[2] / tot
            hhi += sh * sh
            r12 = ret12(m[0])
            if r12 is not None:
                rets.append(r12)
                wtd_num += r12 * m[2]
                wtd_den += m[2]
            members.append({"t": m[0], "name": m[1],
                            "rank": i + 1,
                            "mcap_b": round(m[2] / 1e9, 2),
                            "share_pct": round(sh, 2),
                            "tier": tier_of(sh),
                            "ret_12m_pct": r12})
        rets.sort()
        industries[ind] = {
            "sector": blk["sector"], "n": len(mem),
            "total_mcap_b": round(tot / 1e9, 1),
            "boom": boom_by_ind.get(ind),
            "hhi": round(hhi, 0),
            "top3_share_pct": round(sum(
                100.0 * m[2] / tot for m in mem[:3]), 1),
            "wtd_ret_12m_pct": round(wtd_num / wtd_den, 1)
            if wtd_den and math.isfinite(wtd_num) and math.isfinite(wtd_den) else None,
            "median_ret_12m_pct": rets[len(rets) // 2]
            if rets else None,
            "ret_coverage": len(rets),
            "rev_growth": {"status": "DEFERRED",
                           "why": "census matrix not proven "
                                  "under probed keys (ops "
                                  "4890) -- nulled, never "
                                  "guessed"},
            "members": members,
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
             "tier": tier_of(share),
             "ret_12m_pct": ret12(t),
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
            c["tape"] = project_tape(tape, t, now)
        cases[t] = c
    ai_done = 0
    for tname in sorted(cases,
                        key=lambda x: -cases[x]["mcap_b"]):
        if ai_done >= AI_CAP:
            break
        c = cases[tname]
        line, mode = llm_case(
            narratives_enabled, c["name"],
            {"industry": c["industry"],
             "rank": "%d of %d" % (c["ind_rank"],
                                   c["ind_n"]),
             "share_pct": c["ind_share_pct"],
             "boom": c.get("boom")}, context, narrative_deadline)
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
    doc = build(event, context)
    return {"statusCode": 200,
            "body": json.dumps({"v": doc.get("v"),
                                "status": doc.get("status"),
                                "n": doc.get("n_cases")})}
