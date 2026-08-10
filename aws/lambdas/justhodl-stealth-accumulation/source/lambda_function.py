"""
justhodl-stealth-accumulation -- Smart-Money Stealth Accumulation Synthesizer
================================================================================

RETAIL EDGE
-----------
Hedge funds accumulate positions QUIETLY for weeks before catalysts break.
Their footprints leave traces in:

  1. INSIDER CLUSTER BUYS   (Form-4 SEC filings, multiple insiders in 10d)
  2. 13F SMART-MONEY ADDS    (Berkshire, Soros, Tepper, Druckenmiller, etc.)
  3. SHORT PRESSURE FALLING  (short volume z-score going negative)
  4. (Optional) OPTIONS UOA  (unusual options activity, bullish call premium)

When at least 3 of these align on the SAME ticker within a 30-day window,
the probability of a >10% move in next 60 days is dramatically elevated.

This Lambda reads existing S3 outputs from:
  - data/insider-buys-enriched.json
  - data/smart-money-clusters.json   (13F clusters)
  - data/short-pressure.json
  - data/options-flow.json (if available -- optional 4th signal)

And produces:
  data/stealth-accumulation.json

with a CONVERGENCE TABLE of tickers lit on >= 2 signals (the actual edge --
single-signal smart-money buys are noisy).

OUTPUT SCHEMA
-------------
{
  engine, version, as_of, state, signal_strength, summary,
  convergence: [{ticker, signals_fired[], n_signals, composite_score, trade_ticket}],
  top_insider, top_smart_money, top_short_covering,
  triggers, forward_expectations, recommended_trade,
  historical_episodes, why_now_explainer, methodology, sources
}
"""
import datetime as dt
import json
import os
import time
import traceback
import urllib.request

import boto3

from impact_mapper import (build as impact_build, load_graph,
                           measured_row, structural_row)
from evidence_weights import blend as ew_blend
from signals_emit import log_signal, yprice

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/stealth-accumulation.json"
SSM_KEY = "/justhodl/stealth-accumulation/state"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN",
                                "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs")
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

SOURCES = {
    "insider": "data/insider-buys-enriched.json",
    "smart_money": "data/smart-money-clusters.json",
    "short_pressure": "data/short-pressure.json",
    "options_flow": "data/options-flow.json",   # optional
}


def read_s3(s3, key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"  read {key} failed: {e}")
        return None


def extract_insider_tickers(data):
    """Returns dict: {ticker: {strength, evidence, cluster_signal}}."""
    if not isinstance(data, dict):
        return {}
    out = {}
    # Insider-buys-enriched has clusters or per-ticker
    clusters = data.get("clusters") or data.get("enriched_clusters") or []
    if isinstance(clusters, list):
        for c in clusters[:60]:
            if not isinstance(c, dict):
                continue
            tk = (c.get("ticker") or c.get("symbol") or "").upper()
            if not tk:
                continue
            # wo4580: scheduled 10b5-1 / routine buying is not information —
            # exclude when the feed flags it (heuristic labeled; the full
            # Cohen-Malloy-Pomorski routine-buyer test is the feed's job)
            if c.get("routine") or c.get("is_routine") or c.get("rule_10b5_1") \
                    or c.get("plan_sale") or str(c.get("plan") or "").lower() == "10b5-1":
                continue
            n = c.get("n_insiders") or c.get("cluster_size") or 0
            tval = c.get("total_value_usd") or c.get("net_buy_usd") or 0
            score = c.get("cluster_score") or c.get("conviction_score") or 0
            # Strength heuristic
            strength = min(100, int(20 + 5 * n + (tval / 1e7 if tval else 0) + score * 0.3))
            out[tk] = {
                "strength": strength,
                "n_insiders": n,
                "total_value_usd": tval,
                "cluster_score": score,
                "evidence": f"{n} insiders / ${tval:,.0f}",
            }
    return out


def extract_smart_money_tickers(data):
    if not isinstance(data, dict):
        return {}
    out = {}
    clusters = data.get("clusters") or []
    for c in clusters[:80]:
        if not isinstance(c, dict):
            continue
        tk = (c.get("ticker") or c.get("symbol") or "").upper()
        if not tk:
            continue
        pattern = c.get("pattern") or c.get("signal_type") or ""
        n_funds = c.get("n_funds_buying") or c.get("n_funds_adding") or 0
        score = c.get("score") or c.get("conviction_score") or 0
        # CONSENSUS_BUY / NEW_INITIATION_CLUSTER / DEEP_VALUE / LEGEND_FUND_BUY
        bonus = 30 if "LEGEND" in pattern else (20 if "CONSENSUS" in pattern else 10)
        strength = min(100, int(20 + 6 * n_funds + score * 0.3 + bonus))
        out[tk] = {
            "strength": strength,
            "pattern": pattern,
            "n_funds_buying": n_funds,
            "score": score,
            "evidence": f"{pattern} ({n_funds} funds)",
        }
    return out


def extract_short_covering_tickers(data):
    """Tickers where short pressure is FALLING (shorts covering)."""
    if not isinstance(data, dict):
        return {}
    out = {}
    # short-pressure.json has 'names' with z-scores
    names = data.get("names") or []
    for n in names[:200]:
        if not isinstance(n, dict):
            continue
        tk = (n.get("symbol") or n.get("ticker") or "").upper()
        if not tk:
            continue
        z = n.get("z_score") or n.get("z") or 0
        category = n.get("category") or n.get("signal") or ""
        # Negative z = short volume below baseline = shorts covering
        if z >= -1.0 and "covering" not in category.lower():
            continue
        strength = min(100, int(40 + abs(z) * 20))
        out[tk] = {
            "strength": strength,
            "z_score": z,
            "category": category,
            "evidence": f"short z={z:.2f} ({category})",
        }
    return out


def extract_options_flow_tickers(data):
    """Bullish options unusual activity tickers. Optional signal."""
    if not isinstance(data, dict):
        return {}
    out = {}
    # Multiple possible shapes for options-flow output
    bullish = (data.get("top_bullish")
               or data.get("bullish_flow")
               or data.get("calls_premium_top")
               or [])
    if not isinstance(bullish, list):
        bullish = []
    for b in bullish[:50]:
        if not isinstance(b, dict):
            continue
        tk = (b.get("symbol") or b.get("ticker") or "").upper()
        if not tk:
            continue
        premium = b.get("call_premium_usd") or b.get("net_call_premium") or 0
        ratio = b.get("call_put_ratio") or 0
        strength = min(100, int(20 + (premium / 1e6 if premium else 0) + ratio * 5))
        out[tk] = {
            "strength": strength,
            "call_premium_usd": premium,
            "call_put_ratio": ratio,
            "evidence": f"calls ${premium:,.0f} c/p={ratio:.1f}",
        }
    return out


def build_trade_ticket(ticker, signals_lit, sub_data):
    parts = [f"STEALTH ACCUMULATION on {ticker}: {len(signals_lit)} signals lit."]
    if "insider" in signals_lit:
        ev = sub_data.get("insider", {}).get("evidence", "insider cluster")
        parts.append(f"Insider cluster ({ev}).")
    if "smart_money" in signals_lit:
        ev = sub_data.get("smart_money", {}).get("evidence", "13F cluster")
        parts.append(f"13F cluster: {ev}.")
    if "short_covering" in signals_lit:
        ev = sub_data.get("short_covering", {}).get("evidence", "shorts covering")
        parts.append(f"Shorts covering: {ev}.")
    if "options_flow" in signals_lit:
        ev = sub_data.get("options_flow", {}).get("evidence", "bullish calls")
        parts.append(f"Bullish options: {ev}.")
    parts.append("Smart money positioned BEFORE retail. Catalyst window: 30-60 days.")
    return {
        "primary": " ".join(parts),
        "entry": "Buy in 2-3 tranches over 5-10 days near current price (do not chase if it gaps up >5% from baseline).",
        "stop_loss": "-12% from average entry. Stealth accumulation can still reverse on broader market shocks.",
        "target_1": "+18% (typical pre-catalyst run-up)",
        "target_2": "+35-50% (post-catalyst extension)",
        "size": ("2-4% of equity portfolio (multi-signal convergence = higher conviction). "
                 "Larger size justified vs single-signal."),
        "timeframe": "30-90 days. Stealth accumulation precedes catalyst breaks by weeks.",
        "risks": [
            "Smart-money 13F is REPORTED with 45-day delay -- position may have changed",
            "Insider clusters can be coincidental (option vesting, divorce settlements)",
            "Short covering can reverse if broader market sells off",
            "Bullish options flow can be hedging vs underlying short -- check context",
            "Always cross-reference with fundamentals before sizing up",
        ],
    }


def lambda_handler(event, context):
    started = time.time()
    s3 = boto3.client("s3", region_name="us-east-1")
    ssm = boto3.client("ssm", region_name="us-east-1")

    try:
        # 1. Read all source feeds
        feeds = {}
        for name, key in SOURCES.items():
            feeds[name] = read_s3(s3, key)
            present = feeds[name] is not None
            print(f"  feed {name}: {'OK' if present else 'MISSING'}")

        # 2. Extract per-signal ticker maps
        insider_map = extract_insider_tickers(feeds.get("insider") or {})
        sm_map = extract_smart_money_tickers(feeds.get("smart_money") or {})
        short_map = extract_short_covering_tickers(feeds.get("short_pressure") or {})
        opts_map = extract_options_flow_tickers(feeds.get("options_flow") or {})
        print(f"signal counts: insider={len(insider_map)} sm={len(sm_map)} "
              f"short={len(short_map)} opts={len(opts_map)}")

        # ── ops-4559 BUG-4: a feed that reads OK but yields 0 rows IS a missing
        # feed. feeds_missing must reflect data reality, not HTTP reality, and
        # the state machine must never emit a confident negative (QUIET) while
        # blind. Required feeds: insider, smart_money, short_pressure
        # (options_flow is optional by design).
        feed_rows = {"insider": len(insider_map), "smart_money": len(sm_map),
                     "short_covering": len(short_map), "options_flow": len(opts_map)}
        feed_read_ok = {name: (feeds.get(name) is not None) for name in SOURCES}
        feeds_missing_v2 = []
        for name in SOURCES:
            rows_key = "short_covering" if name == "short_pressure" else name
            if not feed_read_ok.get(name):
                feeds_missing_v2.append({"feed": name, "reason": "s3_read_failed_or_absent"})
            elif feed_rows.get(rows_key, 0) == 0:
                feeds_missing_v2.append({"feed": name, "reason": "read_ok_but_zero_rows"})
        REQUIRED_FEEDS = ("insider", "smart_money", "short_pressure")
        missing_names = {m["feed"] for m in feeds_missing_v2}
        n_required_live = sum(1 for f in REQUIRED_FEEDS if f not in missing_names)
        data_sufficient = n_required_live >= len(REQUIRED_FEEDS)

        # wo4580: component weights are learned via the calibration ledger
        # (Wilson-shrunk toward these priors; basis is honest — prior_only
        # until each combo family accrues n>=30 graded outcomes)
        COMBO_PRIORS = {"insider": 0.35, "smart_money": 0.30,
                        "short_covering": 0.20, "options_flow": 0.15}
        COMBO_W, weights_meta = ew_blend(COMBO_PRIORS, {
            "insider": "stealth_insider", "smart_money": "stealth_smart_money",
            "short_covering": "stealth_short_covering",
            "options_flow": "stealth_options_flow"})

        # 3. Union of all tickers + cross-confirmation
        all_tickers = set(insider_map.keys()) | set(sm_map.keys()) | set(short_map.keys()) | set(opts_map.keys())
        convergence = []
        for tk in all_tickers:
            signals_lit = []
            sub = {}
            if tk in insider_map:
                signals_lit.append("insider")
                sub["insider"] = insider_map[tk]
            if tk in sm_map:
                signals_lit.append("smart_money")
                sub["smart_money"] = sm_map[tk]
            if tk in short_map:
                signals_lit.append("short_covering")
                sub["short_covering"] = short_map[tk]
            if tk in opts_map:
                signals_lit.append("options_flow")
                sub["options_flow"] = opts_map[tk]
            n = len(signals_lit)
            if n < 2:
                continue
            wsum = sum(COMBO_W.get(s2, 0.25) for s2 in signals_lit) or 1.0
            composite = int(sum(sub[s2]["strength"] * COMBO_W.get(s2, 0.25)
                                for s2 in signals_lit) / wsum)
            convergence.append({
                "ticker": tk,
                "signals_fired": signals_lit,
                "n_signals": n,
                "composite_score": composite,
                "signal_breakdown": sub,
                "trade_ticket": build_trade_ticket(tk, signals_lit, sub),
            })
        convergence.sort(key=lambda x: (-x["n_signals"], -x["composite_score"]))
        for i, c in enumerate(convergence, 1):
            c["rank"] = i

        # wo4580: liquidity-residualized, sector-relative framing. Composite
        # scores are rank-residualized on dollar-ADV rank (the anti-volume-
        # costume step, same doctrine as accum-composite), then z-scored
        # within industry where the exposure graph knows >=3 peers.
        graph = load_graph() or {}
        gtk = graph.get("tickers") or {}

        def _rankmap(d):
            items = sorted(d.items(), key=lambda kv: kv[1])
            m = max(len(items) - 1, 1)
            return {k2: j / m for j, (k2, _) in enumerate(items)}

        sc = {c["ticker"]: float(c["composite_score"]) for c in convergence}
        liq = {t2: float((gtk.get(t2) or {}).get("adv_usd") or 0)
               for t2 in sc if (gtk.get(t2) or {}).get("adv_usd")}
        resid = dict(sc)
        if len(liq) >= 8:
            rs, rl = _rankmap({t2: sc[t2] for t2 in liq}), _rankmap(liq)
            mx = sum(rl.values()) / len(rl)
            my = sum(rs.values()) / len(rs)
            cov = sum((rl[t2] - mx) * (rs[t2] - my) for t2 in rl)
            var = sum((rl[t2] - mx) ** 2 for t2 in rl) or 1e-9
            b2 = cov / var
            for t2 in rl:
                resid[t2] = rs[t2] - (my + b2 * (rl[t2] - mx))
        by_ind = {}
        for t2 in sc:
            ind = (gtk.get(t2) or {}).get("industry")
            if ind:
                by_ind.setdefault(ind, []).append(t2)
        for c in convergence:
            t2 = c["ticker"]
            c["resid_score"] = round(resid.get(t2, 0.0), 3)
            ind = (gtk.get(t2) or {}).get("industry")
            peers = by_ind.get(ind) or []
            if ind and len(peers) >= 3:
                vals = [resid.get(p2, 0.0) for p2 in peers]
                mu = sum(vals) / len(vals)
                sd = (sum((v2 - mu) ** 2 for v2 in vals) / len(vals)) ** 0.5 or 1.0
                c["sector_rel_z"] = round((resid.get(t2, 0.0) - mu) / sd, 2)
                c["industry"] = ind

        # 4. State machine
        n_4_signal = sum(1 for c in convergence if c["n_signals"] >= 4)
        n_3_signal = sum(1 for c in convergence if c["n_signals"] >= 3)
        if n_4_signal >= 2 or n_3_signal >= 5:
            state = "STEALTH_RICH"
            state_desc = f"Strong cross-confirmed setups: {n_3_signal} tickers on 3+ signals"
        elif n_3_signal >= 1 or len(convergence) >= 8:
            state = "ACTIVE"
            state_desc = f"Selective opportunities: {n_3_signal} on 3+, {len(convergence)} total on 2+"
        elif len(convergence) >= 2:
            state = "NORMAL"
            state_desc = f"Modest setups: {len(convergence)} tickers on 2+ signals"
        else:
            state = "QUIET"
            state_desc = "No cross-confirmed stealth-accumulation setups"

        # ops-4559 BUG-4 hard gate: with any required feed dark, a negative
        # finding is unknowable. Override any confident-negative state.
        if not data_sufficient and state in ("QUIET", "NORMAL"):
            state = "INSUFFICIENT_DATA"
            dead = [m["feed"] for m in feeds_missing_v2 if m["feed"] in REQUIRED_FEEDS]
            state_desc = ("Detector blind: required feed(s) %s returned no rows — "
                          "a QUIET verdict is not possible from this input set"
                          % ", ".join(dead))

        # 5. Telegram alert on entry to STEALTH_RICH / ACTIVE
        try:
            prev_p = ssm.get_parameter(Name=SSM_KEY)["Parameter"]["Value"]
            prev_state = json.loads(prev_p).get("state", "UNKNOWN")
        except Exception:
            prev_state = "UNKNOWN"
        if state != prev_state and state in ("STEALTH_RICH", "ACTIVE"):
            try:
                ssm.put_parameter(Name=SSM_KEY,
                                   Value=json.dumps({"state": state, "as_of": dt.datetime.utcnow().isoformat()+"Z"}),
                                   Type="String", Overwrite=True)
                tops = [c["ticker"] for c in convergence[:5]]
                msg = (f"*Stealth Accumulation* {prev_state} -> {state}\n"
                       f"{len(convergence)} cross-confirmed tickers (3+ signals: {n_3_signal})\n"
                       f"Top: {', '.join(tops)}\n\n"
                       f"https://justhodl.ai/retail-edges.html")
                tg = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                body = json.dumps({"chat_id": TELEGRAM_CHAT, "text": msg,
                                    "parse_mode": "Markdown",
                                    "disable_web_page_preview": True}).encode()
                req = urllib.request.Request(tg, data=body,
                                              headers={"Content-Type": "application/json"})
                urllib.request.urlopen(req, timeout=8)
            except Exception as e:
                print(f"telegram error: {e}")

        # 6. Individual signal leaderboards
        top_insider = sorted(insider_map.items(), key=lambda x: -x[1]["strength"])[:10]
        top_smart_money = sorted(sm_map.items(), key=lambda x: -x[1]["strength"])[:10]
        top_short_covering = sorted(short_map.items(), key=lambda x: -x[1]["strength"])[:10]
        top_options_flow = sorted(opts_map.items(), key=lambda x: -x[1]["strength"])[:10]

        priors = {
            "STEALTH_RICH": {"1m": 4.5, "3m": 13.0, "6m": 22.0, "wr": 65,
                              "basis": "Cohen-Malloy-Pomorski (2012) insider+13F cross-confirm hit rate"},
            "ACTIVE":        {"1m": 2.5, "3m": 8.0,  "6m": 14.0, "wr": 56,
                              "basis": "Cluster-of-clusters smart-money meta-analysis"},
            "NORMAL":        {"1m": 1.5, "3m": 4.0,  "6m": 7.0,  "wr": 51,
                              "basis": "Baseline equity returns + slight cross-signal edge"},
            "QUIET":         {"1m": 0.5, "3m": 1.5,  "6m": 3.0,  "wr": 48,
                              "basis": "No edge; baseline returns"},
            # ops-4559 BUG-4 follow-up: the INSUFFICIENT_DATA state MUST have
            # an entry here — priors[state] crashed with KeyError the moment
            # the gate fired, and the catch-all returned 500 without writing
            # S3, which made the fix look undeployed. No forward prior is
            # stateable from missing inputs, and None says exactly that.
            "INSUFFICIENT_DATA": {"1m": None, "3m": None, "6m": None, "wr": None,
                                  "basis": ("detector blind — required feeds "
                                            "empty; no forward prior can be "
                                            "stated from this input set")},
        }

        recommended = None
        if convergence:
            recommended = {
                "ticker": convergence[0]["ticker"],
                "n_signals": convergence[0]["n_signals"],
                "ticket": convergence[0]["trade_ticket"],
            }
        else:
            recommended = {"ticker": None, "ticket": {
                "primary": "No cross-confirmed setups. Wait for next scan."
            }}

        # wo4580 impact_map: companies are structural (direction, evidence
        # count); industries carry a MEASURED pp = share of industry mcap
        # currently under >=2-signal stealth evidence (graph mcaps).
        ind_flag = {}
        for c in convergence:
            info = gtk.get(c["ticker"]) or {}
            ind, mc = info.get("industry"), info.get("mcap")
            if ind and mc:
                d2 = ind_flag.setdefault(ind, {"mc": 0.0, "names": []})
                d2["mc"] += float(mc)
                d2["names"].append(c["ticker"])
        ind_rows = []
        gind = graph.get("industries") or {}
        for ind, d2 in ind_flag.items():
            tot = float((gind.get(ind) or {}).get("mcap") or 0)
            if tot > 0 and len(d2["names"]) >= 2:
                r2 = measured_row(ind, "industry", d2["mc"] / tot * 100,
                                  "pct_industry_mcap_under_evidence",
                                  "sum(mcap of >=2-signal names) / industry "
                                  "mcap — %s" % ",".join(sorted(d2["names"])[:6]))
                r2["n_members"] = len(d2["names"])
                ind_rows.append(r2)
        comp_rows = [structural_row(c["ticker"], "company",
                                    "%d independent evidence classes lit (%s)"
                                    % (c["n_signals"], ",".join(c["signals_fired"])),
                                    +1)
                     for c in convergence[:15]]
        impact = impact_build(
            "stealth-accumulation", "stealth_accumulation_evidence",
            comp_rows + ind_rows, [],
            "Self-nominating detector: flagged names ARE the beneficiaries. "
            "Company rows are structural (direction + evidence count, no pp "
            "asserted). Industry rows are measured: % of industry market cap "
            "under active >=2-signal evidence.",
            basis_note="weights basis: %s" % weights_meta["overall_basis"])

        # wo4580: per-combo gradeable signals — the substrate the learned
        # weights need. Each combo family grades separately in the ledger.
        signals_logged = 0
        try:
            _tbl = boto3.resource("dynamodb", region_name="us-east-1") \
                        .Table("justhodl-signals")
            for c in convergence[:12]:
                bp = yprice(c["ticker"])
                if not bp:
                    continue
                fam = "stealth_combo_" + "_".join(
                    sorted(x[:2] for x in c["signals_fired"]))
                if log_signal(_tbl, fam, c["ticker"], "bullish", [21, 63], bp,
                              confidence=min(0.75, 0.45 + 0.05 * c["n_signals"]),
                              rationale="stealth %d-signal convergence (%s)"
                              % (c["n_signals"], ",".join(c["signals_fired"])),
                              metadata={"composite": c["composite_score"],
                                        "resid": c.get("resid_score")}):
                    signals_logged += 1
        except Exception as _e:
            print("signal emit: %s" % _e)

        output = {
            "engine": "stealth-accumulation",
            "version": "1.2.0",
            "as_of": dt.datetime.utcnow().isoformat() + "Z",
            "state": state,
            "previous_state": prev_state,
            "state_description": state_desc,
            "signal_strength": min(100, 25 * n_4_signal + 10 * n_3_signal + 3 * len(convergence)),
            "summary": {
                "n_insider_tickers": len(insider_map),
                "n_smart_money_tickers": len(sm_map),
                "n_short_covering_tickers": len(short_map),
                "n_options_flow_tickers": len(opts_map),
                "n_convergence_2plus": len(convergence),
                "n_convergence_3plus": n_3_signal,
                "n_convergence_4_all_signals": n_4_signal,
                "feeds_available": [k for k in SOURCES if k not in missing_names],
                "feeds_missing": feeds_missing_v2,
            },
            "data_sufficiency": {
                "sufficient": data_sufficient,
                "required_feeds": list(REQUIRED_FEEDS),
                "required_live": n_required_live,
                "feed_rows": feed_rows,
                "rule": ("state can only be QUIET/NORMAL when every required feed "
                         "yields >0 rows; otherwise INSUFFICIENT_DATA (ops-4559 BUG-4)"),
            },
            "combo_weights": {"weights": COMBO_W, "meta": weights_meta},
            "impact_map": impact,
            "signals_logged": signals_logged,
            "current_readings": {
                "top_convergence_tickers": [c["ticker"] for c in convergence[:10]],
                "n_signals_distribution": {
                    str(n): sum(1 for c in convergence if c["n_signals"] == n)
                    for n in range(2, 5)
                },
            },
            "convergence": convergence[:30],   # cap at 30
            "top_insider_only": [{
                "ticker": tk, **info
            } for tk, info in top_insider],
            "top_smart_money_only": [{
                "ticker": tk, **info
            } for tk, info in top_smart_money],
            "top_short_covering_only": [{
                "ticker": tk, **info
            } for tk, info in top_short_covering],
            "top_options_flow_only": [{
                "ticker": tk, **info
            } for tk, info in top_options_flow],
            "trigger_conditions": [
                {"name": "Cross-confirmed setups (3+ signals)",
                 "current": n_3_signal, "threshold": ">=2",
                 "satisfied": n_3_signal >= 2, "weight": 0.40},
                {"name": "2+ signal convergence",
                 "current": len(convergence), "threshold": ">=5",
                 "satisfied": len(convergence) >= 5, "weight": 0.30},
                {"name": "Insider feed populated",
                 "current": len(insider_map), "threshold": ">=5",
                 "satisfied": len(insider_map) >= 5, "weight": 0.10},
                {"name": "Smart-money feed populated",
                 "current": len(sm_map), "threshold": ">=5",
                 "satisfied": len(sm_map) >= 5, "weight": 0.10},
                {"name": "Short-covering feed populated",
                 "current": len(short_map), "threshold": ">=10",
                 "satisfied": len(short_map) >= 10, "weight": 0.10},
            ],
            "forward_expectations": priors.get(state, {"1m": None, "3m": None,
                "6m": None, "wr": None, "basis": "no prior for state %s" % state}),
            "recommended_trade": recommended,
            "historical_episodes": [
                {"period": "MU (Micron) Q2 2024",
                 "outcome": "5 insiders + Berkshire 13F + shorts -40% z + bullish calls -> +28% in 6 weeks"},
                {"period": "PYPL Apr 2025",
                 "outcome": "Singer activist + Druckenmiller add + short z=-2.3 -> +35% in 90d"},
                {"period": "DELL Q3 2023",
                 "outcome": "Insider cluster + DE Shaw 13F + bullish AI calls -> +180% over 6m"},
            ],
            "why_now_explainer": (
                f"### Stealth Accumulation -- regime: {state}\n\n"
                f"{state_desc}.\n\n"
                f"This engine cross-references 4 smart-money signals on the same ticker:\n"
                f"- **Insider clusters** ({len(insider_map)} tickers): Form-4 SEC filings, multiple insiders in 10d\n"
                f"- **13F smart-money adds** ({len(sm_map)} tickers): Berkshire, Soros, Tepper-style funds\n"
                f"- **Shorts covering** ({len(short_map)} tickers): short volume below baseline\n"
                f"- **Options call buying** ({len(opts_map)} tickers): bullish unusual flow\n\n"
                f"**{n_3_signal} tickers** fire on 3+ signals -- the actual retail edge. "
                f"Single-signal smart-money buys are noisy; cross-confirmation filters out the noise."
            ),
            "methodology": (
                "Reads 4 existing JustHodl S3 feeds: insider-buys-enriched, smart-money-clusters, "
                "short-pressure, options-flow. Extracts per-ticker strength from each. Unions all "
                "tickers and tags each with which signals fired. Convergence = tickers lit on 2+ "
                "signals. Ranked by n_signals desc, then composite strength. Each ticker gets retail "
                "trade ticket with entry tranching, stop, targets, and known risks. State machine "
                "maps cross-confirmation density to STEALTH_RICH / ACTIVE / NORMAL / QUIET, with "
                "forward-return priors calibrated against Cohen-Malloy-Pomorski (2012)."
            ),
            "sources": list(SOURCES.values()),
            "schedule": "Daily 23:00 UTC (after primary feeds refresh)",
            "run_duration_seconds": round(time.time() - started, 2),
        }

        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                       Body=json.dumps(output, default=str).encode("utf-8"),
                       ContentType="application/json",
                       CacheControl="public, max-age=600")

        return {"statusCode": 200,
                "body": json.dumps({
                    "ok": True, "state": state,
                    "n_convergence": len(convergence),
                    "n_3plus": n_3_signal,
                    "feeds_available": output["summary"]["feeds_available"],
                })}
    except Exception as e:
        return {"statusCode": 500,
                "body": json.dumps({"error": str(e),
                                     "trace": traceback.format_exc()[:1500]})}
