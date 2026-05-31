"""justhodl-rotation-chain — institutional lead-lag rotation detection.

THESIS
══════
Capital flows through value chains predictably. When a theme triggers:
  Tier 1 (primary beneficiary) pumps first  → "AI: NVDA, TSM"
  Tier 2 (secondary) lags by 1-4 weeks      → "AI: MU memory, AVGO networking"
  Tier 3 (tertiary) lags by 4-12 weeks      → "AI: power VST/GEV, cooling VRT"
  Tier 4 (peripheral) lags by 3+ months     → "AI: cyber CRWD/PANW (defending AI)"

By measuring the lead-lag relationships AND detecting that tier-N has
already moved while tier-(N+1) hasn't, we can position EARLY in the
next-up tier before retail figures it out.

WHAT WE COMPUTE
═══════════════
For each known value chain (config-driven):
  1. TIER RETURNS — 30-day, 90-day return per tier (cap-weighted)
  2. LEAD-LAG CORRELATION — Pearson(tier_N_returns(t-lag), tier_M_returns(t))
                              for lag = 7, 14, 30 days. Strongest lag = lead time.
  3. ROTATION STATE — which tier is currently leading? What's tier-(leader+1)
                      done in the same window?
  4. NEXT-UP TICKERS — tickers in tier-(leader+1) that haven't yet caught up
                      to expected price action

OUTPUT (data/rotation-chains.json)
══════════════════════════════════
  {
    "chains": {
      "AI": {
        "current_leader_tier": 2,
        "leader_perf_30d": +8.4%,
        "next_tier_perf_30d": -1.2%,
        "expected_catchup_pct": 8.0,
        "next_up_tickers": [
          {"ticker": "VRT", "lag_pct": -9.2, "score": 78}, ...
        ]
      },
      ...
    }
  }

DATA
════
  Uses our existing price-history sources (FMP /historical-price-full).
"""

import json
import os
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import mean, stdev

import boto3

from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/rotation-chains.json"

FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
HTTP_TIMEOUT = 15
USER_AGENT = "JustHodlRotationChain/1.0"

s3 = boto3.client("s3", region_name=REGION)


# ─── Value chain ontology ────────────────────────────────────────────────
# Each chain: tier-N → list of tickers
# Hand-curated. Add chains as new themes emerge.
VALUE_CHAINS = {
    # AI compute → infrastructure chain
    "AI": {
        1: ["NVDA", "TSM", "AVGO"],                     # primary AI compute
        2: ["MU", "AMD", "ASML", "AMAT", "LRCX"],       # memory + foundry equipment
        3: ["VST", "GEV", "CEG", "VRT", "ETN", "PWR"],  # power + cooling + grid
        4: ["PANW", "CRWD", "ZS", "S", "NET"],          # cybersecurity (AI defense)
    },
    # EV / energy storage chain
    "EV": {
        1: ["TSLA", "BYDDY"],
        2: ["GM", "F", "STLA"],                          # legacy makers transitioning
        3: ["ALB", "SQM"],                               # lithium upstream
        4: ["CHPT", "EVGO", "BLNK"],                     # charging infrastructure
    },
    # GLP-1 / metabolic chain  
    "GLP1": {
        1: ["LLY", "NVO"],                               # GLP-1 makers
        2: ["CTLT", "WST"],                              # contract manufacturing
        3: ["AMRN", "PFE"],                              # competitors developing
        4: ["DXCM", "MDT"],                              # downstream diagnostics/devices
    },
    # Reshoring / industrials
    "RESHORING": {
        1: ["DE", "CAT", "ETN"],
        2: ["URI", "GNRC", "POWL"],                      # equipment rental + power
        3: ["NUE", "STLD", "X"],                         # domestic steel
        4: ["MLM", "VMC", "CRH"],                        # aggregates + cement
    },
    # Defense modernization
    "DEFENSE": {
        1: ["LMT", "RTX", "NOC", "GD"],                  # primes
        2: ["LDOS", "BAH", "SAIC"],                      # IT/services contractors
        3: ["AVAV", "KTOS", "RKLB"],                     # drones + space
        4: ["HEI", "TDG"],                               # aftermarket parts
    },
    # Quantum / next-gen compute
    "QUANTUM": {
        1: ["IBM", "GOOGL", "MSFT"],                     # corp quantum programs
        2: ["IONQ", "RGTI", "QBTS"],                     # pure-plays
        3: ["MKSI", "FORM"],                             # quantum-adjacent semis
        4: [],
    },
    # Uranium / nuclear renaissance
    "URANIUM": {
        1: ["CCJ", "DNN"],
        2: ["UEC", "URG", "URA"],                        # smaller miners + ETF
        3: ["BWXT", "FLR"],                              # nuclear engineering
        4: ["NEE", "DUK", "SO"],                         # nuclear-heavy utilities
    },
    # ─── New chains (2026-05-31 expansion) ───────────────────────────────
    
    # Biotech — large-cap leaders → next tier → mid-cap specialty → gene therapy
    "BIOTECH": {
        1: ["LLY", "NVO", "REGN", "VRTX"],               # mega-cap biotech leaders
        2: ["BIIB", "MRNA", "BNTX", "GILD", "AMGN"],     # next-tier biotechs
        3: ["ALNY", "ARGX", "INCY", "BMRN"],             # mid-cap specialty
        4: ["IONS", "SRPT", "RARE", "CRSP", "BEAM"],     # gene therapy / small caps
    },
    
    # Copper + silver — base-metals "doctor copper" + monetary metals chain.
    # Crucial for electrification + AI buildout (copper) and currency
    # debasement plays (silver).
    "COPPER_SILVER": {
        1: ["FCX", "SCCO"],                              # copper majors
        2: ["TECK", "ERO", "IVPAF"],                     # mid-cap copper / diversified
        3: ["PAAS", "HL", "FSM"],                        # silver majors
        4: ["CDE", "FNV", "WPM"],                        # smaller silver + royalty
    },
    
    # Data-center REIT chain — owners → adjacent infra → equipment → networking.
    # Note VRT also appears in AI chain T3 (correctly — power+cooling is the
    # bottleneck for both AI compute and DC capacity)
    "DATACENTER": {
        1: ["EQIX", "DLR"],                              # pure-play DC REITs
        2: ["IRM", "AMT", "CCI"],                        # adjacent infrastructure REITs
        3: ["VRT", "NVT", "ETN"],                        # DC cooling + power equipment
        4: ["ANET", "JNPR", "FFIV"],                     # DC networking
    },
    
    # Lithium / battery materials. Upstream miners → pure-plays → EV consumers
    # → specialty / rare-earth alternatives. ALTM = Arcadium (LAC+Livent merger).
    "LITHIUM": {
        1: ["ALB", "SQM"],                               # lithium majors
        2: ["LAC", "PLL", "ALTM"],                       # pure-play lithium
        3: ["GM", "TSLA", "F"],                          # downstream EV buyers
        4: ["MP", "USAR"],                               # processors / rare earth
    },
}

# How far back to look for lead-lag
LOOKBACK_DAYS = 180
# Lag candidates (days)
LAG_CANDIDATES = [5, 10, 20, 40, 60]
# Min correlation to count as "established lead-lag"
MIN_CORRELATION = 0.4


def _get_json(url, timeout=HTTP_TIMEOUT):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return {"_err": str(e)[:200]}


def fmp_history(ticker: str, lookback_days: int = 200) -> list:
    """FMP historical price endpoint. Returns list of {date, close} desc."""
    url = f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={ticker}&apikey={FMP_KEY}"
    data = _get_json(url)
    if isinstance(data, dict) and "_err" in data:
        return []
    items = data if isinstance(data, list) else data.get("historical", [])
    out = []
    for r in items[:lookback_days]:
        try:
            out.append({"date": r["date"], "close": float(r["close"])})
        except Exception:
            continue
    out.sort(key=lambda x: x["date"], reverse=True)
    return out


def returns_window(history, days: int) -> float:
    """% return over last N trading days."""
    if not history or len(history) < days + 1:
        return None
    try:
        latest = history[0]["close"]
        prior  = history[days]["close"]
        if prior <= 0:
            return None
        return (latest - prior) / prior * 100
    except Exception:
        return None


def daily_returns(history) -> list:
    """Series of daily % returns, oldest first."""
    if not history or len(history) < 2:
        return []
    rev = list(reversed(history))  # oldest first
    out = []
    for i in range(1, len(rev)):
        try:
            r = (rev[i]["close"] - rev[i-1]["close"]) / rev[i-1]["close"]
            out.append(r)
        except Exception:
            out.append(0)
    return out


def pearson(xs, ys) -> float:
    """Pearson correlation. Returns None if undefined."""
    n = min(len(xs), len(ys))
    if n < 10:
        return None
    xs = xs[:n]; ys = ys[:n]
    try:
        mx = mean(xs); my = mean(ys)
        num = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
        denx = (sum((xs[i] - mx) ** 2 for i in range(n))) ** 0.5
        deny = (sum((ys[i] - my) ** 2 for i in range(n))) ** 0.5
        if denx * deny == 0:
            return None
        return num / (denx * deny)
    except Exception:
        return None


def lagged_correlation(leader_returns, follower_returns, lag_days: int) -> float:
    """Pearson(leader_returns[t-lag], follower_returns[t])."""
    if lag_days <= 0:
        return pearson(leader_returns, follower_returns)
    if len(leader_returns) <= lag_days or len(follower_returns) <= lag_days:
        return None
    # leader leads → align leader[0:n-lag] with follower[lag:n]
    n = min(len(leader_returns), len(follower_returns)) - lag_days
    if n < 10:
        return None
    return pearson(leader_returns[:n], follower_returns[lag_days:lag_days + n])


def chain_tier_aggregate(histories: dict, tickers: list) -> dict:
    """Aggregate price action across tier's tickers (equal-weight)."""
    if not tickers:
        return {}
    valid = [t for t in tickers if histories.get(t)]
    if not valid:
        return {}
    
    ret_30 = []
    ret_60 = []
    ret_90 = []
    daily_series_list = []
    
    for t in valid:
        h = histories[t]
        for window, container in ((30, ret_30), (60, ret_60), (90, ret_90)):
            r = returns_window(h, window)
            if r is not None:
                container.append(r)
        ds = daily_returns(h)
        if ds:
            daily_series_list.append(ds[-100:])  # last 100 days for correlation
    
    return {
        "tickers":          valid,
        "n_tickers":        len(valid),
        "avg_return_30d":   round(mean(ret_30), 2) if ret_30 else None,
        "avg_return_60d":   round(mean(ret_60), 2) if ret_60 else None,
        "avg_return_90d":   round(mean(ret_90), 2) if ret_90 else None,
        "daily_series":     daily_series_list,
    }


def aggregate_tier_daily(daily_series_list: list) -> list:
    """Average daily returns across all tickers in a tier."""
    if not daily_series_list:
        return []
    min_len = min(len(s) for s in daily_series_list)
    if min_len < 10:
        return []
    out = []
    for i in range(-min_len, 0):
        vals = [s[i] for s in daily_series_list if abs(i) <= len(s)]
        if vals:
            out.append(mean(vals))
    return out


def find_best_lag(leader_daily: list, follower_daily: list) -> dict:
    """Find the lag (in days) at which leader best predicts follower."""
    best = {"lag": None, "correlation": None}
    for lag in LAG_CANDIDATES:
        c = lagged_correlation(leader_daily, follower_daily, lag)
        if c is not None:
            if best["correlation"] is None or abs(c) > abs(best["correlation"]):
                best = {"lag": lag, "correlation": round(c, 3)}
    return best


def detect_next_up(histories: dict, leader_tier_data: dict,
                     follower_tickers: list, leader_perf: float) -> list:
    """For each ticker in the follower tier, identify those that haven't
    caught up to what the leader's performance implies."""
    if not leader_perf or not follower_tickers:
        return []
    
    candidates = []
    for t in follower_tickers:
        h = histories.get(t)
        if not h:
            continue
        own_perf_30d = returns_window(h, 30)
        own_perf_60d = returns_window(h, 60)
        if own_perf_30d is None:
            continue
        # Lag = how much LESS this ticker has moved vs leader
        lag_pct = leader_perf - own_perf_30d
        if lag_pct <= 0:
            continue  # already caught up
        # Score: bigger lag + leader is moving = better catchup opportunity
        score = min(100, lag_pct * 5)  # ~20pp lag → score 100
        candidates.append({
            "ticker":            t,
            "own_30d_pct":       round(own_perf_30d, 2),
            "own_60d_pct":       round(own_perf_60d, 2) if own_perf_60d else None,
            "leader_30d_pct":    round(leader_perf, 2),
            "lag_pct":           round(lag_pct, 2),
            "score":             round(score, 1),
        })
    
    candidates.sort(key=lambda r: -r["score"])
    return candidates[:8]


def analyze_chain(chain_name: str, chain_def: dict, histories: dict) -> dict:
    """Compute everything for one chain."""
    tier_data = {}
    for tier_num, tickers in sorted(chain_def.items()):
        tier_data[tier_num] = chain_tier_aggregate(histories, tickers)
        # Aggregate daily returns for cross-tier lead-lag
        if tier_data[tier_num].get("daily_series"):
            tier_data[tier_num]["aggregated_daily"] = \
                aggregate_tier_daily(tier_data[tier_num]["daily_series"])
    
    # Find current leader: tier with highest 30d avg return
    perf_by_tier = {n: d.get("avg_return_30d") for n, d in tier_data.items()
                     if d.get("avg_return_30d") is not None}
    if not perf_by_tier:
        return {"chain": chain_name, "status": "no_data"}
    
    current_leader = max(perf_by_tier.keys(), key=lambda n: perf_by_tier[n])
    leader_perf = perf_by_tier[current_leader]
    
    # Compute lead-lag relationships
    lead_lag = {}
    if current_leader in tier_data and tier_data[current_leader].get("aggregated_daily"):
        leader_daily = tier_data[current_leader]["aggregated_daily"]
        for n, d in tier_data.items():
            if n <= current_leader:
                continue
            follower_daily = d.get("aggregated_daily")
            if follower_daily:
                lag_info = find_best_lag(leader_daily, follower_daily)
                if lag_info["correlation"] and abs(lag_info["correlation"]) >= MIN_CORRELATION:
                    lead_lag[f"{current_leader}_to_{n}"] = lag_info
    
    # Find next-up tier candidates  
    next_tier_n = current_leader + 1
    next_up_tickers = []
    if next_tier_n in chain_def:
        next_up_tickers = detect_next_up(
            histories,
            tier_data[current_leader],
            chain_def[next_tier_n],
            leader_perf,
        )
    
    return {
        "chain":                chain_name,
        "current_leader_tier":  current_leader,
        "leader_perf_30d_pct":  round(leader_perf, 2),
        "next_tier":            next_tier_n if next_tier_n in chain_def else None,
        "next_tier_perf_30d":   tier_data.get(next_tier_n, {}).get("avg_return_30d"),
        "expected_catchup_pct": round(
            leader_perf - (tier_data.get(next_tier_n, {}).get("avg_return_30d") or 0),
            2,
        ),
        "next_up_tickers":      next_up_tickers,
        "tier_returns_30d":     {n: d.get("avg_return_30d") for n, d in tier_data.items()},
        "lead_lag_relations":   lead_lag,
        "rotation_state": (
            "ROTATING" if next_up_tickers and
            (leader_perf - (tier_data.get(next_tier_n, {}).get("avg_return_30d") or 0)) >= 5
            else "SYNCHRONIZED"
            if abs(leader_perf - (tier_data.get(next_tier_n, {}).get("avg_return_30d") or leader_perf)) < 3
            else "DIVERGING"
        ),
    }


@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)
    
    # Collect all unique tickers across chains
    all_tickers = set()
    for chain in VALUE_CHAINS.values():
        for tickers in chain.values():
            all_tickers.update(tickers)
    print(f"[rotation] {len(VALUE_CHAINS)} chains, {len(all_tickers)} unique tickers")
    
    # Fetch all price histories
    histories = {}
    for i, t in enumerate(sorted(all_tickers)):
        h = fmp_history(t, lookback_days=LOOKBACK_DAYS)
        if h:
            histories[t] = h
        time.sleep(0.1)  # gentle pacing
        if (i + 1) % 20 == 0:
            print(f"[rotation]   loaded {len(histories)}/{len(all_tickers)}")
    print(f"[rotation] loaded {len(histories)}/{len(all_tickers)} histories")
    
    # Analyze each chain
    results = {}
    for chain_name, chain_def in VALUE_CHAINS.items():
        try:
            results[chain_name] = analyze_chain(chain_name, chain_def, histories)
            r = results[chain_name]
            print(f"[rotation]   {chain_name}: tier-{r.get('current_leader_tier')} leading "
                  f"@ {r.get('leader_perf_30d_pct')}%  state={r.get('rotation_state')}")
        except Exception as e:
            print(f"[rotation] err on {chain_name}: {e}")
            results[chain_name] = {"chain": chain_name, "err": str(e)[:200]}
    
    # Cross-chain leaderboard of next-up tickers
    all_next_up = []
    for chain_name, r in results.items():
        for t in r.get("next_up_tickers", []) or []:
            all_next_up.append({**t, "chain": chain_name,
                                  "leader_tier": r.get("current_leader_tier")})
    all_next_up.sort(key=lambda r: -r["score"])
    
    out = {
        "schema_version":  "1.0",
        "method":          "rotation_chain_v1",
        "generated_at":    datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s":      round((datetime.now(timezone.utc) - started).total_seconds(), 1),
        "n_chains":        len(VALUE_CHAINS),
        "n_tickers":       len(histories),
        "lag_candidates":  LAG_CANDIDATES,
        "chains":          results,
        "top_next_up":     all_next_up[:25],
        "notes": (
            "Lead-lag relationships measured via Pearson correlation of tier-N's "
            "daily returns at lag d with tier-(N+1)'s daily returns. Strongest |corr| "
            "across lag candidates {5,10,20,40,60} days wins. Next-up tickers = "
            "those in tier-(leader+1) lagging the leader's 30d return."
        ),
    }
    
    body = json.dumps(out, default=str, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=body,
                  ContentType="application/json",
                  CacheControl="public, max-age=7200")
    print(f"[rotation] wrote {len(body):,}B")
    
    # Emit events for ROTATING chains (operator wants to know)
    try:
        from system_events import publish_many
        events_to_pub = []
        for chain_name, r in results.items():
            if r.get("rotation_state") == "ROTATING" and r.get("next_up_tickers"):
                events_to_pub.append(("rotation.next_up", {
                    "chain":              chain_name,
                    "leader_tier":        r.get("current_leader_tier"),
                    "leader_perf_30d":    r.get("leader_perf_30d_pct"),
                    "expected_catchup":   r.get("expected_catchup_pct"),
                    "top_3_next_up":      [t["ticker"] for t in r["next_up_tickers"][:3]],
                }))
        for i in range(0, len(events_to_pub), 10):
            publish_many(events_to_pub[i:i+10])
    except Exception as e:
        print(f"[rotation] event publish failed: {e}")
    
    return {"statusCode": 200, "body": json.dumps({
        "ok": True,
        "n_chains": len(VALUE_CHAINS),
        "n_rotating": sum(1 for r in results.values() if r.get("rotation_state") == "ROTATING"),
        "top_next_up": all_next_up[0]["ticker"] if all_next_up else None,
        "duration_s": out["duration_s"],
    })}


lambda_handler = handler
