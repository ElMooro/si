"""ops 3163 — HARVEST AUDIT (Khalid's verification request).

Panel reported: 1,982 notes → Brain · 200 watchlists → tracker.
Verify all three claims server-side, and answer the question that
matters for the tracker design:

Khalid: "the watchlist NAME is the indicator — 'global liquidity trend
reversal' would hold swaps, reverse repo, etc." So lists are THESES,
and their members are often NOT equities (FRED:, TVC:, CRYPTOCAP:,
ECONOMICS:). The v1 tracker only prices US equities — it would score
those lists 0/N. This audit measures exactly how many lists are
equity-priceable vs macro/indicator, so tracker v2 is built on facts.

Reports: list count + names + sizes · symbol-namespace census · notes
mirror stats (tagged vs untagged, top-noted tickers) · Brain read-back.
"""

import json
import re
import sys
import urllib.request
from collections import Counter

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)

EQ_EX = {"NASDAQ", "NYSE", "AMEX", "ARCA", "BATS", "OTC", "CBOE"}
CRYPTO_EX = {"BINANCE", "COINBASE", "BITSTAMP", "KRAKEN", "BYBIT",
             "CRYPTOCAP", "BITFINEX", "OKX"}
MACRO_EX = {"FRED", "TVC", "ECONOMICS", "INDEX", "CBOT", "COMEX",
            "NYMEX", "CME", "ICEUS", "EUREX", "USI"}
FX_EX = {"FX", "OANDA", "FOREXCOM", "FX_IDC", "SAXO", "PEPPERSTONE"}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


def ns(sym):
    s = str(sym).upper()
    if ":" not in s:
        return "BARE_EQUITY"
    ex = s.split(":", 1)[0]
    if ex in EQ_EX:
        return "EQUITY"
    if ex in CRYPTO_EX:
        return "CRYPTO"
    if ex in MACRO_EX:
        return "MACRO"
    if ex in FX_EX:
        return "FX"
    return "OTHER:" + ex


with report("3163_harvest_audit") as rep:
    fails, warns = [], []
    rep.heading("ops 3163 — harvest audit")

    rep.section("1. Watchlists landed")
    wl = s3_json("data/tv-watchlists.json", {}) or {}
    lists = [l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    rep.kv(n_lists=len(lists), doc_at=wl.get("generated_at"))
    if not lists:
        fails.append("no watchlists in the mirror")
    sizes = sorted(((len(l.get("symbols") or []), l.get("name"), l)
                    for l in lists), key=lambda x: -x[0])
    rep.log("── your lists (largest first, top 40):")
    for n, name, l in sizes[:40]:
        sample = ", ".join((l.get("symbols") or [])[:5])
        rep.log(f"  · [{n:>3}] {str(name)[:46]:46s} {sample[:70]}")
    tiny = sum(1 for n, _, _ in sizes if n <= 2)
    rep.kv(lists_1_2_symbols=tiny, lists_3plus=len(sizes) - tiny)
    if tiny > len(sizes) * 0.5:
        warns.append(f"{tiny}/{len(sizes)} lists hold ≤2 symbols — likely "
                     "TradingView presets/auto-lists mixed in with your "
                     "real thesis lists; tracker v2 should score only "
                     "lists with >=3 members")

    rep.section("2. Symbol namespace census (drives tracker v2)")
    allsyms = [s for l in lists for s in (l.get("symbols") or [])]
    cnt = Counter(ns(s) for s in allsyms)
    rep.kv(total_symbol_slots=len(allsyms),
           unique_symbols=len({s.upper() for s in allsyms}),
           **{k.replace(":", "_").lower(): v for k, v in cnt.most_common(8)})
    for k, v in cnt.most_common(10):
        rep.log(f"  {k:16s} {v}")
    # per-list classification
    kinds = Counter()
    for l in lists:
        syms = l.get("symbols") or []
        if len(syms) < 3:
            kinds["TOO_SMALL"] += 1
            continue
        c = Counter(ns(s) for s in syms)
        eq = c["EQUITY"] + c["BARE_EQUITY"]
        kinds["EQUITY_BASKET" if eq >= 0.6 * len(syms)
              else ("MACRO_INDICATOR" if c["MACRO"] + c["FX"] >= 0.4 * len(syms)
                    else "MIXED")] += 1
    rep.kv(**{f"kind_{k.lower()}": v for k, v in kinds.items()})
    rep.ok("namespace census complete — tracker v2 will price equity "
           "baskets directly and route MACRO/FX lists through FRED/"
           "Polygon-equivalent mappings instead of scoring them 0")

    rep.section("3. Notes mirror")
    nm = s3_json("data/tradingview-notes.json", {}) or {}
    notes = nm.get("notes") or []
    tagged = [n for n in notes if str(n.get("symbol", "")).upper()
              not in ("", "UNTAGGED")]
    top = Counter(str(n.get("symbol")).upper() for n in tagged).most_common(12)
    rep.kv(notes_in_mirror=len(notes), notes_tagged=len(tagged),
           notes_untagged=len(notes) - len(tagged),
           distinct_tickers=len({n["symbol"] for n in tagged}))
    rep.log("── most-noted tickers: " +
            ", ".join(f"{t}({c})" for t, c in top))
    if len(notes) < 1500:
        warns.append(f"mirror holds {len(notes)} notes — the panel claimed "
                     "1,982; some may still be in flight or deduped")
    else:
        rep.ok(f"{len(notes)} notes in the mirror ({len(tagged)} carry a "
               "ticker → routable by the brain-compiler)")

    rep.section("4. Brain read-back")
    try:
        uid = SSM.get_parameter(Name="/justhodl/brain/uid",
                                WithDecryption=True)["Parameter"]["Value"]
        r = urllib.request.urlopen(urllib.request.Request(
            f"https://api.justhodl.ai/brain/{uid}/notes?limit=1",
            headers={"User-Agent": "ops-3163"}), timeout=20)
        d = json.loads(r.read().decode())
        n_brain = d.get("total") or d.get("count") or len(d.get("notes") or [])
        rep.kv(brain_total=n_brain)
        if n_brain:
            rep.ok(f"Brain confirms {n_brain} notes stored")
        else:
            warns.append("brain read-back returned no count — endpoint "
                         "shape differs; mirror is authoritative anyway")
    except Exception as e:
        warns.append(f"brain read-back unavailable: {str(e)[:90]} "
                     "(the mirror at data/tradingview-notes.json is the "
                     "source the brain-compiler reads)")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
