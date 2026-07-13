"""ops 3184 — DATA GAP ANALYSIS: what is missing, and what it would BUY.

Khalid asked what sources he needs to add and what they cost. The wrong
answer is a vendor list. The right answer is: for each gap, how many of
HIS 207 engines does closing it actually activate — because a gap that
unlocks 40 engines is worth paying for and one that unlocks 2 is not.

For every unmapped symbol: bucket by namespace, count it, and compute the
ENGINE IMPACT — how many currently-DORMANT engines would cross the >=6
resolved-member threshold if that bucket alone were resolved.
"""

import json
import re
import sys
from collections import Counter, defaultdict

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
MIN_MEMBERS = 6


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3184_gap_analysis") as rep:
    fails, warns = [], []
    rep.heading("ops 3184 — what data is missing, and what it would buy")

    wl = s3_json("data/tv-watchlists.json") or {}
    lists = [l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    smap = (s3_json("data/symbol-map.json") or {}).get("map") or {}
    idx = s3_json("data/wl-engines.json") or {}
    eng_state = {e.get("name"): e.get("state")
                 for e in (idx.get("engines") or [])}

    def bucket(sym):
        s = sym.upper()
        if re.search(r"[+\-*/()]", s):
            return "FORMULA (composite)"
        if ":" not in s:
            return "BARE TICKER"
        ex = s.split(":", 1)[0]
        if ex == "ECONOMICS":
            m = re.match(r"^[A-Z]{2}([A-Z0-9]+)$", s.split(":", 1)[1])
            return f"ECONOMICS:{m.group(1)}" if m else "ECONOMICS:?"
        return ex

    uniq = sorted({s.upper() for l in lists for s in (l.get("symbols") or [])})
    unmapped = [s for s in uniq if s not in smap]
    rep.kv(symbols=len(uniq), mapped=len(uniq) - len(unmapped),
           unmapped=len(unmapped),
           coverage_pct=round(100 * (len(uniq) - len(unmapped)) / len(uniq), 1))

    rep.section("1. The gap, bucketed")
    buckets = Counter(bucket(s) for s in unmapped)
    for b, n in buckets.most_common(22):
        rep.log(f"  {b:26s} {n:5d} symbols")

    rep.section("2. ENGINE IMPACT — what each bucket would ACTIVATE")
    # per dormant engine: current resolved members + what each bucket adds
    gain = defaultdict(set)          # bucket -> engines it would activate
    partial = defaultdict(int)       # bucket -> engines it would help
    for l in lists:
        syms = [s.upper() for s in (l.get("symbols") or [])][:120]
        have = sum(1 for s in syms if s in smap)
        if have >= MIN_MEMBERS:
            continue                 # already ACTIVE
        by_b = Counter(bucket(s) for s in syms if s not in smap)
        for b, add in by_b.items():
            if have + add >= MIN_MEMBERS:
                gain[b].add(l.get("name"))
            elif add > 0:
                partial[b] += 1
    rows = sorted(((b, len(v), buckets.get(b, 0), partial.get(b, 0))
                   for b, v in gain.items()),
                  key=lambda r: -r[1])
    rep.log("  bucket                     symbols  ENGINES IT ACTIVATES  helps")
    for b, act, nsym, helps in rows[:18]:
        rep.log(f"  {b:26s} {nsym:5d}   {act:4d} engines        {helps}")
    top = rows[:8]
    rep.kv(**{f"activates_{re.sub(r'[^a-z0-9]+','_',b.lower())[:22]}": n
              for b, n, _, _ in top})

    rep.section("3. Named examples per gap (so he can judge worth)")
    dic = (s3_json("data/symbol-dictionary.json") or {}).get("dictionary") or {}
    for b, act, nsym, _ in rows[:8]:
        ex = [s for s in unmapped if bucket(s) == b][:4]
        names = []
        for s in ex:
            nm = (dic.get(s) or {}).get("name") or s
            names.append(str(nm)[:34])
        rep.log(f"  {b:24s} → {', '.join(names)}")

    rep.section("4. What is already free (do not pay for these)")
    free_note = {
        "COT3": "CFTC publishes Commitments of Traders FREE — the platform "
                "ALREADY has justhodl-cftc-cot (29 contracts)",
        "ECONOMICS": "World Bank / IMF / OECD / Eurostat / DBnomics are FREE "
                     "— more mapping, not more money",
        "FRED": "already free",
        "USI": "US index internals — mostly derivable from free breadth data",
        "INDEX": "most map to Yahoo/Stooq free tickers",
    }
    for k, v in free_note.items():
        hits = sum(n for b, n in buckets.items() if b.startswith(k))
        if hits:
            rep.log(f"  {k:12s} {hits:5d} symbols — {v}")

    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
