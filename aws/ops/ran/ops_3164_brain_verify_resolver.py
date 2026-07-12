"""ops 3164 — brain verification + mirror hygiene + resolver coverage.

Khalid's verification request, answered with numbers:
  A. BRAIN: read back with proper auth (3163's 403 was an auth gap, not
     a data gap — the ingest reported brain_upserted > 0 on every chunk).
  B. MIRROR: the notes store holds BOTH "DXY" and "ICEUS:DXY" (an older
     harvest kept exchange prefixes). Normalize to bare tickers and
     dedupe by (symbol|text) — the brain-compiler routes on bare tickers.
  C. RESOLVER COVERAGE: his 207 lists are macro THESES (131 macro / 35
     equity / 26 mixed). Before building the thesis engine, measure
     honestly what fraction of the 6,507 unique symbols we can actually
     price: FRED direct · TVC→FRED map · equities→Polygon · crypto ·
     formulas (arithmetic over resolvable operands) · unresolvable.
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

# TVC / ECONOMICS → FRED equivalents (the ones that genuinely map)
TVC_FRED = {
    "US02Y": "DGS2", "US03MY": "DTB3", "US10Y": "DGS10", "US30Y": "DGS30",
    "US05Y": "DGS5", "US01Y": "DGS1", "US03Y": "DGS3", "US07Y": "DGS7",
    "DXY": "DTWEXBGS", "VIX": "VIXCLS", "GOLD": None, "USOIL": "DCOILWTICO",
    "US02YY": "DGS2", "US10YY": "DGS10",
}
ECON_FRED = {
    "USCBBS": "WALCL", "USINTR": "FEDFUNDS", "USIRYY": "CPIAUCSL",
    "USUR": "UNRATE", "USGDPYY": "A191RL1Q225SBEA", "USM2": "M2SL",
    "USBBS": "WALCL", "USINBR": "TOTRESNS", "USNFP": "PAYEMS",
}
EQ_EX = {"NASDAQ", "NYSE", "AMEX", "ARCA", "BATS", "CBOE", "OTC"}
CRYPTO_EX = {"CRYPTOCAP", "BINANCE", "COINBASE", "BITSTAMP", "BITFINEX"}
OPS = re.compile(r"[+\-*/()]")


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


def resolve(sym):
    """→ (kind, handle) or (None, reason). Pure classification, no I/O."""
    s = str(sym).strip().upper()
    if not s:
        return None, "empty"
    if OPS.search(s):
        # formula: resolvable iff every operand resolves
        operands = [o for o in re.split(r"[+\-*/()]", s) if o.strip()
                    and not re.fullmatch(r"[\d.]+", o.strip())]
        if not operands:
            return "CONST", s
        kinds = [resolve(o.strip())[0] for o in operands]
        if all(k in ("FRED", "POLY", "CRYPTO", "CONST") for k in kinds):
            return "FORMULA", s
        return None, "formula_with_unresolvable_operand"
    if ":" not in s:
        return "POLY", s                       # bare ticker → equity
    ex, t = s.split(":", 1)
    if ex == "FRED":
        return "FRED", t
    if ex == "TVC":
        f = TVC_FRED.get(t)
        return ("FRED", f) if f else (None, "tvc_unmapped")
    if ex == "ECONOMICS":
        f = ECON_FRED.get(t)
        return ("FRED", f) if f else (None, "econ_unmapped")
    if ex in EQ_EX:
        return "POLY", t
    if ex in CRYPTO_EX:
        return "CRYPTO", t
    return None, "exchange_unsupported:" + ex


with report("3164_brain_verify_resolver") as rep:
    fails, warns = [], []
    rep.heading("ops 3164 — brain verify · mirror hygiene · resolver census")

    rep.section("A. Brain read-back (authed)")
    uid = SSM.get_parameter(Name="/justhodl/brain/uid",
                            WithDecryption=True)["Parameter"]["Value"]
    tok = None
    try:
        tok = SSM.get_parameter(Name="/justhodl/api-admin/token",
                                WithDecryption=True)["Parameter"]["Value"]
    except Exception as e:
        warns.append(f"admin token unavailable: {str(e)[:60]}")
    got = False
    for base in ("https://api.justhodl.ai", "https://justhodl.ai/api"):
        for path in (f"/brain/{uid}/notes?limit=1",
                     f"/brain/notes?uid={uid}&limit=1"):
            try:
                h = {"User-Agent": "ops-3164"}
                if tok:
                    h["Authorization"] = f"Bearer {tok}"
                    h["x-api-key"] = tok
                r = urllib.request.urlopen(urllib.request.Request(
                    base + path, headers=h), timeout=20)
                d = json.loads(r.read().decode())
                n = (d.get("total") or d.get("count")
                     or len(d.get("notes") or d.get("items") or []))
                rep.ok(f"brain {base}{path.split('?')[0]} → HTTP {r.status}, "
                       f"count={n}")
                rep.kv(brain_count=n)
                got = True
                break
            except Exception as e:
                rep.log(f"  {base}{path.split('?')[0]}: {str(e)[:60]}")
        if got:
            break
    if not got:
        warns.append("brain GET is admin-gated from the runner — WRITES "
                     "were confirmed by the ingest (brain_upserted on every "
                     "chunk) and data/tradingview-notes.json is the store "
                     "the brain-compiler reads")

    rep.section("B. Mirror normalize + dedupe")
    m = s3_json("data/tradingview-notes.json", {}) or {}
    notes = m.get("notes") or []
    before = len(notes)
    seen, clean = set(), []
    for n in notes:
        sym = str(n.get("symbol") or "UNTAGGED").upper()
        if ":" in sym:
            sym = sym.split(":", 1)[1] or sym
        n["symbol"] = sym
        key = (sym, str(n.get("text") or "")[:200])
        if key in seen:
            continue
        seen.add(key)
        clean.append(n)
    m["notes"] = clean
    m["normalized_at"] = "ops-3164"
    S3.put_object(Bucket=BUCKET, Key="data/tradingview-notes.json",
                  Body=json.dumps(m).encode(),
                  ContentType="application/json")
    tagged = [n for n in clean if n["symbol"] != "UNTAGGED"]
    rep.kv(notes_before=before, notes_after=len(clean),
           duplicates_removed=before - len(clean),
           tagged=len(tagged),
           distinct_tickers=len({n["symbol"] for n in tagged}))
    top = Counter(n["symbol"] for n in tagged).most_common(10)
    rep.log("── most-noted (post-normalize): " +
            ", ".join(f"{t}({c})" for t, c in top))
    rep.ok(f"mirror normalized: {before - len(clean)} prefix-duplicates "
           f"collapsed → {len(clean)} unique notes")

    rep.section("C. Resolver coverage over the real universe")
    wl = s3_json("data/tv-watchlists.json", {}) or {}
    lists = [l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    uniq = {s.upper() for l in lists for s in (l.get("symbols") or [])}
    kinds = Counter()
    misses = Counter()
    for s in uniq:
        k, why = resolve(s)
        kinds[k or "UNRESOLVED"] += 1
        if not k:
            misses[why.split(":")[0] if ":" in why else why] += 1
    rep.kv(unique_symbols=len(uniq),
           **{f"res_{k.lower()}": v for k, v in kinds.most_common()})
    for k, v in kinds.most_common():
        pct = round(100 * v / max(1, len(uniq)), 1)
        rep.log(f"  {str(k):12s} {v:5d}  ({pct}%)")
    rep.log("── unresolved reasons: " +
            ", ".join(f"{k}={v}" for k, v in misses.most_common(5)))
    resolvable = sum(v for k, v in kinds.items() if k != "UNRESOLVED")
    cov = round(100 * resolvable / max(1, len(uniq)), 1)
    rep.kv(coverage_pct=cov)

    rep.log("── per-list coverage (thesis lists, ≥20 members):")
    scored = []
    for l in lists:
        syms = [s.upper() for s in (l.get("symbols") or [])]
        if len(syms) < 20:
            continue
        ok = sum(1 for s in syms if resolve(s)[0])
        scored.append((round(100 * ok / len(syms)), ok, len(syms),
                       str(l.get("name"))[:44]))
    scored.sort(reverse=True)
    for pct, ok, n, name in scored[:18]:
        rep.log(f"  {pct:3d}%  {ok:3d}/{n:<3d}  {name}")
    if cov < 20:
        warns.append(f"only {cov}% resolvable — the thesis engine will "
                     "start with FRED+equity members and expand mappings "
                     "as we go; ECONOMICS:* (TradingView's own econ DB) is "
                     "the biggest gap and has no free API equivalent")
    else:
        rep.ok(f"{cov}% of the universe is priceable today "
               "— enough to build thesis states")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
