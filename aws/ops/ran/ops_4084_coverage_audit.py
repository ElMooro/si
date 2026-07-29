"""ops_4084 — AUDIT (no build): can the fleet FETCH each watchlist ticker?

Khalid's actual objective, restated: the TradingView import brought in
~9,886 tickers with notes. The point of finding each ticker's data source
was never the label — it was so the system can PULL that series. So the
question that matters is not "who publishes this" but:

    for every imported ticker, does this fleet have a working fetch route,
    and if not, what source would give it one?

This audits that end-to-end and writes no engine code:
  A. the true ticker universe from the imported watchlists, by namespace
  B. what the vault resolves TODAY (adapter by adapter)
  C. which namespaces are ALREADY fetchable through existing fleet keys
     (FMP/Polygon for listed equities, FRED for macro, etc.) even where
     the vault has no row — a gap in the vault is not a gap in capability
  D. the residual: tickers with no route at all, ranked by namespace, so
     the build targets the biggest real gap rather than the loudest one
"""
import json, os, sys
from collections import Counter, defaultdict
from pathlib import Path
import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

def gj(k, d=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception: return d

# Namespaces the fleet can already fetch without any new integration.
EQUITY_VENUES = {"NASDAQ","NYSE","AMEX","ARCA","BATS","OTC","LSE","EURONEXT",
                 "XETR","FWB","SIX","BME","MIL","OMXSTO","OMXCOP","OMXHEX",
                 "TSX","TSXV","ASX","BSE","NSE","HKEX","SSE","SZSE","KRX",
                 "TWSE","TPEX","SGX","IDX","SET","MOEX","BIST","JSE","B3",
                 "BMV","BVL","BCBA","GPW","VIE","PSE","HOSE","ICEUS","CBOT",
                 "COMEX","NYMEX","CME","EUREX","ICEEUR"}
CRYPTO = {"BINANCE","COINBASE","KRAKEN","BITSTAMP","BYBIT","OKX","BITFINEX",
          "CRYPTOCAP","BITGET","KUCOIN","GATEIO","MEXC","HTX","UPBIT"}
FX_IDX = {"FX","FX_IDC","OANDA","FOREXCOM","TVC","SPCFD","INDEX","DJ","CAPITALCOM"}
MACRO  = {"ECONOMICS","FRED","QUANDL","EIA","USI","COT","COT3"}

def main():
    with report("4084_coverage_audit") as rep:
        rep.heading("ops 4084 — AUDIT: is every imported ticker FETCHABLE?")

        # ── A. the universe ──
        rep.section("A. imported ticker universe")
        wl = gj("data/tv-watchlists.json", {}) or {}
        lists = wl.get("watchlists") or wl.get("lists") or []
        seen, order = set(), []
        per_list = []
        for l in lists:
            syms = [str(x).strip() for x in (l.get("symbols") or []) if str(x).strip()]
            per_list.append((l.get("name") or "(unnamed)", len(syms)))
            for x in syms:
                if x not in seen:
                    seen.add(x); order.append(x)
        rep.log(f"  watchlists: {len(lists)}   unique tickers: {len(order)}")
        for n, c in sorted(per_list, key=lambda t: -t[1])[:14]:
            rep.log(f"    {c:6d}  {n}")
        ns = Counter(x.split(":")[0].upper() if ":" in x else "(bare)" for x in order)
        rep.log(f"  distinct namespaces: {len(ns)}")
        rep.kv(watchlists=len(lists), unique_tickers=len(order), namespaces=len(ns))

        rep.section("B. namespace breakdown (top 25)")
        for p, n in ns.most_common(25):
            rep.log(f"  {n:6d}  {p}")

        # ── C. what the vault resolves today ──
        rep.section("C. vault: what is fetchable RIGHT NOW")
        vault = gj("data/tradingview.json", {}) or {}
        vrows = vault.get("symbols") or []
        live = [r for r in vrows if str(r.get("status")).upper() == "LIVE"]
        vsyms = {str(r.get("symbol") or "").upper() for r in live}
        adapters = Counter(str(r.get("resolved_via") or "").split(":")[0] or "(none)"
                           for r in live)
        rep.log(f"  vault rows {len(vrows)}   LIVE {len(live)}")
        rep.log("  adapters: " + ", ".join(f"{k} {v}" for k, v in adapters.most_common(20)))
        def bare(x): return x.split(":",1)[1].upper() if ":" in x else x.upper()
        vault_hit = [x for x in order if bare(x) in vsyms]
        rep.log(f"  imported tickers with a LIVE vault row: {len(vault_hit)}")
        rep.kv(vault_rows=len(vrows), vault_live=len(live), vault_covered=len(vault_hit))

        # ── D. capability by namespace, not just vault presence ──
        rep.section("D. route classification for every imported ticker")
        route = {}
        for x in order:
            p = x.split(":")[0].upper() if ":" in x else "(bare)"
            if bare(x) in vsyms:            route[x] = "vault-live"
            elif p in EQUITY_VENUES:        route[x] = "equity-api"      # FMP/Polygon
            elif p in CRYPTO:               route[x] = "crypto-api"
            elif p in FX_IDX:               route[x] = "fx-index-api"
            elif p in MACRO:                route[x] = "macro-needs-mapping"
            else:                           route[x] = "UNROUTED"
        rc = Counter(route.values())
        for k, v in rc.most_common():
            rep.log(f"  {v:6d}  {k}")
        fetchable = sum(v for k, v in rc.items()
                        if k in ("vault-live","equity-api","crypto-api","fx-index-api"))
        rep.log(f"  → plausibly fetchable today : {fetchable} "
                f"({fetchable/max(len(order),1)*100:.1f}%)")
        rep.log(f"  → macro needing a series map: {rc.get('macro-needs-mapping',0)}")
        rep.log(f"  → no route at all           : {rc.get('UNROUTED',0)}")
        rep.kv(fetchable=fetchable, macro_gap=rc.get("macro-needs-mapping",0),
               unrouted=rc.get("UNROUTED",0))

        rep.section("E. the UNROUTED tail — which namespaces?")
        un = Counter(x.split(":")[0].upper() if ":" in x else "(bare)"
                     for x, r in route.items() if r == "UNROUTED")
        for p, n in un.most_common(20):
            rep.log(f"  {n:6d}  {p}")
        rep.log("  sample: " + ", ".join([x for x, r in route.items()
                                          if r == "UNROUTED"][:12]))

        # ── F. how much of the macro gap is already solved by ledger ──
        rep.section("F. macro: attribution ledger vs fetchability")
        ma = gj("data/macro-attribution.json", {}) or {}
        attr = ma.get("attribution") or {}
        rep.log(f"  macro symbols attributed so far: {len(attr)}")
        # the ones attributed via a FRED route are FETCHABLE (we know the id)
        fredish = [k for k, v in attr.items()
                   if v.get("route") in ("fred-metadata","vault-fred")]
        rep.log(f"  ...of which carry a FRED series id (=> FETCHABLE): {len(fredish)}")
        rep.log("  NOTE: a FRED-routed macro symbol is not just labelled, it is")
        rep.log("  pullable — the series id is the fetch key. That is the bridge")
        rep.log("  from 'who publishes this' to 'my system can chart this'.")
        rep.kv(macro_attributed=len(attr), macro_fetchable=len(fredish))

        rep.section("VERDICT — where the real work is")
        rep.log(f"  universe {len(order)} · fetchable-ish {fetchable} · "
                f"macro-gap {rc.get('macro-needs-mapping',0)} · "
                f"unrouted {rc.get('UNROUTED',0)}")
        rep.log("  AUDIT ONLY — no engine code written.")

if __name__ == "__main__":
    main()
