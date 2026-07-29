"""ops_4082 — PROBE (writes no engine code) for the macro attribution join.

ops 4081 settled that TradingView returns source=null for every macro
symbol, so attribution must come from the fleet, not the browser.  Before
building the join I need three measurements, because the tempting version
of this feature is one that MANUFACTURES attribution:

  H1 VAULT COVERAGE. gov-sources already maps vault rows to agencies via
     a resolved_via prefix (boj:/fred:/ecb:/...).  But the vault's symbol
     names are bare codes (JPLG, US02MY) while the watchlists carry
     ECONOMICS:JPM3.  How many macro symbols does the vault ACTUALLY
     cover once matched properly?  A guess here becomes a fake number.

  H2 FRED AS AUTHORITATIVE SOURCE. FRED's API exposes the real
     publishing agency:  series -> release -> sources.  MABMM301JPM189S
     should name an actual institution.  If that works, every FRED:
     symbol gets REAL attribution from the publisher's own metadata
     rather than inference.  Tested live here on real series.

  H3 THE TEMPTATION. ECONOMICS:JPM3's description is "Japan Money Supply
     M3", which a country+topic heuristic would happily map to the Bank
     of Japan.  That is inference dressed as attribution, and this fleet
     has already shipped one false confirmation that way.  Measured here
     so the wire op can EXCLUDE it deliberately rather than by omission.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def gj(k, d=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception:
        return d


def main():
    with report("4082_macro_join_probe") as rep:
        rep.heading("ops 4082 — PROBE: macro attribution join")

        # ── H1: what does the vault actually contain? ──
        rep.section("H1 — vault shape and coverage")
        vault = gj("data/tradingview.json", {}) or {}
        rows = vault.get("symbols") or []
        rep.log(f"  vault rows: {len(rows)}")
        if rows:
            rep.log(f"  sample row keys: {sorted(rows[0].keys())[:14]}")
            for r in rows[:6]:
                rep.log(f"    {str(r.get('symbol'))[:18]:18} "
                        f"resolved_via={str(r.get('resolved_via'))[:38]:38} "
                        f"status={r.get('status')}")
        rv_pfx = Counter(str(r.get("resolved_via") or "").split(":")[0]
                         for r in rows)
        rep.log("  resolved_via prefixes: " + ", ".join(
            f"{k or '(none)'} {v}" for k, v in rv_pfx.most_common(16)))
        vault_syms = {str(r.get("symbol") or "").upper() for r in rows}
        rep.kv(vault_rows=len(rows), distinct_prefixes=len(rv_pfx))

        # ── the macro symbols we actually need to attribute ──
        wl = gj("data/tv-watchlists.json", {}) or {}
        lists = wl.get("watchlists") or wl.get("lists") or []
        macro = set()
        for l in lists:
            for x in (l.get("symbols") or []):
                x = str(x).strip().upper()
                if x.startswith(("ECONOMICS:", "FRED:")):
                    macro.add(x)
        econ = {m for m in macro if m.startswith("ECONOMICS:")}
        fred = {m for m in macro if m.startswith("FRED:")}
        rep.log(f"  macro symbols in watchlists: {len(macro)} "
                f"(ECONOMICS {len(econ)}, FRED {len(fred)})")

        # honest overlap: bare code match against vault symbols
        def bare(x):
            return x.split(":", 1)[1] if ":" in x else x
        hit = {m for m in macro if bare(m) in vault_syms}
        rep.log(f"  macro symbols matching a vault row: {len(hit)} "
                f"({len(hit) / max(len(macro), 1) * 100:.1f}%)")
        for m in sorted(hit)[:10]:
            rep.log(f"    ✓ {m}")
        rep.kv(macro_total=len(macro), macro_econ=len(econ),
               macro_fred=len(fred), vault_hits=len(hit))

        # ── H2: FRED release -> sources gives the REAL publisher ──
        rep.section("H2 — is FRED's own metadata authoritative?")
        key = None
        try:
            env = lam.get_function_configuration(
                FunctionName="justhodl-tv-workbench").get(
                "Environment", {}).get("Variables", {})
            key = env.get("FRED_KEY")
        except Exception as e:
            rep.log(f"  env read failed: {str(e)[:60]}")
        rep.log(f"  FRED_KEY available: {bool(key)}")

        if key:
            tests = [bare(m) for m in sorted(fred)[:3]] or \
                    ["MABMM301JPM189S", "JPNASSETS", "GDP"]
            for sid in tests:
                try:
                    u = ("https://api.stlouisfed.org/fred/series/release?"
                         + urllib.parse.urlencode(
                             {"series_id": sid, "api_key": key,
                              "file_type": "json"}))
                    rel = json.loads(urllib.request.urlopen(u, timeout=25)
                                     .read())
                    rid = (rel.get("releases") or [{}])[0].get("id")
                    rname = (rel.get("releases") or [{}])[0].get("name")
                    u2 = ("https://api.stlouisfed.org/fred/release/sources?"
                          + urllib.parse.urlencode(
                              {"release_id": rid, "api_key": key,
                               "file_type": "json"}))
                    srcs = json.loads(urllib.request.urlopen(u2, timeout=25)
                                      .read()).get("sources") or []
                    names = [s.get("name") for s in srcs]
                    rep.log(f"  {sid:22} release='{str(rname)[:40]}'")
                    rep.log(f"  {'':22} → PUBLISHER: {names}")
                except Exception as e:
                    rep.log(f"  {sid:22} ✗ {str(e)[:80]}")
            rep.log("  → if publishers came back above, FRED: symbols get "
                    "REAL attribution from the publisher's own metadata")
        else:
            rep.log("  ✗ no FRED_KEY reachable — cannot test")

        # ── H3: name the temptation so the wire op excludes it ──
        rep.section("H3 — the inference trap (to be EXCLUDED, not used)")
        rep.log("  ECONOMICS:JPM3 description = 'Japan Money Supply M3'.")
        rep.log("  A country+topic heuristic would map that to the Bank of")
        rep.log("  Japan and look authoritative. It is a GUESS. This fleet")
        rep.log("  already shipped one false confirmation (global-recession")
        rep.log("  v1.2 'CONFIRMED' off a defaulted 0.0 field). The wire op")
        rep.log("  must mark heuristic rows UNATTRIBUTED, not agency rows.")
        rep.log(f"  symbols that would ONLY resolve by inference: "
                f"{len(econ - hit)}")
        rep.kv(inference_only=len(econ - hit))

        rep.section("FORECAST for the wire op")
        rep.log(f"  real attribution reachable now : vault {len(hit)}"
                f" + FRED {len(fred)} (pending H2 result)")
        rep.log(f"  honestly UNATTRIBUTED          : {len(econ - hit)}")
        rep.log("  PROBE COMPLETE — no engine code written.")


if __name__ == "__main__":
    main()
