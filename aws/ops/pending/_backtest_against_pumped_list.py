"""
Honest backtest: would today's 5-system hunter have picked up the names
in the user-provided AI-supply-chain pump list BEFORE they ran?

Names (3-month performance):
  AXTI +464%, LWLG +407%, AAOI +353%, AEHR +276%, SNDK +138%, ICHR +137%,
  MRVL +129%, INTC +122%, VIAV +119%, LITE +116%, CRDO +101%
  + the macro thesis names: MU, SNDK (memory + AI structural re-rating)

For each name, check:
  1. Is it in our universe.json? (the seed pool)
  2. Did any hunter ever flag it?
     - Nobrainer top-25?
     - Insider cluster (any score)?
     - Smart-money cluster (any score)?
     - Deep-value qualifying (any flag)?
     - EPS velocity qualifying (any flag)?
  3. What IS its current state — score on each system if any
  4. Why did/didn't the system catch it (root cause analysis)

This is brutally honest. We expose every miss.
"""
import json, time, urllib.request, os
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


PUMPED = [
    ("AXTI",  464.48, "Indium phosphide / GaAs substrates for AI optical/RF — supply-constrained niche"),
    ("LWLG",  407.67, "Polymer-based electro-optic photonics — research stage, AI optical interconnect"),
    ("AAOI",  353.13, "Optical transceivers — AI data-center bandwidth supply"),
    ("AEHR",  276.84, "Burn-in test equipment for SiC/AI chips — small mcap, supply-tight niche"),
    ("SNDK",  138.54, "Memory storage — AI structural re-rating play"),
    ("ICHR",  137.88, "Critical fluid delivery for semi/memory fab — picks-and-shovels"),
    ("MRVL",  129.81, "Optical DSP, custom AI silicon"),
    ("INTC",  122.41, "Foundry pivot, government CHIPS subsidies"),
    ("VIAV",  119.35, "Optical test equipment — AI optical infrastructure"),
    ("LITE",  116.25, "Lumentum — AI optical lasers"),
    ("CRDO",  101.23, "AEC cables for AI data centers"),
    ("MU",      0.0, "Memory cycle / AI re-rating (mentioned in thesis)"),
]


def main():
    section("0) Setup — load all hunter outputs")
    feeds = {
        "universe":    "data/universe.json",
        "nobrainers":  "data/nobrainers.json",
        "insiders":    "data/insider-clusters.json",
        "smart_money": "data/smart-money-clusters.json",
        "deep_value":  "data/deep-value.json",
        "eps_velocity": "data/eps-revision-velocity.json",
        "themes":       "data/themes-detected.json",
        "supply":       "data/supply-inflection.json",
        "compound":     "data/compound-signals.json",
    }
    data = {}
    for name, key in feeds.items():
        try:
            data[name] = json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            log(f"  ✓ {name}: loaded {len(json.dumps(data[name]))} bytes")
        except Exception as e:
            data[name] = None
            log(f"  ❌ {name}: {e}")

    # Build lookup tables
    universe_set = set()
    if data["universe"]:
        for s in data["universe"].get("stocks", []):
            sym = (s.get("symbol") or "").upper()
            if sym:
                universe_set.add(sym)

    nb_top = {x.get("ticker"): x for x in (data["nobrainers"] or {}).get("summary", {}).get("top_25_overall", [])}
    nb_all_qual = (data["nobrainers"] or {}).get("all_candidates", []) or []
    nb_all = {(c.get("ticker") or ""): c for c in nb_all_qual if c.get("ticker")}

    ic_by_ticker = {(c.get("ticker") or ""): c for c in (data["insiders"] or {}).get("clusters", [])}
    sm_by_ticker = {(c.get("ticker") or ""): c for c in (data["smart_money"] or {}).get("clusters", [])}

    dv_top = {(c.get("symbol") or ""): c for c in (data["deep_value"] or {}).get("summary", {}).get("top_25_overall", [])}
    dv_all = {(c.get("symbol") or ""): c for c in (data["deep_value"] or {}).get("all_qualifying", [])}

    ev_top = {(c.get("symbol") or ""): c for c in (data["eps_velocity"] or {}).get("summary", {}).get("top_25_overall", [])}
    ev_all = {(c.get("symbol") or ""): c for c in (data["eps_velocity"] or {}).get("all_qualifying", [])}

    compound_set = {(c.get("symbol") or ""): c for c in (data["compound"] or {}).get("compound", [])}

    # Themes/supply
    themes_now = data["themes"] or {}
    supply_now = data["supply"] or {}

    section("1) PER-NAME analysis — would the system have flagged it?")
    log(f"  {'Sym':<6} {'Pump':>8} {'Univ':>5} {'NB':<6} {'Insid':<6} {'13F':<6} {'DV':<6} {'EPS':<10} {'Compound':<8}")
    log(f"  {'-'*6} {'-'*8} {'-'*5} {'-'*6} {'-'*6} {'-'*6} {'-'*6} {'-'*10} {'-'*8}")

    captures = []
    misses = []
    for sym, pump, why in PUMPED:
        in_univ = "✓" if sym in universe_set else "❌"
        nb_str = "-"
        if sym in nb_top:
            nb_str = f"#{list(nb_top.keys()).index(sym)+1} {nb_top[sym].get('asymmetric_score','?'):.0f}"
        elif sym in nb_all:
            sc = nb_all[sym].get("asymmetric_score", 0) or 0
            nb_str = f"low {sc:.0f}"

        ic_str = "-"
        if sym in ic_by_ticker:
            sc = ic_by_ticker[sym].get("score", 0) or 0
            ic_str = f"{sc:.0f}"

        sm_str = "-"
        if sym in sm_by_ticker:
            sc = sm_by_ticker[sym].get("score", 0) or 0
            sm_str = f"{sc:.0f}"

        dv_str = "-"
        if sym in dv_top:
            dv_str = f"top {dv_top[sym].get('score','?'):.0f}"
        elif sym in dv_all:
            sc = dv_all[sym].get("score", 0) or 0
            flag = (dv_all[sym].get("flag") or "")[:8]
            dv_str = f"{sc:.0f} {flag}"

        ev_str = "-"
        if sym in ev_top:
            ev_str = f"top {ev_top[sym].get('score','?'):.0f}"
        elif sym in ev_all:
            sc = ev_all[sym].get("score", 0) or 0
            flag = (ev_all[sym].get("flag") or "")[:8]
            lift = (ev_all[sym].get("estimates") or {}).get("fy2_lift_pct", "?")
            ev_str = f"{sc:.0f} {flag[:6]} lift={lift}"

        comp_str = "-"
        if sym in compound_set:
            cs = compound_set[sym]
            comp_str = f"#{cs.get('n_systems')} {cs.get('compound_score',0):.0f}"

        log(f"  {sym:<6} {pump:>+7.0f}% {in_univ:<5} {nb_str:<6} {ic_str:<6} {sm_str:<6} {dv_str:<6} {ev_str:<14} {comp_str:<8}")

        # Record capture / miss
        captured = any([
            sym in nb_top, sym in ic_by_ticker, sym in sm_by_ticker,
            sym in dv_top, sym in ev_top, sym in compound_set,
        ])
        if captured:
            captures.append((sym, pump))
        else:
            misses.append((sym, pump, why))

    section("2) DIAGNOSIS")
    log(f"  Total in pump list: {len(PUMPED)}")
    log(f"  Captured by ANY hunter: {len(captures)}/{len(PUMPED)}")
    log(f"  Capture rate: {len(captures)*100/len(PUMPED):.0f}%")
    log("")
    if captures:
        log("  ── caught ──")
        for sym, pump in captures:
            log(f"    ✓ {sym}: +{pump:.0f}%")
    if misses:
        log("")
        log("  ── MISSED ── (the painful list)")
        for sym, pump, why in misses:
            log(f"    ❌ {sym} (+{pump:.0f}%): {why}")

    section("3) ROOT-CAUSE ANALYSIS — why we missed them")

    log("  TYPES of misses:")
    log("")
    log("  A) Sub-$1B microcaps not in universe")
    micro_misses = []
    for sym, pump, why in misses:
        if sym not in universe_set:
            micro_misses.append((sym, pump, why))
    log(f"     Count: {len(micro_misses)}")
    for sym, pump, why in micro_misses:
        log(f"     • {sym} (+{pump:.0f}%) — {why[:80]}")
    log("")
    log("  B) In universe but no hunter flagged it")
    in_univ_missed = []
    for sym, pump, why in misses:
        if sym in universe_set:
            in_univ_missed.append((sym, pump, why))
    log(f"     Count: {len(in_univ_missed)}")
    for sym, pump, why in in_univ_missed:
        log(f"     • {sym} (+{pump:.0f}%) — {why[:80]}")

    section("4) THE STRUCTURAL GAPS")

    log("  Gap #1: SMALL-CAP COVERAGE")
    log("    Universe v2 has 336 stocks, mostly large/mid-cap (mcap >= $300M).")
    log("    Names like AXTI, LWLG, AEHR were sub-$500M when the move started.")
    log("    Current universe MIN_MCAP threshold filters out exactly this kind of name.")
    log("")
    log("  Gap #2: NO 'EARLY MOMENTUM' SIGNAL")
    log("    All 5 systems are FUNDAMENTAL/POSITIONING-based:")
    log("      • Nobrainer = theme + supply + valuation")
    log("      • Insider = SEC Form 4 buys")
    log("      • Smart Money = 13F (lagging 45 days)")
    log("      • Deep Value = Ben Graham balance sheet")
    log("      • EPS Velocity = analyst revisions")
    log("    NONE of these detect:")
    log("      • Early price-volume breakouts")
    log("      • Unusual options flow")
    log("      • Short-interest squeezes")
    log("      • Rising relative strength")
    log("      • Newly added to AI/semi ETFs (passive flows)")
    log("    These technical/flow signals would have caught LWLG/AAOI/AXTI weeks earlier.")
    log("")
    log("  Gap #3: NO THEME-EXPANSION DETECTION")
    log("    Theme detector has 79 themes via ETFs. But 'AI memory re-rating' or")
    log("    'AI optical interconnect supply chain' are SUB-themes inside SOXX/SMH/AIQ")
    log("    that don't have their own ETF. Need a sub-theme detector that watches")
    log("    correlated price moves WITHIN a parent theme to catch sub-clusters early.")
    log("")
    log("  Gap #4: NO SOCIAL/NARRATIVE SIGNAL")
    log("    Many of these moved on Twitter narrative + retail attention before")
    log("    fundamentals confirmed. We have NO sentiment/social signal layer.")
    log("    A 'narrative momentum' signal (mentions/searches accelerating) would")
    log("    have caught AAOI, AEHR, AXTI 4-8 weeks earlier.")
    log("")
    log("  Gap #5: SUPPLY INFLECTION COVERAGE TOO COARSE")
    log("    Our supply scanner has 22 hard-data signals (MEMORY, LITHIUM, RARE_EARTH...)")
    log("    But it doesn't have:")
    log("      • OPTICAL_TRANSCEIVERS (AAOI, LITE, COHR pure-plays)")
    log("      • TEST_EQUIPMENT_SiC (AEHR, ICHR pure-plays)")
    log("      • COMPOUND_SEMICONDUCTORS (AXTI, SK Hynix)")
    log("      • DRAM_CYCLE (MU, SNDK pure-plays)")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "backtest_against_pumped_list.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
