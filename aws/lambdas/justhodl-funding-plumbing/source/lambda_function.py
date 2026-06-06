"""justhodl-funding-plumbing — the liquidity-regime / funding-plumbing gauge.

Built from a hard lesson: "Fed ends QT" is NOT "Fed starts QE." Ending QT stops
draining; it does not ADD liquidity. With the ON-RRP buffer already empty, a
FLAT balance sheet + ongoing Treasury issuance still drains bank RESERVES — so
the tape can keep tightening even after QT "ends." Going long on "QT ended" is
front-running an injection that hasn't happened.

This engine makes the distinction explicit and fuses the plumbing signals that
actually lead risk-asset stress into ONE regime + 0-100 stress score:

  • Balance-sheet DIRECTION (WALCL 8wk Δ): DRAINING / FLAT / EXPANDING  ← the lesson
  • RRP cushion (RRPONTSYD): near-zero = the shock absorber is gone
  • Reserves vs LCLoR (WRESBAL vs ~$3.0T): the real danger gauge (2019 repo blowup)
  • SOFR − IORB (bps): the cleanest real-time repo-stress tripwire
  • SOFR − EFFR (bps): secured vs unsecured funding pressure
  • TGA trajectory (WTREGEN): rising = Treasury draining reserves (worse w/o RRP)

OUTPUT: data/funding-plumbing.json
SCHEDULE: daily 13:15 UTC (after FRED updates).
"""
import json, time
import urllib.request, urllib.parse
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/funding-plumbing.json"
FRED_KEY = "2f057499936072679d8843d7fce99989"
s3 = boto3.client("s3", region_name=REGION)

# Lowest Comfortable Level of Reserves — the floor the Fed tries not to breach.
# Estimated ~3.0T (Brookings/JPMorgan range 2.7-3.4T). Tunable.
LCLOR_USD = 3.0e12

SERIES = {
    "walcl": "WALCL",          # Fed total assets (balance sheet), $M, weekly
    "reserves": "WRESBAL",     # Reserve balances at the Fed, $M, weekly
    "rrp": "RRPONTSYD",        # ON RRP usage, $B, daily
    "sofr": "SOFR",            # Secured Overnight Financing Rate, %, daily
    "iorb": "IORB",            # Interest on Reserve Balances, %, daily
    "effr": "EFFR",            # Effective Fed Funds Rate, %, daily
    "tga": "WTREGEN",          # Treasury General Account, $B, weekly
}


def fred(series_id, n=80):
    """Newest-first list of (date, value) for a FRED series."""
    try:
        p = {"series_id": series_id, "api_key": FRED_KEY, "file_type": "json",
             "sort_order": "desc", "limit": str(n)}
        url = "https://api.stlouisfed.org/fred/series/observations?" + urllib.parse.urlencode(p)
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            obs = json.loads(r.read().decode()).get("observations", [])
        out = []
        for o in obs:
            v = o.get("value")
            if v not in (None, ".", ""):
                try: out.append((o.get("date"), float(v)))
                except ValueError: pass
        return out  # newest-first
    except Exception as e:
        print(f"[plumbing] FRED {series_id} err: {str(e)[:60]}")
        return []


def lambda_handler(event=None, context=None):
    t0 = time.time()
    data = {k: fred(v) for k, v in SERIES.items()}
    sig = {}
    stress = []   # each 0-100; higher = more stress

    # ── 1. Balance-sheet DIRECTION (the QT≠QE lesson) ──
    walcl = data["walcl"]
    bs_dir, bs_note, bs_stress = "UNKNOWN", "balance-sheet data unavailable", 50
    if len(walcl) >= 9:
        latest = walcl[0][1]; eight_wk = walcl[8][1]
        chg = latest - eight_wk
        chg_pct = chg / eight_wk * 100 if eight_wk else 0
        # WALCL in $M; ~$20B/8wk ≈ meaningful
        if chg < -20000:
            bs_dir, bs_stress = "DRAINING", 75
            bs_note = f"Fed balance sheet SHRINKING ({chg/1e6:+.0f}B over 8wk) — QT still draining liquidity"
        elif chg > 20000:
            bs_dir, bs_stress = "EXPANDING", 20
            bs_note = f"Fed balance sheet GROWING ({chg/1e6:+.0f}B over 8wk) — genuine liquidity injection (QE-like)"
        else:
            bs_dir, bs_stress = "FLAT", 55
            bs_note = (f"Fed balance sheet FLAT ({chg/1e6:+.0f}B over 8wk) — QT may have ended, but this is "
                       f"NOT QE. No new liquidity is being added; with RRP empty, issuance still drains reserves.")
    sig["balance_sheet"] = {"direction": bs_dir, "latest_usd_t": round(walcl[0][1]/1e6, 2) if walcl else None,
                            "chg_8wk_usd_b": round((walcl[0][1]-walcl[8][1])/1e3, 0) if len(walcl) >= 9 else None,
                            "note": bs_note}
    stress.append(("balance_sheet", bs_stress, 0.22))

    # ── 2. Reserves vs LCLoR (the real danger gauge) ──
    res = data["reserves"]
    if res:
        res_usd = res[0][1] * 1e6  # $M → $
        ratio = res_usd / LCLOR_USD
        # ratio < 1.1 = approaching the comfortable floor; < 1.0 = below = danger
        res_stress = max(0, min(100, (1.25 - ratio) / (1.25 - 0.95) * 100))
        zone = ("BELOW LCLoR — danger (2019-style repo blowup risk)" if ratio < 1.0
                else "approaching LCLoR" if ratio < 1.1 else "ample")
        sig["reserves"] = {"reserves_usd_t": round(res_usd/1e12, 2), "lclor_usd_t": round(LCLOR_USD/1e12, 2),
                           "ratio": round(ratio, 2), "note": f"Reserves ${res_usd/1e12:.2f}T vs ~${LCLOR_USD/1e12:.1f}T floor — {zone}"}
    else:
        res_stress = 50; sig["reserves"] = {"note": "reserves data unavailable"}
    stress.append(("reserves", res_stress, 0.24))

    # ── 3. RRP cushion ──
    rrp = data["rrp"]
    if rrp:
        rrp_b = rrp[0][1]  # already $B
        # near zero = no buffer = higher fragility
        rrp_stress = max(0, min(100, (300 - rrp_b) / 300 * 100))
        sig["rrp_cushion"] = {"rrp_usd_b": round(rrp_b, 1),
                              "note": f"ON-RRP ${rrp_b:.0f}B — {'EMPTY: shock absorber gone, every drain now hits reserves directly' if rrp_b < 50 else 'thin' if rrp_b < 300 else 'ample buffer'}"}
    else:
        rrp_stress = 50; sig["rrp_cushion"] = {"note": "RRP data unavailable"}
    stress.append(("rrp_cushion", rrp_stress, 0.18))

    # ── 4. SOFR − IORB (the cleanest repo-stress tripwire) ──
    sofr = data["sofr"]; iorb = data["iorb"]
    if sofr and iorb:
        spread_bps = (sofr[0][1] - iorb[0][1]) * 100
        # SOFR persistently > IORB = collateral scarce / cash tight = stress
        si_stress = max(0, min(100, 50 + spread_bps * 5))
        sig["sofr_iorb"] = {"spread_bps": round(spread_bps, 1), "sofr": sofr[0][1], "iorb": iorb[0][1],
                            "note": f"SOFR−IORB {spread_bps:+.0f}bps — {'STRESS: secured funding above admin rate (collateral scarce)' if spread_bps > 5 else 'firming' if spread_bps > 0 else 'calm'}"}
    else:
        si_stress = 50; sig["sofr_iorb"] = {"note": "SOFR/IORB unavailable"}
    stress.append(("sofr_iorb", si_stress, 0.20))

    # ── 5. SOFR − EFFR ──
    effr = data["effr"]
    if sofr and effr:
        se_bps = (sofr[0][1] - effr[0][1]) * 100
        se_stress = max(0, min(100, 50 + se_bps * 6))
        sig["sofr_effr"] = {"spread_bps": round(se_bps, 1), "note": f"SOFR−EFFR {se_bps:+.0f}bps ({'secured funding pressure' if se_bps > 4 else 'normal'})"}
    else:
        se_stress = 50; sig["sofr_effr"] = {"note": "unavailable"}
    stress.append(("sofr_effr", se_stress, 0.08))

    # ── 6. TGA trajectory (rising = Treasury draining reserves) ──
    tga = data["tga"]
    tga_note = "TGA data unavailable"; tga_stress = 50
    if len(tga) >= 5:
        tga_chg = tga[0][1] - tga[4][1]  # ~4wk change, $B
        if tga_chg > 100:
            tga_stress = 65; tga_note = f"TGA rising (+${tga_chg:.0f}B/4wk) — Treasury rebuild draining reserves (bites harder with RRP empty)"
        elif tga_chg < -100:
            tga_stress = 35; tga_note = f"TGA falling (${tga_chg:.0f}B/4wk) — releasing cash into reserves (supportive)"
        else:
            tga_stress = 50; tga_note = f"TGA ~flat ({tga_chg:+.0f}B/4wk)"
    sig["tga"] = {"tga_usd_b": round(tga[0][1], 0) if tga else None, "chg_4wk_usd_b": round(tga[0][1]-tga[4][1], 0) if len(tga) >= 5 else None, "note": tga_note}
    stress.append(("tga", tga_stress, 0.08))

    # ── Composite (weighted) + regime ──
    wsum = sum(s * w for _, s, w in stress)
    wtot = sum(w for _, _, w in stress)
    composite = round(wsum / wtot, 1) if wtot else 50
    if composite >= 72: regime, action = "STRESS", "Funding plumbing under acute stress — de-risk; reserves/repo are the fragility."
    elif composite >= 58: regime, action = "FRAGILE", "Buffers thin (RRP empty); any liquidity shock now hits reserves directly. Reduce gross, tighten stops."
    elif composite >= 45: regime, action = "TIGHTENING", "Liquidity is draining/flat, not expanding. Do NOT confuse 'QT ended' with 'QE started' — no new liquidity yet."
    else: regime, action = "AMPLE", "Plumbing ample; liquidity supportive of risk assets."

    drivers = sorted([{"signal": k, "stress": s, "weight": w} for k, s, w in stress], key=lambda x: -x["stress"] * x["weight"])[:4]

    out = {
        "engine": "funding-plumbing", "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "plumbing_stress_score": composite, "regime": regime, "action": action,
        "balance_sheet_direction": bs_dir,
        "qt_ended_not_qe": (bs_dir == "FLAT"),  # the explicit lesson flag
        "signals": sig,
        "top_drivers": [{"signal": d["signal"], "note": sig.get(d["signal"], {}).get("note")} for d in drivers],
        "methodology": ("Weighted plumbing-stress composite: balance-sheet direction (0.22), "
                        "reserves-vs-LCLoR (0.24), RRP cushion (0.18), SOFR−IORB (0.20), "
                        "SOFR−EFFR (0.08), TGA trajectory (0.08). Regime: AMPLE / TIGHTENING / "
                        "FRAGILE / STRESS."),
        "lesson": ("'Fed ends QT' ≠ 'Fed starts QE'. A FLAT balance sheet adds no liquidity; "
                   "with RRP empty, Treasury issuance still drains reserves, so risk assets can "
                   "keep tightening. Wait for EXPANDING (genuine injection), not just the end of QT."),
        "source": "FRED (Federal Reserve / public domain)",
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[funding-plumbing] DONE {round(time.time()-t0,1)}s — {regime} ({composite}); bs={bs_dir}")
    return {"statusCode": 200, "body": json.dumps({"regime": regime, "score": composite, "bs": bs_dir})}
