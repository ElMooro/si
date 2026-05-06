"""Final visual verification: hit live brief.html, confirm 4 critical snapshot tiles
have meaningful data (not '—' or 'not deployed'):
  - eurodollar_stress (built earlier today)
  - asymmetric_setups (just fixed)
  - risk_sizer (just fixed)
  - allocator (existing)

Also pull the brief markdown and show the DECISIVE CALL section to confirm
Claude now references specific tickers from asymmetric/risk_sizer.
"""
import json
import urllib.request
from ops_report import report


UA = {"User-Agent": "justhodl-audit/1.0"}


def fetch_json(url):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())


def main():
    with report("verify_brief_final") as r:
        r.heading("1) Pull live ai-brief.json — all 4 fixed snapshot tiles")
        d = fetch_json("https://justhodl-dashboard-live.s3.amazonaws.com/data/ai-brief.json")
        snap = d.get("snapshot") or {}

        # Tile renders (mirroring brief.html JS logic)
        eur = snap.get("eurodollar_stress") or {}
        asym = snap.get("asymmetric_setups") or {}
        risk = snap.get("risk_sizer") or {}
        alloc = snap.get("allocator") or {}

        r.log("  Tile render preview (what brief.html shows):")
        r.log("")
        # Eurodollar
        sc = eur.get("score")
        if sc is None:
            er = "not deployed"
        else:
            severity = eur.get("severity") or eur.get("regime") or ""
            hot = len(eur.get("hot_signals") or [])
            tag = f" ({hot}🔴)" if hot else ""
            er = f"{sc}/100 · {severity}{tag}"
        r.log(f"  ┌─ Eurodollar Stress    : {er}")

        # Asymmetric
        n = asym.get("n_setups")
        if n is None:
            ar = "—"
        else:
            trap = asym.get("n_value_traps")
            top = (asym.get("top_5_setups") or [{}])[0].get("symbol")
            tail = f" · {trap} traps" if trap else ""
            ar = f"{n} setups · top {top}{tail}" if top else f"{n} setups"
        r.log(f"  ├─ Asymmetric Setups    : {ar}")

        # Risk sizer
        cap = risk.get("max_gross_exposure_pct")
        if cap is None:
            rr = "—"
        else:
            dd = risk.get("current_dd_pct")
            k = risk.get("kelly_fraction")
            kpart = " · ¼Kelly" if k is not None else ""
            ddpart = f" · DD {dd}%" if dd is not None else ""
            rr = f"{cap}% cap{kpart}{ddpart}"
        r.log(f"  ├─ Risk Sizer           : {rr}")

        # Allocator
        regime_h = alloc.get("regime_headline") or alloc.get("regime") or "—"
        cash = alloc.get("cash_buffer_pct") or alloc.get("cash_pct")
        ar2 = f"{regime_h}" if cash is None else f"{regime_h} · {cash}% cash"
        r.log(f"  └─ Allocator            : {ar2}")

        r.heading("2) Brief metadata")
        r.log(f"  generated_at: {d.get('generated_at')}")
        r.log(f"  model:        {d.get('model')}")
        r.log(f"  duration_s:   {d.get('duration_s')}")
        r.log(f"  brief chars:  {len(d.get('brief_md',''))}")
        usage = d.get("usage") or {}
        cost = (usage.get("input_tokens",0)*1.0 + usage.get("output_tokens",0)*5.0) / 1e6
        r.log(f"  tokens: in={usage.get('input_tokens')} out={usage.get('output_tokens')}")
        r.log(f"  cost:   ~${cost:.4f}/run = ~${cost*180:.2f}/month")

        r.heading("3) DECISIVE CALL section from brief")
        md = d.get("brief_md") or ""
        # Find the section starting with DECISIVE CALL
        idx = md.upper().find("DECISIVE CALL")
        if idx >= 0:
            # Find next major header (## or ---) after it
            section = md[idx:idx+2500]
            for line in section.splitlines():
                r.log(f"  {line}")
        else:
            r.log("  (DECISIVE CALL section not found in brief)")

        r.heading("4) brief.html status")
        try:
            req = urllib.request.Request("https://justhodl.ai/brief.html", headers=UA)
            with urllib.request.urlopen(req, timeout=15) as resp:
                body = resp.read().decode("utf-8", errors="replace")
            r.log(f"  status: {resp.status}, size: {len(body):,}b")
            r.log(f"  has 'd?.score' eurodollar reader:  {'d?.score' in body}")
            r.log(f"  has 'top_5_setups' asymmetric:     {'top_5_setups' in body}")
            r.log(f"  has 'max_gross_exposure_pct':      {'max_gross_exposure_pct' in body}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
