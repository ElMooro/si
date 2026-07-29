"""ops_4078 — is v1.7.9 ACTUALLY running? Fingerprint, don't assume.

A version number in a manifest proves what was shipped, not what is
executing in Khalid's browser.  The honest test is a fingerprint only the
new build can produce:

  * tier1_done / rate_per_min / elapsed_s  — these DIAG fields did not
    exist before v1.7.8.  If they appear, the new content script is live.
  * the tier of the symbols being sourced — a v1.7.9 walk sources
    ECONOMICS/FRED/TVC/CBOE/COT first.  Venue-only growth means the OLD
    ordering is still executing regardless of what the manifest says.
  * generated_at movement — a sync that never lands proves nothing.

Reports what is true either way; a walk that has not started yet is a
legitimate answer, not a failure to paper over.
"""
import json
import sys
from collections import Counter
from datetime import datetime, timezone

import boto3

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

TIER1 = {"ECONOMICS", "FRED", "TVC", "COT", "COT3", "CBOE",
         "QUANDL", "USCF", "USI", "EIA", "BLS", "BEA"}


def gj(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def main():
    with report("4078_v179_liveness") as rep:
        rep.heading("ops 4078 — is v1.7.9 executing?")

        sr = gj("data/tv-sources.json", {}) or {}
        diag = sr.get("last_harvest_diag") or {}
        srcs = sr.get("sources") or {}
        gen = sr.get("generated_at")

        rep.section("A. last sync")
        rep.log(f"  generated_at : {gen}")
        age = None
        try:
            age = (datetime.now(timezone.utc)
                   - datetime.fromisoformat(str(gen))).total_seconds() / 60
            rep.log(f"  age          : {age:.1f} min")
        except Exception:
            rep.log("  age          : unparseable")
        rep.log(f"  sources held : {len(srcs)}")
        rep.kv(sourced=len(srcs), age_min=(round(age, 1) if age else None))

        rep.section("B. FINGERPRINT — fields only v1.7.8+ can emit")
        rep.log(f"  raw diag: {json.dumps(diag)[:300]}")
        new_fields = [k for k in ("tier1_done", "rate_per_min", "elapsed_s")
                      if k in diag]
        rep.log(f"  new-build fields present: {new_fields or 'NONE'}")
        new_build = len(new_fields) == 3
        if new_build:
            rep.log("  ✓ the NEW content script is running")
            rep.log(f"    walked {diag.get('done')}/{diag.get('total')} · "
                    f"tier1 {diag.get('tier1_done')} · "
                    f"{diag.get('rate_per_min')}/min · "
                    f"{diag.get('elapsed_s')}s elapsed")
            rpm = diag.get("rate_per_min") or 0
            if rpm:
                left = (diag.get("total") or 0) - (diag.get("done") or 0)
                rep.log(f"    ETA to finish at this rate: "
                        f"{left / rpm / 60:.1f} h")
                # The throttling question I refused to guess at earlier.
                if rpm < 60:
                    rep.log(f"    ⚠ {rpm}/min is far below the 240ms timer "
                            f"(~250/min) — Chrome is throttling the tab, or "
                            f"the walk is pausing. Now measured, not assumed.")
                else:
                    rep.log(f"    rate is consistent with the timer — no "
                            f"throttling")
        else:
            rep.log("  ✗ diag carries NO v1.7.8+ fields — either the reload "
                    "has not happened, or no sync has landed since it did. "
                    "A sync lands at T+75s on a TradingView tab, then every "
                    "15 min.")
        rep.kv(new_build_running=new_build,
               walked=diag.get("done"), tier1_done=diag.get("tier1_done"),
               rate_per_min=diag.get("rate_per_min"))

        rep.section("C. WALK ORDER — what is actually being sourced")
        tiers = Counter()
        for sym in srcs:
            p = sym.split(":")[0].upper() if ":" in sym else "(bare)"
            tiers["AGENCY" if p in TIER1 else "other/venue"] += 1
        for t, n in tiers.most_common():
            rep.log(f"  {n:5d}  {t}")
        pref = Counter(s.split(":")[0].upper() for s in srcs if ":" in s)
        rep.log("  top prefixes sourced: " + ", ".join(
            f"{p} {n}" for p, n in pref.most_common(8)))
        agency_n = tiers.get("AGENCY", 0)
        rep.kv(agency_sourced=agency_n, other_sourced=tiers.get("other/venue", 0))

        if agency_n:
            rep.log(f"  ✓ {agency_n} agency-tier symbols sourced — the "
                    f"priority walk is producing the payoff")
        else:
            rep.log("  · zero agency-tier symbols yet. Expected if the walk "
                    "has not restarted; NOT expected if the new build has "
                    "been walking for more than a few minutes.")

        rep.section("D. refresh the rollup so the page shows current truth")
        try:
            r = lam.invoke(FunctionName="justhodl-source-map",
                           InvocationType="RequestResponse",
                           Payload=b'{"source":"ops4078"}')
            rep.log(f"  source-map: {r['Payload'].read().decode()[:200]}")
        except Exception as e:
            rep.log(f"  source-map invoke failed: {str(e)[:100]}")

        rep.section("VERDICT")
        if new_build and agency_n:
            rep.log("✅ v1.7.9 is running AND the agency-first walk is "
                    "banking the payoff.")
        elif new_build:
            rep.log("◐ v1.7.9 IS running; agency rows have not landed yet. "
                    "Re-read in ~15 min (next auto-sync).")
        else:
            rep.log("○ No evidence the new build has synced yet. Not a "
                    "failure — just nothing to confirm from here. Needs a "
                    "TradingView tab open for ~75s after the reload.")
        # Deliberately no sys.exit(1): "not yet" is a real answer, and
        # failing the op would dress up an unknown as a defect.


if __name__ == "__main__":
    main()
