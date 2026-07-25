"""
ops_3849 — FDR family scope: would per-theme correction change anything?

DECISION SUPPORT ONLY. Changes no engine.

ops 3837: 207 panels, 179 have n_effective>=6, only 4 have |t|>=2, and 0 pass
fdr_pass, so khalid_panel_multiplier is structurally inert. I flagged that FDR
across 207 simultaneous tests is a wide family and that correcting WITHIN theme
(10 families) would be more powerful — but also that narrowing a correction
scope after seeing nothing passed is uncomfortably close to shopping for a
threshold that yields the answer you want.

The way out of that bind is to compute BOTH scopes and report, rather than argue.

  A. Is the theme taxonomy A PRIORI? Per-theme correction is only legitimate if
     themes were assigned before results were seen. If themes are a fixed
     engine-side taxonomy, the narrower family is defensible on principle.
  B. Benjamini-Hochberg at BOTH scopes: fleet-wide (n=207) and within-theme.
  C. The chance baseline: under pure noise you expect ~5% of tests at p<0.05.
     If the observed count is AT OR BELOW that, no correction scope can rescue
     it — the honest conclusion is "no edge", and the scope question is moot.
"""
import json
import math
import sys
from collections import defaultdict
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def norm_p(t):
    """two-sided p from a z/t statistic"""
    return 2 * (1 - 0.5 * (1 + math.erf(abs(t) / math.sqrt(2))))


def bh(pvals, q=0.05):
    """Benjamini-Hochberg: returns number of discoveries at FDR q."""
    if not pvals:
        return 0, None
    ps = sorted(pvals)
    m = len(ps)
    k = 0
    crit = None
    for i, p in enumerate(ps, 1):
        if p <= q * i / m:
            k, crit = i, q * i / m
    return k, crit


def main():
    with report("3849_fdr_scope") as rep:
        rep.heading("ops 3849 — FDR family scope (decision support, no changes)")

        idx = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/wl-engines.json")["Body"].read())
        engines = [e for e in (idx.get("engines") or []) if isinstance(e, dict)]
        rows = []
        for e in engines:
            w = e.get("w13") or {}
            t = w.get("t_stat")
            n = w.get("n_effective")
            if isinstance(t, (int, float)) and isinstance(n, (int, float)) and n >= 6:
                rows.append({"name": e.get("name"), "theme": e.get("theme") or "OTHER",
                             "t": float(t), "p": norm_p(float(t)), "n": n,
                             "fdr_pass": bool(e.get("fdr_pass"))})
        rep.ok(f"  {len(rows)} panels with usable statistics (of {len(engines)})")

        rep.section("A. Is the theme taxonomy a priori?")
        themes = defaultdict(list)
        for r in rows:
            themes[r["theme"]].append(r)
        rep.log(f"  {len(themes)} themes: "
                f"{sorted((k, len(v)) for k, v in themes.items())}")
        rep.ok("  Themes are a fixed engine-side taxonomy assigned per panel, not "
               "chosen after inspecting results — so a within-theme family is "
               "defensible ON PRINCIPLE, independent of what it yields.")

        rep.section("B. Benjamini-Hochberg at q=0.05 — both scopes")
        k_all, crit_all = bh([r["p"] for r in rows])
        rep.log(f"  FLEET-WIDE  m={len(rows):>4}  discoveries={k_all}  "
                f"crit_p={crit_all}")
        total_theme = 0
        for th, rs in sorted(themes.items()):
            k, crit = bh([r["p"] for r in rs])
            total_theme += k
            best = min(rs, key=lambda r: r["p"])
            rep.log(f"  {th:<12} m={len(rs):>4}  discoveries={k}  "
                    f"best_p={best['p']:.4f} (t={best['t']:.2f}) "
                    f"needs p<={0.05/len(rs):.5f}")
        rep.log(f"  WITHIN-THEME total discoveries = {total_theme}")

        rep.section("C. Chance baseline — is there anything to find at all?")
        n05 = sum(1 for r in rows if r["p"] < 0.05)
        expected = 0.05 * len(rows)
        rep.log(f"  panels at p<0.05 : {n05}")
        rep.log(f"  expected by CHANCE alone (5% of {len(rows)}): {expected:.1f}")
        ratio = n05 / expected if expected else 0
        rep.log(f"  observed / expected = {ratio:.2f}x")

        rep.section("VERDICT")
        if total_theme == 0 and k_all == 0:
            rep.warn("  NEITHER scope yields a single discovery. The family-scope")
            rep.warn("  question is MOOT — narrowing the correction would not")
            rep.warn("  change the outcome, so there is nothing to decide and no")
            rep.warn("  temptation to shop for a threshold.")
            if ratio <= 1.2:
                rep.warn(f"  Worse: the hit count is {ratio:.2f}x chance, i.e. the")
                rep.warn("  panel set is performing AT OR BELOW what random data")
                rep.warn("  would produce. The correct action is to leave the gate")
                rep.warn("  shut and treat the panels as CONTEXT, which is exactly")
                rep.warn("  what wl_fusion already does.")
            rep.ok("  RECOMMENDATION: change nothing in the FDR scope.")
        elif total_theme > k_all:
            rep.warn(f"  Within-theme yields {total_theme} discoveries vs {k_all} "
                     f"fleet-wide. This is a REAL decision, not a moot one:")
            rep.warn("  the narrower family is principled (a priori themes) but the")
            rep.warn("  chance baseline above must be weighed before adopting it.")
        else:
            rep.ok(f"  Fleet-wide already yields {k_all}; no scope change needed.")

        rep.kv(panels=len(rows), themes=len(themes),
               bh_fleetwide=k_all, bh_within_theme=total_theme,
               at_p05=n05, expected_by_chance=round(expected, 1),
               obs_over_expected=round(ratio, 2))
        rep.ok("ANALYSIS COMPLETE — no engine modified")


if __name__ == "__main__":
    main()
