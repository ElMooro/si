#!/usr/bin/env python3
"""ops 3082 (marker fix, read-only) -- rank-delta fix + Leaders enrich + RRG barcode (Khalid): (1) ADAPTIVE rank delta -- was hard-gated at 21 sessions with only ~5 accrued so the column showed nothing; now measures over available history (3+) with the horizon shown, converging to 20d; (2) LEADERS soldiers get fleet chips (Wyckoff phase letter w/ since-date, 13F whale $M, ER+) + weight bars + army quadrant chip; (3) ROTATION BARCODE -- per-ETF quadrant-color strip over ~12 weeks, color flips = rotations, stacked by quadrant. IR v3.5. Base redesign (Khalid: RRG scatter
'horrible, so confusing' + ladder too narrow):
(1) QUADRANT CARDS replace the scatter as the primary rotation view
-- four colored panels (LEADING/IMPROVING/WEAKENING/LAGGING) with
per-quadrant guidance, ETFs as strength-sorted chips, each carrying a
rotation-heading arrow (which quadrant it's drifting toward, from the
trail vector) + a ROTATION TAPE strip of the latest dated quadrant
transitions; the classic scatter is demoted behind a 'show map'
disclosure;
(2) LADDER: page widened 1280->1680px, table full-width with sticky
sortable headers (ETF/Sharpe/Scorecard/Score/Drank), zebra rows,
hover highlight, 74vh scroll shell, and a live filter box;
render replayed locally (cards PASS on real-shaped data)."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

T_START = None

AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3082",
      "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    global T_START
    T_START = datetime.now(timezone.utc)
    with report("3082_ranks_leaders") as rep:
        rep.section("0. Engine v3.5: invoke + adaptive delta")
        import boto3
        L = boto3.client("lambda", region_name="us-east-1")
        S3c = boto3.client("s3", region_name="us-east-1")
        import subprocess
        diff = subprocess.run(
            ["git", "diff", "--name-only", "HEAD^", "HEAD"],
            capture_output=True, text=True,
            cwd=str(AWS_DIR.parent)).stdout
        need = "aws/lambdas/justhodl-industry-rotation/" in diff
        rep.kv(need_fresh=need)
        # read-only: v3.5 freshness + invoke proven by ops 3081
        dj = None
        for _ in range(3):
            time.sleep(2)
            try:
                o = S3c.get_object(Bucket="justhodl-dashboard-live",
                                   Key="data/industry-rotation.json")
                if (datetime.now(timezone.utc) - o["LastModified"]
                        ).total_seconds() < 7200:
                    dj = json.loads(o["Body"].read())
                    if dj.get("version") == "3.5":
                        break
                    dj = None
            except Exception:
                pass
        if not dj:
            fails.append("no fresh v3.5 output")
            _fin(rep, fails, warns)
            sys.exit(1)
        lad = dj.get("ladder") or []
        n_rd = sum(1 for x in lad if x.get("rank_delta_20d")
                   is not None)
        n_days = sum(1 for x in lad if x.get("rank_delta_days"))
        rep.kv(rank_delta_rows=n_rd, rank_days_rows=n_days,
               rank_note=(dj.get("rank_note") or "")[:120],
               sample_rd=json.dumps(
                   [{"etf": x["etf"],
                     "rd": x.get("rank_delta_20d"),
                     "d": x.get("rank_delta_days")}
                    for x in lad[:4]]))
        if n_rd < 30:
            fails.append("adaptive rank delta on %d rows (<30)"
                         % n_rd)
        enr = [h2 for l0 in (dj.get("leaders") or [])
               for h2 in (l0.get("holdings_top") or [])
               if h2.get("phase") or h2.get("whale_musd")
               or h2.get("er_plus")]
        rep.kv(enriched_soldiers=len(enr),
               soldier_sample=json.dumps(enr[:3])[:240])
        if not enr:
            warns.append("no soldier carried a fleet chip today "
                         "(joins are membership-honest)")

        rep.section("1. Page live (this-push marker)")
        pg, ok = "", False
        for i in range(24):
            try:
                pg = get("https://justhodl.ai/industry-rotation.html"
                         "?cb=%d" % time.time())
                if "renderRRGBarcode" in pg:
                    ok = True
                    rep.kv(live_after_s=i * 20)
                    break
            except Exception:
                pass
            time.sleep(20)
        if not ok:
            fails.append("redesign not live after 8min")
            _fin(rep, fails, warns)
            sys.exit(1)
        for m in ('id="rrg-barcode"', "ROTATION BARCODE",
                  "rank_delta_days", 'id="rrg-cards"',
                  'id="rrg-movers"',
                  "ROTATION TAPE", "rotating toward",
                  "show the rotation barcode + classic RRG map",
                  'id="lad-filter"', "buildLadderRows",
                  "wireLadder", 'data-sort=\\"leadership_score\\"',
                  "max-width:1680px", "position:sticky",
                  "ladwrap"):
            if m not in pg:
                fails.append("marker missing: %s" % m)
        # scatter still available (demoted, not deleted)
        if "function renderRRG(d)" not in pg:
            fails.append("classic scatter removed instead of "
                         "demoted")

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3082.json").write_text(json.dumps(
        {"ops": 3082, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
