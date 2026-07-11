#!/usr/bin/env python3
"""ops 3091 -- PEG v3.9 verify vs EXTERNAL TRUTH: quote payload has
no pe/eps at all (3087 probe), so trailing PE now comes from
/stable/ratios-ttm (priceToEarningsRatioTTM -- fleet-precedent key);
forward EPS now picks the NEAREST FUTURE fiscal year only (3090's
NVDA 16.7 vs external 20-24 = one-FY-too-far). Bands from the web:
TSM trailing 33-38 / fwd 23-28; NVDA trailing ~31 / fwd 20-24.
Currency normalization + bounds unchanged."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

T_START = None

AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3091",
      "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    global T_START
    T_START = datetime.now(timezone.utc)
    with report("3091_peg_truth") as rep:
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
        okf = False
        for _ in range(30):
            try:
                cfg = L.get_function_configuration(
                    FunctionName="justhodl-industry-rotation")
                lm = datetime.fromisoformat(
                    cfg["LastModified"].replace("+0000", "+00:00"))
                from datetime import timedelta as _td
                if cfg.get("LastUpdateStatus") in ("Successful",
                                                   None) and \
                        (not need or lm >= T_START - _td(seconds=90)):
                    okf = True
                    break
            except Exception:
                pass
            time.sleep(20)
        if not okf:
            fails.append("engine never fresh")
            _fin(rep, fails, warns)
            sys.exit(1)
        L.invoke(FunctionName="justhodl-industry-rotation",
                 InvocationType="Event", Payload=b"{}")
        dj = None
        for _ in range(45):
            time.sleep(20)
            try:
                o = S3c.get_object(Bucket="justhodl-dashboard-live",
                                   Key="data/industry-rotation.json")
                if (datetime.now(timezone.utc) - o["LastModified"]
                        ).total_seconds() < 1400:
                    dj = json.loads(o["Body"].read())
                    if dj.get("version") == "3.9":
                        break
                    dj = None
            except Exception:
                pass
        if not dj:
            fails.append("no fresh v3.9 output")
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

        n20 = sum(1 for x in lad if "above_sma20" in x)
        rrs = [h2 for l0 in (dj.get("leaders") or [])
               for h2 in (l0.get("holdings_top") or [])
               if h2.get("rr")]
        accs = [h2 for l0 in (dj.get("leaders") or [])
                for h2 in (l0.get("holdings_top") or [])
                if h2.get("acc_state") or h2.get("dp")]
        rep.kv(rr_debug=json.dumps(dj.get("rr_debug")))
        rep.kv(sma20_rows=n20, rr_soldiers=len(rrs),
               accdist_soldiers=len(accs),
               rr_sample=json.dumps(
                   [{"t": x["ticker"], **x["rr"]}
                    for x in rrs[:3]])[:280])
        if n20 < 38:
            fails.append("above_sma20 on %d rows (<38)" % n20)
        if len(rrs) < 10:
            fails.append("rr on %d soldiers (<10)" % len(rrs))
        if not accs:
            warns.append("no soldier in radar acc/dist or DP sets "
                         "today (membership-honest)")

        n_rsi = sum(1 for x in lad if x.get("rsi14") is not None)
        n_bb = sum(1 for x in lad if x.get("bb"))
        n_sq = sum(1 for x in lad
                   if (x.get("bb") or {}).get("squeeze"))
        n_mac = sum(1 for x in lad if x.get("macd"))
        n_ext = sum(1 for x in lad if x.get("extremes"))
        pes = [h2 for l0 in (dj.get("leaders") or [])
               for h2 in (l0.get("holdings_top") or [])
               if h2.get("pe") or h2.get("fwd_pe")]
        pegs = [h2 for l0 in (dj.get("leaders") or [])
                for h2 in (l0.get("holdings_top") or [])
                if h2.get("peg_fwd") is not None]
        rep.kv(rsi_rows=n_rsi, bb_rows=n_bb, squeezes=n_sq,
               macd_rows=n_mac, extreme_rows=n_ext,
               pe_soldiers=len(pes), peg_soldiers=len(pegs),
               peg_sample=json.dumps(
                   [{"t": x["ticker"], "pe": x.get("pe"),
                     "fpe": x.get("fwd_pe"),
                     "g": x.get("eps_cagr_pct"),
                     "peg": x.get("peg_fwd")}
                    for x in pegs[:4]])[:300])
        if n_rsi < 38 or n_bb < 38 or n_mac < 38:
            fails.append("technicals thin: rsi=%d bb=%d macd=%d"
                         % (n_rsi, n_bb, n_mac))
        if len(pes) < 25:
            fails.append("pe on %d soldiers (<25)" % len(pes))
        allsold = {h2["ticker"]: h2
                   for l0 in (dj.get("leaders") or [])
                   for h2 in (l0.get("holdings_top") or [])}
        tsm, nvda = allsold.get("TSM"), allsold.get("NVDA")
        rep.kv(tsm=json.dumps({k: tsm.get(k) for k in
                               ("pe", "fwd_pe", "fwd_pe_basis",
                                "eps_cagr_pct", "peg_fwd")})
               if tsm else "absent",
               nvda=json.dumps({k: nvda.get(k) for k in
                                ("pe", "fwd_pe", "peg_fwd")})
               if nvda else "absent")
        if tsm:
            if not tsm.get("pe"):
                fails.append("TSM trailing pe still absent")
            if tsm.get("pe") and not 20 <= tsm["pe"] <= 60:
                fails.append("TSM trailing pe=%s outside 20-60 "
                             "(external truth ~33-38)" % tsm["pe"])
            if tsm.get("fwd_pe") and not 15 <= tsm["fwd_pe"] <= 45:
                fails.append("TSM fwd_pe=%s outside 15-45 "
                             "(external ~23-28)" % tsm["fwd_pe"])
            if tsm.get("peg_fwd") and tsm["peg_fwd"] < 0.5:
                fails.append("TSM PEG=%s still absurd"
                             % tsm["peg_fwd"])
        if nvda:
            if nvda.get("pe") and not 24 <= nvda["pe"] <= 45:
                fails.append("NVDA trailing pe=%s outside 24-45 "
                             "(external ~31)" % nvda["pe"])
            if nvda.get("fwd_pe") and not 15 <= nvda["fwd_pe"] <= 30:
                fails.append("NVDA fwd_pe=%s outside 15-30 "
                             "(external 20-24)" % nvda["fwd_pe"])
        bad = [t2 for t2, h2 in allsold.items()
               if h2.get("fwd_pe") and h2["fwd_pe"] < 3]
        if bad:
            fails.append("fwd_pe<3 leaked on: %s" % bad[:5])
        if len(pegs) < 8:
            warns.append("fwd PEG on only %d soldiers (estimate "
                         "coverage)" % len(pegs))

        rep.section("1. Page live (this-push marker)")
        pg, ok = "", False
        for i in range(24):
            try:
                pg = get("https://justhodl.ai/industry-rotation.html"
                         "?cb=%d" % time.time())
                if "SQUEEZE" in pg:
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
        for m in ('id="crossboard"', "Cross Board", "fPEG",
                  ">WYCK<", ">RSI<", ">MACD<", ">EXTREMES<",
                  "R:R reads",
                  "acc_state", "= \"+x.rr.ratio+\"R",
                  'id="rrg-barcode"', "ROTATION BARCODE",
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

        rep.section("2. Rail hidden-by-default (site-wide)")
        try:
            rj = get("https://justhodl.ai/jh-right-rail.js?cb=%d"
                     % time.time())
            for m in ("jh_rail_open", "Hidden unless summoned"):
                if m not in rj:
                    fails.append("rail marker missing: %s" % m)
            if 'matches) open_();' in rj:
                fails.append("rail still auto-opens on wide screens")
        except Exception as e:
            fails.append("rail fetch: %s" % str(e)[:80])
        if "MAs \\u00b7" in pg:
            fails.append("literal escape codes still in the header")

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3091.json").write_text(json.dumps(
        {"ops": 3091, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
