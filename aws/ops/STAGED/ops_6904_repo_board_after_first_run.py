"""ops 6904 - READ-ONLY: the board after its first scheduled run on engine 3.1, and the page's test numbers cut again.

Khalid 2026-11-21 22:44 ET: "do all these one by one".  Engine 3.1 was deployed at 04:20 UTC and nothing was invoked:
its own 06:10 UTC run is the first on the new code.  This reads what that run published:

  1. the run itself - engine version, what its own-source routines reported, how many history files it wrote;
  2. item 1: the dealer survey rate and the two series built on it, with the days of known crises;
  3. item 2: the haircut group's newest snapshot; item 3: DTCC's daily fails; and the dollar group left over from
     2026-11-21 (it must have moved past 2026-08-07);
  4. the fixture the page's tests run on (ops 6897's, plus the new series and the dealers' financing with the
     history the page falls back on), printed as gzip + base64.  Public market data.  Nothing is written.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
LIVE = "justhodl-dashboard-live"
PAGE_IDS = ("DFEDTARU", "DTCC-TREASURY-FAILS", "D_BTP_BUND_D", "D_BUND_EA_AAA", "D_DVP_SOFR", "D_DVP_TRI", "D_EFFR_TARGET", "D_FED_REPO", "D_GCF_TRI", "D_ON_SHARE_DVP", "D_ON_SHARE_TRI", "D_SOFR_ADMIN", "D_SOFR_EFFR", "D_SOFR_P75_P25", "D_SOFR_TAIL", "D_TRI_TGCR", "FICC-SPONSORED_REPO_VOL", "FICC-SPONSORED_REVREPO_VOL", "FNYR-BGCR-A", "FNYR-EFFR-A", "FNYR-OBFR-A", "FNYR-SOFR-A", "FNYR-SOFR_UV-A", "FNYR-TGCR-A", "HAIRCUT-agency-mbs--median_haircut", "HAIRCUT-corporates-investment-grade--median_haircut", "HAIRCUT-corporates-non-investment-grade--median_haircut", "HAIRCUT-equities--median_haircut", "HAIRCUT-us-treasuries-excluding-strips--median_haircut", "IORB", "MMF-MMF_RP_OO-M", "MMF-MMF_RP_TOT-M", "MMF-MMF_RP_T_TOT-M", "MMF-MMF_RP_wDFI-M", "MMF-MMF_RP_wFFI-M", "MMF-MMF_RP_wFICC-M", "MMF-MMF_RP_wFR-M", "MMF-MY_MMF_RP_T_OO-M", "MMSR-DE-ON-BORROW-TURNOVER", "MMSR-ES-ON-BORROW-TURNOVER", "MMSR-FR-ON-BORROW-TURNOVER", "MMSR-IT-ON-BORROW-TURNOVER", "NYPD-PD_AFtD_AG_MBS-A", "NYPD-PD_AFtD_T-A", "NYPD-PD_AFtD_TOT-A", "NYPD-PD_AFtR_T-A", "NYPD-PD_SB_T_TOT-A", "NYPD-PD_SL_T_TOT-A", "REPO-DVP_AR_OO-P", "REPO-DVP_OV_TOT-P", "REPO-DVP_TV_TOT-P", "REPO-GCF_AR_TOT-P", "REPO-GCF_TV_TOT-P", "REPO-TRIV1_AR_CORD-P", "REPO-TRIV1_AR_OO-P", "REPO-TRIV1_AR_T-P", "REPO-TRIV1_TV_TOT-P", "RRPONTSYAWARD", "RRPONTSYD", "SOFR30DAYAVG", "SOFR90DAYAVG", "SRF_TAKEUP", "SWPT", "TEDRATE", "WALCL", "WLCFLPCL", "WREPOFOR", "WRESBAL", "WTREGEN")
MEASURES = ("D_REPO_TARGET", "D_TARGET_REPO", "D_SOFR_ADMIN", "D_SOFR_TAIL", "D_SOFR_EFFR", "D_GCF_TRI", "D_EFFR_TARGET", "TEDRATE", "NYPD-PD_AFtD_T-A", "NYPD-PD_AFtD_AG_MBS-A",
            "D_FED_REPO", "SRF_TAKEUP", "WLCFLPCL", "SWPT")
EXTRA_HISTORY = ("SOFR", "SOFR_P99")
NEW_IDS = ("GC_SURVEY", "D_REPO_TARGET", "D_TARGET_REPO")
CRISIS_DAYS = ("1998-08-17", "1998-10-08", "1998-12-31", "1999-12-31", "2001-09-10", "2001-09-18", "2007-08-09", "2007-08-20", "2008-03-17", "2008-09-12", "2008-09-17",
               "2008-09-30", "2008-12-31", "2014-08-22", "2018-02-28", "2018-04-03", "2019-09-17", "2020-03-17", "2025-10-31")
FROM = {"D_EFFR_TARGET": "1986-01-01", "TEDRATE": "1986-01-01"}          # the two long daily series: the fixture keeps them from 1986


def trim(values):
    """Four decimals: the board's derived spreads carry float dust (6.999999999999984) that only bloats a fixture."""
    return [round(v, 4) if isinstance(v, float) else v for v in values]


def selftest():
    assert trim([6.999999999999984, 315.0, 3, None]) == [7.0, 315.0, 3, None]
    assert len(PAGE_IDS) == len(set(PAGE_IDS)) and all(m in PAGE_IDS + NEW_IDS for m in MEASURES)


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 8, "mode": "standard"}))

    def jget(key):
        try:
            return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())
        except Exception as e:  # noqa: BLE001
            return {"_unreadable": type(e).__name__}

    with report("ops_6904_repo_board_after_first_run") as r:
        r.heading("ops 6904 - the board after engine 3.1's first scheduled run, and the page's test numbers (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        board = jget("data/repo.json")
        rows = {s["id"]: dict(s, group=g["name"]) for g in board.get("groups") or [] for s in g.get("series") or []}
        ALL_IDS = PAGE_IDS + NEW_IDS
        missing = [i for i in ALL_IDS if i not in rows]
        diag, counts, fails = board.get("diag") or {}, board.get("counts") or {}, []
        r.section("1. The run")
        r.log("engine %s | as of %s | counts %s" % (board.get("engine_v"), board.get("as_of"), json.dumps(counts)))
        r.log("own sources: %s" % json.dumps(diag.get("own_sources"))[:900])
        r.log("dtcc: %s" % json.dumps(diag.get("dtcc"))[:300])
        if board.get("engine_v") != "3.1":
            r.fail("the board on the site is engine %s: the 3.1 run has not happened yet" % board.get("engine_v"))
            sys.exit(1)
        r.section("2. A daily repo rate before August 2014")
        hist_of = {}
        for sid in NEW_IDS:
            row = rows.get(sid) or {}
            doc = jget("data/repo-history/%s.json" % row.get("sid", sid))
            hist_of[sid] = dict(zip(doc.get("dates") or [], doc.get("values") or []))
            r.log("%-14s %s obs, %s to %s, group %s | %s" % (sid, row.get("n_obs"), row.get("first"), row.get("last"), row.get("group"), (row.get("label") or "")[:110]))
        for day in CRISIS_DAYS:
            r.log("  %s  survey rate %s | repo - policy %s bp | policy - repo %s bp" % (day, hist_of["GC_SURVEY"].get(day), None if hist_of["D_REPO_TARGET"].get(day) is None else round(hist_of["D_REPO_TARGET"][day], 2),
                                                                                    None if hist_of["D_TARGET_REPO"].get(day) is None else round(hist_of["D_TARGET_REPO"][day], 2)))
        below = sorted(hist_of["D_TARGET_REPO"].items(), key=lambda kv: -kv[1])[:12]
        above = sorted(hist_of["D_REPO_TARGET"].items(), key=lambda kv: -kv[1])[:12]
        r.log("  repo furthest BELOW policy: %s" % ", ".join("%s %+.0f" % kv for kv in below))
        r.log("  repo furthest ABOVE policy: %s" % ", ".join("%s %+.0f" % kv for kv in above))
        gc = rows.get("GC_SURVEY") or {}
        if not (gc.get("n_obs") == 4998 and gc.get("first") == "1998-02-20" and gc.get("last") == "2018-02-28" and (rows.get("D_REPO_TARGET") or {}).get("first") == "1998-02-20"
                and (rows.get("D_REPO_TARGET") or {}).get("last") == (rows.get("SOFR") or {}).get("last") and round(hist_of["D_REPO_TARGET"].get("2019-09-17", 0), 2) == 312.5
                and round(hist_of["D_TARGET_REPO"].get("2008-09-17", 0), 2) == 175.0):
            fails.append("item 1: the survey rate or the two series built on it are not as expected")
        r.section("3. Haircuts, DTCC fails, the dollar group")
        by_group = {}
        for s_ in rows.values():
            by_group.setdefault(s_["group"], []).append(s_.get("last") or "")
        for name in ("Tri-party haircuts (NY Fed monthly)", "DTCC daily settlement fails (FICC)", "Dollar (FRED trade-weighted)", "Reference rates & volumes", "Derived stress indicators"):
            lasts = sorted(by_group.get(name) or [])
            tally = {}
            for d_ in lasts:
                tally[d_] = tally.get(d_, 0) + 1
            r.log("%-40s %3d series | newest dates %s" % (name, len(lasts), dict(sorted(tally.items(), reverse=True)[:4])))
        for sid in ("HAIRCUT-us-treasuries-excluding-strips--median_haircut", "HAIRCUT-agency-mbs--median_haircut", "HAIRCUT-corporates-non-investment-grade--median_haircut",
                    "HAIRCUT-equities--median_haircut", "HAIRCUT-total--collateral_value", "HAIRCUT-sponsored-gc--collateral_value", "DTCC-TREASURY-FAILS", "DTCC-AGENCY-FAILS", "DTWEXBGS"):
            row = rows.get(sid) or {}
            r.log("  %-58s %s obs to %s: %s | %s" % (sid, row.get("n_obs"), row.get("last"), row.get("last_value"), (row.get("label") or "")[:60]))
        hair = sorted(by_group.get("Tri-party haircuts (NY Fed monthly)") or [])
        if not (len(hair) == 81 and hair.count("2026-10-09") == 73):
            fails.append("item 2: haircut group %d series, %d at 2026-10-09" % (len(hair), hair.count("2026-10-09")))
        dt_ = rows.get("DTCC-TREASURY-FAILS") or {}
        if not ((dt_.get("last") or "") >= "2026-11-19" and (dt_.get("n_obs") or 0) >= 317 and "banked history" not in (dt_.get("label") or "") and (diag.get("dtcc") or {}).get("url", "").startswith("https://cms-prod.dtcc.com/")):
            fails.append("item 3: DTCC fails %s obs to %s, diag %s" % (dt_.get("n_obs"), dt_.get("last"), json.dumps(diag.get("dtcc"))[:160]))
        dollar = max(by_group.get("Dollar (FRED trade-weighted)") or [""])
        if dollar <= "2026-08-07":
            fails.append("the dollar group is still at %s" % dollar)
        r.section("4. The page's test numbers")
        r.log("board engine %s as of %s: %d series in %d groups | ids the page reads: %d, not on the board: %s" % (
            board.get("engine_v"), board.get("as_of"), len(rows), len(board.get("groups") or []), len(ALL_IDS), missing))
        desk = jget("data/repo-market.json")
        r.log("desk feed generated %s, as of %s, policy block %s" % (desk.get("generated_at"), desk.get("as_of"), json.dumps(desk.get("policy"))[:200]))
        pd = jget("data/nyfed-primary-dealer.json")
        fixture = {"cut_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                   "source": "ops 6904: data/repo.json (engine %s), data/repo-history/*, data/repo-market.json, data/nyfed-primary-dealer.json; values to four decimals" % board.get("engine_v"),
                   "board": {"as_of": board.get("as_of"), "engine_v": board.get("engine_v"), "counts": board.get("counts"),
                             "groups": [{"name": g["name"], "n": len(g.get("series") or [])} for g in board.get("groups") or []]},
                   "board_ids": sorted(rows), "rows": {i: rows[i] for i in ALL_IDS if i in rows}, "history": {},
                   "desk": {k: v for k, v in desk.items() if k not in ("how_to_read", "disclaimer")},
                   "dealer": {"version": pd.get("version"), "as_of": pd.get("as_of"),
                              "financing": {"treasury": {k: v for k, v in ((pd.get("financing") or {}).get("treasury") or {}).items() if k in ("as_of", "repo_out_b", "reverse_repo_in_b", "gross_two_sided_b")}},
                              "groups": {g: {"history": ((pd.get("groups") or {}).get(g) or {}).get("history", [])[-12:]} for g in ("treasury_repo", "treasury_reverse_repo")}}}
        r.log("dealer document %s, week %s: %s | last rows of the reverse-repo group %s" % (pd.get("version"), pd.get("as_of"), json.dumps(fixture["dealer"]["financing"]), json.dumps(fixture["dealer"]["groups"]["treasury_reverse_repo"]["history"][-3:])))
        for sid in MEASURES + EXTRA_HISTORY:
            if sid not in rows:
                continue
            doc = jget("data/repo-history/%s.json" % rows[sid]["sid"])
            pairs = [(d, v) for d, v in zip(doc.get("dates") or [], doc.get("values") or []) if d >= FROM.get(sid, "1900-01-01")]
            fixture["history"][sid] = {"label": doc.get("label"), "dates": [d for d, _ in pairs], "values": trim([v for _, v in pairs])}
            r.log("  history %-24s %d observations %s .. %s" % (sid, len(pairs), pairs[0][0] if pairs else "-", pairs[-1][0] if pairs else "-"))
        blob = base64.b64encode(gzip.compress(json.dumps(fixture, separators=(",", ":")).encode(), 9)).decode()
        r.log("fixture: %d rows, %d histories, %d board ids, %d bytes of base64" % (len(fixture["rows"]), len(fixture["history"]), len(fixture["board_ids"]), len(blob)))
        for i in range(0, len(blob), 6000):
            r.log("FIXTURE %04d %s" % (i // 6000, blob[i:i + 6000]))
        r.kv(engine=board.get("engine_v"), page_ids=len(ALL_IDS), not_on_board=len(missing), board_series=len(rows), history_written=counts.get("history_written"),
             haircuts_at_newest=hair.count("2026-10-09"), dtcc_last=dt_.get("last"), dollar_newest=dollar, failed_checks=len(fails))
        if missing:
            fails.append("the page names %d series the board does not carry: %s" % (len(missing), missing))
        if fails:
            r.fail(" | ".join(fails))
            sys.exit(1)
        r.ok("engine 3.1's first scheduled run: the survey rate from 1998, 73 haircut series at 2026-10-09, DTCC fails to %s, the dollar group at %s; every series the page reads is on the board" % (dt_.get("last"), dollar))
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
