"""ops 6897 - READ-ONLY: the real numbers the Repo desk page is tested against, cut from the rebuilt board.

The page (repo.html + jh-repo-desk.js) reads 69 rows of the master board by id and the histories of its 12 crisis
measures.  Its tests run the page's own arithmetic on real numbers, so this cuts them from the live bucket after the
board was rebuilt by engine 3.0 (ops 6896): every id on the board (so a tile naming a series that does not exist fails
a test), the rows the page reads, the histories of the measures, the desk feed and the dealers' financing block.
Printed at the end as gzip + base64 (public market data).  Nothing is written.
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
MEASURES = ("D_SOFR_ADMIN", "D_SOFR_TAIL", "D_SOFR_EFFR", "D_GCF_TRI", "D_EFFR_TARGET", "TEDRATE", "NYPD-PD_AFtD_T-A", "NYPD-PD_AFtD_AG_MBS-A",
            "D_FED_REPO", "SRF_TAKEUP", "WLCFLPCL", "SWPT")
EXTRA_HISTORY = ("SOFR", "SOFR_P99")
FROM = {"D_EFFR_TARGET": "1986-01-01", "TEDRATE": "1986-01-01"}          # the two long daily series: the fixture keeps them from 1986


def trim(values):
    """Four decimals: the board's derived spreads carry float dust (6.999999999999984) that only bloats a fixture."""
    return [round(v, 4) if isinstance(v, float) else v for v in values]


def selftest():
    assert trim([6.999999999999984, 315.0, 3, None]) == [7.0, 315.0, 3, None]
    assert len(PAGE_IDS) == len(set(PAGE_IDS)) and all(m in PAGE_IDS for m in MEASURES)


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

    with report("ops_6897_repo_desk_fixture") as r:
        r.heading("ops 6897 - real numbers for the Repo desk page's tests (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        board = jget("data/repo.json")
        rows = {s["id"]: dict(s, group=g["name"]) for g in board.get("groups") or [] for s in g.get("series") or []}
        missing = [i for i in PAGE_IDS if i not in rows]
        r.log("board engine %s as of %s: %d series in %d groups | ids the page reads: %d, not on the board: %s" % (
            board.get("engine_v"), board.get("as_of"), len(rows), len(board.get("groups") or []), len(PAGE_IDS), missing))
        for i in PAGE_IDS:
            s = rows.get(i)
            if s:
                r.log("  %-52s %-10s %s | %s" % (i, s.get("last"), s.get("last_value"), (s.get("label") or "")[:70]))
        desk = jget("data/repo-market.json")
        r.log("desk feed generated %s, as of %s, policy block %s" % (desk.get("generated_at"), desk.get("as_of"), json.dumps(desk.get("policy"))[:200]))
        pd = jget("data/nyfed-primary-dealer.json")
        fixture = {"cut_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                   "source": "ops 6897: data/repo.json (engine %s), data/repo-history/*, data/repo-market.json, data/nyfed-primary-dealer.json; values to four decimals" % board.get("engine_v"),
                   "board": {"as_of": board.get("as_of"), "engine_v": board.get("engine_v"), "counts": board.get("counts"),
                             "groups": [{"name": g["name"], "n": len(g.get("series") or [])} for g in board.get("groups") or []]},
                   "board_ids": sorted(rows), "rows": {i: rows[i] for i in PAGE_IDS if i in rows}, "history": {},
                   "desk": {k: v for k, v in desk.items() if k not in ("how_to_read", "disclaimer")},
                   "primary_dealer_financing": {"treasury": {k: v for k, v in ((pd.get("financing") or {}).get("treasury") or {}).items() if k in ("as_of", "repo_out_b", "reverse_repo_in_b", "gross_two_sided_b")}}}
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
        r.kv(page_ids=len(PAGE_IDS), not_on_board=len(missing), board_series=len(rows))
        if missing:
            r.warn("the page names %d series the board does not carry: %s" % (len(missing), missing))
        else:
            r.ok("every series the page reads is on the board")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
