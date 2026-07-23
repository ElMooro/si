#!/usr/bin/env python3
"""ops 3782 — verify capture-gap v5 sorting controls are SERVED.

Khalid asked for: sort by industry, ascending/descending, and the most
undervalued list at the top of the page. AUDIT FIRST (per build rule) found two
of the three ALREADY shipped — the leaderboard was already section #1 and the
Full Ledger already had click-to-sort headers since 3772. So v5 adds only the
two real gaps rather than rebuilding what worked:
  [1] leaderboard: click-to-sort on every column + asc/desc toggle
  [2] leaderboard: industry filter dropdown (jump to one industry's names)
  [3] industry board: sortable headers (was fixed-order by median gap)

Verified from the EDGE with markers unique to v5, because repo state is not
proof of live and [skip-deploy] auto-commits have silently withheld page work
in this arc before.
"""
import sys, time, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report

URL = "https://justhodl.ai/capture-gap.html"
UA = "JustHodl.AI ops-verify raafouis@gmail.com"
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


def fetch(url, attempt=0):
    u = url + ("&" if "?" in url else "?") + "v=%d%d" % (int(time.time()), attempt)
    req = urllib.request.Request(u, headers={
        "User-Agent": UA, "Cache-Control": "no-cache", "Pragma": "no-cache"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.status, r.read().decode("utf-8", "replace")


def main():
    with report("3782_verify_sorting_v5") as rep:
        rep.heading("ops 3782 — verify v5 sorting controls served")

        M = {
            "version_stamp": "v5-ops3782",
            "leader_sort_attr": 'data-lk="',
            "industry_sort_attr": 'data-bk="',
            "render_leader": "renderLeader",
            "render_byind": "renderByInd",
            "industry_filter": 'id="ldSel"',
            "asc_desc_hint": "ascending / descending",
        }
        body = ""
        for a in range(1, 10):
            try:
                st, body = fetch(URL, a)
            except Exception as e:
                rep.warn("attempt %d: %s" % (a, str(e)[:110]))
                time.sleep(25); continue
            hits = sum(1 for m in M.values() if m in body)
            rep.log("attempt %d: HTTP %s · %d bytes · %d/%d markers" % (
                a, st, len(body), hits, len(M)))
            if hits == len(M):
                break
            time.sleep(25)

        rep.section("v5 controls")
        for k, m in M.items():
            gate(rep, f"SERVED.{k}", m in body, "present")

        rep.section("Additive — v4 sections and blend note must survive")
        for k in ("Most Undervalued", "By Industry", "Structurally Undervalued",
                  "Hidden Capture Gaps", "Creation vs Capture", "Full Ledger",
                  "Cross-Industry Gap", "Under-Capitalised Industries",
                  "top_undervalued_all_industries", "by_industry",
                  "catchup_pct", "Default rank is blended"):
            gate(rep, "KEPT." + k.replace(" ", "_")[:26], k in body, "intact")

        rep.section("Leaderboard is still the FIRST board on the page")
        i_lead = body.find("Most Undervalued")
        i_ind = body.find("By Industry")
        i_full = body.find("Full Ledger")
        rep.kv(pos_leaderboard=i_lead, pos_by_industry=i_ind, pos_full_ledger=i_full)
        gate(rep, "ORDER.leaderboard_first",
             0 < i_lead < i_ind and i_lead < i_full,
             "Most Undervalued precedes By Industry and Full Ledger")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — sortable leaderboard + industry filter + sortable industry board live")


if __name__ == "__main__":
    main()
