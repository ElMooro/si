#!/usr/bin/env python3
"""ops 3789 — verify v7: filters, growth column, ? tooltips, page description.

Khalid asked for five things. All shipped on capture-gap.html v7:
  [1] size dropdown  — S&P 500 only / mega / large / mid / small+micro
  [2] growth dropdown — high (>=20% YoY) / medium (5-20%) / low (<5%)
  [3] growth + gross-margin columns on the leaderboard
  [4] a ? mark on every metric explaining what it measures
  [5] a description block naming what the page does and which engines feed it

Engine side landed in 3788 (v4.2): revenue_growth_yoy / _3y_cagr / growth_tier /
in_sp500. Growth is a RATIO so it is currency-invariant — it publishes for
non-USD filers even where revenue_share_pct is suppressed, which is why 66
non-USD names carry growth but no share.

Edge-verified: repo state is not proof of live.
"""
import sys, time, json, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
UA = "JustHodl.AI ops-verify raafouis@gmail.com"
URL = "https://justhodl.ai/capture-gap.html"
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


def fetch(u, a=0):
    x = u + ("&" if "?" in u else "?") + "v=%d%d" % (int(time.time()), a)
    req = urllib.request.Request(x, headers={
        "User-Agent": UA, "Cache-Control": "no-cache", "Pragma": "no-cache"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.status, r.read().decode("utf-8", "replace")


def main():
    with report("3789_verify_filters_tooltips") as rep:
        rep.heading("ops 3789 — verify v7 filters / growth / tooltips / description")

        rep.section("Feed precondition (v4.2)")
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                     Key="data/chokepoint.json")["Body"].read())
        cap = d.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        st = cap.get("stats") or {}
        gate(rep, "FEED.v42", str(d.get("version", "")).startswith("4.2"),
             "engine v%s" % d.get("version"))
        rep.kv(scored=st.get("scored"), with_growth=st.get("with_growth"),
               sp500=st.get("sp500_members"))
        gate(rep, "FEED.growth", (st.get("with_growth") or 0) > 500,
             "growth on %s names" % st.get("with_growth"))
        gate(rep, "FEED.sp500", (st.get("sp500_members") or 0) > 200,
             "%s S&P500 members" % st.get("sp500_members"))

        # every filter option must actually match rows, or the dropdown is a lie
        rep.section("Do the filters actually select anything?")
        caps = {"mega": ["mega"], "large": ["large"], "mid": ["mid"],
                "small": ["small", "micro", "nano"]}
        for k, v in caps.items():
            n = sum(1 for r in rows if r.get("cap_bucket") in v)
            gate(rep, f"FILTER.cap_{k}", n > 0, "%d names" % n)
        n_sp = sum(1 for r in rows if r.get("in_sp500") is True)
        gate(rep, "FILTER.sp500", n_sp > 0, "%d names" % n_sp)
        for t in ("HIGH", "MEDIUM", "LOW"):
            n = sum(1 for r in rows if r.get("growth_tier") == t)
            gate(rep, f"FILTER.growth_{t}", n > 0, "%d names" % n)

        # the leaderboard is what the filters act on — it must carry the fields
        lead = cap.get("top_undervalued_all_industries") or []
        rep.kv(leaderboard=len(lead))
        for f in ("growth_tier", "in_sp500", "revenue_growth_yoy", "gm_level"):
            n = sum(1 for r in lead if r.get(f) is not None)
            gate(rep, f"LEAD.{f}", n > 0, "%d of %d leaderboard rows" % (n, len(lead)))

        rep.section("Served page v7")
        M = {"stamp": "v7-ops3789", "gloss": "var GLOSS=", "help_fn": "function help(",
             "cap_select": 'id="ldCap"', "gro_select": 'id="ldGro"',
             "growth_col": "'Rev growth<br>YoY'", "gm_col": "'Gross<br>margin'",
             "gro_fn": "function gro(", "about": 'id="about"',
             "hm_class": 'class="hm"', "sp500_opt": "S&P 500 only"}
        body = ""
        for a in range(1, 11):
            try:
                stt, body = fetch(URL, a)
            except Exception as e:
                rep.warn("attempt %d: %s" % (a, str(e)[:100])); time.sleep(25); continue
            h = sum(1 for m in M.values() if m in body)
            rep.log("attempt %d: HTTP %s · %d bytes · %d/%d" % (a, stt, len(body), h, len(M)))
            if h == len(M):
                break
            time.sleep(25)
        for k, m in M.items():
            gate(rep, f"SERVED.{k}", m in body, "present")

        rep.section("Description names the engines it pulls from")
        for e in ("justhodl-chokepoint", "universe-builder", "justhodl-backlog",
                  "fundamental-census", "supply-chain-graph", "Honest limits"):
            gate(rep, "DESC." + e.replace("-", "_")[:22], e in body, "cited")

        rep.section("Additive — v6 surfaces intact")
        for k in ("Most Undervalued", "By Industry", "Full Ledger", "rsh(", "dep(",
                  "percent_critical_note", "data-lk", "data-bk", "Default rank is blended"):
            gate(rep, "KEPT." + k.replace(" ", "_")[:20], k in body, "intact")

        rep.section("Sample of what a filtered view returns")
        hi = [r for r in lead if r.get("growth_tier") == "HIGH"]
        rep.log("  HIGH-growth names on the leaderboard: %d" % len(hi))
        for r in hi[:8]:
            rep.log("    %-6s %-24s growth=%-7s sp500=%-5s cap=%-6s gap=%+.1fpp" % (
                r.get("ticker"), (r.get("industry") or "")[:24],
                ("%.1f%%" % r["revenue_growth_yoy"]) if r.get("revenue_growth_yoy") is not None else "—",
                r.get("in_sp500"), r.get("cap_bucket"), r.get("capture_gap") or 0))

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — filters, growth, tooltips and provenance all live")


if __name__ == "__main__":
    main()
