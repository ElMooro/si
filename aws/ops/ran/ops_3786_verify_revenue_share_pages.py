#!/usr/bin/env python3
"""ops 3786 — verify revenue-share surfaces on BOTH pages (edge-verified).

Khalid: "add Revenue share of industry to the page and you should add that to
why.html". Shipped on both:
  capture-gap.html v6 — Rev share + Dep % + Crit %ile columns on the
    leaderboard, the full ledger and the industry-member tables, plus the
    three-percentages note in Method.
  why.html — REV SHARE / CRITICALITY %ile / SUPPLY-CHAIN DEP tiles in Vitals.

HONESTY CARRIED INTO THE UI, not just the JSON (ops 3785 found SKHY filing in
KRW took 95% of the semiconductor denominator and crushed NVDA to 0.21%):
  - a suppressed share renders as "—" WITH the reason, never as 0 or a number
    from a mixed-currency total;
  - the sub-label says "of USD-filing peers", not "of the industry";
  - the CRITICALITY tile says explicitly it is NOT a share of the industry, so
    a 0-100 quality composite can never be misread as a dependency percentage.

Verified from the EDGE with markers unique to this change — repo state is not
proof of live, and [skip-deploy] auto-commits have silently withheld page work
in this arc before.
"""
import sys, time, json, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report
import boto3

UA = "JustHodl.AI ops-verify raafouis@gmail.com"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


def fetch(url, a=0):
    u = url + ("&" if "?" in url else "?") + "v=%d%d" % (int(time.time()), a)
    req = urllib.request.Request(u, headers={
        "User-Agent": UA, "Cache-Control": "no-cache", "Pragma": "no-cache"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.status, r.read().decode("utf-8", "replace")


def wait_for(rep, url, markers, label):
    body = ""
    for a in range(1, 10):
        try:
            st, body = fetch(url, a)
        except Exception as e:
            rep.warn("%s attempt %d: %s" % (label, a, str(e)[:100]))
            time.sleep(25); continue
        hits = sum(1 for m in markers.values() if m in body)
        rep.log("%s attempt %d: HTTP %s · %d bytes · %d/%d" % (
            label, a, st, len(body), hits, len(markers)))
        if hits == len(markers):
            break
        time.sleep(25)
    return body


def main():
    with report("3786_verify_revenue_share_pages") as rep:
        rep.heading("ops 3786 — revenue share served on capture-gap + why.html")

        rep.section("Feed precondition (v4.1.1)")
        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        cap = d.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        st = cap.get("stats") or {}
        gate(rep, "FEED.version", str(d.get("version", "")).startswith("4.1"),
             "engine v%s" % d.get("version"))
        pub = [r for r in rows if r.get("revenue_share_pct") is not None]
        sup = [r for r in rows if r.get("revenue_share_suppressed")]
        rep.kv(scored=st.get("scored"), publishing_share=len(pub), suppressed=len(sup),
               with_dependency=st.get("with_dependency"))
        gate(rep, "FEED.share_published", len(pub) > 100, "%d names publish a share" % len(pub))
        gate(rep, "FEED.suppression_reasons", len(sup) > 0,
             "%d names carry an explicit suppression reason" % len(sup))
        bad = [r for r in pub if (r.get("revenue_currency") or "").upper() != "USD"]
        gate(rep, "FEED.usd_purity", len(bad) == 0,
             "%d non-USD filers publishing (must be 0)" % len(bad))

        rep.section("capture-gap.html v6")
        CG = {"stamp": "v6-ops3786", "rsh_fn": "function rsh(", "dep_fn": "function dep(",
              "col_rev": "'Rev share'", "col_dep": "'Dep %'",
              "note": "percent_critical_note", "member_rev": "rsh(m)"}
        b1 = wait_for(rep, "https://justhodl.ai/capture-gap.html", CG, "capture-gap")
        for k, m in CG.items():
            gate(rep, f"CG.{k}", m in b1, "present")
        for k in ("Most Undervalued", "By Industry", "Full Ledger", "catchup_pct",
                  "Default rank is blended", "data-lk", "data-bk"):
            gate(rep, "CG.KEPT." + k.replace(" ", "_")[:20], k in b1, "intact")

        rep.section("why.html")
        WH = {"rev_tile": "REV SHARE", "crit_tile": "CRITICALITY",
              "not_a_share": "NOT a share", "usd_label": "USD-filing peers",
              "capture_fn": "jhCaptureTiles"}
        b2 = wait_for(rep, "https://justhodl.ai/why.html", WH, "why")
        for k, m in WH.items():
            gate(rep, f"WHY.{k}", m in b2, "present")
        for k in ("P/E TTM", "PEG", "EV/EBITDA", "SHARES OUT", "DILUTION",
                  "CAPTURE GAP", "CATCH-UP", "fillJHVitals"):
            gate(rep, "WHY.KEPT." + k.replace("/", "_").replace(" ", "_"), k in b2, "intact")

        rep.section("Sample — what a reader will actually see")
        for t in ("NVDA", "AVGO", "AMD", "TSM", "SKHY", "MSFT"):
            r = next((x for x in rows if x.get("ticker") == t), None)
            if not r:
                rep.log("  %-6s not scored" % t); continue
            rs = ("%.2f%%" % r["revenue_share_pct"]) if r.get("revenue_share_pct") is not None else "—"
            rep.log("  %-6s rev_share=%-8s ccy=%-5s crit_pctile=%-6s dep=%-7s %s" % (
                t, rs, r.get("revenue_currency") or "?", r.get("criticality_pctile"),
                ("%.1f%%" % r["dependency_pct"]) if r.get("dependency_pct") is not None else "—",
                ("[" + str(r.get("revenue_share_suppressed"))[:46] + "]") if r.get("revenue_share_suppressed") else ""))

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — revenue share live on both surfaces, suppression reasons visible")


if __name__ == "__main__":
    main()
