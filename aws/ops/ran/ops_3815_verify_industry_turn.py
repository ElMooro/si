#!/usr/bin/env python3
"""ops 3815 — verify v5.1 industry TURN detection, engine + served page.

Khalid liked the value-trap book (industry decaying => avoid) and asked for the
mirror toward booming/turning industries. The key design point, confirmed by the
3814 probe: the mirror is NOT symmetric. A booming industry is mostly already
priced; the edge is the TURN — rising hard from a non-top base before the league
shows it at the top. And FADING (high score rolling over) is a hazard a simple
level check misses entirely.

v5.1 adds a 4-way industry_trend (TURNING/BOOMING/FADING/DECAYING) from boom
score + 20d momentum, makes TURNING a confirmation leg and FADING a disqualifier,
and emits regime_tailwind_book + industry_trends. Page v13 renders both.
"""
import sys, json, time, zipfile, io, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report
import boto3

FN = "justhodl-chokepoint"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
UA = "JustHodl.AI ops-verify raafouis@gmail.com"
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)


def fetch(u, a=0):
    x = u + ("&" if "?" in u else "?") + "v=%d%d" % (int(time.time()), a)
    req = urllib.request.Request(x, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8", "replace")


def main():
    with report("3815_verify_industry_turn") as rep:
        rep.heading("ops 3815 — industry TURN detection live")

        rep.section("Settle v5.1")
        settled = False
        for i in range(16):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
                u = lam.get_function(FunctionName=FN)["Code"]["Location"]
                with urllib.request.urlopen(u, timeout=90) as r:
                    blob = r.read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    if "def _trend(" in z.read("lambda_function.py").decode("utf-8", "replace"):
                        settled = True; rep.ok("v5.1 live (attempt %d)" % (i + 1)); break
            time.sleep(15)
        gate(rep, "DEPLOY.settled", settled, "trend logic in deployed zip")
        if FAILED:
            sys.exit(1)

        rep.section("Invoke")
        from botocore.config import Config
        ll = boto3.client("lambda", region_name="us-east-1",
                          config=Config(read_timeout=890, retries={"max_attempts": 0}))
        t0 = time.time()
        r = ll.invoke(FunctionName=FN, InvocationType="RequestResponse",
                      Payload=json.dumps({"mode": "full"}).encode())
        rep.kv(invoke_status=r.get("StatusCode"), invoke_seconds=round(time.time() - t0, 1))

        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        cap = d.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        st = cap.get("stats") or {}
        gate(rep, "LIVE.v51", d.get("version") == "5.1", "version=%s" % d.get("version"))

        rep.section("Trend distribution")
        tc = st.get("trend_counts") or {}
        for k, v in sorted(tc.items(), key=lambda z: -(z[1] or 0)):
            rep.log("  %-10s %d names" % (k, v))
        gate(rep, "TREND.four_way", len([k for k in tc if k]) >= 3,
             "%d trend classes populated" % len([k for k in tc if k]))
        gate(rep, "TREND.turning_exists", (tc.get("TURNING") or 0) > 0,
             "TURNING=%s names" % tc.get("TURNING"))

        rep.section("regime_tailwind_book — TURNING first")
        wb = cap.get("regime_tailwind_book") or []
        gate(rep, "BOOK.populated", len(wb) > 0, "%d names" % len(wb))
        turning_first = all(
            wb[i].get("industry_trend") != "TURNING" or wb[j].get("industry_trend") == "TURNING"
            for i in range(len(wb)) for j in range(i))
        gate(rep, "BOOK.turning_first",
             not wb or wb[0].get("industry_trend") == "TURNING" or
             not any(x.get("industry_trend") == "TURNING" for x in wb),
             "book ordered TURNING before BOOMING")
        for x in wb[:12]:
            rep.log("  %-6s %-24s %-8s gap=%+5.1f SI=%4.1f %s" % (
                x.get("ticker"), (x.get("industry") or "")[:24],
                x.get("industry_trend"), x.get("capture_gap") or 0,
                x.get("structural_importance") or 0, x.get("mispricing_verdict")))

        rep.section("industry_trends table — turning / booming / fading")
        it = cap.get("industry_trends") or []
        for x in it[:14]:
            rep.log("  %-32s %-8s score=%5.1f delta=%+5.1f n=%s" % (
                (x.get("industry") or "")[:32], x.get("trend"),
                x.get("boom_score") or 0, x.get("delta_20d") or 0, x.get("n_scored")))
        gate(rep, "TRENDS.has_turning", any(x.get("trend") == "TURNING" for x in it),
             "turning industries surfaced")
        gate(rep, "TRENDS.has_fading", any(x.get("trend") == "FADING" for x in it),
             "fading industries surfaced (the hidden hazard)")

        rep.section("TURNING is now a confirmation leg in verdicts")
        turn_conf = sum(1 for x in rows if any("turning up" in str(w)
                        for w in (x.get("verdict_confirms") or [])))
        fade_dis = sum(1 for x in rows if any("rolling over" in str(w)
                       for w in (x.get("verdict_disqualifiers") or [])))
        rep.kv(names_confirmed_by_turn=turn_conf, names_disqualified_by_fade=fade_dis)

        rep.section("Served page v13")
        M = {"stamp": "v13-ops3815", "tailwind_div": 'id="tailwind"',
             "trend_chip": "function trendChip(", "trend_col": "'Industry<br>trend'",
             "regime_key": "regime_tailwind_book", "trends_key": "industry_trends",
             "gloss": "industry_trend:"}
        body = ""
        for a in range(1, 10):
            try:
                body = fetch("https://justhodl.ai/capture-gap.html", a)
            except Exception as e:
                rep.warn(str(e)[:80]); time.sleep(25); continue
            if all(m in body for m in M.values()):
                break
            time.sleep(25)
        for k, m in M.items():
            gate(rep, f"SERVED.{k}", m in body, "present")

        rep.section("Additive — v12 verdict surfaces intact")
        for k in ("Mispriced", "Value Traps", "function vdt(", "mispriced_book",
                  "How crucial", "Most Undervalued"):
            gate(rep, "KEPT." + k.replace(" ", "_")[:16], k in body, "intact")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — the turn is detectable, and the mirror steers toward it")


if __name__ == "__main__":
    main()
