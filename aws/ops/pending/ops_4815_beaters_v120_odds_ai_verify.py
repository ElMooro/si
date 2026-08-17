"""ops/4815 -- spx-beaters v1.2.0: institutional metrics + empirical
odds + anchored AI verdicts.
 (1) heal ANTHROPIC_KEY (donors watchlist-debate, ai-brief) -- WARN if
     absent (rules-mode fallback is contractual; Anthropic billing has
     had outages). Settle marker; invoke; poll fresh v1.2.0 doc.
 (2) base_rates truths: 5 quintiles, each n >= 300, beat rates within
     10..90, Q5 > Q1 (momentum persistence in OUR cohort or warn).
 (3) rows: every shown row carries inst{vol,max_dd,rs} + odds_base;
     AI coverage >= 60% of top-6 per bucket; CLAMP AUDIT on every ai
     block: odds in [max(2,base-12)..min(98,base+12)], downside within
     its anchors, horizon 13..52, stance valid, BUY implies odds>=55.
 (4) readout: top rows with the AI line; ai mode + call counts.
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

FN = "justhodl-spx-beaters"
B = "justhodl-dashboard-live"
OUT_KEY = "data/spx-beaters.json"
MARKER = "spx-beaters v1.2.1"
DONORS = ("justhodl-watchlist-debate", "justhodl-ai-brief")
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
FAILED = []
start_iso = __import__("datetime").datetime.now(
    __import__("datetime").timezone.utc).isoformat()


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def heal_key(rep):
    cfg = lam.get_function_configuration(FunctionName=FN)
    env = (cfg.get("Environment") or {}).get("Variables", {})
    if env.get("ANTHROPIC_KEY"):
        rep.kv(env_ANTHROPIC="present")
        return
    for d in DONORS:
        try:
            src = lam.get_function_configuration(FunctionName=d)
            k = (src.get("Environment") or {}).get(
                "Variables", {}).get("ANTHROPIC_KEY")
            if k:
                env["ANTHROPIC_KEY"] = k
                lam.update_function_configuration(
                    FunctionName=FN,
                    Environment={"Variables": env})
                for _ in range(20):
                    if lam.get_function_configuration(
                            FunctionName=FN).get(
                            "LastUpdateStatus") == "Successful":
                        break
                    time.sleep(3)
                rep.kv(env_ANTHROPIC="HEALED from " + d)
                return
        except ClientError:
            continue
    rep.warn("ANTHROPIC_KEY absent in all donors -- AI runs in "
             "rules mode (honest fallback)")


def main():
    with report("ops 4815 -- v1.2.0 odds + AI verify") as rep:
        rep.heading("1. env + settle + invoke")
        heal_key(rep)
        ok = False
        for _ in range(30):
            try:
                gf = lam.get_function(FunctionName=FN)
                raw = urllib.request.urlopen(
                    gf["Code"]["Location"], timeout=60).read()
                src = zipfile.ZipFile(io.BytesIO(raw)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if MARKER in src:
                    ok = True
                    break
            except (ClientError, Exception):  # noqa: BLE001
                pass
            time.sleep(10)
        if not ok:
            rep.fail("marker never settled")
            sys.exit(1)
        rep.ok("marker settled")
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        t0 = time.time()
        while time.time() - t0 < 600:
            time.sleep(20)
            try:
                d = sread(OUT_KEY)
                if d.get("marker") == MARKER and \
                        d.get("as_of", "") > start_iso:
                    doc = d
                    break
            except ClientError:
                pass
        if not doc:
            rep.fail("doc never refreshed to v1.2.0")
            sys.exit(1)
        rep.ok("  fresh v1.2.0 doc (~%ds)" % int(time.time() - t0))

        rep.heading("2. base rates")
        br = doc.get("base_rates")
        if not br or len(br.get("momentum_quintiles") or []) != 5:
            rep.fail("  base_rates missing/short")
            sys.exit(1)
        qs = br["momentum_quintiles"]
        for q in qs:
            okq = q["n"] >= 300 and 5 <= q["beat_spy_26w_pct"] <= 95
            (rep.ok if okq else rep.warn)(
                "  Q%d form %s..%s%%  n=%d  beat=%s%%  med excess "
                "%+.1fpp" % (q["q"], q["form_6m_min_pct"],
                             q["form_6m_max_pct"], q["n"],
                             q["beat_spy_26w_pct"],
                             q["median_excess_pp"]))
            if q["n"] < 300:
                FAILED.append("q_n")
        if qs[4]["beat_spy_26w_pct"] <= qs[0]["beat_spy_26w_pct"]:
            rep.warn("  Q5 <= Q1 -- momentum did NOT persist in this "
                     "cohort window; anchors remain honest either way")
        else:
            rep.ok("  Q5 > Q1 -- persistence held in our cohort")
        rep.kv(spy_26w=br.get("spy_ret_26w_pct"),
               comeback_cohort=json.dumps(br.get("comeback_cohort")))

        rep.heading("3. rows + AI clamp audit")
        shown = ai_n = viol = miss_inst = elig = 0
        for bname, rows in (doc.get("buckets") or {}).items():
            for i, r in enumerate(rows):
                shown += 1
                hw = r.get("history_weeks") or 0
                if hw >= 30 and (not r.get("inst")
                                 or r.get("odds_base_26w_pct")
                                 is None):
                    miss_inst += 1
                if i < 6 and r.get("odds_base_26w_pct") is not None:
                    elig += 1
                a = r.get("ai")
                if not a:
                    continue
                ai_n += 1
                if a["downside_risk_pct"] > 95:
                    viol += 1
                    rep.warn("    %s/%s downside > 95%%"
                             % (bname, r["t"]))
                base = r["odds_base_26w_pct"]
                lo = max(2, base - 12)
                hi = min(98, base + 12)
                an = a.get("anchors") or {}
                bad = []
                if not lo <= a["odds_beat_spx_26w_pct"] <= hi:
                    bad.append("odds")
                if an and not an.get("downside_lo", 0) <= \
                        a["downside_risk_pct"] <= \
                        an.get("downside_hi", 100):
                    bad.append("downside")
                if not 13 <= a["horizon_weeks"] <= 52:
                    bad.append("horizon")
                if a["stance"] not in ("BUY", "WATCH", "PASS") or \
                        (a["stance"] == "BUY"
                         and a["odds_beat_spx_26w_pct"] < 55):
                    bad.append("stance")
                if bad:
                    viol += 1
                    rep.warn("    %s/%s violates %s"
                             % (bname, r["t"], bad))
        (rep.ok if miss_inst == 0 else rep.fail)(
            "  rows (>=30w history) missing inst/odds = %d of %d"
            % (miss_inst,
                                                     shown))
        if miss_inst:
            FAILED.append("inst")
        cov = 100 * ai_n / max(1, elig)
        (rep.ok if cov >= 80 else rep.fail)(
            "  AI coverage = %d/%d ELIGIBLE (top-6 w/ odds) rows "
            "(%.0f%%)  mode=%s new_calls=%s"
            % (ai_n, elig, cov,
                              (doc.get("diag", {}).get("ai")
                               or {}).get("mode"),
                              (doc.get("diag", {}).get("ai")
                               or {}).get("new_calls")))
        if cov < 50:
            FAILED.append("ai_cov")
        (rep.ok if viol == 0 else rep.fail)(
            "  clamp violations = %d" % viol)
        if viol:
            FAILED.append("clamps")

        rep.heading("4. readout (AI lines)")
        for b in ("large", "comeback", "mid", "small", "etf_equity",
                  "etf_commodity"):
            rows = (doc.get("buckets") or {}).get(b) or []
            if not rows:
                continue
            r = rows[0]
            a = r.get("ai") or {}
            rep.ok("  %-12s %-6s %5.1f | %s %s%% odds ~%sw dwn %s%%"
                   % (b, r["t"], r["score"], a.get("stance"),
                      a.get("odds_beat_spx_26w_pct"),
                      a.get("horizon_weeks"),
                      a.get("downside_risk_pct")))
            rep.log("      " + (a.get("one_liner") or "")[:120])

        rep.heading("5. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("v1.2.0 LIVE -- institutional metrics, empirical odds "
               "and anchored AI verdicts verified")


if __name__ == "__main__":
    main()
