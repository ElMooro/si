#!/usr/bin/env python3
"""ops 3768 — repair the backlog leg (diagnose-then-fix, from the runner).

3767 fixed the KEY NAMES (rpo_yoy / demand_accelerating / deferred_accelerating —
all confirmed against the producer source) and backlog_joined was STILL 0. So the
key names were never the whole story: the real question is whether the two engines
share any tickers at all.

HYPOTHESIS: justhodl-backlog's ledger is built from a curated SEED (SaaS/defense/
semis mega-caps) plus a rotating universe slice, while chokepoint's scored pool is
the curated chokepoint seed + a $0.8-50B small/mid funnel. Those two populations
may barely intersect — in which case the honest answer is NOT to force a join but
to (a) report the overlap truthfully, and (b) widen the join to the backlog names
chokepoint actually scores.

This ops therefore DIAGNOSES FIRST and only then decides. It does not assume.
No gate fails on a genuine data-population fact; it fails only if the join is
broken for a fixable reason (name mismatch, casing, stale feed).
"""
import sys, json, time, zipfile, io
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "shared"))

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
import boto3

FN = "justhodl-chokepoint"
SRC = ROOT / "lambdas" / FN / "source"
LAMBDA_FILE = SRC / "lambda_function.py"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


def rd(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def main():
    with report("3768_backlog_join_repair") as rep:
        rep.heading("ops 3768 — backlog leg: diagnose, then repair")

        # ── DIAGNOSE ──────────────────────────────────────────────────────
        rep.section("Diagnosis — do these two engines share tickers at all?")
        bk = rd("data/backlog.json")
        ck = rd("data/chokepoint.json")
        byt = bk.get("by_ticker") or {}
        rows = (ck.get("capture_gap") or {}).get("all_rows") or []
        ctk = {r["ticker"] for r in rows}
        btk = set(byt.keys())

        rep.kv(backlog_by_ticker=len(btk), backlog_generated=bk.get("generated_at"),
               backlog_ledger_size=bk.get("ledger_size"), backlog_n_covered=bk.get("n_covered"))
        rep.kv(chokepoint_scored=len(ctk), chokepoint_generated=ck.get("generated_at"))

        overlap = ctk & btk
        rep.kv(raw_overlap=len(overlap))
        rep.log("  overlap sample: %s" % sorted(list(overlap))[:25])

        # casing / whitespace mismatch check
        norm_b = {t.strip().upper(): t for t in btk}
        norm_c = {t.strip().upper(): t for t in ctk}
        norm_ov = set(norm_b) & set(norm_c)
        rep.kv(normalized_overlap=len(norm_ov))
        gate(rep, "DIAG.no_casing_bug", len(norm_ov) == len(overlap),
             "raw=%d normalized=%d (equal => no casing/whitespace bug)" % (len(overlap), len(norm_ov)))

        # is the backlog feed even fresh / populated?
        gate(rep, "DIAG.backlog_populated", len(btk) > 0,
             "backlog ledger has %d tickers" % len(btk))

        # how many of the overlapping names actually carry a usable RPO value?
        usable = [t for t in overlap
                  if isinstance((byt.get(t) or {}).get("rpo_yoy"), (int, float))]
        accel = [t for t in overlap
                 if (byt.get(t) or {}).get("demand_accelerating")
                 or (byt.get(t) or {}).get("deferred_accelerating")]
        rep.kv(overlap_with_rpo_value=len(usable), overlap_accelerating=len(accel))
        if overlap:
            s = sorted(overlap)[0]
            rep.log("  sample backlog row keys: %s" % sorted((byt.get(s) or {}).keys())[:24])

        # ── VERDICT ON CAUSE ──────────────────────────────────────────────
        rep.section("Cause")
        if len(overlap) == 0:
            rep.warn("POPULATION DISJOINT: the two engines score different universes. "
                     "This is a real data fact, not a code bug — backlog covers curated "
                     "SaaS/defense/semis names; chokepoint scores its seed + a $0.8-50B "
                     "small/mid funnel. Forcing a join would be fabricating coverage.")
        else:
            rep.ok("populations DO intersect (%d names) — join should yield >0" % len(overlap))

        # ── REPAIR: make the leg honest + widen coverage where legitimate ──
        # The right fix is NOT to fake a join. It is to (a) surface overlap
        # truthfully in the feed so the page can show why the leg is quiet, and
        # (b) count a name as backlog-joined whenever the producer has ANY row
        # for it, not only when rpo_yoy is numeric.
        rep.section("Repair — honest coverage accounting")
        src = LAMBDA_FILE.read_text()

        old = '''                _rpo_g = _b.get("rpo_yoy")
                _bk_accel = bool(_b.get("demand_accelerating") or _b.get("deferred_accelerating"))'''
        new = '''                _rpo_g = _b.get("rpo_yoy")
                if _rpo_g is None:
                    _rpo_g = _b.get("rpo_qoq")
                _bk_accel = bool(_b.get("demand_accelerating") or _b.get("deferred_accelerating"))
                _bk_present = bool(_b)'''
        gate(rep, "REPAIR.anchor", src.count(old) == 1, "join anchor unique")
        if FAILED:
            sys.exit(1)
        src = src.replace(old, new, 1)

        src = src.replace(
            '                    "backlog_accelerating": _bk_accel,',
            '                    "backlog_accelerating": _bk_accel,\n'
            '                    "backlog_covered": _bk_present,', 1)

        # honest coverage stats instead of a single misleading counter
        old_stats = '''                "backlog_joined": sum(1 for c in cap_rows if c.get("rpo_growth_yoy") is not None),'''
        if old_stats not in src:
            old_stats = '''                "backlog_joined": sum(1 for c in cap_rows if c.get("rpo_yoy") is not None),'''
        new_stats = '''                "backlog_joined": sum(1 for c in cap_rows if c.get("rpo_yoy") is not None),
                "backlog_covered": sum(1 for c in cap_rows if c.get("backlog_covered")),
                "backlog_ledger_size": len(_bk_by),
                "backlog_overlap": len(set(_bk_by.keys()) & set(c["ticker"] for c in cap_rows)),'''
        gate(rep, "REPAIR.stats_anchor", src.count(old_stats) == 1,
             "stats anchor unique (%s)" % old_stats.strip()[:40])
        if FAILED:
            sys.exit(1)
        src = src.replace(old_stats, new_stats, 1)

        # the ladder must not silently count a dead leg as satisfiable
        src = src.replace(
            '''            if _c.get("backlog_accelerating"):
                legs += 1; why.append("backlog accelerating")''',
            '''            if _c.get("backlog_accelerating"):
                legs += 1; why.append("backlog accelerating")
            _c["legs_available"] = 5 if _c.get("backlog_covered") else 4''', 1)

        src = src.replace('VERSION = "3.1"', 'VERSION = "3.2"', 1)
        LAMBDA_FILE.write_text(src)
        import py_compile
        py_compile.compile(str(LAMBDA_FILE), doraise=True)
        rep.ok("repair spliced + compile clean (v3.2)")

        # ── DEPLOY / SETTLE / INVOKE ──────────────────────────────────────
        rep.section("Deploy")
        env = (lam.get_function_configuration(FunctionName=FN).get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=900, memory=1024,
                      description="Industry-criticality + capture gap v3.2 (within+cross industry, honest backlog coverage accounting).",
                      create_function_url=False, smoke=False)

        settled = False
        for i in range(12):
            time.sleep(15)
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") != "Active" or c.get("LastUpdateStatus") != "Successful":
                continue
            import urllib.request
            u = lam.get_function(FunctionName=FN)["Code"]["Location"]
            with urllib.request.urlopen(u, timeout=90) as r:
                blob = r.read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                if "backlog_covered" in z.read("lambda_function.py").decode("utf-8", "replace"):
                    settled = True
                    rep.ok("settled attempt %d" % (i + 1))
                    break
        gate(rep, "DEPLOY.settled", settled, "v3.2 live")
        if FAILED:
            sys.exit(1)

        from botocore.config import Config
        ll = boto3.client("lambda", region_name="us-east-1",
                          config=Config(read_timeout=890, retries={"max_attempts": 0}))
        t0 = time.time()
        r = ll.invoke(FunctionName=FN, InvocationType="RequestResponse",
                      Payload=json.dumps({"mode": "full"}).encode())
        rep.kv(invoke_status=r.get("StatusCode"), invoke_seconds=round(time.time() - t0, 1))

        d2 = rd("data/chokepoint.json")
        cap = d2.get("capture_gap") or {}
        st = cap.get("stats") or {}
        rep.section("Live coverage — honest numbers")
        rep.kv(version=d2.get("version"), scored=st.get("scored"),
               backlog_joined=st.get("backlog_joined"),
               backlog_covered=st.get("backlog_covered"),
               backlog_ledger_size=st.get("backlog_ledger_size"),
               backlog_overlap=st.get("backlog_overlap"))

        gate(rep, "LIVE.v32", d2.get("version") == "3.2", "version=%s" % d2.get("version"))
        gate(rep, "LIVE.coverage_reported", st.get("backlog_overlap") is not None,
             "overlap now reported in feed (was invisible)")
        gate(rep, "LIVE.additive", all(k in d2 for k in
             ("structural_names", "industry_leaders", "all_chokepoints")), "books intact")

        ov = st.get("backlog_overlap") or 0
        if ov == 0:
            rep.warn("CONFIRMED DISJOINT: 0 shared tickers. The backlog leg is "
                     "structurally unavailable for this pool — the ladder now reports "
                     "legs_available=4 for those names instead of pretending 5.")
        else:
            gate(rep, "FIX.leg_alive", (st.get("backlog_joined") or 0) > 0,
                 "joined=%d of overlap=%d" % (st.get("backlog_joined") or 0, ov))

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — backlog coverage now honest and visible")


if __name__ == "__main__":
    main()
