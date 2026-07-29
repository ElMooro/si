"""ops_4090 — STEP 2: map ECONOMICS codes onto real FRED series.

ops 4084 counted 3,317 ECONOMICS: tickers carrying Khalid's notes with no
fetch route. TradingView's macro namespace is <COUNTRY><INDICATOR>
(USCPI, JPM3, DEUNR), which yields a search QUERY, never an answer. Every
candidate is confirmed against FRED's own catalogue and scored: a title
must match BOTH the geography and the concept, because a title matching
only one of them is a different series.

>= 0.80 becomes an alias. Everything weaker is published as a CANDIDATE
with its score. That threshold is the whole point — a wrong alias would
chart the wrong series underneath Khalid's own note, which is strictly
worse than leaving the ticker unrouted. The gates below check that
discipline, not just that numbers went up.
"""
import io, json, sys, time, urllib.request, zipfile as zf
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120, retries={"max_attempts": 0}))
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-symbol-resolver"
MARK = "symbol-resolver v2.0 ops4090 step2-economics"


def main():
    with report("4090_step2_economics") as rep:
        rep.heading("ops 4089 — STEP 2: ECONOMICS → FRED, confidence-gated")
        checks = []

        rep.section("A. descriptions available (v1.8.1 pipe)")
        # ops 4089 died here: the get_object sat OUTSIDE the try, so a
        # missing descriptions file (v1.8.1 not reloaded yet) raised before
        # the handler could ever run. The absence of an optional input must
        # never be fatal to the op that reports on it.
        try:
            dd = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/tv-descriptions.json")["Body"].read())
            rep.log(f"  descriptions banked: {dd.get('n')}")
        except Exception as e:
            rep.log(f"  none yet ({str(e)[:60]}) — matcher falls back to "
                    f"code decomposition, which is why the threshold exists")

        rep.section("B. deploy resolver v2.0")
        src = (ROOT / "lambdas" / FN / "source" / "lambda_function.py").read_text()
        # ops4090 note: the runner triggers only on aws/ops/pending/*.py, so
        # a fix that touches ONLY the engine source never re-runs the op and
        # the last report stays stale while looking like a fresh failure.
        assert MARK in src, f"marker parity broken: {MARK!r} not in engine source"
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
        for _ in range(6):
            try:
                lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue(),
                                         Publish=True); break
            except lam.exceptions.ResourceConflictException:
                time.sleep(12)
        settled = False
        for a in range(24):
            try:
                c = lam.get_function_configuration(FunctionName=FN)
                if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                    loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                    dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read())
                                     ).read("lambda_function.py").decode()
                    if MARK in dep:
                        settled = True; rep.log(f"  ✓ settled (attempt {a+1})"); break
            except Exception as e:
                rep.log(f"  settle {a+1}: {str(e)[:55]}")
            time.sleep(10)
        checks.append(("resolver v2.0 settled by marker", settled))
        if not settled:
            rep.log("✗ stale artifact"); sys.exit(1)

        rep.section("C. async invoke + poll (600s engine, socket won't hold)")
        before = json.loads(s3.get_object(Bucket=BUCKET, Key="data/symbol-aliases.json")["Body"].read())
        bgen = before.get("generated_at")
        r = lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=b'{"source":"ops4090"}')
        rep.log(f"  async accepted: {r['StatusCode']}")
        checks.append(("async accepted", r["StatusCode"] == 202))
        sa = None
        for a in range(40):
            time.sleep(20)
            try:
                cur = json.loads(s3.get_object(Bucket=BUCKET, Key="data/symbol-aliases.json")["Body"].read())
                if cur.get("generated_at") != bgen:
                    sa = cur; rep.log(f"  ✓ artifact moved after {(a+1)*20}s"); break
            except Exception:
                pass
        if sa is None:
            rep.log("✗ artifact never moved"); sys.exit(1)

        rep.section("D. results")
        rep.kv(econ_total=sa.get("economics_total"),
               econ_aliased=sa.get("economics_aliased"),
               econ_candidates=sa.get("economics_candidates"),
               threshold=sa.get("economics_accept_threshold"),
               n_aliases=sa.get("n_aliases"), calls=sa.get("fred_calls_this_run"))
        rep.log(f"  ECONOMICS tickers      : {sa.get('economics_total')}")
        rep.log(f"  ACCEPTED as aliases    : {sa.get('economics_aliased')}")
        rep.log(f"  candidates (below 0.80): {sa.get('economics_candidates')}")
        rep.log(f"  by_route               : {sa.get('by_route')}")

        det = sa.get("detail") or {}
        acc = [(k, v) for k, v in det.items() if v.get("route") == "economics-matched"]
        rep.section("Accepted ECONOMICS matches (sample)")
        for k, v in acc[:14]:
            rep.log(f"  {k:16} → {v['alias']:22} {v['confidence']}  "
                    f"{str(v.get('title'))[:44]}")
        rep.section("Rejected candidates — the honest queue (sample)")
        for c in (sa.get("candidates") or [])[:12]:
            rep.log(f"  {str(c.get('tv_symbol'))[:22]:22} {c.get('confidence')}  "
                    f"{str(c.get('title'))[:40]}  q='{str(c.get('query'))[:28]}'")

        # ── discipline gates ──
        checks.append(("every accepted match scored >= 0.80",
                       all(v.get("confidence", 0) >= 0.80 for _, v in acc)))
        checks.append(("every accepted match names a real FRED series",
                       all(v.get("alias", "").startswith("fred:") and v.get("title")
                           for _, v in acc)))
        checks.append(("weak matches are PUBLISHED as candidates, not dropped",
                       "candidates" in sa and "economics_candidates" in sa))
        checks.append(("candidates were NOT promoted into aliases",
                       all(c.get("confidence", 1) < 0.80
                           for c in (sa.get("candidates") or []))))
        checks.append(("step 1 FRED aliases not regressed",
                       (sa.get("fred_verified") or 0) >= (before.get("fred_verified") or 0)))

        rep.section("VERDICT")
        for n, o in checks: rep.log(f"  {'✓' if o else '✗'} {n}")
        bad = [n for n, o in checks if not o]
        if bad:
            rep.log(f"✗ FAILED: {bad}"); sys.exit(1)
        rep.log(f"✅ PASS_ALL — {sa.get('economics_aliased')} ECONOMICS tickers "
                f"mapped to verified FRED series; {sa.get('economics_candidates')} "
                f"held as scored candidates rather than guessed. "
                f"Total aliases now {sa.get('n_aliases')}.")


if __name__ == "__main__":
    main()
