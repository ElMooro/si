"""
ops_3850 — global-recession wired as CONTEXT into rotation-dashboard

The decisive gate here is NEGATIVE: prove it changed nothing about scoring.
A "context, never rank" claim is only worth the verification behind it, so this
snapshots every confluence score and the overweight list BEFORE the change and
requires them to be byte-identical AFTER. If any score moved, the wire is not
context and the ops fails.
"""
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402
from ops_report import report  # noqa: E402

FN = "justhodl-rotation-dashboard"
SRC = ROOT / "lambdas" / FN / "source"
BUCKET = "justhodl-dashboard-live"
KEY = "data/rotation-dashboard.json"
MARKER = "global_recession_context"
PAGE_MARKER = "v3-ops3850"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
CFG = boto3.session.Config(read_timeout=890, retries={"max_attempts": 0})


def main():
    with report("3850_gr_context") as rep:
        rep.heading("ops 3850 — global-recession as CONTEXT (prove it moves nothing)")

        rep.section("1. Snapshot scoring BEFORE")
        b = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
        before_scores = {a["ticker"]: a["confluence_score"] for a in b.get("assets", [])}
        before_ow = [o["ticker"] for o in b.get("overweight", [])]
        before_elig = b["layer3_layer4"]["n_eligible"]
        rep.ok(f"  {len(before_scores)} scores · overweight={before_ow}")

        rep.section("2. Deploy + zip-settle")
        env = (lam.get_function_configuration(FunctionName=FN)
               .get("Environment", {}).get("Variables", {})) or {}
        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=900, memory=1024,
                      description="Cross-asset rotation dashboard: regime x ratios x trend gate x RS rank x flows x crowding",
                      create_function_url=False, smoke=False)
        import io as _io, zipfile as _z
        for i in range(60):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                try:
                    blob = urllib.request.urlopen(
                        lam.get_function(FunctionName=FN)["Code"]["Location"],
                        timeout=60).read()
                    with _z.ZipFile(_io.BytesIO(blob)) as zf:
                        if MARKER in zf.read("lambda_function.py").decode("utf-8", "ignore"):
                            rep.ok(f"  settled after {i*10}s"); break
                except Exception as e:
                    rep.log(f"    zip retry ({str(e)[:50]})")
            time.sleep(10)
        else:
            rep.fail("marker never landed"); sys.exit(1)

        rep.section("3. Invoke")
        r = boto3.client("lambda", region_name="us-east-1", config=CFG).invoke(
            FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        if r.get("FunctionError"):
            rep.fail(f"  invoke error: {r['Payload'].read()[:500]}"); sys.exit(1)
        rep.ok("  invoked clean")

        rep.section("4. THE NEGATIVE GATE — nothing about scoring may change")
        d = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
        after_scores = {a["ticker"]: a["confluence_score"] for a in d.get("assets", [])}
        after_ow = [o["ticker"] for o in d.get("overweight", [])]
        checks, fails = [], []

        def chk(n, cond, det=""):
            (checks if cond else fails).append(n)
            (rep.ok if cond else rep.fail)(f"  {n} {det}")

        moved = {t: (before_scores[t], after_scores.get(t))
                 for t in before_scores
                 if t in after_scores and before_scores[t] != after_scores[t]}
        chk("no confluence score moved", not moved,
            f"moved={list(moved.items())[:5]}")
        chk("overweight list identical", before_ow == after_ow,
            f"{before_ow} -> {after_ow}")
        chk("eligible count identical",
            d["layer3_layer4"]["n_eligible"] == before_elig,
            f"{before_elig} -> {d['layer3_layer4']['n_eligible']}")

        rep.section("5. The context block itself")
        gr = (d.get("layer1_regime") or {}).get("global_recession_context") or {}
        chk("context block present", bool(gr))
        chk("probability carried", gr.get("probability_pct") is not None,
            f"= {gr.get('probability_pct')}%")
        chk("applied_to_score is FALSE", gr.get("applied_to_score") is False)
        chk("why-not-applied disclosed",
            "double-count" in (gr.get("why_not_applied") or ""))
        chk("coverage verdict carried", bool(gr.get("coverage_verdict")),
            f"= {str(gr.get('coverage_verdict'))[:60]}")
        rep.log(f"    confirmed={gr.get('confirmed')} divergent={gr.get('divergent')} "
                f"unconfirmed_share={gr.get('unconfirmed_share_of_headline_pct')}% "
                f"us_probit={gr.get('us_curve_probit_pct')}%")

        rep.section("6. Page renders it")
        html = ""
        for a in range(1, 13):
            try:
                html = urllib.request.urlopen(urllib.request.Request(
                    f"https://justhodl.ai/rotation-dashboard.html?v={int(time.time())}-{a}",
                    headers={"User-Agent": UA, "Cache-Control": "no-cache"}),
                    timeout=30).read().decode("utf-8", "ignore")
            except Exception as e:
                rep.log(f"  attempt {a}: {str(e)[:60]}"); time.sleep(20); continue
            if PAGE_MARKER in html:
                rep.ok(f"  served on attempt {a} ({len(html):,} bytes)"); break
            time.sleep(20)
        else:
            rep.fail(f"  page marker '{PAGE_MARKER}' never reached the edge")
            sys.exit(1)
        for lbl, m in (("context-only badge", "CONTEXT ONLY"),
                       ("not-applied disclosure", "Not applied to any score"),
                       ("coverage line", "Coverage:")):
            (rep.ok if m in html else rep.fail)(f"  {lbl}")
            if m not in html:
                fails.append(lbl)

        rep.kv(scores_moved=len(moved), gr_prob=gr.get("probability_pct"),
               applied=gr.get("applied_to_score"),
               confirmed=gr.get("confirmed"), divergent=gr.get("divergent"))
        if fails:
            rep.fail(f"FAILED {len(fails)}: {fails}"); sys.exit(1)
        rep.ok(f"PASS_ALL {len(checks)}/{len(checks)} — context wired, zero score impact")


if __name__ == "__main__":
    main()
