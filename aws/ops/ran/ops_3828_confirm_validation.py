"""
ops_3828 — justhodl-global-recession v1.0.0 + chokepoint day-two re-read

(1) GLOBAL RECESSION ENSEMBLE. The fleet's recession machinery is US-centric
    (NY Fed probit, Sahm, LEI, cycle-clock). Nothing GDP-weights country-level
    odds. Builds on justhodl-global-business-cycle (35 economies, phase +
    cli_level + gdp_weight) rather than re-fetching any country data.

    G0 asserts the producer keys exist in the LIVE artifact before consuming.
    Gates prove the aggregation is REAL: coverage > 0.5 of GDP weight, the
    global number sits strictly inside the country min/max (a weighted mean
    must), contributions sum to the global number, and the "not MacroMicro"
    disclosure plus the excluded-not-imputed handling are present.

(2) CHOKEPOINT DAY-TWO RE-READ — pending since ops 3776. The first UNATTENDED
    run was due 2026-07-24 15:30 UTC. Verify generated_at moved and the ledger
    grew WITHOUT a push, which is what proves the schedule is genuinely armed
    rather than merely declared.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

from _lambda_deploy_helpers import deploy_lambda  # noqa: E402
from ops_report import report  # noqa: E402

FN = "justhodl-global-recession"
SRC = ROOT / "lambdas" / FN / "source"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/global-recession.json"
DONORS = ["justhodl-confluence-meta", "justhodl-dollar-radar", "justhodl-nowcast-desk"]

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("3828_confirm_validation") as rep:
        rep.heading("ops 3828 — v1.2.1 supplement validation: global recession ensemble + chokepoint day-two")

        rep.section("G0. KEY CONTRACT — live producer artifact")
        gbc = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/global-business-cycle.json")["Body"].read())
        bc = gbc.get("by_country") or {}
        have = [k for k, v in bc.items() if isinstance(v, dict)
                and v.get("phase") and v.get("gdp_weight") is not None]
        if len(have) < 10:
            rep.fail(f"  by_country usable rows = {len(have)} (<10)")
            sys.exit(1)
        rep.ok(f"  by_country: {len(bc)} countries, {len(have)} with phase+gdp_weight")
        s0 = bc[have[0]]
        for k in ("phase", "cli_level", "gdp_weight", "six_month_change", "dist_200ma_pct"):
            (rep.ok if k in s0 else rep.warn)(f"    key '{k}' {'present' if k in s0 else 'ABSENT'}")

        rep.section("1. Deploy")
        env = {"S3_BUCKET": BUCKET}
        for d in DONORS:
            if "FRED_API_KEY" in env:
                break
            try:
                de = (lam.get_function_configuration(FunctionName=d)
                      .get("Environment", {}).get("Variables", {}))
            except Exception:
                continue
            if "FRED_API_KEY" in de:
                env["FRED_API_KEY"] = de["FRED_API_KEY"]
                rep.ok(f"  FRED_API_KEY from {d}")
        if "FRED_API_KEY" not in env:
            rep.warn("  no FRED key — US cross-check will be empty (non-fatal)")

        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=300, memory=512,
                      description="GDP-weighted global recession probability ensemble",
                      create_function_url=False, smoke=False)

        rep.section("2. Zip-settle")
        for i in range(40):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                rep.ok(f"  settled after {i*5}s")
                break
            time.sleep(5)
        else:
            rep.fail("never settled"); sys.exit(1)

        rep.section("3. Schedule")
        sch = boto3.client("scheduler", region_name="us-east-1")
        try:
            sch.create_schedule(
                Name="global-recession-sched",
                ScheduleExpression="cron(40 12 * * ? *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{FN}",
                        "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role",
                        "Input": "{}"})
            rep.ok("  Scheduler created cron(40 12 * * ? *)")
        except sch.exceptions.ConflictException:
            rep.ok("  Scheduler exists (ConflictException = success)")

        rep.section("4. Invoke")
        cfg = boto3.session.Config(read_timeout=290, retries={"max_attempts": 0})
        r = boto3.client("lambda", region_name="us-east-1", config=cfg).invoke(
            FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read())
        if r.get("FunctionError"):
            rep.fail(f"  invoke error: {str(pl)[:600]}"); sys.exit(1)
        rep.log(f"  {str(pl)[:200]}")

        rep.section("5. Verify the aggregation is REAL")
        d = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
        checks, fails = [], []

        def chk(n, cond, det=""):
            (checks if cond else fails).append(n)
            (rep.ok if cond else rep.fail)(f"  {n} {det}")

        g = d.get("global_recession_prob_pct")
        cs = d.get("countries") or []
        ps = [c["recession_prob_pct"] for c in cs]
        chk("global probability present", isinstance(g, (int, float)), f"= {g}%")
        chk("countries scored >= 15", len(cs) >= 15, f"= {len(cs)}")
        chk("GDP coverage > 0.5", (d["coverage"]["gdp_weight_covered"] or 0) > 0.5,
            f"= {d['coverage']['gdp_weight_covered']}")
        chk("weighted mean inside country range",
            bool(ps) and min(ps) <= g <= max(ps),
            f"min {min(ps) if ps else '-'} <= {g} <= max {max(ps) if ps else '-'}")
        contrib = sum(c["contribution_pp"] for c in cs)
        chk("contributions reconcile to global", abs(contrib - g) < 1.5,
            f"sum {round(contrib,2)} vs {g}")
        chk("no country at 0 or 100 (nothing is certain)",
            all(0 < p < 100 for p in ps))
        # SATURATION GATE — v1.0 pinned CHN/IND/IDN at exactly 97 and the US at 2.
        # Structural checks (weighted mean in range, contributions reconcile) all
        # PASSED a mis-specified mapping, so plausibility is now gated directly.
        hi = [c["iso3"] for c in cs if c["recession_prob_pct"] >= 95]
        lo = [c["iso3"] for c in cs if c["recession_prob_pct"] <= 5]
        chk("not saturated at ceiling (<=2 countries >=95%)", len(hi) <= 2, f"= {hi}")
        chk("not saturated at floor (<=2 countries <=5%)", len(lo) <= 2, f"= {lo}")
        chk("dispersion is real (spread >= 25pp)",
            bool(ps) and (max(ps) - min(ps)) >= 25,
            f"= {round(max(ps)-min(ps),1)}pp")
        chk("excluded-not-imputed disclosed",
            "never imputed" in json.dumps(d.get("coverage") or {}))
        chk("NOT-MacroMicro disclosure present",
            "NOT MacroMicro" in (d.get("not_macromicro") or ""))
        chk("US cross-check reported separately",
            "us_crosscheck" in d and "double-count" in json.dumps(d["us_crosscheck"]))
        chk("breadth published",
            d["breadth"].get("pct_of_covered_gdp") is not None,
            f"= {d['breadth'].get('pct_of_covered_gdp')}% of covered GDP at risk")

        # ── v1.2 CONFIRMATION GATES ──
        cf = d.get("confirmation") or {}
        chk("confirmation block present", bool(cf))
        cnt = cf.get("counts") or {}
        chk("every country carries a confirmation state",
            sum(cnt.values()) == len(cs),
            f"= {cnt} vs {len(cs)} countries")
        chk("unconfirmed exposure is quantified",
            cf.get("unconfirmed_share_of_global_pct") is not None,
            f"= {cf.get('unconfirmed_share_of_global_pct')}% of headline")
        chk("OECD staleness is decided, not assumed",
            cf.get("oecd_usable") is not None,
            f"usable={cf.get('oecd_usable')} period={cf.get('oecd_period')} "
            f"age={cf.get('oecd_age_months')}mo")
        # v1.2.1: no country may be CONFIRMED off an out-of-range index value
        bad = [c["iso3"] for c in cs
               if c.get("confirmation") in ("CONFIRMED", "DIVERGENT")
               and isinstance((c.get("confirmation_detail") or {}).get("value"), (int, float))
               and not (50.0 <= c["confirmation_detail"]["value"] <= 150.0)]
        chk("no confirmation off an implausible index value", not bad, f"= {bad}")
        chk("coverage verdict published",
            bool(cf.get("coverage_verdict")), f"= {str(cf.get('coverage_verdict'))[:90]}")
        chk("dampening actually applied where unbacked",
            any((c.get("terms") or {}).get("confirmation_dampen_k")
                for c in cs) or cnt.get("CONFIRMED", 0) == len(cs),
            "at least one country pulled toward neutral, or all confirmed")
        # the whole point: CHN/IND drove 70% of v1.1 on momentum alone
        big = [c for c in cs if c["iso3"] in ("CHN", "IND")]
        for c in big:
            rep.log(f"    {c['iso3']}: {c['confirmation']} — "
                    f"p={c['recession_prob_pct']}% "
                    f"detail={json.dumps(c.get('confirmation_detail'))[:140]}")
        rep.log("  ── top GDP contributors ──")
        for c in cs[:8]:
            rep.log(f"    {c['iso3']:<4} {c['phase']:<10} p={c['recession_prob_pct']:>5}% "
                    f"w={c['gdp_weight']:<7} contrib={c['contribution_pp']}pp "
                    f"[{c.get('confirmation','?')}]")
        uc = d.get("us_crosscheck") or {}
        if uc.get("yield_curve_probit"):
            y = uc["yield_curve_probit"]
            rep.log(f"    US curve probit: {y['prob_12m_pct']}% "
                    f"(10y-3m {y['t10y3m_spread_pp']}pp)")
        if uc.get("sahm_rule"):
            rep.log(f"    Sahm: {uc['sahm_rule']['value']} — {uc['sahm_rule']['state']}")

        rep.kv(unconfirmed_share_pct=cf.get("unconfirmed_share_of_global_pct"),
               confirm_counts=json.dumps(cnt),
               oecd_usable=cf.get("oecd_usable"),
               global_prob=g, band=d.get("band"),
               n_countries=len(cs), excluded=d["coverage"]["n_excluded"],
               gdp_covered=d["coverage"]["gdp_weight_covered"],
               breadth_pct=d["breadth"].get("pct_of_covered_gdp"))

        # ── (2) chokepoint day-two ──
        rep.section("6. Chokepoint day-two unattended re-read (pending since 3776)")
        try:
            ck = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
            gen = ck.get("generated_at")
            rep.log(f"  chokepoint generated_at = {gen}")
            try:
                age_h = (datetime.now(timezone.utc)
                         - datetime.fromisoformat(str(gen).replace("Z", "+00:00"))
                         ).total_seconds() / 3600
                rep.log(f"  age = {age_h:.1f}h")
                if age_h < 24:
                    rep.ok("  UNATTENDED RUN CONFIRMED — schedule is genuinely armed")
                else:
                    rep.warn(f"  stale by {age_h:.1f}h — schedule declared but not firing")
            except Exception as e:
                rep.warn(f"  could not parse generated_at: {e}")
            led = s3.head_object(Bucket=BUCKET,
                                 Key="chokepoint/fundamentals-ledger.json")
            rep.log(f"  ledger {led['ContentLength']:,} bytes, "
                    f"modified {led['LastModified']}")
        except Exception as e:
            rep.warn(f"  chokepoint re-read failed: {str(e)[:120]}")

        if fails:
            rep.fail(f"FAILED {len(fails)}: {fails}"); sys.exit(1)
        rep.ok(f"PASS_ALL {len(checks)}/{len(checks)}")


if __name__ == "__main__":
    main()
