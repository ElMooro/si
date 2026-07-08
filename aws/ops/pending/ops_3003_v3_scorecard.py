#!/usr/bin/env python3
"""ops 3003 -- THEORY-STACK UPGRADE verify (Khalid email, 15-theory
institutional blueprint applied where genuinely missing):
(A) industry-rotation v2.0: Jegadeesh-Titman skip-month multi-horizon
    momentum, beta-adjusted alpha-RS (BAB separation), quantified
    dump-day/rebound tests, CROWDED flag (peak-percentile + rollover,
    both-tails guarded), holdings breadth BROAD/NARROW, PEAD+ soldier
    badges.
(B) NEW justhodl-factor-regime: 11 style-ETF ratio trend states +
    fresh-THRUST detector + risk-appetite composite. Complementary to
    factor-returns (cross-sectional L/S P&L), documented as such.
Sequence: deploy-gate IR (env persists) -> donor-env + ensure_eb_rule
for NEW factor-regime -> invoke both -> verify batteries -> pages live.
Crash guard writes report+trace on any uncaught exception.
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report
from _lambda_deploy_helpers import ensure_eb_rule

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=310, connect_timeout=10,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
IR, FR = "justhodl-industry-rotation", "justhodl-factor-regime"
DONOR = "justhodl-confluence-meta"
KEYS = ["POLYGON_API_KEY", "POLYGON_KEY", "FMP_API_KEY", "FMP_KEY"]


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3001",
                                               "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status, r.read().decode("utf-8", "replace")


def gate(fn, need_env, tries=50):
    cfg, env_n = {}, 0
    for _ in range(tries):
        try:
            cfg = LAM.get_function_configuration(FunctionName=fn)
        except Exception:
            time.sleep(8)
            continue
        lm = datetime.fromisoformat(
            cfg["LastModified"].replace("+0000", "+00:00"))
        age = (datetime.now(timezone.utc) - lm).total_seconds()
        env_n = len((cfg.get("Environment") or {})
                    .get("Variables") or {})
        if cfg.get("LastUpdateStatus") == "Successful" \
                and age < 1800 and (not need_env or env_n >= 2):
            return True, env_n
        time.sleep(8)
    return False, env_n


def invoke(fn):
    t0 = time.time()
    resp = LAM.invoke(FunctionName=fn, Payload=b"{}")
    body = json.loads(resp["Payload"].read() or b"{}")
    return (round(time.time() - t0, 1), resp.get("FunctionError"),
            body)


def main():
    fails, warns = [], []
    out = {"ops": 3003, "ts": datetime.now(timezone.utc).isoformat()}
    with report("3003_v3_scorecard") as rep:

        rep.section("1. Gates + factor-regime bootstrap")
        time.sleep(75)
        ok_ir, env_ir = gate(IR, need_env=True)
        rep.kv(ir_gate=ok_ir, ir_env=env_ir)
        if not ok_ir:
            fails.append("IR gate: env=%d" % env_ir)
        try:
            LAM.get_function_configuration(FunctionName=FR)
            rep.kv(fr_exists=True)
        except Exception:
            # deploy-lambdas create-branch no-op (known intermittent,
            # memory-documented). Remedy: create from the runner's own
            # checkout via zipfile -> create_function.
            import io
            import zipfile
            rep.kv(fr_exists=False, action="ops-side create_function")
            srcdir = AWS_DIR / "lambdas" / FR / "source"
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w",
                                 zipfile.ZIP_DEFLATED) as z:
                for f in srcdir.rglob("*.py"):
                    z.write(f, f.relative_to(srcdir))
            donor0 = (LAM.get_function_configuration(
                FunctionName=DONOR).get("Environment")
                or {}).get("Variables") or {}
            env0 = {k: v for k, v in donor0.items() if k in KEYS}
            env0["S3_BUCKET"] = BUCKET
            LAM.create_function(
                FunctionName=FR, Runtime="python3.12",
                Role="arn:aws:iam::857687956942:role/"
                     "lambda-execution-role",
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": buf.getvalue()},
                Timeout=180, MemorySize=512,
                Environment={"Variables": env0},
                Description="Style-ETF ratio trend/thrust regime "
                            "detector (factor momentum)")
        ok_fr, _ = gate(FR, need_env=False)
        if not ok_fr:
            fails.append("FR still not Active after create")
        if fails:
            _w(rep, out, fails, warns)
            return
        donor = (LAM.get_function_configuration(FunctionName=DONOR)
                 .get("Environment") or {}).get("Variables") or {}
        env = {k: v for k, v in donor.items() if k in KEYS}
        env["S3_BUCKET"] = BUCKET
        LAM.update_function_configuration(
            FunctionName=FR, Environment={"Variables": env},
            Timeout=180, MemorySize=512)
        ok_fr2, env_fr = gate(FR, need_env=True)
        rep.kv(fr_env=env_fr)
        out["fr_env"] = env_fr
        if not ok_fr2:
            fails.append("FR env fix failed: %d" % env_fr)
            _w(rep, out, fails, warns)
            return
        try:
            ensure_eb_rule(report=rep, rule_name="factor-regime-daily",
                           schedule="cron(28 21 * * ? *)",
                           function_name=FR)
        except Exception as e:
            fails.append("FR eb rule: %s" % str(e)[:120])

        rep.section("2. Invoke factor-regime")
        secs, err, body = invoke(FR)
        rep.kv(fr_secs=secs, fr_err=err, fr_body=json.dumps(body)[:200])
        if err:
            fails.append("FR invoke: %s" % json.dumps(body)[:250])
        else:
            fr = s3_json("data/factor-regime.json")
            prs = fr.get("pairs") or []
            okp = [p for p in prs if "error" not in p]
            out["fr_risk_appetite"] = fr.get("risk_appetite_score")
            out["fr_read"] = fr.get("risk_appetite_read")
            out["fr_pairs_ok"] = len(okp)
            out["fr_thrusts"] = fr.get("thrusts")
            out["fr_leading"] = fr.get("leading_styles")
            out["fr_sample"] = okp[:4]
            rep.kv(appetite=out["fr_risk_appetite"],
                   pairs_ok=len(okp),
                   thrusts=json.dumps(out["fr_thrusts"]),
                   leading=json.dumps(out["fr_leading"]))
            if len(okp) < 9:
                fails.append("FR pairs thin: %d/11" % len(okp))
            states = {p["state"] for p in okp}
            if not states & {"LEADING", "LAGGING", "FADING", "BASING"}:
                fails.append("FR states missing")
            for p in okp:
                if p.get("z_1y") is not None \
                        and abs(p["z_1y"]) > 6:
                    warns.append("FR z outlier %s=%s"
                                 % (p["pair"], p["z_1y"]))

        rep.section("3. Invoke industry-rotation v2")
        secs, err, body = invoke(IR)
        rep.kv(ir_secs=secs, ir_err=err, ir_body=json.dumps(body)[:200])
        if err:
            fails.append("IR invoke: %s" % json.dumps(body)[:250])
        else:
            d = s3_json("data/industry-rotation.json")
            rows = d.get("ladder") or []
            out["ir_version"] = d.get("version")
            out["ir_ladder_n"] = len(rows)
            out["ir_top5"] = [(r["etf"], r["leadership_score"],
                               (r.get("alpha_mom") or {}).get("blend"),
                               r.get("dump_day_excess_bps"),
                               r.get("crowded")) for r in rows[:5]]
            out["ir_crowded"] = [r["etf"] for r in rows
                                 if r.get("crowded")]
            lds = d.get("leaders") or []
            out["ir_breadth"] = [(l["etf"], l.get("breadth"))
                                 for l in lds]
            out["ir_pead_soldiers"] = sum(
                1 for l in lds for x in (l.get("resilient_names")
                                         or []) if x.get("pead"))
            rep.kv(version=d.get("version"), ladder=len(rows),
                   top5=json.dumps(out["ir_top5"])[:250],
                   crowded=json.dumps(out["ir_crowded"]),
                   breadth=json.dumps(out["ir_breadth"])[:300])
            if d.get("version") != "3.0":
                fails.append("IR version %s != 3.0" % d.get("version"))
            sc_rows = [r for r in rows
                       if r.get("scorecard_100") is not None]
            out["scorecard_n"] = len(sc_rows)
            out["scorecard_top6"] = [
                (r["etf"], r["scorecard_100"], r.get("scorecard_band"),
                 r.get("scorecard_basis")) for r in rows[:6]]
            full_n = sum(1 for r in rows if str(
                r.get("scorecard_basis", "")).startswith("full"))
            out["full_basis_n"] = full_n
            vols_n = sum(1 for r in rows if (r.get("volume") or {})
                         .get("updown_vol_ratio_20d") is not None)
            out["volume_fields_n"] = vols_n
            flows_n = sum(1 for r in rows if r.get("fund_flows"))
            out["flows_joined_n"] = flows_n
            bands = {}
            for r in sc_rows:
                bands[r["scorecard_band"]] = bands.get(
                    r["scorecard_band"], 0) + 1
            out["bands"] = bands
            rep.kv(scorecard_n=len(sc_rows), full_basis=full_n,
                   volume_n=vols_n, flows_n=flows_n,
                   bands=json.dumps(bands),
                   top6=json.dumps(out["scorecard_top6"])[:300])
            if len(sc_rows) < 36:
                fails.append("scorecard on only %d rows" % len(sc_rows))
            if vols_n < 34:
                fails.append("volume fields on only %d rows" % vols_n)
            if full_n < 3:
                fails.append("full-basis scorecard on only %d leaders"
                             % full_n)
            oob = [(r["etf"], r["scorecard_100"]) for r in sc_rows
                   if not (0 <= r["scorecard_100"] <= 100)]
            if oob:
                fails.append("scorecard out of bounds: %s"
                             % json.dumps(oob[:3]))
            cr = d.get("industry_credit") or {}
            out["industry_credit"] = {k: {kk: v.get(kk) for kk in
                ("read", "median_z", "distress_pct", "n_scored")}
                for k, v in list(cr.items())[:14]}
            rep.kv(credit=json.dumps(out["industry_credit"])[:400])
            scored = [v for v in cr.values()
                      if v.get("read") in ("OK", "WATCH", "DANGER")]
            if len(scored) < 4:
                fails.append("industry_credit scored only %d ETFs"
                             % len(scored))
            if "XLF" in cr and cr["XLF"].get("read") not in (
                    "N/A_FINANCIALS", "SECTOR_OAS_ONLY"):
                fails.append("XLF not excluded from Altman: %s"
                             % cr["XLF"].get("read"))
            out["confirmed_det"] = [r["etf"] for r in rows
                                    if r.get("tag")
                                    == "CONFIRMED_DETERIORATION"]
            out["resilience_reads"] = {}
            for r in rows:
                rr = r.get("resilience_read")
                if rr:
                    out["resilience_reads"][rr] = \
                        out["resilience_reads"].get(rr, 0) + 1
            rep.kv(reads=json.dumps(out["resilience_reads"]),
                   confirmed=json.dumps(out["confirmed_det"]))
            if len(rows) < 36:
                fails.append("universe not expanded: %d" % len(rows))
            if not out["resilience_reads"]:
                fails.append("resilience_read absent")
            sold_chain = sum(1 for l in lds for x in
                             (l.get("resilient_names") or [])
                             if "vs_etf_3m_pp" in x)
            out["soldier_chain_fields"] = sold_chain
            v2ok = sum(1 for r in rows
                       if (r.get("alpha_mom") or {}).get("blend")
                       is not None
                       and r.get("dump_day_excess_bps") is not None)
            out["ir_v2_fields_ok"] = v2ok
            if v2ok < 25:
                fails.append("v2 fields thin: %d rows" % v2ok)
            tlt_like = [r for r in rows if r["etf"] in
                        ("XLU", "XLP")]
            hib = [r for r in rows if r["etf"] in ("SMH", "SPHB")]
            b_lo = [r.get("beta_spy_1y") for r in tlt_like
                    if r.get("beta_spy_1y")]
            b_hi = [r.get("beta_spy_1y") for r in hib
                    if r.get("beta_spy_1y")]
            if b_lo and b_hi and min(b_hi) <= max(b_lo):
                warns.append("beta ordering odd: defensives %s vs "
                             "high-beta %s" % (b_lo, b_hi))
            br = [b for _, b in out["ir_breadth"] if b]
            if lds and not br:
                fails.append("breadth empty on all leaders")

        rep.section("4b. Best-setups credit-penalty path")
        try:
            r2 = LAM.invoke(FunctionName="justhodl-best-setups",
                            Payload=b"{}")
            b2 = json.loads(r2["Payload"].read() or b"{}")
            if r2.get("FunctionError"):
                fails.append("best-setups invoke: %s"
                             % json.dumps(b2)[:200])
            else:
                bs = s3_json("data/best-setups.json")
                stps = next((v for v in bs.values()
                             if isinstance(v, list) and v
                             and isinstance(v[0], dict)
                             and "industry_mult" in v[0]), [])
                out["bs_setups"] = len(stps)
                cd_notes = sum(1 for s_ in stps if "CREDIT-DANGER" in
                               (s_.get("why") or "")
                               or "CONFIRMED_DETERIORATION" in
                               (s_.get("why") or ""))
                out["bs_credit_mentions"] = cd_notes
                mn = min((s_["industry_mult"] for s_ in stps
                          if s_.get("industry_mult")), default=None)
                out["bs_min_mult"] = mn
                rep.kv(bs_setups=len(stps), credit_mentions=cd_notes,
                       min_mult=mn)
                if stps and mn is not None and mn < 0.85:
                    fails.append("bs mult below floor: %s" % mn)
        except Exception as e:
            fails.append("bs section: %s" % str(e)[:150])

        rep.section("4. Pages live")
        page_ok = wire_ok = False
        for _ in range(9):
            try:
                st1, h1 = get("https://justhodl.ai/factor-regime.html"
                              "?v=%d" % int(time.time()))
                st2, h2 = get("https://justhodl.ai/"
                              "industry-rotation.html?v=%d"
                              % int(time.time()))
                page_ok = (st1 == 200 and "Style Ratio Board" in h1
                           and "THRUST" in h1)
                wire_ok = (st2 == 200 and "Scorecard" in h2
                           and "DRY-UP" in h2)
                if page_ok and wire_ok:
                    break
            except Exception:
                pass
            time.sleep(10)
        rep.kv(factor_page=page_ok, ir_page_v2=wire_ok)
        if not page_ok:
            fails.append("factor-regime.html not live")
        if not wire_ok:
            fails.append("industry-rotation.html v2.1 cols missing")

        if not fails:
            rep.ok("THEORY STACK LIVE: FR appetite %s (%s), thrusts "
                   "%s | IR v2 top %s | crowded %s"
                   % (out.get("fr_risk_appetite"), out.get("fr_read"),
                      json.dumps(out.get("fr_thrusts")),
                      json.dumps(out.get("ir_top5", [])[:3])[:160],
                      json.dumps(out.get("ir_crowded"))))
        _w(rep, out, fails, warns)


def _w(rep, out, fails, warns):
    out["fails"], out["warns"] = fails, warns
    out["verdict"] = "PASS" if not fails else "FAIL"
    (AWS_DIR / "ops" / "reports" / "3003.json").write_text(
        json.dumps(out, indent=1))
    rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
    if fails:
        sys.exit(1)


try:
    main()
except SystemExit:
    raise
except Exception as e:
    import traceback
    (AWS_DIR / "ops" / "reports" / "3003.json").write_text(json.dumps(
        {"ops": 3003, "verdict": "FAIL",
         "fails": ["CRASH: %s" % str(e)[:200]],
         "trace": traceback.format_exc()[-1500:],
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    sys.exit(1)
sys.exit(0)
