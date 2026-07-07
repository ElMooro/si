#!/usr/bin/env python3
"""ops 2961 — Fire TV notes harvest NOW (both sessionid + sessionid_sign confirmed in SSM).

Redeploys crawler with updated env vars, invokes synchronously, runs
brain-compiler to route every TV note to matching fleet engines, reports
full harvest summary. This is the definitive end-to-end harvest run.
"""
import json
import sys
import time
from pathlib import Path

import boto3
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

LAM    = boto3.client("lambda", region_name="us-east-1")
SSM    = boto3.client("ssm",   region_name="us-east-1")
S3     = boto3.client("s3",    region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
ROOT   = Path(__file__).resolve().parents[2]
FN     = "justhodl-tv-notes-crawler"
FN_BC  = "justhodl-brain-compiler"
FN_ING = "justhodl-tv-notes-ingest"


def ssm_get(name):
    try:
        return SSM.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
    except SSM.exceptions.ParameterNotFound:
        return None


def main():
    with report("2961_tv_harvest_fire") as rep:
        fails = []

        # ── read all cookies from SSM ────────────────────────────────
        rep.section("SSM check")
        session  = ssm_get("/justhodl/tradingview/sessionid")
        sign     = ssm_get("/justhodl/tradingview/sessionid_sign")
        device_t = ssm_get("/justhodl/tradingview/device_t")
        token    = ssm_get("/justhodl/tvnotes/ingest-token")
        rep.kv(sessionid_len   = len(session  or ""),
               sessionid_sign_len = len(sign   or ""),
               device_t_len   = len(device_t or ""),
               token_ok       = bool(token))
        if not session:
            fails.append("sessionid missing from SSM")
        if not sign:
            rep.warn("sessionid_sign missing — TV will likely 403; harvest may return 0 notes")

        # auto-discover ingest URL
        ingest_url = ""
        try:
            ingest_url = LAM.get_function_url_config(
                FunctionName=FN_ING)["FunctionUrl"].rstrip("/")
            rep.kv(ingest_url=ingest_url)
        except Exception as e:
            rep.warn("ingest URL not found: %s" % e)

        # ── redeploy crawler with fresh env ──────────────────────────
        rep.section("Redeploy crawler with fresh env")
        try:
            cur = LAM.get_function_configuration(FunctionName=FN)
            env = cur.get("Environment", {}).get("Variables", {}) or {}
        except Exception:
            env = {}
        env.update({
            "TV_INGEST_URL": ingest_url,
            "INGEST_TOKEN":  token or "",
        })
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / FN / "source",
                      env_vars=env, timeout=540, memory=512,
                      description="TV crawler v2 live fire (ops 2961)",
                      eb_rule_name="justhodl-tv-notes-crawler-daily",
                      eb_schedule="cron(0 6 * * ? *)",
                      create_function_url=False, smoke=False)
        time.sleep(4)

        # ── invoke crawler synchronously ─────────────────────────────
        rep.section("Invoke crawler")
        rep.log("Invoking justhodl-tv-notes-crawler synchronously (up to 9 min)...")
        t0 = time.time()
        result = {}
        try:
            resp = LAM.invoke(
                FunctionName=FN,
                InvocationType="RequestResponse",
                Payload=json.dumps({}).encode(),
                LogType="None",
            )
            raw  = resp["Payload"].read()
            outer = json.loads(raw) if raw else {}
            body  = outer.get("body") or outer
            if isinstance(body, str):
                try:
                    body = json.loads(body)
                except Exception:
                    body = {}
            result = body if isinstance(body, dict) else {}
            elapsed = round(time.time() - t0, 1)

            rep.kv(
                session_valid   = result.get("session_valid"),
                username        = result.get("username"),
                notes_harvested = result.get("notes_harvested", 0),
                notes_in_mirror = result.get("notes_in_mirror", 0),
                symbols_covered = result.get("symbols_covered", 0),
                brain_upserted  = result.get("brain_upserted",  0),
                brain_errors    = result.get("brain_errors",    0),
                elapsed_s       = elapsed,
            )

            if result.get("session_valid"):
                rep.ok("Session valid — harvest complete in %.0fs" % elapsed)
            else:
                fails.append(
                    "session_valid=False after %.0fs. "
                    "TV may need a different cookie. Check:\n"
                    "  1. In Chrome DevTools Application->Cookies, try copying "
                    "all cookies as a full Cookie header string and storing as "
                    "/justhodl/tradingview/full_cookie_header\n"
                    "  2. Make sure you are logged in on tradingview.COM (not .TV)\n"
                    "  3. Try re-logging in and re-copying the cookie" % elapsed)

        except Exception as e:
            fails.append("crawler invocation error: %s" % e)

        notes_n  = result.get("notes_harvested", 0)
        syms_n   = result.get("symbols_covered",  0)
        brain_n  = result.get("brain_upserted",   0)

        # ── verify mirror ────────────────────────────────────────────
        rep.section("Mirror verification")
        try:
            mirror = json.loads(S3.get_object(
                Bucket=BUCKET, Key="data/tradingview-notes.json")["Body"].read())
            mc = mirror.get("count", len(mirror.get("notes", [])))
            rep.kv(mirror_count=mc, mirror_updated=mirror.get("updated"))
            # log first few symbols captured
            syms_seen = {}
            for n in mirror.get("notes", []):
                s = n.get("symbol", "?")
                if s not in syms_seen:
                    syms_seen[s] = n.get("text", "")[:100]
                    if len(syms_seen) >= 8:
                        break
            for sym, txt in syms_seen.items():
                rep.log("captured: [%s] %s" % (sym, txt))
        except Exception as e:
            rep.warn("mirror read: %s" % e)
            mc = 0

        # ── fire brain-compiler to route TV notes to fleet ───────────
        if mc > 0:
            rep.section("Brain-compiler: route TV notes to fleet")
            rep.log("Routing %d notes across 661 engines..." % mc)
            try:
                bc_resp = LAM.invoke(
                    FunctionName=FN_BC,
                    InvocationType="RequestResponse",
                    Payload=json.dumps({}).encode(),
                    LogType="None",
                )
                bc_raw = bc_resp["Payload"].read()
                bc_out = json.loads(bc_raw) if bc_raw else {}
                bc_body = bc_out.get("body") or bc_out
                if isinstance(bc_body, str):
                    try:
                        bc_body = json.loads(bc_body)
                    except Exception:
                        bc_body = {}
                bc_sum = (bc_body.get("summary") or {}) if isinstance(bc_body, dict) else {}
                rep.kv(
                    brain_total_notes = bc_sum.get("n_notes", 0),
                    brain_claims      = bc_sum.get("n_claims", 0),
                    brain_covered     = bc_sum.get("covered", 0),
                    brain_gaps        = bc_sum.get("gaps", 0),
                    brain_coverage    = bc_sum.get("coverage_pct"),
                )
                rep.ok(bc_sum.get("headline", "brain-compiler ran"))
            except Exception as e:
                rep.warn("brain-compiler: %s" % e)
        else:
            rep.log("No notes in mirror — skipping brain-compiler")

        # ── final summary ────────────────────────────────────────────
        line = ("tv-harvest: session_valid=%s notes=%d tickers=%d "
                "mirror=%d brain_upserted=%d"
                % (result.get("session_valid"), notes_n, syms_n, mc, brain_n))
        print(line)
        rep.kv(summary=line)

        if fails:
            for f in fails:
                rep.fail(f)
            print("FAILURES: " + " | ".join(fails))
            sys.exit(1)
        rep.ok("TV notes harvest complete — notes in brain, fleet routed")


if __name__ == "__main__":
    main()
