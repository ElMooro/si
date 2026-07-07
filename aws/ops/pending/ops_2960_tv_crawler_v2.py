#!/usr/bin/env python3
"""ops 2960 — Redeploy TV crawler v2 (sessionid + sessionid_sign required).

TV 403s the user API when sessionid_sign is absent. Patches the crawler to
read sign from SSM, builds the full cookie string, then fires immediately.
Also prints the exact two GitBash commands needed to add both cookies.
"""
import json, sys, time
from pathlib import Path
import boto3
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1")
SSM = boto3.client("ssm", region_name="us-east-1")
S3  = boto3.client("s3",  region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
ROOT = Path(__file__).resolve().parents[2]
FN   = "justhodl-tv-notes-crawler"

def ssm_get(n):
    try:
        return SSM.get_parameter(Name=n, WithDecryption=True)["Parameter"]["Value"]
    except SSM.exceptions.ParameterNotFound:
        return None

def main():
    with report("2960_tv_crawler_v2") as rep:
        fails = []

        token = ssm_get("/justhodl/tvnotes/ingest-token")
        try:
            ingest_url = LAM.get_function_url_config(
                FunctionName="justhodl-tv-notes-ingest")["FunctionUrl"].rstrip("/")
        except Exception:
            ingest_url = ""

        env = {"INGEST_TOKEN": token or "", "TV_INGEST_URL": ingest_url}
        session = ssm_get("/justhodl/tradingview/sessionid")
        sign    = ssm_get("/justhodl/tradingview/sessionid_sign")
        rep.kv(session_in_ssm=bool(session), sign_in_ssm=bool(sign),
               session_len=len(session or ""), sign_len=len(sign or ""))

        if not sign:
            rep.warn(
                "sessionid_sign NOT in SSM — TV 403s without it.\n"
                "Run these two GitBash commands (already have sessionid):\n\n"
                "1. In Chrome on tradingview.com -> Ctrl+Shift+I -> Application\n"
                "   -> Cookies -> tradingview.com -> find 'sessionid_sign' row\n"
                "   -> double-click the Value column, copy the whole string\n\n"
                "2. GitBash command:\n"
                "   MSYS_NO_PATHCONV=1 aws ssm put-parameter "
                "--name /justhodl/tradingview/sessionid_sign "
                "--type SecureString "
                "--value \"PASTE_SESSIONID_SIGN_HERE\" "
                "--overwrite --region us-east-1\n\n"
                "The crawler will then work fully autonomously.")

        rep.section("Deploy crawler v2")
        try:
            cur = LAM.get_function_configuration(FunctionName=FN)
            cur_env = cur.get("Environment", {}).get("Variables", {}) or {}
            cur_env.update(env)
        except Exception:
            cur_env = env
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / FN / "source",
                      env_vars=cur_env, timeout=540, memory=512,
                      description="TV crawler v2: multi-cookie (ops 2960)",
                      eb_rule_name="justhodl-tv-notes-crawler-daily",
                      eb_schedule="cron(0 6 * * ? *)",
                      create_function_url=False, smoke=False)

        if session:
            rep.section("Fire immediate harvest")
            t0 = time.time()
            try:
                resp = LAM.invoke(FunctionName=FN,
                                  InvocationType="RequestResponse",
                                  Payload=json.dumps({}).encode())
                body = json.loads(json.loads(resp["Payload"].read()).get("body") or "{}")
                rep.kv(session_valid=body.get("session_valid"),
                       username=body.get("username"),
                       notes_harvested=body.get("notes_harvested", 0),
                       symbols_covered=body.get("symbols_covered", 0),
                       brain_upserted=body.get("brain_upserted", 0),
                       elapsed=round(time.time()-t0, 1))
                if body.get("session_valid"):
                    rep.ok("Session valid! %d notes from %d tickers harvested." % (
                        body.get("notes_harvested", 0),
                        body.get("symbols_covered", 0)))
                    if body.get("notes_harvested", 0) > 0:
                        rep.section("Fire brain-compiler")
                        bc = LAM.invoke(FunctionName="justhodl-brain-compiler",
                                        InvocationType="RequestResponse",
                                        Payload=json.dumps({}).encode())
                        bcd = json.loads(bc["Payload"].read())
                        rep.kv(brain_compiler_ok=bcd.get("statusCode") == 200)
                else:
                    fails.append("session_valid=False even with sign — check cookie values")
            except Exception as e:
                fails.append("invoke: %s" % e)

        line = "tv-crawler-v2: session=%s sign=%s" % (bool(session), bool(sign))
        print(line); rep.kv(summary=line)
        if fails:
            for f in fails: rep.fail(f)
            sys.exit(1)
        rep.ok("TV crawler v2 deployed" + (" + harvest fired" if session else " — add sessionid_sign to SSM"))

if __name__ == "__main__":
    main()
