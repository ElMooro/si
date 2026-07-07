#!/usr/bin/env python3
"""ops 2958 — Deploy justhodl-tv-notes-crawler (autonomous, no browser needed).

Replaces the browser-extractor approach entirely. The crawler Lambda:
  - Reads TV session cookie from SSM (/justhodl/tradingview/sessionid)
  - Hits TV's internal REST API (same calls as the browser, but headless)
  - Enumerates all watchlists -> all symbols -> notes API per symbol
  - Pulls bulk notes endpoint, chart layouts, text annotations
  - Writes to data/tradingview-notes.json + brain upsert
  - Runs daily 06:00 UTC on EventBridge schedule

After deploy, Khalid adds the session cookie once via GitBash and the
system is fully autonomous forever. Sessions expire periodically; the
Lambda writes a clear instructions message to data/tv-crawler-status.json
when the session needs refresh.
"""
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

LAM  = boto3.client("lambda", region_name="us-east-1")
SSM  = boto3.client("ssm",  region_name="us-east-1")
S3   = boto3.client("s3",   region_name="us-east-1")
EVB  = boto3.client("events", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
ROOT = Path(__file__).resolve().parents[2]
FN   = "justhodl-tv-notes-crawler"
RULE = "justhodl-tv-notes-crawler-daily"


def ssm_get(name):
    try:
        return SSM.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
    except SSM.exceptions.ParameterNotFound:
        return None


def main():
    with report("2958_tv_crawler") as rep:
        fails = []

        # ── 1. Read existing ingest config ───────────────────────────
        rep.section("1. Read ingest config")
        token = ssm_get("/justhodl/tvnotes/ingest-token")
        ingest_url = None
        try:
            ingest_url = LAM.get_function_url_config(
                FunctionName="justhodl-tv-notes-ingest")["FunctionUrl"].rstrip("/")
            rep.kv(ingest_url=ingest_url, token_ok=bool(token))
        except Exception as e:
            rep.warn("ingest URL not found: %s" % e)

        # ── 2. Deploy crawler lambda ─────────────────────────────────
        rep.section("2. Deploy justhodl-tv-notes-crawler")
        env_vars = {
            "INGEST_TOKEN": token or "",
            "TV_INGEST_URL": ingest_url or "",
        }
        deploy_lambda(
            report=rep,
            function_name=FN,
            source_dir=ROOT / "lambdas" / FN / "source",
            env_vars=env_vars,
            timeout=540,    # 9 min — many API calls
            memory=512,
            description="Autonomous TV notes crawler (ops 2958)",
            eb_rule_name=RULE,
            eb_schedule="cron(0 6 * * ? *)",   # daily 06:00 UTC
            create_function_url=False,
            smoke=False,     # no smoke — session cookie not in SSM yet
        )
        rep.ok("crawler deployed with daily schedule")

        # ── 3. Publish how-to instructions to S3 ────────────────────
        rep.section("3. Publish setup instructions")
        instructions = {
            "status": "AWAITING_SESSION_COOKIE",
            "updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "what_to_do": (
                "Add your TradingView session cookie to SSM with ONE "
                "command in GitBash, then the crawler runs automatically "
                "every day at 06:00 UTC forever."),
            "steps": [
                "1. Open Chrome on tradingview.com (already logged in)",
                "2. Press Ctrl+Shift+I  (NOT F12 — TradingView captures F12)",
                "3. Click the 'Application' tab at the top of DevTools",
                "4. In the left sidebar: Storage -> Cookies -> https://www.tradingview.com",
                "5. Find the row named exactly: sessionid",
                "6. Double-click the Value column to select it, copy the whole string",
                "7. In GitBash, run this ONE command (replace YOUR_VALUE):",
                ("   aws ssm put-parameter "
                 "--name /justhodl/tradingview/sessionid "
                 "--type SecureString "
                 "--value \"YOUR_SESSIONID_VALUE_HERE\" "
                 "--overwrite --region us-east-1"),
                "8. Done. The crawler fires at 06:00 UTC daily from now on.",
                "9. To run immediately: go to AWS Lambda -> justhodl-tv-notes-crawler -> Test",
            ],
            "what_happens_next": (
                "Crawler reads all your watchlists, hits TV's notes API "
                "for every ticker, scans chart layouts for text annotations, "
                "writes to data/tradingview-notes.json, upserts to your Brain. "
                "Brain-compiler routes the notes to matching engines on every run. "
                "Session cookies expire every few weeks — check "
                "data/tv-crawler-status.json to see if a refresh is needed."),
            "lambda_name": FN,
            "schedule": "daily 06:00 UTC (EventBridge cron(0 6 * * ? *))",
        }
        S3.put_object(Bucket=BUCKET, Key="data/tv-crawler-status.json",
                      Body=json.dumps(instructions, indent=2).encode(),
                      ContentType="application/json")
        rep.ok("instructions written to data/tv-crawler-status.json")

        # ── 4. Verify session cookie already in SSM ──────────────────
        rep.section("4. Session cookie check")
        session = ssm_get("/justhodl/tradingview/sessionid")
        rep.kv(session_in_ssm=bool(session),
               session_length=len(session) if session else 0)
        if session:
            rep.ok("Session already in SSM — crawler will run on next schedule")
            # trigger an immediate run
            rep.section("5. Trigger immediate run")
            try:
                resp = LAM.invoke(FunctionName=FN,
                                  InvocationType="RequestResponse",
                                  Payload=json.dumps({}).encode())
                body = json.loads(resp["Payload"].read())
                result = json.loads(body.get("body") or "{}")
                rep.kv(immediate_run_ok=result.get("ok"),
                       notes_harvested=result.get("notes_harvested", 0),
                       notes_in_mirror=result.get("notes_in_mirror", 0),
                       brain_upserted=result.get("brain_upserted", 0),
                       symbols_covered=result.get("symbols_covered", 0),
                       session_valid=result.get("session_valid"),
                       elapsed=result.get("elapsed_seconds"))
                if result.get("ok"):
                    rep.ok("Immediate harvest complete: %d notes, %d tickers" % (
                        result.get("notes_harvested", 0),
                        result.get("symbols_covered", 0)))
                else:
                    rep.warn("Immediate run: session may need refresh — "
                             "see data/tv-crawler-status.json")
            except Exception as e:
                rep.warn("Immediate run invocation: %s" % e)
        else:
            rep.log("No session yet. Follow instructions in data/tv-crawler-status.json")

        line = ("tv-crawler: deployed=%s schedule=daily-06UTC "
                "session_in_ssm=%s" % (True, bool(session)))
        print(line)
        rep.kv(summary=line)
        if fails:
            for f in fails:
                rep.fail(f)
            sys.exit(1)
        rep.ok("justhodl-tv-notes-crawler deployed — "
               "add session cookie to SSM then it runs autonomously forever")


if __name__ == "__main__":
    main()
