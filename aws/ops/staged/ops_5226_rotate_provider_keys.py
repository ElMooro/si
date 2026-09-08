"""ops_5226 -- audit 2026-09-08 INST-06 rotation fan-out (run AFTER Khalid rotates keys in the provider dashboards
and pastes each NEW value into its SSM parameter in the AWS console -- never in chat).

For each provider parameter:
  1. read the SSM value and VALIDATE it against the provider with one cheap request (a bad paste aborts the
     fan-out for that provider before any Lambda is touched);
  2. find every Lambda whose environment carries the credential under any of the fleet's env names and whose
     value differs from SSM; update the env (merge, nothing else changes) so the live function uses the new key;
  3. report counts. Engines that read through managed_secret with no env var pick the SSM value up on their next
     cold start automatically.
Worker secrets (POLYGON_KEY / FRED_KEY / ADMIN_TOKEN) re-attach on the next deploy-workers run: Claude pushes a
worker no-op after this op is GREEN. Never prints secret values.
Trigger explicitly: keep this file in aws/ops/pending and push it (or workflow_dispatch it) when rotation is done.
"""
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
PROVIDERS = {
    "/justhodl/fmp/api-key":       {"env": ["FMP_KEY", "FMP_API_KEY"],                      "probe": "https://financialmodelingprep.com/stable/quote?symbol=SPY&apikey={k}"},
    "/justhodl/polygon/api-key":   {"env": ["POLYGON_API_KEY", "POLYGON_KEY", "POLY_KEY"],  "probe": "https://api.polygon.io/v2/aggs/ticker/SPY/prev?adjusted=true&apiKey={k}"},
    "/justhodl/fred/api-key":      {"env": ["FRED_API_KEY", "FRED_KEY"],                    "probe": "https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&limit=1&file_type=json&api_key={k}"},
    "/justhodl/cmc/api-key":       {"env": ["CMC_KEY", "COINMARKETCAP_API_KEY"],            "probe": "https://pro-api.coinmarketcap.com/v1/key/info", "header": "X-CMC_PRO_API_KEY"},
    "/justhodl/telegram/bot_token": {"env": ["TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN"],       "probe": "https://api.telegram.org/bot{k}/getMe"},
}
FAILS = []


def mask(v):
    v = str(v or "")
    return (v[:2] + "…" + v[-2:]) if len(v) > 8 else "…"


def probe(spec, key):
    url = spec["probe"].format(k=key)
    req = urllib.request.Request(url, headers={"User-Agent": "ops5226", **({spec["header"]: key} if spec.get("header") else {})})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, r.read()[:200]
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:200]
    except Exception as e:
        return 0, str(e).encode()[:200]


with report("ops_5226_rotate_provider_keys") as R:
    R.heading("ops 5226 -- provider credential rotation fan-out (validate SSM values, update every Lambda env)")
    ssm = boto3.client("ssm", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    fleet = []
    for page in lam.get_paginator("list_functions").paginate():
        fleet.extend(page.get("Functions", []))
    R.log("fleet: %d functions" % len(fleet))
    for name, spec in PROVIDERS.items():
        R.section(name)
        try:
            new = ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
        except Exception as e:
            R.warn("%s unreadable: %s -- skipped" % (name, str(e)[:80])); continue
        st, body = probe(spec, new)
        ok = st == 200 and b"error" not in body.lower()[:60] and b"invalid" not in body.lower()
        if not ok:
            FAILS.append("%s: provider probe HTTP %s %s -- fan-out ABORTED for this provider (fix the SSM value)" % (name, st, body[:80]))
            R.fail(FAILS[-1]); continue
        R.ok("SSM value %s validated against the provider (HTTP %s)" % (mask(new), st))
        updated, same, errors = 0, 0, 0
        for fc in fleet:
            env = dict(((fc.get("Environment") or {}).get("Variables") or {}))
            hit = [k for k in spec["env"] if k in env]
            if not hit:
                continue
            if all(env[k] == new for k in hit):
                same += 1; continue
            for k in hit:
                env[k] = new
            try:
                for attempt in range(6):
                    try:
                        lam.update_function_configuration(FunctionName=fc["FunctionName"], Environment={"Variables": env})
                        break
                    except lam.exceptions.ResourceConflictException:
                        time.sleep(5 * (attempt + 1))
                updated += 1
            except Exception as e:
                errors += 1
                R.warn("%s: env update failed: %s" % (fc["FunctionName"], str(e)[:80]))
        R.log("updated %d functions, %d already current, %d errors" % (updated, same, errors))
        R.kv(parameter=name, updated=updated, already_current=same, errors=errors)
        if errors:
            FAILS.append("%s: %d functions failed to update" % (name, errors))
    R.section("verdict")
    for f in FAILS:
        R.fail(f)
    if FAILS:
        sys.exit(1)
    R.ok("GREEN -- every Lambda env carries the rotated credentials; push a worker no-op to re-attach POLYGON_KEY/FRED_KEY secrets")
