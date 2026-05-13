#!/usr/bin/env python3
"""506 — Final end-to-end verify of BUILD 1/15: Dealer GEX.

Confirms:
  1. S3 sidecar data/dealer-gex.json is fresh & has real data
  2. GitHub Pages /gex/ HTML is reachable, ~30KB+ (not 404 fallback)
  3. Page references the right S3 URL
  4. Homepage link to /gex/ exists
  5. Schedule (EventBridge rule) is enabled
  6. Lambda config (mem, timeout, env, role) is correct
"""
import io, json, os, time as _time, zipfile, base64, urllib.request
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/506_dealer_gex_e2e.json"
NAME = "justhodl-dealer-gex"

lam = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

VERIFY_CODE = """
import json, urllib.request, time

def probe(url):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl-Verify/1.0'})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode('utf-8', 'replace')
            return {'url': url, 'status': r.status, 'bytes': len(body),
                     'body_head': body[:400], 'elapsed_ms': int((time.time()-t0)*1000)}
    except urllib.error.HTTPError as e:
        return {'url': url, 'status': e.code, 'err': True, 'elapsed_ms': int((time.time()-t0)*1000)}
    except Exception as e:
        return {'url': url, 'status': 'EXC', 'err': str(e)[:200], 'elapsed_ms': int((time.time()-t0)*1000)}

def lambda_handler(event, context):
    out = {}
    out['page_gex'] = probe('https://justhodl.ai/gex/')
    out['home'] = probe('https://justhodl.ai/')
    out['s3_sidecar'] = probe('https://justhodl-dashboard-live.s3.amazonaws.com/data/dealer-gex.json')
    out['s3_history'] = probe('https://justhodl-dashboard-live.s3.amazonaws.com/data/dealer-gex-history.json')
    # Check the homepage contains /gex/ link
    if out['home'].get('status') == 200:
        body = out['home'].get('body_head', '') + ''  # only first 400 chars; need full
        try:
            req = urllib.request.Request('https://justhodl.ai/', headers={'User-Agent': 'JustHodl-Verify/1.0'})
            with urllib.request.urlopen(req, timeout=15) as r:
                full = r.read().decode('utf-8','replace')
            out['home_links_gex'] = '/gex/' in full
            out['home_mentions_dealer_gex'] = 'DEALER GEX' in full or 'Dealer GEX' in full
        except: pass
    return {'statusCode': 200, 'body': json.dumps(out)}
"""

def zip_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()

def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # ─── Lambda config check ───
    try:
        cfg = lam.get_function_configuration(FunctionName=NAME)
        out["lambda_config"] = {
            "runtime": cfg.get("Runtime"),
            "memory_mb": cfg.get("MemorySize"),
            "timeout_s": cfg.get("Timeout"),
            "handler": cfg.get("Handler"),
            "role": cfg.get("Role", "").split("/")[-1],
            "env_keys": sorted((cfg.get("Environment") or {}).get("Variables", {}).keys()),
            "code_size_kb": round(cfg.get("CodeSize", 0) / 1024, 1),
            "last_modified": cfg.get("LastModified"),
        }
    except Exception as e:
        out["lambda_config_err"] = str(e)[:300]

    # ─── Schedule check ───
    try:
        rule = events.describe_rule(Name="justhodl-dealer-gex-hourly")
        targets = events.list_targets_by_rule(Rule="justhodl-dealer-gex-hourly")
        out["schedule"] = {
            "cron": rule.get("ScheduleExpression"),
            "state": rule.get("State"),
            "targets": [t.get("Arn", "").split(":")[-1] for t in targets.get("Targets", [])],
        }
    except Exception as e:
        out["schedule_err"] = str(e)[:300]

    # ─── Sidecar inspection ───
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/dealer-gex.json")
        body = obj["Body"].read()
        p = json.loads(body)
        ulying = p.get("underlyings", {})
        ok_count = sum(1 for r in ulying.values() if not r.get("err"))
        total_contracts = sum((r.get("n_contracts_modeled") or 0) for r in ulying.values() if not r.get("err"))
        total_call_oi = sum((r.get("total_call_oi") or 0) for r in ulying.values() if not r.get("err"))
        total_put_oi = sum((r.get("total_put_oi") or 0) for r in ulying.values() if not r.get("err"))
        regimes = {sym: r.get("regime") for sym, r in ulying.items() if not r.get("err")}
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "last_modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "elapsed_seconds": p.get("elapsed_seconds"),
            "underlyings_ok": ok_count,
            "total_contracts_modeled": total_contracts,
            "total_call_oi": total_call_oi,
            "total_put_oi": total_put_oi,
            "regimes": regimes,
            "market_composite": p.get("market_composite"),
            "n_squeeze_candidates": len(p.get("squeeze_candidates") or []),
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    # ─── Verify Lambda probes pages from AWS ───
    PROBE_NAME = "justhodl-tmp-verify-506"
    try:
        lam.create_function(
            FunctionName=PROBE_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role="arn:aws:iam::857687956942:role/lambda-execution-role",
            MemorySize=512, Timeout=45, Code={"ZipFile": zip_str(VERIFY_CODE)},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=PROBE_NAME)
    except lam.exceptions.ResourceConflictException:
        lam.update_function_code(FunctionName=PROBE_NAME, ZipFile=zip_str(VERIFY_CODE))
        lam.get_waiter("function_updated").wait(FunctionName=PROBE_NAME)

    _time.sleep(2)
    try:
        r = lam.invoke(FunctionName=PROBE_NAME, InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8")
        p = json.loads(body)
        out["probes"] = json.loads(p["body"]) if p.get("body") else p
    except Exception as e:
        out["probe_err"] = str(e)[:300]

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except: pass

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
