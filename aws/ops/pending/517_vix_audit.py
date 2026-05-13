#!/usr/bin/env python3
"""517 — Audit justhodl-vix-curve + probe CBOE CDN VIX/VIX9D/VIX3M/VIX6M/VVIX feeds."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/517_vix_audit.json"
lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

PROBE_CODE = """
import json, urllib.request, time

URLS = [
    'https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX9D_History.csv',
    'https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv',
    'https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX3M_History.csv',
    'https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX6M_History.csv',
    'https://cdn.cboe.com/api/global/us_indices/daily_prices/VVIX_History.csv',
    'https://cdn.cboe.com/api/global/us_indices/daily_prices/VXN_History.csv',
    'https://cdn.cboe.com/api/global/us_indices/daily_prices/RVX_History.csv',
    # Also check VIX futures continuous
    'https://cdn.cboe.com/api/global/delayed_quotes/futures.json',
]

def probe(url):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh) Chrome/120 Safari/537.36',
            'Accept': '*/*',
        })
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read()
            head = body[:400].decode('utf-8','replace')
            return {'url': url[-60:], 'status': r.status, 'bytes': len(body),
                     'head': head, 'elapsed_ms': int((time.time()-t0)*1000)}
    except urllib.error.HTTPError as e:
        return {'url': url[-60:], 'status': e.code, 'elapsed_ms': int((time.time()-t0)*1000)}
    except Exception as e:
        return {'url': url[-60:], 'status': 'EXC', 'err': str(e)[:200],
                 'elapsed_ms': int((time.time()-t0)*1000)}

def lambda_handler(event, context):
    return {'statusCode': 200, 'body': json.dumps({'probes': [probe(u) for u in URLS]})}
"""


def zip_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()


def audit(name):
    info = {"name": name}
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
        info["exists"] = True
        info["last_modified"] = cfg.get("LastModified")
        info["memory"] = cfg.get("MemorySize")
        info["timeout"] = cfg.get("Timeout")
        info["env_keys"] = sorted((cfg.get("Environment", {}) or {}).get("Variables", {}).keys())
    except lam.exceptions.ResourceNotFoundException:
        return {"name": name, "exists": False}

    # EventBridge
    try:
        rules = []
        all_rules = eb.list_rules()["Rules"]
        for r in all_rules:
            try:
                targets = eb.list_targets_by_rule(Rule=r["Name"])["Targets"]
                if any(name in t.get("Arn", "") for t in targets):
                    rules.append({"name": r["Name"], "schedule": r.get("ScheduleExpression"),
                                    "state": r.get("State")})
            except: pass
        info["rules"] = rules
    except Exception as e:
        info["rules_err"] = str(e)[:100]

    # Sample invoke to see what happens
    try:
        resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        info["invoke_status"] = resp.get("StatusCode")
        info["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")[:600]
        info["body_preview"] = body
        if resp.get("LogResult"):
            info["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8","replace")[-1500:]
    except Exception as e:
        info["invoke_err"] = str(e)[:200]

    return info


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # ── Existing Lambdas ──
    out["lambdas"] = {
        "justhodl-vix-curve": audit("justhodl-vix-curve"),
        "justhodl-vol-regime": audit("justhodl-vol-regime"),
    }

    # ── Sidecars ──
    out["sidecars"] = {}
    for k in ["data/vix-curve.json", "data/vol-regime.json"]:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=k)
            body = obj["Body"].read()
            p = json.loads(body)
            out["sidecars"][k] = {
                "size_kb": round(len(body)/1024, 1),
                "modified": obj["LastModified"].isoformat()[:19],
                "top_keys": list(p.keys())[:15],
                "generated_at": p.get("generated_at"),
                "version": p.get("version"),
            }
            # Sample interesting fields
            for f in ["regime", "vix_spot", "vix_9d", "vix_3m", "vix_6m",
                       "contango_score", "spread_30d_3m", "spread_9d_30d"]:
                if f in p: out["sidecars"][k][f] = p[f]
        except s3.exceptions.NoSuchKey:
            out["sidecars"][k] = {"exists": False}
        except Exception as e:
            out["sidecars"][k] = {"err": str(e)[:200]}

    # ── Probe CBOE CDN ──
    PROBE_NAME = "justhodl-tmp-vix-probe"
    try:
        lam.create_function(
            FunctionName=PROBE_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role="arn:aws:iam::857687956942:role/lambda-execution-role",
            MemorySize=512, Timeout=60, Code={"ZipFile": zip_str(PROBE_CODE)},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=PROBE_NAME)
    except lam.exceptions.ResourceConflictException:
        lam.update_function_code(FunctionName=PROBE_NAME, ZipFile=zip_str(PROBE_CODE))
        lam.get_waiter("function_updated").wait(FunctionName=PROBE_NAME)

    _time.sleep(2)
    try:
        r = lam.invoke(FunctionName=PROBE_NAME, InvocationType="RequestResponse",
                        Payload=b"{}")
        body = r["Payload"].read().decode("utf-8")
        p = json.loads(body)
        out["cboe_probes"] = json.loads(p["body"]) if p.get("body") else p
    except Exception as e:
        out["probe_err"] = str(e)[:300]

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except: pass

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
