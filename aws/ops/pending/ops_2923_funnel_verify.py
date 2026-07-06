#!/usr/bin/env python3
"""ops 2923 — audit Week-1 verify: DDB table + FnURL + endpoint json + live
subscribe round-trip + baked first paint + metadata/pitch/CTA live."""
import json, sys, time, urllib.request, re
from pathlib import Path
sys.path.insert(0, "aws/ops")
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
import boto3

def get(u, to=15, method="GET", body=None, hdrs=None):
    try:
        req = urllib.request.Request(u, data=body, method=method,
              headers=dict({"User-Agent": "Mozilla/5.0 jh"}, **(hdrs or {})))
        r = urllib.request.urlopen(req, timeout=to)
        return r.getcode(), r.read().decode("utf-8", "replace")
    except Exception as e:
        return None, str(e)

ok_all = True; out = {}
with report("2923") as r:
    r.section("capture backend")
    ddb = boto3.client("dynamodb", region_name="us-east-1")
    try:
        ddb.describe_table(TableName="justhodl-subscribers")
        r.ok("  DDB table exists")
    except ddb.exceptions.ResourceNotFoundException:
        ddb.create_table(TableName="justhodl-subscribers",
            AttributeDefinitions=[{"AttributeName":"email","AttributeType":"S"}],
            KeySchema=[{"AttributeName":"email","KeyType":"HASH"}],
            BillingMode="PAY_PER_REQUEST")
        ddb.get_waiter("table_exists").wait(TableName="justhodl-subscribers")
        r.ok("  DDB table created (on-demand)")
    lam = boto3.client("lambda", region_name="us-east-1")
    deploy_lambda(report=r, function_name="justhodl-subscribe",
                  source_dir=Path("aws/lambdas/justhodl-subscribe/source"),
                  env_vars={"SUB_TABLE": "justhodl-subscribers"}, timeout=10, memory=128)
    try:
        url = lam.get_function_url_config(FunctionName="justhodl-subscribe")["FunctionUrl"]
        r.ok(f"  FnURL exists")
    except lam.exceptions.ResourceNotFoundException:
        url = lam.create_function_url_config(FunctionName="justhodl-subscribe",
            AuthType="NONE", Cors={"AllowOrigins":["*"],"AllowMethods":["POST"],
            "AllowHeaders":["content-type"]})["FunctionUrl"]
        try:
            lam.add_permission(FunctionName="justhodl-subscribe",
                StatementId="FunctionURLAllowPublicAccess",
                Action="lambda:InvokeFunctionUrl", Principal="*",
                FunctionUrlAuthType="NONE")
        except lam.exceptions.ResourceConflictException:
            pass
        r.ok(f"  FnURL created")
    out["endpoint"] = url
    boto3.client("s3").put_object(Bucket="justhodl-dashboard-live",
        Key="data/subscribe-endpoint.json",
        Body=json.dumps({"url": url}).encode(),
        ContentType="application/json", CacheControl="max-age=300")
    r.ok("  endpoint published -> data/subscribe-endpoint.json")

    c, b = get(url, method="POST",
               body=json.dumps({"email":"ops-test-2923@justhodl.ai","source":"ops"}).encode(),
               hdrs={"content-type":"application/json"})
    j = json.loads(b) if c == 200 else {}
    posted = c == 200 and j.get("ok") is True
    it = ddb.get_item(TableName="justhodl-subscribers",
                      Key={"email":{"S":"ops-test-2923@justhodl.ai"}}).get("Item")
    stored = bool(it)
    if stored:
        ddb.delete_item(TableName="justhodl-subscribers",
                        Key={"email":{"S":"ops-test-2923@justhodl.ai"}})
    ok_all &= posted and stored
    out["roundtrip"] = {"post": c, "ok": j.get("ok"), "ddb_stored": stored, "cleaned": stored}
    (r.ok if posted and stored else r.fail)(f"  round-trip: {out['roundtrip']}")

    r.section("live homepage: baked paint + funnel elements")
    idx = ""
    for att in range(12):
        c, idx = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        if 'JH_V="v1.2.7"' in idx and re.search(r'id="tp-spx">[^<]*\d', idx):
            break
        time.sleep(18)
    checks = {
        "v1.2.7": 'JH_V="v1.2.7"' in idx,
        "baked_tape": bool(re.search(r'id="tp-spx">[^<]*\d', idx)),
        "baked_ka": bool(re.search(r'id="kpi-ka">[^<]*\d', idx)),
        "as_of_stamp": "AS OF" in idx,
        "og_description": 'property="og:description"' in idx,
        "unified_title": "Institutional Market Intelligence</title>" in idx,
        "hero_pitch": "The edge hedge funds pay for" in idx,
        "cta": 'id="jh-cta"' in idx,
        "subscribe_ui": 'id="jh-sub-e"' in idx,
    }
    dashes = idx.count('">—</span>')
    checks["residual_dashes_le_8"] = dashes <= 8
    out["page"] = {"attempt": att + 1, "dashes": dashes, "checks": checks}
    ok_all &= all(checks.values())
    for k, v in checks.items():
        (r.ok if v else r.fail)(f"  {k}: {v}")
    r.log(f"  residual dashes: {dashes}")

    json.dump(out, open("aws/ops/reports/2923.json", "w"), indent=2, default=str)
    r.ok("report -> 2923.json")
print("DONE 2923", "PASS" if ok_all else "FAIL")
sys.exit(0 if ok_all else 1)
