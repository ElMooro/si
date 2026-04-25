#!/usr/bin/env python3
"""
Step 182 — Diagnose AI Lambda 'Failed to fetch' from browser.

User reports the AI Research tab gets 'Failed to fetch'. Lambda works
fine when invoked via boto3 (step 180 returned full output for AAPL).
Means the issue is in HOW THE BROWSER REACHES IT — auth, CORS, or
resource policy.

Compare the new AI Lambda's Function URL config + resource policy to
a known-working public Lambda (justhodl-stock-screener), which IS
reachable from the browser at justhodl.ai/screener/.
"""
import json
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
AI_LAMBDA = "justhodl-stock-ai-research"
WORKING = "justhodl-stock-screener"

lam = boto3.client("lambda", region_name=REGION)


def dump_lambda(r, name):
    r.section(f"=== {name} ===")
    cfg = lam.get_function_configuration(FunctionName=name)
    r.log(f"  FunctionArn: {cfg.get('FunctionArn')}")
    r.log(f"  Runtime: {cfg.get('Runtime')}, Handler: {cfg.get('Handler')}")
    r.log(f"  LastModified: {cfg.get('LastModified','?')[:19]}")

    # Function URL config
    try:
        url_cfg = lam.get_function_url_config(FunctionName=name)
        r.log(f"\n  Function URL:")
        r.log(f"    URL:      {url_cfg.get('FunctionUrl')}")
        r.log(f"    AuthType: {url_cfg.get('AuthType')}")
        cors = url_cfg.get("Cors", {})
        if cors:
            r.log(f"    CORS:")
            r.log(f"      AllowOrigins:     {cors.get('AllowOrigins',[])}")
            r.log(f"      AllowMethods:     {cors.get('AllowMethods',[])}")
            r.log(f"      AllowHeaders:     {cors.get('AllowHeaders',[])}")
            r.log(f"      ExposeHeaders:    {cors.get('ExposeHeaders',[])}")
            r.log(f"      AllowCredentials: {cors.get('AllowCredentials',False)}")
            r.log(f"      MaxAge:           {cors.get('MaxAge',0)}")
        else:
            r.warn(f"    NO CORS CONFIG — preflight will fail")
    except ClientError as e:
        r.warn(f"  No Function URL: {e}")
        return

    # Resource-based policy (who can invoke?)
    try:
        pol = lam.get_policy(FunctionName=name)
        policy = json.loads(pol["Policy"])
        r.log(f"\n  Resource policy ({len(policy.get('Statement', []))} statements):")
        for stmt in policy.get("Statement", []):
            sid = stmt.get("Sid", "?")
            principal = stmt.get("Principal", "?")
            action = stmt.get("Action", "?")
            cond = stmt.get("Condition", {})
            r.log(f"    {sid}:")
            r.log(f"      Principal: {principal}")
            r.log(f"      Action:    {action}")
            if cond:
                r.log(f"      Condition: {json.dumps(cond)[:200]}")
    except ClientError as e:
        if "ResourceNotFoundException" in str(e):
            r.warn(f"  ⚠ NO RESOURCE POLICY — anonymous invocation will fail!")
        else:
            r.warn(f"  policy error: {e}")


with report("diagnose_ai_lambda_url") as r:
    r.heading("Diagnose AI Lambda 'Failed to fetch'")

    # ─── Compare both ───────────────────────────────────────────────────
    dump_lambda(r, AI_LAMBDA)
    dump_lambda(r, WORKING)

    # ─── Diagnosis ──────────────────────────────────────────────────────
    r.section("Diagnosis")
    try:
        ai_url = lam.get_function_url_config(FunctionName=AI_LAMBDA)
        wk_url = lam.get_function_url_config(FunctionName=WORKING)
        ai_cors = ai_url.get("Cors", {}) or {}
        wk_cors = wk_url.get("Cors", {}) or {}
        if not ai_cors and wk_cors:
            r.fail("  AI Lambda has NO CORS config but working Lambda does — likely cause")
        elif ai_cors.get("AllowOrigins") != wk_cors.get("AllowOrigins"):
            r.warn(f"  AllowOrigins differ:")
            r.log(f"    AI:      {ai_cors.get('AllowOrigins')}")
            r.log(f"    Working: {wk_cors.get('AllowOrigins')}")

        # Resource policy
        try:
            lam.get_policy(FunctionName=AI_LAMBDA)
            ai_has_policy = True
        except ClientError:
            ai_has_policy = False
        try:
            lam.get_policy(FunctionName=WORKING)
            wk_has_policy = True
        except ClientError:
            wk_has_policy = False
        r.log(f"\n  AI resource policy:      {'present' if ai_has_policy else 'MISSING'}")
        r.log(f"  Working resource policy: {'present' if wk_has_policy else 'MISSING'}")
        if not ai_has_policy and wk_has_policy:
            r.fail(f"  ⚠ Browser CAN'T reach AI Lambda — needs lambda:InvokeFunctionUrl statement")
    except Exception as e:
        r.warn(f"  diagnosis error: {e}")

    r.log("Done")
