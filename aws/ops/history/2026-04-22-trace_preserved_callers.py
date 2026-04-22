#!/usr/bin/env python3
"""
Phase 2b — trace callers of the 11 preserved Lambdas.

For each of the 11 functions that our Phase 2 safety check refused to
delete (because they had invocations we couldn't attribute):

  enhanced-openbb-handler   (94,802 invocations / 90d)
  ecb                        (9,774)
  justhodl-data-collector    (3,521)
  ecb-data-daily-updater     (270)
  ofrapi                     (106)
  justhodl-email-reports-v2  (90)
  justhodl-liquidity-agent   (44)
  ecb-auto-updater           (13)
  justhodl-calibrator        (6)
  nyfed-cmdi-fetcher         (1)
  nyfed-main-aggregator      (1)

Five caller sources to check for each:

  1. Lambda resource policy (get_policy) → shows services with explicit
     invoke permission (API Gateway, S3, SNS, etc.)
  2. Function URL config → direct internet-reachable URL
  3. API Gateway REST + HTTP integrations targeting this ARN
  4. EventBridge rules targeting this ARN (already known, confirm)
  5. Static analysis — grep ALL 98 Lambdas' source code for
     FunctionName='<target>' patterns.

For #5, we download every Python/JS Lambda's current code, grep it,
then discard. About ~50 Lambdas × small zips. Takes ~2 min.

Produces a dependency map per preserved Lambda + a summary table.
Read-only. No changes.
"""

import io
import json
import os
import re
import urllib.request
import zipfile
from collections import defaultdict
from datetime import datetime, timezone

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"

PRESERVED = [
    ("enhanced-openbb-handler",   94802),
    ("ecb",                        9774),
    ("justhodl-data-collector",    3521),
    ("ecb-data-daily-updater",      270),
    ("ofrapi",                      106),
    ("justhodl-email-reports-v2",    90),
    ("justhodl-liquidity-agent",     44),
    ("ecb-auto-updater",             13),
    ("justhodl-calibrator",           6),
    ("nyfed-cmdi-fetcher",            1),
    ("nyfed-main-aggregator",         1),
]
PRESERVED_NAMES = [n for n, _ in PRESERVED]

lam = boto3.client("lambda", region_name=REGION)
apigw  = boto3.client("apigateway", region_name=REGION)
apigw2 = boto3.client("apigatewayv2", region_name=REGION)
ev     = boto3.client("events", region_name=REGION)


def get_resource_policy(fn_name: str):
    """Returns list of statements or []."""
    try:
        raw = lam.get_policy(FunctionName=fn_name)["Policy"]
        return json.loads(raw).get("Statement", [])
    except lam.exceptions.ResourceNotFoundException:
        return []
    except ClientError:
        return []


def get_function_url(fn_name: str):
    try:
        resp = lam.get_function_url_config(FunctionName=fn_name)
        return resp.get("FunctionUrl")
    except lam.exceptions.ResourceNotFoundException:
        return None
    except ClientError:
        return None


def get_eventbridge_rules(fn_arn: str):
    try:
        return ev.list_rule_names_by_target(TargetArn=fn_arn).get("RuleNames", [])
    except ClientError:
        return []


def scan_rest_api_integrations(target_names: set):
    """Return dict {fn_name: [api_info]} for any REST API with integration URI matching function name."""
    hits = defaultdict(list)
    try:
        paginator = apigw.get_paginator("get_rest_apis")
        for page in paginator.paginate():
            for api in page.get("items", []):
                api_id = api["id"]
                try:
                    resources = apigw.get_resources(restApiId=api_id, limit=500).get("items", [])
                except ClientError:
                    continue
                for resource in resources:
                    methods = resource.get("resourceMethods") or {}
                    for http_method in methods:
                        try:
                            integ = apigw.get_integration(
                                restApiId=api_id, resourceId=resource["id"], httpMethod=http_method
                            )
                            uri = integ.get("uri", "")
                            for name in target_names:
                                if f":function:{name}/" in uri or f":function:{name}" in uri:
                                    hits[name].append({
                                        "api_id": api_id,
                                        "api_name": api.get("name"),
                                        "path": resource.get("path"),
                                        "method": http_method,
                                    })
                        except ClientError:
                            continue
    except ClientError:
        pass
    return hits


def scan_http_api_integrations(target_names: set):
    """Same but for API Gateway v2 (HTTP APIs)."""
    hits = defaultdict(list)
    try:
        paginator = apigw2.get_paginator("get_apis")
        for page in paginator.paginate():
            for api in page.get("Items", []):
                api_id = api["ApiId"]
                try:
                    integrations = apigw2.get_integrations(ApiId=api_id, MaxResults="500").get("Items", [])
                except ClientError:
                    continue
                for integ in integrations:
                    uri = integ.get("IntegrationUri", "")
                    for name in target_names:
                        if f":function:{name}/" in uri or f":function:{name}" in uri:
                            hits[name].append({
                                "api_id": api_id,
                                "api_name": api.get("Name"),
                                "integration_id": integ.get("IntegrationId"),
                            })
    except ClientError:
        pass
    return hits


def enumerate_all_lambdas():
    """List every Lambda in the account. Returns [{name, runtime, arn, size}, ...]."""
    lambdas = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for fn in page.get("Functions", []):
            lambdas.append({
                "name": fn["FunctionName"],
                "runtime": fn.get("Runtime", ""),
                "arn": fn["FunctionArn"],
                "size": fn.get("CodeSize", 0),
                "package_type": fn.get("PackageType", "Zip"),
            })
    return lambdas


def download_and_scan(fn_name: str, target_names: set) -> dict:
    """Download a function's zip and grep for references to target names. Returns {target_name: [lines]}."""
    try:
        code_url = lam.get_function(FunctionName=fn_name)["Code"]["Location"]
    except ClientError:
        return {}
    try:
        with urllib.request.urlopen(code_url, timeout=15) as resp:
            zbytes = resp.read()
    except Exception:
        return {}

    hits = defaultdict(list)
    try:
        with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
            for entry in zf.namelist():
                if not (entry.endswith(".py") or entry.endswith(".js") or entry.endswith(".mjs")):
                    continue
                # Skip vendored deps — we care about app code only
                if "/site-packages/" in entry or "node_modules/" in entry or "/dist-info/" in entry:
                    continue
                try:
                    src = zf.read(entry).decode("utf-8", errors="ignore")
                except Exception:
                    continue
                for target in target_names:
                    # Look for function name in quoted context (typical of FunctionName='foo')
                    # Use word boundaries to avoid partial matches
                    pattern = re.compile(r"[\"']" + re.escape(target) + r"[\"']")
                    for line_no, line in enumerate(src.splitlines(), 1):
                        if pattern.search(line):
                            hits[target].append(f"{entry}:{line_no}: {line.strip()[:200]}")
    except zipfile.BadZipFile:
        pass
    return dict(hits)


with report("trace_preserved_callers") as r:
    r.heading("Phase 2b — trace callers of 11 preserved Lambdas")
    r.log(f"Started at {datetime.now(timezone.utc).isoformat()}")

    preserved_set = set(PRESERVED_NAMES)
    findings = {name: {
        "invocations_90d": inv,
        "resource_policy": [],
        "function_url": None,
        "eb_rules": [],
        "rest_api_hits": [],
        "http_api_hits": [],
        "code_callers": [],  # [(caller_fn, file_line)]
    } for name, inv in PRESERVED}

    # ─────────────────────────────────────────────
    # 1. Resource policy + Function URL + EB rules (per preserved)
    # ─────────────────────────────────────────────
    r.section("Step 1: resource policies + function URLs + EB rules")
    for name, _ in PRESERVED:
        try:
            fn_arn = lam.get_function_configuration(FunctionName=name)["FunctionArn"]
        except lam.exceptions.ResourceNotFoundException:
            r.warn(f"  {name}: function not found — may have been deleted since audit")
            continue
        except ClientError as e:
            r.warn(f"  {name}: {e}")
            continue

        policy = get_resource_policy(name)
        url = get_function_url(name)
        rules = get_eventbridge_rules(fn_arn)

        findings[name]["resource_policy"] = policy
        findings[name]["function_url"] = url
        findings[name]["eb_rules"] = rules

        summary = []
        if url:
            summary.append(f"URL=yes")
        if rules:
            summary.append(f"EB={len(rules)}")
        if policy:
            services = set()
            for stmt in policy:
                principal = stmt.get("Principal", {})
                if isinstance(principal, dict):
                    svc = principal.get("Service", "")
                    if svc:
                        services.add(svc if isinstance(svc, str) else ",".join(svc))
            if services:
                summary.append(f"policy={','.join(sorted(services))}")
            else:
                summary.append(f"policy={len(policy)}stmt")
        r.log(f"  {name}: {' | '.join(summary) if summary else '(no URL / no rules / no policy)'}")

    # ─────────────────────────────────────────────
    # 2. API Gateway integrations
    # ─────────────────────────────────────────────
    r.section("Step 2: API Gateway integrations (REST + HTTP)")
    try:
        rest_hits = scan_rest_api_integrations(preserved_set)
        http_hits = scan_http_api_integrations(preserved_set)
    except Exception as e:
        r.warn(f"  API Gateway scan failed: {e}")
        rest_hits, http_hits = {}, {}

    for name in PRESERVED_NAMES:
        if name in rest_hits:
            findings[name]["rest_api_hits"] = rest_hits[name]
            for hit in rest_hits[name]:
                r.log(f"  {name} ← REST API '{hit['api_name']}' {hit['method']} {hit['path']}")
        if name in http_hits:
            findings[name]["http_api_hits"] = http_hits[name]
            for hit in http_hits[name]:
                r.log(f"  {name} ← HTTP API '{hit['api_name']}'")

    if not any(rest_hits.values()) and not any(http_hits.values()):
        r.log("  (no API Gateway integrations target any preserved Lambda)")

    # ─────────────────────────────────────────────
    # 3. Static analysis — scan every Lambda's code
    # ─────────────────────────────────────────────
    r.section("Step 3: static analysis — scan 98 Lambdas' code for callers")
    all_lambdas = enumerate_all_lambdas()
    # Skip container-image Lambdas (can't download zip easily)
    code_lambdas = [l for l in all_lambdas if l["package_type"] == "Zip"]
    r.log(f"  Scanning {len(code_lambdas)} zip-packaged Lambdas…")

    processed = 0
    for fn in code_lambdas:
        # Skip the preserved Lambdas themselves (don't care about self-refs)
        if fn["name"] in preserved_set:
            continue
        if fn["size"] > 20_000_000:  # skip huge bundles
            continue

        hits = download_and_scan(fn["name"], preserved_set)
        if hits:
            for target, lines in hits.items():
                for line in lines:
                    findings[target]["code_callers"].append({
                        "caller": fn["name"],
                        "file_line": line,
                    })
        processed += 1
        if processed % 10 == 0:
            r.log(f"    scanned {processed}/{len(code_lambdas)}…")

    r.log(f"  Done scanning {processed} Lambdas")

    # ─────────────────────────────────────────────
    # 4. Per-target summary
    # ─────────────────────────────────────────────
    r.section("Step 4: caller map per preserved Lambda")
    for name, _ in PRESERVED:
        f = findings[name]
        r.log("")
        r.log(f"### {name} ({f['invocations_90d']} invocations/90d)")

        # Code callers are usually the smoking gun
        if f["code_callers"]:
            callers = sorted({c["caller"] for c in f["code_callers"]})
            r.log(f"  CODE CALLERS ({len(callers)}): {', '.join(callers)}")
            for c in f["code_callers"][:5]:
                r.log(f"    ← {c['caller']}  {c['file_line']}")
            if len(f["code_callers"]) > 5:
                r.log(f"    …and {len(f['code_callers']) - 5} more ref(s)")
        else:
            r.log("  CODE CALLERS: (none found)")

        if f["function_url"]:
            r.log(f"  FUNCTION URL: {f['function_url']}")
        if f["rest_api_hits"]:
            r.log(f"  REST API: {len(f['rest_api_hits'])} integration(s)")
        if f["http_api_hits"]:
            r.log(f"  HTTP API: {len(f['http_api_hits'])} integration(s)")
        if f["eb_rules"]:
            r.log(f"  EB RULES: {', '.join(f['eb_rules'])}")

        # Record in the data table
        top_callers = sorted({c["caller"] for c in f["code_callers"]})
        r.kv(
            target=name,
            inv_90d=f["invocations_90d"],
            code_callers=", ".join(top_callers[:5]) if top_callers else "—",
            has_url=bool(f["function_url"]),
            eb_rules=len(f["eb_rules"]),
            api_gw=len(f["rest_api_hits"]) + len(f["http_api_hits"]),
        )

    # ─────────────────────────────────────────────
    # 5. Verdict per Lambda
    # ─────────────────────────────────────────────
    r.section("Step 5: verdict")
    for name, inv in PRESERVED:
        f = findings[name]
        callers = sorted({c["caller"] for c in f["code_callers"]})
        has_eb = bool(f["eb_rules"])
        has_api = bool(f["rest_api_hits"] or f["http_api_hits"])
        has_url = bool(f["function_url"])

        if callers:
            verdict = f"KEEP — invoked by: {', '.join(callers[:3])}"
        elif has_eb:
            verdict = f"KEEP — scheduled by EB: {f['eb_rules'][0]}"
        elif has_api:
            verdict = "KEEP — API Gateway integration"
        elif has_url:
            verdict = "INVESTIGATE — has Function URL, unknown who hits it"
        else:
            verdict = "SAFE TO DELETE — no callers found yet still getting invocations. CloudTrail needed to be 100% sure."

        r.log(f"  {name}: {verdict}")

    r.log("Done")
