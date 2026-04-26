#!/usr/bin/env python3
"""
Step 205 — Final cleanup verify.

Why did news-sentiment-agent show 11 invocations / 11 errors but step
204 only found schedules for 4 rules (none for news-sentiment)? Need
to find what's triggering it:
  A. List ALL EventBridge rules targeting it (any state)
  B. List ALL event-source-mappings (SQS, DynamoDB streams, etc.)
  C. Get function policy (resource-based perms — which services can invoke?)
  D. Final sweep verification — list all currently-disabled rules
     for the 3 cleanup target Lambdas

Also confirm:
  E. /archive/ pages now exist on GitHub Pages
  F. 3 stubs are actually 404'd
"""
import io, json, time, zipfile
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
PROBE_NAME = "justhodl-tmp-probe-205"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

TARGETS = [
    "news-sentiment-agent",
    "fmp-stock-picks-agent",
    "justhodl-daily-macro-report",
]

PROBE_CODE = '''
import json, urllib.request, urllib.error
def lambda_handler(event, context):
    try:
        req = urllib.request.Request(event["url"], headers=event.get("headers", {}))
        with urllib.request.urlopen(req, timeout=15) as r:
            return {"ok": True, "status": r.status, "len": len(r.read())}
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code}
    except Exception as e:
        return {"ok": False, "error": str(e)}
'''


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", PROBE_CODE)
    buf.seek(0); return buf.read()


with report("final_cleanup_verify") as r:
    r.heading("Final cleanup verification")

    # ─── A+B+C. Per-Lambda trigger source audit ─────────────────────────
    for name in TARGETS:
        r.section(f"🔎 {name}")

        # All EventBridge rules targeting this lambda (regardless of state)
        rules_targeting = []
        next_token = None
        while True:
            kwargs = {"Limit": 100}
            if next_token: kwargs["NextToken"] = next_token
            rules_resp = events.list_rules(**kwargs)
            for rule in rules_resp.get("Rules", []):
                try:
                    tgts = events.list_targets_by_rule(Rule=rule["Name"])
                    for t in tgts.get("Targets", []):
                        arn = t.get("Arn", "")
                        if ":lambda:" in arn and arn.split(":")[-1] == name:
                            rules_targeting.append({
                                "name": rule["Name"],
                                "state": rule.get("State"),
                                "schedule": rule.get("ScheduleExpression", ""),
                                "pattern": rule.get("EventPattern", "")[:150] if rule.get("EventPattern") else "",
                            })
                except Exception:
                    continue
            next_token = rules_resp.get("NextToken")
            if not next_token: break
        r.log(f"  EventBridge rules ({len(rules_targeting)}):")
        for rule in rules_targeting:
            mark = "🟢" if rule["state"] == "ENABLED" else "⚪"
            r.log(f"    {mark} {rule['name']:50} {rule['state']:10} {rule['schedule']}")

        # Event source mappings (SQS / DDB streams / Kinesis)
        try:
            esm = lam.list_event_source_mappings(FunctionName=name, MaxItems=10)
            mappings = esm.get("EventSourceMappings", [])
            r.log(f"  Event source mappings ({len(mappings)}):")
            for m in mappings:
                r.log(f"    {m.get('EventSourceArn', '?'):60} state={m.get('State')}")
        except Exception as e:
            r.log(f"  ESM lookup err: {e}")

        # Function policy (resource-based perms — who can invoke?)
        try:
            policy_resp = lam.get_policy(FunctionName=name)
            policy = json.loads(policy_resp["Policy"])
            stmts = policy.get("Statement", [])
            r.log(f"  Function policy statements ({len(stmts)}):")
            for s in stmts[:8]:
                principal = s.get("Principal", {})
                if isinstance(principal, dict):
                    principal = principal.get("Service") or principal.get("AWS") or "?"
                r.log(f"    sid={s.get('Sid', '?'):40} principal={principal}")
        except lam.exceptions.ResourceNotFoundException:
            r.log(f"  Function policy: none")
        except Exception as e:
            r.log(f"  Function policy err: {e}")

    # ─── D. Quick GitHub Pages probe ────────────────────────────────────
    r.section("D. GitHub Pages — confirm cleanup is live")
    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    lam.create_function(
        FunctionName=PROBE_NAME, Runtime="python3.11",
        Role=ROLE_ARN, Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=20, MemorySize=256, Architectures=["x86_64"],
    )
    time.sleep(3)

    paths_to_check = [
        # Stubs that should now 404
        ("Reports.html", "stub-removed"),
        ("ml.html", "stub-removed"),
        ("stocks.html", "stub-removed"),
        # Archived pages — still served by GitHub Pages but at /archive/
        ("archive/pro.html", "archived"),
        ("archive/exponential-search-dashboard.html", "archived"),
        ("archive/macroeconomic-platform.html", "archived"),
        ("archive/README.md", "archive-readme"),
        # Pages that should 200
        ("repo.html", "real-now"),
        ("volatility.html", "new"),
    ]

    for path, kind in paths_to_check:
        url = f"https://justhodl.ai/{path}"
        resp = lam.invoke(
            FunctionName=PROBE_NAME, InvocationType="RequestResponse",
            Payload=json.dumps({"url": url, "headers": {"User-Agent": UA}}),
        )
        result = json.loads(resp["Payload"].read())
        if result.get("ok"):
            mark = "✅" if (kind != "stub-removed") else "🟡"
            r.log(f"  {mark} {kind:18} {path:55} HTTP {result['status']} {result['len']}B")
        else:
            mark = "✅" if (kind == "stub-removed" and result.get("status") == 404) else "🔴"
            r.log(f"  {mark} {kind:18} {path:55} HTTP {result.get('status', '?')} {result.get('error', '')}")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    r.log("Done")
