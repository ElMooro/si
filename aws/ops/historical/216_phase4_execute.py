#!/usr/bin/env python3
"""Step 216 — Phase 4 execute: rename justhodl-khalid-metrics → justhodl-ka-metrics."""
import io, json, time, urllib.request, zipfile, re
from datetime import datetime, timezone
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
OLD = "justhodl-khalid-metrics"
NEW = "justhodl-ka-metrics"
RULE = "justhodl-khalid-metrics-refresh"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def patch_source_for_dual_write(src):
    """After each s3.put_object(Key='data/khalid-X.json', ...), insert a
    sibling call with Key='data/ka-X.json'. Same Body, same kwargs."""
    pattern = re.compile(
        r"(s3\.put_object\([^)]*Key\s*=\s*['\"]data/khalid-)([a-z]+)(\.json['\"][^)]*\))",
        re.MULTILINE,
    )
    def replacer(m):
        original = m.group(0)
        duplicate = (original.replace("Key='data/khalid-", "Key='data/ka-")
                             .replace('Key="data/khalid-', 'Key="data/ka-'))
        return original + "\n    " + duplicate
    return pattern.sub(replacer, src)


with report("phase4_create_ka_metrics_lambda") as r:
    r.heading("Phase 4 — create justhodl-ka-metrics + dual-write S3")

    abort = False
    abort_reason = None
    old_info = None

    # 1. Pre-flight
    r.section("1. Pre-flight checks")
    try:
        old_info = lam.get_function(FunctionName=OLD)
        r.log(f"  ✅ {OLD} exists, will copy from")
    except ClientError as e:
        r.warn(f"  ✗ {OLD} not found: {e}")
        abort, abort_reason = True, "old Lambda missing"

    if not abort:
        try:
            lam.get_function(FunctionName=NEW)
            r.warn(f"  ⚠ {NEW} already exists — aborting")
            abort, abort_reason = True, f"{NEW} already exists"
        except ClientError as e:
            if "ResourceNotFoundException" in str(e):
                r.log(f"  ✅ {NEW} does not exist — safe to create")
            else:
                r.warn(f"  unexpected: {e}")
                abort, abort_reason = True, f"unexpected: {e}"

    if abort:
        r.warn(f"\nABORTED at pre-flight: {abort_reason}")
    else:
        # 2. Download old code
        r.section("2. Download old Lambda zip")
        old_cfg = old_info["Configuration"]
        with urllib.request.urlopen(old_info["Code"]["Location"], timeout=30) as resp:
            old_zip_bytes = resp.read()
        r.log(f"  old zip: {len(old_zip_bytes)}B")
        with zipfile.ZipFile(io.BytesIO(old_zip_bytes)) as zf:
            files = zf.namelist()
            src = zf.read("lambda_function.py").decode("utf-8", errors="replace")
        r.log(f"  source: {len(src.splitlines())} lines, files: {files}")

        # 3. Patch
        r.section("3. Patch source for S3 dual-write")
        src_patched = patch_source_for_dual_write(src)
        n_legacy = src_patched.count("Key='data/khalid-") + src_patched.count('Key="data/khalid-')
        n_new = src_patched.count("Key='data/ka-") + src_patched.count('Key="data/ka-')
        r.log(f"  legacy writes: {n_legacy}  new ka writes: {n_new}")
        if n_new < 3:
            r.warn(f"  ⚠ only {n_new} ka write sites patched, expected 3+")
            abort, abort_reason = True, f"patch produced only {n_new} ka writes"

    if not abort:
        # 4. Build zip
        r.section("4. Build new Lambda zip")
        new_zip_buf = io.BytesIO()
        with zipfile.ZipFile(new_zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("lambda_function.py", src_patched)
        new_zip_bytes = new_zip_buf.getvalue()
        r.log(f"  new zip: {len(new_zip_bytes)}B")

        # 5. Create Lambda
        r.section(f"5. Create {NEW}")
        lam.create_function(
            FunctionName=NEW,
            Runtime=old_cfg["Runtime"],
            Role=old_cfg["Role"],
            Handler=old_cfg["Handler"],
            Code={"ZipFile": new_zip_bytes},
            Timeout=old_cfg["Timeout"],
            MemorySize=old_cfg["MemorySize"],
            Environment={"Variables": old_cfg.get("Environment", {}).get("Variables", {})},
            Architectures=old_cfg.get("Architectures", ["x86_64"]),
            Description=f"KA Metrics — Phase 4 of Khalid→KA rebrand (replaces {OLD})",
        )
        r.log(f"  ✅ created")
        time.sleep(3)
        lam.get_waiter("function_active_v2").wait(FunctionName=NEW)
        r.log(f"  ✅ state=Active")

        # 6. Function URL
        r.section(f"6. Create Function URL for {NEW}")
        url_resp = lam.create_function_url_config(
            FunctionName=NEW,
            AuthType="NONE",
            Cors={
                "AllowOrigins": ["*"],
                "AllowMethods": ["GET", "POST", "OPTIONS"],
                "AllowHeaders": ["*"],
                "MaxAge": 86400,
            },
        )
        new_url = url_resp["FunctionUrl"]
        r.log(f"  ✅ {new_url}")

        try:
            lam.add_permission(
                FunctionName=NEW,
                StatementId="FunctionURLAllowPublicAccess",
                Action="lambda:InvokeFunctionUrl",
                Principal="*",
                FunctionUrlAuthType="NONE",
            )
            r.log(f"  ✅ public invoke perm added")
        except ClientError as e:
            r.warn(f"  perm: {e}")

        # 7. Test-invoke
        r.section(f"7. Test-invoke {NEW}")
        t0 = time.time()
        inv = lam.invoke(FunctionName=NEW, InvocationType="RequestResponse",
                         Payload=json.dumps({}))
        elapsed = time.time() - t0
        err = inv.get("FunctionError")
        payload = inv["Payload"].read().decode("utf-8", errors="replace")[:300]
        if err:
            r.warn(f"  ✗ err={err} ({elapsed:.1f}s)")
            r.warn(f"  payload: {payload}")
        else:
            r.log(f"  ✅ OK ({elapsed:.1f}s)")
            r.log(f"  payload: {payload}")

        time.sleep(5)

        # 8. Verify both file sets
        r.section("8. Verify dual-write")
        keys = [
            "data/ka-metrics.json", "data/ka-config.json", "data/ka-analysis.json",
            "data/khalid-metrics.json", "data/khalid-config.json", "data/khalid-analysis.json",
        ]
        now = datetime.now(timezone.utc)
        n_fresh = 0
        for k in keys:
            try:
                obj = s3.get_object(Bucket=BUCKET, Key=k)
                age = (now - obj["LastModified"]).total_seconds()
                mark = "✅ FRESH" if age < 60 else "⏰ stale"
                r.log(f"  {mark} {k}  size={obj['ContentLength']}B  age={age:.0f}s")
                if age < 120: n_fresh += 1
            except ClientError as e:
                if "NoSuchKey" in str(e):
                    r.warn(f"  ✗ MISSING {k}")
                else:
                    r.warn(f"  ✗ {k}: {e}")
        r.log(f"\n  {n_fresh}/{len(keys)} keys fresh (<2 min)")

        # 9. EventBridge target migration
        r.section(f"9. Move {RULE} target old → new")
        new_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{NEW}"
        try:
            targets = events.list_targets_by_rule(Rule=RULE).get("Targets", [])
            r.log(f"  current: {[t['Arn'].split(':')[-1] for t in targets]}")

            try:
                lam.add_permission(
                    FunctionName=NEW,
                    StatementId="EventBridgeInvoke",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE}",
                )
                r.log(f"  ✅ EventBridge invoke perm granted")
            except ClientError as e:
                if "ResourceConflictException" in str(e):
                    r.log(f"  ✅ EventBridge invoke perm already exists")
                else:
                    r.warn(f"  ⚠ perm: {e}")

            new_targets = [{**t, "Arn": new_arn} for t in targets]
            resp = events.put_targets(Rule=RULE, Targets=new_targets)
            failed = resp.get("FailedEntryCount", 0)
            if failed:
                r.warn(f"  ⚠ {failed} failed: {resp.get('FailedEntries')}")
            else:
                r.log(f"  ✅ rule now targets {NEW}")
            verify = events.list_targets_by_rule(Rule=RULE).get("Targets", [])
            r.log(f"  verified: {[t['Arn'].split(':')[-1] for t in verify]}")
        except Exception as e:
            r.warn(f"  ⚠ EB cutover: {e}")

        # 10. Summary
        r.section("FINAL")
        r.log(f"  Old: {OLD} (alive, EventBridge no longer triggers)")
        r.log(f"  New: {NEW}")
        r.log(f"  New URL: {new_url}")
        r.log(f"  EventBridge {RULE} → {NEW}")
        r.log("")
        r.log(f"  Step 217 will repoint ka/index.html to new URL + data/ka-*.json")
        r.log(f"  Phase 4b (7-day grace): delete {OLD} + its Function URL")

    r.log("Done")
