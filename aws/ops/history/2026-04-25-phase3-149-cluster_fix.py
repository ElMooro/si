#!/usr/bin/env python3
"""
Step 149 — Fix risk-sizer: sector-based fallback clustering + create
the missing EventBridge schedule that step 147 failed to create.

TWO ISSUES SURFACED IN STEP 148 VERIFICATION:

  1. CLUSTERING DEGENERATE. All 17 ideas became 'isolated_X' clusters
     because data/report.json's stocks field contains only ~80 ETFs/
     major names — INCY/FSLR/RMD/etc are not in there. The correlation
     fallback was: 'no return data → put in own cluster.' Result: no
     clustering ever happens for Phase 2B output (which is mostly
     mid/small-cap names).

     FIX: when return correlation isn\\'t available, fall back to SECTOR
     clustering. INCY and RMD both = Healthcare → same cluster. Less
     precise than 60-day return correlation but far better than treating
     all 17 as 1-stock clusters and missing the obvious 'this is a
     defensive trade' concentration risk.

  2. EVENTBRIDGE RULE MISSING. Step 147 errored at the test invoke step,
     before reaching schedule creation. Lambda was deployed (the GitHub
     Actions deploy-lambdas.yml workflow caught the source change and
     deployed it) but the rule never got made.

This step:
  - Reads the deployed source from aws/lambdas/justhodl-risk-sizer/source/
  - Patches cluster_by_correlation to fall back to sector
  - Adds sector lookup from the screener output
  - Re-deploys
  - Creates the cron(45 13 ? * MON-FRI *) rule
  - Test invokes
"""
import io
import json
import os
import time
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)

BUCKET = "justhodl-dashboard-live"
FNAME = "justhodl-risk-sizer"


with report("fix_risk_sizer_clustering") as r:
    r.heading("Fix risk-sizer — sector fallback clustering + schedule")

    # ─── 1. Read current source ────────────────────────────────────────
    r.section("1. Read current source from disk")
    src_path = REPO_ROOT / "aws/lambdas/justhodl-risk-sizer/source/lambda_function.py"
    src = src_path.read_text()
    r.log(f"  Current source: {len(src):,}B, {src.count(chr(10))} LOC")

    # ─── 2. Apply patch — replace cluster_by_correlation function ──────
    r.section("2. Patch cluster_by_correlation to use sector fallback")

    OLD_FUNC = '''def cluster_by_correlation(symbols, returns_by_symbol, threshold=CLUSTER_CORRELATION_THRESHOLD):
    """Greedy clustering: for each symbol, find others with corr > threshold."""
    clusters = []
    assigned = set()

    for sym in symbols:
        if sym in assigned:
            continue
        if sym not in returns_by_symbol:
            # Symbol with no return data goes alone (defensive)
            clusters.append({"id": f"isolated_{sym}", "members": [sym], "avg_correlation": 0, "size": 1})
            assigned.add(sym)
            continue
        cluster_members = [sym]
        cluster_corrs = []
        for other in symbols:
            if other == sym or other in assigned or other not in returns_by_symbol:
                continue
            corr = correlation(returns_by_symbol[sym], returns_by_symbol[other])
            if corr is not None and corr > threshold:
                cluster_members.append(other)
                cluster_corrs.append(corr)
        for m in cluster_members:
            assigned.add(m)
        clusters.append({
            "id": f"cluster_{cluster_members[0]}",
            "members": sorted(cluster_members),
            "avg_correlation": round(statistics.mean(cluster_corrs), 3) if cluster_corrs else 0,
            "size": len(cluster_members),
        })

    return clusters'''

    NEW_FUNC = '''def cluster_by_correlation(symbols, returns_by_symbol, sector_by_symbol=None, threshold=CLUSTER_CORRELATION_THRESHOLD):
    """Cluster symbols by 60-day return correlation when data exists, else by sector.

    For symbols with sufficient return data we use the original 0.65 correlation
    threshold (precise but data-dependent). For symbols WITHOUT return data —
    most Phase 2B output, since data/report.json only carries ~80 major ETFs/
    names and the screener has 503 — we fall back to sector grouping. INCY +
    RMD = Healthcare → 1 cluster, etc. Less precise but vastly better than
    treating every name as its own cluster (which makes per-cluster caps useless).
    """
    if sector_by_symbol is None:
        sector_by_symbol = {}
    clusters = []
    assigned = set()

    # Pass 1: correlation-based clusters for symbols with return data
    for sym in symbols:
        if sym in assigned or sym not in returns_by_symbol:
            continue
        cluster_members = [sym]
        cluster_corrs = []
        for other in symbols:
            if other == sym or other in assigned or other not in returns_by_symbol:
                continue
            corr = correlation(returns_by_symbol[sym], returns_by_symbol[other])
            if corr is not None and corr > threshold:
                cluster_members.append(other)
                cluster_corrs.append(corr)
        for m in cluster_members:
            assigned.add(m)
        clusters.append({
            "id": f"corr_{cluster_members[0]}",
            "method": "correlation",
            "members": sorted(cluster_members),
            "avg_correlation": round(statistics.mean(cluster_corrs), 3) if cluster_corrs else 0,
            "size": len(cluster_members),
        })

    # Pass 2: sector-based clusters for the remainder
    by_sector = {}
    for sym in symbols:
        if sym in assigned:
            continue
        sector = sector_by_symbol.get(sym, "Unknown")
        by_sector.setdefault(sector, []).append(sym)

    for sector, members in by_sector.items():
        if len(members) == 1:
            # Single-member sector → still mark as sector cluster, not isolated
            clusters.append({
                "id": f"sector_{sector.lower().replace(' ', '_')}",
                "method": "sector_single",
                "members": members,
                "avg_correlation": 0,
                "size": 1,
                "sector": sector,
            })
        else:
            clusters.append({
                "id": f"sector_{sector.lower().replace(' ', '_')}",
                "method": "sector",
                "members": sorted(members),
                "avg_correlation": 0,  # sector grouping doesn\\'t compute corr
                "size": len(members),
                "sector": sector,
            })
        for m in members:
            assigned.add(m)

    return clusters'''

    if OLD_FUNC in src:
        src = src.replace(OLD_FUNC, NEW_FUNC)
        r.ok("  Replaced cluster_by_correlation with sector-fallback version")
    elif "method\": \"sector\"" in src:
        r.log("  Already patched")
    else:
        r.fail("  Couldn't find cluster_by_correlation anchor — manual investigation")
        raise SystemExit(1)

    # ─── 3. Patch the lambda_handler call site to pass sector mapping ───
    r.section("3. Patch lambda_handler to pass sector_by_symbol")

    OLD_CALL = '''    clusters = cluster_by_correlation(list(ideas.keys()), returns_by_symbol)'''
    NEW_CALL = '''    sector_by_symbol = {sym: idea.get("sector") for sym, idea in ideas.items()}
    clusters = cluster_by_correlation(list(ideas.keys()), returns_by_symbol, sector_by_symbol)'''

    if OLD_CALL in src:
        src = src.replace(OLD_CALL, NEW_CALL)
        r.ok("  Patched call site to pass sector_by_symbol")
    elif "sector_by_symbol" in src:
        r.log("  Call site already patched")
    else:
        r.fail("  Couldn't find call site")
        raise SystemExit(1)

    # ─── 4. Validate + write ────────────────────────────────────────────
    r.section("4. Validate + write fixed source")
    import ast
    try:
        ast.parse(src)
        r.ok(f"  Syntax OK; new size: {len(src):,}B")
    except SyntaxError as e:
        r.fail(f"  Syntax: {e}")
        if hasattr(e, "lineno"):
            lines = src.split("\n")
            for i in range(max(0, e.lineno - 3), min(len(lines), e.lineno + 3)):
                marker = " >>> " if i == e.lineno - 1 else "     "
                r.log(f"  {marker}L{i+1}: {lines[i][:200]}")
        raise SystemExit(1)
    src_path.write_text(src)

    # ─── 5. Deploy ──────────────────────────────────────────────────────
    r.section("5. Deploy fixed Lambda")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        info = zipfile.ZipInfo("lambda_function.py")
        info.external_attr = 0o644 << 16
        zout.writestr(info, src)
    zbytes = buf.getvalue()
    lam.update_function_code(
        FunctionName=FNAME, ZipFile=zbytes, Architectures=["arm64"],
    )
    lam.get_waiter("function_updated").wait(
        FunctionName=FNAME, WaiterConfig={"Delay": 3, "MaxAttempts": 30},
    )
    r.ok(f"  Deployed ({len(zbytes):,}B)")

    # ─── 6. Test invoke ─────────────────────────────────────────────────
    r.section("6. Test invoke")
    time.sleep(3)
    invoke_start = time.time()
    resp = lam.invoke(FunctionName=FNAME, InvocationType="RequestResponse")
    elapsed = time.time() - invoke_start
    payload = resp.get("Payload").read().decode()
    if resp.get("FunctionError"):
        r.fail(f"  FunctionError ({elapsed:.1f}s): {payload[:1000]}")
        raise SystemExit(1)
    r.ok(f"  Invoked in {elapsed:.1f}s")

    outer = json.loads(payload)
    body = json.loads(outer.get("body", "{}"))
    r.log(f"  Response body:")
    for k, v in body.items():
        r.log(f"    {k:25} {v}")

    # ─── 7. Read full output to confirm sector clustering working ──────
    r.section("7. Verify sector clustering is producing meaningful clusters")
    obj = s3.get_object(Bucket=BUCKET, Key="risk/recommendations.json")
    snap = json.loads(obj["Body"].read().decode("utf-8"))

    r.log(f"  Total clusters: {len(snap.get('clusters', []))}")
    r.log(f"\n  Cluster breakdown:")
    multi_member_clusters = 0
    for c in snap.get("clusters", []):
        if c["size"] > 1:
            multi_member_clusters += 1
            r.log(f"    {c['id']:30} size={c['size']:>2} method={c.get('method','?'):12} "
                  f"members={c['members']}")
    if multi_member_clusters:
        r.ok(f"\n  ✅ {multi_member_clusters} multi-member clusters formed (sector-based)")
    else:
        r.warn(f"\n  ⚠ No multi-member clusters — every idea is solo")

    r.log(f"\n  Sized recommendations w/ cluster info:")
    for rec in snap.get("sized_recommendations", [])[:10]:
        r.log(f"    {rec['symbol']:6} sector={rec.get('sector','?')[:14]:14} "
              f"size={rec.get('recommended_size_pct'):>5}%  "
              f"cluster={rec.get('cluster','?')[:25]:25}")

    # ─── 8. Create the missing EventBridge rule ─────────────────────────
    r.section("8. Create EventBridge schedule (missing from step 147)")
    rule_name = "justhodl-risk-sizer-daily"
    try:
        existing = events.describe_rule(Name=rule_name)
        r.log(f"  Rule already exists: {existing['State']} {existing['ScheduleExpression']}")
    except events.exceptions.ResourceNotFoundException:
        events.put_rule(
            Name=rule_name,
            ScheduleExpression="cron(45 13 ? * MON-FRI *)",
            State="ENABLED",
            Description="Phase 3 — daily risk sizing after Phase 2B at 13:30 UTC",
        )
        r.ok(f"  Created rule cron(45 13 ? * MON-FRI *)")

    events.put_targets(
        Rule=rule_name,
        Targets=[{"Id": "1",
                  "Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{FNAME}"}],
    )
    try:
        lam.add_permission(
            FunctionName=FNAME,
            StatementId=f"{rule_name}-invoke",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rule_name}",
        )
        r.ok(f"  Added invoke permission")
    except lam.exceptions.ResourceConflictException:
        r.log(f"  Invoke permission already exists")

    r.kv(
        zip_size=len(zbytes),
        invoke_s=f"{elapsed:.1f}",
        n_clusters=body.get("n_clusters"),
        multi_member_clusters=multi_member_clusters,
        total_size_pct=body.get("total_size_pct"),
        regime=body.get("regime"),
    )
    r.log("Done")
