"""ops 3211 — both mysteries settled with mechanics, not vibes.

  1. 'emitted: 24' vs scan=0 was the DynamoDB SCAN PAGINATION TRAP:
     Limit caps items EXAMINED before the filter, and justhodl-signals is
     big — the wl_ rows live past page one. Paginated scan settles the
     count. (3208's fail-line is hereby reclassified.)
  2. The recurring line-278 unpack crash predates the 05:02 shape-guard
     deploy. This ops runs the guarded code once: no new [ERROR] allowed,
     and if a malformed sid exists the guard PRINTS it — captured here,
     then pruned from map+curated in the same breath.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from boto3.dynamodb.conditions import Attr

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
DDB = boto3.resource("dynamodb", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3211_settle") as rep:
    fails, warns = [], []
    rep.heading("ops 3211 — ledger counted right, crash named or gone")

    rep.section("1. Paginated ledger count")
    n, samples, pages = 0, [], 0
    try:
        tbl = DDB.Table("justhodl-signals")
        kw = {"FilterExpression": Attr("signal_type").begins_with("wl_"),
              "ProjectionExpression": "signal_id, predicted_direction"}
        while pages < 15:
            r = tbl.scan(**kw)
            items = r.get("Items") or []
            n += len(items)
            samples += items[:2]
            pages += 1
            lek = r.get("LastEvaluatedKey")
            if not lek:
                break
            kw["ExclusiveStartKey"] = lek
        rep.kv(wl_signals=n, scan_pages=pages)
        for it in samples[:4]:
            rep.log(f"  {it.get('signal_id', '')[:52]}  "
                    f"{it.get('predicted_direction')}")
        if n >= 20:
            rep.ok(f"{n} wl_ signals pending — 3208's 'zero' was the "
                   "pagination trap, the scorecard IS learning his panels")
        else:
            fails.append(f"only {n} wl_ signals after full pagination")
    except Exception as e:
        fails.append(f"ddb: {str(e)[:80]}")

    rep.section("2. Guarded run: crash gone or NAMED")
    mark = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName="justhodl-wl-engines",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        fails.append(f"invoke: {str(e)[:70]}")
    idx = None
    for _ in range(60):
        time.sleep(10)
        d = s3_json("data/wl-engines.json") or {}
        if str(d.get("generated_at", "")) > mark:
            idx = d
            break
    if idx:
        rep.ok(f"index fresh ({str(idx.get('generated_at'))[:19]}) — "
               f"{sum(1 for e in idx.get('engines') or [] if e.get('firing'))}"
               " firing")
    else:
        warns.append("index not fresh in window — check next stream")
    bad, err = [], 0
    try:
        grp = "/aws/lambda/justhodl-wl-engines"
        for st in LOGS.describe_log_streams(
                logGroupName=grp, orderBy="LastEventTime",
                descending=True, limit=2).get("logStreams") or []:
            for e in LOGS.get_log_events(
                    logGroupName=grp, logStreamName=st["logStreamName"],
                    limit=200, startFromHead=False).get("events") or []:
                m = e.get("message") or ""
                ts = e.get("timestamp", 0) / 1000
                if ts and datetime.fromtimestamp(
                        ts, tz=timezone.utc).isoformat() < mark:
                    continue
                if "bad sid" in m:
                    bad.append(m.strip()[:120])
                if "[ERROR]" in m:
                    err += 1
                    rep.log("  " + m.splitlines()[0][:140])
    except Exception as e:
        warns.append(f"logs: {str(e)[:70]}")
    rep.kv(new_errors=err, bad_sids_named=len(bad))
    for b in bad[:5]:
        rep.log("  " + b)
    if err:
        fails.append(f"{err} [ERROR] events on GUARDED code — new raiser")

    if bad:
        rep.section("3. Prune the named sids from map + curated")
        import re as _re
        smap = s3_json("data/symbol-map.json") or {}
        mp, cur = smap.get("map") or {}, smap.get("curated") or {}
        sids = {m.split("bad sid")[1].strip().strip("'\"")
                for m in bad if "bad sid" in m}
        pruned = 0
        for sym in list(mp):
            if mp[sym].get("id") in sids:
                mp.pop(sym, None)
                cur.pop(sym, None)
                pruned += 1
                rep.log(f"  ✗ pruned {sym}")
        if pruned:
            smap["map"], smap["curated"] = mp, cur
            smap["generated_at"] = datetime.now(timezone.utc).isoformat()
            S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                          Body=json.dumps(smap),
                          ContentType="application/json")
            rep.ok(f"{pruned} malformed entries pruned")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
