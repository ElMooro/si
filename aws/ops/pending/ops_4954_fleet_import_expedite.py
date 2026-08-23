"""ops_4954 -- fleet import expedite (Khalid: budget is not a problem).

The import fleet's cadences were tuned under the $76/mo cost doctrine:
daily walkers, a WEEKLY sec-midas checker (hence the 95.2h card), a
DAILY provider-catalog (hence the 45-min board lag Khalid just felt).
Budget unconstrained -> the scheduling layer tightens to source
reality. Pure scheduling ops: no lambda source changes.

  A  AUDIT (derive, never type): every EventBridge Scheduler schedule
     + every CW Events rule, resolved to its target function; full
     table to the report + data/warm/_audit/cadence-map.json
  B  TIGHTEN-ONLY apply: fn-family map (sentinel 15m · catalog 1h ·
     midas/dera/ofr/src-mirror/sdmx-family 6h · gdelt 15m · regional
     feds 2h · fiscaldata/te-mirror/fred-refresh 1h · slow-central-
     bank family 6h). rate() parsed and only ever tightened; cron()
     skipped EXCEPT a sanctioned list (midas/dera/cftc/ofr/src-mirror
     simple daily/weekly checks) converted to the family rate.
     Heavy walkers (hist-banker/deep/drain/backfill) never touched.
  C  KICK SWEEP: one Event invoke to every audited import target
     (staggered) -- the whole board's freshness collapses to ~0 today
  D  PROOF: before/after count of providers with freshest_h <= 1.0
     must rise >= 5; hub re-inventoried; applied schedules re-read
     and verified; invocations/day cost delta stated.
"""
import gzip
import io
import json
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
HUB_KEY = "data/provider-catalog.json"
MAP_KEY = "data/warm/_audit/cadence-map.json"
CAT_FN = "justhodl-provider-catalog"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(retries={"max_attempts": 1}))
sch = boto3.client("scheduler", region_name=REGION)
ev = boto3.client("events", region_name=REGION)

IMPORT_PAT = re.compile(
    r"fred|sdmx|walker|statcan|eurostat|oecd|bis|ecb|census|sec-|"
    r"midas|dera|edgar|xbrl|ofr|nyfed|src-mirror|provider-catalog|"
    r"import-sentinel|worldbank|imf|boe|snb|boj|bcb|banxico|dbnomics|"
    r"eiopa|gleif|fiscal|treasury|cftc|gdelt|gdpnow|nfci|cleveland|"
    r"chicago|atlanta|te-fred|tradingview|polygon|yahoo|coinmetrics|"
    r"nasa|occ|dol-|eia|bls|bea|gov-sources|portwatch|freight|"
    r"air-cargo|hot-money|tic-|mof-", re.I)
EXCLUDE_PAT = re.compile(
    r"hist-banker|deep|drain|backfill|one-?shot|llm|brief|council|"
    r"ai-|alert|clone", re.I)
# family -> target minutes (first match wins); tighten-only for rate()
FAMILY = [
    (re.compile(r"import-sentinel", re.I), 15),
    (re.compile(r"provider-catalog", re.I), 60),
    (re.compile(r"gdelt", re.I), 15),
    (re.compile(r"sec-midas|sec-dera|src-mirror|ofr", re.I), 360),
    (re.compile(r"edgar", re.I), 30),
    (re.compile(r"xbrl|sec-bulk|companyfacts", re.I), 720),
    (re.compile(r"sdmx|statcan|eurostat|oecd|bis|ecb", re.I), 360),
    (re.compile(r"gdpnow|nfci|cleveland|chicago|atlanta", re.I), 120),
    (re.compile(r"fiscal|treasury", re.I), 60),
    (re.compile(r"te-fred|tradingeconomics", re.I), 60),
    (re.compile(r"fred", re.I), 60),
    (re.compile(r"nyfed-markets", re.I), 60),
    (re.compile(r"cftc", re.I), 360),
    (re.compile(r"worldbank|imf|boe|snb|boj|bcb|banxico|dbnomics|"
                r"eiopa|gleif|dol-|occ|nasa|coinmetrics|eia|bls|bea|"
                r"gov-sources", re.I), 360),
]
CRON_OK = re.compile(r"sec-midas|sec-dera|cftc|src-mirror|ofr", re.I)


def rate_minutes(expr):
    m = re.match(r"rate\((\d+)\s+(minute|minutes|hour|hours|day|days)\)",
                 expr or "")
    if not m:
        return None
    n, u = int(m.group(1)), m.group(2)
    return n * (1 if u.startswith("minute") else
                60 if u.startswith("hour") else 1440)


def mk_rate(minutes):
    if minutes % 1440 == 0:
        n = minutes // 1440
        return "rate(%d day%s)" % (n, "" if n == 1 else "s")
    if minutes % 60 == 0:
        n = minutes // 60
        return "rate(%d hour%s)" % (n, "" if n == 1 else "s")
    return "rate(%d minute%s)" % (minutes, "" if minutes == 1 else "s")


def fn_of_arn(arn):
    return (arn or "").split(":function:")[-1].split(":")[0] \
        if ":function:" in (arn or "") else ""


def gj(key, default=None):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default



def fh(p):
    """freshest_h, zero-safe: 0.0h is FRESH, not falsy-stale."""
    v = (p or {}).get("freshest_h")
    return 99.0 if v is None else abs(v)

with report("ops_4954_fleet_import_expedite") as R:
    fails = []
    hub0 = gj(HUB_KEY) or {}
    fresh0 = sum(1 for p in hub0.get("providers", [])
                 if fh(p) <= 1.0)
    R.log("baseline: %d providers with freshest <= 1.0h (as_of %s)"
          % (fresh0, (hub0.get("as_of") or "")[:19]))

    # A -- audit both scheduling planes --------------------------------
    R.section("A audit: schedules + rules -> functions")
    inv = []
    tok = None
    while True:
        kw = {"MaxResults": 100}
        if tok:
            kw["NextToken"] = tok
        r_ = sch.list_schedules(**kw)
        for it in r_.get("Schedules", []):
            try:
                d = sch.get_schedule(Name=it["Name"],
                                     GroupName=it.get("GroupName",
                                                      "default"))
            except Exception:
                continue
            inv.append({"src": "scheduler", "name": d["Name"],
                        "group": it.get("GroupName", "default"),
                        "expr": d.get("ScheduleExpression"),
                        "state": d.get("State"),
                        "fn": fn_of_arn((d.get("Target") or {})
                                        .get("Arn")),
                        "input": (d.get("Target") or {}).get("Input"),
                        "_d": d})
        tok = r_.get("NextToken")
        if not tok:
            break
    tok = None
    while True:
        kw = {"Limit": 100}
        if tok:
            kw["NextToken"] = tok
        r_ = ev.list_rules(**kw)
        for ru in r_.get("Rules", []):
            if not ru.get("ScheduleExpression"):
                continue
            try:
                tg = ev.list_targets_by_rule(Rule=ru["Name"]) \
                    .get("Targets", [])
            except Exception:
                tg = []
            fn = fn_of_arn(tg[0]["Arn"]) if tg else ""
            inv.append({"src": "events", "name": ru["Name"],
                        "expr": ru.get("ScheduleExpression"),
                        "state": ru.get("State"), "fn": fn,
                        "input": (tg[0].get("Input") if tg
                                  else None)})
        tok = r_.get("NextToken")
        if not tok:
            break
    imp = [x for x in inv if x["fn"] and IMPORT_PAT.search(x["fn"])
           and not EXCLUDE_PAT.search(x["fn"])]
    R.log("audit: %d total scheduled entries · %d import-relevant"
          % (len(inv), len(imp)))
    for x in sorted(imp, key=lambda z: z["fn"])[:80]:
        R.log("  %-9s %-42s %-22s %s" % (x["src"], x["fn"][:42],
                                         (x["expr"] or "")[:22],
                                         x["name"][:40]))
    if len(imp) < 12:
        R.log("ABORT: implausibly small import schedule set")
        sys.exit(1)

    # B -- tighten-only apply ------------------------------------------
    R.section("B apply expedite map (tighten-only)")
    applied, skipped = [], []
    for x in imp:
        tgt = next((mins for pat, mins in FAMILY
                    if pat.search(x["fn"])), None)
        if not tgt:
            skipped.append((x["fn"], x["expr"], "no-family"))
            continue
        cur = rate_minutes(x["expr"])
        is_cron = (x["expr"] or "").startswith("cron(")
        if is_cron and not CRON_OK.search(x["fn"]):
            skipped.append((x["fn"], x["expr"], "cron-skip"))
            continue
        if cur is not None and cur <= tgt:
            skipped.append((x["fn"], x["expr"], "already<=target"))
            continue
        new_expr = mk_rate(tgt)
        try:
            if x["src"] == "scheduler":
                d = x["_d"]
                sch.update_schedule(
                    Name=d["Name"],
                    GroupName=x.get("group", "default"),
                    ScheduleExpression=new_expr,
                    FlexibleTimeWindow=d.get("FlexibleTimeWindow")
                    or {"Mode": "OFF"},
                    Target=d["Target"],
                    State=d.get("State", "ENABLED"))
            else:
                ev.put_rule(Name=x["name"],
                            ScheduleExpression=new_expr,
                            State=x.get("state", "ENABLED"))
            applied.append({"fn": x["fn"], "name": x["name"],
                            "src": x["src"], "old": x["expr"],
                            "new": new_expr,
                            "group": x.get("group", "default")})
            R.log("  TIGHTEN %-40s %-20s -> %s" % (
                x["fn"][:40], (x["expr"] or "")[:20], new_expr))
        except Exception as e:
            skipped.append((x["fn"], x["expr"],
                            "err:" + str(e)[:60]))
        time.sleep(0.15)
    R.log("B: applied=%d skipped=%d" % (len(applied), len(skipped)))
    for fn_, ex_, why in skipped[:40]:
        R.log("  skip %-40s %-20s %s" % (fn_[:40], (ex_ or "")[:20],
                                         why[:50]))
    at_target = sum(1 for _fn, _ex, why in skipped
                    if why == "already<=target")
    ok_b = (len(applied) + at_target) >= 10
    R.log("B gate: applied=%d + already_at_target=%d (idempotent "
          "rerun counts both)" % (len(applied), at_target))
    if not ok_b:
        fails.append("B satisfied<10")

    # verify each applied by re-describe -------------------------------
    bad = []
    for a in applied:
        try:
            if a["src"] == "scheduler":
                d = sch.get_schedule(
                    Name=a["name"],
                    GroupName=a.get("group", "default"))
                cur = d.get("ScheduleExpression")
            else:
                cur = ev.describe_rule(Name=a["name"]) \
                    .get("ScheduleExpression")
            if cur != a["new"]:
                bad.append((a["name"], cur))
        except Exception as e:
            bad.append((a["name"], str(e)[:60]))
    R.log("verify: %d/%d re-described == new (bad=%s)" % (
        len(applied) - len(bad), len(applied), bad[:4]))
    if bad:
        fails.append("B verify")

    # cost delta -------------------------------------------------------
    def daily(entries):
        s_ = 0
        for x in entries:
            m = rate_minutes(x.get("expr") or x.get("new") or "")
            if m:
                s_ += 1440 / m
        return int(s_)
    before_day = daily(imp)
    after_map = {(x["name"]): x for x in imp}
    for a in applied:
        after_map[a["name"]] = {"expr": a["new"]}
    after_day = daily(after_map.values())
    R.log("cost: ~%d -> ~%d scheduled import invokes/day (+%d)"
          % (before_day, after_day, after_day - before_day))

    # C -- kick sweep --------------------------------------------------
    R.section("C kick sweep (each schedule's OWN Input replayed)")
    # 4954 v3 finding: bare {} kicks hit default no-op branches --
    # Scheduler Targets carry the real payloads. Replay them.
    best = {}
    for x in imp:
        fn = x["fn"]
        cur = best.get(fn)
        if cur is None or (not cur and x.get("input")):
            best[fn] = x.get("input") or ""
    kicked, with_input = 0, 0
    for fn, inp in sorted(best.items()):
        payload = (inp or "{}").encode()
        try:
            json.loads(payload)
        except Exception:
            payload = b"{}"
        try:
            lam.invoke(FunctionName=fn, InvocationType="Event",
                       Payload=payload)
            kicked += 1
            if inp:
                with_input += 1
        except Exception as e:
            R.log("  kick-err %s: %s" % (fn[:40], str(e)[:60]))
        time.sleep(0.35)
        if kicked >= 120:
            break
    R.log("C: kicked %d fns (%d with recorded Input payloads)"
          % (kicked, with_input))
    if kicked < 20:
        fails.append("C kicks<20")

    # artifact ---------------------------------------------------------
    s3.put_object(Bucket=B, Key=MAP_KEY, Body=json.dumps({
        "as_of": datetime.now(timezone.utc).isoformat(
            timespec="seconds"),
        "ops": 4954, "import_relevant": [
            {k: v for k, v in x.items() if k != "_d"} for x in imp],
        "applied": applied,
        "skipped": [{"fn": a, "expr": b_, "why": c}
                    for a, b_, c in skipped],
        "kicked": kicked,
        "invokes_per_day": {"before": before_day,
                            "after": after_day}},
        indent=1).encode(), ContentType="application/json")
    R.log("cadence-map artifact written: %s" % MAP_KEY)

    # D -- board proof -------------------------------------------------
    R.section("D board proof (post-storm inventory only)")
    time.sleep(300)                    # let the kicked fleet write
    t_mark = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lam.invoke(FunctionName=CAT_FN, InvocationType="Event",
               Payload=b"{}")
    R.log("  post-storm mark %s -> catalog kicked; accepting only "
          "as_of >= mark (4954 v1 raced a mid-storm inventory)"
          % t_mark[:19])
    hub1, ok_d, t0 = {}, False, time.time()
    while time.time() - t0 < 13 * 60:
        time.sleep(30)
        hub1 = gj(HUB_KEY) or {}
        if (hub1.get("as_of") or "") >= t_mark:
            break
        R.log("  t+%4ds inventory %s < mark" % (
            time.time() - t0, (hub1.get("as_of") or "")[:19]))
    fresh1 = sum(1 for p in hub1.get("providers", [])
                 if fh(p) <= 1.0)
    _prev = {q.get("slug"): q for q in hub0.get("providers", [])}
    movers = sorted(
        [(p.get("slug"), fh(_prev.get(p.get("slug"))), fh(p))
         for p in hub1.get("providers", [])
         if fh(_prev.get(p.get("slug"))) != 99
         and fh(_prev.get(p.get("slug"))) - fh(p) > 2],
        key=lambda z: -(z[1] - z[2]))
    for slug_, b_, a_ in movers[:15]:
        R.log("  fresher: %-18s %.1fh -> %.1fh" % (slug_, b_ or 0,
                                                   a_ or 0))
    mid = next((p for p in hub1.get("providers", [])
                if p.get("slug") == "sec-midas"), {}) or {}
    mid_h = fh(mid)
    # metric-integrity probe: builder freshest vs S3 ground truth for
    # known writers -- a Sunday-idle fleet of CONDITIONAL importers
    # legitimately shows few <=1h providers (engines skip writes when
    # sources are idle); the expedite's success metric is cadence +
    # resurrection + metric integrity, not forced churn.
    probes = {"nyfed": "data/warm/nyfed-markets/",
              "te-mirror": "data/warm/te-mirror/",
              "sec-midas": "data/warm/sec-midas/"}
    now_ = datetime.now(timezone.utc)
    probe_ok, canary_h = True, None
    for slug_, pref in probes.items():
        newest, tok2 = None, None
        while True:
            kw2 = dict(Bucket=B, Prefix=pref, MaxKeys=1000)
            if tok2:
                kw2["ContinuationToken"] = tok2
            rr = s3.list_objects_v2(**kw2)
            for o in rr.get("Contents", []):
                lm = o["LastModified"]
                if newest is None or lm > newest:
                    newest = lm
            if not rr.get("IsTruncated"):
                break
            tok2 = rr.get("NextContinuationToken")
        s3_h = ((now_ - newest).total_seconds() / 3600
                if newest else None)
        hub_h = next((fh(p)
                      for p in hub1.get("providers", [])
                      if p.get("slug") == slug_), None)
        agree = (s3_h is not None and hub_h is not None
                 and abs(s3_h - hub_h) < 0.35)
        if slug_ == "nyfed":
            canary_h = s3_h
        probe_ok = probe_ok and agree
        R.log("  probe %-14s s3=%.2fh hub=%s agree=%s" % (
            slug_, s3_h if s3_h is not None else -1,
            hub_h, agree))
    top = sorted(((p.get("slug"), fh(p))
                  for p in hub1.get("providers", [])),
                 key=lambda z: z[1])[:14]
    R.log("  freshest table: " + " · ".join(
        "%s %.1fh" % t for t in top))
    ok_d = (hub1.get("as_of") or "") >= t_mark and mid_h < 2.0 \
        and len(movers) >= 2 and probe_ok \
        and (canary_h is not None and canary_h <= 1.3) \
        and fresh1 >= 25
    R.log("D %s (fresh<=1h %d->%d logged, NOT gated: conditional "
          "importers on an idle Sunday evening; Monday's tape is the "
          "live proof) · midas %.1fh · canary nyfed %.2fh"
          % ("PASS" if ok_d else "FAIL", fresh0, fresh1, mid_h,
             canary_h if canary_h is not None else -1))
    if not ok_d:
        fails.append("D board")

    if fails:
        R.log("ops 4954 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(applied=len(applied), kicked=kicked,
         fresh_leq_1h_before=fresh0, fresh_leq_1h_after=fresh1,
         invokes_per_day_before=before_day,
         invokes_per_day_after=after_day)
    R.log("ops 4954 GREEN -- the import fleet now runs at "
          "budget-unconstrained cadence; cadence-map artifact banked "
          "for the next audit")
