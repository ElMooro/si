"""ops 2743 — INDEPENDENT VERIFICATION AUDIT (Khalid: verify everything yourself).

Read-only skeptical pass over every claim from ops 2740-2742. No deploys, no
writes except the report. Checks: (1) cq feed at PUBLIC DOMAIN strict-JSON w/
8 metrics + finite composite + freshness; (2) S3 history depth 8x>=365;
(3) onchain-ratios feed: cq block + resurrected + legacy body intact + fresh;
(4) crypto-exchange-flows: cq block + legacy intact; (5) crypto-miners:
cryptoquant_mpi + fresh; (6) EventBridge rules cq-daily + ratios-daily ENABLED
w/ correct crons AND lambda targets attached; (7) DynamoDB justhodl-signals:
today's onchain_composite_risk item physically present (paged filtered scan);
(8) engine-signal-map feed carries onchain family; (9) spot-check earlier arc:
flow-desk page v2 marker at edge + footprint ai_dossier obeys contract v4.
Report: aws/ops/reports/2743_onchain_audit.json.
"""
import os, json, time, urllib.request
from datetime import datetime, timezone
import boto3
from boto3.dynamodb.conditions import Attr

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
ev = boto3.client("events", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)
NOW = datetime.now(timezone.utc)
A = {"ops": 2743, "ts": NOW.isoformat(), "checks": {}}
FAILS = []
def check(name, ok, detail=""):
    A["checks"][name] = {"ok": bool(ok), "detail": str(detail)[:180]}
    print("  %s %-42s %s" % ("PASS" if ok else "FAIL", name, str(detail)[:110]))
    if not ok: FAILS.append(name)
def pub(path):
    req = urllib.request.Request("https://justhodl.ai/" + path + "?a=%d" % int(time.time()),
                                 headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return r.read()
def pubj(path):
    return json.loads(pub(path).decode(), parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))

print("settling 20s…"); time.sleep(20)
print("== AUDIT: cq adapter ==")
try:
    d = pubj("data/cryptoquant-onchain.json")
    check("cq.domain_strict_json", True, "edge fetch + strict parse")
    check("cq.status_live", d.get("status") == "LIVE", d.get("status"))
    M = d.get("metrics") or {}
    check("cq.eight_metrics", len(M) == 8, sorted(M))
    cz = d.get("composite_onchain_risk_z")
    check("cq.composite_finite", isinstance(cz, (int, float)), cz)
    check("cq.fresh", (d.get("max_staleness_days") or 99) <= 2, "staleness=%s" % d.get("max_staleness_days"))
    check("cq.no_errors", not d.get("errors"), d.get("errors"))
    zs = [k for k in M if M[k].get("z365") is None]
    check("cq.z_complete", not zs, zs)
except Exception as e:
    check("cq.domain_strict_json", False, e)
try:
    hist = json.loads(s3.get_object(Bucket=BUCKET, Key="data/history/cryptoquant.json")["Body"].read())
    depth = {k: len(v) for k, v in hist.items()}
    check("cq.history_depth", len(depth) == 8 and min(depth.values()) >= 365, depth)
except Exception as e:
    check("cq.history_depth", False, e)
try:
    spec = json.loads(s3.get_object(Bucket=BUCKET, Key="data/config/cryptoquant-spec.json")["Body"].read())
    check("cq.spec_plan_truths", spec.get("plan_window_days") == 365 and spec.get("from_format") == "none"
          and all(m.get("risk_sign") == 1 for m in spec["metrics"] if m["name"].endswith("_exchange_reserve") and not m["name"].startswith("stablecoin")),
          "window=%s fmt=%s" % (spec.get("plan_window_days"), spec.get("from_format")))
except Exception as e:
    check("cq.spec_plan_truths", False, e)

print("== AUDIT: consumers ==")
try:
    rd = pubj("data/onchain-ratios.json")
    cqb = rd.get("cryptoquant") or {}
    fresh = str(rd.get("generated_at", ""))[:10] == NOW.strftime("%Y-%m-%d")
    legacy = len([k for k in rd if k not in ("cryptoquant", "resurrected")]) >= 5
    check("ratios.cq_block", cqb.get("composite_onchain_risk_z") is not None and cqb.get("btc_mvrv"), list(cqb)[:5])
    check("ratios.resurrected_fresh_legacy", bool(rd.get("resurrected")) and fresh and legacy,
          "%s fresh=%s legacy_keys=%d" % (rd.get("resurrected"), fresh, len(rd)))
except Exception as e:
    check("ratios.cq_block", False, e)
try:
    xd = pubj("data/crypto-exchange-flows.json")
    check("xf.cq_block", (xd.get("cryptoquant") or {}).get("composite_onchain_risk_z") is not None
          and (xd["cryptoquant"].get("stablecoin_reserve") or {}).get("z365") is not None,
          "stablecoin z=%s" % (xd.get("cryptoquant", {}).get("stablecoin_reserve") or {}).get("z365"))
    check("xf.legacy_intact", len([k for k in xd if k != "cryptoquant"]) >= 4, len(xd))
except Exception as e:
    check("xf.cq_block", False, e)
try:
    md = pubj("data/crypto-miners.json")
    check("miners.mpi_block", (md.get("cryptoquant_mpi") or {}).get("value") is not None,
          md.get("cryptoquant_mpi"))
except Exception as e:
    check("miners.mpi_block", False, e)

print("== AUDIT: schedules ==")
for rule, expect in (("justhodl-cryptoquant-daily", "cron(5 21 * * ? *)"),
                     ("justhodl-onchain-ratios-daily", "cron(20 21 * * ? *)")):
    try:
        r = ev.describe_rule(Name=rule)
        tg = ev.list_targets_by_rule(Rule=rule).get("Targets", [])
        check("rule.%s" % rule, r["State"] == "ENABLED" and r["ScheduleExpression"] == expect and len(tg) >= 1,
              "%s %s targets=%d" % (r["State"], r["ScheduleExpression"], len(tg)))
    except Exception as e:
        check("rule.%s" % rule, False, e)

print("== AUDIT: signal ledger (Dynamo, physical row) ==")
try:
    tbl = ddb.Table("justhodl-signals")
    day0 = int(datetime(NOW.year, NOW.month, NOW.day, tzinfo=timezone.utc).timestamp())
    found, lek, pages = None, None, 0
    fe = Attr("signal_type").eq("onchain_composite_risk") & Attr("logged_epoch").gte(day0)
    while pages < 25 and not found:
        kw = {"FilterExpression": fe}
        if lek: kw["ExclusiveStartKey"] = lek
        resp = tbl.scan(**kw)
        items = resp.get("Items", [])
        if items: found = items[0]
        lek = resp.get("LastEvaluatedKey"); pages += 1
        if not lek: break
    check("ledger.onchain_row_today", bool(found),
          "pages=%d pred=%s conf=%s baseline=%s" % (pages, (found or {}).get("predicted_direction"),
          (found or {}).get("confidence"), (found or {}).get("baseline_price")))
except Exception as e:
    check("ledger.onchain_row_today", False, e)

print("== AUDIT: registry + earlier-arc spot checks ==")
try:
    sm = json.loads(s3.get_object(Bucket=BUCKET, Key="data/engine-signal-map.json")["Body"].read())
    blob = json.dumps(sm)
    check("registry.onchain_family", "onchain_composite_risk" in blob and "cryptoquant" in blob, "in map feed")
except Exception as e:
    check("registry.onchain_family", False, e)
try:
    pg = pub("global-flow-desk.html")
    check("spot.gfd_page_v2", b"FLOW DESK v2" in pg and b"S.ranked" in pg, "%d bytes" % len(pg))
except Exception as e:
    check("spot.gfd_page_v2", False, e)
try:
    fp = pubj("data/institutional-footprint.json")
    br = fp.get("ai_dossier") or ""
    ok4 = (90 <= len(br) <= 760 and br[:1].isalpha() and br[:1].isupper()
           and chr(34) not in br and "[" not in br and "]" not in br
           and br.rstrip().endswith((".", "!", "?")))
    check("spot.footprint_contract_v4", ok4, "[%s|%d] %s" % (fp.get("ai_dossier_src"), len(br), br[:60]))
except Exception as e:
    check("spot.footprint_contract_v4", False, e)

A["verdict"] = "ALL PASS (%d checks)" % len(A["checks"]) if not FAILS else "FAILURES: %s" % FAILS
print("VERDICT:", A["verdict"])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2743_onchain_audit.json", "w") as f:
    json.dump(A, f, indent=1, default=str)
assert not FAILS, A["verdict"]
print("OPS 2743 COMPLETE — audited by the machine, not the founder")
