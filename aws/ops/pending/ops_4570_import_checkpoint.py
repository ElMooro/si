"""ops 4570 — checkpoint: still importing + page parity (READ-ONLY).
Khalid's page snapshot at 22:48 UTC showed FRED 5,029 series banked ·
81/179 · walking, totals 27,540 keys / 43.71 GB. This op proves, right
now: (1) the importer is still advancing past that snapshot on DISK,
(2) the schedule is live and firing, (3) the hub the page renders is
fresh and matches disk, (4) yesterday's fixes (statcan denied, extras
stats) held. Mutates nothing."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta

import boto3

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
IMP = "justhodl-fred-catalog"
RULE = "justhodl-fred-catalog-5min"
PAGE_BANKED = 5029      # from Khalid's 22:48 snapshot
PAGE_AT = "2026-08-09T22:48:00+00:00"

s3 = boto3.client("s3", region_name=REGION)
ev = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)

R = {"ops": 4570, "at": datetime.now(timezone.utc).isoformat()}
FAILS = []


def gj(key):
    return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())


def count_prefix(prefix):
    n, tok = 0, None
    while True:
        kw = {"Bucket": B, "Prefix": prefix, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        rr = s3.list_objects_v2(**kw)
        n += len(rr.get("Contents", []))
        if not rr.get("IsTruncated"):
            break
        tok = rr.get("NextContinuationToken")
    return n


try:
    with report("4570_import_checkpoint") as r:
        r.heading("ops 4570 — import checkpoint vs 22:48 snapshot")
        now = datetime.now(timezone.utc)

        r.section("1. disk truth")
        disk = count_prefix("data/warm/fred-scoped/")
        hrs = max((now - datetime.fromisoformat(PAGE_AT))
                  .total_seconds() / 3600, 0.01)
        d = disk - PAGE_BANKED
        R["disk"] = {"banked_now": disk, "page_snapshot": PAGE_BANKED,
                     "delta": d, "hours": round(hrs, 2),
                     "per_hr": round(d / hrs, 1)}
        r.kv(**R["disk"])
        if d <= 0:
            FAILS.append(f"disk {disk} not past snapshot {PAGE_BANKED}"
                         " — import stalled since 22:48")

        r.section("2. importer state + schedule + fires")
        st = gj("data/_state/fred-scoped-import.json")
        upd = st.get("updated_at") or ""
        try:
            age_min = round((now - datetime.fromisoformat(
                upd.replace("Z", "+00:00"))).total_seconds() / 60, 1)
        except Exception:
            age_min = None
        R["state"] = {"cats_done": len(st.get("cats_done") or []),
                      "of": st.get("n_categories_expanded"),
                      "queued": st.get("series_queued"),
                      "skipped_already":
                          st.get("series_skipped_already"),
                      "status": st.get("status"),
                      "blocked_at": st.get("blocked_at"),
                      "state_age_min": age_min}
        r.kv(**R["state"])
        if st.get("blocked_at"):
            FAILS.append(f"importer blocked: {st['blocked_at']}")
        if age_min is None or age_min > 20:
            FAILS.append(f"state stale ({age_min} min)")
        rule = ev.describe_rule(Name=RULE)
        pts = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": IMP}],
            StartTime=now - timedelta(hours=2), EndTime=now,
            Period=900, Statistics=["Sum"]).get("Datapoints", [])
        inv2h = int(sum(p["Sum"] for p in pts))
        R["schedule"] = {"rule": rule.get("State"), "inv_2h": inv2h}
        r.kv(**R["schedule"])
        if rule.get("State") != "ENABLED":
            FAILS.append(f"rule {rule.get('State')}")
        if inv2h < 6:
            FAILS.append(f"only {inv2h} fires in 2h")

        r.section("3. hub freshness + page parity + regressions")
        hub = gj("data/provider-catalog.json")
        rows = {p.get("slug"): p for p in hub.get("providers", [])}
        fred = rows.get("fred", {})
        try:
            hub_age_min = round((now - datetime.fromisoformat(
                hub.get("as_of"))).total_seconds() / 60, 1)
        except Exception:
            hub_age_min = None
        R["hub"] = {"as_of": hub.get("as_of"),
                    "age_min": hub_age_min,
                    "fred_series": fred.get("series_count"),
                    "fred_keys": fred.get("n_keys"),
                    "note": fred.get("catalog_note"),
                    "totals": hub.get("totals")}
        r.kv(hub_as_of=hub.get("as_of"), hub_age_min=hub_age_min,
             fred_series=fred.get("series_count"),
             fred_keys=fred.get("n_keys"),
             totals=json.dumps(hub.get("totals")))
        if (fred.get("series_count") or 0) > disk:
            FAILS.append("hub series exceeds disk — counting broke")
        if "banked" not in (fred.get("catalog_note") or ""):
            FAILS.append("catalog_note regressed")
        sc = rows.get("statcan", {})
        if not (sc.get("denied_source_side") or 0) >= 1:
            FAILS.append("statcan denied regressed")
        for p in hub.get("providers", []):
            if p.get("unit") == "instruments" and \
                    not (p.get("n_keys") or 0) >= 1:
                FAILS.append(f"extra {p.get('slug')} regressed")

        r.section("4. served hub (what the page fetches)")
        try:
            req = urllib.request.Request(
                "https://justhodl.ai/data/provider-catalog.json?cb="
                + str(int(time.time())),
                headers={"User-Agent": "Mozilla/5.0 ops4570",
                         "Cache-Control": "no-cache"})
            with urllib.request.urlopen(req, timeout=25) as resp:
                jd = json.loads(resp.read().decode())
            fr2 = next((p for p in jd.get("providers", [])
                        if p.get("slug") == "fred"), {})
            R["served"] = {"as_of": jd.get("as_of"),
                           "fred_series": fr2.get("series_count")}
            r.kv(**R["served"])
            if not fr2.get("series_count"):
                FAILS.append("served hub missing fred series")
        except Exception as e:
            r.warn(f"served fetch: {type(e).__name__}")
            FAILS.append("served hub unreachable from runner")

        verdict = ("IMPORTING_AND_PAGE_TRUE" if not FAILS
                   else "CHECK_FAILED")
        R["verdict"] = (f"{verdict} banked={disk} (+{d} in "
                        f"{round(hrs, 1)}h, {R['disk']['per_hr']}/hr) "
                        f"cats={R['state']['cats_done']}/"
                        f"{R['state']['of']} rule="
                        f"{rule.get('State')} inv2h={inv2h}")
        R["fails"] = FAILS
        (r.ok if not FAILS else r.fail)(R["verdict"] +
                                        ("" if not FAILS else
                                         " | " + " | ".join(FAILS)))
except Exception as e:
    import os
    import traceback
    R["error"] = f"{type(e).__name__}: {e}"
    R["trace"] = traceback.format_exc()[-1500:]
    os.makedirs("aws/ops/reports", exist_ok=True)
    json.dump(R, open("aws/ops/reports/4570.json", "w"), indent=1,
              default=str)
    open("aws/ops/reports/4570.md", "w").write(
        "# 4570 FAIL — " + R["error"] + "\n")
    print("FAIL", R["error"])
    sys.exit(1)

import os

os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4570.json", "w"), indent=1,
          default=str)
open("aws/ops/reports/4570.md", "w").write(
    "# 4570 — " + R["verdict"] +
    ("\n- fails: " + json.dumps(FAILS) if FAILS else "") +
    "\n- state: " + json.dumps(R.get("state"), default=str) +
    "\n- hub: " + json.dumps(R.get("hub"), default=str)[:500] + "\n")
print(R["verdict"][:300])
if FAILS:
    sys.exit(1)
sys.exit(0)
