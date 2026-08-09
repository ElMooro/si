"""ops 4567 — FRED scoped-import liveness verify (READ-ONLY).
Session 2026-08-09 ended at ops 4566 with the importer ENABLED and walking:
cats_done=68/179, imported_total=2057, accounting reconciles, ~16:35 UTC.
Khalid's question: is the data still importing NOW? This op mutates nothing —
it reads the state file, manifest, rule, CloudWatch invocation evidence, and
the provider hub, then issues a verdict with the progress delta since the
session-end baseline."""
import json
import sys
from datetime import datetime, timezone, timedelta

import boto3

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-fred-catalog"
RULE = "justhodl-fred-catalog-5min"
BASE_IMPORTED = 2057
BASE_CATS = 68
BASE_AT = "2026-08-09T16:35:00+00:00"

s3 = boto3.client("s3", region_name=REGION)
ev = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def gj(key):
    return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())


R = {"ops": 4567, "at": datetime.now(timezone.utc).isoformat()}
try:
    with report("4567_fred_liveness") as r:
        r.heading("ops 4567 — FRED scoped-import liveness (read-only)")

        st = gj("data/_state/fred-scoped-import.json")
        now = datetime.now(timezone.utc)
        upd = st.get("updated_at") or ""
        age_min = None
        try:
            age_min = round((now - datetime.fromisoformat(upd.replace("Z", "+00:00"))).total_seconds() / 60, 1)
        except Exception:
            pass
        cur = {
            "cats_done": len(st.get("cats_done") or []),
            "of": st.get("n_categories_expanded"),
            "series_seen": st.get("series_seen"),
            "series_queued": st.get("series_queued"),
            "imported_total": st.get("series_imported"),
            "excluded_stale": st.get("series_excluded_stale"),
            "status": st.get("status"),
            "blocked_at": st.get("blocked_at"),
            "updated_at": upd,
            "state_age_min": age_min,
            "accounting": st.get("accounting"),
        }
        R["state"] = cur
        errs = list((st.get("errors") or {}).items())
        R["err_count"] = len(errs)
        R["err_sample"] = errs[:5]
        r.section("state")
        r.kv(**{k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in cur.items()})

        try:
            hrs = max((now - datetime.fromisoformat(BASE_AT)).total_seconds() / 3600, 0.01)
        except Exception:
            hrs = 0.01
        d_imp = (cur["imported_total"] or 0) - BASE_IMPORTED
        d_cats = cur["cats_done"] - BASE_CATS
        R["delta"] = {
            "since": BASE_AT,
            "hours": round(hrs, 2),
            "imported": d_imp,
            "cats": d_cats,
            "imported_per_hr": round(d_imp / hrs, 1),
        }
        r.section("delta vs ops-4566 baseline (2057 imported, 68/179 cats)")
        r.kv(**R["delta"])

        rule = ev.describe_rule(Name=RULE)
        R["rule"] = {"state": rule.get("State"), "expr": rule.get("ScheduleExpression")}
        (r.ok if rule.get("State") == "ENABLED" else r.warn)(
            "rule %s: %s %s" % (RULE, rule.get("State"), rule.get("ScheduleExpression"))
        )

        t0 = now - timedelta(hours=3)

        def msum(metric):
            pts = cw.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName=metric,
                Dimensions=[{"Name": "FunctionName", "Value": FN}],
                StartTime=t0,
                EndTime=now,
                Period=900,
                Statistics=["Sum"],
            ).get("Datapoints", [])
            return int(sum(p["Sum"] for p in pts))

        inv3h = msum("Invocations")
        err3h = msum("Errors")
        R["cw_3h"] = {"invocations": inv3h, "errors": err3h}
        r.kv(invocations_3h=inv3h, errors_3h=err3h)

        man = gj("data/providers/fred-scoped/manifest.json")
        R["manifest"] = {k: man.get(k) for k in ("categories_done", "categories_total", "series_imported", "updated_at", "status")}
        r.section("manifest")
        r.kv(**R["manifest"])

        hub = gj("data/provider-catalog.json")
        R["hub_totals"] = hub.get("totals")
        rows = {p.get("slug"): p for p in hub.get("providers", [])}
        R["hub_fred"] = {k: rows.get("fred", {}).get(k) for k in ("n_keys", "total_mb", "freshest_h")}
        R["hub_statcan"] = {k: rows.get("statcan", {}).get(k) for k in ("n_keys", "datasets_target", "coverage_pct")}
        r.section("hub")
        r.kv(totals=json.dumps(R["hub_totals"]), fred=json.dumps(R["hub_fred"]), statcan=json.dumps(R["hub_statcan"]))

        fresh = age_min is not None and age_min <= 20
        advancing = d_imp > 0
        enabled = rule.get("State") == "ENABLED"
        firing = inv3h > 0
        if fresh and advancing and enabled and firing and not cur["blocked_at"]:
            verdict = "IMPORTING"
        elif cur["blocked_at"]:
            verdict = "BLOCKED"
        elif enabled and firing and fresh:
            verdict = "ALIVE_NOT_ADVANCING"
        else:
            verdict = "HALTED"
        R["verdict"] = (
            "%s imported=%s (+%s in %sh) cats=%s/%s rule=%s inv3h=%s state_age_min=%s"
            % (verdict, cur["imported_total"], d_imp, round(hrs, 1), cur["cats_done"], cur["of"], rule.get("State"), inv3h, age_min)
        )
        (r.ok if verdict == "IMPORTING" else r.warn)(R["verdict"])
except Exception as e:
    import os
    import traceback

    R["error"] = "%s: %s" % (type(e).__name__, e)
    R["trace"] = traceback.format_exc()[-1500:]
    os.makedirs("aws/ops/reports", exist_ok=True)
    json.dump(R, open("aws/ops/reports/4567.json", "w"), indent=1, default=str)
    open("aws/ops/reports/4567.md", "w").write("# 4567 FAIL — " + R["error"] + "\n")
    print("FAIL", R["error"])
    sys.exit(1)

import os

os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4567.json", "w"), indent=1, default=str)
open("aws/ops/reports/4567.md", "w").write(
    "# 4567 — " + R["verdict"] + "\n- errs(" + str(R["err_count"]) + "): " + json.dumps(R["err_sample"], default=str)[:400] + "\n"
)
print(R["verdict"][:300])
sys.exit(0)
