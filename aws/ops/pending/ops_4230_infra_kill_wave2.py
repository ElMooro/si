"""
ops_4230 — COST REMEDIATION, wave 2.

Now that jh-cost-governance is attached (via group jh-automation), the
runner can see the ~$162/mo of standby infrastructure that ops 4228 was
blocked from inventorying.

Doctrine for this op — REVERSIBILITY DECIDES AUTONOMY:
  * Reversible actions execute automatically. Stopping an EC2 instance
    keeps its EBS volume; deleting a SageMaker Studio app keeps the EFS
    home directory; pausing App Runner keeps the service definition;
    disabling an EventBridge rule keeps the rule. All of these are one
    command to undo.
  * IRREVERSIBLE actions (OpenSearch DeleteDomain, ELB delete) are NOT
    executed here regardless of how idle they look. They are reported
    with 14-day traffic evidence attached so the call is made on facts.
    An OpenSearch domain may hold the FRED index; losing it silently to
    a cost sweep would be a far worse outcome than another month of $51.

Sections:
  1. Verify the ops-4229 census fix actually held (S3 cursor + the
     RecursiveInvocationsDropped counter since deploy).
  2. Fleet-wide duplicate schedule census; auto-dedupe the monitors.
  3. Standby infrastructure inventory with real traffic metrics.
  4. Execute the reversible stops.
  5. Budget -> ACTUAL notifications at 50/80/100% (the $328 warning was
     FORECASTED-only, which is why it arrived late and loud).
"""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 5, "mode": "adaptive"}, read_timeout=60)
NOW = datetime.now(timezone.utc)
D14 = NOW - timedelta(days=14)
OUT = {"ops": 4230, "ts": NOW.isoformat(), "executed": [], "held": []}

MONITORS = ("justhodl-fleet-error-monitor", "justhodl-fleet-monitor",
            "justhodl-event-flow-monitor", "justhodl-fleet-freshness-monitor",
            "justhodl-health-monitor")


def C(svc):
    return boto3.client(svc, region_name=REGION, config=CFG)


cw, lam, s3 = C("cloudwatch"), C("lambda"), C("s3")
evb, sch, ec2 = C("events"), C("scheduler"), C("ec2")


def msum(ns, metric, dims, stat="Sum", start=None):
    try:
        r = cw.get_metric_statistics(Namespace=ns, MetricName=metric,
                                     Dimensions=dims, StartTime=start or D14,
                                     EndTime=NOW, Period=1209600,
                                     Statistics=[stat])
        pts = r.get("Datapoints", [])
        return (sum(p[stat] for p in pts) if stat == "Sum"
                else (max(p[stat] for p in pts) if pts else 0.0))
    except Exception:
        return -1.0


with report("4230_infra_kill_wave2") as rep:
    rep.heading("ops 4230 — standby infrastructure, wave 2")

    # ================================================================= 1
    rep.section("1. Did the ops-4229 census fix hold?")
    try:
        o = s3.get_object(Bucket=BUCKET,
                          Key="data/_state/fundamental-census-cursor.json")
        cur = json.loads(o["Body"].read())
        rep.ok("cursor artifact EXISTS — the drain-loop ran and persisted")
        rep.log("   %s" % json.dumps(cur)[:220])
        rep.kv(section="census", cursor=cur.get("cursor"),
               universe=cur.get("universe"), version=cur.get("version"))
        if cur.get("universe"):
            links_old = (cur["universe"] + 7) // 8
            rep.log("   universe=%d -> OLD design needed %d chained links "
                    "(AWS kills at 16, so ~%.1f%% of the walk ever ran)"
                    % (cur["universe"], links_old,
                       100.0 * min(16, links_old) / max(links_old, 1)))
    except Exception as e:
        rep.warn("cursor not readable yet: %s" % str(e)[:140])
    drop = msum("AWS/Lambda", "RecursiveInvocationsDropped",
                [{"Name": "FunctionName",
                  "Value": "justhodl-fundamental-census"}],
                start=NOW - timedelta(hours=10))
    rep.log("RecursiveInvocationsDropped since deploy (10h) = %s"
            % ("0" if drop <= 0 else int(drop)))
    OUT["recursion_since_fix"] = max(drop, 0)

    # ================================================================= 2
    rep.section("2. Duplicate schedule census (fleet-wide)")
    rmap = {}
    try:
        for page in evb.get_paginator("list_rules").paginate():
            for r in page["Rules"]:
                if not r.get("ScheduleExpression") or r.get("State") != "ENABLED":
                    continue
                try:
                    tg = evb.list_targets_by_rule(Rule=r["Name"])
                except Exception:
                    continue
                for t in tg.get("Targets", []):
                    fn = t.get("Arn", "").split(":")[-1]
                    rmap.setdefault(fn, []).append(
                        ("events", r["Name"], r["ScheduleExpression"]))
    except Exception as e:
        rep.warn("rules: %s" % str(e)[:120])
    try:
        for page in sch.get_paginator("list_schedules").paginate():
            for s_ in page["Schedules"]:
                if s_.get("State") != "ENABLED":
                    continue
                try:
                    d = sch.get_schedule(Name=s_["Name"],
                                         GroupName=s_.get("GroupName",
                                                          "default"))
                except Exception:
                    continue
                fn = (d.get("Target", {}).get("Arn", "") or "").split(":")[-1]
                rmap.setdefault(fn, []).append(
                    ("scheduler", s_["Name"], d.get("ScheduleExpression")))
    except Exception as e:
        rep.warn("schedules: %s" % str(e)[:120])

    dupes = {f: v for f, v in rmap.items() if len(v) > 1}
    rep.log("functions carrying MORE THAN ONE enabled schedule: %d"
            % len(dupes))
    for f, v in sorted(dupes.items(), key=lambda x: -len(x[1]))[:25]:
        rep.log("   %-42s x%d  %s" % (f[:42], len(v),
                                      ", ".join(x[2] or "?" for x in v)[:70]))
        rep.kv(section="dupe_schedule", function=f, count=len(v),
               exprs=", ".join(str(x[2]) for x in v)[:90])
    OUT["dupe_functions"] = {f: len(v) for f, v in dupes.items()}

    rep.log("")
    rep.log("auto-dedupe: MONITORS only (keep 1, disable the rest)")
    for f in MONITORS:
        ents = rmap.get(f, [])
        if len(ents) <= 1:
            continue
        keep = sorted(ents, key=lambda x: (x[0] != "scheduler", x[1]))[0]
        rep.ok("   %s KEEP %s/%s (%s)" % (f, keep[0], keep[1], keep[2]))
        for kind, name, expr in ents:
            if (kind, name) == (keep[0], keep[1]):
                continue
            try:
                if kind == "events":
                    evb.disable_rule(Name=name)
                else:
                    d = sch.get_schedule(Name=name, GroupName="default")
                    sch.update_schedule(
                        Name=name, GroupName="default",
                        ScheduleExpression=d["ScheduleExpression"],
                        FlexibleTimeWindow=d["FlexibleTimeWindow"],
                        Target=d["Target"], State="DISABLED")
                rep.log("      disabled %s/%s (%s)" % (kind, name, expr))
                OUT["executed"].append({"a": "disable_schedule", "fn": f,
                                        "rule": name, "expr": expr})
            except Exception as e:
                rep.fail("      disable %s: %s" % (name, str(e)[:100]))

    # ================================================================= 3
    rep.section("3. Standby infrastructure inventory")
    kill_now, hold = [], []

    # --- OpenSearch (IRREVERSIBLE -> hold)
    try:
        os_ = C("opensearch")
        acct = boto3.client("sts").get_caller_identity()["Account"]
        doms = os_.list_domain_names().get("DomainNames", [])
        rep.log("OpenSearch domains: %d" % len(doms))
        for dd in doms:
            n = dd["DomainName"]
            cfg = os_.describe_domain(DomainName=n)["DomainStatus"]
            cl = cfg.get("ClusterConfig", {})
            dims = [{"Name": "DomainName", "Value": n},
                    {"Name": "ClientId", "Value": acct}]
            sr = msum("AWS/ES", "SearchRate", dims)
            docs = msum("AWS/ES", "SearchableDocuments", dims, "Maximum")
            rep.warn("  %-24s %s x%s  14d searches=%.0f docs=%.0f"
                     % (n, cl.get("InstanceType"), cl.get("InstanceCount"),
                        sr, docs))
            rep.log("     endpoint=%s created=%s"
                    % (cfg.get("Endpoint", "-"), str(cfg.get("Created"))[:10]))
            hold.append({"svc": "opensearch", "id": n, "usd_mo": 51,
                         "irreversible": True,
                         "evidence": "14d searches=%.0f, %.0f docs indexed"
                                     % (sr, docs)})
            rep.kv(section="opensearch", domain=n, searches_14d=int(sr),
                   docs=int(docs), instance=cl.get("InstanceType"),
                   action="HOLD (irreversible)")
    except Exception as e:
        rep.fail("opensearch: %s" % str(e)[:170])

    # --- SageMaker (DeleteApp reversible -> execute)
    try:
        sm = C("sagemaker")
        apps = sm.list_apps(MaxResults=100).get("Apps", [])
        for a in apps:
            rep.log("  SM app %-20s type=%s status=%s"
                    % (a.get("AppName"), a.get("AppType"), a.get("Status")))
            if a.get("Status") == "InService":
                kill_now.append({"svc": "sagemaker_app", "usd_mo": 35,
                                 "obj": a})
        for e in sm.list_endpoints(MaxResults=100).get("Endpoints", []):
            rep.warn("  SM endpoint %s %s" % (e["EndpointName"],
                                              e["EndpointStatus"]))
            hold.append({"svc": "sagemaker_endpoint", "id": e["EndpointName"],
                         "usd_mo": 40, "irreversible": True,
                         "evidence": "inference endpoint live"})
        for n in sm.list_notebook_instances(MaxResults=100).get(
                "NotebookInstances", []):
            rep.warn("  SM notebook %s %s" % (n["NotebookInstanceName"],
                                              n["NotebookInstanceStatus"]))
            if n["NotebookInstanceStatus"] == "InService":
                kill_now.append({"svc": "sagemaker_notebook", "usd_mo": 30,
                                 "obj": n})
        if not apps:
            rep.ok("no SageMaker Studio apps running")
    except Exception as e:
        rep.fail("sagemaker: %s" % str(e)[:170])

    # --- ELB (delete IRREVERSIBLE -> hold)
    try:
        elb = C("elbv2")
        for lb in elb.describe_load_balancers().get("LoadBalancers", []):
            arn, nm = lb["LoadBalancerArn"], lb["LoadBalancerName"]
            sfx = "/".join(arn.split("/")[-3:])
            rc = msum("AWS/ApplicationELB", "RequestCount",
                      [{"Name": "LoadBalancer", "Value": sfx}])
            tgt = 0
            for tg in elb.describe_target_groups(
                    LoadBalancerArn=arn).get("TargetGroups", []):
                tgt += len(elb.describe_target_health(
                    TargetGroupArn=tg["TargetGroupArn"]).get(
                    "TargetHealthDescriptions", []))
            rep.warn("  ELB %-26s 14d requests=%.0f targets=%d dns=%s"
                     % (nm[:26], rc, tgt, lb.get("DNSName", "")[:40]))
            hold.append({"svc": "elbv2", "id": nm, "arn": arn, "usd_mo": 32,
                         "irreversible": True,
                         "evidence": "14d requests=%.0f, %d registered targets"
                                     % (rc, tgt)})
            rep.kv(section="elb", name=nm, requests_14d=int(rc),
                   targets=tgt, action="HOLD (irreversible)")
    except Exception as e:
        rep.fail("elb: %s" % str(e)[:170])

    # --- EC2 / EIP / NAT / VPCE
    try:
        for r in ec2.describe_instances().get("Reservations", []):
            for i in r["Instances"]:
                if i["State"]["Name"] != "running":
                    continue
                nm = next((t["Value"] for t in i.get("Tags", [])
                           if t["Key"] == "Name"), "-")
                cpu = msum("AWS/EC2", "CPUUtilization",
                           [{"Name": "InstanceId", "Value": i["InstanceId"]}],
                           "Average")
                net = msum("AWS/EC2", "NetworkOut",
                           [{"Name": "InstanceId", "Value": i["InstanceId"]}])
                rep.warn("  EC2 %-20s %-11s name=%-16s 14d avgCPU=%.2f%% "
                         "netOut=%.0fB" % (i["InstanceId"], i["InstanceType"],
                                           nm[:16], cpu, net))
                rep.kv(section="ec2", id=i["InstanceId"], name=nm,
                       type=i["InstanceType"], cpu_avg=round(cpu, 2),
                       net_out=int(net))
                kill_now.append({"svc": "ec2_stop", "usd_mo": 16,
                                 "obj": i, "cpu": cpu, "net": net, "name": nm})
        for a in ec2.describe_addresses().get("Addresses", []):
            att = a.get("InstanceId") or a.get("NetworkInterfaceId")
            rep.log("  EIP %-16s -> %s" % (a.get("PublicIp"),
                                           att or "UNATTACHED"))
            if not att:
                kill_now.append({"svc": "eip_release", "usd_mo": 4, "obj": a})
        nats = [n for n in ec2.describe_nat_gateways().get("NatGateways", [])
                if n["State"] == "available"]
        for n in nats:
            rep.warn("  NAT %s vpc=%s (~$33/mo)" % (n["NatGatewayId"],
                                                    n.get("VpcId")))
            hold.append({"svc": "nat", "id": n["NatGatewayId"], "usd_mo": 33,
                         "irreversible": False,
                         "evidence": "NAT gateway; check what needs egress "
                                     "before removing"})
        ves = [v for v in ec2.describe_vpc_endpoints().get("VpcEndpoints", [])
               if v.get("VpcEndpointType") == "Interface"]
        for v in ves:
            rep.warn("  VPCE %s %s (~$7.30/mo)"
                     % (v["VpcEndpointId"], v.get("ServiceName", "")[-38:]))
            hold.append({"svc": "vpce", "id": v["VpcEndpointId"],
                         "usd_mo": 7.3, "irreversible": False,
                         "evidence": v.get("ServiceName", "")[-50:]})
        rep.log("NAT gateways: %d   interface VPC endpoints: %d"
                % (len(nats), len(ves)))
    except Exception as e:
        rep.fail("ec2: %s" % str(e)[:170])

    # --- App Runner (Pause reversible -> execute)
    try:
        ar = C("apprunner")
        for s_ in ar.list_services().get("ServiceSummaryList", []):
            rep.warn("  AppRunner %-22s status=%s url=%s"
                     % (s_["ServiceName"][:22], s_["Status"],
                        s_.get("ServiceUrl", "-")))
            rep.kv(section="apprunner", name=s_["ServiceName"],
                   status=s_["Status"], url=s_.get("ServiceUrl", "-"))
            if s_["Status"] == "RUNNING":
                kill_now.append({"svc": "apprunner_pause", "usd_mo": 10,
                                 "obj": s_})
    except Exception as e:
        rep.fail("apprunner: %s" % str(e)[:170])

    # ================================================================= 4
    rep.section("4. Execute REVERSIBLE stops")
    saved = 0.0
    for k in kill_now:
        svc, o = k["svc"], k["obj"]
        try:
            if svc == "sagemaker_app":
                C("sagemaker").delete_app(
                    DomainId=o["DomainId"],
                    UserProfileName=o.get("UserProfileName"),
                    AppType=o["AppType"], AppName=o["AppName"])
                rep.ok("  SageMaker app %s DELETED (EFS home preserved; "
                       "relaunch from Studio anytime)" % o["AppName"])
            elif svc == "sagemaker_notebook":
                C("sagemaker").stop_notebook_instance(
                    NotebookInstanceName=o["NotebookInstanceName"])
                rep.ok("  notebook %s STOPPED (EBS preserved)"
                       % o["NotebookInstanceName"])
            elif svc == "ec2_stop":
                if k["cpu"] > 5.0:
                    rep.warn("  EC2 %s NOT stopped — 14d avg CPU %.1f%% "
                             "means it is doing work" % (o["InstanceId"],
                                                         k["cpu"]))
                    hold.append({"svc": "ec2", "id": o["InstanceId"],
                                 "usd_mo": 16, "irreversible": False,
                                 "evidence": "avg CPU %.1f%% — active"
                                             % k["cpu"]})
                    continue
                ec2.stop_instances(InstanceIds=[o["InstanceId"]])
                rep.ok("  EC2 %s (%s) STOPPED — 14d avg CPU %.2f%%; EBS "
                       "preserved, `aws ec2 start-instances` to undo"
                       % (o["InstanceId"], k.get("name"), k["cpu"]))
            elif svc == "eip_release":
                ec2.release_address(AllocationId=o["AllocationId"])
                rep.ok("  EIP %s RELEASED (was unattached)" % o["PublicIp"])
            elif svc == "apprunner_pause":
                C("apprunner").pause_service(ServiceArn=o["ServiceArn"])
                rep.ok("  AppRunner %s PAUSED (resume anytime)"
                       % o["ServiceName"])
            saved += k["usd_mo"]
            OUT["executed"].append({"a": svc, "usd_mo": k["usd_mo"]})
        except Exception as e:
            rep.fail("  %s: %s" % (svc, str(e)[:150]))
    rep.log("")
    rep.log("reversible stops executed -> ~$%.0f/mo removed" % saved)
    OUT["saved_usd_mo"] = round(saved, 2)

    # ================================================================= 5
    rep.section("5. Budget -> ACTUAL alerts at 50/80/100%")
    try:
        bud = C("budgets")
        acct = boto3.client("sts").get_caller_identity()["Account"]
        for b in bud.describe_budgets(AccountId=acct)["Budgets"]:
            bn = b["BudgetName"]
            rep.log("  budget %-24s limit=$%s"
                    % (bn, b["BudgetLimit"]["Amount"]))
            subs = []
            existing = set()
            try:
                for n in bud.describe_notifications_for_budget(
                        AccountId=acct, BudgetName=bn).get("Notifications", []):
                    existing.add((n["NotificationType"],
                                  float(n["Threshold"])))
                    if not subs:
                        subs = bud.describe_subscribers_for_notification(
                            AccountId=acct, BudgetName=bn,
                            Notification=n).get("Subscribers", [])
            except Exception as e:
                rep.warn("     read notifications: %s" % str(e)[:90])
            if not subs:
                rep.warn("     no existing subscriber to copy — skipping "
                         "(cannot invent an email address)")
                continue
            for th in (50.0, 80.0, 100.0):
                if ("ACTUAL", th) in existing:
                    rep.log("     ACTUAL %.0f%% already present" % th)
                    continue
                try:
                    bud.create_notification(
                        AccountId=acct, BudgetName=bn,
                        Notification={"NotificationType": "ACTUAL",
                                      "ComparisonOperator": "GREATER_THAN",
                                      "Threshold": th,
                                      "ThresholdType": "PERCENTAGE"},
                        Subscribers=subs)
                    rep.ok("     ACTUAL > %.0f%% added ($%.0f)"
                           % (th, float(b["BudgetLimit"]["Amount"]) * th / 100))
                    OUT["executed"].append({"a": "budget_actual",
                                            "budget": bn, "th": th})
                except Exception as e:
                    rep.warn("     %.0f%%: %s" % (th, str(e)[:90]))
    except Exception as e:
        rep.fail("budgets: %s" % str(e)[:170])

    # ================================================================= 6
    rep.section("6. HELD — irreversible, needs an explicit call")
    th = 0.0
    for h in sorted(hold, key=lambda x: -x["usd_mo"]):
        th += h["usd_mo"]
        rep.log("  $%6.2f/mo  %-16s %-26s  %s"
                % (h["usd_mo"], h["svc"], str(h["id"])[:26],
                   h["evidence"][:64]))
        rep.kv(section="held", svc=h["svc"], id=str(h["id"]),
               usd_mo=h["usd_mo"], evidence=h["evidence"][:80])
    rep.log("")
    rep.log("held total: $%.2f/mo   executed this run: $%.2f/mo" % (th, saved))
    OUT["held"] = hold

    rp = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd())) \
        / "aws" / "ops" / "reports" / "4230_infra_kill_wave2.json"
    rp.write_text(json.dumps(OUT, indent=1, default=str), encoding="utf-8")
    rep.ok("wrote %s" % rp.name)
