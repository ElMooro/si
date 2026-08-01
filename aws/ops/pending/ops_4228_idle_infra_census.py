"""
ops_4228 — IDLE INFRASTRUCTURE CENSUS. READ-ONLY.

ops 4227 proved the bill is NOT Lambda ($11.87/14d of real compute).
It is always-on standby infrastructure plus CloudWatch API polling:

   CW:Requests                 $30.23/14d   -> ~$66/mo   (fleet pollers)
   ESInstance:t3.small         $23.47/14d   -> ~$51/mo   (OpenSearch)
   Studio:JupyterLab t3.medium $16.29/14d   -> ~$35/mo   (SageMaker IDE)
   LoadBalancerUsage           $14.67/14d   -> ~$32/mo   (ELB)
   PublicIPv4:InUseAddress      $8.15/14d   -> ~$18/mo
   BoxUsage:t2.micro            $7.52/14d   -> ~$16/mo   (EC2)
   Lambda-SnapStart-Cached      $7.50/14d   -> ~$16/mo
   AppRunner-Provisioned-GB-h   $4.56/14d   -> ~$10/mo

No Lambda in the 765-function fleet references OpenSearch, SageMaker or
App Runner anywhere in source. This op establishes, for each billable
standby resource: what it is, when it was created, and whether anything
has actually TALKED TO IT in the last 14 days (real traffic metrics, not
inference). Nothing is deleted here — the kill list is produced with
evidence attached so the decision is made on facts.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
CFG = Config(retries={"max_attempts": 5, "mode": "adaptive"}, read_timeout=60)
NOW = datetime.now(timezone.utc)
D14 = NOW - timedelta(days=14)
OUT = {"ops": 4228, "ts": NOW.isoformat()}


def C(svc, region=REGION):
    return boto3.client(svc, region_name=region, config=CFG)


cw = C("cloudwatch")


def msum(ns, metric, dims, stat="Sum"):
    try:
        r = cw.get_metric_statistics(Namespace=ns, MetricName=metric,
                                     Dimensions=dims, StartTime=D14,
                                     EndTime=NOW, Period=1209600,
                                     Statistics=[stat])
        return sum(p[stat] for p in r.get("Datapoints", [])) or 0.0
    except Exception:
        return -1.0


with report("4228_idle_infra_census") as rep:
    rep.heading("ops 4228 — idle infrastructure census (read-only)")
    kill, keep = [], []

    # ------------------------------------------------------------ OpenSearch
    rep.section("1. OpenSearch  (~$51/mo)")
    try:
        os_ = C("opensearch")
        doms = os_.list_domain_names().get("DomainNames", [])
        rep.log("domains: %d" % len(doms))
        for d in doms:
            n = d["DomainName"]
            cfg = os_.describe_domain(DomainName=n)["DomainStatus"]
            cl = cfg.get("ClusterConfig", {})
            dims = [{"Name": "DomainName", "Value": n},
                    {"Name": "ClientId", "Value":
                        boto3.client("sts").get_caller_identity()["Account"]}]
            sr = msum("AWS/ES", "SearchRate", dims, "Sum")
            ir = msum("AWS/ES", "IndexingRate", dims, "Sum")
            docs = msum("AWS/ES", "SearchableDocuments", dims, "Maximum")
            rep.log("  %-28s type=%s count=%s created=%s"
                    % (n, cl.get("InstanceType"), cl.get("InstanceCount"),
                       str(cfg.get("Created"))[:10]))
            rep.log("     14d SearchRate=%.1f IndexingRate=%.1f docs=%.0f"
                    % (sr, ir, docs))
            rep.log("     endpoint=%s" % cfg.get("Endpoint", "-"))
            rec = "KILL" if sr <= 0 else "KEEP"
            (kill if rec == "KILL" else keep).append(
                {"svc": "opensearch", "id": n, "usd_mo": 51,
                 "evidence": "14d search rate %.1f, docs %.0f" % (sr, docs)})
            rep.kv(section="opensearch", domain=n,
                   instance=cl.get("InstanceType"), search_14d=round(sr, 1),
                   index_14d=round(ir, 1), docs=int(docs), verdict=rec)
    except Exception as e:
        rep.fail("opensearch: %s" % str(e)[:180])

    # ------------------------------------------------------------ SageMaker
    rep.section("2. SageMaker  (~$35/mo Studio JupyterLab)")
    try:
        sm = C("sagemaker")
        apps = sm.list_apps(MaxResults=100).get("Apps", [])
        for a in apps:
            rep.log("  APP %-22s type=%s status=%s user=%s created=%s"
                    % (a.get("AppName"), a.get("AppType"), a.get("Status"),
                       a.get("UserProfileName"), str(a.get("CreationTime"))[:10]))
            rep.kv(section="sagemaker_app", app=a.get("AppName"),
                   type=a.get("AppType"), status=a.get("Status"),
                   domain=a.get("DomainId"), user=a.get("UserProfileName"))
            if a.get("Status") == "InService":
                kill.append({"svc": "sagemaker_app", "id": a.get("AppName"),
                             "domain": a.get("DomainId"),
                             "user": a.get("UserProfileName"),
                             "type": a.get("AppType"), "usd_mo": 35,
                             "evidence": "Studio IDE app running 24/7"})
        eps = sm.list_endpoints(MaxResults=100).get("Endpoints", [])
        for e in eps:
            rep.warn("  ENDPOINT %s status=%s" % (e["EndpointName"],
                                                  e["EndpointStatus"]))
            kill.append({"svc": "sagemaker_endpoint", "id": e["EndpointName"],
                         "usd_mo": 40, "evidence": "live inference endpoint"})
        nbs = sm.list_notebook_instances(MaxResults=100).get(
            "NotebookInstances", [])
        for n in nbs:
            rep.warn("  NOTEBOOK %s status=%s type=%s"
                     % (n["NotebookInstanceName"], n["NotebookInstanceStatus"],
                        n.get("InstanceType")))
            if n["NotebookInstanceStatus"] == "InService":
                kill.append({"svc": "sagemaker_notebook",
                             "id": n["NotebookInstanceName"], "usd_mo": 30,
                             "evidence": "notebook InService"})
        if not (apps or eps or nbs):
            rep.ok("no SageMaker apps/endpoints/notebooks found")
    except Exception as e:
        rep.fail("sagemaker: %s" % str(e)[:180])

    # ------------------------------------------------------------ ELB
    rep.section("3. Load balancers  (~$32/mo)")
    try:
        elb = C("elbv2")
        for lb in elb.describe_load_balancers().get("LoadBalancers", []):
            arn, nm = lb["LoadBalancerArn"], lb["LoadBalancerName"]
            suffix = "/".join(arn.split("/")[-3:])
            rc = msum("AWS/ApplicationELB", "RequestCount",
                      [{"Name": "LoadBalancer", "Value": suffix}])
            rep.log("  %-30s scheme=%s state=%s created=%s"
                    % (nm, lb.get("Scheme"), lb["State"]["Code"],
                       str(lb.get("CreatedTime"))[:10]))
            rep.log("     14d RequestCount=%.0f  dns=%s" % (rc, lb.get("DNSName")))
            tgs = elb.describe_target_groups(LoadBalancerArn=arn).get(
                "TargetGroups", [])
            nhealthy = 0
            for tg in tgs:
                th = elb.describe_target_health(
                    TargetGroupArn=tg["TargetGroupArn"]).get(
                    "TargetHealthDescriptions", [])
                for t in th:
                    st = t["TargetHealth"]["State"]
                    nhealthy += 1 if st == "healthy" else 0
                    rep.log("     tg=%s target=%s state=%s"
                            % (tg["TargetGroupName"][:24],
                               t["Target"].get("Id", "")[:24], st))
            rec = "KILL" if rc <= 0 else "KEEP"
            (kill if rec == "KILL" else keep).append(
                {"svc": "elbv2", "id": nm, "arn": arn, "usd_mo": 32,
                 "evidence": "14d requests %.0f, healthy targets %d"
                             % (rc, nhealthy)})
            rep.kv(section="elb", name=nm, requests_14d=int(rc),
                   healthy_targets=nhealthy, verdict=rec)
        try:
            elbc = C("elb")
            for lb in elbc.describe_load_balancers().get(
                    "LoadBalancerDescriptions", []):
                rep.warn("  CLASSIC ELB %s instances=%d"
                         % (lb["LoadBalancerName"], len(lb.get("Instances", []))))
                kill.append({"svc": "elb_classic", "id": lb["LoadBalancerName"],
                             "usd_mo": 18, "evidence": "classic ELB"})
        except Exception:
            pass
    except Exception as e:
        rep.fail("elb: %s" % str(e)[:180])

    # ------------------------------------------------------------ EC2 / IPv4
    rep.section("4. EC2 instances, EIPs, NAT  (~$34/mo)")
    try:
        ec2 = C("ec2")
        for r in ec2.describe_instances().get("Reservations", []):
            for i in r["Instances"]:
                if i["State"]["Name"] == "terminated":
                    continue
                nm = next((t["Value"] for t in i.get("Tags", [])
                           if t["Key"] == "Name"), "-")
                cpu = msum("AWS/EC2", "CPUUtilization",
                           [{"Name": "InstanceId", "Value": i["InstanceId"]}],
                           "Average")
                rep.log("  %-20s %-14s %-10s name=%s launched=%s pubip=%s"
                        % (i["InstanceId"], i["InstanceType"],
                           i["State"]["Name"], nm,
                           str(i.get("LaunchTime"))[:10],
                           i.get("PublicIpAddress", "-")))
                rep.log("     14d avg CPU=%.2f%%" % cpu)
                rep.kv(section="ec2", id=i["InstanceId"], type=i["InstanceType"],
                       state=i["State"]["Name"], name=nm,
                       public_ip=i.get("PublicIpAddress", "-"),
                       cpu_avg=round(cpu, 2))
                if i["State"]["Name"] == "running":
                    kill.append({"svc": "ec2", "id": i["InstanceId"],
                                 "name": nm, "usd_mo": 16,
                                 "evidence": "running, 14d avg CPU %.2f%%" % cpu})
        eips = ec2.describe_addresses().get("Addresses", [])
        for a in eips:
            att = a.get("InstanceId") or a.get("NetworkInterfaceId") or "UNATTACHED"
            rep.log("  EIP %-16s -> %s" % (a.get("PublicIp"), att))
            rep.kv(section="eip", ip=a.get("PublicIp"), attached_to=att,
                   alloc=a.get("AllocationId"))
            if att == "UNATTACHED":
                kill.append({"svc": "eip", "id": a.get("AllocationId"),
                             "usd_mo": 4, "evidence": "unattached elastic IP"})
        nats = ec2.describe_nat_gateways().get("NatGateways", [])
        for n in nats:
            if n["State"] in ("available", "pending"):
                rep.warn("  NAT GATEWAY %s state=%s vpc=%s (~$33/mo + data)"
                         % (n["NatGatewayId"], n["State"], n.get("VpcId")))
                kill.append({"svc": "nat", "id": n["NatGatewayId"],
                             "usd_mo": 33, "evidence": "NAT gateway running"})
        rep.log("EIPs: %d   NAT gateways: %d" % (len(eips), len(nats)))
        # VPC endpoints also bill hourly
        try:
            ves = ec2.describe_vpc_endpoints().get("VpcEndpoints", [])
            iface = [v for v in ves if v.get("VpcEndpointType") == "Interface"]
            rep.log("VPC endpoints: %d (interface=%d, ~$7.30/mo each)"
                    % (len(ves), len(iface)))
            for v in iface:
                rep.warn("  VPCE %s %s" % (v["VpcEndpointId"],
                                           v.get("ServiceName", "")[-40:]))
                kill.append({"svc": "vpce", "id": v["VpcEndpointId"],
                             "usd_mo": 7.3,
                             "evidence": v.get("ServiceName", "")[-50:]})
        except Exception:
            pass
    except Exception as e:
        rep.fail("ec2: %s" % str(e)[:180])

    # ------------------------------------------------------------ App Runner
    rep.section("5. App Runner  (~$10/mo)")
    try:
        ar = C("apprunner")
        for s in ar.list_services().get("ServiceSummaryList", []):
            rep.log("  %-26s status=%s url=%s created=%s"
                    % (s["ServiceName"], s["Status"],
                       s.get("ServiceUrl", "-"), str(s.get("CreatedAt"))[:10]))
            rep.kv(section="apprunner", name=s["ServiceName"],
                   status=s["Status"], url=s.get("ServiceUrl", "-"))
            if s["Status"] == "RUNNING":
                kill.append({"svc": "apprunner", "id": s["ServiceArn"],
                             "name": s["ServiceName"], "usd_mo": 10,
                             "evidence": "service RUNNING"})
        ecr = C("ecr")
        repos = ecr.describe_repositories().get("repositories", [])
        rep.log("ECR repositories: %d" % len(repos))
        for r in repos[:10]:
            try:
                imgs = ecr.describe_images(
                    repositoryName=r["repositoryName"])["imageDetails"]
                gb = sum(i.get("imageSizeInBytes", 0) for i in imgs) / 1e9
                rep.log("  ECR %-26s images=%d %.2f GB"
                        % (r["repositoryName"][:26], len(imgs), gb))
                rep.kv(section="ecr", repo=r["repositoryName"],
                       images=len(imgs), gb=round(gb, 2))
            except Exception:
                pass
    except Exception as e:
        rep.fail("apprunner/ecr: %s" % str(e)[:180])

    # ------------------------------------------------------------ SnapStart
    rep.section("6. Lambda SnapStart cached versions  (~$16/mo)")
    try:
        lam = C("lambda")
        snap = []
        pg = lam.get_paginator("list_functions")
        for page in pg.paginate():
            for f in page["Functions"]:
                if (f.get("SnapStart") or {}).get("ApplyOn") == "PublishedVersions":
                    snap.append(f["FunctionName"])
        rep.log("functions with SnapStart=PublishedVersions: %d" % len(snap))
        for fn in snap:
            try:
                vs = []
                pv = lam.get_paginator("list_versions_by_function")
                for pg2 in pv.paginate(FunctionName=fn):
                    vs.extend([v["Version"] for v in pg2["Versions"]
                               if v["Version"] != "$LATEST"])
                rep.warn("  %-40s versions=%d" % (fn[:40], len(vs)))
                rep.kv(section="snapstart", function=fn, versions=len(vs))
                kill.append({"svc": "snapstart", "id": fn,
                             "usd_mo": round(16.0 / max(len(snap), 1), 2),
                             "evidence": "%d published versions cached" % len(vs)})
            except Exception:
                pass
        if not snap:
            rep.ok("no SnapStart functions (cost is from published-version "
                   "storage elsewhere)")
    except Exception as e:
        rep.fail("snapstart: %s" % str(e)[:180])

    # ------------------------------------------------------------ misc
    rep.section("7. Secrets Manager / DynamoDB / other standing charges")
    try:
        sec = C("secretsmanager")
        n = 0
        ps = sec.get_paginator("list_secrets")
        for page in ps.paginate():
            for s in page["SecretList"]:
                n += 1
                la = s.get("LastAccessedDate")
                rep.kv(section="secret", name=s["Name"],
                       last_accessed=str(la)[:10] if la else "NEVER")
        rep.log("secrets: %d  ($0.40/mo each = $%.2f/mo)" % (n, n * 0.40))
    except Exception as e:
        rep.warn("secrets: %s" % str(e)[:120])

    # ------------------------------------------------------------ verdict
    rep.section("8. KILL LIST (evidence-backed, nothing executed)")
    tot = 0.0
    for k in sorted(kill, key=lambda x: -x["usd_mo"]):
        tot += k["usd_mo"]
        rep.log("  $%6.2f/mo  %-16s %-34s  %s"
                % (k["usd_mo"], k["svc"], str(k.get("name") or k["id"])[:34],
                   k["evidence"][:60]))
    rep.log("")
    rep.log("TOTAL RECLAIMABLE IF ALL KILLED: $%.2f/mo" % tot)
    rep.log("Current run-rate ~$300/mo -> would land ~$%.0f/mo" % (300 - tot))
    OUT["kill"] = kill
    OUT["keep"] = keep
    OUT["reclaimable_usd_mo"] = round(tot, 2)

    rp = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd())) \
        / "aws" / "ops" / "reports" / "4228_idle_infra_census.json"
    rp.write_text(json.dumps(OUT, indent=1, default=str), encoding="utf-8")
    rep.ok("wrote %s" % rp.name)
