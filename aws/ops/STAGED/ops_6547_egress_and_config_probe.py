"""ops 6547 - who pulls ~135 GB/day from justhodl-dashboard-live while every engine is off, plus the config
facts the $600 plan needs.  READ-ONLY (Khalid, 2026-10-20: "never goes past $600 ... tune up my system").

STAGED (direct lane, no serial twin).  Mutates nothing.

  1. S3 server access logs of justhodl-dashboard-live for Oct 16..20 (UTC days): GET bytes by requester class;
     for anonymous traffic by user agent, referer host, client network (/24 and /16), key, HTTP status, hour of
     day, plus the top networks drilled down to their keys and agents.  A sample of the day's log objects is read
     and scaled (the log objects are ~1 request each).
  2. HEAD of the biggest keys: size, Cache-Control, Content-Encoding, Last-Modified (is anything cacheable?).
  3. How the bucket is public: policy statements (principal / action / resource / condition keys only),
     public-access-block, CORS.
  4. Spend governor wiring the cap change depends on: Lambda env (names; values only for allow-listed keys),
     execution-role policies that mention budgets, SNS topic policies + subscription states (endpoints masked),
     the exact budget definition (cost types, filters) and the tick schedule.
  5. Schedule policy plumbing: SSM /justhodl/schedules/*, reconciler function config.
  6. CloudWatch AWS/Usage CallCount for CloudWatch API operations (which API drives the CW:Requests bill).

Cost: ~6k S3 GETs on the log bucket + ~15 HEADs + a few control-plane reads (<$0.05).
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote
import base64
import concurrent.futures as cf
import gzip
import json
import os
import re
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
PRIVATE = "justhodl-ops-private-857687956942"
DAYS = ("2026/10/16", "2026/10/17", "2026/10/18", "2026/10/19", "2026/10/20")
SAMPLE = 6000
D = {"generated_at": datetime.now(timezone.utc).isoformat(), "errors": []}

LOG_RE = re.compile(r'^(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) '
                    r'"([^"]*)" "([^"]*)"')
SAFE_ENV = {"BUDGET_NAME", "STATE_BUCKET", "CE_INTERVAL_H", "ALERT_TOPIC_ARN", "ACCOUNT_ID", "AWS_REGION", "TZ"}


# --------------------------------------------------------------------------------------- log line parsing --
def who(req):
    if "assumed-role" in req and req.count("/") >= 2:
        return req.split("/")[-1]
    if ":user/" in req:
        return "iam-user:" + req.split("/")[-1]
    if req == "-":
        return "anonymous"
    return req.split(":")[-1][:60]


def ref_host(ref):
    m = re.match(r'^https?://([^/:?#]+)', ref or "")
    return m.group(1) if m else "-"


def net(ip, parts):
    p = (ip or "").split(".")
    return ".".join(p[:parts]) + ".*" if len(p) == 4 else (ip or "?")[:19]


def parse_line(line):
    """-> dict for a GET.OBJECT line or None."""
    m = LOG_RE.match(line)
    if not m:
        return None
    op = m.group(7)
    if op not in ("REST.GET.OBJECT", "WEBSITE.GET.OBJECT"):
        return None
    sent = int(m.group(12)) if m.group(12).isdigit() else 0
    t = m.group(3)
    hour = t[12:14] if len(t) > 14 else "??"
    return {"who": who(m.group(5)), "ip": m.group(4), "key": unquote(m.group(8)), "status": m.group(10),
            "bytes": sent, "size": int(m.group(13)) if m.group(13).isdigit() else 0, "hour": hour,
            "ref": ref_host(m.group(16)), "ua": (m.group(17) or "-")[:90]}


def selftest():
    s = ('abc justhodl-dashboard-live [20/Oct/2026:14:05:12 +0000] 17.166.5.9 - REQ1 REST.GET.OBJECT data/best-setups.json '
         '"GET /data/best-setups.json?t=1 HTTP/1.1" 200 - 5242880 5242880 40 38 "https://justhodl.ai/cockpit.html" '
         '"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari" - HOST SigV4 TLS - justhodl-dashboard-live.s3.amazonaws.com TLSv1.3 -')
    p = parse_line(s)
    assert p and p["who"] == "anonymous" and p["bytes"] == 5242880 and p["hour"] == "14" and p["ref"] == "justhodl.ai", p
    assert net("17.166.5.9", 3) == "17.166.5.*" and net("17.166.5.9", 2) == "17.166.*"
    lam = s.replace("17.166.5.9 -", "10.0.0.1 arn:aws:sts::857687956942:assumed-role/lambda-execution-role/justhodl-ai")
    assert parse_line(lam)["who"] == "justhodl-ai"
    assert parse_line(s.replace("REST.GET.OBJECT", "REST.PUT.OBJECT")) is None
    assert parse_line("garbage") is None
    print("selftest ok")


# ------------------------------------------------------------------------------------------------ plumbing --
def make_clients():
    import boto3
    from botocore.config import Config
    cfg = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=60,
                 max_pool_connections=48)
    cl = {}

    def c(name):
        if name not in cl:
            cl[name] = boto3.client(name, region_name=REGION, config=cfg)
        return cl[name]
    return c


def err(e):
    resp = getattr(e, "response", None)
    if resp:
        x = resp.get("Error", {})
        return "%s: %s" % (x.get("Code"), str(x.get("Message") or "")[:200])
    return "%s: %s" % (type(e).__name__, str(e)[:200])


def safe(label, fn, default=None):
    try:
        return fn()
    except Exception as e:  # noqa: BLE001 - a probe records what it could not read and keeps going
        D["errors"].append("%s: %s" % (label, err(e)))
        print("[probe] %s failed: %s" % (label, err(e)), flush=True)
        return default


def mask_ep(ep):
    ep = ep or ""
    if "@" in ep:
        user, dom = ep.split("@", 1)
        return user[:2] + "***@" + dom
    return ep.split(":")[-1][:40] if ep.startswith("arn:") else (ep[:8] + "***" if ep else "")


# ------------------------------------------------------------------------------------------ 1. access logs --
def list_day(c, tb, base, day):
    """All log keys for one UTC day, listed as 16 parallel hex-prefix shards."""
    stamp = day.replace("/", "-") + "-00-00-00-"

    def shard(ch):
        keys, token = [], None
        while True:
            kw = {"Bucket": tb, "Prefix": "%s%s/%s%s" % (base, day, stamp, ch), "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            resp = c("s3").list_objects_v2(**kw)
            keys += [o["Key"] for o in resp.get("Contents", [])]
            if not resp.get("IsTruncated"):
                return keys
            token = resp.get("NextContinuationToken")
    out = []
    with cf.ThreadPoolExecutor(max_workers=16) as ex:
        for ks in ex.map(shard, "0123456789ABCDEF"):
            out += ks
    return sorted(out)


def analyse_day(r, c, tb, base, day):
    t0 = time.time()
    keys = list_day(c, tb, base, day)
    if not keys:
        r.warn("%s: no log objects" % day)
        return None
    step = max(1, len(keys) // SAMPLE)
    sample = keys[::step][:SAMPLE]
    scale = len(keys) / float(len(sample))

    def read(k):
        try:
            return c("s3").get_object(Bucket=tb, Key=k)["Body"].read().decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            return ""

    by_who, anon_ua, anon_ref, anon_ip24, anon_ip16, anon_key, anon_status, anon_hour = (Counter() for _ in range(8))
    anon_req, anon_ua_req = Counter(), Counter()
    drill = defaultdict(lambda: {"bytes": 0, "req": 0, "keys": Counter(), "ua": Counter(), "ips": Counter()})
    nonanon_key = Counter()
    sized = []
    with cf.ThreadPoolExecutor(max_workers=40) as ex:
        for text in ex.map(read, sample):
            for line in text.splitlines():
                p = parse_line(line)
                if not p:
                    continue
                by_who[p["who"]] += p["bytes"]
                if p["who"] != "anonymous":
                    nonanon_key[p["key"]] += p["bytes"]
                    continue
                anon_ua[p["ua"]] += p["bytes"]
                anon_ua_req[p["ua"]] += 1
                anon_ref[p["ref"]] += p["bytes"]
                anon_ip24[net(p["ip"], 3)] += p["bytes"]
                anon_ip16[net(p["ip"], 2)] += p["bytes"]
                anon_key[p["key"]] += p["bytes"]
                anon_status[p["status"]] += 1
                anon_hour[p["hour"]] += p["bytes"]
                anon_req["n"] += 1
                d = drill[net(p["ip"], 3)]
                d["bytes"] += p["bytes"]
                d["req"] += 1
                d["keys"][p["key"]] += p["bytes"]
                d["ua"][p["ua"]] += p["bytes"]
                d["ips"][p["ip"]] += p["bytes"]
                if p["size"]:
                    sized.append(p["size"])
    k = scale
    tot = sum(by_who.values()) * k
    anon = by_who.get("anonymous", 0) * k
    res = {"log_objects": len(keys), "sampled": len(sample), "scale": round(scale, 2),
           "get_gb_total": round(tot / 1e9, 1), "get_gb_anonymous": round(anon / 1e9, 1),
           "anon_requests": int(anon_req["n"] * k),
           "by_requester_gb": [(w, round(b * k / 1e9, 2)) for w, b in by_who.most_common(10)],
           "anon_ua_gb": [(u, round(b * k / 1e9, 2), int(anon_ua_req[u] * k)) for u, b in anon_ua.most_common(12)],
           "anon_ref_gb": [(u, round(b * k / 1e9, 2)) for u, b in anon_ref.most_common(8)],
           "anon_net24_gb": [(u, round(b * k / 1e9, 2)) for u, b in anon_ip24.most_common(12)],
           "anon_net16_gb": [(u, round(b * k / 1e9, 2)) for u, b in anon_ip16.most_common(8)],
           "anon_key_gb": [(u, round(b * k / 1e9, 2)) for u, b in anon_key.most_common(20)],
           "anon_status": dict(anon_status.most_common(8)),
           "anon_hour_gb": [(h, round(anon_hour[h] * k / 1e9, 1)) for h in sorted(anon_hour)],
           "nonanon_key_gb": [(u, round(b * k / 1e9, 2)) for u, b in nonanon_key.most_common(6)],
           "anon_mean_kb": round(sum(sized) / len(sized) / 1024, 1) if sized else None,
           "drill": {}}
    for n24, dd in sorted(drill.items(), key=lambda kv: -kv[1]["bytes"])[:6]:
        res["drill"][n24] = {"gb": round(dd["bytes"] * k / 1e9, 2), "requests": int(dd["req"] * k),
                             "top_keys": [(a, round(b * k / 1e9, 2)) for a, b in dd["keys"].most_common(5)],
                             "top_ua": [(a, round(b * k / 1e9, 2)) for a, b in dd["ua"].most_common(3)],
                             "top_ips": [(a, round(b * k / 1e9, 2)) for a, b in dd["ips"].most_common(4)]}
    D.setdefault("egress", {})[day] = res
    r.log("%s: %d log objects (sampled %d, x%.1f, %.0fs) GET %.1f GB, anonymous %.1f GB in ~%d requests (mean %s KB)" % (
        day, len(keys), len(sample), scale, time.time() - t0, res["get_gb_total"], res["get_gb_anonymous"],
        res["anon_requests"], res["anon_mean_kb"]))
    r.log("    by requester GB: %s" % res["by_requester_gb"][:5])
    r.log("    anonymous by user agent (GB, requests): %s" % [(u[:48], g, n) for u, g, n in res["anon_ua_gb"][:6]])
    r.log("    anonymous by referer host: %s" % res["anon_ref_gb"][:5])
    r.log("    anonymous by network /24: %s" % res["anon_net24_gb"][:6])
    r.log("    anonymous by network /16: %s" % res["anon_net16_gb"][:5])
    r.log("    anonymous top keys: %s" % res["anon_key_gb"][:10])
    r.log("    anonymous HTTP status: %s ; by UTC hour GB: %s" % (res["anon_status"], res["anon_hour_gb"]))
    for n24, dd in list(res["drill"].items())[:3]:
        r.log("    drill %s: %s GB %s req | keys %s | ua %s | ips %s" % (
            n24, dd["gb"], dd["requests"], dd["top_keys"][:3], [(u[:40], g) for u, g in dd["top_ua"][:2]], dd["top_ips"][:2]))
    return res


def logs(r, c):
    r.section("1. S3 access logs of %s" % BUCKET)
    lg = safe("get_bucket_logging", lambda: c("s3").get_bucket_logging(Bucket=BUCKET).get("LoggingEnabled") or {}, {})
    tb, tp = lg.get("TargetBucket"), lg.get("TargetPrefix") or ""
    if not tb:
        r.warn("access logging is off - nothing to analyse")
        return
    base = "%s%s/%s/%s/" % (tp, ACCOUNT, REGION, BUCKET)
    for day in DAYS:
        try:
            analyse_day(r, c, tb, base, day)
        except Exception as e:  # noqa: BLE001
            D["errors"].append("logs %s: %s" % (day, err(e)))
            r.warn("%s failed: %s" % (day, err(e)))
    # head the biggest keys seen on the worst day
    worst = max((v for v in D.get("egress", {}).values()), key=lambda v: v["get_gb_anonymous"], default=None)
    if not worst:
        return
    r.section("2. The biggest keys: size and cacheability")
    heads = {}
    for key, gb in worst["anon_key_gb"][:14]:
        h = safe("head %s" % key, lambda key=key: c("s3").head_object(Bucket=BUCKET, Key=key), None)
        if h:
            heads[key] = {"gb_per_day": gb, "mb": round(h["ContentLength"] / 1e6, 2), "cache_control": h.get("CacheControl"),
                          "content_encoding": h.get("ContentEncoding"), "content_type": h.get("ContentType"),
                          "last_modified": str(h.get("LastModified"))[:19], "storage_class": h.get("StorageClass", "STANDARD")}
            r.log("%s: %s" % (key, heads[key]))
    D["heads"] = heads


# --------------------------------------------------------------------------------------- 3. bucket access --
def policy_summary(doc):
    out = []
    stmts = doc.get("Statement", [])
    stmts = stmts if isinstance(stmts, list) else [stmts]
    for s in stmts:
        princ = s.get("Principal")
        out.append({"sid": s.get("Sid"), "effect": s.get("Effect"),
                    "principal": princ if isinstance(princ, str) else json.dumps(princ, sort_keys=True)[:120],
                    "action": s.get("Action"), "resource": s.get("Resource"),
                    "condition_keys": {k: sorted(v.keys()) for k, v in (s.get("Condition") or {}).items()}})
    return out


def bucket_access(r, c):
    r.section("3. How %s is public" % BUCKET)
    pol = safe("bucket policy", lambda: json.loads(c("s3").get_bucket_policy(Bucket=BUCKET)["Policy"]), None)
    D["bucket_policy"] = policy_summary(pol) if pol else None
    r.log("policy statements: %s" % json.dumps(D["bucket_policy"], default=str)[:3500])
    D["pab"] = safe("public access block", lambda: c("s3").get_public_access_block(Bucket=BUCKET)["PublicAccessBlockConfiguration"], None)
    r.log("public access block: %s" % D["pab"])
    D["cors"] = safe("cors", lambda: c("s3").get_bucket_cors(Bucket=BUCKET)["CORSRules"], None)
    r.log("cors: %s" % json.dumps(D["cors"])[:600])
    web = safe("website", lambda: c("s3").get_bucket_website(Bucket=BUCKET), None)
    r.log("website config: %s" % (json.dumps({k: v for k, v in (web or {}).items() if k != "ResponseMetadata"})[:300]))


# -------------------------------------------------------------------------------------- 4. governor wiring --
def governor(r, c):
    r.section("4. Spend governor wiring (what a $600 cap touches)")
    fc = safe("governor config", lambda: c("lambda").get_function_configuration(FunctionName="justhodl-spend-governor"), {})
    env = (fc.get("Environment") or {}).get("Variables") or {}
    D["gov_cfg"] = {"env": {k: (v if k in SAFE_ENV else "<set>") for k, v in env.items()}, "memory": fc.get("MemorySize"),
                    "timeout": fc.get("Timeout"), "role": (fc.get("Role") or "").split("/")[-1],
                    "last_modified": fc.get("LastModified"), "code_sha": fc.get("CodeSha256")}
    r.log("governor: %s" % D["gov_cfg"])
    conc = safe("governor concurrency", lambda: c("lambda").get_function_concurrency(FunctionName="justhodl-spend-governor"), {})
    r.log("governor reserved concurrency: %s" % conc.get("ReservedConcurrentExecutions"))
    role = D["gov_cfg"]["role"]
    pols = {}

    def keep(stmts):
        stmts = stmts if isinstance(stmts, list) else [stmts]
        return [{"effect": s.get("Effect"), "action": s.get("Action"), "resource": s.get("Resource")} for s in stmts
                if any(w in json.dumps([s.get("Action"), s.get("Resource")]).lower() for w in ("budgets", "scheduler", "sns:"))]
    for n in safe("role inline policies", lambda: c("iam").list_role_policies(RoleName=role)["PolicyNames"], []):
        doc = safe("role policy %s" % n, lambda n=n: c("iam").get_role_policy(RoleName=role, PolicyName=n)["PolicyDocument"], None)
        if doc:
            pols["inline:" + n] = keep(doc.get("Statement"))
    for ap in safe("role attached policies", lambda: c("iam").list_attached_role_policies(RoleName=role)["AttachedPolicies"], []):
        def get_doc(ap=ap):
            v = c("iam").get_policy(PolicyArn=ap["PolicyArn"])["Policy"]["DefaultVersionId"]
            return c("iam").get_policy_version(PolicyArn=ap["PolicyArn"], VersionId=v)["PolicyVersion"]["Document"]
        doc = safe("attached policy %s" % ap["PolicyName"], get_doc, None)
        if doc:
            pols["attached:" + ap["PolicyName"]] = keep(doc.get("Statement"))
    D["gov_role_perms"] = pols
    r.log("governor role statements mentioning budgets/scheduler/sns: %s" % json.dumps(pols, default=str)[:3500])
    for tn in ("justhodl-spend-governor", "justhodl-fleet-alerts"):
        arn = "arn:aws:sns:%s:%s:%s" % (REGION, ACCOUNT, tn)
        attrs = safe("topic %s" % tn, lambda arn=arn: c("sns").get_topic_attributes(TopicArn=arn)["Attributes"], {})
        pol = json.loads(attrs.get("Policy") or "{}")
        subs = [(s.get("Protocol"), mask_ep(s.get("Endpoint")), "PENDING" if s.get("SubscriptionArn") == "PendingConfirmation" else "confirmed")
                for s in safe("subs %s" % tn, lambda arn=arn: c("sns").list_subscriptions_by_topic(TopicArn=arn).get("Subscriptions", []), [])]
        D.setdefault("topics", {})[tn] = {"policy": policy_summary(pol), "subs": subs,
                                          "confirmed": attrs.get("SubscriptionsConfirmed"), "pending": attrs.get("SubscriptionsPending"),
                                          "kms": attrs.get("KmsMasterKeyId")}
        r.log("topic %s: subs %s | confirmed=%s pending=%s | policy %s" % (
            tn, subs, attrs.get("SubscriptionsConfirmed"), attrs.get("SubscriptionsPending"),
            json.dumps(policy_summary(pol), default=str)[:1800]))
    b = safe("budget", lambda: c("budgets").describe_budget(AccountId=ACCOUNT, BudgetName="JustHodl-Monthly-Hard-Cap-500")["Budget"], {})
    D["budget_def"] = json.loads(json.dumps(b, default=str))
    r.log("budget JustHodl-Monthly-Hard-Cap-500 definition: %s" % json.dumps(D["budget_def"], default=str)[:1800])
    sch = safe("tick schedule", lambda: c("scheduler").get_schedule(Name="justhodl-spend-governor-tick"), {})
    D["tick"] = {k: str(sch.get(k)) for k in ("ScheduleExpression", "State", "GroupName")}
    D["tick"]["target"] = {k: v for k, v in (sch.get("Target") or {}).items() if k in ("Arn", "RoleArn", "Input")}
    r.log("tick schedule: %s" % D["tick"])
    alarms = []
    for page in safe("alarms", lambda: list(c("cloudwatch").get_paginator("describe_alarms").paginate(AlarmNamePrefix="justhodl-")), []):
        alarms += page.get("MetricAlarms", [])
    D["alarms"] = [{"name": a["AlarmName"], "state": a["StateValue"], "actions": [x.split(":")[-1] for x in a.get("AlarmActions", [])]}
                   for a in alarms if any(w in a["AlarmName"] for w in ("runaway", "guard", "governor", "breaker", "bucket-size"))]
    r.log("guard alarms (%d of %d justhodl-* alarms): %s" % (len(D["alarms"]), len(alarms), D["alarms"]))


# ---------------------------------------------------------------------------------- 5. schedule plumbing --
def schedules(r, c):
    r.section("5. Schedule policy plumbing")
    params = safe("ssm schedules", lambda: c("ssm").get_parameters_by_path(Path="/justhodl/schedules", Recursive=True)["Parameters"], [])
    D["ssm_schedules"] = {p["Name"]: p["Value"][:200] for p in params}
    r.log("SSM /justhodl/schedules: %s" % D["ssm_schedules"])
    for fn in ("justhodl-schedule-reconciler", "justhodl-schedule-liveness", "justhodl-chain-resumer", "justhodl-scheduler"):
        fc = safe("cfg %s" % fn, lambda fn=fn: c("lambda").get_function_configuration(FunctionName=fn), {})
        env = (fc.get("Environment") or {}).get("Variables") or {}
        conc = safe("conc %s" % fn, lambda fn=fn: c("lambda").get_function_concurrency(FunctionName=fn), {})
        D.setdefault("sched_fns", {})[fn] = {"env_names": sorted(env), "mem": fc.get("MemorySize"), "timeout": fc.get("Timeout"),
                                             "reserved": conc.get("ReservedConcurrentExecutions"), "modified": fc.get("LastModified")}
        r.log("%s: %s" % (fn, D["sched_fns"][fn]))


# ------------------------------------------------------------------------------------- 6. CW API call mix --
def cw_usage(r, c):
    r.section("6. CloudWatch API call counts (AWS/Usage)")
    ms = []
    for page in safe("list usage metrics", lambda: list(c("cloudwatch").get_paginator("list_metrics").paginate(Namespace="AWS/Usage")), []):
        ms += page.get("Metrics", [])
    cw = [m for m in ms if any(d["Name"] == "Service" and d["Value"] == "CloudWatch" for d in m["Dimensions"])]
    r.log("AWS/Usage metrics: %d total, %d for service CloudWatch" % (len(ms), len(cw)))
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    rows = []
    for m in cw[:30]:
        st = safe("usage stat", lambda m=m: c("cloudwatch").get_metric_statistics(
            Namespace="AWS/Usage", MetricName=m["MetricName"], Dimensions=m["Dimensions"], StartTime=end - timedelta(days=14),
            EndTime=end, Period=86400, Statistics=["Sum"]).get("Datapoints", []), [])
        res = [d["Value"] for d in m["Dimensions"] if d["Name"] == "Resource"]
        rows.append({"resource": res[0] if res else "?", "metric": m["MetricName"],
                     "daily": sorted([(str(p["Timestamp"])[:10], int(p["Sum"])) for p in st])})
    D["cw_usage"] = rows
    for x in sorted(rows, key=lambda x: -sum(v for _, v in x["daily"]))[:12]:
        r.log("  %s %s: %s" % (x["resource"], x["metric"], x["daily"][-8:]))


def main():
    from ops_report import report
    c = make_clients()
    with report("ops_6547_egress_and_config_probe") as r:
        r.heading("ops 6547 - egress forensics + config probe (read-only)")
        t = time.time()
        for step in (logs, bucket_access, governor, schedules, cw_usage):
            try:
                step(r, c)
            except Exception as e:  # noqa: BLE001
                D["errors"].append("%s: %s" % (step.__name__, err(e)))
                r.warn("%s failed: %s" % (step.__name__, err(e)))
            r.log("elapsed %.0fs" % (time.time() - t))
        blob = gzip.compress(json.dumps(D, default=str, separators=(",", ":")).encode(), 9)
        safe("put private", lambda: c("s3").put_object(Bucket=PRIVATE, Key="audit/ops6547.json.gz", Body=blob, ServerSideEncryption="AES256"))
        repo = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
        out = repo / "aws" / "ops" / "reports" / "latest" / "ops_6547_egress_and_config_probe_data.md"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("# ops 6547 data (gzip+base64 JSON)\n\n```\n" + base64.b64encode(blob).decode() + "\n```\n")
        r.log("data: %d KB compressed -> %s" % (len(blob) // 1024, out.name))
        if D["errors"]:
            r.log("errors recorded: %d %s" % (len(D["errors"]), D["errors"][:6]))
        r.ok("probe complete")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
