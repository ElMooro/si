"""justhodl-spend-governor - AWS levers.

Every lever follows the same contract so the governor can always undo exactly what it did:

    discover(ctx, stage)  -> entries to act on, each carrying the BEFORE state read from AWS
    apply(ctx, entry)     -> mutate (skipped in monitor mode by the caller)
    restore(ctx, entry)   -> put BEFORE back, but only if the resource is still in the state the
                             governor left it in (a human change made since then always wins)

Entries live in the state ledger keyed "<lever>:<id>".  Levers whose effect cannot be undone
(stopping a SageMaker job, deleting a Studio app) are marked restorable=False and reported.
"""
import concurrent.futures as cf

from botocore.exceptions import ClientError

ASYNC_SUSPEND = {"DynamicScalingInSuspended": False, "DynamicScalingOutSuspended": True,
                 "ScheduledScalingSuspended": True}
NO_SUSPEND = {"DynamicScalingInSuspended": False, "DynamicScalingOutSuspended": False,
              "ScheduledScalingSuspended": False}
DIM = "sagemaker:variant:DesiredInstanceCount"


def err_code(e):
    if isinstance(e, ClientError):
        return e.response.get("Error", {}).get("Code", "ClientError")
    return type(e).__name__


def err_msg(e):
    """The service's own explanation (a code alone - 'ValidationException' - says nothing actionable)."""
    if isinstance(e, ClientError):
        return str(e.response.get("Error", {}).get("Message") or "")[:300]
    return str(e)[:300]


def entry(lever, rid, stage, before, restorable=True, **extra):
    d = {"key": "%s:%s" % (lever, rid), "lever": lever, "id": rid, "stage": stage, "before": before,
         "restorable": restorable, "status": "planned"}
    d.update(extra)
    return d


# ------------------------------------------------------------------ Lambda --
def list_functions(ctx):
    if ctx.cache.get("functions") is None:
        fns = []
        for page in ctx.c("lambda").get_paginator("list_functions").paginate():
            for f in page["Functions"]:
                fns.append({"name": f["FunctionName"], "mem": f.get("MemorySize") or 128,
                            "arch": (f.get("Architectures") or ["x86_64"])[0]})
        ctx.cache["functions"] = fns
    return ctx.cache["functions"]


def reserved_of(ctx, fn):
    return ctx.c("lambda").get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")


def lambda_snapshot(ctx, names):
    """{fn: reserved or None} read concurrently; functions that vanished are dropped.

    Lambda's control plane allows ~15 requests/s per account, shared with every other caller, so a
    ~900-function snapshot takes a minute or more.  Past the time guard the remaining names are left
    for the next tick instead of letting discovery eat the whole invocation."""
    def one(fn):
        if ctx.time_left() < 240:
            return fn, None, "deferred"
        try:
            return fn, reserved_of(ctx, fn), None
        except Exception as e:  # noqa: BLE001
            return fn, None, err_code(e)
    out, errs = {}, {}
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        for fn, val, err in ex.map(one, names):
            if err in ("ResourceNotFoundException", "deferred"):
                continue
            if err:
                errs[fn] = err
            else:
                out[fn] = val
    return out, errs


def lambda_discover(ctx, stage, names):
    """Entries for `names` that are not already throttled to 0 and not already in the ledger."""
    todo = [n for n in names if not ctx.exempt(n) and ("lambda:" + n) not in ctx.ledger]
    snap, errs = lambda_snapshot(ctx, todo)
    for fn, err in errs.items():
        ctx.log("lambda snapshot %s failed: %s" % (fn, err))
    return [entry("lambda", fn, stage, {"reserved": r}) for fn, r in sorted(snap.items()) if r != 0]


def lambda_apply(ctx, e):
    ctx.c("lambda").put_function_concurrency(FunctionName=e["id"], ReservedConcurrentExecutions=0)


def lambda_is_applied(ctx, e):
    return reserved_of(ctx, e["id"]) == 0


def lambda_restore(ctx, e):
    cur = reserved_of(ctx, e["id"])
    if cur != 0:
        return "skipped_changed"
    before = (e.get("before") or {}).get("reserved")
    if before is None:
        ctx.c("lambda").delete_function_concurrency(FunctionName=e["id"])
    else:
        ctx.c("lambda").put_function_concurrency(FunctionName=e["id"], ReservedConcurrentExecutions=int(before))
    return "restored"


# ------------------------------------------------- event source mappings --
def esm_discover(ctx, stage):
    if stage != "FREEZE":
        return []
    out = []
    for page in ctx.c("lambda").get_paginator("list_event_source_mappings").paginate():
        for m in page["EventSourceMappings"]:
            fn = (m.get("FunctionArn") or "").split(":function:")[-1].split(":")[0]
            if m.get("State") == "Enabled" and not ctx.exempt(fn) and ("esm:" + m["UUID"]) not in ctx.ledger:
                out.append(entry("esm", m["UUID"], stage, {"state": "Enabled", "function": fn,
                                                           "source": m.get("EventSourceArn")}))
    return out


def esm_apply(ctx, e):
    ctx.c("lambda").update_event_source_mapping(UUID=e["id"], Enabled=False)


def esm_is_applied(ctx, e):
    return ctx.c("lambda").get_event_source_mapping(UUID=e["id"]).get("State") in ("Disabled", "Disabling")


def esm_restore(ctx, e):
    if not esm_is_applied(ctx, e):
        return "skipped_changed"
    ctx.c("lambda").update_event_source_mapping(UUID=e["id"], Enabled=True)
    return "restored"


# ---------------------------------------------------------------- SageMaker --
JOB_KINDS = (
    ("training", "list_training_jobs", "TrainingJobSummaries", "TrainingJobName", "stop_training_job"),
    ("processing", "list_processing_jobs", "ProcessingJobSummaries", "ProcessingJobName", "stop_processing_job"),
    ("transform", "list_transform_jobs", "TransformJobSummaries", "TransformJobName", "stop_transform_job"),
    ("hpo", "list_hyper_parameter_tuning_jobs", "HyperParameterTuningJobSummaries",
     "HyperParameterTuningJobName", "stop_hyper_parameter_tuning_job"),
)


def smjob_discover(ctx, stage):
    sm, out = ctx.c("sagemaker"), []
    for kind, lister, field, name_key, _ in JOB_KINDS:
        for j in getattr(sm, lister)(StatusEquals="InProgress", MaxResults=100).get(field, []):
            rid = "%s/%s" % (kind, j[name_key])
            if ("smjob:" + rid) not in ctx.ledger:
                out.append(entry("smjob", rid, stage, {"kind": kind}, restorable=False))
    return out


def smjob_apply(ctx, e):
    kind, name = e["id"].split("/", 1)
    stopper = {k[0]: k for k in JOB_KINDS}[kind]
    getattr(ctx.c("sagemaker"), stopper[4])(**{stopper[3]: name})


def _endpoints(ctx):
    if ctx.cache.get("endpoints") is None:
        sm, eps = ctx.c("sagemaker"), []
        for page in sm.get_paginator("list_endpoints").paginate(StatusEquals="InService"):
            for ep in page["Endpoints"]:
                d = sm.describe_endpoint(EndpointName=ep["EndpointName"])
                cfg = sm.describe_endpoint_config(EndpointConfigName=d["EndpointConfigName"])
                eps.append({"name": ep["EndpointName"], "arn": d["EndpointArn"], "config": d["EndpointConfigName"],
                            "async": bool(cfg.get("AsyncInferenceConfig")),
                            "variants": [{"name": v["VariantName"],
                                          "serverless": bool(v.get("CurrentServerlessConfig") or v.get("ServerlessConfig")),
                                          "desired": v.get("DesiredInstanceCount", v.get("CurrentInstanceCount"))}
                                         for v in d.get("ProductionVariants", [])]})
        ctx.cache["endpoints"] = eps
    return ctx.cache["endpoints"]


def _scalable(ctx, ep, variant):
    rid = "endpoint/%s/variant/%s" % (ep, variant)
    t = ctx.c("application-autoscaling").describe_scalable_targets(
        ServiceNamespace="sagemaker", ResourceIds=[rid], ScalableDimension=DIM).get("ScalableTargets", [])
    return rid, (t[0] if t else None)


def smasync_discover(ctx, stage):
    """Async endpoints registered for scale-to-zero: suspend scale-out and drop to 0 instances."""
    out = []
    for ep in _endpoints(ctx):
        if not ep["async"]:
            continue
        for v in ep["variants"]:
            if v["serverless"]:
                continue
            rid, t = _scalable(ctx, ep["name"], v["name"])
            if t is None or int(t.get("MinCapacity", 1)) != 0 or ("smasync:" + rid) in ctx.ledger:
                continue
            out.append(entry("smasync", rid, stage, {"endpoint": ep["name"], "variant": v["name"],
                                                     "min": t["MinCapacity"], "max": t["MaxCapacity"],
                                                     "suspended": t.get("SuspendedState") or NO_SUSPEND,
                                                     "desired": v.get("desired")}))
    return out


def smasync_apply(ctx, e):
    """Suspend scale-OUT only: a queued request can no longer wake an instance, while scale-in stays live so a
    running instance still drains to 0 on its own.  The immediate drop to 0 is best effort and decided on the
    endpoint's CURRENT state, not the snapshot (a retry hours later must not resend a stale capacity update):
    SageMaker refuses capacity updates while the endpoint is Updating, and scale-in gets there anyway."""
    b = e["before"]
    ctx.c("application-autoscaling").register_scalable_target(
        ServiceNamespace="sagemaker", ResourceId=e["id"], ScalableDimension=DIM,
        MinCapacity=int(b["min"]), MaxCapacity=int(b["max"]), SuspendedState=ASYNC_SUSPEND)
    e.pop("scale_down_note", None)
    try:
        d = ctx.c("sagemaker").describe_endpoint(EndpointName=b["endpoint"])
        v = next((x for x in d.get("ProductionVariants", []) if x.get("VariantName") == b["variant"]), {})
        running = int(v.get("CurrentInstanceCount") or 0)
        if running > 0 and d.get("EndpointStatus") == "InService":
            ctx.c("sagemaker").update_endpoint_weights_and_capacities(
                EndpointName=b["endpoint"],
                DesiredWeightsAndCapacities=[{"VariantName": b["variant"], "DesiredInstanceCount": 0}])
            e["scaled_down_from"] = running
        elif running > 0:
            e["scale_down_note"] = "endpoint %s; scale-in drains it" % d.get("EndpointStatus")
    except Exception as ex:  # noqa: BLE001 - the suspension above is the part that matters
        e["scale_down_note"] = "%s: %s" % (err_code(ex), err_msg(ex))


def smasync_is_applied(ctx, e):
    _, t = _scalable(ctx, e["before"]["endpoint"], e["before"]["variant"])
    return bool(t) and (t.get("SuspendedState") or {}).get("DynamicScalingOutSuspended") is True


def smasync_restore(ctx, e):
    b = e["before"]
    _, t = _scalable(ctx, b["endpoint"], b["variant"])
    if t is None:
        return "skipped_gone"
    ctx.c("application-autoscaling").register_scalable_target(
        ServiceNamespace="sagemaker", ResourceId=e["id"], ScalableDimension=DIM,
        MinCapacity=int(b["min"]), MaxCapacity=int(b["max"]), SuspendedState=b.get("suspended") or NO_SUSPEND)
    return "restored"


def smrt_discover(ctx, stage):
    """Instance-backed real-time endpoints (no scale-to-zero): delete, keep the config to re-create."""
    out = []
    for ep in _endpoints(ctx):
        inst = [v for v in ep["variants"] if not v["serverless"]]
        if not inst or ("smrt:" + ep["name"]) in ctx.ledger:
            continue
        if ep["async"] and all(_scalable(ctx, ep["name"], v["name"])[1] and
                               int(_scalable(ctx, ep["name"], v["name"])[1].get("MinCapacity", 1)) == 0 for v in inst):
            continue  # handled by smasync
        tags = ctx.c("sagemaker").list_tags(ResourceArn=ep["arn"]).get("Tags", [])
        out.append(entry("smrt", ep["name"], stage, {"config": ep["config"], "tags": tags}))
    return out


def smrt_apply(ctx, e):
    ctx.c("sagemaker").delete_endpoint(EndpointName=e["id"])


def _endpoint_exists(ctx, name):
    try:
        ctx.c("sagemaker").describe_endpoint(EndpointName=name)
        return True
    except ClientError as ex:
        if err_code(ex) in ("ValidationException", "ResourceNotFound"):
            return False
        raise


def smrt_is_applied(ctx, e):
    return not _endpoint_exists(ctx, e["id"])


def smrt_restore(ctx, e):
    if _endpoint_exists(ctx, e["id"]):
        return "skipped_changed"
    try:
        ctx.c("sagemaker").describe_endpoint_config(EndpointConfigName=e["before"]["config"])
    except ClientError:
        return "skipped_config_gone"
    kw = {"EndpointName": e["id"], "EndpointConfigName": e["before"]["config"]}
    if e["before"].get("tags"):
        kw["Tags"] = e["before"]["tags"]
    ctx.c("sagemaker").create_endpoint(**kw)
    return "restored"


def smnb_discover(ctx, stage):
    out = []
    for nb in ctx.c("sagemaker").list_notebook_instances(StatusEquals="InService").get("NotebookInstances", []):
        n = nb["NotebookInstanceName"]
        if ("smnb:" + n) not in ctx.ledger:
            out.append(entry("smnb", n, stage, {"status": "InService"}))
    return out


def smnb_apply(ctx, e):
    ctx.c("sagemaker").stop_notebook_instance(NotebookInstanceName=e["id"])


def smnb_is_applied(ctx, e):
    st = ctx.c("sagemaker").describe_notebook_instance(NotebookInstanceName=e["id"]).get("NotebookInstanceStatus")
    return st in ("Stopping", "Stopped")


def smnb_restore(ctx, e):
    st = ctx.c("sagemaker").describe_notebook_instance(NotebookInstanceName=e["id"]).get("NotebookInstanceStatus")
    if st != "Stopped":
        return "skipped_changed"
    ctx.c("sagemaker").start_notebook_instance(NotebookInstanceName=e["id"])
    return "restored"


def smapp_discover(ctx, stage):
    out = []
    for a in ctx.c("sagemaker").list_apps(MaxResults=100).get("Apps", []):
        if a.get("Status") != "InService" or a.get("AppType") == "JupyterServer":
            continue
        rid = "%s/%s/%s/%s" % (a["DomainId"], a.get("UserProfileName") or ("space:" + str(a.get("SpaceName"))),
                               a["AppType"], a["AppName"])
        if ("smapp:" + rid) not in ctx.ledger:
            out.append(entry("smapp", rid, stage, {k: a.get(k) for k in ("DomainId", "UserProfileName", "SpaceName",
                                                                           "AppType", "AppName")}, restorable=False))
    return out


def smapp_apply(ctx, e):
    kw = {k: v for k, v in e["before"].items() if v}
    ctx.c("sagemaker").delete_app(**kw)


# ------------------------------------------------------- EC2 / RDS / others --
def ec2_discover(ctx, stage):
    out = []
    pages = ctx.c("ec2").get_paginator("describe_instances").paginate(
        Filters=[{"Name": "instance-state-name", "Values": ["running"]}])
    for page in pages:
        for r in page["Reservations"]:
            for i in r["Instances"]:
                tags = {t["Key"]: t["Value"] for t in i.get("Tags", [])}
                if tags.get("jh:spend-governor") == "exempt" or ("ec2:" + i["InstanceId"]) in ctx.ledger:
                    continue
                out.append(entry("ec2", i["InstanceId"], stage, {"state": "running", "type": i.get("InstanceType")}))
    return out


def ec2_apply(ctx, e):
    ctx.c("ec2").stop_instances(InstanceIds=[e["id"]])


def _ec2_state(ctx, iid):
    r = ctx.c("ec2").describe_instances(InstanceIds=[iid])["Reservations"]
    return r[0]["Instances"][0]["State"]["Name"] if r else "gone"


def ec2_is_applied(ctx, e):
    return _ec2_state(ctx, e["id"]) in ("stopping", "stopped", "gone", "terminated")


def ec2_restore(ctx, e):
    if _ec2_state(ctx, e["id"]) != "stopped":
        return "skipped_changed"
    ctx.c("ec2").start_instances(InstanceIds=[e["id"]])
    return "restored"


def rds_discover(ctx, stage):
    rds, out = ctx.c("rds"), []
    for d in rds.describe_db_instances().get("DBInstances", []):
        if d.get("DBInstanceStatus") == "available" and not d.get("DBClusterIdentifier") \
                and ("rds:db/" + d["DBInstanceIdentifier"]) not in ctx.ledger:
            out.append(entry("rds", "db/" + d["DBInstanceIdentifier"], stage, {"status": "available"}))
    for c in rds.describe_db_clusters().get("DBClusters", []):
        if c.get("Status") == "available" and ("rds:cluster/" + c["DBClusterIdentifier"]) not in ctx.ledger:
            out.append(entry("rds", "cluster/" + c["DBClusterIdentifier"], stage, {"status": "available"}))
    return out


def _rds_status(ctx, rid):
    kind, name = rid.split("/", 1)
    if kind == "db":
        return ctx.c("rds").describe_db_instances(DBInstanceIdentifier=name)["DBInstances"][0]["DBInstanceStatus"]
    return ctx.c("rds").describe_db_clusters(DBClusterIdentifier=name)["DBClusters"][0]["Status"]


def rds_apply(ctx, e):
    kind, name = e["id"].split("/", 1)
    if kind == "db":
        ctx.c("rds").stop_db_instance(DBInstanceIdentifier=name)
    else:
        ctx.c("rds").stop_db_cluster(DBClusterIdentifier=name)


def rds_is_applied(ctx, e):
    return _rds_status(ctx, e["id"]) in ("stopping", "stopped")


def rds_restore(ctx, e):
    if _rds_status(ctx, e["id"]) != "stopped":
        return "skipped_changed"
    kind, name = e["id"].split("/", 1)
    if kind == "db":
        ctx.c("rds").start_db_instance(DBInstanceIdentifier=name)
    else:
        ctx.c("rds").start_db_cluster(DBClusterIdentifier=name)
    return "restored"


def apprunner_discover(ctx, stage):
    out = []
    for s in ctx.c("apprunner").list_services().get("ServiceSummaryList", []):
        if s.get("Status") == "RUNNING" and ("apprunner:" + s["ServiceArn"]) not in ctx.ledger:
            out.append(entry("apprunner", s["ServiceArn"], stage, {"name": s.get("ServiceName")}))
    return out


def apprunner_apply(ctx, e):
    ctx.c("apprunner").pause_service(ServiceArn=e["id"])


def _apprunner_status(ctx, arn):
    return ctx.c("apprunner").describe_service(ServiceArn=arn)["Service"]["Status"]


def apprunner_is_applied(ctx, e):
    return _apprunner_status(ctx, e["id"]) in ("PAUSED", "OPERATION_IN_PROGRESS")


def apprunner_restore(ctx, e):
    if _apprunner_status(ctx, e["id"]) != "PAUSED":
        return "skipped_changed"
    ctx.c("apprunner").resume_service(ServiceArn=e["id"])
    return "restored"


def ecs_discover(ctx, stage):
    ecs, out = ctx.c("ecs"), []
    for cl in ecs.list_clusters().get("clusterArns", []):
        arns = ecs.list_services(cluster=cl, maxResults=100).get("serviceArns", [])
        for i in range(0, len(arns), 10):
            for s in ecs.describe_services(cluster=cl, services=arns[i:i + 10]).get("services", []):
                rid = "%s|%s" % (cl, s["serviceArn"])
                if s.get("desiredCount", 0) > 0 and ("ecs:" + rid) not in ctx.ledger:
                    out.append(entry("ecs", rid, stage, {"desired": s["desiredCount"]}))
    return out


def _ecs_desired(ctx, rid):
    cl, svc = rid.split("|", 1)
    s = ctx.c("ecs").describe_services(cluster=cl, services=[svc]).get("services", [])
    return s[0].get("desiredCount") if s else None


def ecs_apply(ctx, e):
    cl, svc = e["id"].split("|", 1)
    ctx.c("ecs").update_service(cluster=cl, service=svc, desiredCount=0)


def ecs_is_applied(ctx, e):
    return _ecs_desired(ctx, e["id"]) in (0, None)


def ecs_restore(ctx, e):
    if _ecs_desired(ctx, e["id"]) != 0:
        return "skipped_changed"
    cl, svc = e["id"].split("|", 1)
    ctx.c("ecs").update_service(cluster=cl, service=svc, desiredCount=int(e["before"]["desired"]))
    return "restored"


# lever registry: name -> (discover, apply, is_applied, restore).  Lambda discovery needs a name list,
# so the governor calls lambda_discover directly; everything else is generic.
LEVERS = {
    "esm": (esm_discover, esm_apply, esm_is_applied, esm_restore),
    "smjob": (smjob_discover, smjob_apply, None, None),
    "smasync": (smasync_discover, smasync_apply, smasync_is_applied, smasync_restore),
    "smrt": (smrt_discover, smrt_apply, smrt_is_applied, smrt_restore),
    "smnb": (smnb_discover, smnb_apply, smnb_is_applied, smnb_restore),
    "smapp": (smapp_discover, smapp_apply, None, None),
    "ec2": (ec2_discover, ec2_apply, ec2_is_applied, ec2_restore),
    "rds": (rds_discover, rds_apply, rds_is_applied, rds_restore),
    "apprunner": (apprunner_discover, apprunner_apply, apprunner_is_applied, apprunner_restore),
    "ecs": (ecs_discover, ecs_apply, ecs_is_applied, ecs_restore),
}
LAMBDA = ("lambda", lambda_discover, lambda_apply, lambda_is_applied, lambda_restore)
# restore order: compute first so queues and schedules land on running functions
RESTORE_ORDER = ("lambda", "esm", "smasync", "smrt", "smnb", "ec2", "rds", "apprunner", "ecs")


def restore_fn(lever):
    if lever == "lambda":
        return lambda_restore
    return LEVERS.get(lever, (None, None, None, None))[3]


def is_applied_fn(lever):
    if lever == "lambda":
        return lambda_is_applied
    return LEVERS.get(lever, (None, None, None, None))[2]


def apply_fn(lever):
    if lever == "lambda":
        return lambda_apply
    return LEVERS.get(lever, (None, None, None, None))[1]
