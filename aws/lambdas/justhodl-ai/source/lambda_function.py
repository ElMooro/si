"""justhodl-ai v1.0.0 -- AI: the front window to Amazon SageMaker.

Khalid's 2026-09-09 directive: "im signed up for amazon sagemaker but i want a front end window to
train it ... create an engine called ai and a page for it called ai.html ... check for already
existing sagemaker and preload them ... have it learn from my justhodl brain ... upgrade my
sagemaker to the highest most powerful ai training system they have."

What this engine is:
  * INVENTORY -- every SageMaker resource in the account, live, every hour and on demand: Studio
    domains/apps, endpoints (+variants, tags, invocations), models, training jobs, AutoML jobs,
    notebook instances, feature groups, HyperPod clusters; plus the JumpStart public-hub catalog of
    financial / text-embedding cards (the article's RoBERTa-SEC family first). Written to
    data/ai.json -- the page's read model (no note text, no secrets).
  * CONTROL PLANE -- deploy a hub card to an endpoint (serverless by default), build the Brain
    dataset, embed it, train (four tiers: transfer-learning classifier, JumpStart fine-tune,
    Autopilot text/tabular, HyperPod gated), deploy trained models, run inference / retrieval,
    stop jobs, delete endpoints, edit the cost policy. Reached only through the data-proxy
    worker (`/ai/*`, owner role) which presents the service token -- the Function URL itself
    answers 401 to everything else.
  * COST GUARD -- live Price List pricing, daily budget, instance allow-lists, endpoint TTL +
    idle deletion, MaxRuntime on every job, no self-invocation (chain-guard doctrine).

Outputs: data/ai.json (public read model), data/ai/control.json (worker pointer),
private bucket ai/{policy,pricing,catalog,datasets,models,jobs}.
"""
from __future__ import annotations

import base64
import json
import os
import re
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
from botocore.config import Config

import brain_dataset as bd
import cost_guard as cg
import market_read as mr
import sm_hub
import training as tr

try:
    from private_artifact import private_http_denied  # shared: service-token gate for Function URL calls
except Exception:  # pragma: no cover - tests import without the shared bundle
    private_http_denied = None

VERSION = "1.2.1"
ENGINE = "justhodl-ai"
REGION = "us-east-1"
PUBLIC_BUCKET = os.environ.get("AI_PUBLIC_BUCKET", "justhodl-dashboard-live")
PRIVATE_BUCKET = os.environ.get("AI_PRIVATE_BUCKET", "justhodl-ai-857687956942")
OUT_KEY = "data/ai.json"
CONTROL_KEY = "data/ai/control.json"
CATALOG_KEY = "ai/catalog.json"
READ_KEY = "ai/market-read/latest.json"
CALLS_KEY = "ai/market-read/calls.json"
VERDICT_KEY = "data/ai/verdict.json"
SIGNALS_TABLE = os.environ.get("SIGNALS_TABLE", "justhodl-signals")
CFG = Config(retries={"max_attempts": 4, "mode": "adaptive"}, read_timeout=60)

_clients: Dict[str, Any] = {}


def client(name: str):
    if name not in _clients:
        kw = {"region_name": REGION, "config": CFG}
        _clients[name] = boto3.client(name, **kw)
    return _clients[name]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_json(bucket: str, key: str, default=None):
    try:
        return json.loads(client("s3").get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception:
        return default


def put_public(key: str, obj: Any):
    client("s3").put_object(Bucket=PUBLIC_BUCKET, Key=key, Body=json.dumps(obj, default=str).encode(), ContentType="application/json",
                            CacheControl="max-age=60")


def put_private(key: str, obj: Any):
    client("s3").put_object(Bucket=PRIVATE_BUCKET, Key=key, Body=json.dumps(obj, default=str).encode(), ContentType="application/json",
                            ServerSideEncryption="AES256", CacheControl="private, no-store")


def execution_role() -> str:
    r = os.environ.get("SAGEMAKER_ROLE_ARN", "")
    if r:
        return r
    ctl = get_json(PRIVATE_BUCKET, "ai/control.json") or {}
    return ctl.get("sagemaker_role_arn", "")


# ══════════════════════════════════════════════════════════════════ inventory
def _tags(arn: str) -> Dict[str, str]:
    try:
        return {t["Key"]: t["Value"] for t in (client("sagemaker").list_tags(ResourceArn=arn).get("Tags") or [])}
    except Exception:
        return {}


def inventory_endpoints() -> List[dict]:
    sm = client("sagemaker")
    out = []
    for e in (sm.list_endpoints(MaxResults=100, SortBy="CreationTime", SortOrder="Descending").get("Endpoints") or []):
        row = {"name": e["EndpointName"], "arn": e.get("EndpointArn"), "status": e.get("EndpointStatus"), "created_at": str(e.get("CreationTime")),
               "updated_at": str(e.get("LastModifiedTime")), "variants": [], "tags": _tags(e.get("EndpointArn", ""))}
        try:
            d = sm.describe_endpoint(EndpointName=e["EndpointName"])
            row["failure"] = (d.get("FailureReason") or "")[:200]
            for v in d.get("ProductionVariants") or []:
                row["variants"].append({"name": v.get("VariantName"), "instance_type": v.get("CurrentInstanceType") or v.get("InstanceType"),
                                        "instance_count": v.get("CurrentInstanceCount"), "serverless": bool(v.get("CurrentServerlessConfig")),
                                        "serverless_memory_mb": (v.get("CurrentServerlessConfig") or {}).get("MemorySizeInMB"),
                                        "image": ((v.get("DeployedImages") or [{}])[0] or {}).get("SpecifiedImage", "")})
            row["endpoint_config"] = d.get("EndpointConfigName")
        except Exception as ex:
            row["describe_error"] = str(ex)[:120]
        row["invocations_24h"] = cg.endpoint_invocations(client("cloudwatch"), row["name"], 24)
        out.append(row)
    return out


def inventory_snapshot(policy: Dict[str, Any]) -> Dict[str, Any]:
    sm = client("sagemaker")
    snap: Dict[str, Any] = {"errors": {}}

    def section(name, fn):
        try:
            snap[name] = fn()
        except Exception as e:
            snap[name] = None
            snap["errors"][name] = str(e)[:200]

    section("domains", lambda: [{"id": d.get("DomainId"), "name": d.get("DomainName"), "status": d.get("Status"), "created_at": str(d.get("CreationTime")),
                                 "url": d.get("Url")} for d in (sm.list_domains(MaxResults=100).get("Domains") or [])])
    section("apps", lambda: [{"name": a.get("AppName"), "type": a.get("AppType"), "status": a.get("Status"), "domain": a.get("DomainId"),
                              "user": a.get("UserProfileName") or a.get("SpaceName"), "created_at": str(a.get("CreationTime"))}
                             for a in (sm.list_apps(MaxResults=100).get("Apps") or [])])
    section("endpoints", inventory_endpoints)
    section("models", lambda: [{"name": m["ModelName"], "created_at": str(m.get("CreationTime"))} for m in
                               (sm.list_models(MaxResults=100, SortBy="CreationTime", SortOrder="Descending").get("Models") or [])])
    section("endpoint_configs", lambda: len(sm.list_endpoint_configs(MaxResults=100).get("EndpointConfigs") or []))
    section("jobs", lambda: tr.list_jobs(sm))
    section("notebooks", lambda: [{"name": n["NotebookInstanceName"], "status": n.get("NotebookInstanceStatus"), "instance_type": n.get("InstanceType")}
                                  for n in (sm.list_notebook_instances(MaxResults=100).get("NotebookInstances") or [])])
    section("feature_groups", lambda: [{"name": f["FeatureGroupName"], "status": f.get("FeatureGroupStatus"), "created_at": str(f.get("CreationTime"))}
                                       for f in (sm.list_feature_groups(MaxResults=100).get("FeatureGroupSummaries") or [])])
    section("clusters", lambda: [{"name": c.get("ClusterName"), "status": c.get("ClusterStatus"), "created_at": str(c.get("CreationTime"))}
                                 for c in (sm.list_clusters(MaxResults=100).get("ClusterSummaries") or [])])
    # job details for the live ones (instance types feed the run-rate)
    detailed = []
    for j in (snap.get("jobs") or [])[:40]:
        if j.get("kind") == "training" and j.get("status") in ("InProgress", "Stopping", "Completed", "Failed"):
            try:
                d = sm.describe_training_job(TrainingJobName=j["name"])
                rc = d.get("ResourceConfig") or {}
                j.update({"instance_type": rc.get("InstanceType"), "instance_count": rc.get("InstanceCount"), "spot": bool(d.get("EnableManagedSpotTraining")),
                          "billable_s": d.get("BillableTimeInSeconds"), "training_s": d.get("TrainingTimeInSeconds"),
                          "failure": (d.get("FailureReason") or "")[:200], "artifact": (d.get("ModelArtifacts") or {}).get("S3ModelArtifacts"),
                          "image": (d.get("AlgorithmSpecification") or {}).get("TrainingImage", "")[:120],
                          "metrics": [{"name": m.get("MetricName"), "value": m.get("Value")} for m in (d.get("FinalMetricDataList") or [])][:8]})
            except Exception as e:
                j["describe_error"] = str(e)[:100]
        detailed.append(j)
    snap["jobs"] = detailed
    return snap


def run_inventory(context=None, *, refresh_catalog: bool = False, continue_embeddings: bool = True) -> Dict[str, Any]:
    t0 = time.time()
    s3 = client("s3")
    policy = cg.load_policy(s3, PRIVATE_BUCKET)
    snap = inventory_snapshot(policy)
    # cost guard: TTL / idle enforcement on managed endpoints
    ledger = []
    if snap.get("endpoints"):
        ledger = cg.enforce_endpoint_ttl(client("sagemaker"), client("cloudwatch"), snap["endpoints"], policy)
        deleted = {r["endpoint"] for r in ledger if r["action"] == "deleted"}
        for ep in snap["endpoints"]:
            if ep["name"] in deleted:
                ep["status"] = "Deleting"
    price_fn = lambda it, fam: cg.hourly_price(client("pricing"), s3, PRIVATE_BUCKET, it, fam)
    projected = cg.projected_daily_usd(snap.get("endpoints") or [], snap.get("jobs") or [], price_fn)
    # catalog (daily)
    catalog = get_json(PRIVATE_BUCKET, CATALOG_KEY) or {}
    cat_age_h = None
    try:
        cat_age_h = (datetime.now(timezone.utc) - datetime.fromisoformat(catalog.get("generated_at"))).total_seconds() / 3600
    except Exception:
        pass
    if refresh_catalog or not catalog or cat_age_h is None or cat_age_h > 24:
        try:
            catalog = sm_hub.discover_models(client("sagemaker"))
            put_private(CATALOG_KEY, catalog)
        except Exception as e:
            catalog = catalog or {}
            catalog["refresh_error"] = str(e)[:200]
    # datasets + embedding continuation
    ds = bd.latest_dataset(s3, PRIVATE_BUCKET)
    passes = []
    if ds:
        live = {e["name"] for e in (snap.get("endpoints") or []) if e.get("status") == "InService"}
        for ep_name, meta in (ds.get("embeddings") or {}).items():
            passes.append({"endpoint": ep_name, "status": "complete", **{k: meta.get(k) for k in ("dim", "n_embedded", "completed_at")}})
        # running passes are discoverable by state.json under emb/
        try:
            pfx = "ai/datasets/brain/%s/emb/" % ds["dataset_id"]
            for o in (s3.list_objects_v2(Bucket=PRIVATE_BUCKET, Prefix=pfx).get("Contents") or []):
                if o["Key"].endswith("/state.json"):
                    st = get_json(PRIVATE_BUCKET, o["Key"]) or {}
                    if st.get("status") != "complete":
                        if continue_embeddings and st.get("endpoint") in live and context is not None:
                            budget = max(60.0, context.get_remaining_time_in_millis() / 1000.0 - 90.0)
                            st = bd.run_embedding_pass(s3, client("sagemaker-runtime"), PRIVATE_BUCKET, ds["dataset_id"], st["endpoint"], embed_fn=sm_hub.embed_texts, budget_s=min(budget, 600.0))
                            if st.get("status") == "complete" and st.get("auto_train"):
                                st["auto_train_result"] = _auto_train(ds, st, policy)
                                bd._put_json(s3, PRIVATE_BUCKET, o["Key"], st)
                        passes.append({"endpoint": st.get("endpoint"), "status": st.get("status"), "cursor": st.get("cursor"), "n_rows": st.get("n_rows"),
                                       "dim": st.get("dim"), "errors": st.get("errors"), "updated_at": st.get("updated_at"), "auto_train": st.get("auto_train"),
                                       "auto_train_result": st.get("auto_train_result")})
        except Exception as e:
            passes.append({"error": str(e)[:160]})
    cost = cg.mtd_sagemaker_cost(client("ce"))
    role = execution_role()
    out = {
        "engine": ENGINE, "version": VERSION, "generated_at": now_iso(), "elapsed_s": round(time.time() - t0, 1), "region": REGION,
        "account": "857687956942", "sagemaker_role_arn": role, "private_bucket": PRIVATE_BUCKET,
        "policy": policy,
        "inventory": {k: snap.get(k) for k in ("domains", "apps", "endpoints", "models", "endpoint_configs", "jobs", "notebooks", "feature_groups", "clusters")},
        "inventory_errors": snap.get("errors"),
        "cost": {"projected": projected, "mtd": cost, "ttl_ledger": ledger},
        "catalog": {"generated_at": catalog.get("generated_at"), "n": catalog.get("n"), "article_models_present": catalog.get("article_models_present"),
                    "article_models_missing": catalog.get("article_models_missing"), "errors": catalog.get("errors"), "refresh_error": catalog.get("refresh_error"),
                    "cards": (catalog.get("cards") or [])[:120]},
        "brain_dataset": _public_dataset_view(ds, passes),
        "learning": collect_learning(client("sagemaker")),
        "market_read": _safe(public_market_read),
        "pipeline_verdict": get_json(PUBLIC_BUCKET, VERDICT_KEY),
        "tiers": [
            {"tier": 1, "name": "Transfer learning (article recipe)", "how": "RoBERTa-SEC embedding endpoint -> Brain rows embedded -> XGBoost classifier (spot) -> serverless endpoint", "cost": "cents"},
            {"tier": 2, "name": "JumpStart fine-tune", "how": "any hub card with a training recipe, pretrained weights as the `model` channel", "cost": "instance-hours, capped by MaxRuntime"},
            {"tier": 3, "name": "Autopilot / AutoML V2", "how": "text classification on the Brain (transformer fine-tune) or tabular on any warehouse CSV; ensembling mode", "cost": "capped by MaxAutoMLJobRuntime"},
            {"tier": 4, "name": "HyperPod", "how": "resilient GPU cluster for from-scratch runs", "cost": "per node-hour, LOCKED by policy"},
        ],
        "definitions": {
            "endpoint": "a live model server; real-time endpoints bill per hour while InService, serverless ones bill per request",
            "TTL": "managed endpoints are deleted by the hourly guard once older than their TTL unless pinned",
            "spot": "managed spot training -- up to ~70-90% cheaper, may be interrupted and resumed",
            "hub card": "a JumpStart model card (image + artifact + recipe) in the SageMaker public hub",
        },
    }
    put_public(OUT_KEY, out)
    ctl = get_json(PRIVATE_BUCKET, "ai/control.json") or {}
    ctl.update({"sagemaker_role_arn": role, "private_bucket": PRIVATE_BUCKET, "updated_at": out["generated_at"], "version": VERSION})
    put_private("ai/control.json", ctl)
    pub = {"function_url": ctl.get("function_url"), "updated_at": out["generated_at"], "version": VERSION}
    put_public(CONTROL_KEY, pub)
    return out


def _safe(fn):
    try:
        return fn()
    except Exception as e:
        return {"error": str(e)[:140]}


def _public_dataset_view(ds: Optional[dict], passes: List[dict]) -> Optional[dict]:
    if not ds:
        return None
    keep = ("dataset_id", "kind", "built_at", "source_generated_at", "source_n_notes", "n_rows", "n_train", "n_validation", "n_excluded", "by_label", "labels",
            "excluded_labels", "min_class_rows_floor", "n_pinned", "dropped", "min_class_rows", "text_chars_median")
    v = {k: ds.get(k) for k in keep}
    v["embedding_passes"] = passes
    return v


def _auto_train(ds: dict, st: dict, policy: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return action_train_classifier({"dataset_id": ds["dataset_id"], "endpoint": st["endpoint"]}, policy)
    except Exception as e:
        return {"error": str(e)[:200]}


# ══════════════════════════════════════════════════════════════════ actions
class ActionError(Exception):
    pass


def _policy():
    return cg.load_policy(client("s3"), PRIVATE_BUCKET)


def _projected(policy):
    eps = inventory_endpoints()
    jobs = tr.list_jobs(client("sagemaker"), 20)
    price_fn = lambda it, fam: cg.hourly_price(client("pricing"), client("s3"), PRIVATE_BUCKET, it, fam)
    return cg.projected_daily_usd(eps, jobs, price_fn), eps


def _guard_instance(policy, instance_type: Optional[str], family: str, hours: float) -> Dict[str, Any]:
    if not instance_type:
        return {"usd_per_hour": 0.0, "note": "serverless"}
    why = cg.instance_allowed(policy, instance_type, family)
    if why:
        raise ActionError(why)
    price = cg.hourly_price(client("pricing"), client("s3"), PRIVATE_BUCKET, instance_type, family)
    if price.get("usd_per_hour") is None:
        raise ActionError("could not price %s from the AWS Price List (%s) -- refusing an unpriced create" % (instance_type, price.get("error")))
    projected, _ = _projected(policy)
    why = cg.check_budget(policy, projected, price["usd_per_hour"], hours)
    if why:
        raise ActionError(why)
    return price


def action_deploy(body: dict, policy: dict) -> Dict[str, Any]:
    model_id = str(body.get("model_id") or "").strip()
    if not model_id:
        raise ActionError("model_id required (a hub card id, e.g. mxnet-tcembedding-robertafin-base-uncased)")
    role = execution_role()
    if not role:
        raise ActionError("no SageMaker execution role bound yet (ops launch writes ai/control.json)")
    spec = sm_hub.describe_model(client("sagemaker"), model_id, body.get("version"))
    serverless = bool(body.get("serverless", policy.get("serverless_default", True)))
    instance_type = body.get("instance_type") or spec.get("default_inference_instance")
    ttl = float(body.get("ttl_hours") or policy.get("endpoint_ttl_hours") or 3)
    if not serverless:
        _guard_instance(policy, instance_type, "hosting", ttl)
    ep = body.get("endpoint_name") or ("jh-ai-" + re.sub(r"[^a-z0-9]+", "-", model_id.lower()))[:56].rstrip("-")
    res = sm_hub.deploy_model(client("sagemaker"), client("s3"), spec=spec, role_arn=role, endpoint_name=ep, instance_type=instance_type,
                              serverless=serverless, private_bucket=PRIVATE_BUCKET, tags=cg.tags("hub:" + model_id, None if serverless else ttl, bool(body.get("pinned"))),
                              serverless_memory_mb=int(body.get("serverless_memory_mb") or 4096), serverless_max_conc=int(body.get("serverless_max_conc") or 4))
    res.update({"model_id": model_id, "ttl_hours": None if serverless else ttl, "spec": {k: spec.get(k) for k in ("task", "framework", "hosting_image", "default_inference_instance", "training_supported")}})
    put_private("ai/models/deployments/%s.json" % ep, {**res, "at": now_iso()})
    return res


def action_dataset_build(body: dict, policy: dict) -> Dict[str, Any]:
    man = bd.build_brain_dataset(client("s3"), PUBLIC_BUCKET, PRIVATE_BUCKET, min_chars=int(body.get("min_chars") or 24), min_class_rows=int(body.get("min_class_rows") or 20))
    # AutoML text CSV (header text,label) alongside, still private
    rows = bd.load_rows(client("s3"), PRIVATE_BUCKET, man["dataset_id"])
    import csv
    import io
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["text", "label"])
    for r in rows:
        w.writerow([r["text"], r["label"]])
    client("s3").put_object(Bucket=PRIVATE_BUCKET, Key="ai/datasets/brain/%s/automl/brain.csv" % man["dataset_id"], Body=buf.getvalue().encode(),
                            ContentType="text/csv", ServerSideEncryption="AES256")
    return {k: v for k, v in man.items() if k != "rows_key"}


def action_embed(body: dict, policy: dict, context=None) -> Dict[str, Any]:
    ds_id = body.get("dataset_id") or (bd.latest_dataset(client("s3"), PRIVATE_BUCKET) or {}).get("dataset_id")
    ep = str(body.get("endpoint") or "").strip()
    if not ds_id or not ep:
        raise ActionError("dataset_id (or a built dataset) and endpoint required")
    d = client("sagemaker").describe_endpoint(EndpointName=ep)
    if d.get("EndpointStatus") != "InService":
        raise ActionError("endpoint %s is %s -- wait for InService" % (ep, d.get("EndpointStatus")))
    budget = float(body.get("max_seconds") or 600)
    if context is not None:
        budget = min(budget, max(60.0, context.get_remaining_time_in_millis() / 1000.0 - 90.0))
    st = bd.embedding_status(client("s3"), PRIVATE_BUCKET, ds_id, ep) or {}
    auto = bool(body.get("auto_train", False)) or bool(st.get("auto_train"))
    st = bd.run_embedding_pass(client("s3"), client("sagemaker-runtime"), PRIVATE_BUCKET, ds_id, ep, embed_fn=sm_hub.embed_texts, budget_s=budget)
    if auto:
        st["auto_train"] = True
        if st.get("status") == "complete" and not st.get("auto_train_result"):
            st["auto_train_result"] = _auto_train({"dataset_id": ds_id}, st, policy)
        bd._put_json(client("s3"), PRIVATE_BUCKET, bd._emb_state_key(ds_id, ep), st)
    return st


def action_train_classifier(body: dict, policy: dict) -> Dict[str, Any]:
    ds_id = body.get("dataset_id") or (bd.latest_dataset(client("s3"), PRIVATE_BUCKET) or {}).get("dataset_id")
    ep = str(body.get("endpoint") or "").strip()
    man = bd._get_json(client("s3"), PRIVATE_BUCKET, "ai/datasets/brain/%s/manifest.json" % ds_id) if ds_id else None
    if not man:
        raise ActionError("dataset %s not found -- build the Brain dataset first" % ds_id)
    emb = (man.get("embeddings") or {}).get(ep)
    if not emb:
        raise ActionError("dataset %s has no completed embedding pass through %s (available: %s)" % (ds_id, ep, list((man.get("embeddings") or {}).keys())))
    role = execution_role()
    it = body.get("instance_type") or "ml.m5.xlarge"
    spot = bool(body.get("spot", policy.get("training_spot", True)))
    max_rt = int(body.get("max_runtime_s") or policy.get("training_max_runtime_s") or 3600)
    _guard_instance(policy, it, "training", max_rt / 3600.0)
    out_uri = "s3://%s/ai/jobs/classifier/%s/%s/" % (PRIVATE_BUCKET, ds_id, re.sub(r"[^a-z0-9-]", "-", ep.lower()))
    res = tr.start_classifier_job(client("sagemaker"), role_arn=role, train_uri=emb["train_uri"], validation_uri=emb["validation_uri"], out_uri=out_uri,
                                  n_classes=len(man.get("labels") or bd.CATS), instance_type=it, max_runtime_s=max_rt, spot=spot,
                                  tags=cg.tags("brain-classifier:%s:%s" % (ds_id, ep), None))
    res.update({"dataset_id": ds_id, "embedding_endpoint": ep, "labels": man.get("labels") or bd.CATS, "n_train": emb.get("n_embedded")})
    put_private("ai/jobs/%s.json" % res["job_name"], res)
    return res


def action_train_curve(body: dict, policy: dict) -> Dict[str, Any]:
    """Learning curve: one classifier job per training-set fraction (nested subsets, same validation set)."""
    ds_id = body.get("dataset_id") or (bd.latest_dataset(client("s3"), PRIVATE_BUCKET) or {}).get("dataset_id")
    ep = str(body.get("endpoint") or "").strip()
    man = bd._get_json(client("s3"), PRIVATE_BUCKET, "ai/datasets/brain/%s/manifest.json" % ds_id) if ds_id else None
    if not man or not (man.get("embeddings") or {}).get(ep):
        raise ActionError("dataset %s has no completed embedding pass through %s" % (ds_id, ep))
    fractions = body.get("fractions") or [0.1, 0.25, 0.5, 1.0]
    fractions = sorted({min(1.0, max(0.02, float(f))) for f in fractions})
    it = body.get("instance_type") or "ml.m5.xlarge"
    spot = bool(body.get("spot", policy.get("training_spot", True)))
    max_rt = int(body.get("max_runtime_s") or policy.get("training_max_runtime_s") or 3600)
    price = _guard_instance(policy, it, "training", len(fractions) * max_rt / 3600.0)
    curve_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    runs = []
    for f in fractions:
        sub = bd.write_fraction_csv(client("s3"), PRIVATE_BUCKET, ds_id, ep, f)
        out_uri = "s3://%s/ai/jobs/curve/%s/f%03d/" % (PRIVATE_BUCKET, curve_id, int(round(f * 100)))
        res = tr.start_classifier_job(client("sagemaker"), role_arn=execution_role(), train_uri=sub["train_uri"], validation_uri=sub["validation_uri"], out_uri=out_uri,
                                      n_classes=len(man.get("labels") or bd.CATS), instance_type=it, max_runtime_s=max_rt, spot=spot,
                                      tags=cg.tags("brain-curve:%s:f%03d" % (curve_id, int(round(f * 100))), None), job_name=tr._name("jh-ai-curve-f%03d" % int(round(f * 100))))
        runs.append({"fraction": f, "n_train": sub["n_train"], "job_name": res["job_name"]})
        put_private("ai/jobs/%s.json" % res["job_name"], {**res, "dataset_id": ds_id, "embedding_endpoint": ep, "curve_id": curve_id, "fraction": f, "n_train": sub["n_train"], "kind": "curve"})
    doc = {"curve_id": curve_id, "dataset_id": ds_id, "endpoint": ep, "instance_type": it, "spot": spot, "labels": man.get("labels"), "n_validation": (man.get("embeddings") or {}).get(ep, {}).get("n_embedded"),
           "runs": runs, "started_at": now_iso(), "usd_per_hour_each": price.get("usd_per_hour")}
    put_private("ai/curves/%s.json" % curve_id, doc)
    return doc


def collect_learning(sm) -> Dict[str, Any]:
    """Every classifier job this engine started (tier-1 runs + curve runs) with its final metrics -> the two curves the
    page plots: validation loss vs training rows (latest curve) and validation loss over time (every full run)."""
    s3 = client("s3")
    curves = []
    try:
        keys = [o["Key"] for o in (s3.list_objects_v2(Bucket=PRIVATE_BUCKET, Prefix="ai/curves/").get("Contents") or []) if o["Key"].endswith(".json")]
        for k in sorted(keys)[-6:]:
            c = get_json(PRIVATE_BUCKET, k) or {}
            for r in c.get("runs") or []:
                r.update(_job_metrics(sm, r.get("job_name")))
            curves.append(c)
    except Exception as e:
        curves.append({"error": str(e)[:160]})
    runs = []
    try:
        keys = [o["Key"] for o in (s3.list_objects_v2(Bucket=PRIVATE_BUCKET, Prefix="ai/jobs/").get("Contents") or []) if o["Key"].endswith(".json")]
        for k in sorted(keys)[-60:]:
            j = get_json(PRIVATE_BUCKET, k) or {}
            if j.get("tier") != 1 or j.get("kind") == "curve":
                continue
            j.update(_job_metrics(sm, j.get("job_name")))
            runs.append({k2: j.get(k2) for k2 in ("job_name", "dataset_id", "embedding_endpoint", "n_train", "instance_type", "spot", "started_at", "status", "metrics", "billable_s", "labels")})
    except Exception as e:
        runs.append({"error": str(e)[:160]})
    return {"curves": curves, "classifier_runs": runs}


def _job_metrics(sm, job_name: Optional[str]) -> Dict[str, Any]:
    if not job_name:
        return {}
    try:
        d = sm.describe_training_job(TrainingJobName=job_name)
        return {"status": d.get("TrainingJobStatus"), "billable_s": d.get("BillableTimeInSeconds"),
                "metrics": {m.get("MetricName"): m.get("Value") for m in (d.get("FinalMetricDataList") or [])},
                "failure": (d.get("FailureReason") or "")[:160]}
    except Exception as e:
        return {"status": "unknown", "describe_error": str(e)[:100]}


def action_train_finetune(body: dict, policy: dict) -> Dict[str, Any]:
    model_id = str(body.get("model_id") or "").strip()
    uri = str(body.get("training_uri") or "").strip()
    if not model_id or not uri.startswith("s3://"):
        raise ActionError("model_id and an s3:// training_uri are required")
    spec = sm_hub.describe_model(client("sagemaker"), model_id, body.get("version"))
    it = body.get("instance_type") or spec.get("default_training_instance")
    max_rt = int(body.get("max_runtime_s") or policy.get("training_max_runtime_s") or 3600)
    spot = bool(body.get("spot", policy.get("training_spot", True)))
    _guard_instance(policy, it, "training", max_rt / 3600.0)
    out_uri = "s3://%s/ai/jobs/finetune/%s/" % (PRIVATE_BUCKET, re.sub(r"[^a-z0-9-]", "-", model_id.lower()))
    res = tr.start_jumpstart_finetune(client("sagemaker"), spec=spec, role_arn=execution_role(), training_uri=uri, out_uri=out_uri, instance_type=it,
                                      max_runtime_s=max_rt, spot=spot, tags=cg.tags("finetune:" + model_id, None), hyperparameters=body.get("hyperparameters") or {})
    put_private("ai/jobs/%s.json" % res["job_name"], res)
    return res


def action_train_automl(body: dict, policy: dict) -> Dict[str, Any]:
    kind = body.get("kind") or "text"
    max_rt = int(body.get("max_runtime_s") or policy.get("training_max_runtime_s") or 3600)
    projected, _ = _projected(policy)
    if float(projected.get("usd_per_day") or 0) >= float(policy.get("daily_budget_usd") or 0):
        raise ActionError("run-rate already at the daily budget; AutoML refused")
    if kind == "text":
        ds_id = body.get("dataset_id") or (bd.latest_dataset(client("s3"), PRIVATE_BUCKET) or {}).get("dataset_id")
        if not ds_id:
            raise ActionError("build the Brain dataset first")
        csv_uri = "s3://%s/ai/datasets/brain/%s/automl/" % (PRIVATE_BUCKET, ds_id)
        res = tr.start_autopilot_text(client("sagemaker"), role_arn=execution_role(), csv_uri=csv_uri, out_uri="s3://%s/ai/jobs/automl/%s/" % (PRIVATE_BUCKET, ds_id),
                                      max_runtime_s=max_rt, tags=cg.tags("automl-text:" + ds_id, None))
    else:
        csv_uri = str(body.get("csv_uri") or "")
        target = str(body.get("target") or "")
        if not csv_uri.startswith("s3://") or not target:
            raise ActionError("csv_uri (s3://, header row) and target column required")
        res = tr.start_autopilot_tabular(client("sagemaker"), role_arn=execution_role(), csv_uri=csv_uri, out_uri="s3://%s/ai/jobs/automl-tabular/" % PRIVATE_BUCKET,
                                         target=target, problem_type=body.get("problem_type"), max_runtime_s=max_rt, max_candidates=int(body.get("max_candidates") or 20),
                                         tags=cg.tags("automl-tabular:" + target, None))
    put_private("ai/jobs/%s.json" % res["job_name"], res)
    return res


def action_deploy_trained(body: dict, policy: dict) -> Dict[str, Any]:
    job = str(body.get("job_name") or "").strip()
    if not job:
        raise ActionError("job_name required")
    serverless = bool(body.get("serverless", True))
    it = body.get("instance_type")
    ttl = float(body.get("ttl_hours") or policy.get("endpoint_ttl_hours") or 3)
    if not serverless:
        _guard_instance(policy, it, "hosting", ttl)
    ep = body.get("endpoint_name") or ("jh-ai-clf-" + re.sub(r"[^a-z0-9-]", "-", job.lower()))[:56].rstrip("-")
    res = tr.deploy_training_output(client("sagemaker"), job_name=job, role_arn=execution_role(), endpoint_name=ep, serverless=serverless, instance_type=it,
                                    tags=cg.tags("trained:" + job, None if serverless else ttl, bool(body.get("pinned"))))
    put_private("ai/models/deployments/%s.json" % ep, {**res, "at": now_iso()})
    return res


def action_infer(body: dict, policy: dict) -> Dict[str, Any]:
    text = str(body.get("text") or "").strip()
    emb_ep = str(body.get("embedding_endpoint") or "").strip()
    if not text or not emb_ep:
        raise ActionError("text and embedding_endpoint required")
    rt = client("sagemaker-runtime")
    vec = sm_hub.embed_texts(rt, emb_ep, [text])[0]
    if not vec:
        raise ActionError("embedding endpoint returned nothing")
    out: Dict[str, Any] = {"dim": len(vec), "embedding_endpoint": emb_ep}
    clf = body.get("classifier_endpoint")
    ds_id = body.get("dataset_id") or (bd.latest_dataset(client("s3"), PRIVATE_BUCKET) or {}).get("dataset_id")
    if clf:
        pred = sm_hub.predict_csv(rt, clf, [vec])
        man = bd._get_json(client("s3"), PRIVATE_BUCKET, "ai/datasets/brain/%s/manifest.json" % ds_id) if ds_id else None
        labels = body.get("labels") or (man or {}).get("labels") or bd.CATS
        p0 = pred[0] if pred else None
        if isinstance(p0, list) and len(p0) == len(labels):
            ranked = sorted(zip(labels, [float(x) for x in p0]), key=lambda kv: -kv[1])
            out["classification"] = [{"label": l, "p": round(p, 4)} for l, p in ranked]
        else:
            out["classification_raw"] = pred[:3]
    if ds_id and body.get("retrieve", True):
        out["nearest_notes"] = bd.nearest_notes(client("s3"), PRIVATE_BUCKET, ds_id, emb_ep, vec, k=int(body.get("k") or 8))
        out["dataset_id"] = ds_id
    return out


def action_endpoint_delete(body: dict, policy: dict) -> Dict[str, Any]:
    ep = str(body.get("endpoint") or "").strip()
    if not ep:
        raise ActionError("endpoint required")
    client("sagemaker").delete_endpoint(EndpointName=ep)
    return {"endpoint": ep, "action": "deleting"}


def action_job_stop(body: dict, policy: dict) -> Dict[str, Any]:
    name = str(body.get("job_name") or "").strip()
    kind = body.get("kind") or "training"
    if not name:
        raise ActionError("job_name required")
    if kind == "automl":
        client("sagemaker").stop_auto_ml_job(AutoMLJobName=name)
    else:
        client("sagemaker").stop_training_job(TrainingJobName=name)
    return {"job_name": name, "kind": kind, "action": "stopping"}


def action_policy(body: dict, policy: dict) -> Dict[str, Any]:
    return cg.save_policy(client("s3"), PRIVATE_BUCKET, body.get("patch") or {})


def action_hyperpod(body: dict, policy: dict) -> Dict[str, Any]:
    why = tr.hyperpod_gate(policy, body.get("confirm") or "")
    if why:
        raise ActionError(why)
    it = str(body.get("instance_type") or "")
    if not it or not body.get("lifecycle_s3"):
        raise ActionError("instance_type and lifecycle_s3 (s3:// prefix with on_create.sh) required")
    _guard_instance(policy, it, "training", 24.0)
    return tr.create_hyperpod_cluster(client("sagemaker"), name=body.get("name") or "jh-ai-hyperpod", role_arn=execution_role(), instance_type=it,
                                      count=int(body.get("count") or 1), lifecycle_s3=body["lifecycle_s3"], tags=cg.tags("hyperpod", None, True))


# ══════════════════════════════════════════════════════════════ market read
def _signals_table():
    return boto3.resource("dynamodb", region_name=REGION).Table(SIGNALS_TABLE)


def _live_embedding_endpoint(ds: Optional[dict]) -> Optional[str]:
    """An InService endpoint that already has a completed embedding pass on the latest dataset (so retrieval works)."""
    if not ds:
        return None
    done = set((ds.get("embeddings") or {}).keys())
    if not done:
        return None
    try:
        for e in (client("sagemaker").list_endpoints(MaxResults=100).get("Endpoints") or []):
            if e.get("EndpointName") in done and e.get("EndpointStatus") == "InService":
                return e["EndpointName"]
    except Exception:
        pass
    return None


def action_market_read(body: dict, policy: dict, context=None) -> Dict[str, Any]:
    s3 = client("s3")
    prev = get_json(PRIVATE_BUCKET, READ_KEY) or {}
    if not body.get("force") and prev.get("generated_at"):
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(prev["generated_at"])).total_seconds()
            if age < mr.MIN_READ_GAP_S:
                raise ActionError("last read is %d min old; a new LLM read is allowed every %d min (pass force=true to override)" % (age // 60, mr.MIN_READ_GAP_S // 60))
        except ActionError:
            raise
        except Exception:
            pass
    t0 = time.time()
    board = mr.build_board(s3, PUBLIC_BUCKET)
    sentences = mr.setup_sentences(board)
    ds = bd.latest_dataset(s3, PRIVATE_BUCKET)
    ep = body.get("embedding_endpoint") or _live_embedding_endpoint(ds)
    play = mr.playbook(s3, client("sagemaker-runtime"), PRIVATE_BUCKET, (ds or {}).get("dataset_id"), ep, sentences, sm_hub.embed_texts, bd.nearest_notes)
    try:
        import llm_router
    except Exception as e:
        raise ActionError("LLM router unavailable in this bundle: %s" % str(e)[:100])

    def complete(prompt, **kw):
        txt = ""
        try:
            txt = llm_router.complete(prompt, **kw) or ""
        except Exception as e:
            print("[ai] router: %s" % str(e)[:120])
        if txt.strip():
            return txt
        # the router answers "" on budget/mode gates or its fixed 35s timeout; a user-requested read is worth one direct,
        # bounded Sonnet call (same key, 120s) -- metered through llm_cost when it is present
        return _direct_claude(prompt, kw.get("system"), int(kw.get("max_tokens") or 2400))

    read = mr.compose_read(board, play, complete)
    read["llm_path"] = getattr(_direct_claude, "last_path", "router")
    read_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    logged = []
    if read.get("calls") and not read.get("parse_error"):
        try:
            from signals_emit import log_signal, yprice
            logged = mr.log_calls(_signals_table(), read_id, read["calls"], log_signal, yprice)
        except Exception as e:
            logged = [{"error": "ledger unavailable: %s" % str(e)[:120]}]
    calls_doc = get_json(PRIVATE_BUCKET, CALLS_KEY) or {"calls": []}
    calls_doc["calls"] = (calls_doc.get("calls") or []) + [r for r in logged if r.get("signal_id")]
    calls_doc["calls"] = calls_doc["calls"][-400:]
    put_private(CALLS_KEY, calls_doc)
    doc = {"read_id": read_id, "generated_at": now_iso(), "elapsed_s": round(time.time() - t0, 1), "engine": ENGINE, "version": VERSION,
           "board": board, "setup_sentences": sentences, "playbook": play, "read": read, "calls_logged": logged, "signal_type": mr.SIGNAL_TYPE, "windows": mr.WINDOWS}
    put_private(READ_KEY, doc)
    put_private("ai/market-read/history/%s.json" % read_id, doc)
    try:
        run_inventory(context, continue_embeddings=False)
    except Exception:
        pass
    return {k: doc[k] for k in ("read_id", "generated_at", "elapsed_s", "read", "calls_logged")} | {"playbook_available": play.get("available"), "sources": board["sources"]}


def _direct_claude(prompt: str, system: Optional[str], max_tokens: int) -> str:
    import urllib.request
    try:
        import llm_router
        key = llm_router._anthropic_key()
        model = getattr(llm_router, "SONNET", "claude-sonnet-4-6")
    except Exception as e:
        _direct_claude.last_path = "no-key:%s" % str(e)[:60]
        return ""
    payload = {"model": model, "max_tokens": int(max_tokens), "messages": [{"role": "user", "content": prompt}]}
    if system:
        payload["system"] = system
    req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json", "x-api-key": key, "anthropic-version": "2023-06-01", "x-jh-internal": "justhodl-ai"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            d = json.loads(r.read().decode())
        txt = "".join(b.get("text", "") for b in d.get("content", []))
        _direct_claude.last_path = "direct:%s" % model
        try:
            import llm_cost
            u = d.get("usage") or {}
            if hasattr(llm_cost, "record"):
                llm_cost.record(ENGINE, model, int(u.get("input_tokens") or 0), int(u.get("output_tokens") or 0))
        except Exception:
            pass
        return txt
    except Exception as e:
        _direct_claude.last_path = "direct-failed:%s" % str(e)[:80]
        print("[ai] direct claude failed: %s" % str(e)[:160])
        return ""


def action_get_read(body: dict, policy: dict) -> Dict[str, Any]:
    doc = get_json(PRIVATE_BUCKET, READ_KEY)
    if not doc:
        raise ActionError("no market read yet -- press 'Read the market now'")
    calls = (get_json(PRIVATE_BUCKET, CALLS_KEY) or {}).get("calls") or []
    try:
        perf = mr.grade_calls(_signals_table(), calls)
    except Exception as e:
        perf = {"error": str(e)[:140], "n_calls": len(calls)}
    doc["performance"] = perf
    return doc


def public_market_read() -> Optional[dict]:
    doc = get_json(PRIVATE_BUCKET, READ_KEY)
    if not doc:
        return None
    rd = doc.get("read") or {}
    st = {k: (rd.get(k) or {}).get("stance") if isinstance(rd.get(k), dict) else None for k in ("stocks", "bonds", "metals", "crypto")}
    calls = (get_json(PRIVATE_BUCKET, CALLS_KEY) or {}).get("calls") or []
    perf = None
    try:
        g = mr.grade_calls(_signals_table(), calls)
        perf = {"n_calls": g.get("n_calls"), "by_window": g.get("by_window")}
    except Exception as e:
        perf = {"error": str(e)[:100], "n_calls": len(calls)}
    return {"read_id": doc.get("read_id"), "generated_at": doc.get("generated_at"), "stances": st, "n_opportunities": len(rd.get("best_opportunities") or []),
            "n_calls_this_read": len(rd.get("calls") or []), "playbook_available": (doc.get("playbook") or {}).get("available"),
            "sources": {k: v.get("status") for k, v in ((doc.get("board") or {}).get("sources") or {}).items()}, "performance": perf,
            "parse_error": bool(rd.get("parse_error"))}


ACTIONS = {
    ("POST", "/inventory"): lambda b, p, c: run_inventory(c, refresh_catalog=bool(b.get("refresh_catalog"))),
    ("POST", "/catalog"): lambda b, p, c: run_inventory(c, refresh_catalog=True)["catalog"],
    ("GET", "/model"): lambda b, p, c: sm_hub.describe_model(client("sagemaker"), b.get("model_id", ""), b.get("version")),
    ("POST", "/deploy"): lambda b, p, c: action_deploy(b, p),
    ("POST", "/dataset/build"): lambda b, p, c: action_dataset_build(b, p),
    ("POST", "/embed"): lambda b, p, c: action_embed(b, p, c),
    ("POST", "/train/classifier"): lambda b, p, c: action_train_classifier(b, p),
    ("POST", "/train/finetune"): lambda b, p, c: action_train_finetune(b, p),
    ("POST", "/train/curve"): lambda b, p, c: action_train_curve(b, p),
    ("POST", "/train/automl"): lambda b, p, c: action_train_automl(b, p),
    ("POST", "/deploy-trained"): lambda b, p, c: action_deploy_trained(b, p),
    ("POST", "/infer"): lambda b, p, c: action_infer(b, p),
    ("POST", "/endpoint/delete"): lambda b, p, c: action_endpoint_delete(b, p),
    ("POST", "/job/stop"): lambda b, p, c: action_job_stop(b, p),
    ("POST", "/policy"): lambda b, p, c: action_policy(b, p),
    ("POST", "/hyperpod/create"): lambda b, p, c: action_hyperpod(b, p),
    ("POST", "/market-read"): lambda b, p, c: action_market_read(b, p, c),
    ("GET", "/read"): lambda b, p, c: action_get_read(b, p),
}


# ══════════════════════════════════════════════════════════════════ handler
def _resp(status: int, obj: Any) -> Dict[str, Any]:
    return {"statusCode": status, "headers": {"Content-Type": "application/json", "Cache-Control": "private, no-store"},
            "body": json.dumps(obj, default=str)}


def _http_event(event: dict) -> bool:
    return isinstance(event, dict) and ("requestContext" in event or "rawPath" in event or "httpMethod" in event)


def _parse_http(event: dict):
    rc = event.get("requestContext") or {}
    method = ((rc.get("http") or {}).get("method") or event.get("httpMethod") or "GET").upper()
    path = (rc.get("http") or {}).get("path") or event.get("rawPath") or event.get("path") or "/"
    path = re.sub(r"^/+", "/", path)
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    body = event.get("body") or ""
    if event.get("isBase64Encoded") and body:
        body = base64.b64decode(body).decode("utf-8", "ignore")
    try:
        data = json.loads(body) if body else {}
    except Exception:
        data = {}
    q = event.get("queryStringParameters") or {}
    if isinstance(q, dict):
        for k, v in q.items():
            data.setdefault(k, v)
    return method, path, data


def lambda_handler(event=None, context=None):
    event = event or {}
    if _http_event(event):
        if private_http_denied is None:
            return _resp(503, {"error": "service identity module unavailable"})
        denied = private_http_denied(event)
        if denied:
            return denied
        method, path, body = _parse_http(event)
        if method == "GET" and path in ("/", "/status", "/health"):
            snap = get_json(PUBLIC_BUCKET, OUT_KEY) or {}
            return _resp(200, {"engine": ENGINE, "version": VERSION, "read_model_generated_at": snap.get("generated_at"), "ok": True})
        fn = ACTIONS.get((method, path))
        if not fn:
            return _resp(404, {"error": "unknown action %s %s" % (method, path), "actions": sorted("%s %s" % k for k in ACTIONS)})
        try:
            policy = _policy()
            res = fn(body, policy, context)
            return _resp(200, {"ok": True, "action": path, "result": res, "at": now_iso()})
        except ActionError as e:
            return _resp(400, {"ok": False, "action": path, "error": str(e)})
        except Exception as e:
            print("[ai] action %s failed: %s\n%s" % (path, e, traceback.format_exc()[-1200:]))
            return _resp(500, {"ok": False, "action": path, "error": str(e)[:400], "type": type(e).__name__})
    # EventBridge Scheduler / direct invoke: inventory (+ continuation of embedding passes)
    mode = (event.get("mode") if isinstance(event, dict) else None) or "inventory"
    if mode == "inventory":
        out = run_inventory(context, refresh_catalog=bool(event.get("refresh_catalog") or (event.get("body") or {}).get("refresh_catalog")))
        return {"ok": True, "generated_at": out["generated_at"], "endpoints": len(out["inventory"].get("endpoints") or []), "elapsed_s": out["elapsed_s"]}
    fn = ACTIONS.get(("POST", "/" + mode.strip("/"))) or ACTIONS.get(("GET", "/" + mode.strip("/")))
    if not fn:
        return {"ok": False, "error": "unknown mode %s" % mode}
    try:
        return {"ok": True, "result": fn(event.get("body") or {}, _policy(), context)}
    except ActionError as e:
        return {"ok": False, "error": str(e)}
