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
import hashlib
import json
import os
import re
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence

import boto3
from botocore.config import Config

import brain_dataset as bd
import cost_guard as cg
import deployment_gates as dg
import fleet_inputs as fi
import governance_control as governance
import market_read as mr
import pipeline as pl
import model_registry as registry
import outcome_labels as labels
import point_in_time as pit
import prediction_ledger as ledger
import s3_event_outbox as event_outbox
import sagemaker_feature_store as feature_store
import signal_envelope as signals
import sm_hub
import training as tr
import training_eligibility as training_gate
import validation_splits as splits

try:
    from private_artifact import private_http_denied  # shared: service-token gate for Function URL calls
except Exception:  # pragma: no cover - tests import without the shared bundle
    private_http_denied = None

VERSION = "2.0.0-review"
ENGINE = "justhodl-ai"
REGION = "us-east-1"
PUBLIC_BUCKET = os.environ.get("AI_PUBLIC_BUCKET", "justhodl-dashboard-live")
PRIVATE_BUCKET = os.environ.get("AI_PRIVATE_BUCKET", "justhodl-ai-857687956942")
BRAIN_SOURCE_BUCKET = os.environ.get("AI_BRAIN_SOURCE_BUCKET", PRIVATE_BUCKET)
OUT_KEY = "data/ai.json"
CONTROL_KEY = "data/ai/control.json"
CATALOG_KEY = "ai/catalog.json"
READ_KEY = "ai/market-read/latest.json"
CALLS_KEY = "ai/market-read/calls.json"
OWNER_READ_MODEL_KEY = "ai/read-model/latest.json"
VERDICT_KEY = "data/ai/verdict.json"
SIGNALS_TABLE = os.environ.get("SIGNALS_TABLE", "justhodl-signals")
AI_EVENT_BUS = os.environ.get("AI_EVENT_BUS", "")
SIGNAL_FEATURE_GROUP = os.environ.get("SIGNAL_FEATURE_GROUP", "")
PREDICTION_FEATURE_GROUP = os.environ.get("PREDICTION_FEATURE_GROUP", "")
PREDICTION_LEDGER_TABLE = os.environ.get("PREDICTION_LEDGER_TABLE", "")
PREDICTION_LEDGER_ARCHIVE_BUCKET = os.environ.get("PREDICTION_LEDGER_ARCHIVE_BUCKET", "")
MODEL_PACKAGE_GROUP = os.environ.get("MODEL_PACKAGE_GROUP", "")
MLFLOW_TRACKING_SERVER_NAME = os.environ.get("MLFLOW_TRACKING_SERVER_NAME", "")
MLFLOW_TRACKING_SERVER_ARN = os.environ.get("MLFLOW_TRACKING_SERVER_ARN", "")
MLFLOW_EXPERIMENT_NAME = os.environ.get("MLFLOW_EXPERIMENT_NAME", "")
TRAINING_INPUT_BUCKET = os.environ.get("TRAINING_INPUT_BUCKET", "")
FEATURE_STORE_ATHENA_WORKGROUP = os.environ.get("FEATURE_STORE_ATHENA_WORKGROUP", "primary")
FEATURE_STORE_QUERY_OUTPUT = os.environ.get(
    "FEATURE_STORE_QUERY_OUTPUT",
    "s3://%s/ai/feature-store-query-results/" % PRIVATE_BUCKET,
)
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
    if continue_embeddings and (get_json(PRIVATE_BUCKET, pl.STATE_KEY) or {}).get("status") == "running":
        continue_embeddings = False          # the pipeline owns the passes while it runs (no double-writing of parts)
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
    fleet = fi.build_fleet_snapshot(s3, PUBLIC_BUCKET)
    public_fleet = {k: fleet.get(k) for k in ("registry_key", "registry_version", "status", "summary")}
    public_fleet["feeds"] = [{
        "feed": row.get("feed"), "engines": row.get("engines"), "pages": row.get("pages"),
        "schema_versions": row.get("schema_versions"), "status": row.get("status"),
        "eligible": row.get("eligible"), "generated_at": row.get("generated_at"),
        "age_h": row.get("age_h"), "private": row.get("private"),
    } for row in fleet.get("feeds") or []]
    out = {
        "engine": ENGINE, "version": VERSION, "generated_at": now_iso(), "elapsed_s": round(time.time() - t0, 1), "region": REGION,
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
        "pipeline": _safe(lambda: pl.public_view(get_json(PRIVATE_BUCKET, pl.STATE_KEY) or {})),
        "fleet_inputs": public_fleet,
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
    put_private(OWNER_READ_MODEL_KEY, out)
    put_public(OUT_KEY, _public_read_model(out))
    ctl = get_json(PRIVATE_BUCKET, "ai/control.json") or {}
    ctl.update({"sagemaker_role_arn": role, "private_bucket": PRIVATE_BUCKET, "updated_at": out["generated_at"], "version": VERSION})
    put_private("ai/control.json", ctl)
    pub = {"function_url": ctl.get("function_url"), "updated_at": out["generated_at"], "version": VERSION}
    put_public(CONTROL_KEY, pub)
    return out


def _public_read_model(out: Dict[str, Any]) -> Dict[str, Any]:
    """Publish health and aggregate counts, never account topology or resource identifiers."""
    inv = out.get("inventory") or {}
    summary = {}
    for key in ("domains", "apps", "endpoints", "models", "jobs", "notebooks", "feature_groups", "clusters"):
        rows = inv.get(key) or []
        summary[key] = {
            "total": len(rows),
            "by_status": {
                status: sum(1 for row in rows if (row.get("status") or row.get("kind") or "UNKNOWN") == status)
                for status in sorted({row.get("status") or row.get("kind") or "UNKNOWN" for row in rows})
            },
        }
    policy = out.get("policy") or {}
    projected = ((out.get("cost") or {}).get("projected") or {})
    mtd = ((out.get("cost") or {}).get("mtd") or {})
    catalog = out.get("catalog") or {}
    dataset = out.get("brain_dataset") or {}
    verdict = out.get("pipeline_verdict") or {}
    fleet = out.get("fleet_inputs") or {}
    return {
        "engine": out.get("engine"),
        "version": out.get("version"),
        "generated_at": out.get("generated_at"),
        "elapsed_s": out.get("elapsed_s"),
        "region": out.get("region"),
        "access": "PUBLIC_AGGREGATE",
        "inventory_summary": summary,
        "policy": {k: policy.get(k) for k in ("daily_budget_usd", "endpoint_ttl_hours", "idle_hours", "serverless_default")},
        "cost": {"projected": {k: projected.get(k) for k in ("usd_per_day", "unknown")},
                 "mtd": {"usd_mtd": mtd.get("usd_mtd")}},
        "catalog": {k: catalog.get(k) for k in ("generated_at", "n", "article_models_present", "article_models_missing")},
        "brain_dataset": ({k: dataset.get(k) for k in ("n_rows", "source_n_notes", "n_labels", "outcome_labels")} if dataset else None),
        "market_read": out.get("market_read"),
        "fleet_inputs": {k: fleet.get(k) for k in ("registry_version", "status", "summary")},
        "pipeline_verdict": ({k: verdict.get(k) for k in ("status", "finished_at")} if verdict else None),
        "tiers": out.get("tiers"),
        "definitions": out.get("definitions"),
    }


def owner_read_model() -> Dict[str, Any]:
    doc = get_json(PRIVATE_BUCKET, OWNER_READ_MODEL_KEY)
    if not doc:
        raise ActionError("owner inventory has not been generated yet")
    return doc


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


def _assert_endpoint_write_allowed(endpoint_name: str) -> None:
    """A caller-selected name may update only an endpoint already owned by this engine."""
    sm = client("sagemaker")
    try:
        desc = sm.describe_endpoint(EndpointName=endpoint_name)
    except Exception as exc:
        if any(token in str(exc).lower() for token in ("not find", "not found", "validationexception", "404")):
            return
        raise ActionError("could not verify endpoint ownership for %s" % endpoint_name)
    if _tags(desc.get("EndpointArn") or "").get(cg.TAG_MANAGED) != "true":
        raise ActionError("refusing to replace endpoint %s because it is not tagged %s=true" % (endpoint_name, cg.TAG_MANAGED))


def _serverless_config(body: dict, policy: dict) -> tuple:
    memory = int(body.get("serverless_memory_mb") or min(4096, int(policy["serverless_max_memory_mb"])))
    concurrency = int(body.get("serverless_max_conc") or min(4, int(policy["serverless_max_concurrency"])))
    if memory not in (1024, 2048, 3072, 4096, 5120, 6144):
        raise ActionError("serverless_memory_mb must be one of 1024, 2048, 3072, 4096, 5120 or 6144")
    if memory > int(policy["serverless_max_memory_mb"]) or not 1 <= concurrency <= int(policy["serverless_max_concurrency"]):
        raise ActionError("serverless configuration exceeds policy limits")
    return memory, concurrency


def _capped_runtime(body: dict, policy: dict) -> int:
    ceiling = int(policy.get("training_max_runtime_s") or 3600)
    requested = int(body.get("max_runtime_s") or ceiling)
    if requested < 60 or requested > ceiling:
        raise ActionError("max_runtime_s must be between 60 and the policy ceiling %d" % ceiling)
    return requested


def _reject_pinned(body: dict) -> None:
    if body.get("pinned") is True:
        raise ActionError("pinned endpoints are disabled; all billable endpoints require an enforced TTL")


def _reject_direct_production_deploy() -> None:
    if os.environ.get("AI_ENVIRONMENT", "production").strip().lower() == "production":
        raise ActionError(
            "direct deployment is disabled in production; use the approval-gated "
            "blue-green canary workflow"
        )


def action_deploy(body: dict, policy: dict) -> Dict[str, Any]:
    _reject_direct_production_deploy()
    _reject_pinned(body)
    model_id = str(body.get("model_id") or "").strip()
    if not model_id:
        raise ActionError("model_id required (a hub card id, e.g. mxnet-tcembedding-robertafin-base-uncased)")
    role = execution_role()
    if not role:
        raise ActionError("no SageMaker execution role bound yet (ops launch writes ai/control.json)")
    spec = sm_hub.describe_model(client("sagemaker"), model_id, body.get("version"))
    serverless = bool(body.get("serverless", policy.get("serverless_default", True)))
    instance_type = body.get("instance_type")
    if not instance_type and not serverless:
        allowed = policy.get("allowed_inference_instances") or []
        sup = [i for i in (spec.get("supported_inference_instances") or []) if i in allowed]
        cpu = [i for i in sup if re.match(r"^ml\.(m|c|t|r)\d", i)]
        gpu = [i for i in sup if re.match(r"^ml\.(g|p)\d", i)] or [i for i in allowed if re.match(r"^ml\.(g|p)\d", i)]
        # a card whose hub document offers no CPU image needs a GPU instance (ops 5306: libcuda.so.1 missing on ml.m5)
        cpu_image = sm_hub.variant_for(spec, cpu[0] if cpu else "ml.m5.xlarge").get("image") or ""
        gpu_only = bool(re.search(r"-gpu-|cu1\d\d", spec.get("hosting_image") or "")) and not re.search(r"-cpu-", cpu_image)
        if gpu_only:
            if not gpu:
                raise ActionError("%s ships a GPU-only image; no GPU instance is in policy.allowed_inference_instances (add ml.g4dn.xlarge)" % model_id)
            instance_type = spec.get("default_inference_instance") if spec.get("default_inference_instance") in gpu else gpu[0]
        else:
            instance_type = spec.get("default_inference_instance") if spec.get("default_inference_instance") in allowed else (cpu[0] if cpu else (sup[0] if sup else "ml.m5.xlarge"))
    ttl = float(body.get("ttl_hours") or policy.get("endpoint_ttl_hours") or 3)
    if not serverless:
        _guard_instance(policy, instance_type, "hosting", ttl)
    ep = body.get("endpoint_name") or ("jh-ai-" + re.sub(r"[^a-z0-9]+", "-", model_id.lower()))[:56].rstrip("-")
    _assert_endpoint_write_allowed(ep)
    memory, concurrency = _serverless_config(body, policy) if serverless else (4096, 4)
    res = sm_hub.deploy_model(client("sagemaker"), client("s3"), spec=spec, role_arn=role, endpoint_name=ep, instance_type=instance_type,
                              serverless=serverless, private_bucket=PRIVATE_BUCKET, tags=cg.tags("hub:" + model_id, ttl, False),
                              serverless_memory_mb=memory, serverless_max_conc=concurrency)
    res.update({"model_id": model_id, "ttl_hours": None if serverless else ttl, "spec": {k: spec.get(k) for k in ("task", "framework", "hosting_image", "default_inference_instance", "supported_inference_instances", "training_supported")}})
    put_private("ai/models/deployments/%s.json" % ep, {**res, "at": now_iso()})
    return res


def action_dataset_build(body: dict, policy: dict) -> Dict[str, Any]:
    man = bd.build_brain_dataset(client("s3"), BRAIN_SOURCE_BUCKET, PRIVATE_BUCKET, min_chars=int(body.get("min_chars") or 24), min_class_rows=int(body.get("min_class_rows") or 20))
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
    try:
        eligibility = training_gate.evaluate_training_eligibility(
            training_gate.S3TrainingEvidenceResolver(
                client("s3"),
                PRIVATE_BUCKET,
                materialization_bucket=_required_setting(
                    "TRAINING_INPUT_BUCKET", TRAINING_INPUT_BUCKET
                ),
            ).resolve(body.get("governance_evidence")),
            expected_dataset_id=ds_id,
            expected_training_uri=emb["train_uri"],
            expected_validation_uri=emb["validation_uri"],
        )
    except training_gate.TrainingEligibilityError as exc:
        raise ActionError("classifier training is governance-ineligible: %s" % exc) from exc
    manifest_labels = tuple(sorted(man.get("labels") or ()))
    if manifest_labels != eligibility.labels:
        raise ActionError(
            "classifier training is governance-ineligible: dataset labels are not "
            "the verified outcome-label classes"
        )
    role = execution_role()
    it = body.get("instance_type") or "ml.m5.xlarge"
    spot = bool(body.get("spot", policy.get("training_spot", True)))
    max_rt = _capped_runtime(body, policy)
    _guard_instance(policy, it, "training", max_rt / 3600.0)
    out_uri = "s3://%s/ai/jobs/classifier/%s/%s/" % (PRIVATE_BUCKET, ds_id, re.sub(r"[^a-z0-9-]", "-", ep.lower()))
    res = tr.start_classifier_job(client("sagemaker"), role_arn=role, train_uri=eligibility.training_uri, validation_uri=eligibility.validation_uri, out_uri=out_uri,
                                  n_classes=len(manifest_labels), instance_type=it, max_runtime_s=max_rt, spot=spot,
                                  tags=cg.tags("brain-classifier:%s:%s" % (ds_id, ep), None),
                                  eligibility=eligibility)
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
    emb = (man.get("embeddings") or {})[ep]
    fractions = body.get("fractions") or [0.1, 0.25, 0.5, 1.0]
    fractions = sorted({min(1.0, max(0.02, float(f))) for f in fractions})
    evidence_by_fraction = body.get("fraction_governance_evidence")
    if not isinstance(evidence_by_fraction, Mapping):
        raise ActionError(
            "classifier curve requires fraction_governance_evidence for every "
            "materialized subset"
        )
    # Materialize all subsets and verify every exact subset URI before any
    # training call. A full-dataset receipt can never authorize a fraction URI.
    verified_subsets = []
    resolver = training_gate.S3TrainingEvidenceResolver(
        client("s3"),
        PRIVATE_BUCKET,
        materialization_bucket=_required_setting(
            "TRAINING_INPUT_BUCKET", TRAINING_INPUT_BUCKET
        ),
    )
    manifest_labels = tuple(sorted(man.get("labels") or ()))
    try:
        for fraction in fractions:
            fraction_key = "f%03d" % int(round(fraction * 100))
            sub = bd.write_fraction_csv(
                client("s3"), PRIVATE_BUCKET, ds_id, ep, fraction
            )
            eligibility = training_gate.evaluate_training_eligibility(
                resolver.resolve(evidence_by_fraction.get(fraction_key)),
                expected_dataset_id=ds_id,
                expected_training_uri=sub["train_uri"],
                expected_validation_uri=sub["validation_uri"],
            )
            if manifest_labels != eligibility.labels:
                raise training_gate.TrainingEligibilityError(
                    "dataset labels are not the verified outcome-label classes"
                )
            verified_subsets.append((fraction, sub, eligibility))
    except training_gate.TrainingEligibilityError as exc:
        raise ActionError("classifier curve is governance-ineligible: %s" % exc) from exc
    it = body.get("instance_type") or "ml.m5.xlarge"
    spot = bool(body.get("spot", policy.get("training_spot", True)))
    max_rt = _capped_runtime(body, policy)
    price = _guard_instance(policy, it, "training", len(fractions) * max_rt / 3600.0)
    curve_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    runs = []
    for f, sub, eligibility in verified_subsets:
        out_uri = "s3://%s/ai/jobs/curve/%s/f%03d/" % (PRIVATE_BUCKET, curve_id, int(round(f * 100)))
        res = tr.start_classifier_job(client("sagemaker"), role_arn=execution_role(), train_uri=eligibility.training_uri, validation_uri=eligibility.validation_uri, out_uri=out_uri,
                                      n_classes=len(manifest_labels), instance_type=it, max_runtime_s=max_rt, spot=spot,
                                      tags=cg.tags("brain-curve:%s:f%03d" % (curve_id, int(round(f * 100))), None), job_name=tr._name("jh-ai-curve-f%03d" % int(round(f * 100))),
                                      eligibility=eligibility)
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
    try:
        eligibility = training_gate.evaluate_training_eligibility(
            training_gate.S3TrainingEvidenceResolver(
                client("s3"),
                PRIVATE_BUCKET,
                materialization_bucket=_required_setting(
                    "TRAINING_INPUT_BUCKET", TRAINING_INPUT_BUCKET
                ),
            ).resolve(body.get("governance_evidence")),
            expected_dataset_id=str(body.get("dataset_id") or "").strip() or None,
            expected_training_uri=uri,
        )
    except training_gate.TrainingEligibilityError as exc:
        raise ActionError("fine-tune training is governance-ineligible: %s" % exc) from exc
    spec = sm_hub.describe_model(client("sagemaker"), model_id, body.get("version"))
    it = body.get("instance_type") or spec.get("default_training_instance")
    max_rt = _capped_runtime(body, policy)
    spot = bool(body.get("spot", policy.get("training_spot", True)))
    _guard_instance(policy, it, "training", max_rt / 3600.0)
    out_uri = "s3://%s/ai/jobs/finetune/%s/" % (PRIVATE_BUCKET, re.sub(r"[^a-z0-9-]", "-", model_id.lower()))
    res = tr.start_jumpstart_finetune(client("sagemaker"), spec=spec, role_arn=execution_role(), training_uri=eligibility.training_uri, out_uri=out_uri, instance_type=it,
                                      max_runtime_s=max_rt, spot=spot, tags=cg.tags("finetune:" + model_id, None), hyperparameters=body.get("hyperparameters") or {},
                                      eligibility=eligibility)
    put_private("ai/jobs/%s.json" % res["job_name"], res)
    return res


def action_train_automl(body: dict, policy: dict) -> Dict[str, Any]:
    raise ActionError("AutoML is disabled until a worst-case candidate and infrastructure cost estimator is enforced")


def action_deploy_trained(body: dict, policy: dict) -> Dict[str, Any]:
    _reject_direct_production_deploy()
    _reject_pinned(body)
    job = str(body.get("job_name") or "").strip()
    if not job:
        raise ActionError("job_name required")
    serverless = bool(body.get("serverless", True))
    it = body.get("instance_type")
    ttl = float(body.get("ttl_hours") or policy.get("endpoint_ttl_hours") or 3)
    if not serverless:
        _guard_instance(policy, it, "hosting", ttl)
    ep = body.get("endpoint_name") or ("jh-ai-clf-" + re.sub(r"[^a-z0-9-]", "-", job.lower()))[:56].rstrip("-")
    _assert_endpoint_write_allowed(ep)
    memory, concurrency = _serverless_config(body, policy) if serverless else (2048, 2)
    res = tr.deploy_training_output(client("sagemaker"), job_name=job, role_arn=execution_role(), endpoint_name=ep, serverless=serverless, instance_type=it,
                                    tags=cg.tags("trained:" + job, ttl, False),
                                    serverless_memory_mb=memory, serverless_max_conc=concurrency)
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
    sm = client("sagemaker")
    try:
        desc = sm.describe_endpoint(EndpointName=ep)
    except Exception:
        raise ActionError("endpoint %s was not found" % ep)
    arn = desc.get("EndpointArn")
    tags = _tags(arn or "")
    if tags.get(cg.TAG_MANAGED) != "true":
        raise ActionError("refusing to delete an endpoint not tagged %s=true" % cg.TAG_MANAGED)
    sm.delete_endpoint(EndpointName=ep)
    return {"endpoint": ep, "action": "deleting"}


def action_job_stop(body: dict, policy: dict) -> Dict[str, Any]:
    name = str(body.get("job_name") or "").strip()
    kind = body.get("kind") or "training"
    if not name:
        raise ActionError("job_name required")
    sm = client("sagemaker")
    arn = None
    if kind == "automl":
        try:
            arn = sm.describe_auto_ml_job(AutoMLJobName=name).get("AutoMLJobArn")
        except Exception:
            raise ActionError("AutoML job %s was not found or could not be described" % name)
    else:
        try:
            arn = sm.describe_training_job(TrainingJobName=name).get("TrainingJobArn")
        except Exception:
            raise ActionError("training job %s was not found or could not be described" % name)
    if _tags(arn or "").get(cg.TAG_MANAGED) != "true":
        raise ActionError("refusing to stop a job not tagged %s=true" % cg.TAG_MANAGED)
    if kind == "automl":
        sm.stop_auto_ml_job(AutoMLJobName=name)
    else:
        sm.stop_training_job(TrainingJobName=name)
    return {"job_name": name, "kind": kind, "action": "stopping"}


def action_policy(body: dict, policy: dict) -> Dict[str, Any]:
    try:
        return cg.save_policy(client("s3"), PRIVATE_BUCKET, body.get("patch") or {})
    except ValueError as exc:
        raise ActionError(str(exc))


def action_hyperpod(body: dict, policy: dict) -> Dict[str, Any]:
    why = tr.hyperpod_gate(policy, body.get("confirm") or "")
    if why:
        raise ActionError(why)
    it = str(body.get("instance_type") or "")
    if not it or not body.get("lifecycle_s3"):
        raise ActionError("instance_type and lifecycle_s3 (s3:// prefix with on_create.sh) required")
    count = int(body.get("count") or 1)
    if not 1 <= count <= 8:
        raise ActionError("HyperPod count must be between 1 and 8 nodes")
    _guard_instance(policy, it, "training", 24.0 * count)
    return tr.create_hyperpod_cluster(client("sagemaker"), name=body.get("name") or "jh-ai-hyperpod", role_arn=execution_role(), instance_type=it,
                                      count=count, lifecycle_s3=body["lifecycle_s3"], tags=cg.tags("hyperpod", None, False))


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
    if prev.get("generated_at"):
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(prev["generated_at"])).total_seconds()
            if age < mr.MIN_READ_GAP_S:
                raise ActionError("last read is %d min old; a new governed read is allowed every %d min" % (age // 60, mr.MIN_READ_GAP_S // 60))
        except ActionError:
            raise
        except Exception:
            pass
    t0 = time.time()
    board = mr.build_board(s3, PUBLIC_BUCKET, BRAIN_SOURCE_BUCKET)
    fleet = fi.build_fleet_snapshot(s3, PUBLIC_BUCKET)
    board["fleet_coverage"] = {k: fleet.get(k) for k in ("status", "summary")}
    board["fleet_digest"] = fleet.get("digest") or []
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
        # Fail closed. Budget, mode and timeout decisions in the governed router
        # must never be silently bypassed by a direct third-party request.
        return ""

    read = mr.compose_read(board, play, complete)
    read["llm_path"] = "governed-router"
    critical = [name for name in ("fusion", "risk_gate", "khalid_risk") if (board.get("sources") or {}).get(name, {}).get("status") != "FRESH"]
    fleet_summary = fleet.get("summary") or {}
    unique_feeds = int(fleet_summary.get("unique_feeds") or 0)
    eligible_feeds = int(fleet_summary.get("eligible") or 0)
    coverage = (eligible_feeds / unique_feeds) if unique_feeds else 0.0
    blockers = ["critical source %s is not FRESH" % name for name in critical]
    if fleet.get("status") != "READY":
        blockers.append("fleet registry is not READY")
    for blocker in fleet.get("release_blockers") or []:
        if blocker not in blockers:
            blockers.append(blocker)
    read["decision_status"] = "ADVISORY_ONLY" if blockers else "EVIDENCE_READY"
    read["release_blockers"] = blockers
    if blockers:
        read["calls"] = []
    read_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    logged = []
    if read.get("calls") and not read.get("parse_error"):
        try:
            from signals_emit import log_signal, yprice
            logged = mr.log_calls(_signals_table(), read_id, read["calls"], log_signal, yprice)
        except Exception as e:
            logged = [{"error": "ledger unavailable: %s" % str(e)[:120]}]
    calls_doc = get_json(PRIVATE_BUCKET, CALLS_KEY) or {"calls": []}
    calls_doc["calls"] = (calls_doc.get("calls") or []) + [r for r in logged if r.get("signal_id") and r.get("logged") is True]
    calls_doc["calls"] = list({r["signal_id"]: r for r in calls_doc["calls"] if r.get("signal_id")}.values())
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


# ══════════════════════════════════════════════════════════════ pipeline
def endpoint_log_tail(name: str, n: int = 40) -> List[str]:
    logs = client("logs")
    group = "/aws/sagemaker/Endpoints/%s" % name
    out: List[str] = []
    try:
        streams = logs.describe_log_streams(logGroupName=group, orderBy="LastEventTime", descending=True, limit=3).get("logStreams") or []
        for stm in streams[:2]:
            ev = logs.get_log_events(logGroupName=group, logStreamName=stm["logStreamName"], limit=150, startFromHead=False).get("events") or []
            msgs = [e.get("message", "").rstrip()[:240] for e in ev]
            keep = [m for m in msgs if re.search(r"error|exception|traceback|fail|cannot|no module|not found|killed|memory|worker|exit|must be", m, re.I)]
            out.append("== %s (%d events)" % (stm["logStreamName"][-32:], len(msgs)))
            out.extend((keep or msgs)[-n:])
        if not streams:
            out.append("no log streams (container never started: image pull / artifact stage)")
    except Exception as e:
        out.append("log tail unavailable: %s" % str(e)[:120])
    return out


def pipeline_api(policy: dict, context=None) -> Dict[str, Any]:
    sm = client("sagemaker")
    return {
        "dataset_build": lambda b: action_dataset_build(b, policy),
        "deploy": lambda b: action_deploy(b, policy),
        "describe_endpoint": lambda name: sm.describe_endpoint(EndpointName=name),
        "endpoint_delete": lambda name: sm.delete_endpoint(EndpointName=name),
        "log_tail": endpoint_log_tail,
        "embed": lambda b: action_embed(b, policy, context),
        "train_classifier": lambda b: action_train_classifier(b, policy),
        "describe_training_job": lambda name: sm.describe_training_job(TrainingJobName=name),
        "deploy_trained": lambda b: action_deploy_trained(b, policy),
        "infer": lambda b: action_infer(b, policy),
        "market_read": lambda b: action_market_read(b, policy, context),
    }


def _pipeline(policy: dict, context=None) -> pl.Pipeline:
    return pl.Pipeline(pipeline_api(policy, context), lambda b, k: get_json(b, k), lambda b, k, o: put_private(k, o) if b == PRIVATE_BUCKET else put_public(k, o), PRIVATE_BUCKET)


def action_pipeline_start(body: dict, policy: dict, context=None) -> Dict[str, Any]:
    cat = get_json(PRIVATE_BUCKET, CATALOG_KEY) or {}
    ladder = body.get("ladder")
    if not ladder:
        ladder = [m for m in (cat.get("article_models_present") or []) if "base" in m and "wiki" not in m][:1]
        ladder += [c["model_id"] for c in (cat.get("cards") or []) if c["model_id"] in pl.RETRIEVAL_LADDER][:2]
    if not ladder:
        raise ActionError("no embedding card in the catalog yet -- refresh the catalog first")
    evidence = body.get("governance_evidence")
    if evidence is not None:
        if not isinstance(evidence, Mapping):
            raise ActionError("governance_evidence must be an immutable receipt reference")
        unknown = sorted(set(evidence) - {"uri", "version_id", "sha256"})
        if unknown:
            raise ActionError(
                "governance_evidence contains unknown fields: %s" % ", ".join(unknown)
            )
    p = _pipeline(policy, context)
    st = p.start(
        ladder,
        force=bool(body.get("force")),
        governance_evidence=dict(evidence) if evidence is not None else None,
    )
    if body.get("tick", True):
        st = p.tick(budget_s=_budget(context))
    return pl.public_view(st)


def _budget(context, reserve_s: float = 90.0) -> float:
    if context is None:
        return 540.0
    return max(60.0, context.get_remaining_time_in_millis() / 1000.0 - reserve_s)


def action_pipeline_tick(body: dict, policy: dict, context=None) -> Dict[str, Any]:
    st = _pipeline(policy, context).tick(budget_s=_budget(context))
    return pl.public_view(st)


def action_pipeline_stop(body: dict, policy: dict) -> Dict[str, Any]:
    st = get_json(PRIVATE_BUCKET, pl.STATE_KEY) or {}
    if st.get("status") == "running":
        st["status"] = "stopped"
        st["stopped_at"] = now_iso()
        put_private(pl.STATE_KEY, st)
    return pl.public_view(st)


# ═══════════════════════════════════════════════════════════ AI governance
def _required_setting(name: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ActionError("%s is not configured" % name)
    return value.strip()


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ActionError("%s must be an object" % field)
    return value


def _only_keys(value: Mapping[str, Any], allowed: Sequence[str], field: str = "request") -> None:
    unknown = sorted(set(value) - set(allowed))
    if unknown:
        raise ActionError("%s contains unknown fields: %s" % (field, ", ".join(unknown)))


def _boolean(value: Any, field: str, default: bool) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ActionError("%s must be boolean" % field)
    return value


def _bounded_list(value: Any, field: str, *, maximum: int) -> list:
    if not isinstance(value, list) or not value:
        raise ActionError("%s must be a non-empty list" % field)
    if len(value) > maximum:
        raise ActionError("%s exceeds the %d-item limit" % (field, maximum))
    return value


def _json_digest(value: Any) -> str:
    try:
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False, default=str).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ActionError("request must contain finite JSON") from exc
    return hashlib.sha256(raw).hexdigest()


def _conditional_conflict(exc: Exception) -> bool:
    text = (type(exc).__name__ + " " + str(exc)).lower()
    return "precondition" in text or "conditional" in text or "already exists" in text


def _put_immutable(s3: Any, bucket: str, key: str, document: Mapping[str, Any]) -> bool:
    """Put a JSON object once. Return False only for an existing immutable key."""
    body = json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False, default=str).encode("utf-8")
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
            CacheControl="private, no-store",
            IfNoneMatch="*",
        )
        return True
    except Exception as exc:
        if _conditional_conflict(exc):
            return False
        raise


def _governance_call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except ActionError:
        raise
    except (ValueError, TypeError, KeyError) as exc:
        raise ActionError(str(exc)) from exc


def _strict_envelope(value: Any, *, clean: bool = False) -> Dict[str, Any]:
    envelope = _object(value, "envelope")
    _only_keys(
        envelope,
        ("schema_version", "signal_id", "source", "entity_id", "event_time",
         "available_at", "produced_at", "payload", "provenance", "taint"),
        "envelope",
    )
    return signals.require_clean(envelope, purpose="governed ingestion") if clean else signals.validate_signal_envelope(envelope)


def action_signal_validate(body: dict, policy: dict) -> Dict[str, Any]:
    body = _object(body, "request")
    _only_keys(body, ("envelope",))
    normalized = _strict_envelope(body.get("envelope"))
    return {
        "schema_version": normalized["schema_version"],
        "signal_id": normalized["signal_id"],
        "status": normalized["taint"]["status"],
        "fingerprint": signals.envelope_fingerprint(normalized),
        "envelope": normalized,
    }


def action_signal_ingest(
    body: dict,
    policy: dict,
    *,
    s3_client: Any = None,
    events_client: Any = None,
    outbox: Any = None,
) -> Dict[str, Any]:
    """Stage one clean envelope in the durable outbox, archive, then publish.

    Dry-run is the default. Repeating the same envelope is idempotent after a
    PUBLISHED receipt and redrives a prior PENDING/expired delivery.
    EventBridge and the outbox retain at-least-once semantics.
    """
    body = _object(body, "request")
    _only_keys(body, ("envelope", "dry_run"))
    normalized = _strict_envelope(body.get("envelope"), clean=True)
    fingerprint = signals.envelope_fingerprint(normalized)
    dry_run = _boolean(body.get("dry_run"), "dry_run", True)
    archive_key = "signals/v1/%s.json" % fingerprint
    outbox_key = "ai/signals/outbox/v1/%s.json" % fingerprint
    event_contract = {"source": "justhodl.engine", "detail_type": "SignalEnvelope/v1"}
    result = {
        "mode": "DRY_RUN" if dry_run else "INGEST",
        "signal_id": normalized["signal_id"],
        "fingerprint": fingerprint,
        "archive_key": archive_key,
        "outbox_key": outbox_key,
        "event_contract": event_contract,
        "side_effects": 0,
    }
    if dry_run:
        return result
    archive_bucket = _required_setting("AI_BRAIN_SOURCE_BUCKET", BRAIN_SOURCE_BUCKET)
    outbox_bucket = _required_setting("AI_PRIVATE_BUCKET", PRIVATE_BUCKET)
    bus = _required_setting("AI_EVENT_BUS", AI_EVENT_BUS)
    s3 = s3_client or client("s3")
    events = events_client or client("events")
    queue = outbox or event_outbox.S3EventOutbox(
        s3,
        outbox_bucket=outbox_bucket,
        archive_bucket=archive_bucket,
    )

    def publish(payload: Mapping[str, Any], contract: Mapping[str, str]) -> str:
        response = events.put_events(Entries=[{
            "Source": contract["source"],
            "DetailType": contract["detail_type"],
            "Detail": json.dumps(
                payload, sort_keys=True, separators=(",", ":"), allow_nan=False
            ),
            "EventBusName": bus,
        }])
        if not isinstance(response, Mapping) or response.get("FailedEntryCount") != 0:
            raise event_outbox.OutboxError("EventBridge rejected the outbox event")
        entries = response.get("Entries") or []
        if len(entries) != 1 or not entries[0].get("EventId"):
            raise event_outbox.OutboxError("EventBridge did not confirm the outbox event")
        return str(entries[0]["EventId"])

    try:
        outcome = queue.ingest(
            message_id=fingerprint,
            payload=normalized,
            archive_key=archive_key,
            event_contract=event_contract,
            publisher=publish,
        )
    except Exception as exc:
        raise ActionError(
            "signal ingestion failed closed; retry the same envelope to redrive its durable outbox"
        ) from exc
    side_effects = (
        int(bool(outcome.get("created")))
        + int(bool(outcome.get("archive_created")))
        + (3 if outcome.get("delivery_attempted") else 0)
    )
    result.update({
        key: outcome.get(key)
        for key in (
            "archived", "published", "duplicate", "redriven", "status",
            "attempts", "event_id", "outbox_key", "delivery_attempted",
        )
    })
    result["side_effects"] = side_effects
    return result


def action_feature_assemble(
    body: dict,
    policy: dict,
    *,
    store: Any = None,
    sagemaker_client: Any = None,
    athena_client: Any = None,
) -> Dict[str, Any]:
    body = _object(body, "request")
    _only_keys(body, ("entity_id", "as_of", "feature_names", "observations", "max_age_seconds", "require_all", "allow_tainted"))
    names = _bounded_list(
        body.get("feature_names"),
        "feature_names",
        maximum=feature_store.MAX_FEATURE_NAMES,
    )
    entity_id = str(body.get("entity_id") or "").strip()
    as_of = signals.parse_timestamp(body.get("as_of"), "as_of")
    supplied = body.get("observations") is not None
    if supplied:
        observations = _bounded_list(body.get("observations"), "observations", maximum=10_000)
        rows = []
        allowed = ("entity_id", "feature_name", "event_time", "available_at", "value", "source", "revision", "tainted")
        for index, item in enumerate(observations):
            item = _object(item, "observations[%d]" % index)
            _only_keys(item, allowed, "observations[%d]" % index)
            rows.append(pit.FeatureObservation(
                entity_id=item.get("entity_id"),
                feature_name=item.get("feature_name"),
                event_time=signals.parse_timestamp(item.get("event_time"), "observations[%d].event_time" % index),
                available_at=signals.parse_timestamp(item.get("available_at"), "observations[%d].available_at" % index),
                value=item.get("value"),
                source=item.get("source"),
                revision=item.get("revision", 0),
                tainted=item.get("tainted", False),
            ))
        selected_store = pit.InMemoryFeatureStore(rows)
        retrieval_mode = "SUPPLIED_OBSERVATIONS_DRY_RUN"
    else:
        if _boolean(body.get("allow_tainted"), "allow_tainted", False):
            raise ActionError("allow_tainted is not permitted for Feature Store retrieval")
        group_name = _required_setting("SIGNAL_FEATURE_GROUP", SIGNAL_FEATURE_GROUP)
        if store is None:
            query = feature_store.AthenaFeatureStoreQuery(
                sagemaker_client or client("sagemaker"),
                athena_client or client("athena"),
                output_location=FEATURE_STORE_QUERY_OUTPUT,
                workgroup=FEATURE_STORE_ATHENA_WORKGROUP,
            )
            selected_store = feature_store.SageMakerFeatureStore(query, group_name)
        else:
            selected_store = store
        retrieval_mode = "SAGEMAKER_FEATURE_STORE"
    max_age = body.get("max_age_seconds")
    if max_age is not None:
        if isinstance(max_age, bool) or not isinstance(max_age, (int, float)) or max_age < 0:
            raise ActionError("max_age_seconds must be a non-negative number")
        max_age = timedelta(seconds=float(max_age))
    vector = pit.PointInTimeFeatureAssembler(selected_store).assemble(
        entity_id,
        as_of,
        names,
        max_age=max_age,
        require_all=_boolean(body.get("require_all"), "require_all", True),
        allow_tainted=(
            _boolean(body.get("allow_tainted"), "allow_tainted", False)
            if supplied else False
        ),
    )
    return {
        "mode": retrieval_mode,
        "entity_id": vector.entity_id,
        "as_of": vector.as_of.isoformat().replace("+00:00", "Z"),
        "values": dict(vector.values),
        "lineage": list(vector.lineage),
        "fingerprint": vector.fingerprint,
        "feature_group": _required_setting("SIGNAL_FEATURE_GROUP", SIGNAL_FEATURE_GROUP),
    }


def _label_result(value: labels.OutcomeLabel) -> Dict[str, Any]:
    return {
        "matured": value.matured,
        "label": value.label,
        "side": value.side,
        "horizon": {"value": value.horizon.value, "unit": value.horizon.unit, "anchor": value.horizon.anchor},
        "entry_time": value.entry_time.isoformat().replace("+00:00", "Z") if value.entry_time else None,
        "exit_time": value.exit_time.isoformat().replace("+00:00", "Z") if value.exit_time else None,
        "entry_price": value.entry_price,
        "exit_price": value.exit_price,
        "gross_return": value.gross_return,
        "transaction_cost": value.transaction_cost,
        "net_return": value.net_return,
        "reason": value.reason,
    }


def action_outcome_labels(body: dict, policy: dict) -> Dict[str, Any]:
    body = _object(body, "request")
    _only_keys(body, ("prediction_time", "prices", "horizons", "side", "transaction_cost_bps", "flat_threshold_bps"))
    price_items = _bounded_list(body.get("prices"), "prices", maximum=100_000)
    prices = []
    for index, item in enumerate(price_items):
        item = _object(item, "prices[%d]" % index)
        _only_keys(item, ("timestamp", "price"), "prices[%d]" % index)
        prices.append(labels.PricePoint(signals.parse_timestamp(item.get("timestamp"), "prices[%d].timestamp" % index), item.get("price")))
    horizon_items = _bounded_list(body.get("horizons"), "horizons", maximum=100)
    horizons = []
    for index, item in enumerate(horizon_items):
        item = _object(item, "horizons[%d]" % index)
        _only_keys(item, ("value", "unit", "anchor"), "horizons[%d]" % index)
        horizons.append(labels.Horizon(item.get("value"), item.get("unit", "calendar_days"), item.get("anchor", "entry")))
    results = labels.labels_for_horizons(
        prices,
        signals.parse_timestamp(body.get("prediction_time"), "prediction_time"),
        horizons,
        side=body.get("side", "LONG"),
        transaction_cost_bps=body.get("transaction_cost_bps", 0.0),
        flat_threshold_bps=body.get("flat_threshold_bps", 0.0),
    )
    return {"labels": [_label_result(item) for item in results], "count": len(results)}


def _split_inputs(body: Mapping[str, Any]):
    interval_items = _bounded_list(body.get("intervals"), "intervals", maximum=100_000)
    first = _object(interval_items[0], "intervals[0]")
    datetime_axis = isinstance(first.get("start"), str)
    rows = []
    for index, item in enumerate(interval_items):
        item = _object(item, "intervals[%d]" % index)
        _only_keys(item, ("start", "end"), "intervals[%d]" % index)
        if datetime_axis:
            start = signals.parse_timestamp(item.get("start"), "intervals[%d].start" % index)
            end = signals.parse_timestamp(item.get("end"), "intervals[%d].end" % index)
        else:
            start, end = item.get("start"), item.get("end")
            if any(isinstance(v, bool) or not isinstance(v, (int, float)) for v in (start, end)):
                raise ActionError("all interval bounds must be consistently numeric or RFC3339")
        rows.append(splits.SampleInterval(start, end))
    def gap(name: str):
        value = body.get(name, 0)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
            raise ActionError("%s must be a non-negative number" % name)
        return timedelta(seconds=float(value)) if datetime_axis else value
    return rows, gap("purge"), gap("embargo")


def _split_summary(rows: Sequence[splits.SampleInterval], values: Sequence[splits.Split], kind: str) -> Dict[str, Any]:
    canonical = [{
        "train": list(item.train_indices),
        "test": list(item.test_indices),
        "test_groups": list(item.test_groups),
    } for item in values]
    summaries = [{
        "split": index,
        "train_count": len(item.train_indices),
        "test_count": len(item.test_indices),
        "train_first": item.train_indices[0] if item.train_indices else None,
        "train_last": item.train_indices[-1] if item.train_indices else None,
        "test_first": item.test_indices[0] if item.test_indices else None,
        "test_last": item.test_indices[-1] if item.test_indices else None,
        "test_groups": list(item.test_groups),
    } for index, item in enumerate(values)]
    return {
        "kind": kind,
        "sample_count": len(rows),
        "split_count": len(values),
        "splits": summaries,
        "split_digest": _json_digest(canonical),
    }


def action_walk_forward(body: dict, policy: dict) -> Dict[str, Any]:
    body = _object(body, "request")
    _only_keys(body, ("intervals", "n_splits", "test_size", "min_train_size", "purge", "embargo"))
    rows, purge, embargo = _split_inputs(body)
    values = splits.purged_walk_forward_splits(
        rows,
        n_splits=body.get("n_splits"),
        test_size=body.get("test_size"),
        min_train_size=body.get("min_train_size"),
        purge=purge,
        embargo=embargo,
    )
    return _split_summary(rows, values, "PURGED_WALK_FORWARD")


def action_cpcv(body: dict, policy: dict) -> Dict[str, Any]:
    body = _object(body, "request")
    _only_keys(body, ("intervals", "n_groups", "test_groups", "purge", "embargo"))
    rows, purge, embargo = _split_inputs(body)
    values = splits.combinatorial_purged_cv_splits(
        rows,
        n_groups=body.get("n_groups"),
        test_groups=body.get("test_groups"),
        purge=purge,
        embargo=embargo,
    )
    return _split_summary(rows, values, "CPCV")


def _prediction_table():
    name = _required_setting("PREDICTION_LEDGER_TABLE", PREDICTION_LEDGER_TABLE)
    return boto3.resource("dynamodb", region_name=REGION).Table(name)


def _prediction_archive_key(kind: str, identifier: str, created_at: str) -> str:
    stamp = re.sub(r"[^0-9A-Za-z_.-]", "-", created_at)
    return "predictions/%s/%s/%s.json" % (kind, identifier, stamp)


def action_prediction_write(body: dict, policy: dict, *, table: Any = None, s3_client: Any = None) -> Dict[str, Any]:
    body = _object(body, "request")
    _only_keys(body, ("prediction",))
    normalized = ledger.validate_prediction(_object(body.get("prediction"), "prediction"))
    target = table or _prediction_table()
    archive_bucket = _required_setting("PREDICTION_LEDGER_ARCHIVE_BUCKET", PREDICTION_LEDGER_ARCHIVE_BUCKET)
    outcome = ledger.ArchiveFirstPredictionLedger(
        target, s3_client or client("s3"), archive_bucket
    ).write(normalized)
    return {
        "prediction_id": normalized["prediction_id"],
        "written": outcome["written"],
        "duplicate": outcome["duplicate"],
        "archived": outcome["archived"] or outcome["archive_duplicate"],
        "archive_key": outcome["archive_key"],
    }


def _append_ledger_event(table: Any, item: Dict[str, Any]) -> bool:
    try:
        table.put_item(Item=item, ConditionExpression="attribute_not_exists(prediction_id)")
        return True
    except Exception as exc:
        if not _conditional_conflict(exc):
            raise
    try:
        current = table.get_item(
            Key={"prediction_id": item["prediction_id"], "created_at": item["created_at"]},
            ConsistentRead=True,
        ).get("Item")
    except TypeError:
        current = table.get_item(Key={"prediction_id": item["prediction_id"], "created_at": item["created_at"]}).get("Item")
    if current is None or _json_digest(current) != _json_digest(item):
        raise ledger.PredictionConflictError("grade event identity collides with different content")
    return False


def action_prediction_grade(body: dict, policy: dict, *, table: Any = None, s3_client: Any = None) -> Dict[str, Any]:
    body = _object(body, "request")
    _only_keys(body, ("prediction_id", "outcome", "graded_at"))
    prediction_id = str(body.get("prediction_id") or "").strip()
    if not re.fullmatch(r"pred-[a-f0-9]{40}", prediction_id):
        raise ActionError("prediction_id is invalid")
    target = table or _prediction_table()
    original = ledger.PredictionLedger(target)._get(prediction_id)
    if original is None:
        raise ActionError("prediction does not exist")
    normalized_outcome = ledger.validate_outcome(_object(body.get("outcome"), "outcome"))
    graded_at = signals.format_timestamp(signals.parse_timestamp(body.get("graded_at"), "graded_at"))
    outcome_digest = _json_digest(normalized_outcome)
    grade_id = "grade-" + _json_digest({"prediction_id": prediction_id, "outcome": normalized_outcome})[:40]
    event = {
        "schema_version": "1.0",
        "event_type": "OUTCOME_GRADE",
        "prediction_id": grade_id,
        "subject_prediction_id": prediction_id,
        "created_at": normalized_outcome["exit_time"],
        "graded_at": graded_at,
        "outcome": normalized_outcome,
        "outcome_digest": outcome_digest,
        "correct": original.get("prediction") == normalized_outcome["label"],
        "model_id": original.get("model_id"),
        "model_version": original.get("model_version"),
        "entity_id": original.get("entity_id"),
    }
    archive_bucket = _required_setting("PREDICTION_LEDGER_ARCHIVE_BUCKET", PREDICTION_LEDGER_ARCHIVE_BUCKET)
    outcome = ledger.ArchiveFirstPredictionLedger(
        target, s3_client or client("s3"), archive_bucket
    ).append_grade(event)
    return {
        "grade_id": grade_id,
        "subject_prediction_id": prediction_id,
        "graded": outcome["written"],
        "duplicate": outcome["duplicate"],
        "correct": event["correct"],
        "archived": outcome["archived"] or outcome["archive_duplicate"],
        "archive_key": outcome["archive_key"],
    }


def action_model_package_request(
    body: dict,
    policy: dict,
    *,
    sagemaker_client: Any = None,
    control: Optional[governance.ControlledExecution] = None,
) -> Dict[str, Any]:
    body = _object(body, "request")
    allowed = ("package_group_name", "model_data_url", "image_uri", "content_types", "response_types",
               "model_metrics", "customer_metadata", "description", "tags", "dry_run",
               "approval_token", "owner", "estimated_cost_usd")
    _only_keys(body, allowed)
    configured = _required_setting("MODEL_PACKAGE_GROUP", MODEL_PACKAGE_GROUP)
    supplied = body.get("package_group_name")
    if supplied is not None and supplied != configured:
        raise ActionError("package_group_name must match the configured governance boundary")
    args = {key: value for key, value in body.items() if key not in (
        "dry_run", "approval_token", "owner", "estimated_cost_usd"
    )}
    args["package_group_name"] = configured
    dry_run = _boolean(body.get("dry_run"), "dry_run", True)
    result = registry.register_model_package(
        sagemaker_client,
        live=not dry_run,
        control=control,
        approval_token=body.get("approval_token"),
        owner=body.get("owner"),
        estimated_cost_usd=body.get("estimated_cost_usd", 0.0),
        **args,
    )
    if dry_run:
        result["mode"] = "REQUEST_ONLY"
    return result


def action_model_card(
    body: dict,
    policy: dict,
    *,
    model_card_client: Any = None,
    control: Optional[governance.ControlledExecution] = None,
) -> Dict[str, Any]:
    body = _object(body, "request")
    allowed = ("model_card_name", "model_id", "model_version", "owner", "intended_use", "limitations",
               "risk_rating", "training_dataset", "evaluation", "lineage", "created_at", "model_package_arn", "tags",
               "dry_run", "approval_token", "estimated_cost_usd")
    _only_keys(body, allowed)
    args = {key: value for key, value in body.items() if key not in (
        "dry_run", "approval_token", "estimated_cost_usd", "owner"
    )}
    dry_run = _boolean(body.get("dry_run"), "dry_run", True)
    result = registry.create_model_card(
        model_card_client,
        live=not dry_run,
        control=control,
        approval_token=body.get("approval_token"),
        owner=body.get("owner"),
        estimated_cost_usd=body.get("estimated_cost_usd", 0.0),
        **args,
    )
    if dry_run:
        result["mode"] = "REQUEST_ONLY"
    return result


def action_mlflow_lineage(
    body: dict,
    policy: dict,
    *,
    writer: Any = None,
    control: Optional[governance.ControlledExecution] = None,
) -> Dict[str, Any]:
    body = _object(body, "request")
    _only_keys(body, ("metadata", "dry_run", "approval_token", "owner", "estimated_cost_usd"))
    normalized = registry.validate_mlflow_lineage_metadata(_object(body.get("metadata"), "metadata"))
    experiment = _required_setting("MLFLOW_EXPERIMENT_NAME", MLFLOW_EXPERIMENT_NAME)
    if normalized["experiment_name"] != experiment:
        raise ActionError("lineage experiment_name must match the configured governance boundary")
    dry_run = _boolean(body.get("dry_run"), "dry_run", True)
    result = registry.publish_mlflow_lineage(
        writer,
        normalized,
        live=not dry_run,
        control=control,
        approval_token=body.get("approval_token"),
        owner=body.get("owner"),
        estimated_cost_usd=body.get("estimated_cost_usd", 0.0),
    )
    result.update({
        "valid": True,
        "tracking_server": {
            "name": _required_setting("MLFLOW_TRACKING_SERVER_NAME", MLFLOW_TRACKING_SERVER_NAME),
            "arn": _required_setting("MLFLOW_TRACKING_SERVER_ARN", MLFLOW_TRACKING_SERVER_ARN),
        },
    })
    return result


def _governance_live_control(policy: Mapping[str, Any]) -> governance.ControlledExecution:
    token = _required_setting(
        "AI_GOVERNANCE_APPROVAL_TOKEN",
        os.environ.get("AI_GOVERNANCE_APPROVAL_TOKEN", ""),
    )
    owner = _required_setting(
        "AI_GOVERNANCE_OWNER", os.environ.get("AI_GOVERNANCE_OWNER", "")
    )
    return governance.ControlledExecution(
        expected_approval_token=token,
        expected_owner=owner,
        budget=governance.ExecutionBudget(
            max_api_calls=int(policy.get("governance_max_api_calls", 8)),
            max_estimated_cost_usd=float(
                policy.get("governance_max_estimated_cost_usd", 5.0)
            ),
        ),
    )


def _governance_model_package_http(body: dict, policy: dict) -> Dict[str, Any]:
    live = body.get("dry_run") is False
    return action_model_package_request(
        body,
        policy,
        sagemaker_client=client("sagemaker") if live else None,
        control=_governance_live_control(policy) if live else None,
    )


def _governance_model_card_http(body: dict, policy: dict) -> Dict[str, Any]:
    live = body.get("dry_run") is False
    if live and not str(body.get("model_card_name") or "").startswith("jh-ai-"):
        raise ActionError("live model_card_name must use the governed jh-ai- prefix")
    return action_model_card(
        body,
        policy,
        model_card_client=client("sagemaker") if live else None,
        control=_governance_live_control(policy) if live else None,
    )


def _managed_mlflow_writer() -> registry.ManagedMlflowWriter:
    try:
        from mlflow import MlflowClient
        from mlflow.entities import Param, RunTag
    except Exception as exc:
        raise ActionError("the managed MLflow client dependency is unavailable") from exc
    tracking_arn = _required_setting(
        "MLFLOW_TRACKING_SERVER_ARN", MLFLOW_TRACKING_SERVER_ARN
    )
    return registry.ManagedMlflowWriter(
        MlflowClient(tracking_uri=tracking_arn),
        expected_experiment_name=_required_setting(
            "MLFLOW_EXPERIMENT_NAME", MLFLOW_EXPERIMENT_NAME
        ),
        param_factory=Param,
        tag_factory=RunTag,
    )


def _governance_mlflow_http(body: dict, policy: dict) -> Dict[str, Any]:
    live = body.get("dry_run") is False
    return action_mlflow_lineage(
        body,
        policy,
        writer=_managed_mlflow_writer() if live else None,
        control=_governance_live_control(policy) if live else None,
    )


def _thresholds(value: Any) -> dg.ReadinessThresholds:
    if value is None:
        return dg.ReadinessThresholds()
    value = _object(value, "thresholds")
    _only_keys(value, ("min_samples", "max_error_rate", "max_p95_latency_ms",
                       "max_latency_regression_ratio", "max_drift_score", "min_prediction_match_rate"), "thresholds")
    return dg.ReadinessThresholds(**dict(value))


def _decision(value: dg.GateDecision) -> Dict[str, Any]:
    return {
        "allowed": value.allowed,
        "action": value.action,
        "reasons": list(value.reasons),
        "evidence": dict(value.evidence),
    }


def action_deployment_readiness(body: dict, policy: dict) -> Dict[str, Any]:
    body = _object(body, "request")
    _only_keys(body, ("blue", "green", "thresholds"))
    decision = dg.evaluate_blue_green_readiness(
        _object(body.get("blue"), "blue"),
        _object(body.get("green"), "green"),
        thresholds=_thresholds(body.get("thresholds")),
    )
    return _decision(decision)


def action_canary_plan(
    body: dict,
    policy: dict,
    *,
    aws_clients: Optional[Mapping[str, Any]] = None,
    canary_actions: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Plan by default; live execution is possible only through explicit DI."""
    body = _object(body, "request")
    _only_keys(body, (
        "context", "thresholds", "dry_run", "approval_token", "owner",
        "estimated_cost_usd",
    ))
    dry_run = _boolean(body.get("dry_run"), "dry_run", True)
    context = _object(body.get("context", {}), "context")
    thresholds = _thresholds(body.get("thresholds"))
    bound_actions = {}
    if not dry_run:
        if not isinstance(aws_clients, Mapping) or not aws_clients or any(value is None for value in aws_clients.values()):
            raise ActionError("live canary execution requires dependency-injected AWS clients")
        missing_clients = {"sagemaker", "sagemaker-runtime", "cloudwatch"} - set(aws_clients)
        if missing_clients:
            raise ActionError("live canary execution is missing injected clients: %s" % ", ".join(sorted(missing_clients)))
        if not isinstance(canary_actions, Mapping) or not canary_actions:
            raise ActionError("live canary execution requires dependency-injected actions")
        # Bind clients into every callback rather than consulting the module's
        # global boto3 cache. This keeps live execution testable and fail closed.
        bound_actions = {
            name: (lambda context, callback=callback: callback(context, aws_clients))
            for name, callback in canary_actions.items()
        }
    machine = dg.CanaryStateMachine(
        bound_actions,
        expected_approval_token=os.environ.get("AI_CANARY_APPROVAL_TOKEN"),
        expected_owner=os.environ.get("AI_GOVERNANCE_OWNER"),
        budget=governance.ExecutionBudget(
            max_api_calls=int(policy.get("canary_max_api_calls", 9)),
            max_estimated_cost_usd=float(
                policy.get("canary_max_estimated_cost_usd", 25.0)
            ),
        ),
        thresholds=thresholds,
    )
    return machine.run(
        context,
        live=not dry_run,
        approval_token=body.get("approval_token"),
        owner=body.get("owner"),
        estimated_cost_usd=body.get("estimated_cost_usd", 0.0),
    )


def _governance_canary_http(body: dict, policy: dict) -> Dict[str, Any]:
    """Compose concrete AWS canary callbacks only for an explicit live request."""
    if body.get("dry_run") is not False:
        return action_canary_plan(body, policy)
    aws_clients = {
        "sagemaker": client("sagemaker"),
        "sagemaker-runtime": client("sagemaker-runtime"),
        "cloudwatch": client("cloudwatch"),
    }
    concrete = dg.SageMakerCanaryActions(
        aws_clients["sagemaker"],
        aws_clients["sagemaker-runtime"],
        aws_clients["cloudwatch"],
        sleep_fn=time.sleep,
    )
    actions = {
        name: (lambda context, clients, callback=callback: callback(context))
        for name, callback in concrete.callbacks().items()
    }
    return action_canary_plan(
        body, policy, aws_clients=aws_clients, canary_actions=actions
    )


GOVERNANCE_ACTIONS = {
    ("POST", "/governance/signals/validate"): lambda b, p, c: _governance_call(action_signal_validate, b, p),
    ("POST", "/governance/signals/ingest"): lambda b, p, c: _governance_call(action_signal_ingest, b, p),
    ("POST", "/governance/features/assemble"): lambda b, p, c: _governance_call(action_feature_assemble, b, p),
    ("POST", "/governance/outcomes/label"): lambda b, p, c: _governance_call(action_outcome_labels, b, p),
    ("POST", "/governance/splits/walk-forward"): lambda b, p, c: _governance_call(action_walk_forward, b, p),
    ("POST", "/governance/splits/cpcv"): lambda b, p, c: _governance_call(action_cpcv, b, p),
    ("POST", "/governance/predictions/write"): lambda b, p, c: _governance_call(action_prediction_write, b, p),
    ("POST", "/governance/predictions/grade"): lambda b, p, c: _governance_call(action_prediction_grade, b, p),
    ("POST", "/governance/models/package-request"): lambda b, p, c: _governance_call(_governance_model_package_http, b, p),
    ("POST", "/governance/models/card"): lambda b, p, c: _governance_call(_governance_model_card_http, b, p),
    ("POST", "/governance/models/mlflow-lineage/validate"): lambda b, p, c: _governance_call(_governance_mlflow_http, b, p),
    ("POST", "/governance/deployment/readiness"): lambda b, p, c: _governance_call(action_deployment_readiness, b, p),
    ("POST", "/governance/deployment/canary-plan"): lambda b, p, c: _governance_call(_governance_canary_http, b, p),
}


ACTIONS = {
    ("GET", "/inventory"): lambda b, p, c: owner_read_model(),
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
    ("POST", "/pipeline/start"): lambda b, p, c: action_pipeline_start(b, p, c),
    ("POST", "/pipeline/tick"): lambda b, p, c: action_pipeline_tick(b, p, c),
    ("POST", "/pipeline/stop"): lambda b, p, c: action_pipeline_stop(b, p),
    ("GET", "/pipeline"): lambda b, p, c: pl.public_view(get_json(PRIVATE_BUCKET, pl.STATE_KEY) or {}),
    ("GET", "/read"): lambda b, p, c: action_get_read(b, p),
    **GOVERNANCE_ACTIONS,
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
    if mode == "pipeline":
        st = _pipeline(_policy(), context).tick(budget_s=_budget(context))
        if st.get("status") in ("running", "done", "failed"):
            try:
                run_inventory(context, continue_embeddings=False)
            except Exception:
                pass
        return {"ok": True, "status": st.get("status"), "stage": st.get("stage")}
    if mode == "inventory":
        out = run_inventory(context, refresh_catalog=bool(event.get("refresh_catalog") or (event.get("body") or {}).get("refresh_catalog")))
        return {"ok": True, "generated_at": out["generated_at"], "endpoints": len(out["inventory"].get("endpoints") or []), "elapsed_s": out["elapsed_s"]}
    direct_path = "/" + mode.strip("/")
    if any(path == direct_path for _, path in GOVERNANCE_ACTIONS):
        return {"ok": False, "error": "governance actions require the owner-authenticated HTTP route"}
    fn = ACTIONS.get(("POST", direct_path)) or ACTIONS.get(("GET", direct_path))
    if not fn:
        return {"ok": False, "error": "unknown mode %s" % mode}
    try:
        return {"ok": True, "result": fn(event.get("body") or {}, _policy(), context)}
    except ActionError as e:
        return {"ok": False, "error": str(e)}
