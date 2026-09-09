"""cost_guard -- the part of the AI engine that protects the bill.

Doctrine (2026-09-09, after the recursion-loop mail): every SageMaker resource this engine
creates is tagged, priced from the live AWS Price List, and bounded:

  * endpoints carry a TTL tag; the hourly inventory deletes any managed endpoint past its
    TTL unless it is pinned (`justhodl-ai-pinned=true`), and any managed endpoint with zero
    invocations for `idle_hours` (CloudWatch Invocations metric, real);
  * training / AutoML jobs are created with MaxRuntimeInSeconds (and spot where supported);
  * every create action is refused when the projected daily run-rate of managed resources
    would exceed `daily_budget_usd`, or when the instance type is outside the allow list;
  * HyperPod / large GPU tiers are locked behind an explicit policy unlock.

Prices come from pricing:GetProducts (us-east-1 endpoint) and are cached for a day; when
the Price List is unreachable the guard says so and treats the price as UNKNOWN (which
blocks creation -- an unknown price is not a free price).
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

POLICY_KEY = "ai/policy.json"
PRICING_KEY = "ai/pricing.json"
TAG_MANAGED = "justhodl-ai-managed"
TAG_TTL = "justhodl-ai-ttl-hours"
TAG_PINNED = "justhodl-ai-pinned"
TAG_PURPOSE = "justhodl-ai-purpose"

DEFAULT_POLICY = {
    "version": 1,
    "daily_budget_usd": 5.0,
    "endpoint_ttl_hours": 3.0,
    "idle_hours": 2.0,
    "training_max_runtime_s": 3600,
    "training_spot": True,
    "allowed_inference_instances": ["ml.t2.medium", "ml.t2.large", "ml.m5.large", "ml.m5.xlarge", "ml.m5.2xlarge", "ml.c5.xlarge", "ml.c5.2xlarge", "ml.g4dn.xlarge"],
    "allowed_training_instances": ["ml.m5.large", "ml.m5.xlarge", "ml.m5.2xlarge", "ml.m5.4xlarge", "ml.c5.xlarge", "ml.c5.2xlarge", "ml.g4dn.xlarge", "ml.g4dn.2xlarge", "ml.g5.xlarge", "ml.g5.2xlarge"],
    "serverless_default": True,
    "hyperpod_unlocked": False,
    "large_gpu_unlocked": False,
    "updated_at": None,
}

SM_PRODUCT_FAMILY = {"hosting": "Hosting", "training": "Training", "processing": "Processing", "notebook": "Notebook", "studio": "Studio"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_json(s3, bucket, key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception:
        return default


def _put_json(s3, bucket, key, obj):
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(obj, default=str).encode(), ContentType="application/json",
                  ServerSideEncryption="AES256", CacheControl="private, no-store")


# ───────────────────────────────────────────────────────────────── policy
def load_policy(s3, bucket) -> Dict[str, Any]:
    p = dict(DEFAULT_POLICY)
    p.update(_get_json(s3, bucket, POLICY_KEY) or {})
    return p


def save_policy(s3, bucket, patch: Dict[str, Any]) -> Dict[str, Any]:
    p = load_policy(s3, bucket)
    for k, v in (patch or {}).items():
        if k in DEFAULT_POLICY and k != "version":
            if isinstance(DEFAULT_POLICY[k], bool):
                p[k] = bool(v)
            elif isinstance(DEFAULT_POLICY[k], (int, float)) and DEFAULT_POLICY[k] is not None:
                p[k] = float(v)
            elif isinstance(DEFAULT_POLICY[k], list):
                p[k] = [str(x) for x in v] if isinstance(v, list) else p[k]
    p["updated_at"] = now_iso()
    _put_json(s3, bucket, POLICY_KEY, p)
    return p


def tags(purpose: str, ttl_hours: Optional[float], pinned: bool = False) -> List[dict]:
    t = [{"Key": "justhodl", "Value": "ai"}, {"Key": TAG_MANAGED, "Value": "true"}, {"Key": TAG_PURPOSE, "Value": purpose[:120]}]
    if ttl_hours is not None:
        t.append({"Key": TAG_TTL, "Value": "%.2f" % float(ttl_hours)})
    if pinned:
        t.append({"Key": TAG_PINNED, "Value": "true"})
    return t


# ──────────────────────────────────────────────────────────────── pricing
def _price_from_products(products: List[str], want_family: str) -> Optional[float]:
    for raw in products:
        try:
            p = json.loads(raw)
        except Exception:
            continue
        attrs = (p.get("product") or {}).get("attributes") or {}
        fam = (attrs.get("component") or attrs.get("productFamily") or p.get("product", {}).get("productFamily") or "")
        if want_family.lower() not in str(fam).lower():
            continue
        terms = (p.get("terms") or {}).get("OnDemand") or {}
        for t in terms.values():
            for d in (t.get("priceDimensions") or {}).values():
                usd = (d.get("pricePerUnit") or {}).get("USD")
                try:
                    v = float(usd)
                    if v > 0:
                        return v
                except Exception:
                    pass
    return None


def hourly_price(pricing, s3, bucket, instance_type: str, family: str = "hosting") -> Dict[str, Any]:
    """USD per hour for an ml.* instance in us-east-1 from the live Price List; cached 24h."""
    cache = _get_json(s3, bucket, PRICING_KEY) or {"prices": {}, "updated_at": None}
    k = "%s|%s" % (instance_type, family)
    ent = (cache.get("prices") or {}).get(k)
    if ent and ent.get("fetched_at") and (time.time() - ent["fetched_at"]) < 86400:
        return ent
    usd, err = None, None
    try:
        r = pricing.get_products(ServiceCode="AmazonSageMaker", MaxResults=100, Filters=[
            {"Type": "TERM_MATCH", "Field": "regionCode", "Value": "us-east-1"},
            {"Type": "TERM_MATCH", "Field": "instanceName", "Value": instance_type},
        ])
        usd = _price_from_products(r.get("PriceList") or [], SM_PRODUCT_FAMILY.get(family, family))
        if usd is None:
            r2 = pricing.get_products(ServiceCode="AmazonSageMaker", MaxResults=100, Filters=[
                {"Type": "TERM_MATCH", "Field": "location", "Value": "US East (N. Virginia)"},
                {"Type": "TERM_MATCH", "Field": "instanceName", "Value": instance_type},
            ])
            usd = _price_from_products(r2.get("PriceList") or [], SM_PRODUCT_FAMILY.get(family, family))
        if usd is None:
            err = "no on-demand %s price row for %s in the Price List" % (family, instance_type)
    except Exception as e:
        err = str(e)[:140]
    ent = {"instance_type": instance_type, "family": family, "usd_per_hour": usd, "source": "aws-price-list", "fetched_at": time.time(),
           "fetched_at_iso": now_iso(), "error": err}
    cache.setdefault("prices", {})[k] = ent
    cache["updated_at"] = now_iso()
    try:
        _put_json(s3, bucket, PRICING_KEY, cache)
    except Exception:
        pass
    return ent


# ───────────────────────────────────────────────────────────── enforcement
def _tagmap(sm, arn: str) -> Dict[str, str]:
    try:
        return {t["Key"]: t["Value"] for t in (sm.list_tags(ResourceArn=arn).get("Tags") or [])}
    except Exception:
        return {}


def endpoint_invocations(cw, endpoint: str, hours: float) -> Optional[float]:
    try:
        end = datetime.now(timezone.utc)
        r = cw.get_metric_statistics(Namespace="AWS/SageMaker", MetricName="Invocations",
                                     Dimensions=[{"Name": "EndpointName", "Value": endpoint}, {"Name": "VariantName", "Value": "AllTraffic"}],
                                     StartTime=end - timedelta(hours=hours), EndTime=end, Period=3600, Statistics=["Sum"])
        return float(sum(d.get("Sum", 0) for d in r.get("Datapoints") or []))
    except Exception:
        return None


def enforce_endpoint_ttl(sm, cw, endpoints: List[dict], policy: Dict[str, Any]) -> List[dict]:
    """Delete managed endpoints past their TTL or idle beyond policy.idle_hours (never pinned ones).
    Returns the action ledger (one row per endpoint examined)."""
    ledger = []
    now = datetime.now(timezone.utc)
    for ep in endpoints:
        name = ep.get("name")
        t = ep.get("tags") or {}
        managed = t.get(TAG_MANAGED) == "true"
        row = {"endpoint": name, "managed": managed, "action": "keep", "reason": ""}
        if not managed:
            row["reason"] = "not created by this engine -- never touched"
            ledger.append(row)
            continue
        if t.get(TAG_PINNED) == "true":
            row["reason"] = "pinned"
            ledger.append(row)
            continue
        created = ep.get("created_at")
        age_h = None
        if created:
            try:
                age_h = (now - datetime.fromisoformat(str(created).replace("Z", "+00:00"))).total_seconds() / 3600
            except Exception:
                age_h = None
        ttl = None
        try:
            ttl = float(t.get(TAG_TTL)) if t.get(TAG_TTL) else float(policy.get("endpoint_ttl_hours") or 0)
        except Exception:
            ttl = float(policy.get("endpoint_ttl_hours") or 0)
        idle_h = float(policy.get("idle_hours") or 0)
        inv = endpoint_invocations(cw, name, idle_h) if idle_h > 0 else None
        row.update({"age_hours": round(age_h, 2) if age_h is not None else None, "ttl_hours": ttl, "invocations_window": inv})
        kill = None
        if ttl and age_h is not None and age_h > ttl:
            kill = "past TTL %.1fh (age %.1fh)" % (ttl, age_h)
        elif idle_h and age_h is not None and age_h > idle_h and inv == 0 and ep.get("status") == "InService":
            kill = "idle: 0 invocations in %.0fh" % idle_h
        if kill:
            try:
                sm.delete_endpoint(EndpointName=name)
                row.update({"action": "deleted", "reason": kill})
            except Exception as e:
                row.update({"action": "delete_failed", "reason": kill + " | " + str(e)[:120]})
        ledger.append(row)
    return ledger


def projected_daily_usd(endpoints: List[dict], jobs: List[dict], price_fn) -> Dict[str, Any]:
    """Run-rate of what is live right now: real-time endpoints x 24h + in-progress training jobs."""
    total, lines, unknown = 0.0, [], []
    for ep in endpoints:
        if ep.get("status") not in ("InService", "Creating", "Updating"):
            continue
        for v in ep.get("variants") or []:
            it = v.get("instance_type")
            n = int(v.get("instance_count") or 1)
            if not it:
                lines.append({"resource": ep["name"], "kind": "endpoint", "instance": "serverless", "usd_per_day": 0.0, "note": "pay per request"})
                continue
            pr = price_fn(it, "hosting")
            usd = pr.get("usd_per_hour")
            if usd is None:
                unknown.append(ep["name"])
                continue
            d = usd * 24 * n
            total += d
            lines.append({"resource": ep["name"], "kind": "endpoint", "instance": it, "count": n, "usd_per_hour": usd, "usd_per_day": round(d, 2)})
    for j in jobs:
        if j.get("status") not in ("InProgress", "Stopping"):
            continue
        it = j.get("instance_type")
        if not it:
            continue
        pr = price_fn(it, "training")
        usd = pr.get("usd_per_hour")
        if usd is None:
            unknown.append(j["name"])
            continue
        n = int(j.get("instance_count") or 1)
        d = usd * 24 * n
        total += d
        lines.append({"resource": j["name"], "kind": "training_job", "instance": it, "count": n, "usd_per_hour": usd, "usd_per_day": round(d, 2), "note": "if it ran a full day; jobs are capped by MaxRuntime"})
    return {"usd_per_day": round(total, 2), "lines": lines, "unpriced": unknown}


def check_budget(policy: Dict[str, Any], projected: Dict[str, Any], add_usd_per_hour: float, hours: float = 24.0) -> Optional[str]:
    """None when the action fits inside policy.daily_budget_usd, else the refusal reason."""
    budget = float(policy.get("daily_budget_usd") or 0)
    if budget <= 0:
        return "policy.daily_budget_usd is 0 -- every create is refused until you set a budget on the AI page"
    if projected.get("unpriced"):
        return "unpriced live resources %s -- refusing to add spend on top of an unknown run-rate" % projected["unpriced"][:3]
    new_total = float(projected.get("usd_per_day") or 0) + float(add_usd_per_hour) * hours
    if new_total > budget:
        return "projected %.2f USD/day would exceed the daily budget %.2f USD (raise it on the AI page if intended)" % (new_total, budget)
    return None


def instance_allowed(policy: Dict[str, Any], instance_type: str, family: str) -> Optional[str]:
    key = "allowed_training_instances" if family == "training" else "allowed_inference_instances"
    allowed = policy.get(key) or []
    if instance_type in allowed:
        return None
    big = re.match(r"^ml\.(p[3-5]|p4d|p4de|p5|p5e|trn|g5\.(4|8|12|16|24|48)|g6|inf)", instance_type or "")
    if big and not policy.get("large_gpu_unlocked"):
        return "%s is a large GPU/accelerator tier; unlock `large_gpu_unlocked` on the AI page first (prices run into dollars per hour)" % instance_type
    return "%s is not in policy.%s (edit the allow list on the AI page)" % (instance_type, key)


def mtd_sagemaker_cost(ce) -> Dict[str, Any]:
    """Month-to-date SageMaker spend from Cost Explorer (real bill data, ~24h lag)."""
    try:
        today = datetime.now(timezone.utc).date()
        start = today.replace(day=1)
        end = today + timedelta(days=1)
        r = ce.get_cost_and_usage(TimePeriod={"Start": start.isoformat(), "End": end.isoformat()}, Granularity="DAILY", Metrics=["UnblendedCost"],
                                  Filter={"Dimensions": {"Key": "SERVICE", "Values": ["Amazon SageMaker"]}})
        days = []
        total = 0.0
        for row in r.get("ResultsByTime") or []:
            amt = float((row.get("Total") or {}).get("UnblendedCost", {}).get("Amount") or 0)
            total += amt
            days.append({"date": (row.get("TimePeriod") or {}).get("Start"), "usd": round(amt, 4)})
        return {"usd_mtd": round(total, 2), "days": days[-14:], "source": "cost-explorer", "fetched_at": now_iso()}
    except Exception as e:
        return {"usd_mtd": None, "error": str(e)[:160], "source": "cost-explorer", "fetched_at": now_iso()}
