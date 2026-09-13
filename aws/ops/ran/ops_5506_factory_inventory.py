#!/usr/bin/env python3
"""Inspect the existing factory dependencies; never start jobs or modify IAM.

The only AWS write is an inventory record in the existing private bucket.
Public evidence contains schemas and capability summaries, never note contents,
environment values, tokens, or private model locations.
"""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

REGION = "us-east-1"
PUBLIC = "justhodl-dashboard-live"
PRIVATE = "justhodl-ai-857687956942"
CFG = Config(connect_timeout=5, read_timeout=20, retries={"max_attempts": 2})


def error_code(exc):
    return getattr(exc, "response", {}).get("Error", {}).get("Code", type(exc).__name__)


def inspect():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    sm = boto3.client("sagemaker", region_name=REGION, config=CFG)
    result = {"schema": "factory-inventory.v1", "generated_at": datetime.now(timezone.utc).isoformat(),
              "functions": {}, "sources": {}, "schedules": {}, "model_inventory": {}, "mutations": []}
    private = {"functions": {}, "endpoints": []}
    for name in ("justhodl-student-rsi", "justhodl-ai", "justhodl-finviz-signals"):
        try:
            c = lam.get_function_configuration(FunctionName=name)
            private["functions"][name] = {k: c.get(k) for k in
                ("FunctionArn", "Role", "Runtime", "Handler", "State", "LastUpdateStatus", "CodeSha256", "Timeout", "MemorySize")}
            result["functions"][name] = {k: c.get(k) for k in
                ("Runtime", "Handler", "State", "LastUpdateStatus", "CodeSha256", "Timeout", "MemorySize")}
            result["functions"][name]["exists"] = True
            result["functions"][name]["environment_keys"] = sorted((c.get("Environment") or {}).get("Variables", {}))
        except Exception as exc:
            result["functions"][name] = {"exists": None if error_code(exc) != "ResourceNotFoundException" else False,
                                          "error": error_code(exc)}
    keys = ("data/ai.json", "student-state.json", "data/student-state.json", "factory/salon/season.json",
            "factory/salon/board.json", "factory/scoreboard.json", "data/ofr-funding.json", "data/sofr.json",
            "data/fed-liquidity.json", "data/macro-regime.json", "data/market-overview.json",
            "data/warm/ofr/state.json", "data/tv-bars/state.json")
    for key in keys:
        try:
            response = s3.get_object(Bucket=PUBLIC, Key=key)
            body = response["Body"].read(1024 * 1024 + 1)
            if len(body) > 1024 * 1024:
                result["sources"][key] = {"exists": True, "inspection": "over_1mb_limit"}
                continue
            doc = json.loads(body)
            row = {"exists": True, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                   "last_modified": response["LastModified"].isoformat(),
                   "keys": sorted(doc)[:80] if isinstance(doc, dict) else [], "type": type(doc).__name__}
            if isinstance(doc, dict):
                row["field_shapes"] = {k: ({"type": "dict", "keys": sorted(v)[:25]} if isinstance(v, dict)
                                          else {"type": "list", "length": len(v), "first_keys": sorted(v[0])[:25] if v and isinstance(v[0], dict) else []}
                                          if isinstance(v, list) else {"type": type(v).__name__}) for k, v in list(doc.items())[:35]}
                for field in ("generated_at", "updated_at", "as_of", "version", "schema_version", "status"):
                    if isinstance(doc.get(field), (str, int, float, bool)):
                        row[field] = doc[field]
            result["sources"][key] = row
        except Exception as exc:
            result["sources"][key] = {"error": error_code(exc)}
    try:
        endpoints = sm.list_endpoints(MaxResults=100).get("Endpoints", [])
        for entry in endpoints:
            endpoint = sm.describe_endpoint(EndpointName=entry["EndpointName"])
            cfg = sm.describe_endpoint_config(EndpointConfigName=endpoint["EndpointConfigName"])
            variants = []
            for variant in cfg.get("ProductionVariants", []):
                model = sm.describe_model(ModelName=variant["ModelName"])
                container = model.get("PrimaryContainer") or {}
                variants.append({"model_name": variant["ModelName"], "instance_type": variant.get("InstanceType"),
                                 "serverless": variant.get("ServerlessConfig"), "image": container.get("Image"),
                                 "model_data_url": container.get("ModelDataUrl")})
            private["endpoints"].append({"name": entry["EndpointName"], "status": endpoint["EndpointStatus"], "variants": variants})
        result["model_inventory"] = {"endpoint_count": len(endpoints),
                                     "in_service": sum(e["EndpointStatus"] == "InService" for e in endpoints),
                                     "details": "private inventory only", "inference_attempted": False}
    except Exception as exc:
        result["model_inventory"] = {"error": error_code(exc), "inference_attempted": False}
    for service in ("events", "scheduler"):
        try:
            client = boto3.client(service, region_name=REGION, config=CFG)
            if service == "events":
                rows = client.list_rules(NamePrefix="justhodl-student").get("Rules", [])
                result["schedules"][service] = [{k: row.get(k) for k in ("Name", "State", "ScheduleExpression")} for row in rows]
            else:
                rows = client.list_schedules(NamePrefix="justhodl-student").get("Schedules", [])
                result["schedules"][service] = [{k: row.get(k) for k in ("Name", "State")} for row in rows]
        except Exception as exc:
            result["schedules"][service] = {"error": error_code(exc)}
    try:
        prefix = s3.list_objects_v2(Bucket=PRIVATE, Prefix="factory/", MaxKeys=50)
        result["private_factory"] = {"accessible": True, "key_count_sample": len(prefix.get("Contents", []))}
        private["existing_factory_keys"] = [r["Key"] for r in prefix.get("Contents", [])]
        key = "factory/inventory/" + result["generated_at"].replace(":", "-") + ".json"
        s3.put_object(Bucket=PRIVATE, Key=key, Body=json.dumps({**private, "summary": result}, default=str).encode(),
                      ContentType="application/json", IfNoneMatch="*", ServerSideEncryption="AES256")
        result["private_inventory_key"] = key
        result["mutations"].append("add-only inventory in existing private bucket")
    except Exception as exc:
        result["private_factory"] = {"accessible": False, "error": error_code(exc)}
    return result


if __name__ == "__main__":
    try:
        with report("5506_factory_inventory") as rep:
            inventory = inspect()
            target = Path("aws/ops/reports/5506.json")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps(inventory, indent=2, sort_keys=True) + "\n")
            rep.kv(functions=inventory["functions"], models=inventory["model_inventory"], schedules=inventory["schedules"])
            rep.log("Inventory recorded; no jobs invoked, no IAM changes, no endpoints created.")
    except Exception as exc:
        print("Factory inventory failed:", error_code(exc))
        sys.exit(1)
