"""Standalone S3 Lambda wrapper for the authoritative Khalid risk policy."""
from __future__ import annotations
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import boto3
from risk_engine import age_hours, build_output, validate_output

BUCKET=os.environ.get("S3_BUCKET","justhodl-dashboard-live")
OUT_KEY="data/khalid-risk.json"
FUSION_KEY="data/engine-fusion.json"
S3=boto3.client("s3")
REGISTRY=json.loads(Path(__file__).with_name("input_registry.json").read_text())


def _iso(value):
    return value.astimezone(timezone.utc).isoformat() if hasattr(value,"astimezone") else value


def _read(key):
    try:
        obj=S3.get_object(Bucket=BUCKET,Key=key)
        payload=json.loads(obj["Body"].read())
        return (payload if isinstance(payload,dict) else {}),{"key":key,"last_modified":_iso(obj.get("LastModified")),"bytes":obj.get("ContentLength"),"error":None}
    except Exception as exc:
        return {},{"key":key,"last_modified":None,"bytes":None,"error":str(exc)[:240]}


def _write(payload):
    S3.put_object(Bucket=BUCKET,Key=OUT_KEY,Body=json.dumps(payload,separators=(",",":"),allow_nan=False).encode(),ContentType="application/json",CacheControl="public,max-age=60")


def lambda_handler(event=None,context=None):
    validation_only=isinstance(event,dict) and event.get("mode")=="validate_only"
    feeds={}; metas={}
    for spec in REGISTRY["inputs"]:
        feeds[spec["id"]],metas[spec["id"]]=_read(spec["artifact"])
    fusion,fusion_meta=_read(FUSION_KEY)
    fusion_age=age_hours(
        fusion.get("generated_at") if isinstance(fusion,dict) else None,
        datetime.now(timezone.utc),
    )
    if fusion_meta.get("error") or fusion_age is None or fusion_age>2:
        fusion={}
    output=build_output(REGISTRY,feeds,metas,datetime.now(timezone.utc),fusion)
    validate_output(output)
    if validation_only:
        return {"statusCode":200,"body":json.dumps({"ok":True,"validation_only":True,"schema_version":output["schema_version"],"status":output["status"],"artifact_size_bytes":len(json.dumps(output,separators=(",",":"),allow_nan=False).encode())})}
    _write(output)
    return {"statusCode":200,"body":json.dumps({"ok":True,"output":OUT_KEY,"status":output["status"],"mode":output["policy"]["mode"],"capital_decision":output["capital_decision"],"exposure_cap_pct":output["exposure_cap_pct"]})}
