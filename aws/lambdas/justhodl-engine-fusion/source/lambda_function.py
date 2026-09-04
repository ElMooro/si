"""S3 wrapper for the governed engine-fusion compact read model."""
from __future__ import annotations
import json
import os
from datetime import datetime,timezone
from pathlib import Path
import boto3
from fusion_engine import build_output,validate_output

BUCKET=os.environ.get("S3_BUCKET","justhodl-dashboard-live")
OUT_KEY="data/engine-fusion.json"
S3=boto3.client("s3")
HERE=Path(__file__).parent
REGISTRY=json.loads((HERE/"fusion-registry.v1.json").read_text()); SUBSCRIPTIONS=json.loads((HERE/"fusion-subscriptions.v1.json").read_text()); POLICY=json.loads((HERE/"fusion-policy.v1.json").read_text()); SCHEMA=json.loads((HERE/"fusion-schema.v1.json").read_text())

def _read(key):
    try:
        obj=S3.get_object(Bucket=BUCKET,Key=key); payload=json.loads(obj["Body"].read()); lm=obj.get("LastModified"); return (payload if isinstance(payload,dict) else {}),{"last_modified":lm.astimezone(timezone.utc).isoformat() if hasattr(lm,"astimezone") else lm,"error":None}
    except Exception as exc: return {},{"last_modified":None,"error":str(exc)[:240]}

def lambda_handler(event=None,context=None):
    validation_only=isinstance(event,dict) and event.get("mode")=="validate_only"; feeds={}; metas={}
    # Explicit registry only. Never enumerate or read engine-manifest outputs.
    for spec in REGISTRY["sources"]: feeds[spec["id"]],metas[spec["id"]]=_read(spec["artifact"])
    output=build_output(REGISTRY,SUBSCRIPTIONS,POLICY,SCHEMA,feeds,metas,datetime.now(timezone.utc)); validate_output(output,SCHEMA)
    if validation_only: return {"statusCode":200,"body":json.dumps({"ok":True,"validation_only":True,"schema_version":output["schema_version"],"status":output["status"],"active_packets":len(output["packets"]),"artifact_size_bytes":len(json.dumps(output,separators=(",",":"),allow_nan=False).encode())})}
    S3.put_object(Bucket=BUCKET,Key=OUT_KEY,Body=json.dumps(output,separators=(",",":"),allow_nan=False).encode(),ContentType="application/json",CacheControl="public,max-age=60")
    return {"statusCode":200,"body":json.dumps({"ok":True,"output":OUT_KEY,"status":output["status"],"active_packets":len(output["packets"]),"disagreements":len(output["disagreements"]),"vetoes":len(output["vetoes"])})}
