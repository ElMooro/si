#!/usr/bin/env python3
"""Publish a public factory projection next to data/ai.json so /ai.html can paint agents without sign-in.

Does not expose protected exams, IAM, or private Brain notes.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

PUB = "justhodl-dashboard-live"
PRIV = "justhodl-ai-857687956942"
s3 = boto3.client("s3", region_name="us-east-1")


def get(bucket, key):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    except ClientError:
        return None


def put_public(key, value):
    body = json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    args = dict(
        Bucket=PUB,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
        CacheControl="max-age=60, must-revalidate",
        ServerSideEncryption="AES256",
    )
    try:
        s3.put_object(**args, ACL="public-read")
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") not in {"AccessControlListNotSupported", "InvalidArgument"}:
            raise
        s3.put_object(**args)


def main(rep=None):
    doc = None
    src = None
    for bucket, key in (
        (PUB, "data/student-state.json"),
        (PUB, "student-state.json"),
        (PRIV, "data/student-state.json"),
        (PRIV, "student-state.json"),
    ):
        doc = get(bucket, key)
        if doc:
            src = f"{bucket}/{key}"
            break
    if not isinstance(doc, dict) or not isinstance(doc.get("agents"), list):
        raise RuntimeError("student-state missing")
    season = doc.get("season") or {}
    public = {
        "schema_version": "student-state.v1",
        "projection": "public-factory.v1",
        "source": src,
        "generated_at": doc.get("generated_at") or datetime.now(timezone.utc).isoformat(),
        "state_version": int(doc.get("state_version") or 0),
        "gen": int(doc.get("gen") or 0),
        "objective": doc.get("objective"),
        "agents": doc.get("agents") or [],
        "skillbook": doc.get("skillbook") or [],
        "wall": doc.get("wall") or {},
        "season": {
            "id": season.get("id"),
            "weeks": season.get("weeks"),
            "starts_on": season.get("starts_on"),
            "crisis_definition": season.get("crisis_definition"),
            "price_sources": season.get("price_sources") or {},
        },
        "fit": doc.get("fit"),
        "model": {"status": (doc.get("model") or {}).get("status"), "training_runs": 0},
        "health": {
            "status": (doc.get("health") or {}).get("status") or "live",
            "last_tick_at": (doc.get("health") or {}).get("last_tick_at"),
            "errors": (doc.get("health") or {}).get("errors") or [],
        },
        "outer_status": doc.get("outer_status") or {},
        "budget": doc.get("budget"),
        "checksum": doc.get("checksum") or "public-projection",
    }
    board = get(PUB, "factory/salon/board.json") or get(PRIV, "factory/salon/board.json") or {
        "entries": [],
        "top50": [],
        "invited": [],
        "grade_status": "awaiting_official_prints",
        "generated_at": public["generated_at"],
    }
    for key in ("data/ai-factory.json", "data/factory-public.json"):
        put_public(key, public)
    put_public("data/factory-board.json", board)
    if rep is not None:
        rep["source"] = src
        rep["agents"] = [a.get("id") for a in public["agents"]]
        rep["keys"] = ["data/ai-factory.json", "data/factory-public.json", "data/factory-board.json"]
    print("published", src, "agents", len(public["agents"]))


if __name__ == "__main__":
    main()
