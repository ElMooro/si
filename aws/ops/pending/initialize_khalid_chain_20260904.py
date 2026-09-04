"""Initialize and verify the Khalid production dependency chain once."""

from __future__ import annotations

import json

import boto3


REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
CHAIN = (
    ("justhodl-settlement-fails", None),
    ("justhodl-signal-fabric", None),
    ("justhodl-engine-fusion", "live"),
    ("justhodl-khalid-risk", "live"),
    ("justhodl-khalid", "live"),
)


def strict_json(raw: bytes) -> dict:
    def reject_constant(value: str):
        raise ValueError(f"non-finite JSON constant: {value}")

    payload = json.loads(raw, parse_constant=reject_constant)
    if not isinstance(payload, dict):
        raise ValueError("expected a JSON object")
    return payload


def invoke_chain() -> None:
    client = boto3.client("lambda", region_name=REGION)
    for function_name, qualifier in CHAIN:
        args = {
            "FunctionName": function_name,
            "InvocationType": "RequestResponse",
            "Payload": b"{}",
        }
        if qualifier:
            args["Qualifier"] = qualifier
        response = client.invoke(**args)
        body = response["Payload"].read()
        if response.get("FunctionError"):
            raise RuntimeError(
                f"{function_name} failed: {body.decode('utf-8', errors='replace')}"
            )
        envelope = strict_json(body)
        if envelope.get("statusCode") != 200 and envelope.get("ok") is not True:
            raise RuntimeError(f"{function_name} returned {envelope}")
        print(f"invoked {function_name}{':' + qualifier if qualifier else ''}")


def read_artifact(key: str) -> dict:
    response = boto3.client("s3", region_name=REGION).get_object(
        Bucket=BUCKET, Key=key
    )
    payload = strict_json(response["Body"].read())
    print(
        f"verified s3://{BUCKET}/{key} "
        f"generated_at={payload.get('generated_at')} status={payload.get('status')}"
    )
    return payload


def verify_artifacts() -> None:
    settlement = read_artifact("data/settlement-fails.json")
    treasury = settlement.get("treasury")
    if not isinstance(treasury, dict):
        raise ValueError("settlement-fails is missing the normalized treasury block")
    if treasury.get("scope") != "US_TREASURY_INCLUDING_TIPS":
        raise ValueError("settlement-fails Treasury scope is not including TIPS")
    for field in ("ftd_bn", "ftr_bn", "gross_fail_activity_bn", "completeness"):
        if field not in treasury:
            raise ValueError(f"settlement-fails Treasury block is missing {field}")

    fusion = read_artifact("data/engine-fusion.json")
    if fusion.get("schema_version") != "1.0.0":
        raise ValueError("engine-fusion schema mismatch")
    if not isinstance(fusion.get("coverage"), dict):
        raise ValueError("engine-fusion coverage is missing")

    risk = read_artifact("data/khalid-risk.json")
    if risk.get("schema_version") != "1.0.0":
        raise ValueError("khalid-risk schema mismatch")
    if risk.get("capital_decision") not in {
        "INVEST SELECTIVELY",
        "STAY IN CASH / SHORT-TERM TREASURIES",
        "WAIT IN CASH / SHORT-TERM TREASURIES",
    }:
        raise ValueError("khalid-risk capital decision is invalid")
    if not isinstance(risk.get("treasury_fails"), dict):
        raise ValueError("khalid-risk Treasury fails lens is missing")

    khalid = read_artifact("data/khalid.json")
    if khalid.get("schema_version") != "3.0.0":
        raise ValueError("Khalid schema mismatch")
    authority = khalid.get("risk_artifact")
    if not isinstance(authority, dict) or authority.get("direct_risk_recomputed") is not False:
        raise ValueError("Khalid is not using the standalone risk authority")


if __name__ == "__main__":
    invoke_chain()
    verify_artifacts()
