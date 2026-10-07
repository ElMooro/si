"""Prospective context sidecars. Never backfill old forecasts or change grades."""
import hashlib
from research_network import CONTRACT, PERMISSIONS, canonical, stamp
from research_network_store import KEY, PREFIX, read, validate_manifest


def registration_context(s3, bucket, registered_at):
    try:
        doc, raw, _ = read(s3, bucket, KEY, 16 * 1024 * 1024, not_after=registered_at)
        validate_manifest(doc)
        generated = stamp(doc.get("generated_at"))
        if generated is None or not 0 <= (registered_at - generated).total_seconds() <= 26 * 3600:
            raise ValueError("context clock ineligible")
        return {"contract": CONTRACT, "status": "available_at_registration",
                "publication_id": doc["publication_id"], "key": doc["snapshot_key"],
                "sha256": hashlib.sha256(raw).hexdigest(), "generated_at": doc["generated_at"],
                "basis": "S3 publication existed before forecast registration", **PERMISSIONS}
    except Exception as exc:
        return {"contract": CONTRACT, "status": "unavailable_at_registration",
                "reason": type(exc).__name__, **PERMISSIONS}


def retain_context(s3, bucket, record, context, prefix):
    sidecar = {"contract": "prospective-research-context.v1", "forecast_id": record["forecast_id"],
               "registered_at": record["registered_at"], "instrument": record["observation"]["instrument"],
               "research_context": context, "outcome_or_grade_effect": "none", **PERMISSIONS}
    key = prefix + "contexts/" + record["forecast_id"] + ".json"
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=canonical(sidecar), ContentType="application/json",
                      CacheControl="public,max-age=31536000,immutable", IfNoneMatch="*")
        return {"key": key, "sha256": hashlib.sha256(canonical(sidecar)).hexdigest(), "status": "retained"}
    except Exception as exc:
        # No best-effort retry with a later dossier: that would introduce hindsight.
        return {"status": "context_not_retained", "reason": type(exc).__name__}
