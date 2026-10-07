"""Additive read model for existing consumers. Never modifies their decisions.

The embedded receipt binds the consumer publication to the exact immutable
research publication it read. It is not an EventBridge delivery claim. A failure
is explicit but cannot take down the existing decision or private risk engine.
"""
from datetime import datetime, timezone
import hashlib
from research_network import CONTRACT, PERMISSIONS, canonical, identity, stamp, review_group
from research_network_registry import SUBSCRIPTIONS
from research_network_store import KEY, read, read_entity, validate_manifest


def consume(s3, bucket, consumer, *, positions=None, now=None):
    now = now or datetime.now(timezone.utc)
    private = consumer == 'portfolio-risk' or positions is not None
    output = {"contract": CONTRACT, "consumer": consumer, "status": "unavailable",
              "access": "PRIVATE_OWNER" if private else "PUBLIC_RESEARCH",
              "decision_effect": "none", **PERMISSIONS}
    try:
        doc, raw, _ = read(s3, bucket, KEY, 16 * 1024 * 1024)
        validate_manifest(doc)
        generated = stamp(doc.get("generated_at"))
        age = (now - generated).total_seconds() / 3600 if generated else None
        if age is None or age < 0 or age > 26:
            output.update(reason="publication_stale_or_clock_invalid", publication_id=doc.get("publication_id"))
            return output
        selected = SUBSCRIPTIONS[consumer]
        excluded = {consumer}
        if consumer == 'prospective-evaluator':
            excluded.add('prospective-outcomes')
        reviews = {group: review_group(group, doc['sources'], excluded) for group in selected}
        output.update(status="available", publication_id=doc["publication_id"],
                      source_key=doc["snapshot_key"], source_sha256=hashlib.sha256(raw).hexdigest(),
                      generated_at=doc["generated_at"], read_at=now.isoformat(),
                      specialists=reviews,
                      source_contexts={sid: {k: source.get(k) for k in
                                            ('status','snapshot_key','sha256','summary','reported_clocks',
                                             'generated_at','publication_freshness','observation_freshness_verified',
                                             'reported_permissions','reported_dependency_roots')}
                                       for sid, source in doc['sources'].items()
                                       if sid not in excluded and any(g in selected for g in source['groups'])},
                      briefing={**doc["briefing"],
                                "available_angles": [g for g, r in reviews.items() if r['status'] != 'unavailable'],
                                "review_required": [g for g, r in reviews.items() if r['status'] != 'available']},
                      processed_publications={sid: source.get("sha256") for sid, source in doc["sources"].items()
                                              if sid not in excluded and any(g in selected for g in source["groups"])},
                      self_evidence_excluded=True,
                      exclusion_scope="direct producer only; shared upstream ancestry is not independent evidence")
        # There is no portfolio->public return route. Even symbols and misses
        # are stored only inside the existing private artifact publication.
        if private:
            if consumer != "portfolio-risk" or not isinstance(positions, list):
                raise ValueError("invalid private portfolio consumer")
            joined, cache = [], {}
            for index, row in enumerate(positions):
                if not isinstance(row, dict):
                    joined.append({"position_index": index, "status": "invalid_position"})
                    continue
                ident = identity(row.get("symbol", row.get("ticker")), row,
                                 {"identity_scope": "explicit", "id": "portfolio"})
                if ident is None or ident["scope"].startswith("unresolved"):
                    joined.append({"position_index": index, "status": "identity_unresolved"})
                    continue
                entity = read_entity(s3, bucket, doc, ident["id"], cache)
                joined.append({"position_index": index, "identity": ident,
                               "status": "research_context_only" if entity else "no_coverage",
                               "dossier": entity, **PERMISSIONS})
            output["positions"] = joined
            output["holdings_input_sha256"] = hashlib.sha256(canonical(positions)).hexdigest()
            output["portfolio_calculation_effect"] = "none; current risk, hedge and sizing contracts remain authoritative"
    except Exception as exc:
        # Avoid exposing source exception messages, account data or credentials.
        output = {"contract": CONTRACT, "consumer": consumer, "status": "unavailable",
                  "reason": type(exc).__name__, "decision_effect": "none",
                  "access": "PRIVATE_OWNER" if private else "PUBLIC_RESEARCH", **PERMISSIONS}
    return output


def attach(payload, s3, bucket, consumer, **kwargs):
    payload["research_network"] = consume(s3, bucket, consumer, **kwargs)
    return payload
