"""Conditional S3 state transactions and immutable evidence for Gear A."""
from __future__ import annotations

import copy
import json
import uuid
from datetime import timedelta

from factory_core import Invalid, canonical, digest, identifier, iso, seal_state, select_state, timestamp, verify_state


class Conflict(RuntimeError):
    pass


class Busy(RuntimeError):
    pass


def code(exc):
    return getattr(exc, "response", {}).get("Error", {}).get("Code", type(exc).__name__)


class Store:
    CURRENT = "factory/runtime/current.json"
    LEASE = "factory/runtime/lease.json"
    MIRRORS = ("student-state.json", "data/student-state.json")

    def __init__(self, s3, private_bucket, public_bucket, clock):
        self.s3, self.private, self.public, self.clock = s3, private_bucket, public_bucket, clock
        self.lease = None

    def missing(self, exc, bucket, key):
        if code(exc) in ('NoSuchKey', 'NotFound', '404'):
            return True
        if code(exc) in ('AccessDenied', '403'):
            # Prefix-scoped ListBucket permissions can make missing GETs return
            # 403. A permitted exact-prefix listing distinguishes absence from
            # a forbidden existing object; denied listings still fail closed.
            page = self.s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
            return not any(row['Key'] == key for row in page.get('Contents', []))
        return False

    def read(self, bucket, key, *, limit=1024 * 1024):
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key)
        except Exception as exc:
            if self.missing(exc, bucket, key):
                return None, None
            raise
        raw = response["Body"].read(limit + 1)
        if len(raw) > limit:
            raise Invalid("object_too_large")
        try:
            return json.loads(raw, parse_constant=lambda value: (_ for _ in ()).throw(Invalid("non_finite_json"))), response.get("ETag")
        except (ValueError, UnicodeError) as exc:
            exc.object_etag = response.get("ETag")
            raise

    def recoverable(self, bucket, key):
        try:
            return self.read(bucket, key)
        except (ValueError, UnicodeError) as exc:
            if not getattr(exc, "object_etag", None):
                raise
            return {"invalid_object": True}, exc.object_etag

    def put(self, bucket, key, value, *, etag=None, absent=False, public=False):
        arguments = {"Bucket": bucket, "Key": key, "Body": canonical(value), "ContentType": "application/json; charset=utf-8",
                     "CacheControl": "max-age=60, must-revalidate" if public else "private, no-store",
                     "ServerSideEncryption": "AES256"}
        if absent:
            arguments["IfNoneMatch"] = "*"
        elif etag:
            arguments["IfMatch"] = etag
        else:
            raise Invalid("conditional_write_required")
        try:
            return self.s3.put_object(**arguments)
        except Exception as exc:
            if code(exc) in ("PreconditionFailed", "ConditionalRequestConflict", "412", "409"):
                raise Conflict(key) from exc
            raise

    def immutable(self, bucket, key, value, *, public=False):
        try:
            self.put(bucket, key, value, absent=True, public=public)
            return True
        except Conflict:
            existing, _ = self.read(bucket, key)
            if canonical(existing) != canonical(value):
                raise Conflict("immutable_content_conflict")
            return False

    def acquire(self, seconds=150):
        now = self.clock()
        old, etag = self.read(self.private, self.LEASE)
        if old and timestamp(old["expires_at"]) > now:
            raise Busy("factory_tick_in_progress")
        lease = {"owner": uuid.uuid4().hex, "fence": int((old or {}).get("fence", 0)) + 1,
                 "acquired_at": iso(now), "expires_at": iso(now + timedelta(seconds=seconds))}
        try:
            self.put(self.private, self.LEASE, lease, etag=etag, absent=old is None)
        except Conflict as exc:
            raise Busy("factory_tick_in_progress") from exc
        self.lease = lease
        return lease

    def assert_owned(self):
        if not self.lease:
            raise Busy("lease_required")
        lease, etag = self.read(self.private, self.LEASE)
        if not lease or lease["owner"] != self.lease["owner"] or timestamp(lease["expires_at"]) <= self.clock():
            raise Busy("lease_lost")
        return lease, etag

    def release(self):
        if self.lease:
            try:
                lease, etag = self.assert_owned()
                lease["expires_at"] = iso(self.clock())
                self.put(self.private, self.LEASE, lease, etag=etag)
            except (Conflict, Busy):
                pass
            finally:
                self.lease = None

    def load_state(self):
        raw, etag = self.recoverable(self.private, self.CURRENT)
        if raw is not None:
            try:
                return verify_state(raw), etag
            except (Invalid, ValueError, TypeError):
                pass
        candidates = []
        observed_invalid = raw is not None
        for key in self.MIRRORS:
            try:
                item, _ = self.read(self.public, key)
                if item is not None:
                    observed_invalid = True
                    candidates.append(item)
            except (Invalid, ValueError, TypeError):
                observed_invalid = True
        response = self.s3.list_objects_v2(Bucket=self.private, Prefix="factory/runtime/snapshots/", MaxKeys=8)
        for item in response.get("Contents", []):
            try:
                snapshot, _ = self.read(self.private, item["Key"])
                candidates.append(snapshot)
            except (Invalid, ValueError, TypeError):
                observed_invalid = True
        if candidates:
            return select_state(candidates), etag
        if observed_invalid:
            raise Invalid("existing_state_unrecoverable; refusing initialization")
        return None, None

    def commit_state(self, state, base_etag):
        self.assert_owned()
        current, current_etag = self.recoverable(self.private, self.CURRENT)
        if current_etag != base_etag:
            raise Conflict("state_changed")
        if current and type(current.get("state_version")) is int and state["state_version"] <= current["state_version"]:
            raise Conflict("non_monotonic_state")
        sealed = seal_state(state)
        snapshot_key = "factory/runtime/snapshots/%016d-%s.json" % (9999999999999999 - sealed["state_version"], sealed["checksum"][:16])
        self.immutable(self.private, snapshot_key, sealed)
        self.assert_owned()
        self.put(self.private, self.CURRENT, sealed, etag=base_etag, absent=base_etag is None)
        self.publish_mirrors(sealed)
        return sealed

    def publish_mirrors(self, state):
        verify_state(state)
        errors = []
        for key in self.MIRRORS:
            try:
                self.assert_owned()
                old, etag = self.recoverable(self.public, key)
                if old and isinstance(old, dict) and type(old.get("state_version")) is int:
                    if old["state_version"] > state["state_version"]:
                        raise Conflict("newer_public_state")
                    if old["state_version"] == state["state_version"] and old.get("checksum") == state["checksum"]:
                        continue
                self.put(self.public, key, state, etag=etag, absent=etag is None, public=True)
            except Exception as exc:
                errors.append({"key": key, "error": code(exc)})
        if errors:
            # The private transaction remains valid and is repaired on the next tick.
            raise Conflict("state_committed_mirror_repair_required:" + json.dumps(errors))

    def append_event(self, kind, event_id, data, *, public=False):
        identifier(kind)
        identifier(event_id)
        event = {"schema_version": "factory-event.v1", "id": event_id, "kind": kind, "data": data}
        event["content_hash"] = digest(event)
        bucket = self.public if public else self.private
        key = ("factory/salon/events/" if public else "factory/events/") + event_id + ".json"
        self.immutable(bucket, key, event, public=public)
        return event

    def read_events(self, *, public=False, limit=1000):
        bucket = self.public if public else self.private
        prefix = "factory/salon/events/" if public else "factory/events/"
        events, continuation = [], None
        while True:
            options = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": min(1000, limit - len(events))}
            if continuation:
                options["ContinuationToken"] = continuation
            page = self.s3.list_objects_v2(**options)
            for item in page.get("Contents", []):
                event, _ = self.read(bucket, item["Key"])
                events.append(event)
            continuation = page.get("NextContinuationToken")
            if not continuation:
                return sorted(events, key=lambda event: event["id"])
            if len(events) >= limit:
                raise Invalid("event_compaction_required; refusing_truncated_view")

    def jsonl_view(self, key, events):
        if key not in ("factory/salon/wall.jsonl", "factory/salon/events.jsonl"):
            raise Invalid("jsonl_key_not_allowed")
        self.assert_owned()
        if len({event['id'] for event in events}) != len(events):
            raise Invalid('duplicate_event_id')
        body = b"".join(canonical(event) + b"\n" for event in events)
        # This is a materialized view; the individual event objects are authoritative.
        try:
            old = self.s3.get_object(Bucket=self.public, Key=key)
            old_body = old["Body"].read(4 * 1024 * 1024 + 1)
            if len(old_body) > 4 * 1024 * 1024:
                raise Invalid("view_requires_compaction")
            prior = [json.loads(line) for line in old_body.splitlines() if line.strip()]
            next_by_id = {event["id"]: event for event in events}
            if any(event.get("id") in next_by_id and canonical(next_by_id[event["id"]]) != canonical(event) for event in prior):
                raise Conflict("history_rewrite_refused")
            prior_ids = {event['id'] for event in prior}
            new = [event for event in events if event['id'] not in prior_ids]
            if not new:
                return
            body = old_body + b''.join(canonical(event) + b'\n' for event in new)
            conditional = {"IfMatch": old["ETag"]}
        except Exception as exc:
            if not self.missing(exc, self.public, key):
                raise
            conditional = {"IfNoneMatch": "*"}
        self.s3.put_object(Bucket=self.public, Key=key, Body=body, ContentType="application/x-ndjson",
                           CacheControl="max-age=60, must-revalidate", ServerSideEncryption="AES256", **conditional)
