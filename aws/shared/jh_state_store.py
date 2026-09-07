"""aws/shared/jh_state_store.py -- signal registry / current state (phase 5),
S3 archive (phase 5), bus publishing with loop protection (phases 4 + 54) and
CloudWatch metrics (phase 47) for the JustHodl Intelligence Network.

Storage layout
--------------
DynamoDB table ``justhodl-jhsignal-state`` (PAY_PER_REQUEST, created on demand):
    pk  entity_id            e.g. equity:NVDA
    sk  signal_key           engine_id#signal_type#horizon
    attrs: engine_id, signal_type, horizon, family, category, status
           (ACTIVE|STALE|EXPIRED), direction, score, confidence, data_asof,
           published_at, expires_at (ISO), ttl_epoch (DDB TTL, expiry + 7d),
           idem (idempotency key), doc (JSON of the full signal)
    GSI gsi_engine: engine_id (HASH) + published_at (RANGE)
    GSI gsi_status: status (HASH) + expires_at (RANGE)   -- stale / expired sweeps

S3 (the data plane -- everything a page reads lives under data/):
    data/jhsignal/state/latest.json          compact read model of ACTIVE+STALE signals
    data/jhsignal/state/registry.json        the engine registry as deployed
    data/jhsignal/archive/YYYY/MM/DD/<run_id>.jsonl.gz   full JHSIGNAL stream, append-only
    data/jhsignal/bridge-run.json            last bridge run report (observability)

The S3 snapshot is written on every bridge run, so consumers (fusion, pages)
have a single cheap read even when DynamoDB is disabled by flag.
"""
from __future__ import annotations

import gzip
import json
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple

import jhsignal as J

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
TABLE_NAME = os.environ.get("JHSIGNAL_TABLE", "justhodl-jhsignal-state")
REGION = os.environ.get("AWS_REGION", "us-east-1")
STATE_KEY = "data/jhsignal/state/latest.json"
REGISTRY_KEY = "data/jhsignal/state/registry.json"
ARCHIVE_PREFIX = "data/jhsignal/archive/"
RUN_KEY = "data/jhsignal/bridge-run.json"
METRIC_NAMESPACE = "JustHodl/Fusion"
DDB_GRACE_DAYS = 7


def _log(**fields) -> None:
    """Structured log line (phase 47): one JSON object per line."""
    fields.setdefault("ts", J.iso(J.utcnow()))
    print(json.dumps(fields, default=str, separators=(",", ":")))


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------
class S3Store:
    def __init__(self, client=None, bucket: str = BUCKET):
        self.bucket = bucket
        self._s3 = client

    @property
    def s3(self):
        if self._s3 is None:
            import boto3
            self._s3 = boto3.client("s3", region_name=REGION)
        return self._s3

    def get_json(self, key: str) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
        """(doc, meta) -- meta carries last_modified / error; a missing key is (None, {error})."""
        try:
            r = self.s3.get_object(Bucket=self.bucket, Key=key)
            body = r["Body"].read()
            if key.endswith(".gz"):
                body = gzip.decompress(body)
            lm = r.get("LastModified")
            return json.loads(body), {"last_modified": J.iso(lm) if lm else None, "bytes": len(body), "key": key}
        except Exception as exc:
            return None, {"last_modified": None, "error": "%s: %s" % (type(exc).__name__, str(exc)[:160]), "key": key}

    def put_json(self, key: str, doc: Any, *, gz: bool = False) -> int:
        body = json.dumps(doc, default=_json_default, separators=(",", ":")).encode()
        kw = {"Bucket": self.bucket, "Key": key, "ContentType": "application/json"}
        if gz:
            body = gzip.compress(body)
            kw["ContentEncoding"] = "gzip"
        self.s3.put_object(Body=body, **kw)
        return len(body)

    def append_archive(self, run_id: str, signals: Iterable[Dict[str, Any]], *, now: Optional[datetime] = None) -> Optional[str]:
        """Append-only: one gzip NDJSON object per bridge run (never rewritten -> no version churn)."""
        now = now or J.utcnow()
        lines = [J.dumps(s) for s in signals]
        if not lines:
            return None
        key = "%s%s/%s.jsonl.gz" % (ARCHIVE_PREFIX, now.strftime("%Y/%m/%d"), run_id)
        body = gzip.compress(("\n".join(lines) + "\n").encode())
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/x-ndjson", ContentEncoding="gzip")
        return key


def _json_default(o):
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, datetime):
        return J.iso(o)
    return str(o)


# ---------------------------------------------------------------------------
# DynamoDB current-state
# ---------------------------------------------------------------------------
def _to_ddb(v):
    if isinstance(v, float):
        return Decimal(str(round(v, 6)))
    if isinstance(v, dict):
        return {k: _to_ddb(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_to_ddb(x) for x in v]
    return v


class DynamoState:
    def __init__(self, resource=None, client=None, table_name: str = TABLE_NAME):
        self.table_name = table_name
        self._res, self._cli = resource, client
        self._table = None

    @property
    def client(self):
        if self._cli is None:
            import boto3
            self._cli = boto3.client("dynamodb", region_name=REGION)
        return self._cli

    @property
    def table(self):
        if self._table is None:
            if self._res is None:
                import boto3
                self._res = boto3.resource("dynamodb", region_name=REGION)
            self._table = self._res.Table(self.table_name)
        return self._table

    def ensure_table(self, *, wait: bool = True) -> str:
        """Create the table if missing. Returns ACTIVE | CREATED | EXISTS."""
        try:
            d = self.client.describe_table(TableName=self.table_name)["Table"]
            if d.get("TableStatus") == "ACTIVE":
                return "ACTIVE"
            status = "EXISTS"
        except self.client.exceptions.ResourceNotFoundException:
            self.client.create_table(
                TableName=self.table_name,
                BillingMode="PAY_PER_REQUEST",
                AttributeDefinitions=[
                    {"AttributeName": "entity_id", "AttributeType": "S"},
                    {"AttributeName": "signal_key", "AttributeType": "S"},
                    {"AttributeName": "engine_id", "AttributeType": "S"},
                    {"AttributeName": "published_at", "AttributeType": "S"},
                    {"AttributeName": "status", "AttributeType": "S"},
                    {"AttributeName": "expires_at", "AttributeType": "S"},
                ],
                KeySchema=[{"AttributeName": "entity_id", "KeyType": "HASH"}, {"AttributeName": "signal_key", "KeyType": "RANGE"}],
                GlobalSecondaryIndexes=[
                    {"IndexName": "gsi_engine", "KeySchema": [{"AttributeName": "engine_id", "KeyType": "HASH"}, {"AttributeName": "published_at", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "ALL"}},
                    {"IndexName": "gsi_status", "KeySchema": [{"AttributeName": "status", "KeyType": "HASH"}, {"AttributeName": "expires_at", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "KEYS_ONLY"}},
                ],
                Tags=[{"Key": "system", "Value": "justhodl-fusion"}, {"Key": "release", "Value": "fusion-r1"}],
            )
            status = "CREATED"
        if wait:
            self.client.get_waiter("table_exists").wait(TableName=self.table_name, WaiterConfig={"Delay": 5, "MaxAttempts": 40})
            try:
                self.client.update_time_to_live(TableName=self.table_name, TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl_epoch"})
            except Exception as exc:  # already enabled or race -- not fatal
                _log(level="warn", component="ddb", msg="ttl", err=str(exc)[:120])
        return status

    @staticmethod
    def item_from_signal(signal: Dict[str, Any], *, family: str, status: str) -> Dict[str, Any]:
        exp = J.expires_at(signal)
        return _to_ddb({
            "entity_id": signal["entity_id"], "signal_key": J.signal_key(signal),
            "engine_id": signal["engine_id"], "signal_type": signal["signal_type"], "horizon": signal["horizon"],
            "family": family, "category": signal["category"], "status": status,
            "direction": signal["direction"], "score": float(signal["score"]), "confidence": float(signal["confidence"]),
            "data_asof": signal["data_asof"], "published_at": signal["published_at"], "expires_at": J.iso(exp),
            "ttl_epoch": int((exp + timedelta(days=DDB_GRACE_DAYS)).timestamp()),
            "idem": J.idempotency_key(signal), "signal_id": signal["signal_id"], "doc": J.dumps(signal),
        })

    def put_batch(self, items: Iterable[Dict[str, Any]]) -> int:
        n = 0
        with self.table.batch_writer(overwrite_by_pkeys=["entity_id", "signal_key"]) as bw:
            for it in items:
                bw.put_item(Item=it); n += 1
        return n

    def query_entity(self, entity_id: str, *, statuses: Tuple[str, ...] = ("ACTIVE", "STALE")) -> List[Dict[str, Any]]:
        from boto3.dynamodb.conditions import Key
        out, kw = [], {"KeyConditionExpression": Key("entity_id").eq(entity_id)}
        while True:
            r = self.table.query(**kw)
            for it in r.get("Items") or []:
                if it.get("status") in statuses:
                    out.append(json.loads(it["doc"]))
            if not r.get("LastEvaluatedKey"):
                return out
            kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]

    def query_engine(self, engine_id: str, *, limit: int = 500) -> List[Dict[str, Any]]:
        from boto3.dynamodb.conditions import Key
        r = self.table.query(IndexName="gsi_engine", KeyConditionExpression=Key("engine_id").eq(engine_id), ScanIndexForward=False, Limit=limit)
        return [json.loads(it["doc"]) for it in r.get("Items") or []]

    def sweep_expired(self, *, now: Optional[datetime] = None, limit: int = 1000) -> int:
        """Flip ACTIVE/STALE items whose expires_at passed to EXPIRED (DDB TTL purges them a week later)."""
        from boto3.dynamodb.conditions import Key
        now = now or J.utcnow(); n = 0
        for st in ("ACTIVE", "STALE"):
            r = self.table.query(IndexName="gsi_status", KeyConditionExpression=Key("status").eq(st) & Key("expires_at").lt(J.iso(now)), Limit=limit)
            for it in r.get("Items") or []:
                self.table.update_item(Key={"entity_id": it["entity_id"], "signal_key": it["signal_key"]},
                                       UpdateExpression="SET #s = :e", ExpressionAttributeNames={"#s": "status"}, ExpressionAttributeValues={":e": "EXPIRED"})
                n += 1
        return n


# ---------------------------------------------------------------------------
# Snapshot / change detection
# ---------------------------------------------------------------------------
def build_snapshot(signals: List[Dict[str, Any]], *, registry_doc: Dict[str, Any], run_id: str, now: datetime,
                   adapter_reports: List[Dict[str, Any]], flags: Dict[str, Any]) -> Dict[str, Any]:
    fam = {e["engine_id"]: e["engine_family"] for e in registry_doc["engines"]}
    by_entity: Dict[str, List[Dict[str, Any]]] = {}
    n_state = {"FRESH": 0, "STALE": 0, "EXPIRED": 0, "INVALID": 0}
    # one live signal per entity x engine#type#horizon: the NEWEST data_asof wins (an older reading
    # arriving out of order can never overwrite a newer one; ties keep the more confident reading)
    latest: Dict[Tuple[str, str], Dict[str, Any]] = {}
    n_dup = 0
    for s in signals:
        k = (s["entity_id"], J.signal_key(s))
        cur = latest.get(k)
        if cur is None:
            latest[k] = s
            continue
        n_dup += 1
        if (s["data_asof"], float(s["confidence"])) > (cur["data_asof"], float(cur["confidence"])):
            latest[k] = s
    for s in latest.values():
        st = J.freshness_state(s, now)
        n_state[st] = n_state.get(st, 0) + 1
        if st == "EXPIRED":
            continue
        c = J.compact(s); c["family"] = fam.get(s["engine_id"], "UNKNOWN"); c["freshness"] = st
        c["age_days"] = round(J.age_days(s, now), 3)
        c["freshness_weight"] = round(J.freshness_weight(c["age_days"], s["half_life_days"]), 4)
        by_entity.setdefault(s["entity_id"], []).append(c)
    for lst in by_entity.values():
        lst.sort(key=lambda c: (c["family"], c["engine_id"], c["signal_type"], c["horizon"]))
    return {
        "schema_version": "JHSIGNAL-STATE-1.0", "run_id": run_id, "generated_at": J.iso(now),
        "n_entities": len(by_entity), "n_signals": sum(len(v) for v in by_entity.values()), "freshness_counts": n_state,
        "n_duplicates_dropped": n_dup,
        "registry_version": registry_doc.get("schema_version"), "n_engines_registered": len(registry_doc["engines"]),
        "flags": {k: v for k, v in flags.items() if k.startswith("FUSION_")},
        "adapters": adapter_reports, "entities": by_entity,
    }


def diff_snapshots(prev: Optional[Dict[str, Any]], cur: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Material changes between two snapshots, keyed for bus events: new / revised (direction or score
    moved >= 0.15) / expired. The comparison key is entity + engine#type#horizon."""
    def index(snap):
        out = {}
        for eid, lst in ((snap or {}).get("entities") or {}).items():
            for c in lst:
                out[(eid, "%s#%s#%s" % (c["engine_id"], c["signal_type"], c["horizon"]))] = c
        return out
    p, c = index(prev), index(cur)
    new, revised, expired = [], [], []
    for k, v in c.items():
        if k not in p:
            new.append(v)
        else:
            o = p[k]
            if o["direction"] != v["direction"] or abs(float(o["score"]) - float(v["score"])) >= 0.15 or o["data_asof"] != v["data_asof"] and abs(float(o["score"]) - float(v["score"])) >= 0.05:
                revised.append({"prev": {"direction": o["direction"], "score": o["score"], "data_asof": o["data_asof"]}, "cur": v})
    for k, v in p.items():
        if k not in c:
            expired.append(v)
    return {"new": new, "revised": revised, "expired": expired}


# ---------------------------------------------------------------------------
# Bus publishing (through the existing system_events helper + custom bus)
# ---------------------------------------------------------------------------
class Bus:
    def __init__(self, *, enabled: bool = True, origin_engine: str = "jhsignal-bridge", max_depth: int = 3, publisher=None):
        self.enabled = enabled
        self.origin = origin_engine
        self.max_depth = max_depth
        self._publish = publisher
        self.sent = 0
        self.failed = 0
        self.suppressed = 0

    def publish(self, event_name: str, detail: Dict[str, Any], *, parent: Optional[Dict[str, Any]] = None) -> bool:
        if not self.enabled:
            self.suppressed += 1
            return False
        try:
            env = J.envelope(detail, origin_engine=self.origin, parent=parent, max_depth=self.max_depth)
        except J.JHSignalError as exc:  # depth cap -> refuse loudly, never loop
            self.suppressed += 1
            _log(level="warn", component="bus", msg="propagation depth cap", event=event_name, err=exc.problems)
            return False
        if self._publish is None:
            from system_events import publish as _pub
            self._publish = _pub
        ok = bool(self._publish(event_name, env, source_engine=self.origin))
        if ok:
            self.sent += 1
        else:
            self.failed += 1
        return ok


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
class Metrics:
    def __init__(self, client=None, namespace: str = METRIC_NAMESPACE, enabled: bool = True):
        self._cw = client
        self.ns = namespace
        self.enabled = enabled
        self.buffer: List[Dict[str, Any]] = []

    def put(self, name: str, value: float, unit: str = "Count", **dims) -> None:
        d = [{"Name": k, "Value": str(v)} for k, v in dims.items()]
        self.buffer.append({"MetricName": name, "Value": float(value), "Unit": unit, "Dimensions": d, "Timestamp": J.utcnow()})

    def flush(self) -> int:
        if not self.enabled or not self.buffer:
            self.buffer = []
            return 0
        try:
            if self._cw is None:
                import boto3
                self._cw = boto3.client("cloudwatch", region_name=REGION)
            n = 0
            for i in range(0, len(self.buffer), 20):
                self._cw.put_metric_data(Namespace=self.ns, MetricData=self.buffer[i:i + 20]); n += len(self.buffer[i:i + 20])
            self.buffer = []
            return n
        except Exception as exc:
            _log(level="warn", component="metrics", msg="put_metric_data failed", err=str(exc)[:160])
            self.buffer = []
            return 0
