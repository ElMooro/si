"""ops 6686 - read-only: WHICH check makes justhodl-short-interest's per-ticker consumer layer skip ("[tickers-layer]
skipped: ValueError" on every run, including the 600 s run of ops 6684 at 196 s)?  The engine prints only the exception
type by design; this probe replays the layer's own checks on the runner against the live head data/short-interest.json
and its record shards, with the engine's modules (aws/shared/short_interest_*), stopping at the first failing condition
and naming it.  It also reads today's request journal (the producer returns an EARLIER run's result when the same
request_id was already completed - a result whose `replay` no longer matches the head is exactly the layer's first
check) and reports the head's generated_at / replay vs the journal's.  Nothing is written.  No key involved.
"""
from pathlib import Path
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"


def main():
    import short_interest_research_model as model
    import short_interest_research_store as store
    import short_interest_tickers as tickers
    import short_interest_producer as producer
    s3 = boto3.client("s3", region_name=REGION, config=Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=10, read_timeout=60))
    with report("ops_6686_tickers_layer_dry_run") as r:
        r.heading("ops 6686 - short-interest tickers layer: the first failing check, on the runner (read-only)")
        r.section("1. the head and today's request journal")
        head = store.bounded(s3.get_object(Bucket=LIVE, Key=model.CURRENT)["Body"])
        packet = model.strict(head)
        r.log("head %s: %d bytes, contract %s, generated_at %s, settlement_date %s, counts %s, replay %s" % (
            model.CURRENT, len(head), packet.get("contract"), packet.get("generated_at"), packet.get("settlement_date"), json.dumps(packet.get("counts"))[:200],
            json.dumps(packet.get("replay"), default=str)[:300]))
        r.log("permissions: %s" % {k: packet.get(k) for k in model.PERMISSIONS})
        today = datetime.now(timezone.utc).date().isoformat()
        for rid in ("scheduled-short-interest:%s" % today, "scheduled-short-interest:%s" % (datetime.now(timezone.utc).date().fromordinal(datetime.now(timezone.utc).date().toordinal() - 1).isoformat())):
            key = producer.request_key(rid)
            try:
                j = model.strict(producer.raw_read(s3, LIVE, key))
                res = j.get("result") or {}
                r.log("journal %s: status %s, started %s, result.published %s reason %s, result.replay.output_sha256 %s" % (
                    rid, j.get("status"), j.get("started_at"), res.get("published"), res.get("reason"), (res.get("replay") or {}).get("output_sha256")))
                digest = model.digest({k: v for k, v in packet.items() if k != "replay"})
                r.log("   head digest (minus replay) %s | head.replay == journal.replay: %s | digest == journal.replay.output_sha256: %s" % (
                    digest[:16], packet.get("replay") == res.get("replay"), digest == (res.get("replay") or {}).get("output_sha256")))
            except Exception as e:  # noqa: BLE001
                r.log("journal %s: %s" % (rid, type(e).__name__))
        r.section("2. the layer's checks, in order")
        try:
            refs = packet.get("record_shards")
            if not isinstance(refs, dict) or not refs:
                raise ValueError("Published head carries no record shards")
            if len(refs) > 256 or any(not isinstance(p, str) or not re.fullmatch("[a-f0-9]{2}", p) for p in refs):
                raise ValueError("Whole bounded shard index required")
            r.log("record_shards: %d prefixes" % len(refs))
            shards, source_bytes, issue_ids = {}, 0, set()
            t0 = time.time()
            for prefix, ref in sorted(refs.items()):
                key = ref.get("key") if isinstance(ref, dict) else None
                if (not isinstance(key, str) or not re.fullmatch(re.escape(model.PREFIX) + r"records/[a-f0-9]{64}\.json", key)
                        or type(ref.get("bytes")) is not int or not 0 < ref["bytes"] <= model.MAX
                        or not isinstance(ref.get("sha256"), str) or key != model.PREFIX + "records/" + ref["sha256"] + ".json"):
                    raise ValueError("Exact record-shard reference required: %s -> %s" % (prefix, json.dumps(ref)[:200]))
                raw = store.bounded(s3.get_object(Bucket=LIVE, Key=key)["Body"])
                source_bytes += len(raw)
                if source_bytes > 64 * 1024 * 1024:
                    raise ValueError("shard bytes exceed 64 MB at prefix %s (%d)" % (prefix, source_bytes))
                ident = store.identity(raw, "records")
                if ident != ref:
                    raise ValueError("shard identity differs at %s: ref %s vs raw %s" % (prefix, json.dumps(ref)[:160], json.dumps(ident)[:160]))
                shard = model.strict(raw)
                if not isinstance(shard, dict) or shard.get("contract") != "short-interest-record-shard.v1" or not isinstance(shard.get("records"), dict):
                    raise ValueError("Reported record shard required at %s (contract %s)" % (prefix, shard.get("contract") if isinstance(shard, dict) else type(shard).__name__))
                for identifier, record in shard["records"].items():
                    identity = record.get("identity") if isinstance(record, dict) else None
                    expected = model.record_id(tuple(identity.get(k) for k in tickers.GRAIN)) if isinstance(identity, dict) else None
                    if (not isinstance(identifier, str) or not re.fullmatch("[a-f0-9]{64}", identifier) or identifier in issue_ids
                            or not identifier.startswith(prefix) or not isinstance(identity, dict) or identifier != expected):
                        raise ValueError("Exact unique reported issue record required at %s: id %s expected %s identity %s" % (
                            prefix, identifier[:16] if isinstance(identifier, str) else identifier, (expected or "")[:16], json.dumps(identity, default=str)[:200]))
                    issue_ids.add(identifier)
                shards[prefix] = shard
            r.log("%d shards read, %.1f MB, %d issues in %.1fs" % (len(shards), source_bytes / 1e6, len(issue_ids), time.time() - t0))
            expected_issues = (packet.get("counts") or {}).get("issues")
            if type(expected_issues) is not int or len(issue_ids) != expected_issues:
                raise ValueError("Whole issue inventory differs from research head: %s issues vs counts.issues %s" % (len(issue_ids), expected_issues))
            r.log("issue inventory agrees with the head (%d)" % expected_issues)
            t1 = time.time()
            out = tickers.project_verified_tickers(shards, head, datetime.now(timezone.utc).isoformat())
            r.log("project_verified_tickers: %d tickers, settlement %s, measurement_contract %s in %.1fs; encoded %d bytes" % (
                len(out.get("by_ticker") or {}), out.get("settlement_date"), out.get("measurement_contract"), time.time() - t1, len(json.dumps(out, default=str))))
            r.log("sample: %s" % json.dumps({k: (out.get("by_ticker") or {}).get(k) for k in ("AAPL", "GME", "TSLA") if k in (out.get("by_ticker") or {})}, default=str)[:600])
            r.ok("every layer check passes on the runner with the live head - the skip is in the binding to the producer result or the publisher's budgets/retention, not in the data")
        except ValueError as e:
            r.warn("FAILING CHECK: %s" % str(e)[:600])
        except Exception as e:  # noqa: BLE001
            r.warn("probe error: %s %s" % (type(e).__name__, str(e)[:300]))
        r.ok("ops 6686 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
