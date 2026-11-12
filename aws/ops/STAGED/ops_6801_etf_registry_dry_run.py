"""ops 6801 - READ-ONLY dry run of the issuer registry with the engine's own code (justhodl-etf-true-flows 3.1.0, this
checkout) against the live issuer sources: etf_store.collect() with an in-memory evidence store (nothing is written to
S3), then etf_research.build() on what it retained. Reports per registry fund the day's snapshot or history the real
parsers produced, the acquisition status codes, the packet's coverage and gap count, and replays the build once more to
show it is pure. No vendor key is used. Web keys are read from the issuers' own sites at run time and never logged."""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import hashlib
import io
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-etf-true-flows" / "source"))


class Error(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class Memory:
    """Just enough of the S3 client for evidence_store.capture and etf_store.raw_reader."""

    def __init__(self):
        self.data = {}
        self.meta = {}

    def get_object(self, **args):
        key = args["Key"]
        if key not in self.data:
            raise Error("NoSuchKey")
        raw = self.data[key]
        return {"Body": io.BytesIO(raw), "ETag": hashlib.sha256(raw).hexdigest(), "Metadata": self.meta.get(key, {})}

    def put_object(self, **args):
        key = args["Key"]
        if args.get("IfNoneMatch") == "*" and key in self.data:
            raise Error("PreconditionFailed")
        raw = args["Body"]
        self.data[key] = raw if isinstance(raw, bytes) else raw.encode()
        self.meta[key] = args.get("Metadata", {})


def main():
    from ops_report import report
    import etf_issuers as iss
    import etf_research as model
    import etf_store as store

    now = datetime.now(timezone.utc)
    with report("ops_6801_etf_registry_dry_run") as r:
        r.heading("ops 6801 - issuer registry dry run with the engine's own code %s (read-only)" % now.isoformat(timespec="minutes"))
        client = Memory()
        read = store.raw_reader(client, "memory")
        t0 = time.time()
        originals, errors = store.collect(client, "memory", None, read, time.monotonic() + 500)
        r.log("collect: %d originals, %d errors in %.0fs; retained %.1f MB" % (len(originals), len(errors), time.time() - t0, sum(len(v) for v in client.data.values()) / 1e6))
        registry = [t for t in iss.snapshot_funds() + iss.history_funds()]
        r.section("1. registry acquisitions")
        for t in registry:
            labels = [lbl for lbl, _, _, _ in iss.requests_for(t)]
            for lbl in labels:
                if lbl in originals:
                    ev = originals[lbl]["evidence"]
                    r.log("  %-5s %-22s retained %8d B sha %s" % (t, lbl, ev["bytes"], ev["sha256"][:12]))
                else:
                    r.log("  %-5s %-22s MISSING: %s" % (t, lbl, errors.get(lbl)))
        for k in sorted(errors):
            if not k.startswith("vendor_") and not k.startswith("ishares_") and not k.startswith("ssga_") and not k.startswith("proshares_"):
                r.log("  error %s: %s" % (k, errors[k]))
        r.log("  non-registry errors: %d (%s)" % (sum(1 for k in errors if k.startswith(("ishares_", "ssga_", "proshares_"))), [k for k in errors if k.startswith(("ishares_", "ssga_", "proshares_"))][:8]))
        r.section("2. what the parsers read")
        at = store.now()
        for t in iss.snapshot_funds():
            try:
                issuer, snap = iss.parse_snapshot(t, originals, read, at)
                r.log("  %-5s %-16s snapshot %s nav %s shares %s net assets %s%s" % (t, issuer, snap["date"], snap["nav_decimal"], snap["shares_decimal"], snap["net_assets_decimal"] or snap.get("net_assets_display"),
                                                                                    "" if snap.get("aum_within_display_precision") in (True, None) else " RESIDUAL OUTSIDE PRECISION"))
            except Exception as exc:  # noqa: BLE001
                r.warn("  %-5s snapshot failed: %s %s" % (t, type(exc).__name__, str(exc)[:160]))
        for t in iss.history_funds():
            try:
                issuer, doc = iss.parse_history(t, originals, read, at)
                rows = doc["rows"]
                r.log("  %-5s %-16s history %d rows %s -> %s; last nav %s shares %s net assets %s" % (t, issuer, len(rows), rows[0]["date"], rows[-1]["date"], rows[-1]["nav_decimal"], rows[-1]["shares_decimal"], rows[-1]["net_assets_decimal"]))
            except Exception as exc:  # noqa: BLE001
                r.warn("  %-5s history failed: %s %s" % (t, type(exc).__name__, str(exc)[:160]))
        r.section("3. build on the retained originals (no ledgers yet: every snapshot fund starts its ledger today)")
        inputs = {"contract": "etf-original-inputs.v1", "originals": originals, "acquisition_errors": errors, "legacy": {"dry_run": True}, "ledgers": {}}
        t1 = time.time()
        output, histories = model.build(inputs, read, at)
        r.log("build: %.0fs, %d history/ledger shards, version %s, quality %s" % (time.time() - t1, len(histories), output["version"], json.dumps(output["quality"])))
        r.log("coverage: %s" % json.dumps(output["coverage"]))
        r.log("gaps (%d): %s" % (len(output["gaps"]), " ".join(output["gaps"])))
        r.log("nav_source_counts: %s" % json.dumps(output["nav_source_counts"]))
        for t in iss.history_funds():
            e = output["by_etf"][t]
            r.log("  %-5s %s net_flow_5d %s net_flow_20d %s obs %s status %s" % (t, e["source_status"], e["net_flow_5d_usd"], e["net_flow_20d_usd"], e.get("history_observations"), e["quality"]["status"]))
        for t in iss.snapshot_funds():
            e = output["by_etf"][t]
            r.log("  %-5s %s ledger %s" % (t, e["source_status"], json.dumps({k: v for k, v in e.get("ledger", {}).items() if k in ("rows", "latest_date", "status", "issuer")})))
        reviewed = output["reviewed_limits"]
        r.log("reviewed limits (%d): %s" % (len(reviewed), json.dumps(reviewed)))
        output2, histories2 = model.build(inputs, read, at)
        r.log("replay equal: output %s, shards %s" % (output2 == output, histories2 == histories))
        unresolved = [t for t in registry if output["by_etf"][t]["source_status"] not in ("retained_native_snapshot_ledger", "retained_native_history")]
        if unresolved:
            r.warn("registry funds without a retained observation today: %s" % unresolved)
        r.ok("dry run complete; nothing written")
    sys.exit(0)


if __name__ == "__main__":
    main()
