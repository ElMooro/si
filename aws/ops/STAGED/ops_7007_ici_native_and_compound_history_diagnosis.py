"""ops 7007 - READ-ONLY diagnosis of the two stale outputs ops 7006 (daily health run 2026-11-28) found.

1. data/ici-flows.json (justhodl-ici-flows' native ici-research.v1 packet) is 158 h old. The ICI lane
   (.github/workflows/ici-flows-lane.yml) ran green on Nov 26 03:03, Nov 27 03:15 and Nov 27 23:43 and each time printed
   {"native": {"published": false, "error_type": "ValueError"}} - the desk feed (data/ici-fund-flows.json, latest week
   2026-11-18) was published, the native packet was not. The request journal keeps only the error TYPE. This run reads the
   newest journals under audit-private/.../ici-research/requests/, fetches the retained originals they reference and
   replays ici_research.parse_release + reconcile + compile_releases on the runner to print the exact message. Hypothesis:
   the Thanksgiving-week MMF print (observations 2026-11-24 and 2026-11-18, six days apart) fails
   'Weekly reported-change endpoints required' (ici_research.py:147).
2. data/compound-history.json (justhodl-compound-aggregator, cron 21:15 Mon-Fri) crossed 7 d. compound_overlays.apply
   writes the history only when input_coverage_complete is True; this run reads data/compound-signals.json's
   input_evidence / withheld fields to name the input that is incomplete.

Nothing invoked, nothing written. Exit 0 always (a diagnosis that cannot be completed is a finding).
"""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import re
import sys
import traceback

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
ICI_SRC = ROOT / "aws" / "lambdas" / "justhodl-ici-flows" / "source"
sys.path.insert(0, str(ICI_SRC))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
PRIVATE = "audit-private/20260909-originals/ici-research/"
COMPOUND_KEY = "data/compound-signals.json"
SECRET_KV = re.compile(r"(?i)\b(api[_-]?key|apikey|access[_-]?key|token|secret|password|authorization|signature)(\s*[=:]\s*)[^\s&\"',}]+")


def redact(text):
    return SECRET_KV.sub(lambda m: m.group(1) + m.group(2) + "***", str(text))


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    cfg = Config(retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=10, read_timeout=120)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)

    def get_bytes(key):
        return s3.get_object(Bucket=LIVE, Key=key)["Body"].read()

    with report("ops_7007_ici_native_and_compound_history_diagnosis") as r:
        r.heading("ops 7007 - why data/ici-flows.json and data/compound-history.json stopped (read-only, %s)" % now.date())

        r.section("1. ICI native packet: replay the research model on the retained originals of the newest requests")
        try:
            import ici_research as model
            import verify_ici_research as independent
            pg = s3.get_paginator("list_objects_v2")
            journals = []
            for page in pg.paginate(Bucket=LIVE, Prefix=PRIVATE + "requests/"):
                journals.extend(page.get("Contents") or [])
            journals.sort(key=lambda o: o["LastModified"], reverse=True)
            r.log("%d request journals; newest 6:" % len(journals))
            seen_msgs = set()
            for o in journals[:6]:
                j = json.loads(get_bytes(o["Key"]) or b"{}")
                srcs = j.get("sources") or {}
                r.log("  %s %.1f h: status %s error_type %s attempts %s sources %s" % (
                    o["Key"].rsplit("/", 1)[-1][:16], (now - o["LastModified"]).total_seconds() / 3600, j.get("status"), j.get("error_type"),
                    j.get("provider_request_attempts"), {k: (v.get("http_status"), v.get("acquired_at")) for k, v in srcs.items()}))
                if j.get("status") != "failed" or not srcs:
                    continue
                raws = {}
                for kind, entry in srcs.items():
                    ref = entry.get("original") or {}
                    raw = get_bytes(ref["key"])
                    ok = len(raw) == ref.get("bytes") and hashlib.sha256(raw).hexdigest() == ref.get("sha256")
                    raws[kind] = (raw, entry.get("acquired_at"))
                    r.log("    original %s: %d B, identity %s" % (kind, len(raw), "ok" if ok else "DIFFERS"))
                parsed = {}
                for kind, (raw, acquired_at) in raws.items():
                    try:
                        p = model.parse_release(raw, kind, acquired_at)
                        parsed[kind] = p
                        r.log("    parse_release(%s): release %s observations %s" % (kind, p.get("release_date"), p.get("observation_dates")))
                    except Exception as exc:  # noqa: BLE001
                        msg = "%s: %s" % (type(exc).__name__, exc)
                        seen_msgs.add(msg)
                        r.warn("    parse_release(%s) -> %s" % (kind, redact(msg)[:300]))
                for kind, p in parsed.items():
                    try:
                        model.reconcile(p, kind)
                        r.log("    reconcile(%s): ok" % kind)
                    except Exception as exc:  # noqa: BLE001
                        msg = "%s: %s" % (type(exc).__name__, exc)
                        seen_msgs.add(msg)
                        r.warn("    reconcile(%s) -> %s" % (kind, redact(msg)[:300]))
                        if kind == "mmf":
                            dates = [model.observation(lbl) for lbl in p["table_header"][1:] if model.observation(lbl)]
                            r.log("    mmf printed date columns: %s" % dates)
                if len(parsed) == len(raws):
                    try:
                        out = model.compile_releases({k: {"raw": v[0], "acquired_at": v[1]} for k, v in raws.items()}, now.isoformat())
                        r.log("    compile_releases: ok (%d keys)" % len(out))
                        if independent is not None:
                            proof = independent.verify({k: v[0] for k, v in raws.items()}, out)
                            r.log("    independent.verify issues: %s" % json.dumps(proof.get("reconciliation_issues"))[:400])
                    except Exception as exc:  # noqa: BLE001
                        msg = "%s: %s" % (type(exc).__name__, exc)
                        seen_msgs.add(msg)
                        r.warn("    compile_releases -> %s" % redact(msg)[:300])
            if seen_msgs:
                r.warn("ICI native packet refusal(s) reproduced: %s" % sorted(seen_msgs))
            else:
                r.log("no refusal reproduced from the journals read (the lane's ValueError is elsewhere: the browser transport or S3 steps)")
        except Exception:  # noqa: BLE001
            r.warn("ICI replay could not complete: %s" % redact(traceback.format_exc())[-900:])

        r.section("2. compound-aggregator: which input withholds the history write")
        try:
            obj = s3.get_object(Bucket=LIVE, Key=COMPOUND_KEY)
            doc = json.loads(obj["Body"].read() or b"{}")
            age = (now - obj["LastModified"]).total_seconds() / 3600
            r.log("%s %.1f h old: generated_at %s | top-level keys %s" % (COMPOUND_KEY, age, doc.get("generated_at"), sorted(doc)[:40]))
            for name in ("withheld", "selection_complete", "input_coverage_complete", "overlay_evidence", "overlays"):
                if name in doc:
                    r.log("  %s: %s" % (name, json.dumps(doc[name], default=str)[:1200]))
            ev = doc.get("input_evidence") or doc.get("inputs") or {}
            if isinstance(ev, dict):
                bad = {k: v for k, v in ev.items() if not (isinstance(v, dict) and v.get("status") in ("usable", "empty", "excluded"))}
                r.log("  input_evidence: %d inputs, %d not usable/empty/excluded" % (len(ev), len(bad)))
                for k, v in list(bad.items())[:30]:
                    r.warn("  incomplete input %s: %s" % (k, json.dumps(v, default=str)[:300]))
                if not bad and ev:
                    r.log("  every input reads usable/empty/excluded at the last run - the withheld flag or the overlay evidence above says why the history was not written")
            else:
                r.log("  no input_evidence dict in the document; keys: %s" % sorted(doc)[:60])
            hist_obj = s3.head_object(Bucket=LIVE, Key="data/compound-history.json")
            r.log("  data/compound-history.json %.1f h old, %d B" % ((now - hist_obj["LastModified"]).total_seconds() / 3600, hist_obj["ContentLength"]))
        except Exception:  # noqa: BLE001
            r.warn("compound read could not complete: %s" % redact(traceback.format_exc())[-600:])

        r.ok("read-only; nothing invoked, nothing written")
    sys.exit(0)


if __name__ == "__main__":
    main()
