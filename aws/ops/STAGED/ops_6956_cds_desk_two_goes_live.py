"""ops 6956 - CDS desk 2.0 goes live: the whole public tape measured again WITH WRITES ON, then the Lambda itself.

Khalid 2026-11-25: "audit and improve the CDS engine next and improve it also to include more companies and all asset
classes and even sovereign CDS and public companies worldwide".

Two steps, both disclosed one-offs (the engine's own schedule stays once a day, 04:00 UTC Tuesday to Saturday):

  1. The rebuild ops 6955 ran twice dry, now writing: every DTCC public credit file since 2024-09-03 read through the
     deployed 2.0 code on this runner (about twenty minutes - longer than one Lambda run allows), which writes each day's
     prints, the bank, the document, the history and the tape of prints. It refuses to write unless the deployed
     package is this checkout's 2.0 with 2048 MB and 900 s. The 1.6 bank, document and history are copied to
     data/warm/cds/before-2.0/ first; nothing is deleted.
  2. One invoke of justhodl-cds-desk with {"action": "daily", "dry_run": true}: the scheduled run's own path on the
     Lambda's own memory and clock, writing nothing - so tomorrow's 04:00 UTC run is not the first time 2.0 runs there.

Then the published document is read back and compared with what was built.

Run once. Running it again measures the tape again and writes the same documents (the copy aside is not overwritten).
"""
from pathlib import Path
import gzip
import importlib.util
import json
import sys
import time

HERE = Path(__file__).resolve().parent
NAME = "ops_6956_cds_desk_two_goes_live"
LIVE = "justhodl-dashboard-live"


def load():
    spec = importlib.util.spec_from_file_location("ops_6955", str(HERE / "ops_6955_cds_desk_two_dry_run.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def after(r, packet):
    """The Lambda's own daily path, dry; then the documents as the warehouse now holds them."""
    import boto3
    from botocore.config import Config
    r.section("5. The Lambda itself: its scheduled path, writing nothing")
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=920, connect_timeout=20, retries={"max_attempts": 0}))
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-cds-desk", InvocationType="RequestResponse", LogType="Tail", Payload=json.dumps({"action": "daily", "dry_run": True}).encode())
    body = json.loads(resp["Payload"].read() or b"{}")
    import base64
    tail = base64.b64decode(resp.get("LogResult") or b"").decode("utf-8", "replace")
    used = [ln for ln in tail.splitlines() if "Max Memory Used" in ln]
    r.log("invoke {action: daily, dry_run: true}: %.0f s, status %s, error %s" % (time.time() - t0, resp.get("StatusCode"), resp.get("FunctionError")))
    r.log("returned: %s" % json.dumps({k: body.get(k) for k in ("ok", "action", "file", "as_of", "dry_run", "packet_bytes", "history_bytes", "tape_bytes", "bank_bytes_gz", "n_priced", "error", "errorMessage")})[:700])
    r.log("lambda report: %s" % (used[-1].strip() if used else tail[-300:]))
    if resp.get("FunctionError") or not body.get("ok") or not body.get("dry_run"):
        r.fail("the Lambda did not finish its own daily path on 2.0: %s" % json.dumps(body)[:500])
        raise SystemExit(1)

    r.section("6. The documents as the warehouse now holds them")
    s3 = boto3.client("s3", region_name="us-east-1")

    def get(key):
        raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
        return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)
    doc, tape, hist, bank = get("data/cds-desk.json"), get("data/cds-desk-tape.json"), get("data/cds-desk-history.json"), get("data/warm/cds/bank.json.gz")
    same = doc.get("generated_at") == packet.get("generated_at") and doc.get("version") == packet.get("version") == "2.0.0"
    r.log("document: version %s, as of %s, generated %s (the one built above: %s) | groups %s | indices %d" % (
        doc.get("version"), doc.get("as_of"), doc.get("generated_at"), same, {g: b.get("n_priced") for g, b in doc["groups"].items()}, len(doc["indices"])))
    r.log("tape: %s names, %s prints | history: %d names | bank: %d series, %d days, census %d days, version %s" % (
        tape.get("n_names"), tape.get("n_prints"), len(hist.get("names") or {}), len(bank.get("series") or {}), len(bank.get("days") or []), len(bank.get("census") or {}), bank.get("version")))
    kept = s3.list_objects_v2(Bucket=LIVE, Prefix="data/warm/cds/before-2.0/").get("Contents") or []
    r.log("kept aside (1.6): %s" % [(o["Key"].rsplit("/", 1)[-1], o["Size"]) for o in kept])
    prints = s3.list_objects_v2(Bucket=LIVE, Prefix="data/warm/cds/prints/", MaxKeys=1000)
    r.log("day files of prints: %s%s" % (prints.get("KeyCount"), "+" if prints.get("IsTruncated") else ""))
    if not same or len(kept) < 3:
        r.fail("the warehouse does not hold the document built above, or the 1.6 copies are not aside")
        raise SystemExit(1)


if __name__ == "__main__":
    load().main(True, NAME, after)
