"""ops 6873 - ACTING: the history book's first runs on v1.2.0, watched from its own records.

Runs after deploy-lambdas has shipped justhodl-bank-histories 1.2.0 (ops 6871 was the dry run: 381,200 documents
compared, 109,043 really changed, 272,157 would be left alone and all of them were recognised through the old hash;
240 of 240 "unchanged" documents read back equal to the stored object, 66 of 66 "changed" ones differ).

  1. The deployed function: when it was last updated, and - through a dry-run invoke of two small blocks, which writes
     nothing - which version answers.  If it is not 1.2.0 the script stops here and kicks nothing.
  2. Kicks the engine (the conductor's own way: an Event invoke, only when the lease is free), waits for its state
     write, and reads its record: blocks read in full / confirmed unread, documents written, left alone, failed.
     Repeats until the phase is STEADY, three kicks at most; whatever is left the daily conductor finishes.
  3. With the book steady, one whole-book dry-run invoke: every block must be confirmed from its files' signatures
     alone, nothing to write, in seconds.  That is the run every day will be when the bank has not changed.

The only thing this script causes to be written is what the engine itself writes: the documents that changed.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
FN = "justhodl-bank-histories"
LIVE = "justhodl-dashboard-live"
STATE = "data/warm/history-book/_state/state.json"
MANIFEST = "data/warm/history-book/manifest.json"
WANT = "1.2.0"
MAX_KICKS = 3
WALL_S = 50 * 60
WAIT_S = 16 * 60


def summarise(run, blocks):
    """One line per block of a run's record."""
    out = []
    for bid in run.get("blocks") or []:
        b = blocks.get(bid) or {}
        out.append("%-46s %-16s docs %6d | written %6d (changed %d, new %d) | left alone %6d (old hash %d) | failed %d | %s s" % (
            bid, b.get("checked") or "?", b.get("symbols") or 0, b.get("written") or 0, b.get("changed") or 0, b.get("new") or 0, b.get("unchanged") or 0,
            b.get("migrated") or 0, b.get("failed") or 0, b.get("seconds") if b.get("checked") == "read in full" else b.get("check_seconds")))
    return out


def selftest():
    lines = summarise({"blocks": ["a/annual", "b/latest"]}, {"a/annual": {"checked": "read in full", "symbols": 3, "written": 1, "changed": 1, "unchanged": 2, "migrated": 2, "seconds": 7.5},
                                                             "b/latest": {"checked": "inputs unchanged", "symbols": 5, "unchanged": 5, "check_seconds": 0.2, "seconds": 9}})
    assert len(lines) == 2 and "read in full" in lines[0] and "written      1 (changed 1, new 0)" in lines[0] and lines[0].endswith("7.5 s"), lines
    assert "inputs unchanged" in lines[1] and lines[1].endswith("0.2 s"), lines


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    t0 = time.time()
    cfg = Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=10, read_timeout=930)
    lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 8, "mode": "standard"}))

    def jget(key):
        try:
            return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())
        except Exception:  # noqa: BLE001
            return {}

    def dry(payload):
        res = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps(dict(payload, dry_run=True)).encode())
        body = res["Payload"].read()
        try:
            return json.loads(body), res.get("FunctionError")
        except Exception:  # noqa: BLE001
            return {"raw": body[:400].decode("utf-8", "replace")}, res.get("FunctionError")

    with report("ops_6873_history_book_first_runs") as r:
        r.heading("ops 6873 - history book v1.2.0: the first runs, from its own records")

        r.section("1. What is deployed")
        fc = lam.get_function_configuration(FunctionName=FN)
        r.log("%s: last updated %s, code %s..., %s MB, timeout %s s" % (FN, fc.get("LastModified"), str(fc.get("CodeSha256"))[:16], fc.get("MemorySize"), fc.get("Timeout")))
        before = jget(STATE)
        man0 = jget(MANIFEST)
        r.log("book before: manifest version %s, phase %s, state written %s, last run puts %s unchanged %s" % (
            man0.get("version"), before.get("phase"), before.get("updated_at"), (before.get("last_run") or {}).get("puts"), (before.get("last_run") or {}).get("unchanged")))
        probe, err = dry({"blocks": ["peers-bulk/latest", "price-target-summary-bulk/latest"]})
        r.log("dry-run invoke of two small blocks: %s%s" % (json.dumps({k: probe.get(k) for k in ("ok", "dry_run", "version", "blocks", "would_put", "unchanged", "seconds", "raw")}), " [FunctionError %s]" % err if err else ""))
        after_probe = jget(STATE)
        if probe.get("version") != WANT or not probe.get("dry_run"):
            r.fail("the deployed function does not answer as %s (it answered %s); nothing was kicked" % (WANT, probe.get("version")))
            sys.exit(0)
        if after_probe.get("updated_at") != before.get("updated_at") or "lease_until" in after_probe and after_probe.get("lease_until") != before.get("lease_until"):
            r.fail("the dry-run invoke changed the state file (written %s -> %s)" % (before.get("updated_at"), after_probe.get("updated_at")))
            sys.exit(0)
        r.ok("version %s answers, and its dry run left the state file as it was" % WANT)

        r.section("2. The first runs")
        kicks, totals, steady = 0, {"puts": 0, "unchanged": 0, "failed": 0, "read": 0, "confirmed": 0}, False
        while kicks < MAX_KICKS and time.time() - t0 < WALL_S - WAIT_S:
            st = jget(STATE)
            if kicks and st.get("phase") == "STEADY":
                steady = True
                break
            lease = float(st.get("lease_until") or 0)
            if lease > time.time():
                r.log("lease held until %s - waiting" % datetime.fromtimestamp(lease, timezone.utc).strftime("%H:%M:%S"))
                time.sleep(min(60, max(5, lease - time.time())))
                continue
            prev = st.get("updated_at")
            res = lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
            kicks += 1
            r.log("kick %d -> %s" % (kicks, res.get("StatusCode")))
            deadline = time.time() + WAIT_S
            done = None
            while time.time() < deadline:
                time.sleep(15)
                st2 = jget(STATE)
                if st2.get("updated_at") and st2.get("updated_at") != prev and not float(st2.get("lease_until") or 0) > time.time():
                    done = st2
                    break
            if done is None:
                r.fail("kick %d: no state write within %d minutes" % (kicks, WAIT_S // 60))
                break
            run = done.get("last_run") or {}
            blocks = done.get("blocks") or {}
            man = jget(MANIFEST)
            r.log("run %d: version %s, %s blocks in %s s, phase %s | written %s, left alone %s | confirmed without a read: %d | notes %s" % (
                kicks, man.get("version"), len(run.get("blocks") or []), run.get("seconds"), done.get("phase"), run.get("puts"), run.get("unchanged"),
                len(run.get("confirmed_unread") or []), (run.get("notes") or [])[:4]))
            for line in summarise(run, blocks):
                r.log("    " + line)
            totals["puts"] += run.get("puts") or 0
            totals["unchanged"] += run.get("unchanged") or 0
            totals["failed"] += sum((blocks.get(b) or {}).get("failed") or 0 for b in run.get("blocks") or [])
            totals["read"] += len(run.get("blocks") or []) - len(run.get("confirmed_unread") or [])
            totals["confirmed"] += len(run.get("confirmed_unread") or [])
            if man.get("version") != WANT:
                r.fail("the manifest was written by version %s" % man.get("version"))
                break
            if done.get("phase") == "STEADY":
                steady = True
                break
        final = jget(STATE)
        man = jget(MANIFEST)
        blocks = final.get("blocks") or {}
        signed = [b for b, v in blocks.items() if v.get("symbols") and v.get("hash_rev") == 2 and v.get("source_sig")]
        held = [b for b, v in blocks.items() if v.get("symbols")]
        r.log("after %d kick(s): phase %s; %d of %d blocks read by %s and signed off; pending %s" % (kicks, final.get("phase"), len(signed), len(held), WANT, man.get("pending_blocks")))
        r.log("documents written in these runs %d, left alone %d, failed %d" % (totals["puts"], totals["unchanged"], totals["failed"]))
        if totals["failed"]:
            r.warn("%d puts failed; those documents keep their old hash and are written by the next run" % totals["failed"])

        r.section("3. A run with nothing new")
        if not steady:
            r.warn("the book is not steady yet (%d blocks still to be read by %s); the daily conductor finishes them. The no-change check below covers the blocks already signed off." % (len(held) - len(signed), WANT))
        again, err = dry({})
        conf = again.get("confirmed_unread") or []
        r.log("whole-book dry run: %s blocks looked at, %d confirmed from their files' signatures without a read, would write %s, %s s%s" % (
            len(again.get("blocks") or []), len(conf), again.get("would_put"), again.get("seconds"), " [FunctionError %s]" % err if err else ""))
        unconfirmed = [b for b in signed if b not in conf]
        if unconfirmed and not again.get("pending") and len(again.get("blocks") or []) >= len(held):
            due = [b for b in unconfirmed if (again.get("detail") or {}).get(b, {}).get("checked") == "read in full"]
            r.log("signed-off blocks read in full again (their weekly day, or the bank wrote their files meanwhile): %s" % due)
        if steady and again.get("would_put") == 0 and len(conf) >= len(signed) - 9:
            r.ok("with nothing new in the bank the engine writes nothing: %d blocks confirmed unread, 0 documents to write, %s s" % (len(conf), again.get("seconds")))
        elif steady and again.get("would_put"):
            r.warn("a second pass would still write %s documents - the bank's files changed between the runs, or a block is not stable: %s" % (
                again.get("would_put"), {b: d.get("written") for b, d in (again.get("detail") or {}).items() if d.get("written")}))
        r.kv(version=man.get("version"), kicks=kicks, phase=final.get("phase"), blocks_signed_off=len(signed), blocks=len(held), written=totals["puts"], left_alone=totals["unchanged"],
             failed=totals["failed"], second_pass_would_write=again.get("would_put"), second_pass_confirmed_unread=len(conf), second_pass_seconds=again.get("seconds"),
             wall_s=round(time.time() - t0))
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
