"""ops 6888 - ACTING: history book 1.3.0 - dry run of the one-snapshot-document block on live data, then its first run,
read back against the nine documents it replaces.

Khalid 2026-11-21: "Merge the one-file-per-symbol change".  Runs after deploy-lambdas has shipped
justhodl-bank-histories 1.3.0 and after ops 6887 found the new reader in 224 of the 225 engines that read the book.

  1. DRY RUN first.  The deployed engine's own dry-run invoke of the block snapshots/latest: it reads the live bank
     files, builds every symbol's document and compares - and writes nothing (the state file is checked before and
     after).  The script stops here unless the answer is version 1.3.0, covers a plausible number of symbols and
     reports no failure.
  2. The first run of that one block (an Event invoke, only when the lease is free), watched from the engine's own
     record; a second kick if the deadline cut it short.
  3. Read back: for 60 symbols the new document is fetched and every family in it is compared, row for row, with the
     one-family document it replaces (still on the bucket).  The public manifest must list the combined block with
     its nine families and none of the nine old blocks.
  4. The reader, as the engines carry it (aws/shared/fmp_bank.py from this checkout, against the live bucket): the
     layout it picks and the rows it returns for the same symbols.
  5. A second dry run: with nothing new in the bank the block must have nothing to write.

The only thing this script causes to be written is what the engine itself writes: one document per symbol.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import random
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
FN = "justhodl-bank-histories"
LIVE = "justhodl-dashboard-live"
BOOK = "data/warm/history-book/"
STATE = BOOK + "_state/state.json"
MANIFEST = BOOK + "manifest.json"
HASHES = BOOK + "_state/hashes/snapshots__latest.json.gz"
BLOCK = "snapshots/latest"
WANT = "1.3.0"
FAMILIES = ["profile-bulk", "ratios-ttm-bulk", "key-metrics-ttm-bulk", "scores-bulk", "rating-bulk", "dcf-bulk", "price-target-summary-bulk", "upgrades-downgrades-consensus-bulk", "peers-bulk"]
CORE = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM", "XOM", "KO", "PLTR", "COIN", "MSTR", "BMNR", "SPY", "QQQ", "IWM", "TLT", "GLD", "SMH"]
MAX_KICKS = 3
WAIT_S = 17 * 60


def plausible(detail):
    """Is a dry-run answer for the block one the first run may follow?  -> (ok, why)."""
    if not isinstance(detail, dict):
        return False, "no detail for the block"
    if detail.get("failed"):
        return False, "%s documents failed" % detail.get("failed")
    if detail.get("partial"):
        return False, "the dry run did not finish the block"
    n = detail.get("symbols") or 0
    if not 5000 <= n <= 150000:
        return False, "%s symbols is outside 5,000 - 150,000" % n
    if (detail.get("written") or 0) + (detail.get("unchanged") or 0) != n:
        return False, "written + unchanged does not add up to the symbols"
    return True, "ok"


def same_rows(a, b):
    return json.dumps(a, sort_keys=True, default=str) == json.dumps(b, sort_keys=True, default=str)


def selftest():
    assert plausible({"symbols": 24000, "written": 24000, "unchanged": 0}) == (True, "ok")
    assert plausible({"symbols": 24000, "written": 23990, "unchanged": 0})[0] is False and plausible({"symbols": 12, "written": 12})[0] is False
    assert plausible({"symbols": 24000, "written": 24000, "failed": 3})[0] is False and plausible({"symbols": 24000, "written": 24000, "partial": True})[0] is False and plausible(None)[0] is False
    assert same_rows([{"a": 1, "b": 2}], [{"b": 2, "a": 1}]) and not same_rows([{"a": 1}], [{"a": 2}])


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    t0 = time.time()
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(retries={"max_attempts": 2, "mode": "standard"}, connect_timeout=10, read_timeout=930))
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 8, "mode": "standard"}, max_pool_connections=24))

    def jget(key, default=None):
        try:
            raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
            if key.endswith(".gz"):
                raw = gzip.decompress(raw)
            return json.loads(raw)
        except Exception:  # noqa: BLE001
            return {} if default is None else default

    def dry(payload):
        res = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps(dict(payload, dry_run=True)).encode())
        body = res["Payload"].read()
        try:
            return json.loads(body), res.get("FunctionError")
        except Exception:  # noqa: BLE001
            return {"raw": body[:400].decode("utf-8", "replace")}, res.get("FunctionError")

    with report("ops_6888_history_book_one_document_first_run") as r:
        r.heading("ops 6888 - history book 1.3.0: dry run, first run and read-back of the one-document block, %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. Dry run on live data (nothing is written)")
        fc = lam.get_function_configuration(FunctionName=FN)
        r.log("%s: last updated %s, code %s..., %s MB, timeout %s s" % (FN, fc.get("LastModified"), str(fc.get("CodeSha256"))[:16], fc.get("MemorySize"), fc.get("Timeout")))
        before = jget(STATE)
        man0 = jget(MANIFEST)
        old_blocks = sorted(b for b in (man0.get("blocks") or {}) if b.endswith("/latest") and b != BLOCK)
        r.log("book before: manifest version %s, phase %s, %d blocks, one-family snapshot blocks %d, combined block %s" % (
            man0.get("version"), before.get("phase"), len(man0.get("blocks") or {}), len(old_blocks), "present" if BLOCK in (man0.get("blocks") or {}) else "absent"))
        probe, err = dry({"blocks": [BLOCK]})
        detail = (probe.get("detail") or {}).get(BLOCK) or {}
        r.log("dry run of %s: version %s, would write %s, would leave alone %s, %s s%s" % (BLOCK, probe.get("version"), probe.get("would_put"), probe.get("unchanged"), probe.get("seconds"), " [FunctionError %s] %s" % (err, probe.get("raw") or probe.get("errorMessage")) if err else ""))
        r.log("  block: %s | notes %s" % (json.dumps(detail), (probe.get("notes") or [])[:4]))
        after_probe = jget(STATE)
        if probe.get("version") != WANT or not probe.get("dry_run"):
            r.fail("the deployed function does not answer as %s (it answered %s); nothing was run" % (WANT, probe.get("version")))
            sys.exit(1)
        if after_probe.get("updated_at") != before.get("updated_at"):
            r.fail("the dry-run invoke changed the state file; nothing was run")
            sys.exit(1)
        already = bool(((before.get("blocks") or {}).get(BLOCK) or {}).get("source_sig"))
        ok, why = plausible(detail)
        if not ok and not (already and detail.get("checked") == "inputs unchanged"):
            r.fail("the dry run is not one a first run may follow: %s; nothing was run" % why)
            sys.exit(1)
        r.ok("version %s answers; its dry run built %s documents (%s new, %s changed, %s the same as stored) and left the state file as it was" % (WANT, detail.get("symbols"), detail.get("new"), detail.get("changed"), detail.get("unchanged")))

        r.section("2. The first run of the block")
        kicks, done = 0, None
        while kicks < MAX_KICKS and time.time() - t0 < 40 * 60:
            st = jget(STATE)
            blk = (st.get("blocks") or {}).get(BLOCK) or {}
            if blk.get("source_sig") and not blk.get("partial") and not blk.get("failed"):
                done = st
                break
            lease = float(st.get("lease_until") or 0)
            if lease > time.time():
                r.log("lease held until %s - waiting" % datetime.fromtimestamp(lease, timezone.utc).strftime("%H:%M:%S"))
                time.sleep(min(60, max(5, lease - time.time())))
                continue
            prev = st.get("updated_at")
            res = lam.invoke(FunctionName=FN, InvocationType="Event", Payload=json.dumps({"blocks": [BLOCK]}).encode())
            kicks += 1
            r.log("kick %d -> %s" % (kicks, res.get("StatusCode")))
            deadline = time.time() + WAIT_S
            got = None
            while time.time() < deadline:
                time.sleep(15)
                st2 = jget(STATE)
                if st2.get("updated_at") and st2.get("updated_at") != prev and not float(st2.get("lease_until") or 0) > time.time():
                    got = st2
                    break
            if got is None:
                r.fail("kick %d: no state write within %d minutes" % (kicks, WAIT_S // 60))
                break
            run, blk = got.get("last_run") or {}, (got.get("blocks") or {}).get(BLOCK) or {}
            r.log("run %d: %s s | documents %s, written %s (new %s, changed %s), left alone %s, failed %s, %s MB | %s | notes %s" % (
                kicks, run.get("seconds"), blk.get("symbols"), blk.get("written"), blk.get("new"), blk.get("changed"), blk.get("unchanged"), blk.get("failed"), round((blk.get("bytes") or 0) / 1e6, 1),
                "complete" if blk.get("source_sig") and not blk.get("partial") else "NOT complete", (run.get("notes") or [])[:4]))
        final = jget(STATE)
        man = jget(MANIFEST)
        blk = (final.get("blocks") or {}).get(BLOCK) or {}
        mblocks = man.get("blocks") or {}
        left = sorted(b for b in mblocks if b.endswith("/latest") and b != BLOCK)
        retired = final.get("retired_blocks") or {}
        r.log("after %d kick(s): manifest version %s; combined block %s with %s symbols, families %s; one-family blocks still listed %d; retired record: %s" % (
            kicks, man.get("version"), "listed" if BLOCK in mblocks else "NOT listed", (mblocks.get(BLOCK) or {}).get("symbols"), len((mblocks.get(BLOCK) or {}).get("bulks") or []), len(left),
            json.dumps({k: (len(v) if isinstance(v, list) else v) for k, v in retired.items()})))
        complete = bool(blk.get("source_sig")) and not blk.get("partial") and not blk.get("failed") and BLOCK in mblocks and not left
        if not complete:
            r.fail("the combined block is not complete yet (partial %s, failed %s, one-family blocks listed %d); the read-back below covers what is there" % (blk.get("partial"), blk.get("failed"), len(left)))
        else:
            r.ok("the combined block is complete and the nine one-family blocks are off the manifest")

        r.section("3. Read back: the new document against the nine it replaces")
        hashes = jget(HASHES)
        names = sorted(hashes) if isinstance(hashes, dict) else []
        random.seed(6888)
        sample = [s for s in CORE if s in names] + random.sample([n for n in names if n not in CORE], min(40, max(0, len(names) - len(CORE))))
        r.log("%d documents in the block's hash state; reading %d" % (len(names), len(sample)))
        fam_checked = fam_equal = docs_ok = 0
        fam_count, diffs = {}, []
        for sym in sample:
            doc = jget(BOOK + "snapshots/latest/%s.json.gz" % sym.replace("/", "_"), default={})
            fams = doc.get("families") if isinstance(doc, dict) else None
            if not isinstance(fams, dict) or not fams:
                diffs.append("%s: no combined document" % sym)
                continue
            good = True
            for bulk, fam in fams.items():
                fam_count[bulk] = fam_count.get(bulk, 0) + 1
                old = jget(BOOK + "%s/latest/%s.json.gz" % (bulk, sym.replace("/", "_")), default={})
                if not old:
                    continue                                    # the family had no one-family document for this symbol
                fam_checked += 1
                if same_rows(fam.get("rows"), old.get("rows")) and fam.get("endpoint") == old.get("endpoint"):
                    fam_equal += 1
                else:
                    good = False
                    if len(diffs) < 8:
                        diffs.append("%s %s: rows differ (new as_of %s, old as_of %s, %s vs %s rows)" % (sym, bulk, doc.get("as_of"), old.get("as_of"), len(fam.get("rows") or []), len(old.get("rows") or [])))
            docs_ok += int(good)
        r.log("families compared with their one-family document: %d, equal row for row: %d; documents with every family equal: %d of %d" % (fam_checked, fam_equal, docs_ok, len(sample)))
        r.log("families present in the sampled documents: %s" % json.dumps(fam_count, sort_keys=True))
        for d in diffs:
            r.log("  " + d)
        aapl = jget(BOOK + "snapshots/latest/AAPL.json.gz", default={})
        if aapl:
            r.log("AAPL: as_of %s, %d families, n %s: %s" % (aapl.get("as_of"), len(aapl.get("families") or {}), aapl.get("n"), {b: len(f.get("rows") or []) for b, f in (aapl.get("families") or {}).items()}))
        if fam_checked and fam_equal == fam_checked:
            r.ok("%d of %d families read back equal to the documents they replace" % (fam_equal, fam_checked))
        elif fam_checked:
            r.warn("%d of %d families differ from the one-family documents (a bank refresh between the two writes shows as different as_of above)" % (fam_checked - fam_equal, fam_checked))

        r.section("4. The reader the engines carry, against the live bucket")
        try:
            sys.path.insert(0, str(ROOT / "aws" / "shared"))
            import fmp_bank
            paths = {bulk: path for path, bulk in fmp_bank.SNAPSHOTS.items()}
            layouts = {bulk: fmp_bank.snapshot_layouts(bulk) for bulk in FAMILIES}
            r.log("layout per family: %s" % json.dumps(layouts, sort_keys=True))
            hits = asked = 0
            for sym in sample[:25]:
                doc = jget(BOOK + "snapshots/latest/%s.json.gz" % sym.replace("/", "_"), default={})
                for bulk, fam in (doc.get("families") or {}).items():
                    if bulk not in paths:
                        continue
                    asked += 1
                    got = fmp_bank.snapshot_doc(paths[bulk], sym)
                    hits += int(bool(got) and same_rows(got.get("rows"), fam.get("rows")))
            r.log("fmp_bank.snapshot_doc for %d (symbol, family) pairs: %d returned the combined document's rows; reader counters %s" % (asked, hits, json.dumps({k: v for k, v in fmp_bank.STATS.items() if v})[:200]))
            row = fmp_bank.snapshot("profile", "AAPL") or []
            r.log("fmp_bank.snapshot('profile', 'AAPL'): %d row(s), companyName %s, price %s" % (len(row), (row[0] if row else {}).get("companyName"), (row[0] if row else {}).get("price")))
            if asked and hits == asked and all(v[:1] == ["combined"] for v in layouts.values()):
                r.ok("the reader takes every family from the combined document")
            else:
                r.warn("the reader did not take every family from the combined document (see the layouts above)")
        except Exception as e:  # noqa: BLE001
            r.warn("reader check could not run on the runner: %s: %s" % (type(e).__name__, str(e)[:160]))

        r.section("5. A second pass with nothing new")
        again, err = dry({"blocks": [BLOCK]})
        d2 = (again.get("detail") or {}).get(BLOCK) or {}
        r.log("dry run again: would write %s, left alone %s, block checked as '%s', %s s%s" % (again.get("would_put"), again.get("unchanged"), d2.get("checked"), again.get("seconds"), " [FunctionError %s]" % err if err else ""))
        if complete and again.get("would_put") == 0:
            r.ok("with nothing new in the bank the block has nothing to write")
        r.kv(version=man.get("version"), kicks=kicks, complete=complete, documents=blk.get("symbols"), written=blk.get("written"), failed=blk.get("failed"), one_family_blocks_left=len(left),
             families_compared=fam_checked, families_equal=fam_equal, second_pass_would_write=again.get("would_put"), wall_s=round(time.time() - t0))
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
