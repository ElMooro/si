"""ops 6871 - READ-ONLY dry run: what justhodl-bank-histories v1.2.0 would write, block by block, against the live book.

Khalid 2026-11-20 22:20 CT: "Yes do 1 then 2 then 3 thrn have my data update as needed to stay fresh and make sure my
bill stays the same".  Step 1 is the history book.  ops 6870 found it writing 52-77% of every file the live bucket
receives (110,292 on a one-run day, 352,089 on a three-run day; its own record: puts 123,623, unchanged 0), each write
billed twice.  The cause is in its code: the write-if-changed hash covers `as_of`, the FMP bank manifest's updated_at,
so no document ever matches its stored hash.  v1.2.0 (in this commit, NOT deployed) hashes the document without the
stamp, recognises a v1.1 document from its old hash, and confirms a block whose source files did not change without
reading it.

This script runs the v1.2.0 code from the checkout on the runner against the live bank files and the live hash state,
with every write path replaced by a function that raises.  Per block it reports how many documents the new code would
write (changed / new) and how many it would leave alone, and it PROVES both verdicts on samples read back from the
bucket: a document called unchanged must equal the stored object in everything but `as_of`; a document called changed
must differ from it, and the report names the fields that differ.  It also reads the bank side: how many source files
changed in the last day and week (what the block signature will see).

Nothing is written but this report.  Cost of the run: the bank files leave AWS once (about 3 GB, under 30 cents).
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import concurrent.futures as cf
import gzip
import hashlib
import json
import multiprocessing
import os
import random
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "aws" / "lambdas" / "justhodl-bank-histories" / "source"
SHARED = ROOT / "aws" / "shared"
sys.path.insert(0, str(ROOT / "aws" / "ops"))
LIVE = "justhodl-dashboard-live"
WALL_S = 44 * 60                     # the runner allows 60 minutes; blocks not reached are named, never guessed
EGRESS_CAP = 8 * 1024 ** 3           # bytes of bank files this run may read out of AWS
SAME_SAMPLE, CHANGED_SAMPLE = 10, 6  # documents read back from the bucket per block and verdict
PROCS = 3
_W = {}


def load_engine(db):
    """The v1.2.0 module from the checkout with its two write helpers replaced: a write attempt raises."""
    os.environ["HISTORY_BOOK_DB"] = db
    os.environ["HISTORY_BOOK_WORKERS"] = "16"
    os.environ.setdefault("S3_BUCKET", LIVE)
    os.environ.setdefault("AWS_REGION", "us-east-1")
    for p in (str(SHARED), str(SRC)):
        if p not in sys.path:
            sys.path.insert(0, p)
    import lambda_function as lf

    def refuse(*a, **k):
        raise RuntimeError("dry run: a write was attempted (%s)" % (a[0] if a else "?"))
    lf._put_json = lf._put_json_gz = refuse
    return lf


def norm(doc):
    """A document as it reads back from JSON, without the vintage stamp."""
    return json.loads(json.dumps({k: v for k, v in (doc or {}).items() if k != "as_of"}, separators=(",", ":"), default=str))


def describe(new, old):
    """What differs between a built document and the stored one (both normalised): a short, countable description."""
    if old is None:
        return {"kind": "no stored object"}
    if new == old:
        return {"kind": "identical"}
    out = {"kind": "differs", "top": sorted(k for k in set(new) | set(old) if k != "rows" and new.get(k) != old.get(k))}
    nr, orows = new.get("rows") or [], old.get("rows") or []
    out["rows_new"], out["rows_old"] = len(nr), len(orows)
    key = lambda r: "%s|%s|%s" % (r.get("date"), r.get("fiscalYear"), r.get("period"))
    od = {key(r): r for r in orows if isinstance(r, dict)}
    nd = {key(r): r for r in nr if isinstance(r, dict)}
    out["rows_added"] = sorted(set(nd) - set(od), reverse=True)[:4]
    out["rows_gone"] = sorted(set(od) - set(nd), reverse=True)[:4]
    fields = Counter()
    touched = 0
    for k in set(nd) & set(od):
        a, b = nd[k], od[k]
        diff = [f for f in set(a) | set(b) if a.get(f) != b.get(f)]
        if diff:
            touched += 1
            fields.update(diff)
    out["rows_touched"] = touched
    out["fields"] = [f for f, _ in fields.most_common(8)]
    return out


def selftest():
    a = {"symbol": "A", "as_of": "x", "n": 2, "rows": [{"date": "2025-09-27", "fiscalYear": "2025", "period": "FY", "revenue": 2, "eps": 1.5}, {"date": "2024-09-28", "fiscalYear": "2024", "period": "FY", "revenue": 1}]}
    b = json.loads(json.dumps(a))
    b["as_of"] = "y"
    assert describe(norm(a), norm(b)) == {"kind": "identical"}, "a vintage alone is not a difference"
    b["rows"][0]["revenue"] = 3
    d = describe(norm(a), norm(b))
    assert d["kind"] == "differs" and d["rows_touched"] == 1 and d["fields"] == ["revenue"] and d["top"] == [] and not d["rows_added"], d
    b["rows"].pop()
    b["n"] = 1
    d = describe(norm(a), norm(b))
    assert d["top"] == ["n"] and d["rows_added"] == ["2024-09-28|2024|FY"] and d["rows_new"] == 2 and d["rows_old"] == 1, d
    assert describe(norm(a), None) == {"kind": "no stored object"}
    src = (SRC / "lambda_function.py").read_text(encoding="utf-8")
    assert src.count(".put_object(") == 2 and "delete_object" not in src and "copy_object" not in src, "the engine writes only through _put_json / _put_json_gz"
    assert 'VERSION = "1.2.0"' in src and "def doc_hash(" in src and "def legacy_hash(" in src and "def source_signature(" in src


def init_worker():
    lf = load_engine("/tmp/history-book-dry-%d.sqlite" % os.getpid())
    _W["lf"] = lf
    _W["universe"] = lf.load_universe()


def do_block(job):
    bid, endpoint, bulk, kind, files, prev, as_of, t_end = job
    lf, universe = _W["lf"], _W["universe"]
    out = {"bid": bid, "files": len(files), "pid": os.getpid()}
    t0 = time.time()
    notes = []
    st, files_done, rows_n = lf.stage_block(files, universe, t_end, notes)
    out["notes"] = notes[:4]
    if files_done < len(files):
        st.close()
        out["skipped"] = "the run's clock ended at %d of %d files" % (files_done, len(files))
        return out
    out["stage_s"] = round(time.time() - t0, 1)
    hashes = lf._get_json(lf.ROOT + "_state/hashes/%s__%s.json.gz" % (bulk, kind)) or {}
    legacy = (prev.get("legacy_as_of") or prev.get("as_of")) if prev.get("hash_rev") != lf.HASH_REV else None
    rng = random.Random(bid)
    picks = {"same": [], "changed": [], "new": []}
    seen = Counter()

    def on_doc(sym, key, doc, verdict):
        seen[verdict] += 1
        cap = SAME_SAMPLE if verdict == "same" else CHANGED_SAMPLE
        bag = picks[verdict]
        if len(bag) < cap:
            bag.append((sym, key, doc))
        else:
            j = rng.randrange(seen[verdict])               # reservoir: every document has the same chance of being read back
            if j < cap:
                bag[j] = (sym, key, doc)
    stats = {}
    built = lf.emit_block(endpoint, bulk, kind, st, as_of, hashes, time.time() + 3600, stats, legacy_as_of=legacy, dry_run=True, on_doc=on_doc)
    st.close()
    out.update({k: stats.get(k, 0) for k in ("symbols", "unchanged", "changed", "new", "migrated", "written", "failed")})
    # orphans: documents the book holds for symbols the block no longer produces (the engine never deletes; reported, not touched)
    out.update(rows=rows_n, seconds=round(time.time() - t0, 1), stored_hashes=len(hashes), legacy_as_of=legacy, orphans=len(set(hashes) - set(built)))
    s3 = lf._s3()

    def stored(key):
        try:
            return json.loads(gzip.decompress(s3.get_object(Bucket=LIVE, Key=key)["Body"].read()))
        except Exception:  # noqa: BLE001
            return None
    proof = {"same_read": 0, "same_identical": 0, "same_wrong": [], "same_stamp_is_block_vintage": 0, "changed_read": 0, "changed_differs": 0,
             "changed_identical": [], "changed_missing": 0, "fields": Counter(), "rows_added": 0, "examples": [], "new_read": 0, "new_absent": 0, "new_present": []}
    for sym, key, doc in picks["same"]:
        old = stored(key)
        proof["same_read"] += 1
        if old is not None and describe(norm(doc), norm(old))["kind"] == "identical":
            proof["same_identical"] += 1
            proof["same_stamp_is_block_vintage"] += int(old.get("as_of") == (legacy or prev.get("as_of")))
        else:
            proof["same_wrong"].append(sym)
    for sym, key, doc in picks["changed"]:
        old = stored(key)
        d = describe(norm(doc), norm(old) if old is not None else None)
        proof["changed_read"] += 1
        if d["kind"] == "differs":
            proof["changed_differs"] += 1
            proof["fields"].update(d["fields"] + ["(top) " + t for t in d["top"]])
            proof["rows_added"] += int(bool(d["rows_added"]))
            if len(proof["examples"]) < 3:
                proof["examples"].append("%s: %d rows touched, +%d rows %s, fields %s" % (sym, d["rows_touched"], len(d["rows_added"]), d["rows_added"][:2], d["fields"][:5]))
        elif d["kind"] == "identical":
            proof["changed_identical"].append(sym)
        else:
            proof["changed_missing"] += 1
    for sym, key, doc in picks["new"]:
        proof["new_read"] += 1
        if stored(key) is None:
            proof["new_absent"] += 1
        else:
            proof["new_present"].append(sym)
    proof["fields"] = proof["fields"].most_common(8)
    out["proof"] = proof
    return out


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    t0 = time.time()
    t_end = t0 + WALL_S
    lf = load_engine("/tmp/history-book-dry-main.sqlite")
    import fmp_bank
    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=64)
    s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
    now = datetime.now(timezone.utc)
    with report("ops_6871_history_book_dry_run") as r:
        r.heading("ops 6871 - history book v1.2.0, dry run against the live book (read-only)")

        r.section("A. The book as the deployed engine left it")
        state = lf._get_json(lf.STATE_KEY, gz=False) or {}
        man = lf._get_json(lf.MANIFEST_KEY, gz=False) or {}
        blocks = state.get("blocks") or {}
        lr = state.get("last_run") or {}
        r.log("deployed version %s, phase %s, state written %s; last run: %s blocks in %ss, puts %s, unchanged %s" % (
            man.get("version"), state.get("phase"), state.get("updated_at"), len(lr.get("blocks") or []), lr.get("seconds"), lr.get("puts"), lr.get("unchanged")))
        held = {b: v for b, v in blocks.items() if v.get("symbols")}
        docs_total = sum(v.get("symbols", 0) for v in held.values())
        r.log("%d blocks hold documents, %d documents in all; one full pass by the deployed engine writes every one of them" % (len(held), docs_total))
        for bid, v in sorted(held.items(), key=lambda t: -t[1].get("symbols", 0)):
            r.log("  %-46s symbols %6d | last pass: written %6d unchanged %d failed %d in %5.0f s | refreshed %s | vintage %s" % (
                bid, v.get("symbols", 0), v.get("written", 0), v.get("unchanged", 0), v.get("failed", 0), v.get("seconds") or 0, str(v.get("refreshed_at"))[:16], str(v.get("as_of"))[:16]))

        r.section("B. The bank files the book is built from")
        man_fmp = fmp_bank.manifest() or {}
        as_of = man_fmp.get("updated_at")
        r.log("FMP bank manifest updated_at %s (the value the old hash covered); %d dated keys" % (as_of, len(man_fmp.get("dated_keys") or [])))
        order = []
        for endpoint, bulk in lf.SNAPSHOT_BULKS:
            files = lf.snapshot_files(bulk)
            if files:
                order.append(("%s/latest" % bulk, endpoint, bulk, "latest", files))
        for endpoint, bulk in lf.BULKS:
            for kind in ("annual", "quarter"):
                files = lf.held_files(man_fmp, bulk, kind)
                if files:
                    order.append(("%s/%s" % (bulk, kind), endpoint, bulk, kind, files))

        def head(key):
            try:
                h = s3.head_object(Bucket=LIVE, Key=key)
                return key, int(h.get("ContentLength") or 0), h.get("LastModified")
            except Exception:  # noqa: BLE001
                return key, None, None
        with cf.ThreadPoolExecutor(max_workers=32) as ex:
            heads = {k: (sz, lm) for k, sz, lm in ex.map(head, [f[2] for o in order for f in o[4]])}
        size_of = {}
        tot_files = tot_bytes = day_files = week_files = 0
        for bid, _e, _b, _k, files in order:
            sz = sum(heads[f[2]][0] or 0 for f in files)
            size_of[bid] = sz
            ages = [(now - heads[f[2]][1]).total_seconds() / 86400.0 for f in files if heads[f[2]][1]]
            d1, d7 = sum(1 for a in ages if a <= 1.0), sum(1 for a in ages if a <= 7.0)
            tot_files += len(files)
            tot_bytes += sz
            day_files += d1
            week_files += d7
            r.log("  %-46s %3d files %7.1f MB | missing %d | written by the bank in the last day %d, week %d | newest %s" % (
                bid, len(files), sz / 1e6, sum(1 for f in files if heads[f[2]][0] is None), d1, d7, max((heads[f[2]][1] for f in files if heads[f[2]][1]), default=None) and max(heads[f[2]][1] for f in files if heads[f[2]][1]).strftime("%m-%d %H:%M")))
        r.log("%d blocks, %d source files, %.2f GB; the bank wrote %d of them in the last day and %d in the last week" % (len(order), tot_files, tot_bytes / 1e9, day_files, week_files))

        r.section("C. The dry run - v1.2.0 from the checkout, writes refused")
        jobs, skipped, planned = [], [], 0
        for bid, endpoint, bulk, kind, files in order:
            if planned + size_of[bid] > EGRESS_CAP:
                skipped.append((bid, "not read: the run's %.0f GB read allowance" % (EGRESS_CAP / 1e9)))
                continue
            planned += size_of[bid]
            jobs.append((bid, endpoint, bulk, kind, files, blocks.get(bid) or {}, as_of, t_end))
        results, write_attempts = {}, 0
        if os.environ.get("OPS_6871_INLINE"):                # the offline harness: one process, the caller's S3 double
            _W.update(lf=lf, universe=lf.load_universe())
            pool = cf.ThreadPoolExecutor(max_workers=1)
        else:                                               # spawn: every worker imports the engine itself (its own SQLite file, its own clients)
            pool = cf.ProcessPoolExecutor(max_workers=PROCS, initializer=init_worker, mp_context=multiprocessing.get_context("spawn"))
        with pool:
            futs = {pool.submit(do_block, j): j[0] for j in jobs}
            for f in cf.as_completed(futs):
                bid = futs[f]
                try:
                    res = f.result()
                except Exception as e:  # noqa: BLE001
                    write_attempts += int("a write was attempted" in str(e))
                    r.fail("%s: the dry run raised %s: %s" % (bid, type(e).__name__, str(e)[:240]))
                    continue
                results[bid] = res
                if res.get("skipped"):
                    skipped.append((bid, res["skipped"]))
                    continue
                p = res["proof"]
                r.log("  %-46s docs %6d | would write %6d (changed %d, new %d) | left alone %6d (seen through the old hash %d) | orphans %d | %4.0f s (read %4.0f s)" % (
                    bid, res["symbols"], res["written"], res["changed"], res["new"], res["unchanged"], res["migrated"], res["orphans"], res["seconds"], res["stage_s"]))
                if p["changed_read"]:
                    r.log("      changed, read back %d: %d differ from the stored object; fields %s%s" % (
                        p["changed_read"], p["changed_differs"], ", ".join("%s x%d" % (k, n) for k, n in p["fields"][:6]) or "-",
                        "; e.g. " + " || ".join(p["examples"][:2]) if p["examples"] else ""))
        done = {b: v for b, v in results.items() if not v.get("skipped")}

        r.section("D. The proof on documents read back from the bucket")
        T = Counter()
        wrong_same, wrong_changed, wrong_new = [], [], []
        for bid, v in done.items():
            p = v["proof"]
            for k in ("same_read", "same_identical", "same_stamp_is_block_vintage", "changed_read", "changed_differs", "changed_missing", "new_read", "new_absent"):
                T[k] += p[k]
            wrong_same += ["%s %s" % (bid, s) for s in p["same_wrong"]]
            wrong_changed += ["%s %s" % (bid, s) for s in p["changed_identical"]]
            wrong_new += ["%s %s" % (bid, s) for s in p["new_present"]]
        r.log("called unchanged: %d read back, %d equal the stored object in everything but as_of" % (T["same_read"], T["same_identical"]))
        r.log("called changed:   %d read back, %d differ from the stored object, %d have no stored object, %d are identical to it" % (
            T["changed_read"], T["changed_differs"], T["changed_missing"], len(wrong_changed)))
        r.log("called new:       %d read back, %d have no stored object, %d do" % (T["new_read"], T["new_absent"], len(wrong_new)))
        if wrong_same:
            r.fail("a document called unchanged differs from the stored object - it would be left stale: %s" % wrong_same[:12])
        elif T["same_read"]:
            r.ok("every document the new code would leave alone is the stored object, bar the stamp (%d of %d read back)" % (T["same_identical"], T["same_read"]))
        if wrong_changed:
            r.warn("a document called changed is identical to the stored object (the old hash was not recognised; it would be rewritten once): %s" % wrong_changed[:12])
        elif T["changed_read"]:
            r.ok("every document the new code would write really differs from what is stored (%d of %d read back; %d had no stored object)" % (
                T["changed_differs"], T["changed_read"], T["changed_missing"]))
        if wrong_new:
            r.warn("a document with no stored hash exists in the bucket (it would be rewritten once): %s" % wrong_new[:12])

        r.section("E. What it comes to")
        docs = sum(v["symbols"] for v in done.values())
        would = sum(v["written"] for v in done.values())
        left = sum(v["unchanged"] for v in done.values())
        migrated = sum(v["migrated"] for v in done.values())
        snap = {b: v for b, v in done.items() if b.endswith("/latest")}
        dated = {b: v for b, v in done.items() if not b.endswith("/latest")}
        r.log("blocks compared %d of %d (%d documents); not compared: %s" % (len(done), len(order), docs, ["%s (%s)" % s for s in skipped] or "none"))
        r.log("the deployed engine writes all %d of these on every pass; v1.2.0 would write %d (%.1f%%) and leave %d alone (%d of them recognised through the old hash)" % (
            docs, would, would / docs * 100 if docs else 0, left, migrated))
        r.log("  dated blocks (statements, ratios, metrics, growth): %d documents, would write %d" % (sum(v["symbols"] for v in dated.values()), sum(v["written"] for v in dated.values())))
        r.log("  snapshot blocks (profile, TTM, scores, rating, DCF, targets, grades, peers): %d documents, would write %d" % (sum(v["symbols"] for v in snap.values()), sum(v["written"] for v in snap.values())))
        r.log("read time for the compared blocks on this runner: %.0f s in all (the engine's run budget is 840 s; blocks whose files did not change are not read at all)" % sum(v["stage_s"] for v in done.values()))
        r.kv(blocks=len(order), blocks_compared=len(done), documents=docs, would_write=would, left_alone=left, seen_through_old_hash=migrated,
             share_written_pct=round(would / docs * 100, 2) if docs else None, same_read_back=T["same_read"], same_identical=T["same_identical"],
             changed_read_back=T["changed_read"], changed_differ=T["changed_differs"], bank_files=tot_files, bank_gb=round(tot_bytes / 1e9, 2),
             bank_files_written_last_day=day_files, bank_files_written_last_week=week_files, writes_attempted=write_attempts, wall_s=round(time.time() - t0))
        if not done:
            r.fail("no block was compared")
        if write_attempts:
            r.fail("the new code attempted %d write(s) in dry-run mode (refused)" % write_attempts)
        else:
            r.ok("read-only; every write path of the engine was replaced by a function that raises, and none was reached")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
