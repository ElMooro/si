"""ops 6887 - READ-ONLY: does every engine that reads the history book now carry the reader that understands one
snapshot document per symbol?

Khalid 2026-11-21: "Merge the one-file-per-symbol change" (history book 1.3.0: the nine per-family snapshot files of a
symbol become one document; about 90,000 writes on a refresh day become about 25,000).  The writer may only go live
after every reader can read both layouts, because the nine old blocks leave the manifest when the combined one is
complete.  The reader (aws/shared/fmp_bank.py, commit 9dcdd2b4) is bundled into 225 engines; the fleet lane (run
44931013294) redeployed 224 of them - all but justhodl-bond-desk, whose own suite is red for another reason.

This checks, for every one of the 224: the function was updated after the lane started, and - for every function, by
downloading its deployed package - that the bundled fmp_bank.py is byte-identical to the one in the repository.
Nothing is written or invoked.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import io
import json
import sys
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))
REGION = "us-east-1"
LANE_STARTED = "2026-11-21T17:37:00"
HELD_BACK = {"justhodl-bond-desk"}
MODULE = "aws/shared/fmp_bank.py"


def verdict(modified, bundled_sha, repo_sha, lane_started=LANE_STARTED):
    if bundled_sha is None:
        return "NO READER IN PACKAGE"
    if bundled_sha != repo_sha:
        return "OLD READER"
    return "ok" if (modified or "")[:19] >= lane_started else "ok (deployed earlier)"


def selftest():
    assert verdict("2026-11-21T18:02:11.000+0000", "a", "a") == "ok" and verdict("2026-11-20T01:00:00.000+0000", "a", "a") == "ok (deployed earlier)"
    assert verdict("2026-11-21T18:02:11.000+0000", "b", "a") == "OLD READER" and verdict("2026-11-21T18:02:11.000+0000", None, "a") == "NO READER IN PACKAGE"


def main():
    selftest()
    import boto3
    from botocore.config import Config
    import shared_dependents
    from ops_report import report
    lam = boto3.client("lambda", region_name=REGION, config=Config(retries={"max_attempts": 10, "mode": "adaptive"}, max_pool_connections=16))
    repo_bytes = (ROOT / MODULE).read_bytes()
    repo_sha = hashlib.sha256(repo_bytes).hexdigest()
    names = sorted(shared_dependents.dependents(ROOT, [MODULE]))
    with report("ops_6887_fleet_carries_the_new_reader") as r:
        r.heading("ops 6887 - the fleet's bundled history-book reader (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        r.log("%d engines import %s; repository copy sha256 %s (%d bytes); new-layout marker present: %s" % (len(names), MODULE, repo_sha[:16], len(repo_bytes), b'SNAPSHOT_BOOK = "snapshots/latest"' in repo_bytes))

        def one(fn):
            try:
                g = lam.get_function(FunctionName=fn)
                cfg = g["Configuration"]
                with urllib.request.urlopen(g["Code"]["Location"], timeout=120) as resp:
                    raw = resp.read()
                bundled = None
                with zipfile.ZipFile(io.BytesIO(raw)) as z:
                    hits = [n for n in z.namelist() if n == "fmp_bank.py" or n.endswith("/fmp_bank.py")]
                    if hits:
                        bundled = hashlib.sha256(z.read(hits[0])).hexdigest()
                return fn, cfg.get("LastModified"), cfg.get("CodeSize"), bundled, None
            except Exception as e:  # noqa: BLE001
                return fn, None, None, None, "%s: %s" % (type(e).__name__, str(e)[:80])

        with ThreadPoolExecutor(max_workers=12) as ex:
            rows = list(ex.map(one, names))
        r.section("1. Every importer")
        tally, bad, mb = {}, [], 0
        for fn, modified, size, bundled, err in rows:
            v = "UNREADABLE (%s)" % err if err else verdict(modified, bundled, repo_sha)
            if fn in HELD_BACK:
                v = "held back: " + v
            tally[v.split(" (")[0] if v.startswith("UNREADABLE") else v] = tally.get(v.split(" (")[0] if v.startswith("UNREADABLE") else v, 0) + 1
            mb += (size or 0) / 1e6
            if not v.startswith("ok"):
                bad.append((fn, v, modified))
        r.log("%d packages read (%.0f MB): %s" % (len(rows), mb, json.dumps(tally, sort_keys=True)))
        stamps = sorted((m or "")[:19] for _f, m, _s, _b, e in rows if not e and m)
        r.log("LastModified of the importers: earliest %s, latest %s; updated since the lane started (%s): %d" % (stamps[0] if stamps else "-", stamps[-1] if stamps else "-", LANE_STARTED, sum(1 for s in stamps if s >= LANE_STARTED)))
        for fn, v, modified in bad:
            r.log("  %-44s %s | modified %s" % (fn, v, (modified or "-")[:19]))
        not_ready = [fn for fn, v, _m in bad if fn not in HELD_BACK]
        r.kv(importers=len(names), carrying_new_reader=sum(1 for _f, m, _s, b, e in rows if not e and b == repo_sha), not_ready=len(not_ready), held_back=sorted(HELD_BACK))
        if not_ready:
            r.fail("%d importers do not carry the new reader: the one-document writer must not go live yet" % len(not_ready))
            raise SystemExit(1)
        r.ok("every importer except the one held back carries the reader that understands both layouts")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
