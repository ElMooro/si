#!/usr/bin/env python3
"""Stop the company-book publish dying at the 300s Lambda limit.

The first invoke of the new code ran FRED, the ECB book and TRACE, then
started the fund files, and the task was killed at 300.00s before it wrote
data/corp-bonds.json. Start the holdings downloads immediately so they overlap
those feeds, and stop a slow body from trickling past its deadline. The
function timeout moves from 300 to 600 so a slow day still writes the packet.
"""
import hashlib
from pathlib import Path

SRC = Path("aws/lambdas/justhodl-corp-bonds/source/lambda_function.py")
CFG = Path("aws/lambdas/justhodl-corp-bonds/config.json")
SRC_SHA = "dd9f5c722ebbc81475c124d6aa179a2a4ec42b6af61d6cfbd3ea8b21948435a8"

OLD_IMPORT = "import json\nimport os\n"
NEW_IMPORT = "import json\nimport os\nimport time\n"

OLD_GET = '''def _get_bytes(url, timeout=45):
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": "text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()
'''
NEW_GET = '''def _get_bytes(url, timeout=25):
    # urlopen's timeout resets on every socket read, so a trickled file can
    # outlive it. The monotonic deadline is the real cap, plus one blocked read.
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": "text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
    })
    deadline = time.monotonic() + timeout
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        chunks, total = [], 0
        while True:
            if time.monotonic() > deadline:
                raise TimeoutError(url)
            block = resp.read(1 << 16)
            if not block:
                break
            total += len(block)
            if total > 25000000:
                raise TimeoutError("oversized")
            chunks.append(block)
        return b"".join(chunks)
'''

OLD_PUB = '''    prior_bonds = ((prior.get("issues") or {}).get("bonds")) or []
    sleeves = []
'''
NEW_PUB = '''    prior_bonds = ((prior.get("issues") or {}).get("bonds")) or []
    # Holdings files overlap FRED, the euro book and TRACE. They were sequential
    # before, and that sum crossed the 300s task limit before the packet was written.
    issue_pool = ThreadPoolExecutor(max_workers=1)
    issue_future = issue_pool.submit(fetch_issues, prior_bonds)
    sleeves = []
'''

OLD_JOIN = '''    try:
        issue_book = fetch_issues(prior_bonds)
        sources["issues"] = {row["id"]: "%s %s" % (row["status"], row["n"]) for row in issue_book.get("funds") or []}
    except Exception as exc:
        issue_book = None
        sources["issues"] = "error %s" % type(exc).__name__
'''
NEW_JOIN = '''    try:
        issue_book = issue_future.result()
        sources["issues"] = {row["id"]: "%s %s" % (row["status"], row["n"]) for row in issue_book.get("funds") or []}
    except Exception as exc:
        issue_book = None
        sources["issues"] = "error %s" % type(exc).__name__
    finally:
        issue_pool.shutdown(wait=False, cancel_futures=True)
'''


def main():
    src = SRC.read_text(encoding="utf-8")
    if "issue_pool = ThreadPoolExecutor" in src and "time.monotonic()" in src:
        print("publish budget already in the handler")
    else:
        got = hashlib.sha256(src.encode()).hexdigest()
        if got != SRC_SHA:
            raise SystemExit("publish budget: handler hash " + got)
        for label, old, new in (
            ("import", OLD_IMPORT, NEW_IMPORT),
            ("get", OLD_GET, NEW_GET),
            ("start", OLD_PUB, NEW_PUB),
            ("join", OLD_JOIN, NEW_JOIN),
        ):
            if src.count(old) != 1:
                raise SystemExit("publish budget: %s needle count %s" % (label, src.count(old)))
            src = src.replace(old, new, 1)
        compile(src, str(SRC), "exec")
        SRC.write_text(src, encoding="utf-8")
        print("handler overlaps the fund downloads")
    cfg = CFG.read_text(encoding="utf-8")
    if '"timeout": 600' in cfg:
        print("timeout already 600")
    elif cfg.count('"timeout": 300') != 1:
        raise SystemExit("publish budget: timeout needle missing")
    else:
        CFG.write_text(cfg.replace('"timeout": 300', '"timeout": 600', 1), encoding="utf-8")
        print("timeout 300 -> 600")


if __name__ == "__main__":
    main()
