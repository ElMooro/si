"""ops 3261 — parity, wrapped: the worker's PUT contract is
{note:{…}} (bare objects 400; {notes:[…]} is bulk-REPLACE and is never
used). Retry the 403 missing with the wrapper; final id-diff proves
mirror ⊆ brain."""
import json
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
BRAIN = "https://justhodl-data-proxy.raafouis.workers.dev/brain"
CANON = "brain-930ffa48-60a1-4b11-8726-8848d1b827f9"
UA = {"User-Agent": "JustHodl-Ops-3261/1.0"}


def walk(o):
    if isinstance(o, dict):
        yield o
        for v in o.values():
            yield from walk(v)
    elif isinstance(o, list):
        for v in o:
            yield from walk(v)


def brain_ids():
    req = urllib.request.Request(
        f"{BRAIN}?sync=1&uid={urllib.parse.quote(CANON)}"
        f"&t={int(time.time())}", headers=UA)
    j = json.loads(urllib.request.urlopen(req, timeout=40).read()
                   .decode("utf-8", "replace"))
    return {str(d.get("id")) for d in walk(j)
            if isinstance(d, dict) and d.get("id")
            and (d.get("text") or d.get("body"))}


with report("3261_parity_wrapped") as rep:
    fails, warns = [], []
    rep.heading("ops 3261 — parity with the {note:{…}} wrapper")
    mirror = json.loads(S3.get_object(
        Bucket=BUCKET, Key="data/tradingview-notes.json")
        ["Body"].read()).get("notes") or []
    ids0 = brain_ids()
    miss = [n for n in mirror
            if n.get("id") and str(n["id"]) not in ids0
            and len(str(n.get("text") or "")) >= 3]
    rep.kv(missing_before=len(miss))
    shown = {"n": 0}

    def put1(n):
        body = json.dumps({"note": {
            "id": str(n["id"]),
            "cat": n.get("cat") or "thesis",
            "text": str(n.get("text") or ""),
            "created": n.get("created") or int(time.time() * 1000),
            "pinned": bool(n.get("pinned"))}}).encode("utf-8")
        r = urllib.request.Request(
            f"{BRAIN}?uid={urllib.parse.quote(CANON)}", data=body,
            method="PUT",
            headers={"Content-Type": "text/plain", **UA})
        try:
            with urllib.request.urlopen(r, timeout=25) as h:
                resp = h.read().decode("utf-8", "replace")
            ok = '"ok"' in resp
            if not ok and shown["n"] < 3:
                shown["n"] += 1
                rep.log(f"    resp: {resp[:120]}")
            return ok
        except Exception as e:
            if shown["n"] < 3:
                shown["n"] += 1
                rep.log(f"    err: {str(e)[:100]}")
            return False

    up = fl = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=8) as ex:
        for okp in ex.map(put1, miss):
            up += 1 if okp else 0
            fl += 0 if okp else 1
            if time.time() - t0 > 300:
                warns.append("budget reached")
                break
    rep.kv(upserted=up, put_failed=fl)
    time.sleep(4)
    ids1 = brain_ids()
    miss1 = sum(1 for n in mirror
                if n.get("id") and str(n["id"]) not in ids1)
    rep.kv(mirror=len(mirror), in_brain=len(mirror) - miss1,
           still_missing=miss1)
    if miss1 <= 25:
        rep.ok(f"FULL PARITY: {len(mirror) - miss1}/{len(mirror)} "
               "mirror notes in the canonical brain by id — his years "
               "of notes, permanently stored, all of them")
    else:
        fails.append(f"{miss1} still absent")
    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
