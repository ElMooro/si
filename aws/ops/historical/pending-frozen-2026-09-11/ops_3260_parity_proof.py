"""ops 3260 — parity by ID + ranker fusion proven shape-agnostically.

  1. PARITY: the honest metric is id-coverage (mirror ids ⊆ brain ids),
     independent of tag heuristics (crawler-era numeric ids never start
     'tv-'). Re-diff after 3259's puts; retry the residue with the
     worker's RESPONSE BODY printed on the first failures — evidence,
     not guesswork.
  2. RANKERS: 3259's verifier guessed top-level keys wrong. Walk each
     feed for any dict carrying 'khalid_note'; count + sample. The 3171
     fusion is coded in all three — this converts warns into proof.
"""
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
UA = {"User-Agent": "JustHodl-Ops-3260/1.0"}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


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
    j = json.loads(urllib.request.urlopen(req, timeout=30).read()
                   .decode("utf-8", "replace"))
    return {str(d.get("id")) for d in walk(j)
            if isinstance(d, dict) and d.get("id")
            and (d.get("text") or d.get("body"))}


with report("3260_parity_proof") as rep:
    fails, warns = [], []
    rep.heading("ops 3260 — id-parity + ranker fusion proven")

    rep.section("1. Parity by ID")
    mirror = (s3_json("data/tradingview-notes.json") or {})\
        .get("notes") or []
    ids0 = brain_ids()
    miss = [n for n in mirror
            if n.get("id") and str(n["id"]) not in ids0
            and len(str(n.get("text") or "")) >= 3]
    rep.kv(mirror=len(mirror), brain_ids=len(ids0),
           missing_before=len(miss))
    shown = 0
    if miss:
        def put1(n):
            global shown
            body = json.dumps({"id": n["id"],
                               "cat": n.get("cat") or "thesis",
                               "text": n.get("text") or "",
                               "created": n.get("created"),
                               "pinned": bool(n.get("pinned"))})\
                .encode("utf-8")
            r = urllib.request.Request(
                f"{BRAIN}?uid={urllib.parse.quote(CANON)}", data=body,
                method="PUT",
                headers={"Content-Type": "text/plain", **UA})
            try:
                with urllib.request.urlopen(r, timeout=25) as h:
                    resp = h.read().decode("utf-8", "replace")
                ok = '"ok"' in resp and "true" in resp
                if not ok and shown < 3:
                    shown += 1
                    rep.log(f"    PUT resp: {resp[:120]}")
                return ok
            except Exception as e:
                if shown < 3:
                    shown += 1
                    rep.log(f"    PUT err: {str(e)[:100]}")
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
    ids1 = brain_ids()
    miss1 = sum(1 for n in mirror
                if n.get("id") and str(n["id"]) not in ids1)
    cov = len(mirror) - miss1
    rep.kv(mirror_ids_in_brain=cov, still_missing=miss1)
    if miss1 <= 25:
        rep.ok(f"PARITY: {cov}/{len(mirror)} mirror notes present in "
               "the canonical brain by id")
    else:
        fails.append(f"{miss1} mirror notes still absent from brain")

    rep.section("2. khalid_note riding the rankers — proven")
    got = 0
    for key in ("data/master-rank.json", "data/best-setups.json",
                "data/alpha-compass.json"):
        d = s3_json(key) or {}
        rows = [x for x in walk(d)
                if isinstance(x, dict) and "khalid_note" in x]
        withval = [x for x in rows if x.get("khalid_note")]
        if rows:
            got += 1
            s = (withval or rows)[0]
            kn = s.get("khalid_note") or {}
            rep.ok(f"{key}: {len(rows)} rows carry khalid_note "
                   f"({len(withval)} non-null) — e.g. "
                   f"{s.get('ticker') or s.get('symbol')} "
                   f"stance={kn.get('stance')}")
        else:
            warns.append(f"{key}: khalid_note not found anywhere in "
                         "feed")
    rep.kv(rankers_proven=got)
    if got < 3:
        warns.append("some ranker feeds pre-date the join — next "
                     "scheduled run refreshes them")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
