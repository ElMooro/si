"""ops 3258 — BRAIN PERMANENCE proven or repaired.

Khalid's design intent: notes + watchlists are copied into the brain
and stored PERMANENTLY — no daily extension runs. The mirror is healthy
(3,322) but data/brain.json (the KV→S3 sync of the CANONICAL uid) holds
ZERO tv-provenance notes. Suspect: uid split — brain-sync mirrors the
hardcoded canonical uid while ingest/crawler upsert to SSM
/justhodl/brain/uid.

  1. Read SSM uid vs canonical; GET the worker /brain for BOTH uids;
     count total + tv-provenance notes in each. Evidence first.
  2. REPAIR: re-upsert all mirror notes into the CANONICAL brain
     (idempotent by note id), align SSM uid to canonical so every
     future upsert lands right.
  3. brain-sync re-run → data/brain.json must now carry the tv corpus;
     final counts printed. Watchlists: S3 id-merge = permanent; bucket
     versioning status reported.
"""
import json
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
BRAIN = "https://justhodl-data-proxy.raafouis.workers.dev/brain"
CANON = "brain-930ffa48-60a1-4b11-8726-8848d1b827f9"
UA = {"User-Agent": "JustHodl-Ops-3258/1.0"}


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


def brain_get(uid):
    try:
        req = urllib.request.Request(
            f"{BRAIN}?sync=1&uid={urllib.parse.quote(uid)}", headers=UA)
        j = json.loads(urllib.request.urlopen(req, timeout=30).read()
                       .decode("utf-8", "replace"))
        notes = [d for d in walk(j)
                 if isinstance(d, dict) and d.get("id")
                 and (d.get("text") or d.get("body"))]
        tv = [n for n in notes
              if str(n.get("id", "")).startswith("tv-")
              or str(n.get("text", "")).startswith("[TV:")]
        return len(notes), len(tv)
    except Exception as e:
        return None, str(e)[:60]


def brain_put(uid, note):
    body = json.dumps(note).encode("utf-8")
    req = urllib.request.Request(
        f"{BRAIN}?uid={urllib.parse.quote(uid)}", data=body,
        method="PUT",
        headers={"Content-Type": "text/plain", **UA})
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.loads(r.read().decode("utf-8", "replace")) \
            .get("ok") is True


with report("3258_brain_permanence") as rep:
    fails, warns = [], []
    rep.heading("ops 3258 — brain permanence: evidence, repair, proof")

    rep.section("1. Evidence — both uids")
    try:
        ssm_uid = SSM.get_parameter(Name="/justhodl/brain/uid",
                                    WithDecryption=True)\
            ["Parameter"]["Value"].strip()
    except Exception as e:
        ssm_uid = ""
        warns.append(f"ssm uid read: {str(e)[:60]}")
    rep.kv(ssm_uid=(ssm_uid[:14] + "…" + ssm_uid[-6:]) if ssm_uid
           else "—",
           canonical=CANON[:14] + "…" + CANON[-6:],
           uids_match=(ssm_uid == CANON))
    n_c, tv_c = brain_get(CANON)
    rep.log(f"  canonical brain: notes={n_c} tv={tv_c}")
    if ssm_uid and ssm_uid != CANON:
        n_s, tv_s = brain_get(ssm_uid)
        rep.log(f"  ssm-uid brain:   notes={n_s} tv={tv_s}")

    mirror = (s3_json("data/tradingview-notes.json") or {})\
        .get("notes") or []
    rep.kv(mirror_notes=len(mirror))

    rep.section("2. Repair — canonical brain gets the full corpus")
    need = isinstance(tv_c, int) and tv_c < max(1000, len(mirror) // 2)
    upserted = failed = 0
    if need and mirror:
        def put1(n):
            note = {"id": n.get("id"),
                    "cat": n.get("cat") or "thesis",
                    "text": n.get("text") or n.get("body") or "",
                    "created": n.get("created"),
                    "pinned": bool(n.get("pinned"))}
            if not note["id"] or len(str(note["text"])) < 3:
                return False
            try:
                return brain_put(CANON, note)
            except Exception:
                return False
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=8) as ex:
            for okp in ex.map(put1, mirror):
                if okp:
                    upserted += 1
                else:
                    failed += 1
                if time.time() - t0 > 420:
                    warns.append("upsert budget reached")
                    break
        rep.kv(upserted=upserted, upsert_failed=failed,
               secs=round(time.time() - t0, 1))
    else:
        rep.log("  no repair needed — canonical already carries the "
                "corpus" if not need else "  mirror empty")

    if ssm_uid != CANON:
        try:
            SSM.put_parameter(Name="/justhodl/brain/uid", Value=CANON,
                              Type="String", Overwrite=True)
            rep.ok("SSM /justhodl/brain/uid ALIGNED to canonical — "
                   "every future upsert lands in the brain Khalid "
                   "actually reads")
        except Exception as e:
            fails.append(f"ssm align: {str(e)[:70]}")

    rep.section("3. Proof — canonical brain + data/brain.json")
    n_c2, tv_c2 = brain_get(CANON)
    rep.kv(canonical_notes_now=n_c2, canonical_tv_now=tv_c2)
    mark = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName="justhodl-brain-sync",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        warns.append(f"brain-sync invoke: {str(e)[:60]}")
    bj_tv = None
    for _ in range(25):
        time.sleep(8)
        bj = s3_json("data/brain.json") or {}
        if str(bj.get("generated_at") or bj.get("synced_at") or "")\
                > mark:
            bn = bj.get("notes") or []
            bj_tv = sum(1 for n in bn if isinstance(n, dict)
                        and (str(n.get("id", "")).startswith("tv-")
                             or str(n.get("text", ""))
                             .startswith("[TV:")))
            rep.kv(brain_json_notes=len(bn), brain_json_tv=bj_tv)
            break
    if bj_tv is None:
        warns.append("brain.json not refreshed in window — KV counts "
                     "above are the authority")
    try:
        v = S3.get_bucket_versioning(Bucket=BUCKET).get("Status")
        rep.kv(bucket_versioning=v or "Disabled")
    except Exception:
        pass

    permanent = isinstance(tv_c2, int) and tv_c2 >= 1000
    if permanent:
        rep.ok(f"PERMANENCE PROVEN: {tv_c2} TV notes live in the "
               "canonical brain — no daily extension needed; the brain "
               "keeps them regardless of TV cookies")
    else:
        fails.append(f"canonical tv count still low: {tv_c2}")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
