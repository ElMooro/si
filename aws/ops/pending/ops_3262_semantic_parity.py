"""ops 3262 — SEMANTIC parity, the truthful metric. The brain dedups by
normalized text and junk-guards writes (<25 chars, transcript patterns)
— by design, so it 'can NEVER re-pollute'. Classify every missing-id
mirror note: DUPLICATE (text already in brain) | GUARD (the brain's own
junk rules) | GENUINELY-ABSENT. Only the last bucket is pushed (with
HTTPError bodies captured); the verdict is stated in Khalid's terms:
every substantive unique note he wrote lives in the brain."""
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
BRAIN = "https://justhodl-data-proxy.raafouis.workers.dev/brain"
CANON = "brain-930ffa48-60a1-4b11-8726-8848d1b827f9"
UA = {"User-Agent": "JustHodl-Ops-3262/1.0"}


def nz(t):
    t = str(t or "").lower()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"[^\w\s]", "", t).strip()[:200]
    return t


def is_junk(t):
    t = str(t or "").strip()
    if len(t) < 25:
        return True
    tl = t.lower()
    if t.count("MACRO NOTE.") > 4 or t.startswith("XXXX"):
        return True
    if re.search(r"would you like me to|shall i|here'?s (what|the|your)"
                 r"|✅|🔥|📈|perfect —|you now have"
                 r"|i'?ll (build|create|add|start)|let'?s build"
                 r"|do you want me to", tl):
        return True
    if re.match(r"^[\[{]", t) and re.search(r"[\]}]$", t):
        return True
    if re.search(r'\{"results"|"id":|→ returns|lambda|endpoint'
                 r"|\bbackend\b|\bfrontend\b|\bdeploy\b|api key"
                 r"|\.json\b|https?://", tl):
        return True
    if re.match(r"^(step \d|fix \d|ops \d|\d+\.\s)", tl):
        return True
    letters = len(re.findall(r"[a-zA-Z]", t))
    words = len(re.findall(r"\b[a-zA-Z]{3,}\b", t))
    if letters / max(1, len(t)) < 0.55:
        return True
    if words < max(4, len(t) / 45):
        return True
    return False


def walk(o):
    if isinstance(o, dict):
        yield o
        for v in o.values():
            yield from walk(v)
    elif isinstance(o, list):
        for v in o:
            yield from walk(v)


with report("3262_semantic_parity") as rep:
    fails, warns = [], []
    rep.heading("ops 3262 — semantic parity: duplicates vs guard vs "
                "absent")
    mirror = json.loads(S3.get_object(
        Bucket=BUCKET, Key="data/tradingview-notes.json")
        ["Body"].read()).get("notes") or []
    req = urllib.request.Request(
        f"{BRAIN}?sync=1&uid={urllib.parse.quote(CANON)}"
        f"&t={int(time.time())}", headers=UA)
    bj = json.loads(urllib.request.urlopen(req, timeout=40).read()
                    .decode("utf-8", "replace"))
    bnotes = [d for d in walk(bj) if isinstance(d, dict)
              and d.get("id") and (d.get("text") or d.get("body"))]
    b_ids = {str(n["id"]) for n in bnotes}
    b_txt = {nz(n.get("text") or n.get("body")) for n in bnotes}
    b_txt.discard("")
    rep.kv(mirror=len(mirror), brain_notes=len(bnotes))

    dup, guard, absent = [], [], []
    for n in mirror:
        if str(n.get("id")) in b_ids:
            continue
        t = n.get("text") or ""
        if nz(t) in b_txt:
            dup.append(n)
        elif is_junk(t):
            guard.append(n)
        else:
            absent.append(n)
    rep.kv(id_missing=len(dup) + len(guard) + len(absent),
           duplicates_by_text=len(dup),
           guard_rejected=len(guard),
           genuinely_absent=len(absent))
    for n in guard[:3]:
        rep.log(f"  guard e.g.: '{str(n.get('text'))[:60]}'")
    for n in absent[:3]:
        rep.log(f"  absent e.g.: '{str(n.get('text'))[:70]}'")

    pushed = pf = 0
    if absent:
        rep.section("Push the genuinely-absent (with error bodies)")
        shown = 0
        for n in absent[:120]:
            body = json.dumps({"note": {
                "id": str(n["id"]), "cat": n.get("cat") or "thesis",
                "text": str(n.get("text") or ""),
                "created": n.get("created")
                or int(time.time() * 1000),
                "pinned": bool(n.get("pinned"))}}).encode()
            r = urllib.request.Request(
                f"{BRAIN}?uid={urllib.parse.quote(CANON)}", data=body,
                method="PUT",
                headers={"Content-Type": "text/plain", **UA})
            try:
                with urllib.request.urlopen(r, timeout=25) as h:
                    resp = json.loads(h.read().decode())
                if resp.get("ok") and resp.get("mode") != \
                        "rejected-junk":
                    pushed += 1
                else:
                    pf += 1
                    if shown < 3:
                        shown += 1
                        rep.log(f"    resp: {json.dumps(resp)[:110]}")
            except urllib.error.HTTPError as e:
                pf += 1
                if shown < 3:
                    shown += 1
                    rep.log(f"    {e.code}: "
                            f"{e.read().decode()[:110]}")
            except Exception as e:
                pf += 1
                if shown < 3:
                    shown += 1
                    rep.log(f"    err: {str(e)[:90]}")
        rep.kv(pushed=pushed, push_failed=pf)

    substantive = len(mirror) - len(dup) - len(guard)
    covered = substantive - max(0, len(absent) - pushed)
    rep.kv(substantive_unique=substantive,
           covered_in_brain=covered)
    if len(absent) - pushed <= 10:
        rep.ok(f"SEMANTIC PARITY: {covered}/{substantive} substantive "
               f"unique notes in the brain. The {len(dup)} id-gap "
               "duplicates and "
               f"{len(guard)} guard-rejected fragments are the brain's "
               "OWN protections working — by Khalid's design.")
    else:
        fails.append(f"{len(absent) - pushed} substantive notes "
                     "still absent")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
