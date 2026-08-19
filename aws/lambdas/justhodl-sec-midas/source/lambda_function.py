"""justhodl-sec-midas — v1.0 (ops 4909).

The last zero-key provider on the board. Banks SEC MIDAS market-
structure quarterly datasets: individual_security_exchange_{Y}_q{Q}.zip
discovered by parsing SEC's dataset page (ops 4908 hop3 evidence,
16+ real links, path variant changed at 2024_q1) with a URL-pattern
fallback for both path variants. Streams each zip to /tmp (they run
tens-to-hundreds of MB) and stores it verbatim under the registry
prefix data/warm/sec-midas/raw/ (deny-Delete protected), newest
quarter first, then backfills history. State + manifest, self-chain
while missing (playbook 2.6), weekly Scheduler as watchdog + new-
quarter detector. Honest UA per SEC fair-access policy.
"""
import gzip
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com"}
PAGE = ("https://www.sec.gov/data-research/sec-markets-data/"
        "market-structure-data-security-exchange")
PATHS = ("/files/opa/data/market-structure/"
         "metrics-individual-security-exchange/",
         "/files/opa/data/market-structure/"
         "metrics-individual-security-and-exchange/")
OUT = "data/warm/sec-midas/raw"
STATE_KEY = "data/_state/sec-midas.json"
BUDGET_S = int(os.environ.get("MIDAS_BUDGET_S", "820"))
PER_RUN = 2
TMP_CAP = int(6.5 * 1024 * 1024 * 1024)

s3 = boto3.client("s3", region_name="us-east-1")


def _j(key, default=None):
    try:
        b = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if b[:2] == b"\x1f\x8b":
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return default


def _put_json(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _discover():
    """Quarter inventory: page-parse primary, pattern probe fallback.
    Returns {(year, q): url} newest-first."""
    inv = {}
    try:
        req = urllib.request.Request(PAGE, headers=UA)
        txt = urllib.request.urlopen(req, timeout=45).read(
            2500000).decode("utf-8", "ignore")
        for m in re.finditer(
                r'href="([^"]*individual_security_exchange_'
                r'(\d{4})_q([1-4])\.zip)"', txt, re.I):
            u = m.group(1)
            if u.startswith("/"):
                u = "https://www.sec.gov" + u
            inv[(int(m.group(2)), int(m.group(3)))] = u
    except Exception:
        pass
    if not inv:  # pattern fallback across both path variants
        yr = datetime.now(timezone.utc).year
        for y in range(2012, yr + 1):
            for q in (1, 2, 3, 4):
                for base in PATHS:
                    u = ("https://www.sec.gov%s"
                         "individual_security_exchange_%d_q%d.zip"
                         % (base, y, q))
                    try:
                        rq = urllib.request.Request(
                            u, headers=UA, method="HEAD")
                        with urllib.request.urlopen(rq, timeout=15):
                            inv[(y, q)] = u
                            break
                    except Exception:
                        continue
    return dict(sorted(inv.items(), reverse=True))


def _stream_zip(url, deadline):
    tmp = "/tmp/midas.zip"
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=300) as r, \
                open(tmp, "wb") as f:
            n = 0
            while True:
                c = r.read(8 * 1024 * 1024)
                if not c:
                    break
                f.write(c)
                n += len(c)
                if n >= TMP_CAP or time.time() > deadline:
                    return None, n
        return tmp, n
    except urllib.error.HTTPError as e:
        return f"err:HTTP{e.code}", 0
    except Exception as e:
        return f"err:{type(e).__name__}", 0


def _write_manifest(state):
    have = state.get("have") or {}
    _put_json("data/warm/sec-midas/manifest.json", {
        "as_of": _now(), "engine": "justhodl-sec-midas",
        "n_quarters": len(have),
        "quarters": sorted(have.keys(), reverse=True),
        "total_bytes": sum(v.get("bytes") or 0
                           for v in have.values()),
        "note": ("individual_security_exchange quarterly zips, "
                 "verbatim from sec.gov; page-parsed inventory + "
                 "pattern fallback; deny-Delete prefix")})


def lambda_handler(event, context):
    t0 = time.time()
    state = _j(STATE_KEY, {}) or {}
    if float(state.get("lease_until") or 0) > time.time():
        return {"statusCode": 200,
                "body": json.dumps({"skipped": "lease_held"})}
    state.setdefault("have", {})
    state["lease_until"] = time.time() + BUDGET_S + 120
    _put_json(STATE_KEY, state)

    inv = _discover()
    state["inventory_n"] = len(inv)
    missing = [(yq, u) for yq, u in inv.items()
               if ("%d_q%d" % yq) not in state["have"]]
    did = []
    for (y, q), url in missing[:PER_RUN]:
        if time.time() - t0 > BUDGET_S - 60:
            break
        qid = "%d_q%d" % (y, q)
        deadline = min(t0 + BUDGET_S - 45, time.time() + 700)
        tmp, nb = _stream_zip(url, deadline)
        if tmp and not str(tmp).startswith("err:"):
            key = f"{OUT}/individual_security_exchange_{qid}.zip"
            s3.upload_file(tmp, BUCKET, key, ExtraArgs={
                "ContentType": "application/zip",
                "Metadata": {"engine": "sec-midas",
                             "quarter": qid}})
            try:
                os.remove(tmp)
            except Exception:
                pass
            state["have"][qid] = {"bytes": nb, "key": key,
                                  "at": _now()}
            did.append(f"{qid}:ok:{nb}")
        else:
            fl = state.setdefault("failures", {})
            fl[qid] = {"err": str(tmp), "bytes": nb,
                       "tries": (fl.get(qid) or {}).get("tries", 0)
                       + 1, "at": _now()}
            did.append(f"{qid}:{tmp}")
        _put_json(STATE_KEY, dict(
            state, lease_until=time.time() + BUDGET_S + 120))

    _write_manifest(state)
    n_missing = max(0, len(inv) - len(state["have"]))
    _depth = int((event or {}).get("chain_depth") or 0)
    _chain = bool(n_missing and did and _depth < 40
                  and not (event or {}).get("no_chain"))
    state["lease_until"] = 0
    state["as_of"] = _now()
    state["n_have"] = len(state["have"])
    state["n_missing"] = n_missing
    _put_json(STATE_KEY, state)
    if _chain:
        try:
            boto3.client("lambda", region_name="us-east-1").invoke(
                FunctionName=os.environ.get(
                    "AWS_LAMBDA_FUNCTION_NAME",
                    "justhodl-sec-midas"),
                InvocationType="Event",
                Payload=json.dumps(
                    {"chain_depth": _depth + 1}).encode())
        except Exception as e:
            print("chain failed: %s" % type(e).__name__)
            _chain = False
    res = {"ok": True, "inventory": len(inv),
           "have": len(state["have"]), "missing": n_missing,
           "chained": _chain, "actions": did[:4],
           "elapsed_s": round(time.time() - t0, 1)}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
