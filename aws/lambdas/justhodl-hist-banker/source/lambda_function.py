"""justhodl-hist-banker — v1.0 (ops 4910).

Three thin-history providers get their full archives (Khalid:
expedite these). One engine, three lanes, the MIDAS-proven skeleton:
inventory -> stream-to-/tmp -> bank verbatim under each provider's
registry prefix (deny-Delete) -> manifest -> self-chain while missing
-> weekly Scheduler as watchdog + new-item detector.

  dera   : SEC DERA Financial Statement Data Sets, quarterly zips
           2009q1 -> now. Page-parse primary, deterministic pattern
           fallback (/files/dera/data/financial-statement-data-sets/
           {Y}q{Q}.zip). -> data/warm/sec-dera/hist/
  edgar  : EDGAR full-index master.idx per quarter, 1993Q1 -> now,
           deterministic URLs, gzip-compressed on bank.
           -> data/warm/edgar-filings/full-index/
  eiopa  : EIOPA Solvency II RFR monthly term-structure zips,
           page-parsed (any .zip href carrying a 20xx date token).
           -> data/warm/eiopa/hist/

Honest UA (SEC fair-access). Per-item wall-clock deadline (the ops
4908 slow-drip trap). Failures ledgered with tries, never silent.
"""
import gzip
import json
import os
import re
import shutil
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com"}
STATE_KEY = "data/_state/hist-banker.json"
BUDGET_S = int(os.environ.get("HB_BUDGET_S", "820"))
PER_RUN = 5
TMP_CAP = int(6.0 * 1024 * 1024 * 1024)
DERA_PAGE = ("https://www.sec.gov/data-research/sec-markets-data/"
             "financial-statement-data-sets")
DERA_BASE = ("https://www.sec.gov/files/dera/data/"
             "financial-statement-data-sets/")
EIOPA_PAGE = ("https://www.eiopa.europa.eu/tools-and-data/"
              "risk-free-interest-rate-term-structures_en")

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


def _page(url, nb=2500000):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=45).read(nb).decode(
        "utf-8", "ignore")


def _head_ok(url):
    try:
        rq = urllib.request.Request(url, headers=UA, method="HEAD")
        with urllib.request.urlopen(rq, timeout=15):
            return True
    except Exception:
        return False


def inv_dera():
    inv = {}
    try:
        txt = _page(DERA_PAGE)
        for m in re.finditer(r'href="([^"]*?(\d{4}q[1-4])\.zip)"',
                             txt, re.I):
            u = m.group(1)
            if u.startswith("/"):
                u = "https://www.sec.gov" + u
            inv[m.group(2).lower()] = u
    except Exception:
        pass
    if len(inv) < 10:
        now = datetime.now(timezone.utc)
        for y in range(2009, now.year + 1):
            for q in (1, 2, 3, 4):
                qid = f"{y}q{q}"
                if qid in inv:
                    continue
                u = f"{DERA_BASE}{qid}.zip"
                if _head_ok(u):
                    inv[qid] = u
    return inv


def inv_edgar():
    now = datetime.now(timezone.utc)
    cq = (now.month - 1) // 3 + 1
    inv = {}
    for y in range(1993, now.year + 1):
        for q in (1, 2, 3, 4):
            if y == now.year and q > cq:
                break
            inv[f"{y}-QTR{q}"] = ("https://www.sec.gov/Archives/"
                                  f"edgar/full-index/{y}/QTR{q}/"
                                  "master.idx")
    return inv


def inv_eiopa():
    inv = {}
    try:
        txt = _page(EIOPA_PAGE)
        for m in re.finditer(r'href="([^"]+\.zip)"', txt, re.I):
            u = m.group(1)
            if "20" not in u:
                continue
            if u.startswith("/"):
                u = "https://www.eiopa.europa.eu" + u
            inv[u.rsplit("/", 1)[-1][:120]] = u
    except Exception:
        pass
    return inv


LANES = {
    "dera": {"inv": inv_dera, "out": "data/warm/sec-dera/hist",
             "gz": False, "ext": ".zip"},
    "edgar": {"inv": inv_edgar,
              "out": "data/warm/edgar-filings/full-index",
              "gz": True, "ext": ".master.idx.gz"},
    "eiopa": {"inv": inv_eiopa, "out": "data/warm/eiopa/hist",
              "gz": False, "ext": ""},
}


def _bank(url, key, gz, deadline):
    tmp = "/tmp/hb.bin"
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=240) as r, \
                open(tmp, "wb") as f:
            n = 0
            while True:
                c = r.read(8 * 1024 * 1024)
                if not c:
                    break
                f.write(c)
                n += len(c)
                if n >= TMP_CAP or time.time() > deadline:
                    return "slow_or_big", n
        up = tmp
        if gz:
            up = "/tmp/hb.gz"
            with open(tmp, "rb") as fi, gzip.open(
                    up, "wb", compresslevel=6) as fo:
                shutil.copyfileobj(fi, fo, length=8 * 1024 * 1024)
        s3.upload_file(up, BUCKET, key, ExtraArgs={
            "Metadata": {"engine": "hist-banker"}})
        for p in (tmp, "/tmp/hb.gz"):
            try:
                os.remove(p)
            except Exception:
                pass
        return "ok", n
    except urllib.error.HTTPError as e:
        return f"err:HTTP{e.code}", 0
    except Exception as e:
        return f"err:{type(e).__name__}", 0


def _manifest(lane, L, have):
    _put_json(f"{L['out']}/manifest.json", {
        "as_of": _now(), "engine": "justhodl-hist-banker",
        "lane": lane, "n_items": len(have),
        "items": sorted(have.keys(), reverse=True)[:400],
        "total_bytes": sum(v.get("bytes") or 0
                           for v in have.values())})


def lambda_handler(event, context):
    t0 = time.time()
    state = _j(STATE_KEY, {}) or {}
    if float(state.get("lease_until") or 0) > time.time():
        return {"statusCode": 200,
                "body": json.dumps({"skipped": "lease_held"})}
    state.setdefault("lanes", {})
    state["lease_until"] = time.time() + BUDGET_S + 120
    _put_json(STATE_KEY, state)

    did, missing_total = [], 0
    for lane, L in LANES.items():
        if time.time() - t0 > BUDGET_S - 90:
            break
        ls = state["lanes"].setdefault(lane, {"have": {},
                                              "failures": {}})
        try:
            inv = L["inv"]()
        except Exception as e:
            did.append(f"{lane}:inv-err:{type(e).__name__}")
            continue
        ls["inventory_n"] = len(inv)
        missing = [(k, u) for k, u in sorted(inv.items(),
                                             reverse=True)
                   if k not in ls["have"]
                   and (ls["failures"].get(k) or {}
                        ).get("tries", 0) < 3]
        missing_total += len(missing)
        for k, url in missing[:PER_RUN]:
            if time.time() - t0 > BUDGET_S - 60:
                break
            key = "%s/%s%s" % (L["out"],
                               k if not k.endswith(L["ext"])
                               else k, "" if k.endswith(
                                   (".zip", ".gz")) else L["ext"])
            dl = min(t0 + BUDGET_S - 45, time.time() + 420)
            status, nb = _bank(url, key, L["gz"], dl)
            if status == "ok":
                ls["have"][k] = {"bytes": nb, "key": key,
                                 "at": _now()}
                ls["failures"].pop(k, None)
                did.append(f"{lane}:{k}:ok:{nb}")
            else:
                fl = ls["failures"].setdefault(k, {"tries": 0})
                fl.update(err=status, tries=fl["tries"] + 1,
                          at=_now())
                did.append(f"{lane}:{k}:{status}")
            _put_json(STATE_KEY, dict(
                state, lease_until=time.time() + BUDGET_S + 120))
        _manifest(lane, L, ls["have"])
        ls["n_have"] = len(ls["have"])

    _depth = int((event or {}).get("chain_depth") or 0)
    still_missing = sum(
        max(0, (ls.get("inventory_n") or 0) - len(ls.get("have")
                                                  or {})
            - len([1 for f in (ls.get("failures") or {}).values()
                   if f.get("tries", 0) >= 3]))
        for ls in state["lanes"].values())
    _chain = bool(still_missing and did and _depth < 60
                  and not (event or {}).get("no_chain"))
    state["lease_until"] = 0
    state["as_of"] = _now()
    state["still_missing"] = still_missing
    _put_json(STATE_KEY, state)
    if _chain:
        try:
            boto3.client("lambda", region_name="us-east-1").invoke(
                FunctionName=os.environ.get(
                    "AWS_LAMBDA_FUNCTION_NAME",
                    "justhodl-hist-banker"),
                InvocationType="Event",
                Payload=json.dumps(
                    {"chain_depth": _depth + 1}).encode())
        except Exception as e:
            print("chain failed: %s" % type(e).__name__)
            _chain = False
    res = {"ok": True, "chained": _chain,
           "still_missing": still_missing,
           "lanes": {k: {"inv": v.get("inventory_n"),
                         "have": v.get("n_have")}
                     for k, v in state["lanes"].items()},
           "actions": did[:6],
           "elapsed_s": round(time.time() - t0, 1)}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
