"""justhodl-dol-full v1.0.0 -- COMPLETE US DOL ETA report corpus.

Khalid priority lane #4 (4962 evidence: 70 report CSVs on
oui.doleta.gov/unemploy/DataDownloads.asp vs the board's 6 curated
keys). Small corpus, full-fidelity doctrine:

  harvest  the DataDownloads page each run -> every .csv href
           (self-extending when DOL posts new reports)
  mirror   verbatim streamed; conditional HEAD Last-Modified+Length
           vs stored metadata (405/no-HEAD -> unconditional GET,
           corpus is tiny); failures NAMED 3-strike
  single-run scale (~70 files) -- no chain needed; rate(6h) schedule
State data/warm/dol-full/_state/state.json; manifest -> dol-note-v2.
"""
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

ENGINE_VERSION = "justhodl-dol-full v1.0.0 ops4966 corpus"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
PAGE = "https://oui.doleta.gov/unemploy/DataDownloads.asp"
SITE = "https://oui.doleta.gov"
ROOT = "data/warm/dol-full/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com)"}
BUDGET_S = 640
SPACING = 0.2

s3 = boto3.client("s3", region_name="us-east-1")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _j(key, default=None):
    try:
        return json.loads(
            s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def _put_json(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, indent=1).encode(),
                  ContentType="application/json")


def resolve(lk):
    if lk.startswith("http"):
        return lk
    if lk.startswith("/"):
        return SITE + lk
    return SITE + "/unemploy/" + lk


def harvest():
    req = urllib.request.Request(PAGE, headers=UA)
    with urllib.request.urlopen(req, timeout=60) as r:
        html = r.read(5_000_000).decode("utf-8", "replace")
    links = sorted(set(re.findall(r'href="([^"]+\.csv)"', html,
                                  re.I)))
    return {re.sub(r"[^A-Za-z0-9._-]+", "_",
                   lk.rsplit("/", 1)[-1]): resolve(lk)
            for lk in links}


def head(url):
    try:
        req = urllib.request.Request(url, headers=UA,
                                     method="HEAD")
        with urllib.request.urlopen(req, timeout=40) as r:
            return (r.headers.get("Last-Modified") or "",
                    int(r.headers.get("Content-Length") or 0))
    except Exception:
        return None, None


def mirror(state, name, url):
    key = ROOT + "src/" + name
    lm, ln = head(url)
    if lm:
        try:
            h = s3.head_object(Bucket=BUCKET, Key=key)
            md = h.get("Metadata") or {}
            if md.get("src_lm") == lm and \
                    str(md.get("src_len")) == str(ln):
                state["have"][name] = {"bytes": ln, "lm": lm,
                                       "status": "unchanged",
                                       "url": url}
                return True
        except Exception:
            pass
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=180) as r:
            nl = int(r.headers.get("Content-Length") or 0)
            nlm = r.headers.get("Last-Modified") or lm or ""
            s3.upload_fileobj(
                r, BUCKET, key,
                ExtraArgs={"ContentType": "text/csv",
                           "Metadata": {"engine": "dol-full",
                                        "src_lm": nlm,
                                        "src_len": str(nl or ln
                                                       or 0)}})
        state["have"][name] = {"bytes": nl or ln or 0,
                               "lm": nlm, "status": "fresh",
                               "url": url}
        state["failures"].pop(name, None)
        return True
    except Exception as e:
        fl = state["failures"]
        tries = (fl.get(name) or {}).get("tries", 0) + 1
        fl[name] = {"err": str(e)[:80], "tries": tries}
        return tries >= 3


def write_manifest(state):
    have = state.get("have") or {}
    _put_json(MANIFEST_KEY, {
        "as_of": _now(), "engine": "justhodl-dol-full",
        "version": "1.0.0",
        "files": len(have),
        "bytes": sum(v.get("bytes") or 0 for v in have.values()),
        "fresh": sum(1 for v in have.values()
                     if v.get("status") == "fresh"),
        "unchanged": sum(1 for v in have.values()
                         if v.get("status") == "unchanged"),
        "failures": len(state.get("failures") or {}),
        "note": ("complete DOL ETA DataDownloads corpus mirrored "
                 "verbatim; page-harvest self-extends; conditional "
                 "Last-Modified refresh")})


def lambda_handler(event, ctx=None):
    t0 = time.time()
    state = _j(STATE_KEY, None) or {"version": "1.0.0",
                                    "have": {}, "failures": {}}
    if float(state.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held"}
    state["lease_until"] = time.time() + BUDGET_S + 120
    _put_json(STATE_KEY, state)
    try:
        uni = harvest()
        state["universe_n"] = len(uni)
    except Exception as e:
        state["failures"]["_harvest"] = {"err": str(e)[:100]}
        uni = {n: (state["have"].get(n) or {}).get("url")
               for n in state.get("have", {})}
        uni = {n: u for n, u in uni.items() if u}
    done = 0
    for name, url in sorted(uni.items()):
        if time.time() - t0 > BUDGET_S - 30:
            state["failures"]["_budget"] = {
                "err": "stopped after %d" % done}
            break
        mirror(state, name, url)
        done += 1
        if done % 15 == 0:
            _put_json(STATE_KEY, state)
        time.sleep(SPACING)
    state["lease_until"] = 0
    state["as_of"] = _now()
    state["n_files"] = len(state["have"])
    state["bytes_total"] = sum(v.get("bytes") or 0
                               for v in state["have"].values())
    _put_json(STATE_KEY, state)
    write_manifest(state)
    return {"ok": True, "files": state["n_files"],
            "mb": round(state["bytes_total"] / 1e6, 2),
            "universe": state.get("universe_n"),
            "elapsed_s": round(time.time() - t0, 1)}
