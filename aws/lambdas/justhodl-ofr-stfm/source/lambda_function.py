"""justhodl-ofr-stfm — council unanimous #1 (ops 4462).

OFR Short-Term Funding Monitor: free, keyless, ~442 mnemonics across repo /
primary-dealer / MMF / rates / yields — THE primary dataset for a funding-
plumbing platform, at 0% share until today. 100%-of-provider pattern:
(1) discover the full mnemonic catalog (candidate-chain over metadata
endpoints — never assume shape), (2) pull FULL history for a bounded
tranche per run (cursor in the doc itself), (3) converge to every mnemonic
over runs, COMPLETE-and-maintain. Explicit failures; F4 snapshots.

ops 4759: also converges the sibling Hedge Fund Monitor catalog
(/hf/v1, 497+ mnemonics, discovered ops 4752) with its own state at
data/warm/ofr-hfm/state.json -- new HFM series auto-bank, and the
held set refreshes on a rolling 10-per-run cursor once complete.

2026-11-21: the short-term-funding bank itself had no refresh at all - a mnemonic in state.done was never fetched
again, so every series file stayed as written on 2026-08-06 and the repo board that reads them stopped there
(ops 6893).  Every run now refreshes the whole bank from the five dataset calls (ofr_refresh), writing only the
series whose observations changed, and the sponsored-repo pair of the hedge-fund catalog the same way.
{"dry_run": true} asks the source, compares with the state and writes nothing."""
import gzip
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
try:
    from raw_snapshot import snapshot
except Exception:
    snapshot = None

from ofr_refresh import DATASETS, HF_DATASETS, record, refresh

BASE = "https://data.financialresearch.gov/v1"
HF_BASE = "https://data.financialresearch.gov/hf/v1"
STATE_KEY = "data/warm/ofr/state.json"
HF_STATE_KEY = "data/warm/ofr-hfm/state.json"
SERIES_PREFIX = "data/warm/ofr/series/"
HF_SERIES_PREFIX = "data/warm/ofr-hfm/series/"
TRANCHE = int(os.environ.get("TRANCHE", "30"))
SEED = ["REPO-TRI_AR_OO-P", "REPO-TRI_TV_OO-P", "REPO-DVP_AR_OO-P",
        "REPO-GCF_AR_OO-P", "MMF-MMF_RP_TOT-M", "NYPD-PD_RP_TOT-W",
        "FNYR-SOFR-A", "FNYR-EFFR-A", "FNYR-BGCR-A", "FNYR-TGCR-A"]


def _fetch(url):
    req = urllib.request.Request(url, headers={
        "User-Agent": "JustHodl research admin@justhodl.ai"})
    with urllib.request.urlopen(req, timeout=45) as r:
        return r.read()


def _fetch_dataset(url):
    """A whole dataset (up to ~10 MB) needs more than the 45 seconds one series gets."""
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl research (OFR bank refresh)"})
    with urllib.request.urlopen(req, timeout=150) as r:
        return r.read()


def _put_series(key, body):
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/gzip")


def _put_hf_series(key, body):
    """The hedge-fund catalog's files have always been stored as gzip-encoded JSON; a refreshed file keeps that."""
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json", ContentEncoding="gzip")


def _state(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return {"done": [], "catalog": None}


def dry_run(now):
    """What a run would write, read from the source and the two state files; nothing is written."""
    stfm = refresh(_fetch_dataset, None, dict(_state(STATE_KEY).get("series") or {}), BASE, DATASETS, now,
                   SERIES_PREFIX, dry_run=True)
    hf = refresh(_fetch_dataset, None, dict(_state(HF_STATE_KEY).get("series") or {}), HF_BASE, HF_DATASETS, now,
                 HF_SERIES_PREFIX, layout="raw", dry_run=True)
    res = {"ok": True, "dry_run": True, "refresh": record(stfm), "would_write": stfm["written_ids"],
           "hf": {"refresh": record(hf), "would_write": hf["written_ids"]}}
    print(json.dumps({"dry_run": True, "would_write": stfm["written"], "unchanged": stfm["unchanged"],
                      "hf_would_write": hf["written"]}))
    return {"statusCode": 200, "body": json.dumps(res)}


def _catalog():
    """Candidate-chain discovery of the full mnemonic list."""
    cands = [f"{BASE}/metadata/mnemonics",
             f"{BASE}/metadata/query?output=by_mnemonic",
             f"{BASE}/metadata/search?query="]
    for cu in cands:
        try:
            raw = _fetch(cu)
            d = json.loads(raw)
            mns = []
            if isinstance(d, list):
                mns = [x if isinstance(x, str) else
                       (x.get("mnemonic") or x.get("id"))
                       for x in d]
            elif isinstance(d, dict):
                mns = list(d.keys()) or \
                    [x.get("mnemonic") for x in
                     (d.get("mnemonics") or d.get("results") or [])
                     if isinstance(x, dict)]
            mns = sorted({m for m in mns if m and isinstance(m, str)})
            if len(mns) > 50:
                return mns, cu, None
        except Exception as e:
            last = f"{cu} -> {type(e).__name__}: {str(e)[:60]}"
    return SEED, "seed-fallback", last


def lambda_handler(event, context):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if isinstance(event, dict) and event.get("dry_run"):
        return dry_run(now)
    state_key = STATE_KEY
    try:
        state = json.loads(s3.get_object(
            Bucket=BUCKET, Key=state_key)["Body"].read())
    except Exception:
        state = {"done": [], "catalog": None}
    # rev (Khalid: repo completeness is critical): discovery used to run
    # ONCE and cache forever — mnemonics OFR added later (the NCCBR
    # haircut releases are recent) were never discovered. Now every run
    # rediscovers and MERGES; new mnemonics enter the worklist
    # automatically, and repo-family names drain first.
    mns, src, err = _catalog()
    prev = set(state.get("catalog") or [])
    if mns:
        newly = sorted(set(mns) - prev)
        state["catalog"] = sorted(prev | set(mns))
        state["catalog_source"] = src
        if newly:
            state["catalog_new_last_run"] = newly[:50]
            state["catalog_added_total"] = \
                (state.get("catalog_added_total") or 0) + len(newly)
    elif not state.get("catalog"):
        state["catalog"] = mns
        state["catalog_source"] = src
    if err and not mns:
        state["catalog_note"] = ("discovery fell back; "
                                 f"last err: {err}")
    state["catalog_checked_at"] = now
    _REPO_TAGS = ("REPO", "NCCBR", "TRI", "GCF", "DVP", "BILAT",
                  "HAIRCUT")
    todo = [m for m in (state.get("catalog") or [])
            if m not in set(state["done"])]
    todo.sort(key=lambda m: (0 if any(t in str(m).upper()
                                      for t in _REPO_TAGS) else 1,
                             str(m)))
    todo = todo[:TRANCHE]
    got = failed = 0
    for m in todo:
        url = f"{BASE}/series/full?mnemonic={m}"
        try:
            raw = _fetch(url)
            rk = snapshot("ofr", url, raw) if snapshot else None
            d = json.loads(raw)
            s3.put_object(Bucket=BUCKET,
                          Key=f"data/warm/ofr/series/{m}.json.gz",
                          Body=gzip.compress(json.dumps(
                              {"mnemonic": m, "source_url": url,
                               "raw_snapshot_key": rk, "as_of": now,
                               "payload": d}).encode()),
                          ContentType="application/gzip")
            state["done"].append(m)
            got += 1
        except Exception as e:
            failed += 1
            state.setdefault("failures", {})[m] = \
                f"{type(e).__name__}: {str(e)[:60]}"
        time.sleep(0.4)
    # the daily refresh: every series of the five datasets, written only where the observations changed
    ref = refresh(_fetch_dataset, _put_series, state.setdefault("series", {}), BASE, DATASETS, now, SERIES_PREFIX)
    state["done"] = sorted(set(state["done"]) | set(ref["written_ids"]))
    state["refresh"] = record(ref)
    n_cat = len(state["catalog"])
    n_done = len(set(state["done"]))
    state["as_of"] = now
    state["progress_pct"] = round(100 * n_done / n_cat, 1) if n_cat else 0
    state["status"] = ("COMPLETE-maintaining" if n_done >= n_cat
                       else "converging")
    s3.put_object(Bucket=BUCKET, Key=state_key,
                  Body=json.dumps(state, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    # ── ops 4759: sibling catalog /hf/v1 (Hedge Fund Monitor) ──────
    # ops 4752 discovered data.financialresearch.gov/hf/v1 (497
    # mnemonics, bigger than STFM) and one-shot banked it. Same
    # rediscover-merge-converge pattern here, own state, so new HFM
    # series auto-bank exactly like STFM's do, and the whole set
    # refreshes on a rolling tranche.
    hf_key = HF_STATE_KEY
    try:
        hf = json.loads(s3.get_object(
            Bucket=BUCKET, Key=hf_key)["Body"].read())
    except Exception:
        hf = {"done": [], "catalog": None}
    hf_pulled = hf_failed = 0
    try:
        d = json.loads(_fetch(f"{HF_BASE}/metadata/mnemonics"))
        mns_hf = []
        if isinstance(d, list):
            mns_hf = [x if isinstance(x, str) else
                      (x.get("mnemonic") or x.get("id")) for x in d]
        elif isinstance(d, dict):
            mns_hf = [x.get("mnemonic") for x in
                      (d.get("mnemonics") or d.get("results") or [])
                      if isinstance(x, dict)]
        mns_hf = sorted({m for m in mns_hf
                          if m and isinstance(m, str)})
        prev_hf = set(hf.get("catalog") or [])
        if mns_hf:
            newly_hf = sorted(set(mns_hf) - prev_hf)
            hf["catalog"] = sorted(prev_hf | set(mns_hf))
            if newly_hf:
                hf["catalog_new_last_run"] = newly_hf[:50]
    except Exception as e:
        hf["catalog_note"] = f"{type(e).__name__}: {str(e)[:60]}"
    hf_todo = [m for m in (hf.get("catalog") or [])
               if m not in set(hf["done"])]
    hf_todo = hf_todo[:int(os.environ.get("HF_TRANCHE", "25"))]
    if not hf_todo and hf.get("catalog"):
        # COMPLETE-maintaining: rolling refresh, oldest cursor first
        cur = hf.get("refresh_cursor") or 0
        cat = hf["catalog"]
        hf_todo = cat[cur:cur + 10]
        hf["refresh_cursor"] = (cur + 10) % max(1, len(cat))
        hf["done_refresh"] = True
    for m in hf_todo:
        url = f"{HF_BASE}/series/full?mnemonic={m}"
        try:
            raw = _fetch(url)
            json.loads(raw)
            s3.put_object(Bucket=BUCKET,
                          Key=f"data/warm/ofr-hfm/series/{m}.json.gz",
                          Body=gzip.compress(raw),
                          ContentType="application/json",
                          ContentEncoding="gzip")
            if m not in hf["done"]:
                hf["done"].append(m)
            hf_pulled += 1
        except Exception as e:
            hf_failed += 1
            hf.setdefault("failures", {})[m] = \
                f"{type(e).__name__}: {str(e)[:60]}"
        time.sleep(0.3)
    # sponsored repo is two series of one small dataset: refreshed every run, not once in fifty days
    hf_ref = refresh(_fetch_dataset, _put_hf_series, hf.setdefault("series", {}), HF_BASE, HF_DATASETS, now,
                     HF_SERIES_PREFIX, layout="raw")
    hf["done"] = sorted(set(hf["done"]) | set(hf_ref["written_ids"]))
    hf["refresh"] = record(hf_ref)
    hf["as_of"] = now
    n_hf_cat = len(hf.get("catalog") or [])
    hf["progress_pct"] = (round(100 * len(hf["done"]) / n_hf_cat, 1)
                           if n_hf_cat else 0)
    hf["status"] = ("COMPLETE-maintaining"
                     if n_hf_cat and len(hf["done"]) >= n_hf_cat
                     else "converging")
    s3.put_object(Bucket=BUCKET, Key=hf_key,
                  Body=json.dumps(hf, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True, "catalog": n_cat,
           "catalog_source": state.get("catalog_source"),
           "refresh": {k: state["refresh"][k] for k in ("written", "unchanged", "newest")},
           "refresh_problems": {"shrank": len(ref["shrank"]), "failed": len(ref["failed"]),
                                "datasets": {d: v["error"] for d, v in ref["datasets"].items() if v.get("error")}},
           "hf_refresh": {k: hf["refresh"][k] for k in ("written", "unchanged", "newest")},
           "pulled_this_run": got, "failed": failed,
           "done_total": n_done, "progress_pct": state["progress_pct"],
           "hf": {"catalog": n_hf_cat, "pulled": hf_pulled,
                  "failed": hf_failed, "done_total": len(hf["done"]),
                  "progress_pct": hf["progress_pct"],
                  "status": hf["status"]}}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
