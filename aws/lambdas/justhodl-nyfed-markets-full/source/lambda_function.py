"""justhodl-nyfed-markets-full — registry worklist item 2 (ops 4463).

Beyond E8's five reference rates: the rest of the NY Fed Markets API that
the council unanimously flagged (0.8% share vs plumbing-platform thesis).
v1 pulls the bounded JSON endpoints — SOMA holdings summary, repo &
reverse-repo operation results, securities lending — and DISCOVERS the
Primary Dealer timeseries catalog (candidate-chain) storing it as the
cursor worklist for 100% PD convergence via hourly tranches. Explicit
failures; F4 snapshots; keyless."""
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

BASE = "https://markets.newyorkfed.org/api"
BOUNDED = {
    "soma_summary": "/soma/summary.json",
    "repo_ops_latest": "/rp/all/all/results/latest.json",
    # ops 4502: real shapes vary — candidate lists, first-hit wins
    "seclending_latest": ["/seclending/all/results/latest.json",
                          "/seclending/all/results/lastTwoWeeks.json",
                          "/seclending/all/all/results/latest.json"],
    "ambs_latest": ["/ambs/all/all/results/latest.json",
                    "/ambs/all/results/latest.json",
                    "/ambs/all/all/results/lastTwoWeeks.json"],
}
PD_CATALOG_CANDS = ["/pd/list/timeseries.json",
                    "/pd/list/seriesbreaks.json"]
TRANCHE = int(os.environ.get("PD_TRANCHE", "20"))


def _fetch(path):
    req = urllib.request.Request(BASE + path, headers={
        "User-Agent": "JustHodl research admin@justhodl.ai"})
    with urllib.request.urlopen(req, timeout=45) as r:
        return r.read()


def lambda_handler(event, context):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    summary = {"as_of": now, "bounded": {}, "pd": {}}
    for name, path in BOUNDED.items():
        cands = path if isinstance(path, list) else [path]
        last = "none"
        for cp in cands:
            try:
                raw = _fetch(cp)
                d = json.loads(raw)
                rk = (snapshot("nyfed", BASE + cp, raw)
                      if snapshot else None)
                s3.put_object(Bucket=BUCKET,
                              Key=f"data/warm/nyfed-markets/{name}"
                                  f".json.gz",
                              Body=gzip.compress(json.dumps(
                                  {"endpoint": cp, "as_of": now,
                                   "raw_snapshot_key": rk,
                                   "payload": d}).encode()),
                              ContentType="application/gzip")
                summary["bounded"][name] = {"ok": True,
                                            "endpoint": cp,
                                            "bytes": len(raw)}
                break
            except Exception as e:
                last = (f"{cp} -> {type(e).__name__}: "
                        f"{str(e)[:50]}")
        else:
            summary["bounded"][name] = {"data_unavailable": True,
                                        "reason": last}
    # PD catalog discovery + cursored tranche
    state_key = "data/warm/nyfed-markets/pd-state.json"
    try:
        state = json.loads(s3.get_object(
            Bucket=BUCKET, Key=state_key)["Body"].read())
    except Exception:
        state = {"catalog": None, "done": []}
    # ── v2 (Khalid: "1,500 series but only 5MB"): /pd/get/{k}.json
    # returns ONLY the current seriesbreak (~3KB gz — the whole 5MB
    # mystery). Full FR2004 history = every break via
    # /pd/get/{brk}/timeseries/{k}.json. hist_v=2 resets done[] once so
    # all keys re-pull deep; hourly tranches converge in ~6-7h.
    if state.get("hist_v") != 2:
        state["hist_v"] = 2
        state["done"] = []
        state["failures"] = {}
        state["status"] = "converging-v2-full-history"
    if not state.get("seriesbreaks"):
        try:
            sb = json.loads(_fetch("/pd/list/seriesbreaks.json"))
            labs = []
            for lst in (sb.get("pd") or {}).values():
                if isinstance(lst, list):
                    for x in lst:
                        if isinstance(x, dict):
                            lab = (x.get("label") or x.get("keyid")
                                   or x.get("seriesbreak"))
                            if lab:
                                labs.append(str(lab))
            state["seriesbreaks"] = sorted(set(labs)) or None
        except Exception as e:
            state["seriesbreaks_note"] = ("%s: %s"
                                          % (type(e).__name__,
                                             str(e)[:60]))
    if not state.get("catalog"):
        for cand in PD_CATALOG_CANDS:
            try:
                d = json.loads(_fetch(cand))
                ts = (d.get("pd", {}).get("timeseries")
                      or d.get("timeseries") or [])
                keys = sorted({(x.get("keyid") or x.get("key")
                                or x.get("timeseries"))
                               for x in ts if isinstance(x, dict)} - {None})
                if len(keys) > 20:
                    state["catalog"] = keys
                    state["catalog_source"] = cand
                    break
            except Exception as e:
                state["catalog_note"] = (f"{cand}: {type(e).__name__}: "
                                         f"{str(e)[:60]}")
    got = 0
    if state.get("catalog"):
        todo = [k for k in state["catalog"]
                if k not in set(state["done"])][:TRANCHE]
        breaks = state.get("seriesbreaks") or []
        for k in todo:
            try:
                rows, used = [], []
                for brk in breaks:
                    try:
                        d2 = json.loads(_fetch(
                            f"/pd/get/{brk}/timeseries/{k}.json"))
                        r2 = ((d2.get("pd") or {}).get("timeseries")
                              or [])
                        if r2:
                            rows += r2
                            used.append(brk)
                        time.sleep(0.15)
                    except Exception:
                        continue
                if not rows:
                    # fallback: the bare path (current break only) —
                    # better than nothing, honestly labeled
                    d2 = json.loads(_fetch(f"/pd/get/{k}.json"))
                    rows = ((d2.get("pd") or {}).get("timeseries")
                            or [])
                    used = ["<current-only>"]
                seen, dedup = set(), []
                for r2 in rows:
                    ad = r2.get("asofdate")
                    if ad and ad not in seen:
                        seen.add(ad)
                        dedup.append(r2)
                dedup.sort(key=lambda x: str(x.get("asofdate")))
                s3.put_object(
                    Bucket=BUCKET,
                    Key=f"data/warm/nyfed-markets/pd/{k}.json.gz",
                    Body=gzip.compress(json.dumps(
                        {"series": k, "as_of": now, "hist_v": 2,
                         "breaks_used": used,
                         "n_obs": len(dedup),
                         "first": (dedup[0].get("asofdate")
                                   if dedup else None),
                         "last": (dedup[-1].get("asofdate")
                                  if dedup else None),
                         "rows": dedup}).encode()),
                    ContentType="application/gzip")
                state["done"].append(k)
                got += 1
            except Exception as e:
                state.setdefault("failures", {})[k] = \
                    f"{type(e).__name__}: {str(e)[:50]}"
            time.sleep(0.2)
        n = len(state["catalog"])
        nd = len(set(state["done"]))
        state["progress_pct"] = round(100 * nd / n, 1) if n else 0
        state["status"] = ("COMPLETE-maintaining" if nd >= n
                           else "converging")
    state["as_of"] = now
    s3.put_object(Bucket=BUCKET, Key=state_key,
                  Body=json.dumps(state, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    summary["pd"] = {"catalog": len(state.get("catalog") or []),
                     "catalog_source": state.get("catalog_source"),
                     "pulled_this_run": got,
                     "done_total": len(set(state.get("done", []))),
                     "progress_pct": state.get("progress_pct")}
    s3.put_object(Bucket=BUCKET,
                  Key="data/warm/nyfed-markets/latest-summary.json",
                  Body=json.dumps(summary, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True,
           "bounded_ok": [k for k, v in summary["bounded"].items()
                          if v.get("ok")],
           "bounded_failed": [k for k, v in summary["bounded"].items()
                              if v.get("data_unavailable")],
           "pd": summary["pd"]}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
