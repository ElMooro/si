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
TRANCHE = int(os.environ.get("PD_TRANCHE", "150"))


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
    # ── v3 (Khalid, 2nd report of the 5MB freeze): the v2 reconvergence
    # ran under rev-2's label URLs — every historical break 404'd into
    # the silent continue, every key fell to the <current-only> fallback
    # and was marked done SHALLOW. rev-3 fixed the URLs but never bumped
    # hist_v, so nothing ever re-queued (ops 4602, the depth proof, died
    # on a sync-invoke read timeout before it could catch this).
    # hist_v=3 resets done[] once; hourly tranches re-pull deep.
    if state.get("hist_v") != 3:
        state["hist_v"] = 3
        state["done"] = []
        state["failures"] = {}
        state["shallow"] = []
        state["shallow_n"] = 0
        state["depth"] = None
        state["status"] = "converging-v3-full-history"
    # rev-4: rev-3 GUESSED the id field and zero deep files ever landed,
    # so the guess is unproven. Discovery now takes every candidate
    # string a seriesbreaks entry carries and keeps only ids that pass a
    # LIVE probe fetch (probe-then-wire): a real break id returns
    # parseable JSON for a known key; labels and wrong fields 404 (or
    # InvalidURL on spaces) out of the list. Validated set cached once.
    if (state.get("seriesbreaks_v") != 3
            or not state.get("seriesbreaks")
            or any(" " in str(b) for b in state["seriesbreaks"])):
        try:
            sb = json.loads(_fetch("/pd/list/seriesbreaks.json"))
            cands = []
            for lst in (sb.get("pd") or {}).values():
                if isinstance(lst, list):
                    for x in lst:
                        if isinstance(x, dict):
                            for v in x.values():
                                v = str(v)
                                if v and v.lower() != "none" \
                                        and len(v) < 40:
                                    cands.append(v)
            ordered = sorted(set(cands),
                             key=lambda z: (" " in z, len(z), z))
            probes_k = [(state.get("catalog") or ["PDABTOT"])[0],
                        "PDPOSGS-B"]
            valid, probes = [], {}
            for c in ordered[:24]:
                verdict = None
                for pk in probes_k:
                    try:
                        json.loads(_fetch(
                            "/pd/get/%s/timeseries/%s.json" % (c, pk)))
                        verdict = "ok"
                        break
                    except Exception as e2:
                        verdict = type(e2).__name__
                    time.sleep(0.12)
                probes[c] = verdict
                if verdict == "ok":
                    valid.append(c)
            state["seriesbreaks"] = valid or None
            state["seriesbreaks_probe"] = probes
            state["seriesbreaks_v"] = 3
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
    # v2 lease: hourly schedule + manual kicks must not race the cursor
    # (done[] last-write-wins would lose progress)
    if (state.get("lease_until") or 0) > time.time():
        state["skipped"] = "lease_held"
        s3.put_object(Bucket=BUCKET, Key=state_key,
                      Body=json.dumps(state, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
        return {"pd": "lease_held"}
    state["lease_until"] = time.time() + 620
    state.pop("skipped", None)
    if state.get("catalog"):
        # depth ledger backfill (once): keys converged before the ledger
        # existed get counted from their own stored docs
        if state.get("done") and not state.get("depth"):
            dep = {"keys": 0, "n_obs_sum": 0, "first_min": None,
                   "multi": 0, "ge500": 0}
            for k in list(state["done"]):
                if context.get_remaining_time_in_millis() < 120000:
                    break
                try:
                    d0 = json.loads(gzip.decompress(s3.get_object(
                        Bucket=BUCKET,
                        Key=f"data/warm/nyfed-markets/pd/{k}.json.gz"
                    )["Body"].read()))
                    dep["keys"] += 1
                    dep["n_obs_sum"] += int(d0.get("n_obs") or 0)
                    f0 = d0.get("first")
                    if f0 and (dep["first_min"] is None
                               or str(f0) < str(dep["first_min"])):
                        dep["first_min"] = str(f0)
                    if len(d0.get("breaks_used") or []) >= 2:
                        dep["multi"] += 1
                    if int(d0.get("n_obs") or 0) >= 500:
                        dep["ge500"] += 1
                except Exception:
                    continue
            state["depth"] = dep
        todo = [k for k in state["catalog"]
                if k not in set(state["done"])][:TRANCHE]
        breaks = state.get("seriesbreaks") or []
        if not breaks:
            todo = []   # blocked-honest: retry discovery next run
        for k in todo:
            if context.get_remaining_time_in_millis() < 90000:
                state["budget_break"] = ("stopped after %d this run "
                                         "(90s headroom kept)" % got)
                break
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
                    sh = state.setdefault("shallow", [])
                    if len(sh) < 50:
                        sh.append(k)
                    state["shallow_n"] = state.get("shallow_n", 0) + 1
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
                        {"series": k, "as_of": now, "hist_v": 3,
                         "breaks_used": used,
                         "n_obs": len(dedup),
                         "first": (dedup[0].get("asofdate")
                                   if dedup else None),
                         "last": (dedup[-1].get("asofdate")
                                  if dedup else None),
                         "rows": dedup}).encode()),
                    ContentType="application/gzip")
                dep = state.get("depth") or {
                    "keys": 0, "n_obs_sum": 0, "first_min": None,
                    "multi": 0, "ge500": 0}
                dep["keys"] += 1
                dep["n_obs_sum"] += len(dedup)
                f0 = dedup[0].get("asofdate") if dedup else None
                if f0 and (dep["first_min"] is None
                           or str(f0) < str(dep["first_min"])):
                    dep["first_min"] = str(f0)
                if len(used) >= 2:
                    dep["multi"] += 1
                if len(dedup) >= 500:
                    dep["ge500"] += 1
                state["depth"] = dep
                state["done"].append(k)
                got += 1
            except Exception as e:
                state.setdefault("failures", {})[k] = \
                    f"{type(e).__name__}: {str(e)[:50]}"
            time.sleep(0.2)
        n = len(state["catalog"])
        nd = len(set(state["done"]))
        state["progress_pct"] = round(100 * nd / n, 1) if n else 0
        state["status"] = ("blocked-no-valid-breaks" if not breaks
                           else "COMPLETE-maintaining" if nd >= n
                           else "converging-v3")
    state["as_of"] = now
    state["lease_until"] = 0
    s3.put_object(Bucket=BUCKET, Key=state_key,
                  Body=json.dumps(state, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    summary["pd"] = {"catalog": len(state.get("catalog") or []),
                     "catalog_source": state.get("catalog_source"),
                     "pulled_this_run": got,
                     "done_total": len(set(state.get("done", []))),
                     "progress_pct": state.get("progress_pct"),
                     "depth": (dict(state["depth"],
                                    mean_n_obs=round(
                                        state["depth"]["n_obs_sum"]
                                        / max(1, state["depth"]["keys"]),
                                        1))
                               if state.get("depth") else None)}
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
