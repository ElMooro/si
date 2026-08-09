"""justhodl-fred-catalog (ops 4552) — Khalid: what about FRED's remaining
data? FRED has no discovery mechanism (unlike the SDMX four which publish
a flat dataflow list) — it's a CATEGORY TREE, ~800k+ series total.

Two-phase, budget-bounded, resumable:
  Phase 1: recursive category-tree crawl (few thousand nodes, fast,
           cheap) -> data/_state/fred-categories.json
  Phase 2: per-category series-LIST banking (metadata only: id, title,
           freq, units, popularity, obs range — NOT full observation
           histories yet, that's a distinct, much bigger phase decided
           only after Phase 1 gives Khalid a real total to react to)
           -> data/warm/fred-catalog/series-meta/page-NNNN.json

ADDITIVE ONLY: writes to brand-new prefixes, never touches the existing
298 curated canary-macro keys.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
s3 = boto3.client("s3", region_name="us-east-1")
BUDGET_S = 220
PAGE = 500


def _get(url, tries=3):
    for i in range(tries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent":
                              "JustHodl research admin@justhodl.ai"})
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read())
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(1.5)


def lambda_handler(event, context):
    t0 = time.time()
    phase = (event or {}).get("phase", "categories")
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    if phase == "categories":
        # ---- Phase 1: recursive category-tree crawl ----
        try:
            st = json.loads(s3.get_object(
                Bucket=BUCKET,
                Key="data/_state/fred-categories.json")["Body"].read())
        except Exception:
            st = {"visited": [], "frontier": [0], "cats": {}}
        visited = set(st["visited"])
        frontier = st["frontier"] or [0]
        while frontier and time.time() - t0 < BUDGET_S:
            cid = frontier.pop(0)
            if cid in visited:
                continue
            try:
                d = _get(
                    "https://api.stlouisfed.org/fred/category/children"
                    f"?category_id={cid}&api_key={FRED_KEY}"
                    "&file_type=json")
                kids = d.get("categories", [])
                for k in kids:
                    st["cats"][str(k["id"])] = {
                        "name": k.get("name"),
                        "parent_id": k.get("parent_id")}
                    if k["id"] not in visited:
                        frontier.append(k["id"])
            except Exception as e:
                st.setdefault("errors", {})[str(cid)] = str(e)[:80]
            visited.add(cid)
        st["visited"] = sorted(visited)
        st["frontier"] = frontier
        st["updated_at"] = now
        st["n_categories"] = len(st["cats"])
        st["status"] = "COMPLETE" if not frontier else "walking"
        s3.put_object(Bucket=BUCKET,
                      Key="data/_state/fred-categories.json",
                      Body=json.dumps(st, default=str).encode(),
                      ContentType="application/json")
        res = {"phase": "categories", "n_categories": st["n_categories"],
              "frontier_left": len(frontier), "status": st["status"]}
        print(json.dumps(res))
        return {"statusCode": 200, "body": json.dumps(res)}

    # ---- Phase 2: per-category series-list metadata banking ----
    try:
        cst = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/_state/fred-categories.json")["Body"].read())
    except Exception:
        return {"statusCode": 400,
                "body": json.dumps({"error": "run phase=categories "
                                             "first"})}
    try:
        pst = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/_state/fred-series-meta.json")["Body"].read())
    except Exception:
        pst = {"cats_done": [], "n_series": 0, "n_pages": 0,
              "buffer": []}
    cat_ids = [int(c) for c in cst.get("cats", {}).keys()]
    todo = [c for c in cat_ids if c not in pst["cats_done"]]
    buf = pst.get("buffer", [])
    done_this_run = 0
    for cid in todo:
        if time.time() - t0 > BUDGET_S:
            break
        try:
            offset = 0
            while True:
                d = _get(
                    "https://api.stlouisfed.org/fred/category/series"
                    f"?category_id={cid}&api_key={FRED_KEY}"
                    f"&file_type=json&limit=1000&offset={offset}")
                serieslist = d.get("seriess", [])
                for s in serieslist:
                    buf.append({
                        "id": s.get("id"), "title": s.get("title"),
                        "freq": s.get("frequency_short"),
                        "units": s.get("units_short"),
                        "seasonal_adj": s.get("seasonal_adjustment_short"),
                        "popularity": s.get("popularity"),
                        "obs_start": s.get("observation_start"),
                        "obs_end": s.get("observation_end"),
                        "last_updated": s.get("last_updated"),
                        "category_id": cid,
                        "source_url": ("https://fred.stlouisfed.org/"
                                       "series/" + str(s.get("id"))),
                    })
                    if len(buf) >= PAGE:
                        s3.put_object(
                            Bucket=BUCKET,
                            Key=("data/warm/fred-catalog/series-meta/"
                                 f"page-{pst['n_pages']:04d}.json"),
                            Body=json.dumps({"page": pst["n_pages"],
                                            "count": len(buf),
                                            "rows": buf},
                                           default=str).encode(),
                            ContentType="application/json",
                            CacheControl="no-cache")
                        pst["n_pages"] += 1
                        pst["n_series"] += len(buf)
                        buf = []
                if len(serieslist) < 1000:
                    break
                offset += 1000
            pst["cats_done"].append(cid)
            done_this_run += 1
        except Exception as e:
            pst.setdefault("errors", {})[str(cid)] = str(e)[:80]
        if time.time() - t0 > BUDGET_S:
            break
    pst["buffer"] = buf
    pst["updated_at"] = now
    s3.put_object(Bucket=BUCKET, Key="data/_state/fred-series-meta.json",
                  Body=json.dumps(pst, default=str).encode(),
                  ContentType="application/json")
    manifest = {
        "provider": "fred", "updated_at": now,
        "categories_total": len(cat_ids),
        "categories_parsed": len(pst["cats_done"]),
        "series_discovered": pst["n_series"] + len(buf),
        "n_pages": pst["n_pages"], "page_size": PAGE,
        "note": ("Metadata only (id/title/freq/units/popularity/date "
                "range) — full observation histories are a separate, "
                "much larger phase, not yet started."),
    }
    s3.put_object(Bucket=BUCKET,
                  Key="data/providers/fred/catalog-manifest.json",
                  Body=json.dumps(manifest, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"phase": "series_meta", "cats_processed_this_run":
          done_this_run, **manifest}
    print(json.dumps({k: res[k] for k in
                      ("cats_processed_this_run", "series_discovered",
                       "categories_parsed", "categories_total")}))
    return {"statusCode": 200, "body": json.dumps(res, default=str)}
