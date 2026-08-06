"""justhodl-backfill-orchestrator — E10 v1 (ops 4455).

One bounded backfill unit per run, progress-tracked, rate-limit-polite —
the machine that deepens every warm archive over nights instead of one
giant pull. v1 worklist: TGA operating cash (fiscaldata page[number]
pagination; E7 loaded 2022->now, this walks pages backward to the
dataset's start). Each run: fetch next older page, MERGE into
data/warm/treasury/tga_operating_cash.json.gz (dedupe by date), advance
cursor in data/audit/backfill-progress.json. When a source is exhausted it
says COMPLETE and stops — no infinite polling. Framework: add worklist
entries per archive (nyfed, cftc, fred) in later passes."""
import gzip
import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
try:
    from raw_snapshot import snapshot
except Exception:
    snapshot = None

PROG_KEY = "data/audit/backfill-progress.json"
BASE = ("https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
        "/v1/accounting/dts/operating_cash_balance")
FILTER = ("?filter=account_type:eq:Treasury General Account (TGA) "
          "Closing Balance&sort=-record_date&page[size]=2500")


def _get(k, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception:
        return default


def _get_gz(k):
    try:
        import io
        b = s3.get_object(Bucket=BUCKET, Key=k)["Body"].read()
        return json.loads(gzip.decompress(b))
    except Exception:
        return None


def lambda_handler(event, context):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    prog = _get(PROG_KEY, {"tasks": {}})
    task = prog["tasks"].get("tga_deep") or {
        "dataset": "tga_operating_cash", "next_page": 2,
        "status": "running", "pages_done": 1,
        "note": "page 1 = E7's live pull; walking older pages"}
    if task.get("status") == "COMPLETE":
        res = {"ok": True, "tga_deep": "COMPLETE", "skipped": True}
        print(json.dumps(res))
        return {"statusCode": 200, "body": json.dumps(res)}
    page = task["next_page"]
    url = (BASE + FILTER.replace(" ", "%20")
           + f"&page[number]={page}")
    added = 0
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "JustHodl research admin@justhodl.ai"})
        with urllib.request.urlopen(req, timeout=60) as r:
            raw = r.read()
        if snapshot:
            snapshot("treasury", url, raw)
        data = (json.loads(raw).get("data") or [])
        if not data:
            task["status"] = "COMPLETE"
            task["completed_at"] = now
        else:
            warm = _get_gz("data/warm/treasury/"
                           "tga_operating_cash.json.gz") or \
                {"dataset": "tga_operating_cash",
                 "unit": "USD millions", "observations": []}
            have = {o["date"] for o in warm.get("observations", [])}
            for o in data:
                d = o.get("record_date")
                try:
                    v = float(o.get("open_today_bal"))
                except (TypeError, ValueError):
                    continue
                if d and d not in have:
                    warm["observations"].append({"date": d, "value": v})
                    added += 1
            warm["observations"].sort(key=lambda x: x["date"])
            warm["n_obs"] = len(warm["observations"])
            warm["span"] = (f"{warm['observations'][0]['date']}.."
                            f"{warm['observations'][-1]['date']}"
                            if warm["observations"] else None)
            warm["backfill_note"] = (f"E10 orchestrator: {task['pages_done']}"
                                     f"+1 pages merged as of {now}")
            s3.put_object(Bucket=BUCKET,
                          Key="data/warm/treasury/"
                              "tga_operating_cash.json.gz",
                          Body=gzip.compress(
                              json.dumps(warm).encode()),
                          ContentType="application/gzip")
            task["next_page"] = page + 1
            task["pages_done"] = task.get("pages_done", 1) + 1
            task["last_span"] = warm["span"]
            task["n_obs_total"] = warm["n_obs"]
    except Exception as e:
        task["last_error"] = f"{type(e).__name__}: {str(e)[:80]}"
    task["updated_at"] = now
    prog["tasks"]["tga_deep"] = task
    prog["as_of"] = now
    s3.put_object(Bucket=BUCKET, Key=PROG_KEY,
                  Body=json.dumps(prog, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True, "page_fetched": page, "rows_added": added,
           "status": task.get("status"),
           "n_obs_total": task.get("n_obs_total"),
           "span": task.get("last_span")}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
