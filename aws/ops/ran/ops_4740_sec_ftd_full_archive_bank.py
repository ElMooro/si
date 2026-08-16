"""
ops/4740 -- SEC fails-to-deliver: bank the ENTIRE archive to warm.

justhodl-equity-ftd already proves the fetch+parse for SEC's CNS
fails-to-deliver files (cnsfails{YYYYMM}{a|b}.zip, pipe-delimited,
per-symbol quantities) -- but keeps only 6 half-month files. SEC's
archive reaches back to 2004. Khalid's requirement: ALL fails data,
ALL history, permanently on S3.

This op walks every half-month tag 2004-01a -> present, and for each:
  - skips it if already banked (idempotent -- reruns resume, never
    clobber; deny-Delete on data/warm/* makes the bank permanent)
  - fetches the zip from sec.gov with a compliant declared UA
  - extracts the single pipe-delimited member, gzips it RAW (model
    training wants raw rows, not my aggregation), stores at
    data/warm/sec-ftd/cnsfails{tag}.csv.gz
  - records rows/bytes in data/warm/sec-ftd/_manifest.json

404 tags are recorded, not papered over: if entire early years come
back 404 the report says so explicitly (pre-2009 files may use a
different naming scheme -- that gets chased against SEC's own index
page in a follow-up, not guessed here). Time-capped at 70 minutes;
if the cap hits, it stops cleanly and a rerun continues from the
manifest. SEC fair-use pacing: ~2 requests/second max, well under
their 10/s guideline.
"""
import gzip
import io
import json
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
PREFIX = "data/warm/sec-ftd/"
MANIFEST_KEY = PREFIX + "_manifest.json"
BASE = "https://www.sec.gov/files/data/fails-deliver-data/cnsfails{tag}.zip"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com"}
TIME_CAP_S = 70 * 60

s3 = boto3.client("s3", region_name=REGION)


def fetch(url, timeout=60):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def all_tags():
    now = datetime.now(timezone.utc)
    tags = []
    for y in range(2004, now.year + 1):
        for m in range(1, 13):
            if (y, m) > (now.year, now.month):
                break
            for half in ("a", "b"):
                tags.append(f"{y:04d}{m:02d}{half}")
    return tags


def main():
    started = time.time()
    with report("4740_sec_ftd_full_archive_bank") as rep:
        rep.heading("ops 4740 -- SEC CNS fails-to-deliver: full archive -> permanent warm")

        try:
            manifest = json.loads(
                s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)["Body"].read())
        except Exception:
            manifest = {"files": {}, "missing_404": []}
        manifest.setdefault("files", {})
        manifest.setdefault("missing_404", [])
        already = set(manifest["files"].keys())
        known_404 = set(manifest["missing_404"])

        tags = all_tags()
        rep.kv(check="tags_total", value=len(tags))
        rep.kv(check="already_banked", value=len(already))
        rep.kv(check="known_404", value=len(known_404))

        banked_now = 0
        err_now = 0
        new_404 = 0
        stopped_for_time = False
        for tag in tags:
            if tag in already or tag in known_404:
                continue
            if time.time() - started > TIME_CAP_S:
                stopped_for_time = True
                rep.warn(f"time cap reached at tag {tag} -- stopping cleanly; "
                         "rerun resumes from manifest")
                break
            url = BASE.format(tag=tag)
            try:
                raw = fetch(url)
            except urllib.error.HTTPError as e:
                if e.code in (403, 404):
                    manifest["missing_404"].append(tag)
                    new_404 += 1
                else:
                    err_now += 1
                    rep.log(f"  {tag}: HTTP {e.code}")
                time.sleep(0.4)
                continue
            except Exception as e:
                err_now += 1
                rep.log(f"  {tag}: {type(e).__name__}: {str(e)[:80]}")
                time.sleep(1.0)
                continue
            try:
                zf = zipfile.ZipFile(io.BytesIO(raw))
                member = zf.namelist()[0]
                text = zf.read(member)
                n_rows = text.count(b"\n")
                gz = gzip.compress(text)
                s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}cnsfails{tag}.csv.gz",
                               Body=gz, ContentType="text/csv",
                               ContentEncoding="gzip")
                manifest["files"][tag] = {
                    "rows": n_rows, "raw_bytes": len(text), "gz_bytes": len(gz),
                    "member": member,
                    "banked_at": datetime.now(timezone.utc).isoformat()}
                banked_now += 1
                if banked_now % 25 == 0:
                    rep.log(f"  progress: {banked_now} banked this run "
                            f"(latest {tag}, {n_rows} rows)")
                    # checkpoint manifest so a crash loses at most 25 files
                    s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY,
                                   Body=json.dumps(manifest, separators=(",", ":")),
                                   ContentType="application/json")
            except Exception as e:
                err_now += 1
                rep.log(f"  {tag}: parse/store {type(e).__name__}: {str(e)[:80]}")
            time.sleep(0.4)

        manifest["updated_at"] = datetime.now(timezone.utc).isoformat()
        s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY,
                       Body=json.dumps(manifest, separators=(",", ":")),
                       ContentType="application/json")

        files = manifest["files"]
        have_tags = sorted(files.keys())
        total_rows = sum(f.get("rows", 0) for f in files.values())
        total_gz_mb = round(sum(f.get("gz_bytes", 0) for f in files.values()) / 1e6, 1)
        m404 = sorted(manifest["missing_404"])
        # summarize 404s by year so pre-2009 naming gaps are explicit
        by_year = {}
        for t in m404:
            by_year[t[:4]] = by_year.get(t[:4], 0) + 1
        rep.section("Result")
        rep.kv(check="banked_this_run", value=banked_now)
        rep.kv(check="errors_this_run", value=err_now)
        rep.kv(check="new_404_this_run", value=new_404)
        rep.kv(check="total_files_banked", value=len(files))
        rep.kv(check="earliest_tag_banked", value=have_tags[0] if have_tags else None)
        rep.kv(check="latest_tag_banked", value=have_tags[-1] if have_tags else None)
        rep.kv(check="total_rows_banked", value=total_rows)
        rep.kv(check="total_gz_mb", value=total_gz_mb)
        rep.kv(check="stopped_for_time_cap", value=stopped_for_time)
        if by_year:
            rep.log("404 tags by year (naming-scheme gaps to chase against "
                    "SEC's own index page, NOT assumed absent): " +
                    ", ".join(f"{y}={n}" for y, n in sorted(by_year.items())))
        rep.ok(f"warm bank now holds {len(files)} half-month files, "
                f"{total_rows} raw rows, {total_gz_mb} MB gz -- under deny-Delete, "
                f"versioned, permanent")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("BANK ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
