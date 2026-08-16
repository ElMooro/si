"""
ops/4741 -- chase the pre-2017 FTD files via SEC's own index pages.

ops 4740 banked 209 half-month files (2017-06b -> 2026-07a, 11.33M
rows) and hit a perfectly clean 404 wall on 2004-2016 (exactly 24
tags/year) plus partial 2017 -- the signature of a different URL path
for older files, not absent data. Instead of guessing path shapes,
this op fetches SEC's index pages for the fails-to-deliver dataset,
extracts every real cnsfails*.zip href, and banks everything not
already in the manifest. Same rules as 4740: idempotent, checkpointed,
time-capped, raw gzipped CSVs to permanent warm.
"""
import gzip
import io
import json
import re
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
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com"}
TIME_CAP_S = 70 * 60
INDEX_PAGES = [
    "https://www.sec.gov/data/foiadocsfailsdatahtm",
    "https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data",
    "https://www.sec.gov/foia/docs/failsdata.htm",
]

s3 = boto3.client("s3", region_name=REGION)


def fetch(url, timeout=60):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def main():
    started = time.time()
    with report("4741_sec_ftd_pre2017_via_index") as rep:
        rep.heading("ops 4741 -- pre-2017 FTD files via SEC's own index pages")

        manifest = json.loads(
            s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)["Body"].read())
        banked = set(manifest.get("files", {}).keys())
        rep.kv(check="banked_before", value=len(banked))

        # 1) discover real hrefs from SEC's index pages
        hrefs = {}
        for page in INDEX_PAGES:
            try:
                html = fetch(page).decode("utf-8", "replace")
                found = re.findall(r'href="([^"]*cnsfails[^"]*\.zip)"', html,
                                    re.IGNORECASE)
                for h in found:
                    absu = h if h.startswith("http") else ("https://www.sec.gov" + h)
                    m = re.search(r"cnsfails(\d{6}[ab])", absu, re.IGNORECASE)
                    key = m.group(1).lower() if m else absu.rsplit("/", 1)[-1]
                    hrefs.setdefault(key, absu)
                rep.log(f"{page} -> {len(found)} cnsfails hrefs")
            except Exception as e:
                rep.log(f"{page} -> {type(e).__name__}: {str(e)[:90]}")
        rep.kv(check="distinct_files_discovered", value=len(hrefs))
        todo = {k: v for k, v in hrefs.items() if k not in banked}
        rep.kv(check="new_files_to_bank", value=len(todo))
        if todo:
            sample = sorted(todo.keys())[:6]
            rep.log("sample new: " + ", ".join(sample))
            rep.log("sample url: " + todo[sample[0]])

        # 2) bank them, oldest first
        banked_now = 0
        err_now = 0
        for key in sorted(todo.keys()):
            if time.time() - started > TIME_CAP_S:
                rep.warn(f"time cap at {key} -- rerun resumes")
                break
            url = todo[key]
            try:
                raw = fetch(url)
                zf = zipfile.ZipFile(io.BytesIO(raw))
                member = zf.namelist()[0]
                text = zf.read(member)
                n_rows = text.count(b"\n")
                gz = gzip.compress(text)
                safe = re.sub(r"[^a-z0-9]+", "", key)[:24]
                s3.put_object(Bucket=BUCKET,
                               Key=f"{PREFIX}cnsfails{safe}.csv.gz",
                               Body=gz, ContentType="text/csv",
                               ContentEncoding="gzip")
                manifest["files"][key] = {
                    "rows": n_rows, "raw_bytes": len(text), "gz_bytes": len(gz),
                    "member": member, "source_url": url,
                    "banked_at": datetime.now(timezone.utc).isoformat()}
                banked_now += 1
                if banked_now % 25 == 0:
                    rep.log(f"  progress: {banked_now} (latest {key}, {n_rows} rows)")
                    s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY,
                                   Body=json.dumps(manifest, separators=(",", ":")),
                                   ContentType="application/json")
            except Exception as e:
                err_now += 1
                rep.log(f"  {key}: {type(e).__name__}: {str(e)[:90]}")
            time.sleep(0.4)

        manifest["updated_at"] = datetime.now(timezone.utc).isoformat()
        s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY,
                       Body=json.dumps(manifest, separators=(",", ":")),
                       ContentType="application/json")

        files = manifest["files"]
        tags6 = sorted(k for k in files if re.fullmatch(r"\d{6}[ab]", k))
        total_rows = sum(f.get("rows", 0) for f in files.values())
        rep.section("Result")
        rep.kv(check="banked_this_run", value=banked_now)
        rep.kv(check="errors_this_run", value=err_now)
        rep.kv(check="total_files_banked", value=len(files))
        rep.kv(check="earliest_tag_banked", value=tags6[0] if tags6 else None)
        rep.kv(check="latest_tag_banked", value=tags6[-1] if tags6 else None)
        rep.kv(check="total_rows_banked", value=total_rows)
        rep.ok(f"archive now {len(files)} files / {total_rows} rows, permanent")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
