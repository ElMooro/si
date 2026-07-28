"""ops_4029 — dynamic version badge from the served zip's own manifest."""
import io
import json
import sys
import time
import urllib.request
import zipfile as zf
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
ZIP_KEY = "tools/jh-tv-extension.zip"
VER_KEY = "tools/jh-tv-extension.version.json"
PAGE = "https://justhodl.ai/tv-notes.html"
MARKS = ['id="extVer"', "version.json", "Harvest sources", "auto-uploads"]


def main():
    with report("4029_version_badge2") as rep:
        rep.heading("ops 4022 — never-stale version badge")
        body = s3.get_object(Bucket=BUCKET, Key=ZIP_KEY)["Body"].read()
        z = zf.ZipFile(io.BytesIO(body))
        man = json.loads(z.read([n for n in z.namelist()
                                 if n.endswith("manifest.json")][0]))
        ver = man.get("version")
        rep.kv(zip_bytes=len(body), zip_version=ver)
        s3.put_object(Bucket=BUCKET, Key=VER_KEY,
                      Body=json.dumps({"version": ver, "bytes": len(body),
                                       "updated_utc": datetime.now(
                                           timezone.utc).isoformat()}),
                      ContentType="application/json",
                      CacheControl="max-age=120")
        got = json.loads(urllib.request.urlopen(
            "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/" +
            VER_KEY + f"?t={int(time.time())}", timeout=25).read())
        rep.kv(public_version=got.get("version"))
        checks = [("zip is v1.6.0", ver == "1.6.1"),
                  ("version.json publicly serves it",
                   got.get("version") == "1.6.1")]
        n, htm = 0, ""
        for i in range(9):
            try:
                r = urllib.request.Request(PAGE + f"?cb={int(time.time())}",
                                           headers={"User-Agent": "Mozilla/5.0",
                                                    "Cache-Control": "no-cache"})
                htm = urllib.request.urlopen(r, timeout=25).read().decode(
                    "utf8", "ignore")
                n = sum(1 for m in MARKS if m in htm)
                if n == len(MARKS):
                    break
            except Exception:
                pass
            time.sleep(20)
        rep.kv(page_markers=f"{n}/{len(MARKS)}", page_bytes=len(htm))
        checks.append(("page badge dynamic + harvest step live",
                       n == len(MARKS)))
        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — badge feeds from the zip itself (v{ver})")


if __name__ == "__main__":
    main()
