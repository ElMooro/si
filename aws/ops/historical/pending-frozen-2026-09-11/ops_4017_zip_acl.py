"""ops_4017 — fix the zip's public ACL and verify the download."""
import io
import sys
import time
import urllib.request
import zipfile as zf
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "tools/jh-tv-extension.zip"
URL = f"https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/{KEY}"


def main():
    with report("4017_zip_acl") as rep:
        rep.heading("ops 4017 — public ACL on the v1.5.0 zip")
        body = s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
        man = zf.ZipFile(io.BytesIO(body))
        import json as _j
        ver = _j.loads(man.read([n for n in man.namelist()
                                 if n.endswith("manifest.json")][0]))["version"]
        rep.kv(s3_bytes=len(body), version=ver)
        try:
            s3.put_object_acl(Bucket=BUCKET, Key=KEY, ACL="public-read")
            rep.ok("  ACL public-read applied")
        except Exception as e:
            rep.log(f"  put_object_acl: {type(e).__name__} — re-putting with ACL")
            s3.put_object(Bucket=BUCKET, Key=KEY, Body=body,
                          ContentType="application/zip", ACL="public-read",
                          CacheControl="max-age=300")
        got, code = 0, 0
        for i in range(8):
            try:
                r = urllib.request.urlopen(URL + f"?v={int(time.time())}",
                                           timeout=25)
                got, code = len(r.read()), r.status
                if got == len(body):
                    break
            except Exception as e:
                code = getattr(e, "code", 0)
                rep.log(f"  [{i}] {type(e).__name__} code={code}")
            time.sleep(8)
        rep.kv(http=code, edge_bytes=got)
        if got != len(body) or ver != "1.5.0":
            rep.fail("still not serving the new zip")
            sys.exit(1)
        rep.ok(f"PASS_ALL — v1.5.0 downloadable ({got}B)")


if __name__ == "__main__":
    main()
