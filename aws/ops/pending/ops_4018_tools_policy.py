"""ops_4018 — grant public GetObject on tools/* (bucket is
BucketOwnerEnforced: ACLs disabled, policy is the only lever) + verify."""
import io
import json
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
ARN = f"arn:aws:s3:::{BUCKET}/tools/*"


def main():
    with report("4018_tools_policy") as rep:
        rep.heading("ops 4018 — bucket-policy public read for tools/*")
        pol = json.loads(s3.get_bucket_policy(Bucket=BUCKET)["Policy"])
        sids = [st.get("Sid") for st in pol.get("Statement", [])]
        rep.kv(existing_sids=json.dumps(sids)[:200])
        covered = any(
            st.get("Effect") == "Allow"
            and "s3:GetObject" in (st.get("Action") if isinstance(
                st.get("Action"), list) else [st.get("Action")])
            and ARN in (st.get("Resource") if isinstance(
                st.get("Resource"), list) else [st.get("Resource")])
            for st in pol["Statement"])
        rep.kv(tools_already_covered=covered)
        if not covered:
            pol["Statement"].append({
                "Sid": "ToolsPublicRead", "Effect": "Allow",
                "Principal": "*", "Action": "s3:GetObject",
                "Resource": ARN})
            s3.put_bucket_policy(Bucket=BUCKET, Policy=json.dumps(pol))
            rep.ok("  statement ToolsPublicRead appended (additive only)")
        body = s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
        ver = json.loads(zf.ZipFile(io.BytesIO(body)).read(
            [n for n in zf.ZipFile(io.BytesIO(body)).namelist()
             if n.endswith("manifest.json")][0]))["version"]
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
                rep.log(f"  [{i}] code={code}")
            time.sleep(8)
        rep.kv(http=code, edge_bytes=got, s3_bytes=len(body), version=ver)
        if got != len(body) or ver != "1.5.0":
            rep.fail("download still failing")
            sys.exit(1)
        rep.ok(f"PASS_ALL — v1.5.0 publicly downloadable ({got}B)")


if __name__ == "__main__":
    main()
