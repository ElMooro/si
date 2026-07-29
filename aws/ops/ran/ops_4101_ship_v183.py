"""ops_4101 — ship v1.8.3: streak-halving recovery + start self-test.
Fresh self-contained shipper: zip, upload, byte-verify served, version.json."""
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
ZKEY = "tools/jh-tv-extension.zip"
VKEY = "tools/jh-tv-extension.version.json"
EXT = ROOT.parent / "chrome-extension"


def main():
    with report("4101_ship_v183") as rep:
        rep.heading("ops 4101 — ship v1.8.3")
        checks = []
        man = json.loads((EXT / "manifest.json").read_text())
        checks.append(("local manifest 1.8.3", man.get("version") == "1.8.3"))
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            for f in sorted(EXT.iterdir()):
                if f.is_file():
                    z.writestr(f.name, f.read_bytes())
        s3.put_object(Bucket=BUCKET, Key=ZKEY, Body=buf.getvalue(),
                      ContentType="application/zip", CacheControl="max-age=60")
        time.sleep(2)
        served = urllib.request.urlopen(
            "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/" +
            ZKEY + f"?t={int(time.time())}", timeout=30).read()
        checks.append(("served zip byte-identical",
                       served == buf.getvalue()))
        chk = zf.ZipFile(io.BytesIO(served))
        sman = json.loads(chk.read("manifest.json"))
        cjs = chk.read("content.js").decode()
        checks.append(("served manifest 1.8.3",
                       sman.get("version") == "1.8.3"))
        checks.append(("streak-halving in served zip",
                       "streak_ok" in cjs and "Math.round(DELAY / 2)" in cjs))
        checks.append(("self-test in served zip",
                       "SELF-TEST" in cjs and "selftest" in cjs))
        s3.put_object(Bucket=BUCKET, Key=VKEY,
                      Body=json.dumps({"version": sman.get("version"),
                                       "bytes": len(served)}),
                      ContentType="application/json",
                      CacheControl="max-age=60")
        vj = json.loads(urllib.request.urlopen(
            "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/" +
            VKEY + f"?t={int(time.time())}", timeout=30).read())
        checks.append(("version.json serves 1.8.3",
                       vj.get("version") == "1.8.3"))
        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — v1.8.3 live ({len(served)}B): streak-halving "
               f"+ start self-test, badge verdict in ~8s, server-visible "
               f"within a minute")


if __name__ == "__main__":
    main()
