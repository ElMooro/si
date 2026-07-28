"""ops_4024 — ship the one-click Windows installer + page button."""
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
REPO = ROOT.parents[0]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "tools/install-jh-extension.bat"
URL = f"https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/{KEY}"
PAGE = "https://justhodl.ai/tv-notes.html"


def main():
    with report("4024_installer_v2") as rep:
        rep.heading("ops 4023 — one-click installer live")
        ps1 = (REPO / "tools-src" / "install-jh-extension.ps1").read_bytes()
        s3.put_object(Bucket=BUCKET, Key="tools/install-jh-extension.ps1",
                      Body=ps1, ContentType="text/plain",
                      CacheControl="max-age=120")
        got1 = urllib.request.urlopen(
            "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/"
            f"tools/install-jh-extension.ps1?t={int(time.time())}",
            timeout=25).read()
        rep.kv(ps1_bytes=len(ps1), ps1_edge=len(got1),
               ps1_caret_free=b"^" not in got1)
        body = (REPO / "tools-src" / "install-jh-extension.bat").read_bytes()
        s3.put_object(Bucket=BUCKET, Key=KEY, Body=body,
                      ContentType="application/octet-stream",
                      ContentDisposition="attachment; "
                                         "filename=install-jh-extension.bat",
                      CacheControl="max-age=120")
        got = b""
        for i in range(6):
            try:
                got = urllib.request.urlopen(URL + f"?t={int(time.time())}",
                                             timeout=25).read()
                if got == body:
                    break
            except Exception:
                pass
            time.sleep(8)
        rep.kv(bat_bytes=len(body), edge_bytes=len(got))
        checks = [("installer publicly downloadable", got == body),
                  ("stub fetches the ps1; logic is caret-free and byte-exact",
                   b"install-jh-extension.ps1" in got and
                   b"load-extension" in got1 and b"^" not in got1
                   and got1 == ps1)]
        n, htm = 0, ""
        MARKS = ["install-jh-extension.bat", "One-click install",
                 "Harvest sources"]
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
        rep.kv(page_markers=f"{n}/{len(MARKS)}")
        checks.append(("page shows the one-click button", n == len(MARKS)))
        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok("PASS_ALL — double-click install path live")


if __name__ == "__main__":
    main()
