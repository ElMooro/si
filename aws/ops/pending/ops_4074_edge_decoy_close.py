"""ops_4074 — close the extension-zip decoy, end to end.

ops 4072/4073 found justhodl.ai/tools/jh-tv-extension.zip serving v1.4.0
(ops 3162 — no autoStart, no harvester) while S3 held v1.7.8.  Root cause
turned out to be two separate faults stacked:

  1. pages.yml's `paths:` trigger never listed tools/**, so a commit that
     touched only the zip fired NO Pages build at all.  (Fixed in the
     same push as this op.)
  2. Cloudflare caches the .zip far longer than HTML, so even after a
     build the edge kept serving the old object.

This op proves which of the two is still binding, rather than guessing:
it fetches the edge both plainly and cache-busted, and reports the CDN
headers (cf-cache-status / age / etag).  A cache-busted fetch that
returns 1.7.8 means the origin is correct and only the cached object is
stale — a materially different situation from a build that never ran.

It also verifies the chain Khalid actually installs through, which is
NOT the edge: the .bat stub fetches the .ps1 from S3, and the .ps1
downloads the zip from S3.  That path must be current regardless.
"""
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
S3BASE = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/"


def ver_of(b):
    return json.loads(zf.ZipFile(io.BytesIO(b)).read("manifest.json"))["version"]


def fetch(url, tag):
    req = urllib.request.Request(url, headers={
        "User-Agent": f"justhodl-ops/4074-{tag}",
        "Cache-Control": "no-cache", "Pragma": "no-cache"})
    r = urllib.request.urlopen(req, timeout=45)
    return r.read(), dict(r.headers)


def main():
    with report("4074_edge_decoy_close") as rep:
        rep.heading("ops 4074 — extension zip: origin vs edge vs install path")
        checks = []

        # ── A. the path Khalid actually installs through ──
        rep.section("A. the REAL install chain (.bat → .ps1 → S3 zip)")
        s3zip = s3.get_object(Bucket=BUCKET,
                              Key="tools/jh-tv-extension.zip")["Body"].read()
        s3v = ver_of(s3zip)
        rep.log(f"  S3 zip                : {s3v}  ({len(s3zip)} B)")
        checks.append(("S3 zip is v1.7.8", s3v == "1.7.8"))

        bat, _ = fetch(S3BASE + f"tools/install-jh-extension.bat?t={int(time.time())}",
                       "bat")
        ps1, _ = fetch(S3BASE + f"tools/install-jh-extension.ps1?t={int(time.time())}",
                       "ps1")
        rep.log(f"  .bat stub             : {len(bat)} B")
        rep.log(f"  .ps1 logic            : {len(ps1)} B")
        checks.append((".bat is the stub that fetches the .ps1",
                       b"install-jh-extension.ps1" in bat))
        checks.append((".ps1 pulls the zip from S3 (not the edge decoy)",
                       b"jh-tv-extension.zip" in ps1
                       and b"s3.us-east-1.amazonaws.com" in ps1))
        checks.append((".ps1 still caret-free (the v2 bug class)",
                       b"^" not in ps1))
        checks.append((".ps1 loads the extension + makes the shortcut",
                       b"load-extension" in ps1 and b"CreateShortcut" in ps1))
        rep.log("  → re-running the .bat already on his machine pulls v1.7.8; "
                "the stub pattern means no re-download was ever needed")

        # ── B. edge: plain vs cache-busted, with CDN headers ──
        rep.section("B. edge — plain vs cache-busted (which fault is binding?)")
        plain_v = busted_v = None
        try:
            b1, h1 = fetch("https://justhodl.ai/tools/jh-tv-extension.zip",
                           "plain")
            plain_v = ver_of(b1)
            rep.log(f"  plain fetch           : {plain_v}  ({len(b1)} B)")
            rep.log(f"    cf-cache-status={h1.get('cf-cache-status')} "
                    f"age={h1.get('age')} etag={str(h1.get('etag'))[:24]}")
        except Exception as e:
            rep.log(f"  plain fetch failed: {str(e)[:90]}")

        for a in range(6):
            try:
                b2, h2 = fetch("https://justhodl.ai/tools/jh-tv-extension.zip"
                               f"?v={int(time.time())}-{a}", f"bust{a}")
                busted_v = ver_of(b2)
                rep.log(f"  cache-busted #{a + 1}       : {busted_v}  "
                        f"({len(b2)} B) cf={h2.get('cf-cache-status')} "
                        f"age={h2.get('age')}")
                if busted_v == s3v:
                    break
            except Exception as e:
                rep.log(f"  busted #{a + 1} failed: {str(e)[:70]}")
            time.sleep(25)

        rep.kv(s3_zip=s3v, edge_plain=plain_v, edge_busted=busted_v)

        rep.section("DIAGNOSIS")
        if busted_v == s3v and plain_v != s3v:
            rep.log("  Origin is CORRECT — Pages holds v1.7.8. Only the "
                    "cached CDN object is stale; it ages out on its own.")
            rep.log("  The binding fault was the pages.yml paths filter, now "
                    "carrying tools/** so zip-only ships build in future.")
        elif busted_v == s3v and plain_v == s3v:
            rep.log("  Fully closed — edge and origin both serve v1.7.8.")
        else:
            rep.log("  Origin still stale even cache-busted: the Pages build "
                    "has not swept the new zip into _site yet.")

        # Origin correctness is the real gate. A CDN object that will age
        # out on its own is not a reason to fail the op, and the install
        # path does not traverse the edge at all.
        checks.append(("edge origin serves v1.7.8 when cache is bypassed",
                       busted_v == s3v))

        rep.section("VERDICT")
        for n, o in checks:
            rep.log(f"  {'✓' if o else '✗'} {n}")
        bad = [n for n, o in checks if not o]
        if bad:
            rep.log(f"✗ FAILED: {bad}")
            sys.exit(1)
        rep.log("✅ PASS_ALL — install chain current, origin serving v1.7.8.")


if __name__ == "__main__":
    main()
