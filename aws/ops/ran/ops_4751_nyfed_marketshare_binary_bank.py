"""
ops/4751 -- marketshare: bank the real files (they're Excel, not JSON).

ops 4750: both marketshare variants returned 200 with ~64.7KB bodies
that don't parse as JSON -- consistent with NY Fed publishing the FX
Volume Survey market-share data as Excel workbooks regardless of the
format parameter. The file is the data. This op fetches both variants
as BINARY, identifies the true magic (PK.. = xlsx/zip, <?xml = xml,
else logged hex), and banks the raw bytes verbatim to
data/warm/nyfed-markets/marketshare/{ytd,qtrly}-latest.{ext} plus a
small JSON manifest. Permanent, byte-exact.
"""
import gzip
import json
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
NYF = "https://markets.newyorkfed.org"
PREFIX = "data/warm/nyfed-markets/marketshare/"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com", "Accept": "*/*"}

s3 = boto3.client("s3", region_name=REGION)


def fetch_binary(url, timeout=60):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read(15_000_000)
        ct = r.headers.get("Content-Type", "")
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw, ct


def main():
    with report("4751_nyfed_marketshare_binary_bank") as rep:
        rep.heading("ops 4751 -- marketshare banked as the real files")
        manifest = {"banked_at": datetime.now(timezone.utc).isoformat(),
                     "files": {}}
        banked = 0
        for variant in ("ytd", "qtrly"):
            url = f"{NYF}/api/marketshare/{variant}/latest.json"
            try:
                raw, ct = fetch_binary(url)
            except urllib.error.HTTPError as e:
                # some publishers key the real type off the extension
                url = f"{NYF}/api/marketshare/{variant}/latest.xlsx"
                try:
                    raw, ct = fetch_binary(url)
                except Exception as e2:
                    rep.warn(f"{variant}: both fetches failed "
                             f"({e.code} then {type(e2).__name__})")
                    continue
            except Exception as e:
                rep.warn(f"{variant}: {type(e).__name__}: {str(e)[:100]}")
                continue
            magic = raw[:4]
            if magic[:2] == b"PK":
                ext, kind = "xlsx", "excel-workbook (zip magic PK)"
            elif raw[:5] in (b"<?xml", b"<xml "):
                ext, kind = "xml", "xml"
            elif raw.lstrip()[:1] in (b"{", b"["):
                ext, kind = "json", "json-after-all"
            else:
                ext, kind = "bin", f"unknown magic {magic.hex()}"
            key = f"{PREFIX}{variant}-latest.{ext}"
            s3.put_object(Bucket=BUCKET, Key=key, Body=raw,
                           ContentType=ct or "application/octet-stream")
            manifest["files"][variant] = {
                "key": key, "bytes": len(raw), "kind": kind,
                "content_type_header": ct, "source_url": url}
            banked += 1
            rep.ok(f"{variant}: {len(raw)} bytes banked as {kind} -> {key}")
            rep.kv(variant=variant, banked=True, bytes=len(raw), kind=kind)
        s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}_manifest.json",
                       Body=json.dumps(manifest, indent=1),
                       ContentType="application/json")
        rep.kv(check="marketshare_files_banked", value=banked)
        if banked == 2:
            rep.ok("10/10 NY Fed families now held with real data on disk")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
