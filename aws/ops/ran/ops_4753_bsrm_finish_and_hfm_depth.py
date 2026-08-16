"""
ops/4753 -- finish BSRM properly + mark my duplicate + prove HFM depth.

ops 4752's BSRM branch hit my hardcoded /hf/v1 probe, "validated" the
HFM catalog a second time, re-banked all 497 HFM series under
data/warm/ofr-bsrm/series/ (wrong prefix), and returned BEFORE banking
the two real BSRM files it had just discovered. deny-Delete on warm/*
means the duplicates are immutable by design -- so the honest fix is:

  A. bank the two REAL BSRM workbooks verbatim:
     /bank-systemic-risk-monitor/data/ofr_bsrm.xlsx and
     ofr_bsrm_international_scores.xlsx -> data/warm/ofr-bsrm/
  B. write data/warm/ofr-bsrm/series/_DUPLICATE_NOTE.json stating
     plainly that everything under that sub-prefix is an accidental
     copy of data/warm/ofr-hfm/series/ (ops 4752 bug), canonical home
     is ofr-hfm
  C. depth-proof two HFM series (n_obs/earliest/latest) so the 497-
     series claim carries verified spans like every other bank
"""
import gzip
import json
import re
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
FRG = "https://www.financialresearch.gov"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com", "Accept": "*/*"}

s3 = boto3.client("s3", region_name=REGION)


def get_raw(url, timeout=60):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read(20_000_000)
        ct = r.headers.get("Content-Type", "")
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw, ct


def deep_dates(obj, out=None, depth=0):
    if out is None:
        out = []
    if depth > 7:
        return out
    if isinstance(obj, str):
        if len(obj) >= 10 and obj[:4].isdigit() and obj[4] == "-" and obj[7] == "-":
            out.append(obj[:10])
    elif isinstance(obj, (list, tuple)):
        for x in obj[:40000]:
            deep_dates(x, out, depth + 1)
    elif isinstance(obj, dict):
        for v in obj.values():
            deep_dates(v, out, depth + 1)
    return out


def main():
    with report("4753_bsrm_finish_and_hfm_depth") as rep:
        rep.heading("ops 4753 -- BSRM workbooks banked, duplicate marked, HFM depth proven")

        rep.section("A. Bank the two real BSRM workbooks")
        for fn in ("ofr_bsrm.xlsx", "ofr_bsrm_international_scores.xlsx"):
            url = f"{FRG}/bank-systemic-risk-monitor/data/{fn}"
            try:
                raw, ct = get_raw(url)
                kind = "xlsx (PK magic)" if raw[:2] == b"PK" else \
                    f"unexpected magic {raw[:4].hex()}"
                key = f"data/warm/ofr-bsrm/{fn}"
                s3.put_object(Bucket=BUCKET, Key=key, Body=raw,
                               ContentType=ct or
                               "application/vnd.openxmlformats-officedocument"
                               ".spreadsheetml.sheet")
                rep.ok(f"{fn}: {len(raw)} bytes banked ({kind}) -> {key}")
                rep.kv(file=fn, banked=True, bytes=len(raw), kind=kind)
            except Exception as e:
                rep.warn(f"{fn}: {type(e).__name__}: {str(e)[:110]}")
                rep.kv(file=fn, banked=False)

        rep.section("B. Mark the accidental duplicate sub-prefix")
        note = {
            "note": ("EVERYTHING under data/warm/ofr-bsrm/series/ is an "
                      "accidental duplicate of data/warm/ofr-hfm/series/ -- "
                      "ops 4752's BSRM branch re-validated the /hf/v1 catalog "
                      "via a hardcoded probe and re-banked all 497 HFM series "
                      "here before this was caught. deny-Delete makes these "
                      "objects immutable by design; the canonical home is "
                      "data/warm/ofr-hfm/series/. Do not wire readers to this "
                      "sub-prefix."),
            "canonical_prefix": "data/warm/ofr-hfm/series/",
            "cause": "ops 4752", "written_by": "ops 4753",
            "written_at": datetime.now(timezone.utc).isoformat()}
        s3.put_object(Bucket=BUCKET,
                       Key="data/warm/ofr-bsrm/series/_DUPLICATE_NOTE.json",
                       Body=json.dumps(note, indent=1),
                       ContentType="application/json")
        rep.ok("duplicate marker written")

        rep.section("C. HFM depth proof")
        # list a couple of banked HFM series and measure real spans
        resp = s3.list_objects_v2(Bucket=BUCKET,
                                    Prefix="data/warm/ofr-hfm/series/",
                                    MaxKeys=1000)
        keys = [o["Key"] for o in resp.get("Contents") or []]
        rep.kv(check="hfm_series_objects", value=len(keys))
        for k in keys[:1] + keys[len(keys) // 2:len(keys) // 2 + 1]:
            try:
                raw = s3.get_object(Bucket=BUCKET, Key=k)["Body"].read()
                if raw[:2] == b"\x1f\x8b":
                    raw = gzip.decompress(raw)
                doc = json.loads(raw)
                dd = sorted(set(deep_dates(doc)))
                m = re.sub(r"\.json\.gz$", "", k.rsplit("/", 1)[-1])
                rep.kv(mnemonic=m, n=len(dd),
                       earliest=dd[0] if dd else None,
                       latest=dd[-1] if dd else None)
                rep.ok(f"{m}: {len(dd)} distinct dates, "
                        f"{dd[0] if dd else '-'} -> {dd[-1] if dd else '-'}")
            except Exception as e:
                rep.warn(f"{k.rsplit('/', 1)[-1]}: {type(e).__name__}: "
                         f"{str(e)[:100]}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
