"""
ops_4031 — ship v1.5.0: rebuild the zip Khalid downloads, settle the
ingest lambda, correct the blackswan audit gate, verify end to end.
"""
import io
import json
import subprocess
import sys
import time
import urllib.request
import zipfile as zf
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
REPO = ROOT.parents[0]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=90, retries={"max_attempts": 1}))
BUCKET = "justhodl-dashboard-live"
ZIP_KEY = "tools/jh-tv-extension.zip"
FN = "justhodl-tv-notes-ingest"
MARK = "ops4016 sources"


def main():
    with report("4031_ship_v170") as rep:
        rep.heading("ops 4016 — extension v1.7.0 + sources persistence")
        checks = []

        rep.section("A. rebuild + upload the zip (mirror current layout)")
        try:
            old = s3.get_object(Bucket=BUCKET, Key=ZIP_KEY)["Body"].read()
            names = zf.ZipFile(io.BytesIO(old)).namelist()[:4]
            rooted = not any(n.startswith("chrome-extension/") for n in names)
            rep.kv(old_bytes=len(old), sample=names, files_at_root=rooted)
        except Exception:
            rooted = True
            rep.log("  no existing zip readable — defaulting to files-at-root")
        buf = io.BytesIO()
        src = REPO / "chrome-extension"
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            for f in sorted(src.rglob("*")):
                if f.is_file():
                    arc = f.relative_to(src if rooted else REPO)
                    z.write(f, str(arc))
        data = buf.getvalue()
        s3.put_object(Bucket=BUCKET, Key=ZIP_KEY, Body=data,
                      ContentType="application/zip",
                      CacheControl="max-age=300")
        chk = zf.ZipFile(io.BytesIO(
            s3.get_object(Bucket=BUCKET, Key=ZIP_KEY)["Body"].read()))
        man = json.loads(chk.read("manifest.json" if rooted
                                  else "chrome-extension/manifest.json"))
        rep.kv(new_bytes=len(data), version=man.get("version"))
        checks.append(("zip carries v1.5.0", man.get("version") == "1.7.0"))
        inj = chk.read("inject.js" if rooted
                       else "chrome-extension/inject.js").decode()
        checks.append(("zip inject carries the ACTIVE harvester",
                       "__jh_cmd" in inj and "symbol_search/v3" in inj))
        cjs = chk.read("content.js" if rooted
                       else "chrome-extension/content.js").decode()
        checks.append(("zip content is FULLY AUTONOMOUS",
                       "jh_auto_day" in cjs and "autoSync" in cjs))

        rep.section("B. settle the ingest lambda")
        srct = (ROOT / "lambdas" / FN / "source" / "lambda_function.py").read_text()
        assert MARK in srct
        b2 = io.BytesIO()
        with zf.ZipFile(b2, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", srct)
        for a in range(6):
            try:
                lam.update_function_code(FunctionName=FN, ZipFile=b2.getvalue(),
                                         Publish=True)
                break
            except lam.exceptions.ResourceConflictException:
                time.sleep(12)
        ok = False
        for _ in range(24):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                    lam.get_function(FunctionName=FN)["Code"]["Location"],
                    timeout=60).read())).read("lambda_function.py").decode()
                if MARK in dep:
                    ok = True
                    break
            time.sleep(8)
        checks.append(("ingest lambda persists sources", ok))

        rep.section("C. corrected blackswan gate (prefer 'swan' over 'black*')")
        wl = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/tv-watchlists.json")["Body"].read())
        wls = wl.get("watchlists") or wl.get("lists") or []
        if isinstance(wls, dict):
            wls = list(wls.values())
        bs = next((w for w in wls if "swan" in str(w.get("name", "")).lower()),
                  None)
        rep.kv(blackswan=str((bs or {}).get("name")),
               n_symbols=len((bs or {}).get("symbols") or []),
               artifact_generated=wl.get("generated_at"))
        for x in ((bs or {}).get("symbols") or [])[:30]:
            rep.log(f"    {x}")
        checks.append(("Black Swan Event >=100 symbols captured",
                       bool(bs) and len(bs.get("symbols") or []) >= 100))

        rep.section("D. zip reachable at the edge download URL")
        got = 0
        for i in range(6):
            try:
                r = urllib.request.urlopen(
                    "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/"
                    "tools/jh-tv-extension.zip", timeout=25)
                got = len(r.read())
                if got == len(data):
                    break
            except Exception:
                pass
            time.sleep(10)
        rep.kv(edge_bytes=got)
        checks.append(("download serves the new zip", got == len(data)))

        failed = [l for l, ok2 in checks if not ok2]
        for l, ok2 in checks:
            (rep.ok if ok2 else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — v1.5.0 live ({len(data)}B); Black Swan "
               f"{len(bs.get('symbols') or [])} symbols; sources persistence armed. "
               f"Khalid: re-download, replace folder, reload extension, browse TV, "
               f"Upload → data/tv-sources.json fills.")


if __name__ == "__main__":
    main()
# retrigger 1785249737
