"""
ops/4758 -- fix the truncations, verify integrity, go one level deeper.

ops 4757 banked 5 files but three SCE microdata workbooks landed at
EXACTLY 26,000,000 bytes -- my read cap, i.e. truncated xlsx = corrupt.
A permanent bank must never hold silently-broken files. Also, the
hhdc/dsge/research-data seeds returned 200 yet zero candidates: their
data links live on interactive SUBPAGES one level down, not the
landing pages. This op:

  A. re-fetches the three truncated SCE files with a chunked streaming
     reader (200MB cap), verifies each ends with a real ZIP
     end-of-central-directory signature (PK\\x05\\x06 in the last 64KB)
     AND is not exactly at any cap, then overwrites (PUT = new version;
     deny-Delete untouched). A file that can't verify is NOT written --
     flagged instead.
  B. integrity-audits every xlsx already under nyfed-research/ the same
     way; results into the manifest so nothing corrupt hides.
  C. for hhdc / dsge / research-data: collects internal links from each
     landing page (preferring interactive/medialibrary/data paths, cap
     20/seed), fetches those subpages + bundles, corpus-extracts, and
     banks first-party files with the same streaming+verify rules.
"""
import json
import re
import sys
import time
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
NYF = "https://www.newyorkfed.org"
PREFIX = "data/warm/nyfed-research/"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com", "Accept": "*/*"}
CAP = 200_000_000
TIME_CAP_S = 70 * 60
started = time.time()
EXCLUDE = ("google-analytics", "dap.digitalgov", "youtube", "twitter",
            "facebook", "linkedin", "doubleclick")

s3 = boto3.client("s3", region_name=REGION)


def stream_get(url, timeout=120):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        ct = r.headers.get("Content-Type", "")
        chunks = []
        total = 0
        while total < CAP:
            c = r.read(1_048_576)
            if not c:
                break
            chunks.append(c)
            total += len(c)
        truncated = total >= CAP
    return b"".join(chunks), ct, truncated


def page_get(url, timeout=60):
    try:
        raw, ct, _ = stream_get(url, timeout)
        return {"ok": True, "raw": raw, "ct": ct,
                 "body": raw.decode("utf-8", "replace")}
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code, "raw": b"", "body": "", "ct": ""}
    except Exception as e:
        return {"ok": False, "status": None, "raw": b"", "body": "", "ct": "",
                 "error": f"{type(e).__name__}: {str(e)[:110]}"}


def zip_ok(raw):
    return raw[:2] == b"PK" and b"PK\x05\x06" in raw[-65536:]


def absu(h):
    return h if h.startswith("http") else NYF + (h if h.startswith("/")
                                                   else "/" + h)


def bank(rep, tag, url, manifest):
    try:
        raw, ct, truncated = stream_get(url)
    except Exception as e:
        rep.warn(f"  {url[:95]} -> {type(e).__name__}: {str(e)[:90]}")
        return 0
    ext = url.rsplit(".", 1)[-1].lower()
    if truncated:
        rep.warn(f"  {url[:90]} hit the 200MB cap -- NOT banked, "
                 "flagged NEEDS_MULTIPART")
        return 0
    if ext in ("xlsx", "xls", "zip") and not zip_ok(raw):
        rep.warn(f"  {url[:90]} failed zip end-signature check "
                 f"({len(raw)} bytes) -- NOT banked")
        return 0
    if len(raw) < 300:
        return 0
    name = re.sub(r"[^A-Za-z0-9._-]+", "_",
                   url.split("newyorkfed.org", 1)[-1].strip("/"))[:110]
    key = f"{PREFIX}{tag}/{name}"
    s3.put_object(Bucket=BUCKET, Key=key, Body=raw,
                   ContentType=ct or "application/octet-stream")
    manifest["files"][f"{tag}/{name}"] = {
        "source_url": url, "bytes": len(raw), "zip_verified": ext in
        ("xlsx", "xls", "zip"),
        "banked_at": datetime.now(timezone.utc).isoformat()}
    rep.ok(f"  banked {tag}/{name} ({len(raw)} bytes, verified)")
    return 1


def main():
    with report("4758_nyfed_research_integrity_and_depth") as rep:
        rep.heading("ops 4758 -- truncation fix + integrity audit + one level deeper")

        manifest = json.loads(s3.get_object(
            Bucket=BUCKET, Key=f"{PREFIX}_manifest.json")["Body"].read())
        manifest.setdefault("files", {})

        rep.section("A. Re-bank the three truncated SCE microdata files (streaming)")
        trunc = [(k, v) for k, v in manifest["files"].items()
                  if v.get("bytes") == 26_000_000]
        rep.kv(check="truncated_found", value=len(trunc))
        fixed = 0
        for k, v in trunc:
            fixed += bank(rep, k.split("/")[0], v["source_url"], manifest)
        rep.kv(check="truncated_fixed", value=fixed)

        rep.section("B. Integrity audit of everything banked")
        bad = 0
        for k, v in list(manifest["files"].items()):
            ext = k.rsplit(".", 1)[-1].lower()
            if ext not in ("xlsx", "xls", "zip"):
                continue
            try:
                raw = s3.get_object(Bucket=BUCKET,
                                     Key=f"{PREFIX}{k}")["Body"].read()
                ok = zip_ok(raw) and len(raw) != 26_000_000
                v["zip_verified"] = ok
                if not ok:
                    bad += 1
                    rep.warn(f"  CORRUPT in bank: {k} ({len(raw)} bytes)")
            except Exception as e:
                rep.warn(f"  {k}: {type(e).__name__}")
        rep.kv(check="corrupt_remaining", value=bad)

        rep.section("C. One level deeper: hhdc / dsge / research-data")
        for tag, seed in (("hhdc", f"{NYF}/microeconomics/hhdc"),
                            ("dsge", f"{NYF}/research/policy/dsge"),
                            ("research-data",
                             f"{NYF}/research/data_indicators")):
            if time.time() - started > TIME_CAP_S:
                rep.warn("time cap")
                break
            r = page_get(seed)
            corpus = r.get("body", "")
            subs = set()
            for m in re.findall(r'href="(/[A-Za-z0-9_\-/.%]+)"', corpus):
                score = sum(w in m.lower() for w in
                             ("interactive", "medialibrary", "data", "hhdc",
                              "householdcredit", "dsge", "download", "chart"))
                if score and not m.endswith((".pdf", ".jpg", ".png")):
                    subs.add(m)
            rep.kv(seed=tag, subpages_found=len(subs))
            for h in sorted(subs)[:20]:
                sr = page_get(absu(h))
                if sr["ok"]:
                    corpus += "\n" + sr["body"]
                    for sm in re.findall(
                            r'<script[^>]+src="([^"]+\.js[^"]*)"',
                            sr["body"])[:4]:
                        br = page_get(absu(sm))
                        if br["ok"]:
                            corpus += "\n" + br["body"]
                time.sleep(0.15)
            cands = set()
            for m in re.findall(
                    r'href="([^"]+\.(?:xlsx|xls|csv|zip|json))"', corpus, re.I):
                cands.add(m)
            for m in re.findall(
                    r'["\']((?:https?://[^"\']+|/[A-Za-z0-9_\-/.%]+)\.'
                    r'(?:xlsx|xls|csv|zip|json))["\']', corpus, re.I):
                cands.add(m)
            urls = sorted({absu(c) for c in cands
                            if "newyorkfed.org" in absu(c)
                            and not any(x in absu(c) for x in EXCLUDE)})
            rep.kv(seed=tag, candidates=len(urls))
            for u in urls[:15]:
                rep.log(f"  cand: {u[:115]}")
            n = 0
            for u in urls:
                if n >= 50 or time.time() - started > TIME_CAP_S:
                    break
                if any(v.get("source_url") == u
                        for v in manifest["files"].values()):
                    continue
                n += bank(rep, tag, u, manifest)
                time.sleep(0.25)
            rep.kv(seed=tag, banked=n)

        manifest["swept_at"] = datetime.now(timezone.utc).isoformat()
        s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}_manifest.json",
                       Body=json.dumps(manifest, indent=1),
                       ContentType="application/json")
        rep.kv(check="manifest_total_files", value=len(manifest["files"]))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
