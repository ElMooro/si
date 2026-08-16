"""
ops/4755 -- site vacuum v2: the regex that actually worked.

ops 4754 vacuumed 0 files because it only matched href="...ext"
attributes -- but OFR links its data through JS strings (ops 4752
found the BSRM xlsx files on the SAME page using quoted-string
extraction over page+bundle corpus). This pass:

  - crawls /data/ and /monitoring-tools/ plus ONE level of their
    internal financialresearch.gov links (cap 18 subpages)
  - builds a corpus of every page + its JS bundles
  - extracts BOTH href attributes AND any quoted string ending in
    .csv/.xlsx/.json/.zip/.xls
  - dedupes against what's already banked (ofr-site manifest, the two
    BSRM workbooks, fsi.csv), excludes analytics hosts and the two
    machine-API hosts already fully held
  - banks the remainder verbatim (cap 80 x 15MB) to
    data/warm/ofr-site/ and updates the manifest

Targets this should finally catch: the Interagency Data Inventory,
any NCCBR pilot tables, FIRD files, and whatever else the site links
only through scripts.
"""
import gzip
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
FRG = "https://www.financialresearch.gov"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com", "Accept": "*/*"}
TIME_CAP_S = 70 * 60
started = time.time()
EXCLUDE = ("touchpoints", "google-analytics", "dap.digitalgov", "youtube",
            "twitter", "facebook", "data.financialresearch.gov")

s3 = boto3.client("s3", region_name=REGION)


def get(url, timeout=60):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read(16_000_000)
            ct = r.headers.get("Content-Type", "")
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            return {"ok": True, "status": r.status, "raw": raw, "ct": ct,
                     "body": raw.decode("utf-8", "replace")}
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code, "raw": b"", "body": "", "ct": ""}
    except Exception as e:
        return {"ok": False, "status": None, "raw": b"", "body": "", "ct": "",
                 "error": f"{type(e).__name__}: {str(e)[:110]}"}


def absu(h):
    return h if h.startswith("http") else FRG + (h if h.startswith("/")
                                                   else "/" + h)


def main():
    with report("4755_ofr_site_vacuum_v2") as rep:
        rep.heading("ops 4755 -- site vacuum v2 (corpus extraction + 1-level crawl)")

        seeds = [f"{FRG}/data/", f"{FRG}/monitoring-tools/"]
        pages = []
        sub_links = set()
        for pg in seeds:
            r = get(pg)
            rep.log(f"seed {pg.rsplit('.gov', 1)[-1]} -> status={r.get('status')} "
                    f"bytes={len(r.get('body', ''))}")
            if r["ok"]:
                pages.append((pg, r["body"]))
                for m in re.findall(r'href="(/[A-Za-z0-9_\-/]+/?)"', r["body"]):
                    if not any(x in m for x in ("#", "mailto", ".pdf")):
                        sub_links.add(m)
        rep.kv(check="internal_links_found", value=len(sub_links))
        for h in sorted(sub_links)[:18]:
            if time.time() - started > TIME_CAP_S:
                break
            u = absu(h)
            r = get(u)
            if r["ok"]:
                pages.append((u, r["body"]))
            time.sleep(0.15)
        rep.kv(check="pages_in_corpus", value=len(pages))

        # add JS bundles referenced by any page
        bundle_urls = set()
        for _, body in pages:
            for sm in re.findall(r'<script[^>]+src="([^"]+\.js[^"]*)"', body):
                bundle_urls.add(absu(sm))
        corpus_parts = [b for _, b in pages]
        for bu in sorted(bundle_urls)[:10]:
            r = get(bu)
            if r["ok"]:
                corpus_parts.append(r["body"])
        corpus = "\n".join(corpus_parts)
        rep.kv(check="bundles_fetched", value=min(len(bundle_urls), 10))
        rep.kv(check="corpus_bytes", value=len(corpus))

        cands = set()
        for m in re.findall(
                r'href="([^"]+\.(?:csv|xlsx|xls|json|zip))"', corpus, re.I):
            cands.add(m)
        for m in re.findall(
                r'["\']((?:https?://[^"\']+|/[A-Za-z0-9_\-/.]+)\.'
                r'(?:csv|xlsx|xls|json|zip))["\']', corpus, re.I):
            cands.add(m)
        urls = sorted({absu(c) for c in cands
                        if not any(x in absu(c) for x in EXCLUDE)})
        rep.kv(check="data_file_candidates", value=len(urls))
        for u in urls[:30]:
            rep.log(f"  cand: {u[:110]}")

        # dedupe against already banked
        try:
            manifest = json.loads(s3.get_object(
                Bucket=BUCKET,
                Key="data/warm/ofr-site/_manifest.json")["Body"].read())
        except Exception:
            manifest = {"files": {}}
        manifest.setdefault("files", {})
        already_urls = {v.get("source_url") for v in manifest["files"].values()}
        already_urls.add(f"{FRG}/financial-stress-index/data/fsi.csv")
        already_urls.add(f"{FRG}/bank-systemic-risk-monitor/data/ofr_bsrm.xlsx")
        already_urls.add(f"{FRG}/bank-systemic-risk-monitor/data/"
                          "ofr_bsrm_international_scores.xlsx")

        n_banked = 0
        for u in urls:
            if u in already_urls:
                rep.log(f"  already held: {u.rsplit('/', 1)[-1]}")
                continue
            if n_banked >= 80 or time.time() - started > TIME_CAP_S:
                break
            r = get(u)
            raw = r.get("raw", b"")
            if not (r["ok"] and len(raw) > 200):
                rep.log(f"  skip {u[:95]} -> status={r.get('status')} "
                        f"bytes={len(raw)}")
                continue
            name = re.sub(r"[^A-Za-z0-9._-]+", "_",
                           u.split(".gov", 1)[-1].strip("/"))[:100]
            key = f"data/warm/ofr-site/{name}"
            s3.put_object(Bucket=BUCKET, Key=key, Body=raw,
                           ContentType=r.get("ct") or
                           "application/octet-stream")
            manifest["files"][name] = {"source_url": u, "bytes": len(raw),
                                        "banked_at": datetime.now(
                                            timezone.utc).isoformat()}
            n_banked += 1
            rep.ok(f"  banked {name} ({len(raw)} bytes)")
            time.sleep(0.25)
        manifest["swept_at"] = datetime.now(timezone.utc).isoformat()
        s3.put_object(Bucket=BUCKET, Key="data/warm/ofr-site/_manifest.json",
                       Body=json.dumps(manifest, indent=1),
                       ContentType="application/json")
        rep.kv(check="files_banked_this_run", value=n_banked)
        rep.kv(check="files_in_manifest_total", value=len(manifest["files"]))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
