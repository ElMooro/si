"""ops_5400 -- warehouse vs page/engine consumption map.

Reads data/provider-catalog.json, lists Lambda names, greps the
checked-out repo for provider slugs, writes data/provider-consumption.json
(no secrets, no full S3 walk). Proves which catalogued providers are
orphans so Khalid can cancel overlapping paid seats.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
REGION = "us-east-1"
UA = "JustHodl Research raafouis@gmail.com"

OVERLAY = {
    "te-mirror": "FRED mirror -- keep only if ICE BofA cross-check still unique",
    "te-feed": "single stale country snapshot vs FRED/OECD/WB",
    "dbnomics": "aggregator over Eurostat/OECD/IMF already warehoused",
    "yahoo": "equity tape overlap with Polygon grouped-daily",
    "kr-ecos": "0 series -- key never landed",
    "ofr-bsrm": "catalog notes duplicate of ofr-h family",
}

KEEP = {
    "fred", "ecb", "treasury", "nyfed", "polygon", "bls", "bea",
    "cftc", "cboe", "finra", "sec-edgar", "imf", "oecd", "worldbank",
    "bis", "boe", "boj", "fed-board", "chicagofed", "census-us", "dol",
}


def _get_json(s3, key):
    try:
        body = s3.get_object(Bucket=B, Key=key)["Body"].read()
        return json.loads(body)
    except Exception as e:
        return {"_error": "%s: %s" % (type(e).__name__, str(e)[:120])}


def _list_functions(lam):
    names = []
    token = None
    while True:
        kw = {"MaxItems": 50}
        if token:
            kw["Marker"] = token
        resp = lam.list_functions(**kw)
        for fn in resp.get("Functions") or []:
            names.append(fn.get("FunctionName") or "")
        token = resp.get("NextMarker")
        if not token:
            break
        if len(names) > 4000:
            break
    return names


def _repo_hits(root: Path, slugs):
    hits = {s: [] for s in slugs}
    exts = {".html", ".js", ".py", ".json", ".md"}
    skip_parts = {
        "aws/ops/pending", "aws/ops/ran", "aws/ops/reports",
        "aws/ops/_archive", "node_modules", ".git",
    }
    files = []
    for p in root.rglob("*"):
        if not p.is_file() or p.suffix.lower() not in exts:
            continue
        rel = p.as_posix()
        if any(sp in rel for sp in skip_parts):
            continue
        if p.stat().st_size > 2_000_000:
            continue
        files.append(p)
        if len(files) > 8000:
            break
    needles = sorted(slugs, key=len, reverse=True)
    for p in files:
        try:
            text = p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        low = text.lower()
        rel = str(p.relative_to(root))
        for s in needles:
            if s and s.lower() in low:
                bucket = hits[s]
                if len(bucket) < 12:
                    bucket.append(rel)
    return hits, len(files)


def main():
    with report("ops_5400_provider_consumption") as R:
        R.heading("ops 5400 -- provider consumption / cancel map")
        s3 = boto3.client("s3", region_name=REGION)
        lam = boto3.client("lambda", region_name=REGION)
        root = Path(__file__).resolve().parents[3]
        if not (root / "data.html").exists():
            root = Path.cwd()

        cat = _get_json(s3, "data/provider-catalog.json")
        if cat.get("_error"):
            R.fail("catalog unreadable: %s" % cat["_error"])
            sys.exit(1)
        providers = cat.get("providers") or []
        slugs = [p.get("slug") for p in providers if p.get("slug")]
        R.log("catalog as_of=%s providers=%d" % (cat.get("as_of"), len(providers)))

        fnames = _list_functions(lam)
        R.log("lambda functions listed=%d" % len(fnames))
        fn_l = " ".join(fnames).lower()

        hits, nfiles = _repo_hits(root, slugs)
        R.log("repo files scanned=%d" % nfiles)

        rows = []
        orphans, overlay, keepers = [], [], []
        for p in providers:
            slug = p.get("slug") or ""
            hf = p.get("hot_feeds")
            try:
                hf_n = int(hf) if hf is not None else 0
            except Exception:
                hf_n = 0
            ds = p.get("datasets") or 0
            fh = p.get("freshest_h")
            repo_n = len(hits.get(slug) or [])
            in_lambda = slug.replace("-", "") in fn_l.replace("-", "") or slug in fn_l
            verdict = "KEEP"
            note = p.get("catalog_note") or ""
            if slug in OVERLAY:
                verdict = "CANCEL_CANDIDATE"
                note = OVERLAY[slug]
            elif slug == "kr-ecos" or (ds == 0 and (fh is None)):
                verdict = "DEAD"
                note = "zero series or no freshness -- do not renew a key"
            elif hf_n == 0 and repo_n <= 1 and not in_lambda:
                verdict = "ORPHAN"
                note = "catalogued, almost no page/engine reference"
            elif slug in KEEP:
                verdict = "KEEP"
            row = {
                "slug": slug,
                "name": p.get("name"),
                "datasets": ds,
                "hot_feeds": hf_n,
                "freshest_h": fh,
                "mb": p.get("total_mb"),
                "repo_refs": repo_n,
                "sample_refs": hits.get(slug) or [],
                "lambda_name_hit": bool(in_lambda),
                "verdict": verdict,
                "note": note[:240] if note else None,
            }
            rows.append(row)
            if verdict == "ORPHAN":
                orphans.append(slug)
            elif verdict in ("CANCEL_CANDIDATE", "DEAD"):
                overlay.append(slug)
            else:
                keepers.append(slug)
            R.kv(slug=slug, verdict=verdict, hot=hf_n, refs=repo_n, ds=ds)

        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": 1,
            "source_catalog_as_of": cat.get("as_of"),
            "lambda_count": len(fnames),
            "repo_files_scanned": nfiles,
            "counts": {
                "providers": len(rows),
                "keep": len(keepers),
                "orphan": len(orphans),
                "cancel_or_dead": len(overlay),
            },
            "orphans": orphans,
            "cancel_candidates": overlay,
            "providers": rows,
        }
        body = json.dumps(payload, default=str).encode("utf-8")
        s3.put_object(
            Bucket=B,
            Key="data/provider-consumption.json",
            Body=body,
            ContentType="application/json",
        )
        back = _get_json(s3, "data/provider-consumption.json")
        if back.get("_error") or back.get("schema_version") != 1:
            R.fail("read-back failed")
            sys.exit(1)

        R.section("verdict")
        R.log("KEEP=%d ORPHAN=%d CANCEL/DEAD=%d" % (
            len(keepers), len(orphans), len(overlay)))
        R.log("cancel/dead: %s" % (", ".join(overlay) or "none"))
        R.log("orphans (first 20): %s" % ", ".join(orphans[:20]))
        R.ok("GREEN -- data/provider-consumption.json written and read back")


if __name__ == "__main__":
    main()
