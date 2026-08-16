"""
ops/4744 -- read NY Fed's JS bundles for the real ambs/tsy/seclending routes.

The grid (ops 4743) exhausted honest guessing: 400s through four
segments. The docs page is a JS app -- which means the true route
strings live in its JavaScript bundles. This op fetches the homepage,
extracts <script src> bundle URLs (up to 5 files, 10MB guard each),
regexes every quoted /api/... string out of them, and reports the
distinct routes per family. Any ambs/tsy/seclending search-style route
found is validated with a real 2024 probe and, if it returns rows,
banked 2000->present to permanent warm on the spot -- same idempotent
pattern as fxs/soma. If the bundles yield nothing for a family, that
family is declared out of reach via this API surface and the follow-up
moves to NY Fed's non-API publications (CSV/XML pages), not more URL
guessing.
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
NYF = "https://markets.newyorkfed.org"
WARM_PREFIX = "data/warm/nyfed-markets/"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com"}
ROW_CAP = 400_000
FAMS = ("ambs", "tsy", "seclending")

s3 = boto3.client("s3", region_name=REGION)


def get(url, timeout=50, binary=False):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read(10_000_000)
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            return {"ok": True, "status": r.status,
                     "body": raw if binary else raw.decode("utf-8", "replace")}
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code, "body": ""}
    except Exception as e:
        return {"ok": False, "status": None, "body": "",
                 "error": f"{type(e).__name__}: {str(e)[:100]}"}


def first_rows(d):
    if isinstance(d, list):
        return d
    if isinstance(d, dict):
        for v in d.values():
            if isinstance(v, list) and v:
                return v
        for v in d.values():
            if isinstance(v, dict):
                for v2 in v.values():
                    if isinstance(v2, list) and v2:
                        return v2
    return []


def deep_dates(obj, out=None, depth=0):
    if out is None:
        out = []
    if depth > 6:
        return out
    if isinstance(obj, str):
        if len(obj) >= 10 and obj[:4].isdigit() and obj[4] == "-" and obj[7] == "-":
            out.append(obj[:10])
    elif isinstance(obj, (list, tuple)):
        for x in obj[:30000]:
            deep_dates(x, out, depth + 1)
    elif isinstance(obj, dict):
        for v in obj.values():
            deep_dates(v, out, depth + 1)
    return out


def bank(rep, fam, base):
    year_now = int(time.strftime("%Y"))
    all_rows = []
    for y in range(2000, year_now + 1):
        r = get(f"{base}?startDate={y}-01-01&endDate={y}-12-31")
        if r["ok"] and r["body"]:
            try:
                all_rows.extend(first_rows(json.loads(r["body"])))
            except Exception:
                pass
        if len(all_rows) > ROW_CAP:
            rep.warn(f"{fam}: row cap -- NEEDS_DEDICATED_ENGINE, nothing written")
            rep.kv(family=fam, banked=False, reason="row_cap")
            return
        time.sleep(0.3)
    seen, deduped = set(), []
    for row in all_rows:
        k = json.dumps(row, sort_keys=True) if isinstance(row, dict) else str(row)
        if k not in seen:
            seen.add(k)
            deduped.append(row)
    out_key = f"{WARM_PREFIX}{fam}-history.json.gz"
    try:
        praw = s3.get_object(Bucket=BUCKET, Key=out_key)["Body"].read()
        for row in json.loads(gzip.decompress(praw)).get("rows") or []:
            k = json.dumps(row, sort_keys=True) if isinstance(row, dict) else str(row)
            if k not in seen:
                seen.add(k)
                deduped.append(row)
    except Exception:
        pass
    dd = sorted(set(deep_dates(deduped)))
    out = {"source": f"NY Fed Markets API -- {fam}", "endpoint": base,
           "banked_at": datetime.now(timezone.utc).isoformat(),
           "n_rows": len(deduped), "earliest": dd[0] if dd else None,
           "latest": dd[-1] if dd else None, "rows": deduped}
    s3.put_object(Bucket=BUCKET, Key=out_key,
                   Body=gzip.compress(json.dumps(out, separators=(",", ":")).encode()),
                   ContentType="application/json", ContentEncoding="gzip")
    rep.ok(f"{fam}: banked {len(deduped)} rows, "
            f"{dd[0] if dd else '-'} -> {dd[-1] if dd else '-'}")
    rep.kv(family=fam, banked=True, n_rows=len(deduped),
           earliest=dd[0] if dd else None, latest=dd[-1] if dd else None)


def main():
    with report("4744_nyfed_js_bundle_routes") as rep:
        rep.heading("ops 4744 -- NY Fed JS bundle route extraction -> validate -> bank")

        rep.section("A. Collect bundles")
        home = get(NYF + "/")
        srcs = []
        if home["ok"]:
            srcs = re.findall(r'<script[^>]+src="([^"]+\.js[^"]*)"', home["body"])
        rep.kv(check="script_tags_found", value=len(srcs))
        blobs = []
        for s_url in srcs[:5]:
            absu = s_url if s_url.startswith("http") else NYF + (
                s_url if s_url.startswith("/") else "/" + s_url)
            r = get(absu)
            rep.log(f"bundle {absu.rsplit('/',1)[-1][:60]} -> status={r.get('status')} "
                    f"bytes={len(r.get('body',''))}")
            if r["ok"]:
                blobs.append(r["body"])
        corpus = "\n".join(blobs)
        rep.kv(check="bundle_corpus_bytes", value=len(corpus))

        rep.section("B. Extract /api/ route strings")
        routes = sorted(set(re.findall(
            r'["\'](/?api/[A-Za-z0-9_\-/{}$.]+)["\']', corpus)))
        rep.kv(check="distinct_api_routes", value=len(routes))
        fam_routes = {f: [r for r in routes if f"/{f}/" in ("/" + r)] for f in FAMS}
        for f in FAMS:
            rep.kv(check=f"routes_{f}", value=len(fam_routes[f]))
            for rt in fam_routes[f][:12]:
                rep.log(f"  {f}: {rt}")
        if len(routes) and not any(fam_routes.values()):
            rep.log("sample of all routes: " + " | ".join(routes[:15]))

        rep.section("C. Validate + bank")
        for f in FAMS:
            cands = []
            for rt in fam_routes[f]:
                path = rt if rt.startswith("/") else "/" + rt
                path = re.sub(r"\{[^}]+\}|\$\{[^}]+\}", "all", path)
                if not path.endswith(".json"):
                    path += ".json" if not path.endswith("/") else "search.json"
                if "search" in path:
                    cands.insert(0, NYF + path)
                else:
                    cands.append(NYF + path)
            hit = None
            for c in dict.fromkeys(cands)[:10] if cands else []:
                probe = c + ("&" if "?" in c else "?") + \
                    "startDate=2024-01-01&endDate=2024-12-31"
                r = get(probe)
                rows = []
                if r["ok"] and r["body"].strip()[:1] in "[{":
                    try:
                        rows = first_rows(json.loads(r["body"]))
                    except Exception:
                        rows = []
                rep.log(f"{f}: {c.rsplit('/api/',1)[-1][:70]} -> "
                        f"status={r.get('status')} rows={len(rows)}")
                if rows:
                    hit = c
                    break
            if hit:
                bank(rep, f, hit)
            else:
                rep.kv(family=f, banked=False, reason="no_route_validated")
                rep.warn(f"{f}: no bundle route validated -- declared out of reach "
                         "via this API surface; follow-up = NY Fed's non-API "
                         "publications for this family")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
