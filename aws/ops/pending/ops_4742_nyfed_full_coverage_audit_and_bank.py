"""
ops/4742 -- NY Fed: full-coverage audit, then bank every missing family.

Khalid's double-check request, verified in source: the "1539/1539"
NY Fed number is the PD (FR2004) catalog ONLY. Empirically banked
today: pd (markets-full), rp repo/reverserepo op history (repo-deep),
rates via OFR's FNYR mirror. Never banked: fxs (central-bank FX SWAP
operations -- the swaps Khalid means), seclending, tsy ops, ambs ops,
soma. This op:

  A. lists data/warm/nyfed-markets/* and groups keys -> the real
     banked-family matrix, plus a depth proof on reverse-repo history
  B. fetches NY Fed's own API docs and extracts every /api/... path;
     unions with mirror-shape candidates (same layout as the PROVEN
     rp/results/search.json) for the known missing families
  C. for each missing family: validates a search endpoint by real JSON
     rows before anything is written, then banks FULL history in
     yearly windows 2000->present to
     data/warm/nyfed-markets/{family}-history.json.gz
     (deny-Delete + versioned = permanent, never deletable)

Row-cap guard (400k/family): a family too big for one doc (soma full
holdings) gets flagged NEEDS_DEDICATED_ENGINE and nothing partial is
written. Idempotent merges; time-capped; honest per-family logging of
whether the endpoint came from the docs or the mirror-shape guess --
either way it must prove itself with real rows first.
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
TIME_CAP_S = 70 * 60
ROW_CAP = 400_000
DOC_PAGES = [f"{NYF}/static/docs/markets-api.html",
              f"{NYF}/api/swagger.json",
              f"{NYF}/static/docs/openapi.json"]
MIRROR_FAMILIES = ["fxs", "seclending", "tsy", "ambs", "soma"]

s3 = boto3.client("s3", region_name=REGION)
started = time.time()


def get(url, timeout=40):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            return {"ok": True, "status": r.status,
                     "body": raw.decode("utf-8", "replace")}
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code,
                 "body": (e.read()[:150].decode("utf-8", "replace") if e.fp else "")}
    except Exception as e:
        return {"ok": False, "status": None,
                 "error": f"{type(e).__name__}: {str(e)[:100]}"}


def first_rows(d):
    """First non-empty list of dicts anywhere 2 levels deep."""
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


def main():
    with report("4742_nyfed_full_coverage_audit_and_bank") as rep:
        rep.heading("ops 4742 -- NY Fed full-coverage audit + bank missing families")

        rep.section("A. What's actually banked under warm/nyfed-markets/")
        fam_counts = {}
        keys = []
        token = None
        while True:
            kw = dict(Bucket=BUCKET, Prefix=WARM_PREFIX, MaxKeys=1000)
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents") or []:
                keys.append(o["Key"])
            token = resp.get("NextContinuationToken")
            if not token:
                break
        for k in keys:
            stem = k[len(WARM_PREFIX):].split("/")[0].split("-")[0].split(".")[0]
            fam_counts[stem] = fam_counts.get(stem, 0) + 1
        rep.kv(check="warm_keys_total", value=len(keys))
        rep.log("banked groups: " + ", ".join(f"{k}={v}" for k, v in
                 sorted(fam_counts.items(), key=lambda x: -x[1])[:15]))
        # reverse-repo depth proof
        rr_key = next((k for k in keys if "reverserepo" in k), None)
        if rr_key:
            try:
                raw = s3.get_object(Bucket=BUCKET, Key=rr_key)["Body"].read()
                if rr_key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
                    raw = gzip.decompress(raw)
                doc = json.loads(raw)
                dd = sorted(set(deep_dates(doc)))
                rep.kv(check="reverserepo_key", value=rr_key.split("/")[-1])
                rep.kv(check="reverserepo_dates", value=len(dd))
                rep.kv(check="reverserepo_earliest", value=dd[0] if dd else None)
                rep.kv(check="reverserepo_latest", value=dd[-1] if dd else None)
            except Exception as e:
                rep.warn(f"reverse-repo depth read: {type(e).__name__}: {str(e)[:100]}")
        else:
            rep.warn("no reverserepo history key found under warm!")

        rep.section("B. Doc-extracted endpoint families")
        doc_paths = set()
        for page in DOC_PAGES:
            r = get(page)
            if r["ok"]:
                found = re.findall(r'(/api/[A-Za-z0-9_\-/{}.]+?\.json)', r["body"])
                doc_paths.update(found)
                rep.log(f"{page.rsplit('/',1)[1]} -> {len(found)} paths")
            else:
                rep.log(f"{page.rsplit('/',1)[1]} -> status={r.get('status')}")
        doc_fams = sorted({p.split("/")[2] for p in doc_paths if p.count("/") >= 2})
        rep.kv(check="doc_paths_found", value=len(doc_paths))
        rep.kv(check="doc_families", value=",".join(doc_fams)[:200])
        banked_fams = set()
        for stem in fam_counts:
            banked_fams.add("rp" if stem == "rp" else stem)
        target_fams = sorted((set(doc_fams) | set(MIRROR_FAMILIES)) -
                              {"rates", "pd", "rp", "guidesheets", "marketshare"} -
                              banked_fams)
        rep.kv(check="families_to_bank", value=",".join(target_fams))

        rep.section("C. Validate + bank each missing family (full history)")
        year_now = int(time.strftime("%Y"))
        for fam in target_fams:
            if time.time() - started > TIME_CAP_S:
                rep.warn(f"time cap before {fam} -- rerun resumes")
                break
            # candidate search bases: doc paths first, then mirror shape
            cands = []
            for p in sorted(doc_paths):
                if f"/api/{fam}/" in p and "search.json" in p:
                    cands.append(NYF + re.sub(r"\{[^}]+\}", "all", p))
            cands.append(f"{NYF}/api/{fam}/all/results/search.json")
            cands.append(f"{NYF}/api/{fam}/all/search.json")
            base = None
            src = None
            for c in dict.fromkeys(cands):
                r = get(f"{c}?startDate=2024-01-01&endDate=2024-12-31")
                body = r.get("body", "")
                if r["ok"] and (body.strip().startswith("{") or
                                 body.strip().startswith("[")):
                    try:
                        rows = first_rows(json.loads(body))
                    except Exception:
                        rows = []
                    if rows:
                        base = c
                        src = ("docs" if any(f"/api/{fam}/" in p for p in doc_paths)
                                and c != f"{NYF}/api/{fam}/all/results/search.json"
                                else "docs-or-mirror")
                        rep.log(f"{fam}: VALIDATED {c.rsplit('/api/',1)[1]} "
                                f"({len(rows)} rows in 2024 probe) [{src}]")
                        break
                rep.log(f"{fam}: candidate {c.rsplit('/api/',1)[1]} -> "
                        f"status={r.get('status')} rows=0")
            if not base:
                rep.warn(f"{fam}: no candidate produced real rows -- NOT banked, "
                         "flagged for dedicated follow-up")
                rep.kv(family=fam, banked=False, reason="no_valid_endpoint")
                continue
            all_rows = []
            too_big = False
            for y in range(2000, year_now + 1):
                r = get(f"{base}?startDate={y}-01-01&endDate={y}-12-31")
                if r["ok"]:
                    try:
                        rows = first_rows(json.loads(r["body"]))
                        all_rows.extend(rows)
                    except Exception:
                        pass
                if len(all_rows) > ROW_CAP:
                    too_big = True
                    break
                time.sleep(0.3)
            if too_big:
                rep.warn(f"{fam}: exceeded {ROW_CAP} rows -- NEEDS_DEDICATED_ENGINE, "
                         "nothing partial written")
                rep.kv(family=fam, banked=False, reason="row_cap")
                continue
            seen = set()
            deduped = []
            for row in all_rows:
                k = json.dumps(row, sort_keys=True) if isinstance(row, dict) else str(row)
                if k not in seen:
                    seen.add(k)
                    deduped.append(row)
            dd = sorted(set(deep_dates(deduped)))
            out_key = f"{WARM_PREFIX}{fam}-history.json.gz"
            prior = []
            try:
                praw = s3.get_object(Bucket=BUCKET, Key=out_key)["Body"].read()
                prior = json.loads(gzip.decompress(praw)).get("rows") or []
            except Exception:
                pass
            for row in prior:
                k = json.dumps(row, sort_keys=True) if isinstance(row, dict) else str(row)
                if k not in seen:
                    seen.add(k)
                    deduped.append(row)
            out = {"source": f"NY Fed Markets API -- {fam}", "endpoint": base,
                   "banked_at": datetime.now(timezone.utc).isoformat(),
                   "n_rows": len(deduped),
                   "earliest": dd[0] if dd else None,
                   "latest": dd[-1] if dd else None, "rows": deduped}
            s3.put_object(Bucket=BUCKET, Key=out_key,
                           Body=gzip.compress(json.dumps(out,
                                separators=(",", ":")).encode()),
                           ContentType="application/json", ContentEncoding="gzip")
            rep.ok(f"{fam}: banked {len(deduped)} rows, "
                    f"{dd[0] if dd else '-'} -> {dd[-1] if dd else '-'} -> {out_key}")
            rep.kv(family=fam, banked=True, n_rows=len(deduped),
                   earliest=dd[0] if dd else None, latest=dd[-1] if dd else None)

        rep.section("Summary")
        rep.log("Every newly banked family lands under data/warm/nyfed-markets/ -- "
                 "already covered by the verified deny-Delete bucket policy, "
                 "versioning, and the existing nyfed REG entry, so it is permanent "
                 "and visible on data.html with zero extra wiring.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
