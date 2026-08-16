"""
ops/4743 -- crack ambs/tsy/seclending/soma path grammar, bank what proves.

ops 4742 banked fxs on its SECOND shape ({fam}/all/search.json) and
proved the docs page is a JS app (0 extractable paths). The remaining
families rejected both 2-segment shapes -- consistent with deeper
grammars (operation type / security type / status segments). This op
walks a segment-depth grid per family: /api/{fam}/ + ('all/' * k) for
k in 2..4, each tried with 'results/search.json' and bare
'search.json'. First candidate returning real rows on a 2024 probe
wins and gets the full 2000->present yearly bank, same idempotent
permanent pattern. soma is special-cased: it is holdings-as-of, not
operations -- probe /api/soma/summary.json (full weekly summary
history if it returns dated rows -> bank), and count
/api/soma/asofdates/list.json (per-CUSIP holdings = flagged
NEEDS_DEDICATED_ENGINE, never half-banked). Anything that still
refuses is reported as-is; next escalation is reading the site's JS
bundle for route strings, not guessing blind.
"""
import gzip
import json
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


def bank_family(rep, fam, base):
    year_now = int(time.strftime("%Y"))
    all_rows = []
    for y in range(2000, year_now + 1):
        r = get(f"{base}?startDate={y}-01-01&endDate={y}-12-31")
        if r["ok"]:
            try:
                all_rows.extend(first_rows(json.loads(r["body"])))
            except Exception:
                pass
        if len(all_rows) > ROW_CAP:
            rep.warn(f"{fam}: exceeded {ROW_CAP} rows -- NEEDS_DEDICATED_ENGINE")
            rep.kv(family=fam, banked=False, reason="row_cap")
            return
        time.sleep(0.3)
    seen = set()
    deduped = []
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
    with report("4743_nyfed_grammar_grid_and_soma") as rep:
        rep.heading("ops 4743 -- segment-depth grid for ambs/tsy/seclending + soma special-case")

        rep.section("A. Grid search: ambs / tsy / seclending")
        for fam in ("ambs", "tsy", "seclending"):
            if time.time() - started > TIME_CAP_S:
                rep.warn("time cap -- rerun resumes")
                break
            base = None
            for k in (2, 3, 4):
                for tail in ("results/search.json", "search.json"):
                    c = f"{NYF}/api/{fam}/" + "all/" * k + tail
                    r = get(f"{c}?startDate=2024-01-01&endDate=2024-12-31")
                    body = r.get("body", "")
                    rows = []
                    if r["ok"] and (body.strip().startswith("{") or
                                     body.strip().startswith("[")):
                        try:
                            rows = first_rows(json.loads(body))
                        except Exception:
                            rows = []
                    rep.log(f"{fam}: {c.rsplit('/api/',1)[1]} -> "
                            f"status={r.get('status')} rows={len(rows)}")
                    if rows:
                        base = c
                        break
                if base:
                    break
            if base:
                bank_family(rep, fam, base)
            else:
                rep.kv(family=fam, banked=False, reason="grid_exhausted")
                rep.warn(f"{fam}: grid exhausted 2-4 segments -- next escalation "
                         "is the site's JS bundle route strings, not more guessing")

        rep.section("B. soma special-case")
        r = get(f"{NYF}/api/soma/summary.json")
        rows = []
        if r["ok"]:
            try:
                rows = first_rows(json.loads(r.get("body", "")))
            except Exception:
                rows = []
        rep.log(f"soma/summary.json -> status={r.get('status')} rows={len(rows)}")
        if rows:
            dd = sorted(set(deep_dates(rows)))
            out_key = f"{WARM_PREFIX}soma-summary-history.json.gz"
            out = {"source": "NY Fed Markets API -- SOMA summary",
                   "endpoint": f"{NYF}/api/soma/summary.json",
                   "banked_at": datetime.now(timezone.utc).isoformat(),
                   "n_rows": len(rows), "earliest": dd[0] if dd else None,
                   "latest": dd[-1] if dd else None, "rows": rows}
            s3.put_object(Bucket=BUCKET, Key=out_key,
                           Body=gzip.compress(json.dumps(out,
                                separators=(",", ":")).encode()),
                           ContentType="application/json", ContentEncoding="gzip")
            rep.ok(f"soma summary: banked {len(rows)} rows, "
                    f"{dd[0] if dd else '-'} -> {dd[-1] if dd else '-'}")
            rep.kv(family="soma-summary", banked=True, n_rows=len(rows),
                   earliest=dd[0] if dd else None, latest=dd[-1] if dd else None)
        r2 = get(f"{NYF}/api/soma/asofdates/list.json")
        n_asof = 0
        if r2["ok"]:
            try:
                n_asof = len(first_rows(json.loads(r2.get("body", ""))))
            except Exception:
                pass
        rep.kv(check="soma_asof_dates_available", value=n_asof)
        if n_asof:
            rep.log(f"per-CUSIP holdings exist for {n_asof} as-of dates -- "
                    "that is a dedicated-engine build (row volume), flagged, "
                    "never half-banked here")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
