"""
ops/4739 -- securities lending: discover, bank full history to warm.

ops 4738 found the two real gaps in the repo stack: (1) NY Fed
securities lending -- SOMA collateral lent out, the rehypothecation-
adjacent dataset -- is pulled by no engine, and my endpoint guess
returned 400 (path exists, wrong shape); (2) my depth-probe parser
misread the warm OFR file layout and reported 0 obs on a file that
plainly has data.

This op: (A) re-proves depth on two banked OFR series with a robust
nested parse; (B) walks a candidate list of seclending endpoint shapes
(modeled on rp/results/search.json, which nyfed-repo-deep uses today)
and takes the first that returns real JSON; (C) if found, banks the
FULL history 2000->present in yearly search windows, merged and
deduped, gzipped to data/warm/nyfed-markets/seclending-history.json.gz
(covered by the verified deny-Delete policy + existing nyfed REG
prefix -- permanent and visible on data.html with zero extra wiring),
plus a small hot summary at data/seclending.json. Idempotent: re-runs
merge, never clobber. If discovery fails, it says so and writes nothing.
"""
import gzip
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
NYF = "https://markets.newyorkfed.org/api"
WARM_KEY = "data/warm/nyfed-markets/seclending-history.json.gz"
HOT_KEY = "data/seclending.json"
UA = {"User-Agent": "Mozilla/5.0 (justhodl-seclending/1.0)"}

s3 = boto3.client("s3", region_name=REGION)


def get_url(url, timeout=30):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            return {"ok": True, "status": r.status, "body": raw.decode("utf-8", "replace")}
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code,
                 "body": (e.read()[:200].decode("utf-8", "replace") if e.fp else "")}
    except Exception as e:
        return {"ok": False, "status": None, "error": f"{type(e).__name__}: {str(e)[:120]}"}


def deep_dates(obj, out=None, depth=0):
    """Collect ISO-date-shaped strings anywhere in a nested structure."""
    if out is None:
        out = []
    if depth > 6:
        return out
    if isinstance(obj, str):
        if len(obj) >= 10 and obj[:4].isdigit() and obj[4] == "-" and obj[7] == "-":
            out.append(obj[:10])
    elif isinstance(obj, (list, tuple)):
        for x in obj[:20000]:
            deep_dates(x, out, depth + 1)
    elif isinstance(obj, dict):
        for v in obj.values():
            deep_dates(v, out, depth + 1)
    return out


def s3_json(key):
    raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("4739_seclending_bank_full_history") as rep:
        rep.heading("ops 4739 -- NY Fed securities lending: discover + bank full history")

        rep.section("A. Corrected depth proof on banked OFR series")
        for m in ("REPO-TRIV1_AR_AG-F", "FNYR-SOFR-A"):
            try:
                doc = s3_json(f"data/warm/ofr/series/{m}.json.gz")
                dates = sorted(set(deep_dates(doc)))
                rep.kv(mnemonic=m, n_dates=len(dates),
                       earliest=dates[0] if dates else None,
                       latest=dates[-1] if dates else None)
                rep.ok(f"{m}: {len(dates)} distinct dates, "
                        f"{dates[0] if dates else '-'} -> {dates[-1] if dates else '-'}")
            except Exception as e:
                rep.warn(f"{m}: {type(e).__name__}: {str(e)[:120]}")

        rep.section("B. Endpoint discovery -- candidate shapes")
        candidates = [
            f"{NYF}/seclending/results/search.json?startDate=2026-07-01&endDate=2026-08-16",
            f"{NYF}/seclending/all/results/search.json?startDate=2026-07-01&endDate=2026-08-16",
            f"{NYF}/seclending/all/results/lastTwoWeeks.json",
            f"{NYF}/seclending/results/lastTwoWeeks.json",
            f"{NYF}/seclending/all/results/last/10.json",
        ]
        found = None
        rows_key = None
        for url in candidates:
            r = get_url(url)
            body = r.get("body", "")
            looks_json = body.strip().startswith("{") or body.strip().startswith("[")
            rep.log(f"{url.rsplit('/api/', 1)[1]} -> status={r.get('status')} json={looks_json} "
                    f"body[:150]={body[:150]}")
            if r["ok"] and looks_json:
                try:
                    d = json.loads(body)
                    if isinstance(d, dict):
                        for k, v in d.items():
                            if isinstance(v, list) and v:
                                found, rows_key = url, k
                                break
                        else:
                            # dict but no non-empty list; still note shape
                            if not found:
                                rep.log(f"  parsed dict keys: {list(d.keys())[:8]}")
                    elif isinstance(d, list) and d:
                        found, rows_key = url, None
                except Exception:
                    pass
            if found:
                break
        rep.kv(check="seclending_endpoint_found", value=bool(found))
        if not found:
            rep.warn("no candidate returned usable JSON -- NOT writing anything. "
                     "Next step is reading NY Fed's API doc page for the exact "
                     "seclending path rather than guessing further shapes.")
            return
        rep.kv(check="seclending_endpoint", value=found)
        rep.kv(check="rows_key", value=rows_key or "(bare list)")

        rep.section("C. Bank full history 2000 -> present in yearly windows")
        base = found.split("?")[0]
        if "search.json" not in base:
            base = found.rsplit("/results/", 1)[0] + "/results/search.json"
        all_rows = []
        errs = 0
        year_now = int(time.strftime("%Y"))
        for y in range(2000, year_now + 1):
            url = f"{base}?startDate={y}-01-01&endDate={y}-12-31"
            r = get_url(url)
            if not r["ok"]:
                errs += 1
                rep.log(f"  {y}: status={r.get('status')} err={r.get('error','')[:80]}")
                time.sleep(1.0)
                continue
            try:
                d = json.loads(r["body"])
                rows = d.get(rows_key) if (rows_key and isinstance(d, dict)) else d
                if isinstance(d, dict) and not isinstance(rows, list):
                    for k, v in d.items():
                        if isinstance(v, list):
                            rows = v
                            break
                n = len(rows) if isinstance(rows, list) else 0
                if n:
                    all_rows.extend(rows)
                rep.log(f"  {y}: {n} rows")
            except Exception as e:
                errs += 1
                rep.log(f"  {y}: parse {type(e).__name__}")
            time.sleep(0.5)
        # dedupe on the full row repr keyed by its most identifying fields
        seen = set()
        deduped = []
        for row in all_rows:
            k = json.dumps(row, sort_keys=True) if isinstance(row, dict) else str(row)
            if k not in seen:
                seen.add(k)
                deduped.append(row)
        dates = sorted(set(deep_dates(deduped)))
        rep.kv(check="rows_fetched", value=len(all_rows))
        rep.kv(check="rows_deduped", value=len(deduped))
        rep.kv(check="window_errors", value=errs)
        rep.kv(check="earliest_date", value=dates[0] if dates else None)
        rep.kv(check="latest_date", value=dates[-1] if dates else None)
        if not deduped:
            rep.warn("0 rows across all windows -- NOT writing empty history")
            return

        # merge with any prior banked doc (idempotent)
        prior = []
        try:
            prior_doc = s3_json(WARM_KEY)
            prior = prior_doc.get("rows") or []
        except Exception:
            pass
        for row in prior:
            k = json.dumps(row, sort_keys=True) if isinstance(row, dict) else str(row)
            if k not in seen:
                seen.add(k)
                deduped.append(row)
        out = {"source": "NY Fed Markets API — securities lending operations",
               "endpoint": base, "banked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "n_rows": len(deduped), "earliest": dates[0] if dates else None,
               "latest": dates[-1] if dates else None, "rows": deduped}
        s3.put_object(Bucket=BUCKET, Key=WARM_KEY,
                       Body=gzip.compress(json.dumps(out, separators=(",", ":")).encode()),
                       ContentType="application/json", ContentEncoding="gzip")
        rep.ok(f"banked {len(deduped)} rows -> {WARM_KEY} (deny-Delete + versioned = permanent)")

        hot = {"source": out["source"], "updated_at": out["banked_at"],
               "n_rows_total": len(deduped), "earliest": out["earliest"],
               "latest": out["latest"],
               "recent": deduped[-40:] if len(deduped) > 40 else deduped}
        s3.put_object(Bucket=BUCKET, Key=HOT_KEY,
                       Body=json.dumps(hot, separators=(",", ":")),
                       ContentType="application/json")
        rep.ok(f"wrote hot summary -> {HOT_KEY}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
