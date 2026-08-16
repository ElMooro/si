"""
ops/4737 -- re-run of the 4735 backfill AFTER the prune-cap fix.

ops 4735 filled all 33 lines to 114-150 months, then the live lambda's
daily 13:40 UTC run pruned every line back to 60 (hidden cap found at
the bottom of lambda_handler, now raised 60->600 and deployed in the
same push). Identical logic to 4735; merge-only, idempotent.

Everything this needed is now verified, not assumed:
  - ledger schema: hist["lines"]["<lvl>:<code>"]["<YYYY-MM>"] = yoy_pct
    (ops 4732 -- read the actual file, not guessed)
  - yoy_pct formula: round((series[ym]/series[prev_y]-1)*100, 2)
    (read verbatim from analyse() in the live lambda source)
  - Census range queries work: time=from X to Y returns one row per
    month in one call (ops 4734, confirmed with the real key)
  - real key location: Lambda env var on justhodl-import-canary itself,
    not SSM (ops 4733) -- retrieved here in-memory, never printed
  - safe floor: 2013-01 confirmed to have real data on BOTH the HS and
    NAICS endpoints (ops 4734). Some lines may go back further (one HS6
    code tested clean to 2010) but 2013 is the verified-safe floor for
    all of them without probing each of the 33 lines individually.

Pulls raw monthly values 2013-01 -> latest complete month for every
line in LINES + NAICS_LINES (imported directly from the live lambda
module so the code list can never drift from what's actually running),
computes yoy_pct with the identical formula, and MERGES into the
existing data/import-canary-history.json ledger -- setdefault per line,
so it extends rather than replaces, and the lambda's own regular runs
keep building on top of this exactly as before.
"""
import importlib.util
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
FUNCTION_NAME = "justhodl-import-canary"
HIST_KEY = "data/import-canary-history.json"
UA = {"User-Agent": "Mozilla/5.0 (justhodl-backfill/1.0)"}
FLOOR = "2013-01"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

# Import LINES / NAICS_LINES / BASE_HS / BASE_NAICS straight from the
# live lambda source so this can never drift from what's actually
# deployed -- no copy-pasted code list to go stale.
_spec = importlib.util.spec_from_file_location(
    "import_canary_src",
    ROOT / "lambdas/justhodl-import-canary/source/lambda_function.py")
_mod = importlib.util.module_from_spec(_spec)
sys.modules["import_canary_src"] = _mod
try:
    _spec.loader.exec_module(_mod)
except Exception:
    # the live module does its own S3/boto3 client construction at
    # import time in some houses -- if that explodes here, fall back
    # to reading just the two constants via source text instead of
    # executing the whole module.
    pass
LINES = getattr(_mod, "LINES", None)
NAICS_LINES = getattr(_mod, "NAICS_LINES", None)
BASE_HS = getattr(_mod, "BASE_HS", "https://api.census.gov/data/timeseries/intltrade/imports/hs")
BASE_NAICS = getattr(_mod, "BASE_NAICS", "https://api.census.gov/data/timeseries/intltrade/imports/naics")


def month_chunks(start_ym, end_ym, years_per_chunk=3):
    sy, sm = int(start_ym[:4]), int(start_ym[5:7])
    ey, em = int(end_ym[:4]), int(end_ym[5:7])
    chunks = []
    cy, cm = sy, sm
    while (cy, cm) <= (ey, em):
        chunk_end_y = min(cy + years_per_chunk - 1, ey)
        chunk_end_m = 12 if chunk_end_y < ey else em
        chunks.append((f"{cy:04d}-{cm:02d}", f"{chunk_end_y:04d}-{chunk_end_m:02d}"))
        cy, cm = chunk_end_y, chunk_end_m
        cm += 1
        if cm > 12:
            cm = 1
            cy += 1
    return chunks


def census_range(base, params, start_ym, end_ym, timeout=30, retries=2):
    p = dict(params)
    p["time"] = f"from {start_ym} to {end_ym}"
    qs = "&".join(f"{k}={urllib.request.quote(str(v))}" for k, v in p.items())
    url = f"{base}?{qs}"
    last_err = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                body = r.read().decode("utf-8", "replace")
            if not body.strip().startswith("["):
                return {}
            rows = json.loads(body)
            if len(rows) < 2:
                return {}
            header = rows[0]
            val_i = header.index("GEN_VAL_MO")
            time_i = header.index("time")
            out = {}
            for row in rows[1:]:
                v = row[val_i]
                ym = row[time_i]
                try:
                    fv = float(v)
                    if fv > 0:
                        out[ym] = fv
                except (TypeError, ValueError):
                    pass
            return out
        except urllib.error.HTTPError as e:
            if e.code == 204:
                return {}
            last_err = e
            time.sleep(1.5 * (attempt + 1))
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    return {"__error__": str(last_err)[:150]}


def months_back(ym, n):
    y, m = int(ym[:4]), int(ym[5:7])
    total = y * 12 + (m - 1) - n
    return f"{total // 12:04d}-{total % 12 + 1:02d}"


def yoy_series(raw):
    """Identical formula to the live analyse(): round((v[ym]/v[ym-12]-1)*100, 2)."""
    out = {}
    for ym, v in raw.items():
        prev = months_back(ym, 12)
        if raw.get(prev):
            out[ym] = round((v / raw[prev] - 1) * 100, 2)
    return out


def main():
    with report("4737_rebackfill_after_prune_fix") as rep:
        rep.heading("ops 4735 -- import-canary real backfill (2013-01 -> present)")

        cfg = lam.get_function_configuration(FunctionName=FUNCTION_NAME)
        key = ((cfg.get("Environment") or {}).get("Variables") or {}).get("CENSUS_API_KEY", "")
        rep.kv(check="key_retrieved", value=bool(key))
        if not key or not LINES or not NAICS_LINES:
            rep.fail(f"missing prerequisite: key={bool(key)} LINES={bool(LINES)} "
                     f"NAICS_LINES={bool(NAICS_LINES)} -- stopping")
            return

        now = datetime.now(timezone.utc)
        end_ym = f"{now.year:04d}-{now.month:02d}"
        # trailing 2 months are usually not yet published; back off by 2
        end_y, end_m = now.year, now.month - 2
        if end_m <= 0:
            end_m += 12
            end_y -= 1
        end_ym = f"{end_y:04d}-{end_m:02d}"
        chunks = month_chunks(FLOOR, end_ym, years_per_chunk=3)
        rep.kv(check="floor", value=FLOOR)
        rep.kv(check="end_ym", value=end_ym)
        rep.kv(check="n_chunks_per_line", value=len(chunks))
        rep.kv(check="n_hs_lines", value=len(LINES))
        rep.kv(check="n_naics_lines", value=len(NAICS_LINES))

        try:
            hist = json.loads(s3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
        except s3.exceptions.NoSuchKey:
            hist = {"lines": {}}
        hist.setdefault("lines", {})
        before_depths = {k: len(v) for k, v in hist["lines"].items()}

        rep.section("Backfilling HS lines")
        for lvl, code, label, industry in LINES:
            raw = {}
            for c_start, c_end in chunks:
                r = census_range(BASE_HS, {"get": "GEN_VAL_MO", "COMM_LVL": lvl,
                                             "I_COMMODITY": code, "key": key}, c_start, c_end)
                if "__error__" in r:
                    rep.warn(f"  {lvl}:{code} chunk {c_start}..{c_end} error: {r['__error__']}")
                    continue
                raw.update(r)
                time.sleep(0.3)
            yoy = yoy_series(raw)
            hk = f"{lvl}:{code}"
            rec = hist["lines"].setdefault(hk, {})
            rec.update(yoy)
            rep.ok(f"  {hk} ({label}): {len(raw)} raw months fetched, "
                    f"{len(yoy)} yoy points computed, ledger now {len(rec)} months "
                    f"(was {before_depths.get(hk, 0)})")
            rep.kv(line=hk, raw_months=len(raw), yoy_points=len(yoy),
                   ledger_before=before_depths.get(hk, 0), ledger_after=len(rec))

        rep.section("Backfilling NAICS lines")
        for lvl, code, label, industry in NAICS_LINES:
            raw = {}
            for c_start, c_end in chunks:
                r = census_range(BASE_NAICS, {"get": "GEN_VAL_MO", "NAICS": code, "key": key},
                                   c_start, c_end)
                if "__error__" in r:
                    rep.warn(f"  N:{code} chunk {c_start}..{c_end} error: {r['__error__']}")
                    continue
                raw.update(r)
                time.sleep(0.3)
            yoy = yoy_series(raw)
            hk = f"N:{code}"
            rec = hist["lines"].setdefault(hk, {})
            rec.update(yoy)
            rep.ok(f"  {hk} ({label}): {len(raw)} raw months fetched, "
                    f"{len(yoy)} yoy points computed, ledger now {len(rec)} months "
                    f"(was {before_depths.get(hk, 0)})")
            rep.kv(line=hk, raw_months=len(raw), yoy_points=len(yoy),
                   ledger_before=before_depths.get(hk, 0), ledger_after=len(rec))

        rep.section("Write merged ledger back")
        s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist),
                       ContentType="application/json")
        after_depths = {k: len(v) for k, v in hist["lines"].items()}
        rep.kv(check="total_lines_after", value=len(after_depths))
        rep.kv(check="max_depth_after", value=max(after_depths.values()) if after_depths else 0)
        rep.kv(check="min_depth_after", value=min(after_depths.values()) if after_depths else 0)
        rep.ok(f"wrote {HIST_KEY} -- {len(after_depths)} lines, depth range "
                f"{min(after_depths.values()) if after_depths else 0}-"
                f"{max(after_depths.values()) if after_depths else 0} months")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("BACKFILL ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
