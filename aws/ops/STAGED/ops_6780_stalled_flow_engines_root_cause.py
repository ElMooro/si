"""ops 6780 - READ-ONLY follow-up to ops 6779 (the two stalled flow engines): the exact root causes.

6779 established: justhodl-ici-flows' weekly rule is DISABLED (switched off 2026-10-20 by the retire plan as a "broken
writer": its declared output data/ici-flows.json is missing on S3), ici.org answers 403 to AWS and GitHub alike, the
newest Wayback snapshot (2026-08-21) parses to nothing, and data/ici-fund-flows.json (what the Flows desk reads) has
only the 86-week hardcoded seed. justhodl-etf-true-flows runs twice a day and has failed every run since 2026-10-12
with failure_class ValueError at the iShares catalog identity step (the retained attempts say so), with an FMP 429
storm on most days.

This op settles what is left: (1) WHICH handler is deployed for each function - the deployed package is downloaded
through get_function's presigned URL and its files are hashed against the repo (source/*.py and the legacy copy);
(2) the native ICI engine's own request journals (audit-private/.../ici-research/requests/) - status, error and the
HTTP status it saw from ici.org; whether data/ici-flows.json exists; (3) the ETF failure's exact message - the
retained original iShares catalog bytes of the newest failed attempt are replayed through the repo's own
etf_native.ishares_catalog, then every universe ticker is checked field by field; the live screener is fetched from
the runner and replayed the same way; the FMP 429 share is counted. Invokes nothing, changes nothing, writes nothing
but this report.
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import gzip
import hashlib
import importlib.util
import io
import json
import re
import sys
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
ETF_SRC = ROOT / "aws" / "lambdas" / "justhodl-etf-true-flows" / "source"
sys.path.insert(0, str(ETF_SRC))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FUNCTIONS = ("justhodl-ici-flows", "justhodl-etf-true-flows")
ICI_PRIVATE_REQUESTS = "audit-private/20260909-originals/ici-research/requests/"
ISHARES_URL = ("https://www.ishares.com/us/product-screener/product-screener-v3.1.jsn"
               "?dcrPath=/templatedata/config/product-screener-v3/data/en/us-ishares/"
               "ishares-product-screener-backend-config&siteEntryPassthrough=true")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def repo_hashes(fn):
    out = {}
    base = ROOT / "aws" / "lambdas" / fn
    for p in sorted((base / "source").glob("*.py")):
        out[sha(p.read_bytes())] = "source/" + p.name
    for p in sorted((base / "tests").glob("legacy-*.py.txt")):
        out[sha(p.read_bytes())] = "tests/" + p.name
    return out


def ishares_field_check(doc, universe):
    """the same conditions as etf_native.ishares_catalog, one ticker at a time, never raising"""
    rows = []
    seen = set()
    for pid, v in doc.items():
        if not isinstance(v, dict):
            continue
        ticker = v.get("localExchangeTicker")
        if ticker not in universe:
            continue
        problems = []
        if ticker in seen:
            problems.append("duplicate ticker")
        seen.add(ticker)
        if str(v.get("portfolioId")) != pid:
            problems.append("portfolioId %r != key %r" % (v.get("portfolioId"), pid))
        if not re.fullmatch(r"/us/products/" + re.escape(pid) + r"/[a-z0-9-]+", v.get("productPageUrl", "") or ""):
            problems.append("productPageUrl %r" % (v.get("productPageUrl"),))
        if not re.fullmatch(r"US[A-Z0-9]{10}", v.get("isin", "") or ""):
            problems.append("isin %r" % (v.get("isin"),))
        for field in ("fundName", "navAmount", "navAmountAsOf", "totalNetAssets", "totalNetAssetsFundAsOf"):
            if field not in v:
                problems.append("missing %s" % field)
            elif field in ("navAmount", "navAmountAsOf", "totalNetAssets", "totalNetAssetsFundAsOf"):
                val = v.get(field)
                if not isinstance(val, dict) or "r" not in val:
                    problems.append("%s not {r:…}: %r" % (field, str(val)[:40]))
                elif field in ("navAmount", "totalNetAssets") and (val.get("r") is None or str(val.get("r")).strip() in ("", "--", "-")):
                    problems.append("%s.r empty" % field)
                elif field.endswith("AsOf"):
                    try:
                        datetime.strptime(str(val.get("r")), "%Y%m%d")
                    except Exception:  # noqa: BLE001
                        problems.append("%s.r %r not YYYYMMDD" % (field, val.get("r")))
        rows.append((ticker, problems))
    return rows


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=60)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    facts = {}
    with report("ops_6780_stalled_flow_engines_root_cause") as r:
        r.heading("ops 6780 - stalled flow engines, exact root causes (%s; read-only)" % now.isoformat(timespec="minutes"))

        r.section("1. which handler is deployed (deployed package files hashed against the repo)")
        for fn in FUNCTIONS:
            try:
                g = lam.get_function(FunctionName=fn)
                loc = g["Code"]["Location"]
                with urllib.request.urlopen(loc, timeout=120) as resp:
                    blob = resp.read()
                z = zipfile.ZipFile(io.BytesIO(blob))
                known = repo_hashes(fn)
                top = [i for i in z.infolist() if "/" not in i.filename.rstrip("/")]
                r.log("%s: package %d B, %d entries, %d top-level; config sha %s" % (fn, len(blob), len(z.infolist()), len(top), g["Configuration"].get("CodeSha256")))
                matched = {}
                for i in sorted(top, key=lambda x: x.filename):
                    if i.filename.endswith("/"):
                        continue
                    raw = z.read(i.filename)
                    h = sha(raw)
                    m = known.get(h)
                    if i.filename.endswith(".py") or i.filename in ("lambda_function.py",):
                        r.log("  %-32s %7d B  %s" % (i.filename, i.file_size, ("= repo " + m) if m else "(not in the repo's source/ or legacy copy)"))
                    if m:
                        matched[i.filename] = m
                handler = z.read("lambda_function.py") if "lambda_function.py" in z.namelist() else b""
                head = handler.decode("utf-8", "ignore")[:160].replace("\n", " | ")
                r.log("  deployed lambda_function.py: %d B, first line(s): %s" % (len(handler), head))
                facts[fn.replace("justhodl-", "") + "_deployed_handler"] = matched.get("lambda_function.py", "NOT the repo's source/lambda_function.py (%d B)" % len(handler))
                shared = sorted(i.filename for i in top if i.filename.endswith(".py") and i.filename not in matched)
                if shared:
                    r.log("  other top-level .py in the package (shared modules or older files): %s" % ", ".join(shared)[:600])
            except Exception as exc:  # noqa: BLE001
                r.warn("%s: %s: %s" % (fn, type(exc).__name__, str(exc)[:200]))

        r.section("2. the native ICI engine (deployed 2026-09-30): what it saw before it was switched off")
        for key in ("data/ici-flows.json", "data/ici-fund-flows.json"):
            try:
                h = s3.head_object(Bucket=LIVE, Key=key)
                r.log("%s: exists, %d B, LastModified %s" % (key, h["ContentLength"], h["LastModified"].isoformat(timespec="minutes")))
                facts[key] = "exists %s" % h["LastModified"].date()
            except Exception as exc:  # noqa: BLE001
                r.log("%s: %s" % (key, "ABSENT (404)" if "404" in str(exc) or "Not Found" in str(exc) else str(exc)[:120]))
                facts[key] = "absent"
        try:
            objs = []
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix=ICI_PRIVATE_REQUESTS):
                objs += page.get("Contents", [])
            objs.sort(key=lambda o: o["LastModified"])
            per_day = Counter(o["LastModified"].date().isoformat() for o in objs)
            r.log("%s: %d request journals, first %s, newest %s; per day: %s" % (
                ICI_PRIVATE_REQUESTS, len(objs), objs[0]["LastModified"].date() if objs else "-", objs[-1]["LastModified"].isoformat(timespec="minutes") if objs else "-",
                ", ".join("%s:%d" % (d[5:], n) for d, n in sorted(per_day.items())[-16:])))
            statuses = Counter()
            for o in objs[-8:]:
                try:
                    j = json.loads(s3.get_object(Bucket=LIVE, Key=o["Key"])["Body"].read())
                    srcs = {k: (v.get("http_status"), str((v.get("response_headers") or {}).get("server") or (v.get("response_headers") or {}).get("Server") or "")[:20]) for k, v in (j.get("sources") or {}).items()}
                    statuses[(j.get("status"), j.get("error_type"))] += 1
                    r.log("  %s status %s error %s attempts %s sources %s" % (o["LastModified"].isoformat(timespec="minutes"), j.get("status"), j.get("error_type"), j.get("provider_request_attempts"), json.dumps(srcs)))
                except Exception as exc:  # noqa: BLE001
                    r.warn("  journal %s: %s" % (o["Key"][-20:], str(exc)[:100]))
            facts["ici_native_journal_statuses"] = json.dumps({"%s/%s" % k: v for k, v in statuses.items()})
        except Exception as exc:  # noqa: BLE001
            r.warn("ici request journals: %s: %s" % (type(exc).__name__, str(exc)[:160]))

        r.section("3. the ETF engine's failure, exactly (retained original iShares catalog replayed through the repo's parser)")
        try:
            import etf_native as native  # noqa: E402  (repo module; needs aws/shared on the path for evidence_store)
            import etf_universe
            universe = sorted({t for members in etf_universe.ETFS.values() for t in members})
            r.log("universe: %d tickers" % len(universe))
            attempts = []
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix="data/etf-research/attempts/"):
                attempts += page.get("Contents", [])
            attempts.sort(key=lambda o: o["LastModified"])
            picks = [attempts[-1]] + [a for a in attempts if a["LastModified"].date().isoformat() in ("2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16")][:2]
            seen_inputs = set()
            for a in picks:
                att = json.loads(s3.get_object(Bucket=LIVE, Key=a["Key"])["Body"].read())
                in_key = (att.get("input") or {}).get("key")
                if not in_key or in_key in seen_inputs:
                    continue
                seen_inputs.add(in_key)
                inputs = json.loads(s3.get_object(Bucket=LIVE, Key=in_key)["Body"].read())
                originals = inputs.get("originals") or {}
                errs = inputs.get("acquisition_errors") or {}
                vendor_total = sum(1 for k in list(originals) + list(errs) if k.startswith("vendor_"))
                vendor_429 = sum(1 for k, v in errs.items() if k.startswith("vendor_") and v == "HTTP_429")
                r.log("attempt %s (%s): acquisition errors %d (vendor 429: %d of %d vendor calls; non-vendor: %s)" % (
                    a["LastModified"].isoformat(timespec="minutes"), att.get("failure_class"), len(errs), vendor_429, vendor_total,
                    json.dumps({k: v for k, v in errs.items() if not k.startswith("vendor_")})))
                ref = originals.get("ishares_catalog")
                if not ref:
                    r.log("  no ishares_catalog original in this attempt (acquisition error %s)" % errs.get("ishares_catalog"))
                    continue
                ev_key = (ref.get("evidence") or {}).get("key")
                raw = s3.get_object(Bucket=LIVE, Key=ev_key)["Body"].read()
                if ev_key.endswith(".gz"):
                    raw = gzip.decompress(raw)
                r.log("  original iShares catalog: %s, %d B, acquired %s, starts %r" % (ev_key[-28:], len(raw), ref.get("acquired_at"), raw[:60]))
                try:
                    cat = native.ishares_catalog(raw, universe)
                    r.log("  parser: OK, %d universe funds identified" % len(cat))
                except Exception as exc:  # noqa: BLE001
                    r.log("  parser: %s: %s" % (type(exc).__name__, str(exc)[:200]))
                    facts["etf_ishares_parser_error"] = "%s: %s" % (type(exc).__name__, str(exc)[:120])
                try:
                    doc = json.loads(raw)
                    r.log("  document: %s with %d entries; sample keys of one entry: %s" % (type(doc).__name__, len(doc), str(list((next(iter(doc.values())) if isinstance(doc, dict) and doc else {}).keys())[:14])[:300]))
                    if isinstance(doc, dict):
                        rows = ishares_field_check(doc, universe)
                        bad = [(t, p) for t, p in rows if p]
                        kinds = Counter(p[0].split(" ")[0] + (" " + p[0].split(" ")[1] if p[0].startswith("missing") else "") for t, p in bad for p in [p])
                        r.log("  universe tickers present %d / %d; failing a check: %d; kinds: %s" % (len(rows), len(universe), len(bad), dict(kinds)))
                        for t, p in bad[:24]:
                            r.log("    %-6s %s" % (t, "; ".join(p)[:200]))
                        missing = sorted(set(universe) - {t for t, _ in rows})
                        r.log("  universe tickers absent from the catalog (other issuers or delisted): %d: %s" % (len(missing), " ".join(missing)[:400]))
                        facts["etf_ishares_failing_tickers"] = " ".join(t for t, _ in bad)[:200] or "none"
                except Exception as exc:  # noqa: BLE001
                    r.warn("  field check: %s: %s" % (type(exc).__name__, str(exc)[:160]))
            # live screener from the runner, same replay
            try:
                req = urllib.request.Request(ISHARES_URL, headers={"User-Agent": "JustHodl original ETF research (ops@justhodl.ai)"})
                with urllib.request.urlopen(req, timeout=60) as resp:
                    live = resp.read()
                    status = resp.status
                r.log("live screener from the runner: HTTP %s, %d B" % (status, len(live)))
                try:
                    cat = native.ishares_catalog(live, universe)
                    r.log("  parser on the live document: OK, %d universe funds" % len(cat))
                    facts["etf_ishares_live"] = "parses, %d funds" % len(cat)
                except Exception as exc:  # noqa: BLE001
                    r.log("  parser on the live document: %s: %s" % (type(exc).__name__, str(exc)[:200]))
                    facts["etf_ishares_live"] = "%s: %s" % (type(exc).__name__, str(exc)[:100])
                    doc = json.loads(live)
                    if isinstance(doc, dict):
                        bad = [(t, p) for t, p in ishares_field_check(doc, universe) if p]
                        r.log("  live failing tickers: %d: %s" % (len(bad), "; ".join("%s: %s" % (t, p[0][:80]) for t, p in bad[:12])))
            except Exception as exc:  # noqa: BLE001
                r.warn("live screener: %s: %s" % (type(exc).__name__, str(exc)[:160]))
        except Exception as exc:  # noqa: BLE001
            r.warn("etf replay: %s: %s" % (type(exc).__name__, str(exc)[:200]))
        r.kv(**{k: str(v) for k, v in facts.items()})
        r.ok("read-only; nothing invoked, nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    main()
