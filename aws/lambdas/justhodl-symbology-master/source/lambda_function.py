"""SEC ticker/CIK spine with conservative optional identifier enrichment.

The current SEC ticker file is a source population, not a complete market
universe or a historical issuer-security relationship. Prior identities and
unknown fields are retained; conflicting issuers cannot inherit identifiers.
The legacy CUSIP/GLEIF chain remains unqualified, with name-only joins and US
ISIN derivations retained as candidates. Writes the existing master and raw
snapshot on the original schedule; no new investment authority is granted.
"""
import json
import os
import urllib.request
from datetime import datetime, timezone

import time

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
try:
    from raw_snapshot import snapshot
except Exception:
    snapshot = None




# ── ops 4442: FIGI enrichment (OpenFIGI v3, key in SSM — repo is public) ──
_ssm_cache = {}


def _figi_key():
    if "k" not in _ssm_cache:
        try:
            ssm = boto3.client("ssm", region_name="us-east-1")
            _ssm_cache["k"] = ssm.get_parameter(
                Name="/justhodl/openfigi/api-key",
                WithDecryption=True)["Parameter"]["Value"]
        except Exception as e:
            print("figi key unavailable:", str(e)[:60])
            _ssm_cache["k"] = None
    return _ssm_cache["k"]




# ── ops 4469: CUSIP→ISIN→LEI chain (13F map + GLEIF cross-walk) ──────────
import io as _io
import zipfile as _zipfile




def _norm_name(s):
    s = (s or "").upper()
    for ch in ".,'&/()-":
        s = s.replace(ch, " ")
    drop = {"INC", "CORP", "CORPORATION", "CO", "COMPANY", "LTD", "PLC",
            "HOLDINGS", "HOLDING", "GROUP", "THE", "CLASS", "A", "B", "C",
            "COM", "NEW", "DEL", "TRUST", "LP", "SA", "NV", "AG"}
    toks = [w for w in s.split() if w and w not in drop]
    return " ".join(toks[:3])


def _isin_check_digit(body11):
    s = "".join(str(int(c, 36)) for c in body11)
    digits = [int(c) for c in s]
    total = 0
    dbl = True
    for d in reversed(digits):
        v = d * 2 if dbl else d
        total += v - 9 if v > 9 else v
        dbl = not dbl
    return str((10 - total % 10) % 10)


def enrich_cusip_chain(by_ticker):
    """Retain optional legacy map evidence without claiming identity authority.

    Direct CUSIPs remain unqualified; name-only matches and country-assumed
    ISIN derivations are candidates. Existing compatible ISINs may use the
    inherited GLEIF path, whose transport and relationship proof remain open.
    """
    stats = {"cusip": 0, "isin": 0, "lei": 0, "map_shape": None}
    try:
        # ops 4473: v2 (full-holdings rebuild) overlays v1
        m = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/13f-cusip-map.json")["Body"].read())
        try:
            v2 = json.loads(s3.get_object(
                Bucket=BUCKET,
                Key="data/13f-cusip-map-v2.json")["Body"].read())
            if isinstance(m, dict) and isinstance(v2, dict):
                m = {**m, **v2}
        except Exception:
            pass
    except Exception as e:
        stats["error"] = f"13f map: {type(e).__name__}: {str(e)[:60]}"
        return stats
    candidate_sets = {}
    items = (m.items() if isinstance(m, dict) else
             [(None, x) for x in m] if isinstance(m, list) else [])
    for k, v in items:
        if isinstance(v, dict):
            cus = (v.get("cusip") or (k if k and len(str(k)) == 9
                                      else None))
            tkr = (v.get("ticker") or v.get("symbol")
                   or (str(k) if str(k).upper() in by_ticker else ""))
        else:
            cus, tkr = k, str(v)
        tkr = (tkr or "").upper().strip()
        if cus and tkr and len(str(cus)) == 9 and tkr in by_ticker:
            candidate_sets.setdefault(tkr, set()).add(str(cus).upper())
    cus_by_t = {t: next(iter(values)) for t, values in candidate_sets.items() if len(values) == 1}
    stats["ambiguous_tickers"] = sum(len(values) > 1 for values in candidate_sets.values())
    for t, values in candidate_sets.items():
        by_ticker[t]["cusip_direct_candidates"] = sorted(values)
        by_ticker[t]["cusip_match_qualified"] = False
    # ops 4470: pass 2 — name-normalized join for map rows whose ticker
    # field is absent. Even a unique normalized name is only a candidate;
    # it does not establish the security or issuer relationship.
    name_to_t = {}
    for tkr, r in by_ticker.items():
        n = _norm_name(r.get("name"))
        if n:
            name_to_t.setdefault(n, []).append(tkr)
    name_joined = 0
    for k, v in items:
        # ops 4471: map values are NAME STRINGS keyed by cusip — handle
        # both shapes (pass 1 had mistaken names for tickers).
        if isinstance(v, dict):
            cus = (v.get("cusip") or (k if k and len(str(k)) == 9
                                      else None))
            nm_raw = (v.get("name") or v.get("issuer")
                      or v.get("company"))
        else:
            cus = k if k and len(str(k)) == 9 else None
            nm_raw = str(v)
        if not cus or len(str(cus)) != 9:
            continue
        nm = _norm_name(nm_raw)
        cands = name_to_t.get(nm) or []
        if len(cands) == 1 and cands[0] not in cus_by_t:
            row = by_ticker[cands[0]]
            values = row.setdefault("cusip_name_candidates", [])
            if str(cus).upper() not in values:
                values.append(str(cus).upper())
            row["cusip_name_match_qualified"] = False
    stats["name_joined"] = name_joined
    stats["name_candidate_rows"] = sum(len(r.get("cusip_name_candidates", [])) for r in by_ticker.values())
    stats["conflicts"] = 0
    stats["map_shape"] = (type(m).__name__ + f"/{len(cus_by_t)} joinable")
    want_isin = {}
    for tkr, cus in cus_by_t.items():
        r = by_ticker[tkr]
        if r.get("cusip") is not None and r["cusip"] != cus:
            r["cusip_mapping_conflict"] = {"retained": r["cusip"], "observed": cus, "eligible": False}
            stats["conflicts"] += 1
            continue
        if r.get("cusip") is None:
            r["cusip"] = cus
            stats["cusip"] += 1
        # A CUSIP does not establish an ISIN's issuing country. Retain this
        # legacy US derivation as a candidate only, never as an observed ISIN.
        if not cus.isalnum() or not cus.isascii():
            r["isin_derivation_status"] = "unsupported_cusip_characters"
            continue
        body = "US" + cus
        candidate = body + _isin_check_digit(body)
        r["isin_derivation_candidate"] = {"value": candidate, "cusip": cus,
                                         "assumed_country": "US", "qualified": False}
        if r.get("isin") == candidate:
            want_isin[candidate] = tkr
        elif r.get("isin") is not None:
            r["isin_mapping_review"] = {"retained": r["isin"], "legacy_us_candidate": candidate, "eligible": False}
    if want_isin:
        try:
            zb = s3.get_object(Bucket=BUCKET,
                               Key="data/warm/gleif/isin-lei-latest.zip"
                               )["Body"].read()
            zf = _zipfile.ZipFile(_io.BytesIO(zb))
            name = zf.namelist()[0]
            with zf.open(name) as fh:
                header = fh.readline().decode("utf-8",
                                              "replace").strip()
                cols = [c.strip().strip('"').upper()
                        for c in header.split(",")]
                try:
                    ii = cols.index("ISIN")
                    li = cols.index("LEI")
                except ValueError:
                    ii, li = 1, 0
                for line in fh:
                    parts = line.decode("utf-8", "replace")                         .strip().split(",")
                    if len(parts) <= max(ii, li):
                        continue
                    isin = parts[ii].strip().strip('"')
                    tkr = want_isin.get(isin)
                    if tkr and by_ticker[tkr].get("lei") is None:
                        by_ticker[tkr]["lei"] =                             parts[li].strip().strip('"')
                        stats["lei"] += 1
        except Exception as e:
            stats["gleif_error"] = (f"{type(e).__name__}: "
                                    f"{str(e)[:60]}")
    return stats


def enrich_figi(by_ticker, limit=2500, context=None):
    from equity_identity import enrich_figi as resolve_equities
    try:
        working = {t: dict(row) for t, row in by_ticker.items()}
        result = resolve_equities(working, context, limit=limit)
        by_ticker.update(working)
        return result
    except Exception:
        return {"enriched": 0, "no_match": 0, "errors": 1, "ambiguous": 0,
                "remaining_null": sum(r.get("figi") is None for r in by_ticker.values()),
                "reason": "optional_figi_enrichment_failed"}


def enrich_cusip_safely(by_ticker):
    # The legacy optional chain may fail on malformed map fields. Do not leave
    # half-applied identifiers behind or prevent the primary SEC publication.
    try:
        working = {t: dict(row) for t, row in by_ticker.items()}
        for row in working.values():
            if "cusip_name_candidates" in row:
                if not isinstance(row["cusip_name_candidates"], list):
                    raise ValueError("Prior candidate list malformed")
                row["cusip_name_candidates"] = list(row["cusip_name_candidates"])
        result = enrich_cusip_chain(working)
        by_ticker.update(working)
        return result
    except Exception:
        return {"cusip": 0, "isin": 0, "lei": 0, "errors": 1,
                "error": "optional_cusip_chain_failed", "lookup_qualified": False}


# ── ops 9/10: bond-CUSIP enrichment (OpenFIGI ID_CUSIP bridge) ──────────
BOND_CUSIP_QUEUE_KEY = "data/_state/bond-cusip-queue.json"
BOND_CUSIP_MASTER_KEY = "data/symbology/bond-cusips.json"


def enrich_bond_cusips(limit=100, context=None):
    """Optional structured enrichment; preserve the primary equity publication.

    Requires a real remaining-time clock, reserves twenty seconds for the final
    equity write, and caps the optional work at forty seconds. Dedicated bounded
    S3/SSM clients prevent legacy default storage retries consuming that reserve.
    This is a cooperative deadline, not a claim of deterministic network latency.
    """
    counts = {"resolved": 0, "no_match": 0, "errors": 0, "remaining": None}
    try:
        from botocore.config import Config
        import openfigi
        from bond_symbology import enrich
        if context is None or not callable(getattr(context, 'get_remaining_time_in_millis', None)):
            return {**counts, "status": "deferred", "reason": "remaining_time_unavailable", "published": False}
        remaining = context.get_remaining_time_in_millis()
        if type(remaining) not in (int, float) or not 30000 < remaining <= 900000:
            return {**counts, "status": "deferred", "reason": "insufficient_time", "published": False}
        deadline = time.monotonic() + min(40, remaining / 1000 - 20)
        client = boto3.client("s3", region_name="us-east-1", config=Config(
            connect_timeout=2, read_timeout=3, retries={"total_max_attempts": 1}))
        return enrich(client, BUCKET, openfigi.resolve_cusip, deadline=deadline, limit=limit)
    except Exception:
        return {**counts, "status": "deferred", "reason": "optional_enrichment_failed", "errors": 1, "published": False}


def lambda_handler(event, context):
    from equity_identity import read_prior, spine, carry_previous, publish, IdentityError
    from bond_symbology import _pairs, _reject
    # Read and validate complete prior state before a provider request or write.
    # Only NoSuchKey/404 permits a new master; other failures retain production.
    prev, prior_etag = read_prior(s3, BUCKET)
    url = "https://www.sec.gov/files/company_tickers.json"
    req = urllib.request.Request(url, headers={
        "User-Agent": "JustHodl research admin@justhodl.ai"})
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read()
    raw_key = snapshot("sec", url, raw) if snapshot else None
    data = json.loads(raw, object_pairs_hook=_pairs, parse_constant=_reject)
    by_ticker, by_cik = spine(data, {"kind": "sec", "url": url, "raw_snapshot_key": raw_key})
    if prev.get("by_ticker") and len(by_ticker)*2 < len(prev["by_ticker"]):
        raise IdentityError("Source population contracted more than half; prior master retained")
    retained_prior = carry_previous(by_ticker, prev)
    figi_stats = enrich_figi(by_ticker, context=context)
    cusip_stats = enrich_cusip_safely(by_ticker)
    bond_cusip_stats = enrich_bond_cusips(context=context)
    n = len(by_ticker)
    doc = {**prev, "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "spec": "SEC ticker/CIK spine with typed optional FIGI resolution; "
                   "legacy identifiers are retained with separate qualification",
           "n_tickers": n, "n_ciks": len(by_cik),
           "coverage": {
               "vs_bloomberg_320k_pct": round(100 * n / 320000, 2),
               "note": "Legacy 320k comparison is unvalidated; this source population "
                       "does not establish full-market coverage",
               "denominator_qualified": False},
           "enrichment_status": {"cik": "complete", "cusip_chain": cusip_stats, "figi": figi_stats, "isin": "pending",
                                 "sedol": "pending", "bond_cusips": bond_cusip_stats},
           "by_ticker": by_ticker,
           "retained_prior_tickers": retained_prior,
           "identity_quality": {"contract": "sec-equity-identity.v1", "source_records": len(data),
                                "universe_coverage_qualified": False,
                                "issuer_security_relationships_qualified": False,
                                "source_replay_verified": False, "investment_authority": False}}
    publish(s3, BUCKET, doc, prior_etag)
    res = {"ok": True, "n_tickers": n, "n_ciks": len(by_cik),
           "raw_key": raw_key}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
