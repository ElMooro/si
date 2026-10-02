"""justhodl-regsho: Nasdaq Reg SHO threshold list -> S3.

FREE, daily, no key. Downloads the pipe-delimited threshold list from
https://www.nasdaqtrader.com/dynamic/symdir/regsho.txt (with a case-variant
fallback URL), parses Symbol|Security Name|..., and tracks days_on_list per
symbol by reading the previous run's data/regsho-threshold.json from S3.
Persists symbols keep (and increment) their day count; new symbols start at 1.

FAIL-SOFT: everything is wrapped in try/except; the handler never crashes and
always writes the artifact, even if partial or empty.
"""

import boto3
import datetime
import json
import re
import urllib.error
import urllib.request

BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/regsho-threshold.json"
URLS = [
    "https://www.nasdaqtrader.com/dynamic/symdir/regsho.txt",
    "https://www.nasdaqtrader.com/dynamic/SymDir/regsho.txt",
]
HTTP_TIMEOUT = 40

_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.$/]{0,9}$")


def _load_previous(errors):
    """Read previous run's file from S3 -> {symbol: {'name':..., 'days_on_list':...}}."""
    prev = {}
    try:
        s3 = boto3.client("s3")
        resp = s3.get_object(Bucket=BUCKET, Key=S3_KEY)
        data = json.loads(resp["Body"].read().decode("utf-8"))
        for item in data.get("threshold_list") or []:
            sym = (item.get("symbol") or "").strip().upper()
            if not sym:
                continue
            try:
                days = int(item.get("days_on_list") or 0)
            except (TypeError, ValueError):
                days = 0
            prev[sym] = {"name": item.get("name") or "", "days_on_list": days}
    except Exception as e:  # missing/corrupt previous file is fine on first run
        errors.append("previous run file unavailable (seeding fresh): %s" % e)
    return prev


def _download(errors):
    last = None
    for url in URLS:
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-ai/1.0 (data pipeline)"}
            )
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return resp.read().decode("utf-8", errors="replace"), url
        except Exception as e:
            last = e
            errors.append("download failed for %s: %s" % (url, e))
    raise RuntimeError("all regsho download urls failed: %s" % last)


def _parse(text, prev):
    sym_idx = name_idx = None
    items = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or "|" not in line:
            continue
        fields = [f.strip() for f in line.split("|")]
        if sym_idx is None:
            # header row: first field is "Symbol"
            if fields[0].lower() == "symbol":
                lowered = [f.lower() for f in fields]
                sym_idx = lowered.index("symbol")
                try:
                    name_idx = lowered.index("security name")
                except ValueError:
                    name_idx = 1 if len(fields) > 1 else 0
            continue
        if len(fields) <= max(sym_idx, name_idx):
            continue
        if fields[sym_idx].lower() == "symbol":
            continue  # repeated header/footer
        sym = fields[sym_idx].upper()
        if not _SYMBOL_RE.match(sym):
            continue
        name = fields[name_idx] if len(fields) > name_idx else ""
        old = prev.get(sym) or {}
        items[sym] = {
            "symbol": sym,
            "name": name or old.get("name") or "",
            "days_on_list": (old.get("days_on_list") or 0) + 1,
        }
    return items


def lambda_handler(event, context):
    errors = []
    prev = _load_previous(errors)

    items = {}
    source_url = None
    try:
        text, source_url = _download(errors)
        items = _parse(text, prev)
    except Exception as e:
        errors.append("regsho run failed; reusing previous list: %s" % e)
        # fail-soft: keep yesterday's list with unchanged day counts
        items = {
            s: {"symbol": s, "name": v["name"], "days_on_list": v["days_on_list"]}
            for s, v in prev.items()
        }

    threshold_list = sorted(
        items.values(), key=lambda x: (-x["days_on_list"], x["symbol"])
    )

    output = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "count": len(threshold_list),
        "source_url": source_url,
        "threshold_list": threshold_list,
    }
    if errors:
        output["errors"] = errors

    try:
        boto3.client("s3").put_object(
            Bucket=BUCKET,
            Key=S3_KEY,
            Body=json.dumps(output),
            ContentType="application/json",
        )
        output["s3"] = "s3://%s/%s" % (BUCKET, S3_KEY)
    except Exception as e:
        output["s3_error"] = str(e)
        errors.append("s3 write failed: %s" % e)
        output["errors"] = errors

    return {
        "statusCode": 200,
        "body": json.dumps(
            {"ok": True, "s3_key": S3_KEY, "count": len(threshold_list), "errors": errors}
        ),
    }
