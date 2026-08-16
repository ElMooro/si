"""
ops/4749 -- close out marketshare + guidesheets: 10/10 NY Fed families.

The contract (banked at data/warm/nyfed-markets/markets-api-spec.yml.gz)
lists exactly 10 families; 8 are banked with full history. This op
finishes the last two -- marketshare (FX volume share statistics, 2
templates) and guidesheets (operation reference sheets, 2 templates) --
using the identical spec-driven method that cracked ambs/tsy/seclending:
PyYAML parse, first-enum substitution per path param, live validation,
then bank. These families may be non-dated lists; templates without a
startDate parameter are banked single-shot (the soma pattern), dated
ones in yearly windows. Idempotent merge to
data/warm/nyfed-markets/{fam}-history.json.gz, permanent.
"""
import gzip
import json
import re
import subprocess
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

try:
    import yaml  # noqa: E402
except Exception:
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "pyyaml"],
                    check=False)
    import yaml  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
NYF = "https://markets.newyorkfed.org"
WARM_PREFIX = "data/warm/nyfed-markets/"
SPEC_KEY = WARM_PREFIX + "markets-api-spec.yml.gz"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com", "Accept": "*/*"}
ROW_CAP = 400_000
FAMS = ("marketshare", "guidesheets")

s3 = boto3.client("s3", region_name=REGION)


def get(url, timeout=60):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read(12_000_000)
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            return {"ok": True, "status": r.status,
                     "body": raw.decode("utf-8", "replace")}
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code, "body": ""}
    except Exception as e:
        return {"ok": False, "status": None, "body": "",
                 "error": f"{type(e).__name__}: {str(e)[:120]}"}


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


def resolve(spec, node):
    if isinstance(node, dict) and "$ref" in node:
        cur = spec
        for part in node["$ref"].lstrip("#/").split("/"):
            cur = cur.get(part, {})
        return cur
    return node


def param_value(p):
    sch = p.get("schema") or {}
    enum = sch.get("enum") or p.get("enum")
    if enum:
        return str(enum[0])
    if sch.get("default") is not None:
        return str(sch["default"])
    return "all"


def main():
    with report("4749_nyfed_marketshare_guidesheets") as rep:
        rep.heading("ops 4749 -- final two families: marketshare + guidesheets")

        raw = s3.get_object(Bucket=BUCKET, Key=SPEC_KEY)["Body"].read()
        spec = yaml.safe_load(gzip.decompress(raw).decode("utf-8", "replace"))
        paths = spec.get("paths") or {}
        servers = spec.get("servers") or []
        base = (servers[0].get("url") if servers else f"{NYF}/api").rstrip("/")
        if base.startswith("/"):
            base = NYF + base

        def fam_of(pth):
            parts = pth.strip("/").split("/")
            return parts[1] if parts and parts[0] == "api" and len(parts) > 1 \
                else parts[0]

        for fam in FAMS:
            fam_paths = sorted([p for p in paths if fam_of(p) == fam],
                                key=lambda t: ("search" not in t, len(t)))
            rep.kv(check=f"templates_{fam}", value=len(fam_paths))
            hit = None
            has_dates = False
            for t in fam_paths[:6]:
                op = (paths[t] or {}).get("get") or {}
                params = [resolve(spec, p) for p in
                           (op.get("parameters") or []) +
                           ((paths[t] or {}).get("parameters") or [])]
                subs = {p["name"]: param_value(p) for p in params
                         if p.get("in") == "path"}
                q_req = {p["name"]: param_value(p) for p in params
                          if p.get("in") == "query" and p.get("required")
                          and p["name"] not in ("startDate", "endDate")}
                date_ok = any(p.get("name") == "startDate" for p in params)
                url = base + re.sub(r"\{(\w+)\}",
                                     lambda m: subs.get(m.group(1), "all"), t)
                if q_req:
                    url += "?" + "&".join(f"{k}={v}" for k, v in q_req.items())
                probe = url + (("&" if "?" in url else "?") +
                                "startDate=2024-01-01&endDate=2024-12-31"
                                if date_ok else "")
                pr = get(probe)
                rows = []
                if pr["ok"] and pr["body"].strip()[:1] in "[{":
                    try:
                        rows = first_rows(json.loads(pr["body"]))
                    except Exception:
                        rows = []
                rep.log(f"{fam}: {t} subs={subs} -> status={pr.get('status')} "
                        f"rows={len(rows)}")
                if rows:
                    hit, has_dates = url, date_ok
                    break
            if not hit:
                rep.kv(family=fam, banked=False, reason="no_template_validated")
                rep.warn(f"{fam}: no template validated")
                continue
            all_rows = []
            if has_dates:
                j = "&" if "?" in hit else "?"
                for y in range(2000, int(time.strftime("%Y")) + 1):
                    r = get(f"{hit}{j}startDate={y}-01-01&endDate={y}-12-31")
                    if r["ok"] and r["body"]:
                        try:
                            all_rows.extend(first_rows(json.loads(r["body"])))
                        except Exception:
                            pass
                    if len(all_rows) > ROW_CAP:
                        break
                    time.sleep(0.3)
            else:
                r = get(hit)
                if r["ok"] and r["body"]:
                    try:
                        all_rows = first_rows(json.loads(r["body"]))
                    except Exception:
                        all_rows = []
            seen, deduped = set(), []
            for row in all_rows:
                k = (json.dumps(row, sort_keys=True)
                     if isinstance(row, dict) else str(row))
                if k not in seen:
                    seen.add(k)
                    deduped.append(row)
            if not deduped:
                rep.kv(family=fam, banked=False, reason="zero_rows_at_bank")
                rep.warn(f"{fam}: validated but bank fetch got 0 rows")
                continue
            out_key = f"{WARM_PREFIX}{fam}-history.json.gz"
            try:
                praw = s3.get_object(Bucket=BUCKET, Key=out_key)["Body"].read()
                for row in json.loads(gzip.decompress(praw)).get("rows") or []:
                    k = (json.dumps(row, sort_keys=True)
                         if isinstance(row, dict) else str(row))
                    if k not in seen:
                        seen.add(k)
                        deduped.append(row)
            except Exception:
                pass
            dd = sorted(set(deep_dates(deduped)))
            out = {"source": f"NY Fed Markets API -- {fam}", "endpoint": hit,
                   "banked_at": datetime.now(timezone.utc).isoformat(),
                   "n_rows": len(deduped),
                   "earliest": dd[0] if dd else None,
                   "latest": dd[-1] if dd else None, "rows": deduped}
            s3.put_object(Bucket=BUCKET, Key=out_key,
                           Body=gzip.compress(json.dumps(
                               out, separators=(",", ":")).encode()),
                           ContentType="application/json",
                           ContentEncoding="gzip")
            rep.ok(f"{fam}: banked {len(deduped)} rows, "
                    f"{dd[0] if dd else '-'} -> {dd[-1] if dd else '-'}")
            rep.kv(family=fam, banked=True, n_rows=len(deduped),
                   earliest=dd[0] if dd else None,
                   latest=dd[-1] if dd else None)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
