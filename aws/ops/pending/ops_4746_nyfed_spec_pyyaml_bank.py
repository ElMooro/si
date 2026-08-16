"""
ops/4746 -- parse the found spec PROPERLY (PyYAML), then bank.

ops 4745 located and fetched the real contract -- markets-api.yml,
143,732 bytes -- but my 2-space-indent path regex extracted 0 templates
from it (quoted keys / indentation variants defeat naive regex). This
op stops hand-parsing: pip-installs PyYAML in the runner if absent,
yaml.safe_load's the spec, and works from the real object:

  A. stores the spec itself to data/warm/nyfed-markets/
     markets-api-spec.yml.gz (the contract is data too -- permanent)
  B. prints the TRUE family list from spec['paths'] with counts --
     the from-the-horse's-mouth coverage table
  C. for ambs/tsy/seclending: extracts each GET template's parameters
     (resolving $ref into components), substitutes the FIRST REAL ENUM
     value per path param, appends required query params likewise
  D. validates each built URL with a 2024 (or no-date) probe;
     real rows -> full bank: yearly windows when the template takes
     startDate/endDate, single-shot otherwise (soma pattern) --
     idempotent merge to data/warm/nyfed-markets/{fam}-history.json.gz

Base URL comes from spec['servers'] when present, never hardcoded
guesses. Anything that still fails is reported with the exact template
and substitutions tried.
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
SPEC_URL = f"{NYF}/static/docs/markets-api.yml"
WARM_PREFIX = "data/warm/nyfed-markets/"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com", "Accept": "*/*"}
ROW_CAP = 400_000
FAMS = ("ambs", "tsy", "seclending")

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
        body = ""
        try:
            body = e.read()[:200].decode("utf-8", "replace")
        except Exception:
            pass
        return {"ok": False, "status": e.code, "body": body}
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


def bank(rep, fam, url_no_dates, has_dates):
    all_rows = []
    if has_dates:
        joiner = "&" if "?" in url_no_dates else "?"
        for y in range(2000, int(time.strftime("%Y")) + 1):
            r = get(f"{url_no_dates}{joiner}startDate={y}-01-01&endDate={y}-12-31")
            if r["ok"] and r["body"]:
                try:
                    all_rows.extend(first_rows(json.loads(r["body"])))
                except Exception:
                    pass
            if len(all_rows) > ROW_CAP:
                rep.warn(f"{fam}: row cap -- NEEDS_DEDICATED_ENGINE")
                rep.kv(family=fam, banked=False, reason="row_cap")
                return
            time.sleep(0.3)
    else:
        r = get(url_no_dates)
        if r["ok"] and r["body"]:
            try:
                all_rows = first_rows(json.loads(r["body"]))
            except Exception:
                all_rows = []
    seen, deduped = set(), []
    for row in all_rows:
        k = json.dumps(row, sort_keys=True) if isinstance(row, dict) else str(row)
        if k not in seen:
            seen.add(k)
            deduped.append(row)
    if not deduped:
        rep.warn(f"{fam}: bank fetch produced 0 rows -- nothing written")
        rep.kv(family=fam, banked=False, reason="zero_rows_at_bank")
        return
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
    out = {"source": f"NY Fed Markets API -- {fam}", "endpoint": url_no_dates,
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
    with report("4746_nyfed_spec_pyyaml_bank") as rep:
        rep.heading("ops 4746 -- PyYAML parse of markets-api.yml -> validate -> bank")

        r = get(SPEC_URL)
        rep.kv(check="spec_status", value=r.get("status"))
        rep.kv(check="spec_bytes", value=len(r.get("body", "")))
        if not r["ok"]:
            rep.warn("spec fetch failed this run -- nothing to do")
            return
        spec = yaml.safe_load(r["body"])
        s3.put_object(Bucket=BUCKET, Key=f"{WARM_PREFIX}markets-api-spec.yml.gz",
                       Body=gzip.compress(r["body"].encode()),
                       ContentType="text/yaml", ContentEncoding="gzip")
        rep.ok("spec itself banked to warm (the contract is data too)")

        servers = spec.get("servers") or []
        base = (servers[0].get("url") if servers else f"{NYF}/api").rstrip("/")
        if base.startswith("/"):
            base = NYF + base
        rep.kv(check="server_base", value=base)

        paths = spec.get("paths") or {}
        fams_all = {}
        for p in paths:
            fams_all.setdefault(p.strip("/").split("/")[0], 0)
            fams_all[p.strip("/").split("/")[0]] += 1
        rep.kv(check="paths_total", value=len(paths))
        rep.log("TRUE family list from the contract: " +
                 ", ".join(f"{k}={v}" for k, v in sorted(fams_all.items())))

        for fam in FAMS:
            fam_paths = [p for p in paths if p.strip("/").split("/")[0] == fam]
            rep.kv(check=f"templates_{fam}", value=len(fam_paths))
            fam_paths.sort(key=lambda t: ("search" not in t, len(t)))
            hit = None
            has_dates = False
            for t in fam_paths[:10]:
                op = (paths[t] or {}).get("get") or {}
                params = [resolve(spec, p) for p in
                           (op.get("parameters") or []) +
                           ((paths[t] or {}).get("parameters") or [])]
                path_subs = {p["name"]: param_value(p) for p in params
                              if p.get("in") == "path"}
                q_req = {p["name"]: param_value(p) for p in params
                          if p.get("in") == "query" and p.get("required")
                          and p["name"] not in ("startDate", "endDate")}
                date_ok = any(p.get("name") == "startDate" for p in params)
                url_path = re.sub(r"\{(\w+)\}",
                                   lambda m: path_subs.get(m.group(1), "all"), t)
                url = base + url_path
                if q_req:
                    url += "?" + "&".join(f"{k}={v}" for k, v in q_req.items())
                probe = url
                if date_ok:
                    probe += ("&" if "?" in probe else "?") + \
                        "startDate=2024-01-01&endDate=2024-12-31"
                pr = get(probe)
                rows = []
                if pr["ok"] and pr["body"].strip()[:1] in "[{":
                    try:
                        rows = first_rows(json.loads(pr["body"]))
                    except Exception:
                        rows = []
                rep.log(f"{fam}: {t} subs={path_subs} q={q_req} "
                        f"-> status={pr.get('status')} rows={len(rows)}")
                if rows:
                    hit, has_dates = url, date_ok
                    break
            if hit:
                bank(rep, fam, hit, has_dates)
            else:
                rep.kv(family=fam, banked=False, reason="no_template_validated")
                rep.warn(f"{fam}: every spec template failed even with real "
                         "enum substitutions -- exact attempts logged above")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
