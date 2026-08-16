"""
ops/4745 -- the Swagger spec hunt: ambs/tsy/seclending via the real
API contract instead of guesses.

markets-api.html is a Swagger UI shell -- ops 4742 fetched it, regexed
for /api/ paths, found 0, and (my bug) never logged its status or size.
Swagger UI shells contain a reference to the actual spec file (yml or
json), and THAT file contains every path template with its parameter
enums -- ambs and tsy templates need real enum values (security type /
operation type / status), which is exactly why every 'all'-substituted
guess 400'd. This op:

  A. fetches markets-api.html with FULL logging (status, bytes,
     first chars) -- no more silent zeros
  B. extracts every spec-file candidate referenced in the shell, plus
     a hardcoded candidate list (.yml/.yaml/.json variants), fetches
     until one parses as a spec (JSON 'paths' key, or YAML path-line
     regex)
  C. extracts all path templates + nearby enum values for the three
     locked families; builds candidate URLs by substituting the FIRST
     REAL ENUM VALUE per parameter (falling back to 'all' only when a
     param has no enum)
  D. validates each candidate with a 2024 probe; anything returning
     real rows gets the full 2000->present yearly bank to
     data/warm/nyfed-markets/{fam}-history.json.gz -- idempotent,
     permanent, same pattern as fxs/soma

If no spec file is found anywhere, that is reported plainly and the
follow-up moves to NY Fed's non-API results pages -- not more URL
permutations.
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
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com",
       "Accept": "*/*"}
ROW_CAP = 400_000
FAMS = ("ambs", "tsy", "seclending")

s3 = boto3.client("s3", region_name=REGION)


def get(url, timeout=50):
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


def bank(rep, fam, base, joiner):
    year_now = int(time.strftime("%Y"))
    all_rows = []
    for y in range(2000, year_now + 1):
        r = get(f"{base}{joiner}startDate={y}-01-01&endDate={y}-12-31")
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
    with report("4745_nyfed_swagger_spec_hunt") as rep:
        rep.heading("ops 4745 -- Swagger spec hunt for ambs/tsy/seclending")

        rep.section("A. Fetch the Swagger UI shell -- with full logging this time")
        shell = get(f"{NYF}/static/docs/markets-api.html")
        rep.kv(check="shell_status", value=shell.get("status"))
        rep.kv(check="shell_bytes", value=len(shell.get("body", "")))
        if shell.get("error"):
            rep.log(f"shell error: {shell['error']}")
        rep.log(f"shell first 300 chars: {shell.get('body','')[:300]!r}")

        rep.section("B. Locate + fetch the spec file")
        cands = []
        if shell["ok"]:
            cands += re.findall(r'["\']([^"\']+\.(?:ya?ml|json))["\']',
                                 shell["body"])
        cands += ["/static/docs/markets-api.yml", "/static/docs/markets-api.yaml",
                   "/static/docs/markets-api.json", "/static/docs/swagger.yml",
                   "/static/docs/swagger.yaml", "/static/docs/swagger.json",
                   "/static/docs/api-docs.json", "/static/docs/openapi.yml",
                   "/static/docs/openapi.yaml"]
        spec_text = None
        spec_kind = None
        spec_url = None
        for c in dict.fromkeys(cands):
            absu = c if c.startswith("http") else NYF + (
                c if c.startswith("/") else "/static/docs/" + c)
            r = get(absu)
            body = r.get("body", "")
            kind = None
            if r["ok"] and body:
                if body.lstrip().startswith("{") and '"paths"' in body[:20000]:
                    kind = "json"
                elif re.search(r"^paths:\s*$", body, re.M) or \
                        re.search(r"^\s{2}/[A-Za-z]", body, re.M):
                    kind = "yaml"
            rep.log(f"{absu.rsplit('/static/',1)[-1][:60]} -> "
                    f"status={r.get('status')} bytes={len(body)} kind={kind}")
            if kind:
                spec_text, spec_kind, spec_url = body, kind, absu
                break
        rep.kv(check="spec_found", value=bool(spec_text))
        rep.kv(check="spec_url", value=spec_url)
        rep.kv(check="spec_kind", value=spec_kind)
        if not spec_text:
            rep.warn("no spec file located anywhere -- follow-up moves to NY Fed's "
                     "non-API results pages, not more permutations")
            return

        rep.section("C. Extract templates + enums for the locked families")
        templates = []
        if spec_kind == "json":
            try:
                spec = json.loads(spec_text)
                templates = list((spec.get("paths") or {}).keys())
            except Exception as e:
                rep.warn(f"json spec parse: {type(e).__name__}")
        else:
            templates = re.findall(r"^\s{2}(/[A-Za-z0-9_\-/{}.]+):\s*$",
                                    spec_text, re.M)
        rep.kv(check="templates_total", value=len(templates))
        fam_templates = {f: [t for t in templates
                              if re.search(rf"/{f}(/|$)", t)] for f in FAMS}
        for f in FAMS:
            rep.kv(check=f"templates_{f}", value=len(fam_templates[f]))
            for t in fam_templates[f][:10]:
                rep.log(f"  {f}: {t}")
        # enum harvest: param name -> first enum value (yaml + json best-effort)
        enums = {}
        if spec_kind == "yaml":
            for m in re.finditer(
                    r"name:\s*(\w+).{0,400}?enum:\s*\n((?:\s*-\s*[\w .$-]+\n)+)",
                    spec_text, re.S):
                vals = re.findall(r"-\s*([\w .$-]+)", m.group(2))
                if vals:
                    enums.setdefault(m.group(1), vals[0].strip())
        else:
            try:
                spec = json.loads(spec_text)
                def walk(o):
                    if isinstance(o, dict):
                        if o.get("name") and isinstance(o.get("schema"), dict) \
                                and o["schema"].get("enum"):
                            enums.setdefault(o["name"], str(o["schema"]["enum"][0]))
                        if o.get("name") and o.get("enum"):
                            enums.setdefault(o["name"], str(o["enum"][0]))
                        for v in o.values():
                            walk(v)
                    elif isinstance(o, list):
                        for v in o:
                            walk(v)
                walk(spec)
            except Exception:
                pass
        rep.kv(check="enums_harvested", value=len(enums))
        if enums:
            rep.log("enum picks: " + ", ".join(f"{k}={v}" for k, v in
                     list(enums.items())[:12]))

        rep.section("D. Validate + bank")
        for f in FAMS:
            hit = None
            joiner = "?"
            search_first = sorted(fam_templates[f],
                                   key=lambda t: ("search" not in t, len(t)))
            for t in search_first[:8]:
                path = re.sub(r"\{(\w+)\}",
                               lambda m: enums.get(m.group(1), "all"), t)
                base = NYF + ("/api" if not path.startswith("/api") else "") + path
                if not base.endswith(".json"):
                    base += ".json"
                joiner = "&" if "?" in base else "?"
                probe = f"{base}{joiner}startDate=2024-01-01&endDate=2024-12-31"
                r = get(probe)
                rows = []
                if r["ok"] and r["body"].strip()[:1] in "[{":
                    try:
                        rows = first_rows(json.loads(r["body"]))
                    except Exception:
                        rows = []
                rep.log(f"{f}: {base.rsplit('/api/',1)[-1][:80]} -> "
                        f"status={r.get('status')} rows={len(rows)}")
                if rows:
                    hit = base
                    break
            if hit:
                bank(rep, f, hit, joiner)
            else:
                rep.kv(family=f, banked=False, reason="no_template_validated")
                rep.warn(f"{f}: no spec template validated with rows")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
