"""ops 4568 — data page truth: verify the catalog fixes end to end.
Push 4267eb1 changed the provider-catalog engine (FRED scoped prefixes +
series count + catalog_note, StatCan denied surfacing, extras real
keys/MB/freshness) and data.html (instruments + catalog_note render).
This op: (1) zip-settles the redeployed engine by marker, (2) refreshes
the hub via Event invoke + as_of poll, (3) hard-gates every corrected
number, (4) proves the SERVED page from the edge, CF-purging if stale."""
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-provider-catalog"
MARKER = "fred-scoped"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=60,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

R = {"ops": 4568, "at": datetime.now(timezone.utc).isoformat()}
FAILS = []


def gj(key):
    return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA,
                                               "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return r.read().decode("utf-8", "replace"), dict(r.headers)


def cf(path, method="GET", data=None):
    import os
    tok = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    if not tok:
        return None, "no CLOUDFLARE_API_TOKEN in env"
    req = urllib.request.Request(
        "https://api.cloudflare.com/client/v4" + path,
        data=(json.dumps(data).encode() if data else None), method=method,
        headers={"Authorization": "Bearer " + tok,
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read()), None
    except Exception as e:
        return None, str(e)[:150]


try:
    with report("4568_data_page_truth") as r:
        r.heading("ops 4568 — data page truth: verify catalog fixes")

        r.section("1. zip-settle the redeployed engine")
        settled = False
        for i in range(30):
            try:
                cfg = lam.get_function(FunctionName=FN)
                loc = cfg["Code"]["Location"]
                with urllib.request.urlopen(loc, timeout=60) as z:
                    zb = z.read()
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode()
                if MARKER in src and "catalog_note" in src:
                    settled = True
                    r.ok(f"marker present after {i * 12}s "
                         f"(zip {len(zb) // 1024}KB)")
                    break
            except Exception as e:
                r.log(f"settle probe: {type(e).__name__}")
            time.sleep(12)
        if not settled:
            FAILS.append("engine never settled with new markers")
            r.fail("engine zip never showed ops-4568 markers")

        r.section("2. refresh the hub (Event + as_of poll)")
        prior = gj("data/provider-catalog.json")
        prior_asof = prior.get("as_of")
        r.kv(prior_as_of=prior_asof,
             prior_keys=(prior.get("totals") or {}).get("keys"))
        if settled:
            lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=b"{}")
        hub = None
        for i in range(40):
            time.sleep(18)
            h2 = gj("data/provider-catalog.json")
            if h2.get("as_of", "") > (prior_asof or ""):
                hub = h2
                r.ok(f"hub refreshed after {(i + 1) * 18}s -> "
                     f"as_of {h2.get('as_of')}")
                break
        if hub is None:
            FAILS.append("hub as_of never advanced")
            r.fail("hub never refreshed")
            hub = prior

        r.section("3. gates on the corrected numbers")
        rows = {p.get("slug"): p for p in hub.get("providers", [])}
        fred = rows.get("fred", {})
        R["fred_row"] = {k: fred.get(k) for k in
                         ("n_keys", "series_count", "catalog_note",
                          "coverage_pct", "total_mb", "freshest_h")}
        r.kv(**{("fred_" + k): v for k, v in R["fred_row"].items()})
        if (fred.get("n_keys") or 0) < 3000:
            FAILS.append(f"fred n_keys {fred.get('n_keys')} < 3000 — "
                         "scoped prefixes not counted")
        if (fred.get("series_count") or 0) < 5000:
            FAILS.append(f"fred series_count {fred.get('series_count')}"
                         " < 5000 — manifest join missing")
        if not fred.get("catalog_note"):
            FAILS.append("fred catalog_note missing")
        if fred.get("coverage_pct") is not None:
            FAILS.append("fred grew a fake coverage bar (guard failed)")
        sc = rows.get("statcan", {})
        R["statcan"] = {"denied": sc.get("denied_source_side"),
                        "coverage_pct": sc.get("coverage_pct")}
        r.kv(**{("statcan_" + k): v for k, v in R["statcan"].items()})
        if not (sc.get("denied_source_side") or 0) >= 1:
            FAILS.append("statcan denied_source_side not surfaced")
        inst = [p for p in hub.get("providers", [])
                if p.get("unit") == "instruments"]
        R["instrument_rows"] = [
            {"slug": p.get("slug"), "datasets": p.get("datasets"),
             "n_keys": p.get("n_keys"), "mb": p.get("total_mb"),
             "freshest_h": p.get("freshest_h")} for p in inst]
        for p in inst:
            r.kv(**{("x_" + (p.get("slug") or "?").replace("-", "_")):
                    json.dumps({"n": p.get("datasets"),
                                "keys": p.get("n_keys"),
                                "fr": p.get("freshest_h")})})
            if p.get("slug") == "symbol-vault":
                continue
            if not (p.get("n_keys") or 0) >= 1:
                FAILS.append(f"extra {p.get('slug')} still 0 keys")
        bd = hub.get("breakdown") or {}
        tot = hub.get("totals") or {}
        recon_ok = ((bd.get("provider_datasets") or 0) +
                    (bd.get("instruments") or 0)
                    == (hub.get("datasets_total") or -1))
        R["totals"] = tot
        R["reconcile_ok"] = recon_ok
        r.kv(totals=json.dumps(tot), reconcile_ok=recon_ok)
        if not recon_ok:
            FAILS.append("breakdown no longer reconciles to total")
        if (tot.get("keys") or 0) < 25000:
            FAILS.append(f"totals.keys {tot.get('keys')} < 25000 — "
                         "fred keys did not land in totals")

        r.section("4. served-page proof (edge)")
        page_ok = False
        purged = False
        for att in range(9):
            try:
                body, hdr = fetch("https://justhodl.ai/data.html?v="
                                  + str(int(time.time())) + str(att))
                if "catalog_note" in body and "instruments" in body:
                    page_ok = True
                    r.ok(f"edge serves new page (attempt {att + 1}, "
                         f"cf={hdr.get('CF-Cache-Status')})")
                    break
                r.log(f"attempt {att + 1}: stale "
                      f"(cf={hdr.get('CF-Cache-Status')})")
            except Exception as e:
                r.log(f"attempt {att + 1}: {type(e).__name__}")
            if att == 4 and not purged:
                zj, zerr = cf("/zones?name=justhodl.ai")
                zid = (((zj or {}).get("result") or [{}])[0]
                       .get("id") if zj else None)
                if zid:
                    pj, perr = cf(f"/zones/{zid}/purge_cache", "POST",
                                  {"files": [
                                      "https://justhodl.ai/data.html",
                                      "https://justhodl.ai/data/"
                                      "provider-catalog.json",
                                      "https://justhodl.ai/data/"
                                      "providers/fred.json"]})
                    purged = bool((pj or {}).get("success"))
                    r.log(f"cf purge: ok={purged} err={perr}")
                else:
                    r.log(f"cf zone lookup failed: {zerr}")
            time.sleep(22)
        if not page_ok:
            FAILS.append("edge never served the new data.html")
            r.fail("served page still stale after retries + purge")
        served_hub_ok = False
        try:
            jb, _ = fetch("https://justhodl.ai/data/provider-catalog"
                          ".json?cb=" + str(int(time.time())))
            jd = json.loads(jb)
            fr2 = next((p for p in jd.get("providers", [])
                        if p.get("slug") == "fred"), {})
            served_hub_ok = bool(fr2.get("catalog_note"))
            r.kv(served_fred_series=fr2.get("series_count"),
                 served_fred_keys=fr2.get("n_keys"),
                 served_note_ok=served_hub_ok)
        except Exception as e:
            r.warn(f"served hub fetch: {type(e).__name__}")
        if not served_hub_ok:
            FAILS.append("served provider-catalog.json lacks fred note")

        R["fails"] = FAILS
        if FAILS:
            r.fail("GATES FAILED: " + " | ".join(FAILS))
        else:
            r.ok("PASS_ALL — page numbers are now the true numbers")
except Exception as e:
    import os
    import traceback
    R["error"] = f"{type(e).__name__}: {e}"
    R["trace"] = traceback.format_exc()[-1500:]
    os.makedirs("aws/ops/reports", exist_ok=True)
    json.dump(R, open("aws/ops/reports/4568.json", "w"), indent=1,
              default=str)
    open("aws/ops/reports/4568.md", "w").write(
        "# 4568 FAIL — " + R["error"] + "\n")
    print("FAIL", R["error"])
    sys.exit(1)

import os

os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4568.json", "w"), indent=1,
          default=str)
verdict = ("PASS_ALL" if not FAILS else "FAIL: " + " | ".join(FAILS))
open("aws/ops/reports/4568.md", "w").write(
    "# 4568 — " + verdict + "\n- fred: " +
    json.dumps(R.get("fred_row"), default=str) + "\n- statcan: " +
    json.dumps(R.get("statcan"), default=str) + "\n- extras: " +
    json.dumps(R.get("instrument_rows"), default=str) + "\n- totals: " +
    json.dumps(R.get("totals"), default=str) + "\n")
print(verdict[:300])
if FAILS:
    sys.exit(1)
sys.exit(0)
