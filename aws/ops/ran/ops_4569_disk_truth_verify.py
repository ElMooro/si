"""ops 4569 — disk-true FRED accounting: verify both patched engines.
Push 874d801 made the catalog count banked scoped objects as FRED's
series truth and gave the importer a key-exists dedup (7d refresh
window). This op settles both zips by marker, regenerates the hub,
gates the corrected numbers (independent disk recount), proves the
importer's dedup on a live fire, and re-proves the served hub JSON."""
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
CAT = "justhodl-provider-catalog"
IMP = "justhodl-fred-catalog"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=60,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

R = {"ops": 4569, "at": datetime.now(timezone.utc).isoformat()}
FAILS = []


def gj(key):
    return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())


def zip_has(fn, needles):
    cfg = lam.get_function(FunctionName=fn)
    with urllib.request.urlopen(cfg["Code"]["Location"],
                                timeout=60) as z:
        src = zipfile.ZipFile(io.BytesIO(z.read())).read(
            "lambda_function.py").decode()
    return all(n in src for n in needles)


def count_prefix(prefix):
    n, tok = 0, None
    while True:
        kw = {"Bucket": B, "Prefix": prefix, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        rr = s3.list_objects_v2(**kw)
        n += len(rr.get("Contents", []))
        if not rr.get("IsTruncated"):
            break
        tok = rr.get("NextContinuationToken")
    return n


try:
    with report("4569_disk_truth_verify") as r:
        r.heading("ops 4569 — disk-true FRED accounting verify")

        r.section("1. settle both engines by marker")
        ok_cat = ok_imp = False
        for i in range(30):
            try:
                if not ok_cat:
                    ok_cat = zip_has(CAT, ["series banked"])
                if not ok_imp:
                    ok_imp = zip_has(IMP, ["disk-true dedup"])
            except Exception as e:
                r.log(f"settle probe: {type(e).__name__}")
            if ok_cat and ok_imp:
                r.ok(f"both settled after {i * 12}s")
                break
            time.sleep(12)
        if not (ok_cat and ok_imp):
            FAILS.append(f"settle failed cat={ok_cat} imp={ok_imp}")
            r.fail(f"settle: cat={ok_cat} imp={ok_imp}")

        r.section("2. importer dedup proof (state before/after fire)")
        st0 = gj("data/_state/fred-scoped-import.json")
        pre = {"skipped": st0.get("series_skipped_already", 0),
               "imported": st0.get("series_imported", 0),
               "updated_at": st0.get("updated_at")}
        r.kv(**{("pre_" + k): v for k, v in pre.items()})
        st1 = st0
        if ok_imp:
            # the 5-min cron fires naturally; the lease makes any
            # overlap with our own Event invoke safe.
            lam.invoke(FunctionName=IMP, InvocationType="Event",
                       Payload=json.dumps(
                           {"phase": "scoped_import"}).encode())
            for i in range(20):
                time.sleep(20)
                st1 = gj("data/_state/fred-scoped-import.json")
                if (st1.get("updated_at") or "") > \
                        (pre["updated_at"] or ""):
                    break
        post = {"skipped": st1.get("series_skipped_already", 0),
                "imported": st1.get("series_imported", 0),
                "blocked_at": st1.get("blocked_at"),
                "updated_at": st1.get("updated_at")}
        R["importer"] = {"pre": pre, "post": post}
        r.kv(**{("post_" + k): v for k, v in post.items()})
        if (post["updated_at"] or "") <= (pre["updated_at"] or ""):
            FAILS.append("importer state never advanced post-deploy")
        if post.get("blocked_at"):
            FAILS.append(f"importer blocked: {post['blocked_at']}")

        r.section("3. hub regen + gates (independent disk recount)")
        disk_scoped = count_prefix("data/warm/fred-scoped/")
        R["disk_scoped"] = disk_scoped
        r.kv(disk_scoped_objects=disk_scoped)
        prior_asof = gj("data/provider-catalog.json").get("as_of")
        lam.invoke(FunctionName=CAT, InvocationType="Event",
                   Payload=b"{}")
        hub = None
        for i in range(40):
            time.sleep(18)
            h2 = gj("data/provider-catalog.json")
            if h2.get("as_of", "") > (prior_asof or ""):
                hub = h2
                r.ok(f"hub refreshed after {(i + 1) * 18}s")
                break
        if hub is None:
            FAILS.append("hub as_of never advanced")
            hub = gj("data/provider-catalog.json")
        rows = {p.get("slug"): p for p in hub.get("providers", [])}
        fred = rows.get("fred", {})
        R["fred_row"] = {k: fred.get(k) for k in
                         ("n_keys", "series_count", "catalog_note",
                          "coverage_pct", "total_mb", "freshest_h")}
        r.kv(**{("fred_" + k): str(v) for k, v in
                R["fred_row"].items()})
        scount = fred.get("series_count") or 0
        if abs(scount - disk_scoped) > 60:
            FAILS.append(f"series_count {scount} vs disk "
                         f"{disk_scoped} — drift > 60")
        if "banked" not in (fred.get("catalog_note") or ""):
            FAILS.append("catalog_note lacks disk-true wording")
        if fred.get("coverage_pct") is not None:
            FAILS.append("fred fake coverage bar returned")
        sc = rows.get("statcan", {})
        if not (sc.get("denied_source_side") or 0) >= 1:
            FAILS.append("statcan denied regressed")
        for p in hub.get("providers", []):
            if p.get("unit") == "instruments" and \
                    not (p.get("n_keys") or 0) >= 1:
                FAILS.append(f"extra {p.get('slug')} regressed to 0")
        tot = hub.get("totals") or {}
        bd = hub.get("breakdown") or {}
        R["totals"] = tot
        recon_ok = ((bd.get("provider_datasets") or 0) +
                    (bd.get("instruments") or 0)
                    == (hub.get("datasets_total") or -1))
        r.kv(totals=json.dumps(tot), reconcile_ok=recon_ok)
        if not recon_ok:
            FAILS.append("reconcile broke")
        if (tot.get("keys") or 0) < 23000:
            FAILS.append(f"totals.keys {tot.get('keys')} < 23000")

        r.section("4. served hub proof (edge)")
        served_ok = False
        for att in range(6):
            try:
                jb, hdr = urllib.request.urlopen(
                    urllib.request.Request(
                        "https://justhodl.ai/data/provider-catalog"
                        ".json?cb=" + str(int(time.time())) + str(att),
                        headers={"User-Agent": UA,
                                 "Cache-Control": "no-cache"}),
                    timeout=25), None
                jd = json.loads(jb.read().decode())
                fr2 = next((p for p in jd.get("providers", [])
                            if p.get("slug") == "fred"), {})
                if "banked" in (fr2.get("catalog_note") or ""):
                    served_ok = True
                    r.ok(f"edge hub carries disk-true note "
                         f"(attempt {att + 1}): "
                         f"{fr2.get('series_count')} series")
                    break
                r.log(f"attempt {att + 1}: served note stale")
            except Exception as e:
                r.log(f"attempt {att + 1}: {type(e).__name__}")
            time.sleep(20)
        if not served_ok:
            FAILS.append("edge hub never showed banked note")

        R["fails"] = FAILS
        if FAILS:
            r.fail("GATES FAILED: " + " | ".join(FAILS))
        else:
            r.ok("PASS_ALL — disk is the source of truth end to end")
except Exception as e:
    import os
    import traceback
    R["error"] = f"{type(e).__name__}: {e}"
    R["trace"] = traceback.format_exc()[-1500:]
    os.makedirs("aws/ops/reports", exist_ok=True)
    json.dump(R, open("aws/ops/reports/4569.json", "w"), indent=1,
              default=str)
    open("aws/ops/reports/4569.md", "w").write(
        "# 4569 FAIL — " + R["error"] + "\n")
    print("FAIL", R["error"])
    sys.exit(1)

import os

os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4569.json", "w"), indent=1,
          default=str)
verdict = ("PASS_ALL" if not FAILS else "FAIL: " + " | ".join(FAILS))
open("aws/ops/reports/4569.md", "w").write(
    "# 4569 — " + verdict + "\n- fred: " +
    json.dumps(R.get("fred_row"), default=str) + "\n- disk: " +
    str(R.get("disk_scoped")) + "\n- importer: " +
    json.dumps(R.get("importer"), default=str) + "\n- totals: " +
    json.dumps(R.get("totals"), default=str) + "\n")
print(verdict[:300])
if FAILS:
    sys.exit(1)
sys.exit(0)
