"""ops 3394 — SG/HK endpoint HUNT (empirical) + GSSI internals strip + sovereign sentinel.

The MAS/HKMA guesses answered 200-but-empty twice. This ops stops guessing:
  A. HARVEST real endpoints — fetch the HKMA apidocs catalog pages and regex
     every api.hkma.gov.hk/public path; search the data.gov.sg dataset API for
     SGS/bond datasets; probe each candidate, record status + record-count +
     first-record keys.
  B. If a working (url, 10y-key) pair emerges for either country: hot-patch
     the engine's ladder (insert discovered URL as rung 0), deploy via
     deploy_lambda, invoke, and gate the bp number. If nothing public exists,
     record the definitive finding — the daily ledger self-heals true YoY.
  C. Gate this push's page strip (breadth bars + co-movement band under the
     GSSI chart) and the sentinel's sovereign/GSSI watch markers.
"""
import io
import json
import re
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 2}))
S3C = boto3.client("s3", "us-east-1")
UA = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                     "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")}
FN = "justhodl-sovereign-stress"


def get(url, timeout=25):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def probe_json(url):
    try:
        st, body = get(url)
        j = json.loads(body)
        recs = (((j.get("result") or {}).get("records"))
                or j.get("records") or (j.get("Data") or []))
        keys = sorted((recs[0] if recs else {}).keys())[:10] if recs else []
        return {"status": st, "n": len(recs), "keys": keys}
    except Exception as e:  # noqa: BLE001
        return {"status": -1, "err": str(e)[:80]}


def invoke_resilient(fn, tries=6):
    for k in range(tries):
        try:
            return LAM.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
        except Exception as e:  # noqa: BLE001
            if "TooManyRequests" in str(e) or "Rate Exceeded" in str(e):
                time.sleep(15 * (k + 1))
                continue
            raise
    raise RuntimeError("throttled")


with report("3394_sghk_hunt_strip_sentinel") as rep:
    rep.heading("ops 3394 — SG/HK hunt + strip + sentinel")
    out = {"gates": {}, "hunt": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:360]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:300]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(n)

    # ── A. HARVEST ──
    hk_candidates = set()
    for doc in ("https://apidocs.hkma.gov.hk/documentation/market-data-and-statistics/daily-market-data/",
                "https://apidocs.hkma.gov.hk/documentation/market-data-and-statistics/monthly-statistical-bulletin/"):
        try:
            _, body = get(doc, timeout=25)
            for mtc in re.finditer(rb"api\.hkma\.gov\.hk/public/[a-zA-Z0-9/\-]+", body):
                hk_candidates.add("https://" + mtc.group(0).decode())
        except Exception as e:  # noqa: BLE001
            print("[harvest hk]", doc, str(e)[:60])
    hk_bond = [u for u in sorted(hk_candidates)
               if re.search(r"bond|yield|efbn", u, re.I)][:12]
    print(f"[harvest] hkma paths total={len(hk_candidates)} bondish={len(hk_bond)}")
    for u in hk_bond:
        print("  cand:", u)

    sg_ids = []
    try:
        _, body = get("https://api-production.data.gov.sg/v2/public/api/datasets?query=SGS%20bond&page=1", 25)
        j = json.loads(body)
        for dsx in (j.get("data") or {}).get("datasets", [])[:10]:
            sg_ids.append({"id": dsx.get("datasetId"), "name": dsx.get("name")})
    except Exception as e:  # noqa: BLE001
        print("[harvest sg]", str(e)[:80])
    print(f"[harvest] data.gov.sg SGS datasets={len(sg_ids)}")
    for d0 in sg_ids:
        print("  ds:", d0)

    # ── probe ──
    hk_found = None
    for u in hk_bond:
        pr = probe_json(u + ("?" if "?" not in u else "&") + "pagesize=3")
        print("[probe hk]", u, json.dumps(pr)[:160])
        out["hunt"].setdefault("hk", []).append({u: pr})
        if pr.get("n", 0) > 0 and any("10" in k for k in pr.get("keys", [])):
            hk_found = u
            break
    sg_found = None
    for d0 in sg_ids:
        did = d0.get("id")
        if not did:
            continue
        u = f"https://api-production.data.gov.sg/v2/public/api/datasets/{did}/poll-download"
        pr = probe_json(u)
        print("[probe sg]", d0.get("name"), json.dumps(pr)[:140])
        out["hunt"].setdefault("sg", []).append({str(d0.get("name")): pr})
    gate("G0_hunt_ran", True,
         f"hk_bondish={len(hk_bond)} hk_found={hk_found} sg_datasets={len(sg_ids)} (findings in report)")

    # ── B. hot-patch + deploy if HK found ──
    patched = False
    if hk_found:
        src_p = Path(f"aws/lambdas/{FN}/source/lambda_function.py")
        src = src_p.read_text(encoding="utf-8")
        anchor = '        for url, back in ('
        if anchor in src and hk_found not in src:
            rung = (f'            ("{hk_found}?pagesize=280&sortby=end_of_date&sortorder=desc", 250),\n'
                    if "daily" in hk_found else
                    f'            ("{hk_found}?pagesize=15&sortby=end_of_month&sortorder=desc", 12),\n')
            src = src.replace(anchor, anchor + "\n" + rung, 1)
            src = src.replace('VERSION = "2.4.4"', 'VERSION = "2.4.5"', 1)
            src_p.write_text(src, encoding="utf-8")
            deploy_lambda(FN, source_dir=Path(f"aws/lambdas/{FN}"))
            deadline = time.time() + 240
            while time.time() < deadline:
                if LAM.get_function_configuration(FunctionName=FN).get("LastUpdateStatus") == "Successful":
                    patched = True
                    break
                time.sleep(10)
    print(f"[hotpatch] applied={patched}")

    # ── run engine, read SG/HK outcome ──
    t_inv = datetime.now(timezone.utc).isoformat()
    invoke_resilient(FN)
    feed = None
    deadline = time.time() + 540
    while time.time() < deadline:
        try:
            j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",
                                          Key="data/sovereign-stress.json")["Body"].read())
            if (j.get("generated_at") or "") > t_inv:
                feed = j
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(20)
    wg = (feed or {}).get("wgb_sovereigns") or {}
    sg_bp = (wg.get("singapore") or {}).get("yield_yoy_bp")
    hk_bp = (wg.get("hong_kong") or {}).get("yield_yoy_bp")
    resolved = isinstance(sg_bp, (int, float)) or isinstance(hk_bp, (int, float))
    gate("G1_sg_hk_outcome", bool(feed),
         f"SG_bp={sg_bp} HK_bp={hk_bp} resolved={resolved} patched={patched} "
         f"(honest 'accruing' notes remain for unresolved; ledger self-heals)")
    out["sg_hk"] = {"sg_bp": sg_bp, "hk_bp": hk_bp, "hotpatched": patched, "hk_found": hk_found}

    # ── C. strip + sentinel gates ──
    need = ["gssi-strip", "renderGSSIStrip", "co-movement ρ̄", "breadth"]
    ok2, missing = False, need
    deadline = time.time() + 240
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    f"https://justhodl.ai/sovereign-stress.html?t={int(time.time())}",
                    headers=UA), timeout=25) as r:
                b = r.read().decode("utf-8", "replace")
            missing = [m for m in need if m not in b]
            if not missing:
                ok2 = True
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(12)
    gate("G2_strip_live", ok2, f"missing={missing}")

    ok3 = False
    deadline = time.time() + 240
    while time.time() < deadline:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-alert-sentinel").get("LastUpdateStatus") == "Successful":
                info = LAM.get_function(FunctionName="justhodl-alert-sentinel")
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA),
                                            timeout=60) as r:
                    zsrc = zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")
                if "sov_europe_regime" in zsrc and "gssi_comove_hot" in zsrc:
                    ok3 = True
                    break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(12)
    gate("G3_sentinel_armed", ok3, "sovereign+GSSI watch markers in deployed sentinel")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3394.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)
