"""ops/4878 -- MOF<->TIC concordance verify (global-flows v1.4).
 G0  TIC japan bank on S3: rows >= 24, print latest 3.
 (1) settle marker v1.4.0; invoke; poll v==1.4.0.
 (2) truths: japan.tic_concordance LIVE; INDEPENDENT recompute
     from BOTH S3 banks (engine's shared month_of_period +
     _pearson imported) matches doc sign_agree/corr exactly;
     print the verdict numbers; page committed+served.
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "lambdas"
                       / "justhodl-global-flows" / "source"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402
import lambda_function as engmod  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-global-flows"
B = "justhodl-dashboard-live"
OUT_KEY = "data/global-flows.json"
MARKER = "global-flows v1.4.0"
PAGE = Path(__file__).resolve().parents[3] / "global-flows.html"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("ops 4878 -- concordance verify") as rep:
        rep.heading("G0. banks")
        tic = sread(engmod.TIC_JP_BANK).get("rows") or {}
        if len(tic) >= 24:
            rep.ok("tic japan bank rows=%d" % len(tic))
        else:
            rep.fail("tic bank thin: %d" % len(tic))
            sys.exit(1)
        for d in sorted(tic)[-3:]:
            rep.log("  %s  %+0.0f $mn" % (d, tic[d]))
        mof = sread(engmod.MOF_BANK).get("rows") or {}
        rep.ok("mof bank rows=%d" % len(mof))

        rep.heading("1. settle + invoke")
        for _ in range(40):
            try:
                cfg = lam.get_function_configuration(
                    FunctionName=FN)
                if cfg.get("State") == "Active" and \
                        cfg.get("LastUpdateStatus") \
                        != "InProgress":
                    break
            except ClientError:
                pass
            time.sleep(6)
        settled = False
        for att in range(30):
            try:
                gf = lam.get_function(FunctionName=FN)
                raw = urllib.request.urlopen(
                    gf["Code"]["Location"], timeout=60).read()
                src = zipfile.ZipFile(io.BytesIO(raw)).read(
                    "lambda_function.py").decode("utf-8",
                                                 "replace")
                if MARKER in src:
                    rep.ok("marker settled (attempt %d)"
                           % (att + 1))
                    settled = True
                    break
            except (ClientError, Exception):  # noqa: BLE001
                pass
            time.sleep(10)
        if not settled:
            rep.fail("no marker")
            sys.exit(1)
        try:
            prev = sread(OUT_KEY).get("generated_at")
        except ClientError:
            prev = None
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        t0 = time.time()
        while time.time() - t0 < 240:
            time.sleep(10)
            try:
                d = sread(OUT_KEY)
            except ClientError:
                continue
            if d.get("generated_at") != prev \
                    and d.get("v") == "1.4.0":
                doc = d
                break
        if not doc:
            rep.fail("no fresh v1.4.0 doc")
            sys.exit(1)
        rep.ok("fresh in %ds" % int(time.time() - t0))

        rep.heading("2. independent recompute")
        cc = ((doc.get("countries") or {}).get("japan")
              or {}).get("tic_concordance") or {}
        mof2 = sread(engmod.MOF_BANK).get("rows") or {}
        mof_m = {}
        for per, r in mof2.items():
            m = engmod.month_of_period(per)
            v = (r or {}).get("out_lt")
            if m and v is not None:
                mof_m[m] = round(mof_m.get(m, 0) + v, 1)
        tic_m = {str(d)[:7]: round(v / 1000.0, 2)
                 for d, v in tic.items()
                 if isinstance(v, (int, float))}
        common = sorted(set(mof_m) & set(tic_m))
        a = [mof_m[m] for m in common]
        b = [tic_m[m] for m in common]
        agree = round(100.0 * sum((a[i] > 0) == (b[i] > 0)
                                  for i in range(len(a)))
                      / len(a), 1)
        c0 = engmod._pearson(a, b)
        if cc.get("status") == "LIVE" \
                and cc.get("n_months") == len(common) \
                and cc.get("sign_agree_pct") == agree \
                and cc.get("corr_lag0") == c0:
            rep.ok("doc == recompute: n=%d sign_agree=%.1f%% "
                   "corr_lag0=%s leads_1m=%s"
                   % (len(common), agree, c0,
                      cc.get("corr_mof_leads_1m")))
        else:
            rep.fail("mismatch doc=%s vs n=%d agree=%s c0=%s"
                     % ({k: cc.get(k) for k in
                         ("status", "n_months",
                          "sign_agree_pct", "corr_lag0")},
                        len(common), agree, c0))
            FAILED.append("recompute")
        for r in cc.get("last3") or []:
            rep.log("  %s  MOF %+0.1f JPYbn | TIC %+0.2f $bn"
                    % (r["month"], r["mof_out_lt_jpybn"],
                       r["tic_japan_treas_usdbn"]))

        rep.heading("3. page")
        html = PAGE.read_text(encoding="utf-8")
        if "tic_concordance" in html:
            rep.ok("  committed token")
        else:
            rep.fail("  token missing")
            sys.exit(1)
        t0 = time.time()
        while time.time() - t0 < 480:
            try:
                req2 = urllib.request.Request(
                    "https://justhodl.ai/global-flows.html"
                    "?t=%d" % int(time.time()),
                    headers={"User-Agent": "ops-4878",
                             "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req2, timeout=45) \
                        as r2:
                    if "tic_concordance" in r2.read().decode(
                            "utf-8", "replace"):
                        rep.ok("  SERVED (%ds)"
                               % int(time.time() - t0))
                        break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(30)
        else:
            rep.fail("  not served")
            FAILED.append("served")

        rep.heading("4. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("weekly carry bid formally joined to the "
               "monthly TIC print -- stats honest, "
               "independently recomputed")


if __name__ == "__main__":
    main()
