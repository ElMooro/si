"""ops/4876 -- global-flows v1.3 Japan MOF weekly verify.
 G0  import the ENGINE's parse_mof and run it on the LIVE CSV:
     >=100 rows, print the last 3 parsed (shared-parser proof).
 (1) active-wait + settle 'global-flows v1.3.0'; invoke; poll
     v==1.3.0.
 (2) truths: japan LIVE weekly (n_weeks == G0 count); sampled
     inward_lt latest == last G0-parsed row (independent path);
     mof bank rows >= 100; peru + taiwan intact; page carries
     window_label render; served.
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
MARKER = "global-flows v1.3.0"
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
    with report("ops 4876 -- mof wire verify") as rep:
        rep.heading("G0. shared parser vs live CSV")
        req = urllib.request.Request(
            engmod.MOF_URL,
            headers={"User-Agent": "ops-4876"})
        with urllib.request.urlopen(req, timeout=60) as r:
            txt = r.read().decode("cp932", "replace")
        rows = engmod.parse_mof(txt)
        if len(rows) >= 100:
            rep.ok("parse_mof: %d weekly rows" % len(rows))
        else:
            rep.fail("only %d rows parsed" % len(rows))
            sys.exit(1)
        for r in rows[-3:]:
            rep.log("  %s | out_lt %s out_eq %s | in_lt %s "
                    "in_eq %s in_total %s"
                    % (r["period"][:22], r["out_lt"],
                       r["out_eq"], r["in_lt"], r["in_eq"],
                       r["in_total"]))
        last = rows[-1]

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
                    and d.get("v") == "1.3.0":
                doc = d
                break
        if not doc:
            rep.fail("no fresh v1.3.0 doc")
            sys.exit(1)
        rep.ok("fresh in %ds" % int(time.time() - t0))

        rep.heading("2. truths")
        jp = (doc.get("countries") or {}).get("japan") or {}
        if jp.get("status") == "LIVE" \
                and jp.get("n_weeks") == len(rows) \
                and jp.get("latest_period") == last["period"]:
            rep.ok("  japan LIVE: %d weeks, latest %r"
                   % (jp["n_weeks"], jp["latest_period"][:22]))
        else:
            rep.fail("  japan %s n=%s latest=%r"
                     % (jp.get("status"), jp.get("n_weeks"),
                        str(jp.get("latest_period"))[:22]))
            FAILED.append("jp")
        ser = jp.get("series") or {}
        if (ser.get("inward_lt_bonds") or {}).get("latest") \
                == last["in_lt"] \
                and (ser.get("outward_lt_bonds") or {}
                     ).get("latest") == last["out_lt"]:
            rep.ok("  sampled nets == independent parse "
                   "(in_lt %+0.1f, out_lt %+0.1f JPYbn)"
                   % (last["in_lt"], last["out_lt"]))
        else:
            rep.fail("  net mismatch: doc in_lt=%s vs %s"
                     % ((ser.get("inward_lt_bonds") or {})
                        .get("latest"), last["in_lt"]))
            FAILED.append("nets")
        for nm in ("inward_equity", "outward_equity",
                   "inward_total", "outward_total"):
            s = ser.get(nm) or {}
            rep.log("  %-16s latest %+0.1f 4w %+0.1f z %s"
                    % (nm, s.get("latest") or 0,
                       s.get("sum_4q") or 0, s.get("z_all")))
        try:
            bank = sread(engmod.MOF_BANK)
            n = len(bank.get("rows") or {})
            (rep.ok if n >= 100 else rep.fail)(
                "  mof bank rows=%d" % n)
            if n < 100:
                FAILED.append("bank")
        except ClientError:
            rep.fail("  mof bank missing")
            FAILED.append("bank")
        pe = (doc["countries"].get("peru") or {}).get("status")
        tw = (doc["countries"].get("taiwan") or {}).get(
            "status")
        if pe == "LIVE" and tw == "LIVE":
            rep.ok("  peru + taiwan intact")
        else:
            rep.fail("  pe=%s tw=%s" % (pe, tw))
            FAILED.append("others")

        rep.heading("3. page")
        html = PAGE.read_text(encoding="utf-8")
        if "window_label" in html:
            rep.ok("  committed window_label render")
        else:
            rep.fail("  page token missing")
            sys.exit(1)
        t0 = time.time()
        while time.time() - t0 < 480:
            try:
                req2 = urllib.request.Request(
                    "https://justhodl.ai/global-flows.html?t=%d"
                    % int(time.time()),
                    headers={"User-Agent": "ops-4876",
                             "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req2, timeout=45) \
                        as r2:
                    if "window_label" in r2.read().decode(
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
        rep.ok("Japan MOF weekly LIVE both directions -- the "
               "carry channel is on the desk")


if __name__ == "__main__":
    main()
