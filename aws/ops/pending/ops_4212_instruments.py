"""ops_4212 — pass-2 check + TE offered-vs-taken (done right) + DESCS
corpus hunt (workbench/notes), conditional vault fire."""
import json
import re
import sys
import time
import urllib.request
from collections import Counter
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def fetch(url, timeout=45):
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except Exception as e:
        return -1, str(e)[:160]


def main():
    with report("4212_instruments") as rep:
        rep.heading("ops 4212 — pass-2 + offered-vs-taken + descs hunt")

        rep.section("A. lottery yield-class status")
        v = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        ylive, ypend, ynfs = [], 0, 0
        lotto_pend = 0
        for r in v.get("symbols") or []:
            sy = str(r.get("symbol"))
            if re.match(r"^[A-Z]{2}\d{2}Y?$", sy):
                st2 = r.get("status")
                if st2 == "LIVE":
                    ylive.append(sy)
                elif st2 == "PENDING_RESOLUTION":
                    ypend += 1
                else:
                    ynfs += 1
            if "lottery" in str(r.get("resolution_note")):
                lotto_pend += 1
        rep.kv(yield_live=len(ylive), yield_pending=ypend,
               yield_nfs=ynfs, lottery_notes=lotto_pend)
        rep.log("  live yields: " + json.dumps(sorted(ylive)[:16]))
        if ypend or lotto_pend:
            lam.invoke(FunctionName="justhodl-tradingview",
                       InvocationType="Event", Payload=b"{}")
            rep.ok("  vault fired (ladder pass for pending lottery)")

        rep.section("B. TE offered-vs-taken (CAT-only regex)")
        te = ssm.get_parameter(Name="/justhodl/te_api",
                               WithDecryption=True)["Parameter"]["Value"]
        fs = (ROOT / "lambdas" / "justhodl-te-feed" / "source" /
              "lambda_function.py").read_text()
        catblk = fs[fs.index("CAT = {"):fs.index("PRIORITY = ")]
        mapped = set(re.findall(r'"([^"]+)":', catblk))
        rep.kv(cat_mapped=len(mapped))
        st, x = fetch("https://api.tradingeconomics.com/country/"
                      "united%20states?c=" + te + "&f=json", 60)
        if st != 200:
            time.sleep(20)
            st, x = fetch("https://api.tradingeconomics.com/country/"
                          "united%20states?c=" + te + "&f=json", 60)
        rep.kv(te_status=st)
        unmapped = Counter()
        if st == 200:
            try:
                for r2 in json.loads(x):
                    c = str(r2.get("Category") or "")
                    if c and c not in mapped and \
                            r2.get("LatestValue") is not None:
                        unmapped[c] += 1
            except Exception:
                pass
            rep.kv(us_unmapped=len(unmapped))
            rep.log("  OFFERED-NOT-TAKEN: " + json.dumps(
                sorted(unmapped)[:44])[:900])
        else:
            rep.log("  TE wall persists: " + x[:150])

        rep.section("C. DESCS corpus hunt")
        for key in ("data/tv-workbench.json",):
            try:
                head = s3.get_object(Bucket=BUCKET, Key=key,
                                     Range="bytes=0-40000")["Body"].read()
                t = head.decode("utf-8", "ignore")
                has_desc = t.count('"description"') \
                    + t.count('"desc"')
                rep.log(f"  {key}: first40KB desc-fields={has_desc} "
                        f"keys~ {t[:180]}")
            except Exception as e3:
                rep.log(f"  {key}: {type(e3).__name__}")
        try:
            nk = [o["Key"] for o in s3.list_objects_v2(
                Bucket=BUCKET, Prefix="notes/").get("Contents", [])[:5]]
            rep.log("  notes/ sample keys: " + json.dumps(nk)[:200])
        except Exception:
            pass
        rep.ok("INSTRUMENTS PASS DONE")


if __name__ == "__main__":
    main()
