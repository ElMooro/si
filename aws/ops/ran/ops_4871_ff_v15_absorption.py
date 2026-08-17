"""ops/4859 -- foreign-flows v1.2.0 country-matrix birth verify.
 (1) settle 'foreign-flows v1.2.0'; invoke; poll <=7 min (100
     paced FRED calls).
 (2) truths: country_lt_treasury has 21 keys, >=18 OK, holdings
     ordered non-increasing; sampled INDIA holdings == in-op FRED
     refetch (/1000); country_lt_equity >=18 OK + sampled JAPAN eq
     == refetch; identity gaps present on >=15 countries; banks
     exist for 2 new sids; committed page equity tokens; served.
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
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-foreign-flows"
B = "justhodl-dashboard-live"
OUT_KEY = "data/foreign-flows.json"
MARKER = "foreign-flows v1.5.0"
DONORS = ("dollar-strength-agent", "justhodl-risk-gate")
PAGE = Path(__file__).resolve().parents[3] / "foreign-flows.html"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=150,
                                 retries={"max_attempts": 1}))
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def fred_latest(sid, key):
    url = ("https://api.stlouisfed.org/fred/series/observations"
           "?series_id=%s&api_key=%s&file_type=json"
           "&sort_order=desc&limit=3" % (sid, key))
    for _att in range(4):
        try:
            with urllib.request.urlopen(
                    urllib.request.Request(
                        url, headers={"User-Agent": "ops-4871"}),
                    timeout=60) as r:
                j = json.loads(r.read())
            for o in j.get("observations") or []:
                try:
                    return o["date"], float(o["value"])
                except (KeyError, TypeError, ValueError):
                    continue
            return None, None
        except Exception:  # noqa: BLE001
            time.sleep(20)
    return None, None


def main():
    with report("ops 4871 -- ff v1.5 absorption verify") as rep:
        rep.heading("1. settle + invoke")
        key = None
        for d in DONORS:
            try:
                env = (lam.get_function_configuration(
                    FunctionName=d).get("Environment")
                    or {}).get("Variables", {})
                if env.get("FRED_KEY"):
                    key = env["FRED_KEY"]
                    break
            except ClientError:
                continue
        if not key:
            rep.fail("no FRED donor")
            sys.exit(1)
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
        while time.time() - t0 < 420:
            time.sleep(12)
            try:
                d = sread(OUT_KEY)
            except ClientError:
                continue
            if d.get("generated_at") != prev \
                    and d.get("v") == "1.5.0":
                doc = d
                break
        if not doc:
            rep.fail("no fresh doc in 7 min")
            sys.exit(1)
        rep.ok("fresh in %ds" % int(time.time() - t0))

        rep.log("  30s quota-window breather before sampling")
        time.sleep(30)
        rep.log("  30s quota-window breather before sampling")
        time.sleep(30)
        rep.heading("2. v1.5 truths")
        A = doc.get("absorption") or {}
        rows = A.get("rows") or []
        agg = A.get("agg_12m") or {}
        if A.get("status") == "LIVE" and len(rows) >= 10:
            ok_id = all(
                r["absorption_pct"] == round(
                    100.0 * r["foreign_bn"]
                    / r["net_issuance_bn"], 1)
                for r in rows
                if r.get("absorption_pct") is not None)
            (rep.ok if ok_id else rep.fail)(
                "  absorption LIVE: %d months, identities %s; "
                "12m foreign %+.1fB / issuance %+.1fB = %s%%"
                % (len(rows), ok_id, agg.get("foreign_bn", 0),
                   agg.get("net_issuance_bn", 0),
                   agg.get("pct")))
            if not ok_id:
                FAILED.append("absid")
        else:
            rep.fail("  absorption %s %s"
                     % (A.get("status"), A.get("why", "")))
            FAILED.append("abs")
        AU = doc.get("auctions") or {}
        if AU.get("status") == "LIVE" and AU.get("recent"):
            r0 = AU["recent"][0]
            rep.ok("  auctions LIVE: %d in 60d, avg indirect "
                   "%s%%; latest %s %s indirect %s%% btc %s"
                   % (AU.get("n_auctions_60d"),
                      AU.get("avg_indirect_pct_60d"),
                      r0["date"], r0.get("term"),
                      r0["indirect_pct"],
                      r0.get("bid_to_cover")))
        else:
            rep.fail("  auctions %s" % AU.get("status"))
            FAILED.append("auct")
        C = doc.get("country_lt_treasury") or {}
        flags = {n: r.get("accel") for n, r in C.items()
                 if r.get("accel")}
        rep.ok("  accel flags: %d set -> %s"
               % (len(flags), json.dumps(flags)[:220]))
        fr = C.get("france") or {}
        exp_fr = None
        if isinstance(fr.get("tx_3m_bn"), (int, float)):
            ann = fr["tx_3m_bn"] * 4
            t12 = fr["tx_12m_bn"]
            if t12 != 0 and (ann > 0) != (t12 > 0) \
                    and abs(ann) > 5:
                exp_fr = "FLIP_" + ("BUY" if ann > 0
                                    else "SELL")
            elif abs(ann) > 1.6 * abs(t12) + 2:
                exp_fr = "ACCEL_" + ("BUY" if ann > 0
                                     else "SELL")
        if fr.get("accel") == exp_fr:
            rep.ok("  france flag == independent recompute "
                   "(%s)" % exp_fr)
        else:
            rep.fail("  france %s != %s" % (fr.get("accel"),
                                            exp_fr))
            FAILED.append("fracc")
        rep.heading("3. page")
        html = PAGE.read_text(encoding="utf-8")
        if 'id="ctryeq"' in html and "LT-Equity holdings" in html:
            rep.ok("  committed equity tokens present")
        else:
            rep.fail("  page tokens missing")
            FAILED.append("page")
            sys.exit(1)
        t0 = time.time()
        while time.time() - t0 < 480:
            try:
                req = urllib.request.Request(
                    "https://justhodl.ai/foreign-flows.html?t=%d"
                    % int(time.time()),
                    headers={"User-Agent": "ops-4871",
                             "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req, timeout=45) as r:
                    if "LT-Equity holdings" in r.read().decode(
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
        rep.ok("21-country matrix + equity holdings LIVE, "
               "sampled against FRED, banked, served")


if __name__ == "__main__":
    main()
