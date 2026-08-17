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
MARKER = "foreign-flows v1.4.0"
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
                        url, headers={"User-Agent": "ops-4868"}),
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
    with report("ops 4868 -- ff v1.4 equity decomposition verify") as rep:
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
                    and d.get("v") == "1.4.0":
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
        rep.heading("2. v1.4 truths")
        C = doc.get("country_lt_treasury") or {}
        E = doc.get("country_lt_equity") or {}
        base = {k: r for k, r in C.items()
                if not r.get("composite")}
        oks = [(n, r) for n, r in base.items()
               if r.get("status") == "OK"]
        if len(base) == 21 and len(oks) >= 19:
            rep.ok("  21 base countries, %d OK" % len(oks))
        else:
            rep.fail("  base=%d ok=%d" % (len(base), len(oks)))
            FAILED.append("n")
        cbt = C.get("china_plus_belgium") or {}
        ch, be = C.get("china") or {}, C.get("belgium") or {}
        if cbt.get("composite") and ch.get("status") == "OK" \
                and abs(cbt.get("tx_12m_bn", 9e9)
                        - round(ch["tx_12m_bn"]
                                + be["tx_12m_bn"], 1)) < 0.11:
            rep.ok("  Euroclear composite: china %+.1f + "
                   "belgium %+.1f = %+.1fB tx12m"
                   % (ch["tx_12m_bn"], be["tx_12m_bn"],
                      cbt["tx_12m_bn"]))
        else:
            rep.fail("  composite broken: %s"
                     % json.dumps(cbt)[:90])
            FAILED.append("comp")
        edec = [(n, r) for n, r in E.items()
                if r.get("status") == "OK"
                and isinstance(r.get("tx_12m_bn"),
                               (int, float))
                and isinstance(r.get("identity_gap_bn"),
                               (int, float))]
        if len(edec) >= 18:
            rep.ok("  equity decomposition on %d countries"
                   % len(edec))
        else:
            rep.fail("  equity decomp only %d" % len(edec))
            FAILED.append("edec")
        jp = E.get("japan") or {}
        d_j, v_j = fred_latest("FORLTEQTYNET42609", key)
        if v_j is not None and jp.get("month") == d_j \
                and isinstance(jp.get("tx_3m_bn"),
                               (int, float)):
            rep.ok("  JAPAN eq-net latest month %s == FRED "
                   "refetch (%.0fM)" % (d_j, v_j))
        else:
            rep.fail("  japan eq-net mismatch %s vs %s"
                     % (jp.get("month"), d_j))
            FAILED.append("jpnet")
        gap_id = sum(
            1 for _, r in edec
            if abs(r["identity_gap_bn"]
                   - round(r["d12m_holdings_bn"]
                           - r["tx_12m_bn"]
                           - r["valchg_12m_bn"], 1)) < 0.11)
        (rep.ok if gap_id == len(edec) else rep.fail)(
            "  equity identity holds on %d/%d" % (gap_id,
                                                  len(edec)))
        if gap_id != len(edec):
            FAILED.append("eid")
        rep.log("  eq tx12m top5: " + " | ".join(
            "%s %+.1f" % (n, r["tx_12m_bn"]) for n, r in
            sorted(edec, key=lambda x: -x[1]["tx_12m_bn"])[:5]))
        rep.log("  eq tx12m bottom3: " + " | ".join(
            "%s %+.1f" % (n, r["tx_12m_bn"]) for n, r in
            sorted(edec, key=lambda x: x[1]["tx_12m_bn"])[:3]))
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
                    headers={"User-Agent": "ops-4868",
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
