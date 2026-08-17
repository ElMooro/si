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
MARKER = "foreign-flows v1.3.0"
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
                        url, headers={"User-Agent": "ops-4861"}),
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
    with report("ops 4861 -- ff v1.3 full-render verify") as rep:
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
            if d.get("generated_at") != prev:
                doc = d
                break
        if not doc:
            rep.fail("no fresh doc in 7 min")
            sys.exit(1)
        rep.ok("fresh in %ds" % int(time.time() - t0))

        rep.log("  30s quota-window breather before sampling")
        time.sleep(30)
        rep.heading("2. matrix truths")
        C = doc.get("country_lt_treasury") or {}
        oks = [(n, r) for n, r in C.items()
               if r.get("status") == "OK"]
        if len(C) == 21 and len(oks) >= 19:
            rep.ok("  21 countries, %d OK" % len(oks))
        else:
            rep.fail("  keys=%d ok=%d" % (len(C), len(oks)))
            FAILED.append("n")
        hb = [r["holdings_bn"] for _, r in oks]
        if hb == sorted(hb, reverse=True):
            rep.ok("  ordered by holdings desc (top: %s %.0fB)"
                   % (oks[0][0], hb[0]))
        else:
            rep.fail("  ordering broken")
            FAILED.append("ord")
        ind = C.get("india") or {}
        d_i, v_i = fred_latest("FORLTTREASPOS42102", key)
        if ind.get("status") == "OK" and v_i is not None \
                and abs(ind["holdings_bn"]
                        - round(v_i / 1000.0, 1)) < 0.11 \
                and ind.get("month") == d_i:
            rep.ok("  INDIA %.1fB @ %s == FRED refetch"
                   % (ind["holdings_bn"], ind["month"]))
        else:
            rep.fail("  india %s vs fred %s/%s"
                     % (json.dumps(ind)[:70], d_i, v_i))
            FAILED.append("india")
        gaps = sum(1 for _, r in oks
                   if isinstance(r.get("identity_gap_bn"),
                                 (int, float)))
        (rep.ok if gaps >= 15 else rep.fail)(
            "  identity gaps on %d countries" % gaps)
        if gaps < 15:
            FAILED.append("gaps")
        E = doc.get("country_lt_equity") or {}
        eoks = [(n, r) for n, r in E.items()
                if r.get("status") == "OK"]
        jp = E.get("japan") or {}
        d_j, v_j = fred_latest("FORLTEQTYPOS42609", key)
        if len(eoks) >= 18 and jp.get("status") == "OK" \
                and v_j is not None \
                and abs(jp["holdings_bn"]
                        - round(v_j / 1000.0, 1)) < 0.11:
            rep.ok("  equity block %d OK; JAPAN eq %.1fB == "
                   "refetch" % (len(eoks), jp["holdings_bn"]))
        else:
            rep.fail("  eq ok=%d japan=%s vs %s"
                     % (len(eoks), json.dumps(jp)[:60], v_j))
            FAILED.append("eq")
        for sid in ("FORLTTREASPOS42102", "FORLTEQTYPOS42609"):
            try:
                s3.head_object(
                    Bucket=B,
                    Key="data/providers/tic-cslt/%s.json" % sid)
                rep.ok("  bank exists: %s" % sid)
            except ClientError:
                rep.fail("  bank missing: %s" % sid)
                FAILED.append("bank")
        H = doc.get("hist_10y") or {}
        need = ("total", "treas", "equity", "corp", "agency",
                "tbills", "sig_risk_appetite", "sig_safe_haven",
                "sig_total_demand", "sig_official_private")
        okH = (all(k in H for k in need)
               and H["total"]["vals"][-1]
               == doc["flows_bn"]["total"]["latest"]
               and H["total"]["dates"][-1]
               == doc["latest_month"]
               and all(len(v["vals"]) <= 120
                       and len(v["dates"]) == len(v["vals"])
                       for v in H.values()))
        if okH:
            rep.ok("  hist_10y: 10 series, identity + calendar "
                   "verified (total tail %s=%.1f)"
                   % (H["total"]["dates"][-1],
                      H["total"]["vals"][-1]))
        else:
            rep.fail("  hist_10y broken: keys=%s"
                     % sorted(H)[:12])
            FAILED.append("hist")
        SP = doc.get("holder_splits") or {}
        sp_ok = sum(1 for r in SP.values()
                    if r.get("status") == "OK")
        if len(SP) >= 5 and sp_ok >= 4:
            rep.ok("  holder splits: %d families, %d OK"
                   % (len(SP), sp_ok))
        else:
            rep.fail("  splits %d/%d" % (sp_ok, len(SP)))
            FAILED.append("splits")
        t3 = sum(1 for _, r in oks
                 if isinstance(r.get("tx_3m_bn"), (int, float)))
        (rep.ok if t3 >= 18 else rep.fail)(
            "  tx_3m on %d countries" % t3)
        if t3 < 18:
            FAILED.append("t3")
        rep.log("  readout top6: " + " | ".join(
            "%s %.0f (tx12m %s)" % (n, r["holdings_bn"],
                                    r.get("tx_12m_bn"))
            for n, r in oks[:6]))

        rep.heading("3. page")
        html = PAGE.read_text(encoding="utf-8")
        if 'id="ctryeq"' in html and "LT-Equity holdings" in html \
                and 'id="hist"' in html \
                and 'id="splits"' in html \
                and "tx 3m" in html:
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
                    headers={"User-Agent": "ops-4861",
                             "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req, timeout=45) as r:
                    if 'id="splits"' in r.read().decode(
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
        rep.ok("full render LIVE: 10-series 10y history, six split "
               "families, tx_3m -- charts + tables served")


if __name__ == "__main__":
    main()
