"""ops/4841 -- Taiwan regate: invoke-to-complete backfill.
4839 truth: TWSE throttles bursts -- 35/90 days landed.  v1.1.1
paces (2.2s) + caps 45 attempts/invoke; this op loops invokes
(up to 4 rounds) until the ledger holds >=55 days, then reruns the
full truth block.
 G0  live CBC: labels locate (total/equity/debt-after-equity),
     row width == len(labels)+1, last period >= 2025Q4; live TWSE:
     stat OK + foreign rows present (net printed).
 (1) settle marker 'global-flows v1.1.0'; schedule swap weekly ->
     daily 10:00 UTC.
 (2) Event-invoke {"twse_backfill_days": 90}; poll <=6 min.
 (3) truths: taiwan macro LIVE, portfolio_liab_total latest ==
     in-op CBC refetch (+1 offset), period 2026Q1; hot_money
     ledger >=55 days, 5/20/60d sums present, sampled 20260814 ==
     in-op TWSE refetch; peru untouched LIVE; deferred ==
     {korea, chile, imf_layer}; banks exist.
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
ACCOUNT = "857687956942"
FN = "justhodl-global-flows"
B = "justhodl-dashboard-live"
OUT_KEY = "data/global-flows.json"
MARKER = "global-flows v1.1.1"
LAB_TOT = "Portfolio investment-Liabilities"
LAB_EQ = ("Portfolio investment-Equity and investment fund "
          "shares-Liabilities")
LAB_DEBT = "Debt securities-Liabilities"
UA = {"User-Agent": "ops-4839"}

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
sched = boto3.client("scheduler", region_name=REGION)
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def get_json(url, timeout=70):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def cbc():
    j = get_json("https://cpx.cbc.gov.tw/API/DataAPI/Get"
                 "?FileName=BPP2Q01en")
    labels = [(x or {}).get("data") or ""
              for x in j["data"]["structure"]["Table1"]]
    rows = j["data"]["dataSets"]
    return labels, rows


def twse(day=None):
    url = ("https://www.twse.com.tw/rwd/en/fund/BFI82U"
           "?response=json"
           + ("&dayDate=%s&type=day" % day if day else ""))
    return get_json(url, timeout=45)


def settle(rep):
    for att in range(30):
        try:
            gf = lam.get_function(FunctionName=FN)
            raw = urllib.request.urlopen(gf["Code"]["Location"],
                                         timeout=60).read()
            src = zipfile.ZipFile(io.BytesIO(raw)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if MARKER in src:
                rep.ok("marker settled (attempt %d)" % (att + 1))
                return True
        except (ClientError, Exception):  # noqa: BLE001
            pass
        time.sleep(10)
    rep.fail("no marker")
    FAILED.append("settle")
    return False


def swap_schedule(rep):
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FN}"
    role = f"arn:aws:iam::{ACCOUNT}:role/justhodl-scheduler-role"
    target = {"Arn": fn_arn, "RoleArn": role, "Input": "{}",
              "RetryPolicy": {"MaximumRetryAttempts": 2,
                              "MaximumEventAgeInSeconds": 3600}}
    try:
        sched.create_schedule(Name="justhodl-global-flows-daily",
                              ScheduleExpression="cron(0 10 * * "
                              "? *)",
                              ScheduleExpressionTimezone="UTC",
                              FlexibleTimeWindow={"Mode": "OFF"},
                              State="ENABLED", Target=target,
                              Description="global-flows daily "
                              "(ops 4839)")
        rep.ok("daily schedule created (10:00 UTC)")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConflictException":
            rep.ok("daily schedule exists")
        else:
            rep.fail("daily schedule: %s" % e)
            FAILED.append("sched")
    try:
        sched.delete_schedule(Name="justhodl-global-flows-weekly")
        rep.ok("weekly schedule retired")
    except ClientError:
        rep.log("weekly schedule already gone")


def main():
    with report("ops 4841 -- taiwan regate invoke-to-complete") as rep:
        rep.heading("G0. CBC + TWSE live contracts")
        try:
            labels, rows = cbc()
            i_tot = labels.index(LAB_TOT)
            i_eq = labels.index(LAB_EQ)
            i_debt = next(i for i in range(i_eq + 1, len(labels))
                          if labels[i] == LAB_DEBT)
            widths = {len(r) for r in rows[-4:]}
            last_per = str(rows[-1][0])
            if widths == {len(labels) + 1} \
                    and last_per >= "2025Q4":
                rep.ok("CBC: labels @%d/%d/%d, width %d==%d+1, "
                       "last %s" % (i_tot, i_eq, i_debt,
                                    len(labels) + 1, len(labels),
                                    last_per))
            else:
                rep.fail("CBC shape: widths=%s last=%s"
                         % (widths, last_per))
                FAILED.append("g0_cbc")
        except Exception as e:  # noqa: BLE001
            rep.fail("CBC G0 died: %s" % str(e)[:90])
            FAILED.append("g0_cbc")
        try:
            j = twse()
            frows = [r for r in j.get("data") or []
                     if "foreign" in str(r[0]).lower()]
            net = sum(float(str(r[3]).replace(",", ""))
                      for r in frows)
            if j.get("stat") == "OK" and frows:
                rep.ok("TWSE: stat OK date=%s foreign rows=%d "
                       "net=%.2fbn NT$" % (j.get("date"),
                                           len(frows), net / 1e9))
            else:
                rep.fail("TWSE: stat=%s" % j.get("stat"))
                FAILED.append("g0_twse")
        except Exception as e:  # noqa: BLE001
            rep.fail("TWSE G0 died: %s" % str(e)[:90])
            FAILED.append("g0_twse")
        if FAILED:
            sys.exit(1)

        rep.heading("1. settle + schedule swap")
        if not settle(rep):
            sys.exit(1)
        swap_schedule(rep)

        rep.heading("2. invoke-to-complete backfill rounds")
        doc = None
        for rnd in range(1, 5):
            try:
                prev = sread(OUT_KEY).get("generated_at")
            except ClientError:
                prev = None
            lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=json.dumps({"twse_backfill_days":
                                           90}).encode())
            t0 = time.time()
            fresh = None
            while time.time() - t0 < 300:
                time.sleep(12)
                try:
                    d = sread(OUT_KEY)
                except ClientError:
                    continue
                if d.get("generated_at") != prev:
                    fresh = d
                    break
            if not fresh:
                rep.fail("round %d: no fresh doc" % rnd)
                sys.exit(1)
            doc = fresh
            hm_r = ((doc.get("countries") or {})
                    .get("taiwan") or {}).get("hot_money") or {}
            rep.ok("round %d: ledger=%s attempts=%s new=%s"
                   % (rnd, hm_r.get("ledger_days"),
                      hm_r.get("backfill_attempts"),
                      hm_r.get("backfilled")))
            if (hm_r.get("ledger_days") or 0) >= 55:
                break

        rep.heading("3. truths")
        tw = (doc.get("countries") or {}).get("taiwan") or {}
        mac = tw.get("macro") or {}
        hm = tw.get("hot_money") or {}
        if tw.get("status") == "LIVE" and mac.get("status") \
                == "LIVE":
            rep.ok("  taiwan LIVE; macro %s"
                   % mac.get("latest_period"))
        else:
            rep.fail("  taiwan=%s macro=%s why=%s"
                     % (tw.get("status"), mac.get("status"),
                        mac.get("why")))
            FAILED.append("tw")
        labels, rows = cbc()
        i_tot = labels.index(LAB_TOT)
        last_ok = next(r for r in reversed(rows)
                       if r[1 + i_tot] not in (None, "-", ""))
        iv = round(float(last_ok[1 + i_tot]), 1)
        s_tot = (mac.get("series") or {}).get(
            "portfolio_liab_total") or {}
        if s_tot.get("latest") == iv and s_tot.get("period") \
                == str(last_ok[0]):
            rep.ok("  macro total == in-op CBC refetch "
                   "(%+.1fM @ %s)" % (iv, last_ok[0]))
        else:
            rep.fail("  macro diverges eng=%s@%s ind=%s@%s"
                     % (s_tot.get("latest"), s_tot.get("period"),
                        iv, last_ok[0]))
            FAILED.append("cbc_ind")
        n_led = hm.get("ledger_days") or 0
        sums_ok = all(hm.get("sum_%dd_bn" % w) is not None
                      for w in (5, 20, 60))
        if n_led >= 55 and sums_ok:
            rep.ok("  hot_money ledger=%d days; 5/20/60d present"
                   % n_led)
        else:
            rep.fail("  hot_money thin: n=%d sums_ok=%s"
                     % (n_led, sums_ok))
            FAILED.append("hm")
        try:
            j14 = twse("20260814")
            net14 = sum(float(str(r[3]).replace(",", ""))
                        for r in j14["data"]
                        if "foreign" in str(r[0]).lower())
            led = sread("data/providers/twse/"
                        "bfi82u-foreign.json")["rows"]
            if abs(led.get("20260814", 1e18) - net14) < 1.0:
                rep.ok("  sampled 20260814 ledger == refetch "
                       "(%.2fbn)" % (net14 / 1e9))
            else:
                rep.fail("  20260814 ledger=%s refetch=%s"
                         % (led.get("20260814"), net14))
                FAILED.append("sample")
        except Exception as e:  # noqa: BLE001
            rep.fail("  sample check died: %s" % str(e)[:80])
            FAILED.append("sample")
        pe = (doc.get("countries") or {}).get("peru") or {}
        if pe.get("status") == "LIVE":
            rep.ok("  peru untouched LIVE (%s)"
                   % pe.get("latest_period"))
        else:
            rep.fail("  peru=%s" % pe.get("status"))
            FAILED.append("peru")
        if set(doc.get("deferred") or {}) == {"korea", "chile",
                                              "imf_layer"}:
            rep.ok("  deferred == korea/chile/imf_layer")
        else:
            rep.fail("  deferred=%s"
                     % sorted(doc.get("deferred") or {}))
            FAILED.append("def")
        for bk in ("data/providers/cbc/portfolio_liab_total.json",
                   "data/providers/twse/bfi82u-foreign.json"):
            try:
                n = len(sread(bk).get("rows") or {})
                rep.ok("  bank %s n=%d" % (bk.split("/")[-1], n))
            except ClientError:
                rep.fail("  bank %s missing" % bk)
                FAILED.append("bank")

        rep.heading("4. readout")
        for k, s in (mac.get("series") or {}).items():
            rep.log("  TW %-22s %+9.1fM  4Q %+10.1f  z=%s"
                    % (k, s.get("latest", 0), s.get("sum_4q", 0),
                       s.get("z_all")))
        rep.log("  TW hot money: %s @ %s | 5d %s | 20d %s | "
                "60d %s NT$bn z=%s"
                % (hm.get("latest_bn"), hm.get("latest_day"),
                   hm.get("sum_5d_bn"), hm.get("sum_20d_bn"),
                   hm.get("sum_60d_bn"), hm.get("z_60d")))

        rep.heading("5. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("Taiwan LIVE -- semiconductor specialist wired: "
               "quarterly CBC macro + daily TWSE hot money with "
               "backfilled ledger")


if __name__ == "__main__":
    main()
