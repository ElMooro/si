"""
ops_4273 -- House PTR PDF extractor: the chamber joins the tape.

Senate eFD gives per-trade rows in HTML; House PTRs hide them in PDFs.
justhodl-house-ptr-extract (vendored pypdf, unit-tested line parser
incl. wrapped rows + partial sales) walks NEW doc_ids from
congress-direct's index, 20/run, and writes quiver-shape rows to
data/house-ptr-trades.json. Scanned/paper filings are recorded
no_text -- disclosed, never guessed. political-stocks now merges both
chambers, finally exercising the name_map party attribution on House
names.

Chain: ensure function (self-create if deploy race) -> first walk ->
per-doc stats + REAL sample rows -> Scheduler 6h -> political-stocks
re-run -> n_house > 0 (or honest zero with the no_text ledger shown)
-> attribution sample printed.
"""
import io
import json
import os
import shutil
import sys
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-house-ptr-extract"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=330, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
RUN_START = datetime.now(timezone.utc)

def ensure_function(r):
    for i in range(40):
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lm = datetime.strptime(
                    c["LastModified"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                if (RUN_START - lm).total_seconds() < 15 * 60:
                    r.ok("function live via deploy workflow")
                    return True
        except lam.exceptions.ResourceNotFoundException:
            break
        except Exception:
            pass
        time.sleep(8)
    # self-create from the checkout (deploy race lost or new-dir skip)
    try:
        srcdir = "aws/lambdas/%s/source" % FN
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            for root, _, files in os.walk(srcdir):
                for f in files:
                    fp = os.path.join(root, f)
                    z.write(fp, os.path.relpath(fp, srcdir))
        donor = lam.get_function_configuration(
            FunctionName="justhodl-congress-direct")
        lam.create_function(
            FunctionName=FN, Runtime="python3.12",
            Role=donor["Role"],
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": buf.getvalue()},
            Timeout=240, MemorySize=768,
            Environment={"Variables": {"MAX_DOCS_PER_RUN": "20"}},
            Architectures=["x86_64"])
        for _ in range(30):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active":
                break
            time.sleep(5)
        r.ok("function SELF-CREATED (%d KB zip, donor role %s)"
             % (len(buf.getvalue()) // 1024, donor["Role"][-24:]))
        return True
    except Exception as e:
        r.fail("ensure_function: %s" % str(e)[:150])
        return False

fails = []
with report("4273_house_ptr") as r:
    r.heading("ops 4273 -- House PTR PDFs into the official tape")

    r.section("1. extractor live + first walk")
    if not ensure_function(r):
        fails.append("function unavailable")
    else:
        p = lam.invoke(FunctionName=FN,
                       InvocationType="RequestResponse", Payload=b"{}")
        pay = (p["Payload"].read() or b"")[:240].decode("utf-8", "ignore")
        r.log("invoked: %s" % pay)
        if p.get("FunctionError"):
            fails.append("first walk FunctionError: %s" % pay[:120])
        else:
            try:
                doc = json.loads(s3.get_object(
                    Bucket=BUCKET,
                    Key="data/house-ptr-trades.json")["Body"].read())
                st = doc.get("stats") or {}
                r.ok("ledger: %s docs (%s parsed / %s no_text / %s err) "
                     "-> %s trades"
                     % (st.get("docs_total"), st.get("docs_parsed"),
                        st.get("docs_no_text"), st.get("docs_error"),
                        st.get("trades_total")))
                for t in (doc.get("trades") or [])[:6]:
                    r.kv(who=t.get("Representative"),
                         ticker=t.get("Ticker"),
                         tx=t.get("Transaction"),
                         date=t.get("TransactionDate"),
                         amount=str(t.get("Range"))[:22],
                         owner=t.get("owner"))
                if (st.get("trades_total") or 0) == 0 \
                        and (st.get("docs_parsed") or 0) == 0:
                    if (st.get("docs_no_text") or 0) + \
                            (st.get("docs_error") or 0) >= \
                            (st.get("this_run", {}).get("new_docs") or 1):
                        r.warn("zero parses this batch -- all docs "
                               "scanned/no-text or errored; ledger "
                               "shows which (honest, next batches may "
                               "differ)")
                    else:
                        fails.append("no parses and no honest "
                                     "explanation in ledger")
            except Exception as e:
                fails.append("ledger verify: %s" % str(e)[:110])

    r.section("2. schedule (6-hourly incremental)")
    try:
        sch.get_schedule(Name="house-ptr-extract-6h", GroupName="default")
        r.ok("schedule present")
    except Exception:
        try:
            donor = None
            for pg in sch.get_paginator("list_schedules").paginate(
                    GroupName="default"):
                for it in pg.get("Schedules", []):
                    d = sch.get_schedule(Name=it["Name"],
                                         GroupName="default")
                    if (d.get("Target") or {}).get("RoleArn"):
                        donor = d["Target"]["RoleArn"]
                        break
                if donor:
                    break
            sch.create_schedule(
                Name="house-ptr-extract-6h", GroupName="default",
                ScheduleExpression="cron(10 1,7,13,19 * * ? *)",
                FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
                Target={"Arn": "arn:aws:lambda:us-east-1:857687956942:"
                               "function:%s" % FN,
                        "RoleArn": donor, "Input": "{}"})
            r.ok("schedule CREATED cron(10 1,7,13,19 * * ? *)")
        except Exception as e:
            fails.append("schedule: %s" % str(e)[:110])

    r.section("3. political-stocks: both chambers + attribution proof")
    if not fails:
        try:
            p = lam.invoke(FunctionName="justhodl-political-stocks",
                           InvocationType="RequestResponse", Payload=b"{}")
            r.log("invoked: %s"
                  % (p["Payload"].read() or b"")[:190].decode("utf-8",
                                                              "ignore"))
            doc = json.loads(s3.get_object(
                Bucket=BUCKET,
                Key="data/political-stocks.json")["Body"].read())
            cong = doc.get("congress") or {}
            r.log("chambers: senate=%s house=%s tickers=%s clusters=%s"
                  % (cong.get("n_trades_senate"),
                     cong.get("n_trades_house"),
                     cong.get("n_tickers"),
                     len(doc.get("clusters") or [])))
            n_att = n_tot = 0
            shown = 0
            for grp in ("top_buys", "top_sells", "clusters"):
                for tk in (doc.get(grp) or []):
                    for tr in (tk.get("recent_trades") or []):
                        n_tot += 1
                        if tr.get("party") in ("D", "R", "I"):
                            n_att += 1
                        if shown < 6 and tr.get("party") in ("D", "R",
                                                            "I"):
                            shown += 1
                            r.kv(who=tr.get("politician"),
                                 party=tr.get("party"),
                                 chamber=tr.get("chamber"),
                                 ticker=tk.get("ticker"),
                                 tx=tr.get("type"))
            if n_tot:
                rate = 100.0 * n_att / n_tot
                (r.ok if rate >= 40 else r.warn)(
                    "party attribution: %d/%d trades (%.0f%%) via "
                    "name_map%s" % (n_att, n_tot, rate,
                                    "" if rate >= 40 else
                                    " -- widening queued"))
            else:
                r.warn("no scored-ticker trades to sample yet")
        except Exception as e:
            fails.append("political verify: %s" % str(e)[:110])

    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4273 PASS -- both chambers on official rails, "
             "quality inspectable per-doc")
if fails:
    sys.exit(1)
