#!/usr/bin/env python3
"""ops 2979 -- DIAGNOSTICS BUNDLE before surgical fixes.
Three open items need ground truth, gathered runner-side in one pass:

A) NAAIM (feed data/naaim.json STALE per wiring doc): invoke the engine
   synchronously and capture the real failure; check its schedule/rule
   state; probe naaim.org from the runner (GH IPs) to separate
   WAF-blocking from parse rot; read the stale doc's age + last values.
B) revision-breadth fallback source: dump data/sellside-views.json
   structure (top keys, row count, first-row keys, sample values) --
   the module note truncated before revealing why zero rows signed.
C) implied-corr: probe CBOE endpoints (delayed_quotes charts historical
   + us_indices daily_prices COR3M_History.csv) recording status +
   first bytes, so the next engine edit targets a URL proven to work.

PASS = diagnostics gathered (fails only on total inability to gather).
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=310, connect_timeout=10,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
EVT = boto3.client("events", region_name="us-east-1")
SCHED = boto3.client("scheduler", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]


def http_probe(url, timeout=25):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
                          "AppleWebKit/537.36"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read(4000)
            return {"status": r.status, "bytes": len(body),
                    "head": body[:220].decode("utf-8", "replace")}
    except Exception as e:
        return {"error": str(e)[:150]}


def main():
    fails, warns = [], []
    hl = {}
    with report("2979_diagnostics") as rep:

        rep.section("A1. NAAIM stale doc state")
        try:
            doc = json.loads(S3.get_object(
                Bucket=BUCKET, Key="data/naaim.json")["Body"].read())
            hl["naaim_doc"] = {
                "generated_at": doc.get("generated_at"),
                "latest": doc.get("latest"),
                "history_n": doc.get("history_n"),
                "column_mode": doc.get("column_mode"),
                "latest_source": doc.get("latest_source")}
            rep.kv(**{"naaim_" + k: json.dumps(v)[:120]
                      for k, v in hl["naaim_doc"].items()})
        except Exception as e:
            warns.append("naaim doc read: %s" % str(e)[:80])

        rep.section("A2. NAAIM schedule state")
        sched_state = {}
        try:
            s = SCHED.get_schedule(Name="justhodl-naaim-weekly",
                                   GroupName="default")
            sched_state = {"scheduler": s.get("State"),
                           "expr": s.get("ScheduleExpression")}
        except Exception:
            pass
        try:
            rules = EVT.list_rules(NamePrefix="justhodl-naaim")["Rules"] \
                + EVT.list_rules(NamePrefix="naaim")["Rules"]
            for r in rules:
                sched_state["rule:" + r["Name"]] = "%s %s" % (
                    r.get("State"), r.get("ScheduleExpression"))
        except Exception as e:
            warns.append("rule list: %s" % str(e)[:60])
        rep.kv(naaim_schedules=json.dumps(sched_state)[:300] or "NONE")
        hl["naaim_schedules"] = sched_state
        if not sched_state:
            warns.append("NAAIM has NO schedule at all -- root cause "
                         "candidate")

        rep.section("A3. NAAIM synchronous invoke (capture real error)")
        try:
            t0 = time.time()
            resp = LAM.invoke(FunctionName="justhodl-naaim",
                              InvocationType="RequestResponse",
                              LogType="Tail", Payload=b"{}")
            body = resp["Payload"].read().decode("utf-8", "replace")
            import base64
            logs = base64.b64decode(resp.get("LogResult") or b""
                                    ).decode("utf-8", "replace")
            rep.kv(invoke_s=round(time.time() - t0, 1),
                   fn_error=resp.get("FunctionError"),
                   body=body[:300])
            rep.log("---- naaim log tail ----")
            for ln in logs.splitlines()[-18:]:
                rep.log(ln[:180])
            hl["naaim_invoke"] = {"fn_error": resp.get("FunctionError"),
                                  "body": body[:400],
                                  "log_tail": logs[-1200:]}
        except Exception as e:
            warns.append("naaim invoke: %s" % str(e)[:100])

        rep.section("A4. naaim.org probes from runner")
        pr = {"page": http_probe(
            "https://naaim.org/programs/naaim-exposure-index/")}
        rep.kv(naaim_page=json.dumps(pr["page"])[:200])
        hl["naaim_probes"] = pr

        rep.section("B. sellside-views shape dump")
        try:
            sv = json.loads(S3.get_object(
                Bucket=BUCKET, Key="data/sellside-views.json")
                ["Body"].read())
            shape = {"type": type(sv).__name__}
            if isinstance(sv, dict):
                shape["top_keys"] = list(sv)[:10]
                best = []
                for k, v in sv.items():
                    if isinstance(v, list) and v and \
                            isinstance(v[0], dict) and len(v) > len(best):
                        best, shape["rows_key"] = v, k
                shape["rows_n"] = len(best)
                if best:
                    shape["row0_keys"] = sorted(best[0])[:16]
                    shape["row0_sample"] = {k: str(best[0].get(k))[:40]
                                            for k in list(best[0])[:8]}
            elif isinstance(sv, list):
                shape["rows_n"] = len(sv)
                if sv and isinstance(sv[0], dict):
                    shape["row0_keys"] = sorted(sv[0])[:16]
                    shape["row0_sample"] = {k: str(sv[0].get(k))[:40]
                                            for k in list(sv[0])[:8]}
            rep.kv(sellside=json.dumps(shape)[:600])
            hl["sellside_shape"] = shape
        except Exception as e:
            warns.append("sellside dump: %s" % str(e)[:80])

        rep.section("C. CBOE implied-corr endpoint probes")
        cb = {}
        for name, url in (
            ("hist_json", "https://cdn.cboe.com/api/global/"
             "delayed_quotes/charts/historical/_COR3M.json"),
            ("chart_json", "https://cdn.cboe.com/api/global/"
             "delayed_quotes/charts/_COR3M.json"),
            ("indices_csv", "https://cdn.cboe.com/api/global/us_indices/"
             "daily_prices/COR3M_History.csv"),
        ):
            cb[name] = http_probe(url)
            rep.kv(**{("cboe_" + name): json.dumps(cb[name])[:180]})
        hl["cboe_probes"] = cb

        gathered = sum(1 for k in ("naaim_invoke", "sellside_shape",
                                   "cboe_probes") if k in hl)
        if gathered < 2:
            fails.append("diagnostics gathering mostly failed (%d/3)"
                         % gathered)
        else:
            rep.ok("diagnostics gathered %d/3 sections" % gathered)

        out = {"ops": 2979, "fails": fails, "warns": warns,
               "verdict": "PASS" if not fails else "FAIL",
               "ts": datetime.now(timezone.utc).isoformat()}
        out.update(hl)
        rp = AWS_DIR / "ops" / "reports" / "2979.json"
        rp.parent.mkdir(parents=True, exist_ok=True)
        rp.write_text(json.dumps(out, indent=1))
        rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
        if fails:
            sys.exit(1)


main()
sys.exit(0)
