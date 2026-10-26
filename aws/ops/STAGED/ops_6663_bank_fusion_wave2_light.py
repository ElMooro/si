"""ops 6663 - bank fusion wave 2, first light (read-mostly; generic): kick each fused engine once, wait for its output to
refresh, then read the output for the fusion labels (generic walk: universe_basis / price_source / source_used /
feed_basis / book_as_of / bank_context / squeeze / source / target_basis / n_book_candidates / ret_60d_basis ...), the
REPORT lines (duration / max memory / status) and the log group for errors.  Verdict per engine.  Nothing is written
except the engines' own outputs.  No key involved.

FUNCTIONS (env, space-separated) overrides the wave-2 list; OUTPUTS (env, JSON {fn: key}) overrides the output keys
(default data/<name without justhodl->.json).
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import os
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=300)
WAVE2 = ("justhodl-pre-pump-detector justhodl-pump-mechanics justhodl-ignition justhodl-momentum-leaders justhodl-pump-positioning "
         "justhodl-coffee-can justhodl-buyback-yield-ranking justhodl-eva-spread justhodl-smart-beta justhodl-dividend-growth "
         "justhodl-pead-detector justhodl-post-earnings-mean-rev justhodl-rating-change-cluster justhodl-starmine justhodl-estimate-revisions "
         "justhodl-insider-cluster-scanner justhodl-insider-sell-cluster justhodl-insider-buyback-confluence justhodl-smart-money-holdings "
         "justhodl-smart-money-tracker justhodl-political-trades")
FUNCTIONS = (os.environ.get("FUNCTIONS") or WAVE2).split()
OUTPUTS = {"justhodl-pre-pump-detector": "data/pre-pump-signals.json", "justhodl-pead-detector": "data/pead-signals.json",
           "justhodl-insider-cluster-scanner": "data/insider-clusters.json", "justhodl-smart-money-holdings": "screener/smart-money-holdings.json",
           "justhodl-smart-money-tracker": "screener/smart-money.json"}
OUTPUTS.update(json.loads(os.environ.get("OUTPUTS") or "{}"))
LABELS = ("universe_basis", "price_source", "source_used", "feed_basis", "book_as_of", "bank_context", "squeeze", "source", "target_basis",
          "surprise_basis", "n_book_candidates", "book_screen", "ret_60d_basis", "vol_source", "universe", "n_from_book", "method", "version")
STAMPS = ("generated_at", "as_of", "updated_at", "generated", "timestamp", "ts", "run_at", "date")
WAIT_S = int(os.environ.get("WAIT_S") or 14 * 60)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def jget(s3, key):
    try:
        return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def out_key(fn):
    return OUTPUTS.get(fn) or "data/%s.json" % fn.replace("justhodl-", "", 1)


def stamp_of(doc):
    if not isinstance(doc, dict):
        return None
    for k in STAMPS:
        v = doc.get(k)
        if isinstance(v, str) and len(v) >= 10:
            return v
        if isinstance(v, (int, float)) and v > 1e9:
            return datetime.fromtimestamp(v if v < 1e11 else v / 1000, timezone.utc).isoformat()
    return None


def walk_labels(doc, found, depth=0, budget=(200000,)):
    """Count label keys anywhere in the document (bounded), keeping up to 4 distinct sample values per label."""
    if budget[0] <= 0 or depth > 7:
        return
    if isinstance(doc, dict):
        for k, v in doc.items():
            budget = (budget[0] - 1,)
            if k in LABELS:
                slot = found.setdefault(k, {"n": 0, "samples": []})
                slot["n"] += 1
                sv = json.dumps(v, default=str)[:90] if not isinstance(v, dict) else "{%s}" % ",".join(list(v)[:6])
                if sv not in slot["samples"] and len(slot["samples"]) < 4:
                    slot["samples"].append(sv)
            walk_labels(v, found, depth + 1, budget)
    elif isinstance(doc, list):
        for v in doc[:400]:
            walk_labels(v, found, depth + 1, budget)


def logs_tail(logs, fn, since_ms, pattern, limit=40):
    try:
        r = logs.filter_log_events(logGroupName="/aws/lambda/" + fn, startTime=since_ms, filterPattern=pattern, limit=limit)
        return [e.get("message", "").strip() for e in r.get("events", [])]
    except Exception as e:  # noqa: BLE001
        return ["(logs: %s)" % type(e).__name__]


def main():
    s3, lam, logs = c("s3"), c("lambda"), c("logs")
    with report("ops_6663_bank_fusion_wave2_light") as r:
        r.heading("ops 6663 - bank fusion wave 2: first light (%d engines)" % len(FUNCTIONS))
        r.section("0. the factor book")
        man = jget(s3, "data/warm/factor-book/manifest.json") or {}
        r.log("factor book: symbols %s updated %s as_of %s" % (man.get("symbols"), man.get("updated_at"), json.dumps(man.get("as_of"))))
        if not man.get("symbols"):
            r.warn("the factor book is empty - engines will run on their fallbacks")
        r.section("1. deployed versions + kicks")
        t_kick = int(time.time() * 1000)
        before = {}
        for fn in FUNCTIONS:
            before[fn] = stamp_of(jget(s3, out_key(fn)))
            try:
                cf = lam.get_function_configuration(FunctionName=fn)
                r.log("%s: modified %s, %s MB, %s s, desc %s" % (fn, (cf.get("LastModified") or "")[:19], cf.get("MemorySize"), cf.get("Timeout"), (cf.get("Description") or "")[:70]))
            except Exception as e:  # noqa: BLE001
                r.fail("%s: %s" % (fn, type(e).__name__))
                continue
            try:
                resp = lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
                r.log("   kick -> %s (output %s, previous stamp %s)" % (resp.get("StatusCode"), out_key(fn), before[fn]))
            except Exception as e:  # noqa: BLE001
                r.warn("   kick failed: %s" % type(e).__name__)
        r.section("2. outputs after the kicks")
        deadline = time.time() + WAIT_S
        pending = set(FUNCTIONS)
        fresh = {}
        while pending and time.time() < deadline:
            time.sleep(30)
            for fn in list(pending):
                doc = jget(s3, out_key(fn))
                st = stamp_of(doc)
                if st and st != before.get(fn):
                    fresh[fn] = doc
                    pending.discard(fn)
        verdict = {}
        for fn in FUNCTIONS:
            doc = fresh.get(fn) if fn in fresh else jget(s3, out_key(fn))
            r.log("== %s (%s) %s" % (fn, out_key(fn), "FRESH output" if fn in fresh else "output NOT refreshed within %d min - showing the previous one" % (WAIT_S // 60)))
            if isinstance(doc, dict):
                found = {}
                walk_labels(doc, found)
                r.log("   stamp %s | top keys %s" % (stamp_of(doc), sorted(doc.keys())[:18]))
                for k in LABELS:
                    if k in found:
                        r.log("   %s x%d: %s" % (k, found[k]["n"], " | ".join(found[k]["samples"])))
                if not any(k in found for k in ("universe_basis", "price_source", "source_used", "feed_basis", "bank_context", "squeeze", "book_as_of", "ret_60d_basis", "target_basis")):
                    r.warn("   no fusion label found in the output")
            else:
                r.warn("   no / non-dict output at %s" % out_key(fn))
            reports = logs_tail(logs, fn, t_kick - 60000, '"REPORT RequestId"', 10)
            for line in reports[-3:]:
                dur = mem = size = None
                for part in line.split("\t"):
                    p = part.strip()
                    if p.startswith("Duration:"):
                        dur = p.split()[1]
                    elif p.startswith("Max Memory Used:"):
                        mem = p.split()[3]
                    elif p.startswith("Memory Size:"):
                        size = p.split()[2]
                r.log("   REPORT dur %s ms | max mem %s / %s MB%s" % (dur, mem, size, " | status error" if "Status: error" in line else ""))
            errs = logs_tail(logs, fn, t_kick - 60000, '?Traceback ?"Task timed out" ?"Runtime.OutOfMemory" ?"[ERROR]" ?"factor book" ?"factor-book" ?"universe:" ?"bank/" ?"budget"')
            r.log("   logs: %s" % ([e[:240] for e in errs[:10]] if errs else "nothing matched (no error, no fusion line)"))
            bad = any(("Traceback" in e or "Task timed out" in e or "OutOfMemory" in e) for e in errs)
            verdict[fn] = "FAILED" if bad else ("fresh" if fn in fresh else "not refreshed")
            if bad:
                r.warn("   %s raised, timed out or ran out of memory - read the log group" % fn)
        r.section("3. verdict")
        for fn, v in verdict.items():
            (r.fail if v == "FAILED" else (r.log if v == "fresh" else r.warn))("%s: %s" % (fn, v))
        r.ok("ops 6663 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
