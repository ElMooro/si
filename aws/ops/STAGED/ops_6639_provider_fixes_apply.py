"""ops 6639 - provider-coverage fixes, AWS side (Khalid 2026-10-23: "fix it all, 100% of the data pulled and used").

Repo side (same push): earnings-tracker no longer calls the not-entitled Polygon /vX/reference/financials (FMP
bank/live instead; financial-secretary's twin is dormant - no call site - and byte-pinned, left alone); freight-pulse +
eia-energy-agent inherit EIA_API_KEY from justhodl-real-economy-collector (= SSM /justhodl/eia/api-key; their source is
AST-pinned so the key arrives by config, not code); cryptoquant reads /justhodl/cryptoquant_api first; lobbying-intel sends a Quiver bearer the day a
key lands in SSM; symbology-master keeps shareClassFIGI/compositeFIGI/securityType/exchCode/micCode; justhodl-sec-bulk
restored weekly in config/schedule-manifest.json + retired-engines.json.

This op (direct lane, idempotent):
  1. deployed schedule manifest := committed, allowing exactly ONE state change (justhodl-sec-bulk-weekly DISABLED ->
     ENABLED) and applying it live with scheduler.update_schedule; everything else must be equal (ops 6599 pattern).
  2. after the redeploys: invoke freight-pulse, eia-energy-agent, cryptoquant, symbology-master once (async) and read
     back their outputs for the leg that was broken (EIA leg present, CQ n_core_live, FIGI fields on AAPL).
  3. read-only entitlement probe (key from SSM, never printed): Polygon /v2/last/trade/AAPL and /v3/quotes/AAPL -
     records 200/403 so the audit's "called but maybe not entitled" becomes a fact.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import re
import sys
import time
import urllib.error
import urllib.request

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
KEY = "config/schedule-manifest.json"
ALLOWED_CHANGE = {("scheduler", "justhodl-sec-bulk-weekly"): ("DISABLED", "ENABLED")}
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else arn


def entries(m):
    return {(e["kind"], e["name"]): e for sec in ("rules", "schedules") for e in m.get(sec, [])}


def jget(s3, key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return default


def probe(url, headers=None):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl ops 6639", **(headers or {})})
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read(2000)
            return r.status, body[:160].decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, (e.read(300).decode("utf-8", "replace") if hasattr(e, "read") else "")
    except Exception as e:  # noqa: BLE001
        return None, type(e).__name__


def main():
    s3, lam, sch, ssm = c("s3"), c("lambda"), c("scheduler"), c("ssm")
    with report("ops_6639_provider_fixes_apply") as r:
        r.heading("ops 6639 - provider fixes: sec-bulk weekly live, key-fix engines re-run, Polygon entitlement probe")

        r.section("1. deployed schedule manifest := committed (one allowed state change)")
        repo_text = (ROOT / KEY).read_text(encoding="utf-8")
        repo = json.loads(repo_text)
        old_text = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8")
        a, b = entries(json.loads(old_text)), entries(repo)
        removed = sorted(k for k in a if k not in b)
        added = sorted(k for k in b if k not in a)
        changed = sorted(k for k in a if k in b and {x: a[k][x] for x in ("expr", "state")} != {x: b[k][x] for x in ("expr", "state")})
        r.log("S3 %d entries, repo %d; added %s removed %d changed %s" % (len(a), len(b), added, len(removed), changed))
        if removed:
            r.fail("manifest removes entries: %s" % removed[:5])
            raise SystemExit(1)
        for k in changed:
            if k not in ALLOWED_CHANGE or (a[k]["state"], b[k]["state"]) != ALLOWED_CHANGE[k] or a[k]["expr"] != b[k]["expr"]:
                r.fail("unexpected change %s: %s -> %s" % (k, a[k].get("state"), b[k].get("state")))
                raise SystemExit(1)
        for k in added:
            if k[0] != "scheduler":
                r.fail("unexpected added %s" % (k,))
                raise SystemExit(1)
            e = b[k]
            try:
                lv = sch.get_schedule(Name=k[1], GroupName=e.get("group") or "default")
            except Exception as ex:  # noqa: BLE001
                r.fail("added %s not live yet (%s) - deploy-lambdas creates it; run again" % (k[1], type(ex).__name__))
                raise SystemExit(1)
            if not (lv.get("ScheduleExpression") == e["expr"] and lv.get("State") == e["state"] and fn_of((lv.get("Target") or {}).get("Arn")) == fn_of(e["targets"][0]["arn"])):
                r.fail("declared %s does not match live" % k[1])
                raise SystemExit(1)
        for k in changed:
            e = b[k]
            lv = sch.get_schedule(Name=k[1], GroupName=e.get("group") or "default")
            if lv.get("State") != e["state"]:
                kw = {x: lv[x] for x in ("Name", "GroupName", "ScheduleExpression", "ScheduleExpressionTimezone", "FlexibleTimeWindow", "Target", "Description", "StartDate", "EndDate", "KmsKeyArn", "ActionAfterCompletion") if x in lv and lv[x] is not None}
                kw["State"] = e["state"]
                sch.update_schedule(**kw)
                lv = sch.get_schedule(Name=k[1], GroupName=e.get("group") or "default")
                r.ok("%s: %s -> %s live (%s)" % (k[1], a[k]["state"], lv.get("State"), lv.get("ScheduleExpression")))
            else:
                r.log("%s already %s live" % (k[1], lv.get("State")))
        if added or changed:
            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            s3.put_object(Bucket=PRIVATE, Key="backups/schedule-manifest/%s/schedule-manifest.json" % stamp, Body=old_text.encode("utf-8"), ServerSideEncryption="AES256")
            s3.put_object(Bucket=LIVE, Key=KEY, Body=repo_text.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
            r.ok("manifest uploaded (added %d, changed %d), backup backups/schedule-manifest/%s/" % (len(added), len(changed), stamp))
        else:
            r.ok("manifest already equal")

        r.section("2. the key-fix engines: redeployed? then one run each, read back the fixed leg")
        push_cut = datetime(2026, 10, 23, 23, 30, tzinfo=timezone.utc)
        targets = {
            "justhodl-freight-pulse": ("data/freight-pulse.json", lambda d: json.dumps(d)[:0] or ("eia" in json.dumps(d).lower() and "no EIA key" not in json.dumps(d))),
            "eia-energy-agent": ("data/eia-energy.json", lambda d: "needs a key" not in json.dumps(d).lower() and "api_key" not in json.dumps(d).lower()),
            "justhodl-cryptoquant": ("data/cryptoquant-onchain.json", lambda d: int(d.get("n_metrics") or d.get("n_core_live") or 0) >= 40),
            "justhodl-symbology-master": ("data/symbology/master.json", lambda d: any(k in json.dumps((d.get("tickers") or d.get("rows") or {}).get("AAPL") or {}) for k in ("share_class_figi",))),
        }
        before = {}
        for fn, (key, _ok) in targets.items():
            try:
                cf = lam.get_function_configuration(FunctionName=fn)
                mod = datetime.fromisoformat(cf["LastModified"].replace("+0000", "+00:00"))
                fresh = mod >= push_cut
                r.log("%s modified %s (%s)" % (fn, cf["LastModified"][:19], "redeployed" if fresh else "NOT yet redeployed - deploy-lambdas still working"))
                if not fresh:
                    continue
                h = None
                try:
                    h = s3.head_object(Bucket=LIVE, Key=key)["LastModified"]
                except Exception:  # noqa: BLE001
                    pass
                before[fn] = h
                lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
            except Exception as ex:  # noqa: BLE001
                r.warn("%s: %s" % (fn, type(ex).__name__))
        if before:
            deadline = time.time() + 14 * 60
            pending = set(before)
            while pending and time.time() < deadline:
                time.sleep(30)
                for fn in list(pending):
                    key, ok = targets[fn]
                    try:
                        h = s3.head_object(Bucket=LIVE, Key=key)["LastModified"]
                    except Exception:  # noqa: BLE001
                        continue
                    if h != before[fn]:
                        pending.discard(fn)
                        d = jget(s3, key, {}) or {}
                        try:
                            verdict = bool(ok(d))
                        except Exception:  # noqa: BLE001
                            verdict = False
                        (r.ok if verdict else r.warn)("%s rewrote %s at %s - fixed leg %s" % (fn, key, h.strftime("%H:%M:%S"), "PRESENT" if verdict else "not visible yet (read the file)"))
            for fn in pending:
                r.warn("%s: output not rewritten within 14 min" % fn)

        r.section("3. Polygon entitlement probe (read-only; key never printed)")
        try:
            pk = ssm.get_parameter(Name="/justhodl/polygon/api-key", WithDecryption=True)["Parameter"]["Value"]
        except Exception as ex:  # noqa: BLE001
            pk = ""
            r.warn("polygon key unreadable: %s" % type(ex).__name__)
        if pk:
            for label, url in (("last/trade", "https://api.polygon.io/v2/last/trade/AAPL?apiKey=" + pk),
                               ("quotes", "https://api.polygon.io/v3/quotes/AAPL?limit=1&apiKey=" + pk),
                               ("financials (must be refused)", "https://api.polygon.io/vX/reference/financials?ticker=AAPL&limit=1&apiKey=" + pk),
                               ("benzinga earnings", "https://api.polygon.io/benzinga/v1/earnings?limit=1&apiKey=" + pk),
                               ("short-interest", "https://api.polygon.io/stocks/v1/short-interest?ticker=AAPL&limit=1&apiKey=" + pk)):
                code, snip = probe(url)
                r.kv(endpoint=label, http=code, body=snip.replace(pk, "<key>")[:120])
        r.ok("ops 6639 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
