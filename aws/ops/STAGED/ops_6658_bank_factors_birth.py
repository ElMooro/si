"""ops 6658 - justhodl-bank-factors birth: declare, first join, data.html card, read back (Khalid 2026-10-25: "fuse the new
data in all the engines that need them to improve and enrichen those engines").

After deploy-lambdas ships justhodl-bank-factors v1.0.0 (its config creates the Scheduler schedule justhodl-bank-factors-
daily, 02:50 UTC):
  1. function + schedule exist;
  2. deployed schedule manifest := committed (additions-only, live-verified);
  3. first join now (sync invoke is not possible at 900 s from the runner's 60-min lane? it is - we invoke async and poll
     the state): the whole-market factor book is written under data/warm/factor-book/ (symbols/<S>.json.gz, matrix/core,
     universe, manifest); the manifest's coverage per source and schema_found (which bank column answered each field)
     are printed - a None there is a header the book did not recognise and must be added, never guessed;
  4. data.html card: data/providers/extra-providers.json gains factor-book (count_from the manifest), catalog refreshed;
  5. read back: AAPL / a small cap / an ETF document, and the matrix's first screen (Altman Z < 1.8, mcap > $300M).
Direct lane (STAGED): idempotent, no serial twin; lease-guarded kick.  No key involved (bank-only engine).
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import re
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
KEY = "config/schedule-manifest.json"
FN, SCHED = "justhodl-bank-factors", "justhodl-bank-factors-daily"
BOOK = "data/warm/factor-book/"
MAN, STATE = BOOK + "manifest.json", BOOK + "_state/state.json"
EXTRA = "data/providers/extra-providers.json"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=300)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else arn


def entries(m):
    return {(e["kind"], e["name"]): e for sec in ("rules", "schedules") for e in m.get(sec, [])}


def jget(s3, key, default=None):
    try:
        raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
        return json.loads(gzip.decompress(raw) if key.endswith(".gz") else raw)
    except Exception:  # noqa: BLE001
        return default


def main():
    s3, lam, sch = c("s3"), c("lambda"), c("scheduler")
    with report("ops_6658_bank_factors_birth") as r:
        r.heading("ops 6658 - justhodl-bank-factors: declare, first join, data.html card, read back")
        r.section("1. function + schedule")
        try:
            cf = lam.get_function_configuration(FunctionName=FN)
        except Exception as e:  # noqa: BLE001
            r.fail("%s not deployed yet (%s) - dispatch deploy-lambdas function=%s, then run again" % (FN, type(e).__name__, FN))
            raise SystemExit(1)
        r.log("%s: runtime %s, memory %s, timeout %s, state %s, modified %s" % (FN, cf.get("Runtime"), cf.get("MemorySize"), cf.get("Timeout"), cf.get("State"), (cf.get("LastModified") or "")[:19]))
        try:
            live = sch.get_schedule(Name=SCHED, GroupName="default")
            r.log("schedule %s: %s %s tz %s target %s" % (SCHED, live.get("ScheduleExpression"), live.get("State"), live.get("ScheduleExpressionTimezone"), fn_of((live.get("Target") or {}).get("Arn"))))
        except Exception as e:  # noqa: BLE001
            r.fail("schedule %s missing (%s)" % (SCHED, type(e).__name__))
            raise SystemExit(1)

        r.section("2. deployed schedule manifest := committed (additions-only)")
        repo_text = (ROOT / KEY).read_text(encoding="utf-8")
        repo = json.loads(repo_text)
        old_text = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8")
        a, b = entries(json.loads(old_text)), entries(repo)
        removed = sorted(k for k in a if k not in b)
        added = sorted(k for k in b if k not in a)
        changed = sorted(k for k in a if k in b and {x: a[k][x] for x in ("expr", "state")} != {x: b[k][x] for x in ("expr", "state")})
        r.log("S3 %d entries, repo %d; added %s removed %d changed %d" % (len(a), len(b), added, len(removed), len(changed)))
        if removed or changed:
            r.fail("not additions-only: removed %s changed %s" % (removed[:5], changed[:5]))
            raise SystemExit(1)
        for kind, name in added:
            e = b[(kind, name)]
            if kind != "scheduler":
                r.fail("unexpected added entry %s %s" % (kind, name))
                raise SystemExit(1)
            lv = sch.get_schedule(Name=name, GroupName=e.get("group") or "default")
            if not (lv.get("ScheduleExpression") == e["expr"] and lv.get("State") == e["state"] and fn_of((lv.get("Target") or {}).get("Arn")) == fn_of(e["targets"][0]["arn"])):
                r.fail("declared %s does not match live" % name)
                raise SystemExit(1)
        if added:
            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            s3.put_object(Bucket=PRIVATE, Key="backups/schedule-manifest/%s/schedule-manifest.json" % stamp, Body=old_text.encode("utf-8"), ServerSideEncryption="AES256")
            s3.put_object(Bucket=LIVE, Key=KEY, Body=repo_text.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
            r.ok("manifest uploaded (+%d), backup backups/schedule-manifest/%s/" % (len(added), stamp))
        else:
            r.ok("manifest already equal")

        r.section("3. first join (async invoke, poll the state)")
        st = jget(s3, STATE) or {}
        prev = st.get("updated_at")
        if float(st.get("lease_until") or 0) > time.time():
            r.log("lease held until %s - a run is in progress; waiting for it" % datetime.fromtimestamp(st["lease_until"], timezone.utc).strftime("%H:%M:%S"))
        else:
            resp = lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
            r.log("async invoke %s at %s" % (resp.get("StatusCode"), datetime.now(timezone.utc).strftime("%H:%M:%S")))
        t_end = time.time() + 17 * 60
        st = None
        while time.time() < t_end:
            time.sleep(20)
            cur = jget(s3, STATE) or {}
            if cur.get("last_run") and cur.get("updated_at") != prev and not float(cur.get("lease_until") or 0) > time.time():
                st = cur
                break
        if not st:
            r.warn("no finished state within 17 min - read CloudWatch /aws/lambda/%s" % FN)
        else:
            lr = st.get("last_run") or {}
            r.kv(step="first join", seconds=lr.get("seconds"), symbols=lr.get("symbols"), written=lr.get("written"), unchanged=lr.get("unchanged"), failed=lr.get("failed"),
                 mb=round((lr.get("bytes") or 0) / 1e6, 1), notes=json.dumps(lr.get("notes"))[:600])
        man = jget(s3, MAN) or {}
        r.log("manifest: symbols %s universe %s common %s etfs %s otc %s" % (man.get("symbols"), man.get("universe"), man.get("common_stocks"), man.get("etfs"), man.get("otc")))
        r.log("as_of: %s" % json.dumps(man.get("as_of")))
        r.log("coverage (symbols with the block): %s" % json.dumps(man.get("coverage")))
        r.log("sources: %s" % json.dumps(man.get("sources"))[:1200])
        sf = man.get("schema_found") or {}
        unresolved = sorted(k for k, v in sf.items() if v is None)
        r.log("schema_found: %d fields resolved, unresolved: %s" % (sum(1 for v in sf.values() if v), unresolved))
        r.log("schema_found detail: %s" % json.dumps(sf)[:1500])
        if man.get("notes"):
            r.log("notes: %s" % json.dumps(man.get("notes"))[:800])
        if not man.get("symbols"):
            r.fail("no factor book written")
            raise SystemExit(1)

        r.section("4. data.html card (extra-providers.json, add-only) + catalog refresh")
        extra = jget(s3, EXTRA, {}) or {}
        if isinstance(extra, list):
            extra = {p.get("slug", p.get("id")): p for p in extra if isinstance(p, dict)}
        if "factor-book" not in extra:
            extra["factor-book"] = {
                "name": "Factor book — every US symbol's fundamentals, scores, consensus, short interest, float and returns, joined from the banks",
                "api": "none (bank-only: fmp-bank + fmp-walker + polygon-bank + polygon-full)",
                "engines": ["justhodl-bank-factors"], "prefixes": [BOOK],
                "count_from": [MAN, "pages", "pages_bytes"], "hot": [MAN, BOOK + "matrix/core.json.gz"],
                "cadence": "daily 02:50 UTC",
                "label": "One document per US-listed symbol (data/warm/factor-book/symbols/<S>.json.gz) and one whole-market matrix: Altman/Piotroski, rating, DCF, price targets, consensus, TTM metrics/ratios, growth, surprises, float, short interest, ETF holders, peers, returns, percentile ranks and composites — read by engines through aws/shared/bank_enrich"}
            s3.put_object(Bucket=LIVE, Key=EXTRA, Body=json.dumps(extra, indent=1).encode(), ContentType="application/json", CacheControl="no-cache")
            r.ok("extra-providers.json += factor-book (%d entries)" % len(extra))
        else:
            r.ok("extra-providers.json already carries factor-book")
        try:
            lam.invoke(FunctionName="justhodl-provider-catalog", InvocationType="Event", Payload=b"{}")
            r.log("justhodl-provider-catalog invoked async (card lands within ~10 min; daily 06:20 otherwise)")
        except Exception as e:  # noqa: BLE001
            r.warn("provider-catalog invoke: %s" % type(e).__name__)

        r.section("5. read back")
        for sym in ("AAPL", "NVDA", "GME", "SPY"):
            d = jget(s3, BOOK + "symbols/%s.json.gz" % sym) or {}
            if not d:
                r.warn("%s: no document" % sym)
                continue
            r.log("%s: sector %s mcap %s | altman %s piotroski %s rating %s | dcf_upside %s pt_upside %s consensus %s | pe %s ev/ebitda %s roic %s | growth rev %s | beat4q %s | float %s SI%%float %s dtc %s | ret_21d %s from_52w_high %s | composites %s" % (
                sym, (d.get("identity") or {}).get("sector"), (d.get("identity") or {}).get("mcap"),
                (d.get("scores") or {}).get("altmanZScore"), (d.get("scores") or {}).get("piotroskiScore"), (d.get("rating") or {}).get("rating"),
                (d.get("derived") or {}).get("dcf_upside_pct"), (d.get("derived") or {}).get("pt_upside_pct"), (d.get("derived") or {}).get("consensus_score"),
                (d.get("ratios_ttm") or {}).get("priceToEarningsRatioTTM"), (d.get("key_metrics_ttm") or {}).get("evToEBITDATTM"), (d.get("key_metrics_ttm") or {}).get("returnOnInvestedCapitalTTM"),
                (d.get("growth") or {}).get("growthRevenue"), (d.get("derived") or {}).get("beat_rate_4q"), (d.get("float") or {}).get("floatShares"),
                (d.get("derived") or {}).get("short_pct_float"), (d.get("short") or {}).get("days_to_cover"), (d.get("returns") or {}).get("ret_21d"), (d.get("returns") or {}).get("from_52w_high_pct"),
                json.dumps(d.get("composites"))))
            r.log("%s blocks present: %s" % (sym, [k for k in ("profile", "scores", "rating", "dcf", "price_target", "consensus", "key_metrics_ttm", "ratios_ttm", "growth", "cf_growth", "surprises", "float", "short", "short_volume", "etf_holders", "peers", "returns") if d.get(k)]))
        m = jget(s3, BOOK + "matrix/core.json.gz") or {}
        cols = m.get("cols") or {}
        if cols:
            n = m.get("n")
            z, mc, sy = cols.get("altman_z") or [], cols.get("mcap") or [], m.get("symbols") or []
            hits = [(sy[i], z[i], mc[i]) for i in range(n) if z[i] is not None and z[i] < 1.8 and (mc[i] or 0) > 3e8 and not (cols.get("is_etf") or [None] * n)[i] and not (cols.get("is_otc") or [None] * n)[i]]
            r.log("matrix: n %s, fields %d; screen Altman Z < 1.8 & mcap > $300M -> %d names, e.g. %s" % (n, len(cols), len(hits), hits[:12]))
            filled = {k: sum(1 for v in cols[k] if v is not None) for k in ("altman_z", "pe", "ev_ebitda", "pt_upside_pct", "consensus_score", "revenue_growth", "beat_rate_4q", "float_shares", "short_pct_float", "ret_21d", "composite_value") if k in cols}
            r.log("matrix fill (non-null of %s): %s" % (n, json.dumps(filled)))
        r.ok("ops 6658 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
