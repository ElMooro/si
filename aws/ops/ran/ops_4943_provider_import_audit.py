"""ops 4943 -- fleet-wide provider import audit.

Khalid (2026-08-23): "audit and check if all the data is imported from
all the different data providers, if any stalled data (many providers
are showing low mb which tell me that they havent imported all their
data)".

Low MB is NOT evidence of a stall: JSON timeseries are tiny (NY Fed
Markets banks 1,539 full-depth series in 13 MB). A stall is one of:
an engine that stopped mid-queue, a schedule that no longer fires, a
source that denies at origin, or a prefix nothing refreshes. This op
measures those DIRECTLY, per provider, from live state:

  1. data/provider-catalog.json      keys / MB / freshest / target
  2. walker state files              fred manifest+queue, sdmx-walk-*,
                                     bea-walk, te-mirror, nyfed pd-state
  3. data/import-health.json         sentinel classification + incidents
  4. EventBridge Scheduler + rules   is a live schedule backing each
                                     provider's engines?
  5. CloudWatch Invocations (24h)    for every provider flagged stale:
                                     did its engines actually run?

Verdicts: COMPLETE / RUNNING / PARTIAL_SOURCE_DENIED /
SCOPED_BY_DESIGN / STALE_VS_CADENCE / ORPHANED_NO_ENGINE / BLOCKED /
CHECK. Observational op: findings are the deliverable; RED only if the
audit itself cannot read the hub.
"""
import gzip
import io
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

REGION, B = "us-east-1", "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
ev = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)

# expected max freshness (hours) before a provider is even a stale
# CANDIDATE. Slow publishers get honest windows -- SEC MIDAS posts on
# a lagged weekly/monthly cycle, DERA monthly, NY Fed research weekly.
CADENCE_H = {"sec-midas": 240, "sec-dera": 1100, "nyfed-research": 400,
             "sec-bulk": 48, "gleif": 48, "ofr-bsrm": 48,
             "ofr-site": 48, "ofr-fsi": 96, "te-country": 72,
             "taiwan-moea": 240, "peru-copper": 240, "eiopa": 48}
DEFAULT_CADENCE_H = 36


def gj(key, default=None):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if key.endswith(".gz"):
            raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
        return json.loads(raw)
    except Exception:
        return default


def reg_engines():
    """Parse the provider-catalog REG literal for slug -> engines[].

    Read from the checkout (never exec'd -- the module builds boto3
    clients at import). Keeps the audit self-updating as REG grows.
    """
    try:
        src = (ROOT / "aws" / "lambdas" / "justhodl-provider-catalog"
               / "source" / "lambda_function.py").read_text()
    except Exception:
        return {}
    out = {}
    for m in re.finditer(r'"([a-z0-9-]+)":\s*\{"name":.*?"engines":'
                         r'\s*\[(.*?)\]', src, re.S):
        out[m.group(1)] = re.findall(r'"([^"]+)"', m.group(2))
    return out


def all_schedules():
    names, tok = [], None
    while True:
        kw = {"MaxResults": 100}
        if tok:
            kw["NextToken"] = tok
        r = sch.list_schedules(**kw)
        names += [(x["Name"], x.get("State")) for x in r["Schedules"]]
        tok = r.get("NextToken")
        if not tok:
            break
    tok = None
    while True:
        kw = {"Limit": 100}
        if tok:
            kw["NextToken"] = tok
        r = ev.list_rules(**kw)
        names += [(x["Name"], x.get("State")) for x in r["Rules"]]
        tok = r.get("NextToken")
        if not tok:
            break
    return names


def inv24(fn):
    try:
        now = datetime.now(timezone.utc)
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=now - timedelta(hours=24), EndTime=now,
            Period=86400, Statistics=["Sum"])
        pts = r.get("Datapoints") or []
        return int(sum(p["Sum"] for p in pts)) if pts else 0
    except Exception:
        return None


with report("ops_4943_provider_import_audit") as R:
    hub = gj("data/provider-catalog.json")
    if not hub or not hub.get("providers"):
        R.log("RED: cannot read data/provider-catalog.json")
        sys.exit(1)
    health = gj("data/import-health.json") or {}
    hmap = {p.get("name"): p for p in (health.get("pipelines") or [])}
    engines = reg_engines()
    scheds = all_schedules()
    sched_txt = " ".join(n for n, _ in scheds)
    R.kv(providers=len(hub["providers"]), schedules=len(scheds),
         hub_as_of=hub.get("as_of"))

    # walker ground truth ------------------------------------------------
    R.section("walker states (queue truth)")
    fredm = gj("data/providers/fred-scoped/manifest.json") or {}
    R.log("fred: status=%s scope=%s cats=%s/%s queue=%s/%s" % (
        fredm.get("status"), fredm.get("scope"),
        fredm.get("categories_done"), fredm.get("categories_total"),
        fredm.get("queue_cursor"), fredm.get("queue_total")))
    sdmx_state = {}
    for w in ("eurostat", "oecd", "statcan", "bis", "ecb"):
        st = gj("data/_state/sdmx-walk-%s.json" % w) or {}
        sdmx_state[w] = st
        R.log("sdmx-%s: done=%s n_total=%s failures=%s updated=%s" % (
            w, st.get("n_done") or st.get("done"), st.get("n_total"),
            len(st.get("failures") or {}), st.get("updated_at")))
    for label, key, fields in (
            ("bea-walk", "data/_state/bea-walk.json",
             ("tables", "cursor", "updated_at")),
            ("te-mirror", "data/warm/te-mirror/_state.json",
             ("catalog", "updated_at")),
            ("nyfed-pd", "data/warm/nyfed-markets/pd-state.json",
             ("catalog", "updated_at")),
            ("ofr", "data/warm/ofr/state.json",
             ("catalog", "updated_at"))):
        st = gj(key) or {}
        vals = []
        for f in fields:
            v = st.get(f)
            vals.append("%s=%s" % (f, (len(v) if isinstance(
                v, (list, dict)) else v)))
        R.log("%s: %s" % (label, " ".join(vals) if st else "ABSENT"))

    # per-provider verdicts ---------------------------------------------
    R.section("per-provider verdicts")
    findings, table = [], []
    for p in hub["providers"]:
        slug = p.get("slug")
        fh = p.get("freshest_h")
        cov = p.get("coverage_pct")
        tgt = p.get("datasets_target")
        den = p.get("denied_source_side")
        note = (p.get("catalog_note") or "")
        engs = engines.get(slug) or []
        has_sched = any(e in sched_txt for e in engs) if engs else None
        cadence = CADENCE_H.get(slug, DEFAULT_CADENCE_H)
        hp = hmap.get(slug) or hmap.get("sdmx-" + str(slug)) or {}
        verdict, why = "OK", ""
        if slug == "fred":
            done = (fredm.get("status") == "COMPLETE_WITH_LEAKS" or
                    str(fredm.get("queue_cursor")) ==
                    str(fredm.get("queue_total")))
            verdict = "COMPLETE" if done else "RUNNING"
            why = "queue %s/%s scope=%s" % (fredm.get("queue_cursor"),
                                            fredm.get("queue_total"),
                                            fredm.get("scope"))
        elif slug in sdmx_state:
            st = sdmx_state[slug]
            nt, ndone = st.get("n_total"), (st.get("n_done") or
                                            st.get("done"))
            nf = len(st.get("failures") or {})
            if den or nf:
                verdict = "PARTIAL_SOURCE_DENIED"
                why = "%s denied/failed at source of %s" % (den or nf,
                                                            nt)
            elif cov is not None and cov >= 99.5:
                verdict, why = "COMPLETE", "%.1f%% of %s" % (cov, nt)
            else:
                verdict, why = "RUNNING", "cov=%s of %s" % (cov, nt)
        elif slug == "nyfed-research":
            verdict = "ORPHANED_NO_ENGINE"
            why = "seeded 4793-94, no refresh engine (4913 queued map)"
        elif tgt is None and cov is None:
            # scoped engines pulling fixed lists -- small by DESIGN.
            if fh is not None and fh <= cadence:
                verdict = "SCOPED_BY_DESIGN"
                why = "fixed-list engine, fresh %.1fh" % fh
            else:
                verdict = "CHECK"
                why = "fresh %sh > cadence %sh" % (fh, cadence)
        if fh is not None and fh > cadence and verdict in ("OK",
                                                           "COMPLETE",
                                                           "SCOPED_BY_DESIGN"):
            verdict, why = "STALE_VS_CADENCE", "fresh %.1fh > %dh" % (
                fh, cadence)
        if str(hp.get("status")) in ("BLOCKED", "BLOCKED_403",
                                     "KEY_INVALID", "ACTION_REQUIRED"):
            verdict = "BLOCKED"
            why = "sentinel: %s %s" % (hp.get("status"),
                                       (hp.get("detail") or "")[:60])
        row = "%-16s %7s keys %9.2f MB  fresh %6sh  %-22s %s" % (
            slug, p.get("n_keys"), p.get("total_mb") or 0,
            ("%.1f" % fh) if fh is not None else "?",
            verdict, why[:70])
        table.append(row)
        if verdict in ("STALE_VS_CADENCE", "ORPHANED_NO_ENGINE",
                       "BLOCKED", "CHECK"):
            findings.append((slug, verdict, why, engs, has_sched))
    for row in table:
        R.log(row)

    # deep-verify every flagged provider: schedule + 24h invocations ----
    R.section("flagged: schedule + invocation liveness")
    if not findings:
        R.log("none flagged -- fleet importing on cadence")
    for slug, verdict, why, engs, has_sched in findings:
        inv = {e: inv24(e) for e in engs[:4]}
        R.log("%s [%s] %s | engines=%s sched_named=%s inv24h=%s" % (
            slug, verdict, why, engs or "none-registered", has_sched,
            inv or "n/a"))

    R.section("incidents (sentinel, last sweep)")
    for i in (health.get("incidents") or [])[:8]:
        R.log(str(i)[:160])
    R.kv(flagged=len(findings), verdict_table_rows=len(table))
    R.log("ops 4943 GREEN -- audit complete; findings above are the "
          "deliverable")
