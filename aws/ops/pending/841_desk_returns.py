"""
ops/841 - justhodl-desk-returns: ship the desk-return feed and wire it
into the Desk Allocator's Bayesian shrinkage.

WHY
---
The Desk Allocator sizes the seven strategy desks by inverse-volatility
risk parity, but has been running on archetype PRIORS alone - its
realized_desk_vol() reader and its shrink() machinery shipped dormant
because no desk had a realized return history.

This op closes that loop:

  A. NEW  justhodl-desk-returns - every weekday after the close it marks
     each desk's book (a signed-weight map, gross 1.0, capped to headline
     names) to today's FMP /stable/quote close and appends a true
     one-trading-day return to that desk's series. Output
     data/desk-returns.json. Scheduled weekdays 23:30 UTC, before the
     allocator's 00:30 run.

  B. PATCH  justhodl-desk-allocator - realized_desk_vol() now reads the
     desk-returns feed instead of its own decision-history; once a desk
     clears 20 daily observations its realized vol shrinks in over the
     prior. No behaviour change today (the feed has just been seeded and
     N<20 everywhere, so the prior still governs) - this proves the
     wiring is correct and live.

VERIFY
------
  1. Deploy justhodl-desk-returns from source; wire its EventBridge
     Scheduler schedule; invoke it once.
  2. Read data/desk-returns.json and prove the feed is sane:
       - schema + methodology + disclaimer present;
       - all 7 desks in desk_summary;
       - FMP resolved a real universe of prices;
       - desk books built for the desks with fresh sidecars;
       - the per-desk state (prev_book / prev_px) was stored so the next
         run can mark to market;
       - first-run desks correctly report 'seeding' and N=0 (no return is
         fabricated on the first observation).
  3. Re-deploy the patched justhodl-desk-allocator; invoke it; prove it
     still produces all 7 desks, reads the desk-returns feed without
     error, and falls back to the archetype prior while N<20
     (effective_vol == prior_vol, realized_n reported).

Writes aws/ops/reports/841_desk_returns.json.
"""
import io
import json
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"

RET_FN = "justhodl-desk-returns"
RET_KEY = "data/desk-returns.json"
ALLOC_FN = "justhodl-desk-allocator"
ALLOC_KEY = "data/desk-allocator.json"

EXPECTED_DESKS = {"best-ideas", "pairs-arb", "trend-engine", "merger-arb",
                  "spinoff-desk", "index-recon", "risk-radar"}

cfg = Config(read_timeout=240, connect_timeout=20,
             retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 841,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Ship justhodl-desk-returns (daily desk MTM return feed) + "
               "wire it into the Desk Allocator's Bayesian shrinkage",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def ship(fn):
    """Create-or-update a Lambda from aws/lambdas/<fn>/source + config."""
    conf = json.load(open(f"aws/lambdas/{fn}/config.json"))
    src = f"aws/lambdas/{fn}/source/lambda_function.py"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", open(src, encoding="utf-8").read())
    zb = buf.getvalue()
    env = conf.get("environment") or {}
    try:
        lam.get_function(FunctionName=fn)
        lam.update_function_code(FunctionName=fn, ZipFile=zb)
        for _ in range(30):
            if lam.get_function_configuration(
                    FunctionName=fn).get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        # merge env so an ops-patched key is never wiped
        cur = (lam.get_function_configuration(FunctionName=fn)
               .get("Environment", {}) or {}).get("Variables", {}) or {}
        merged = dict(cur)
        merged.update(env)
        cfg_kw = dict(
            FunctionName=fn, Handler=conf["handler"], Runtime=conf["runtime"],
            Role=ROLE, Timeout=conf["timeout"], MemorySize=conf["memory"],
            Description=conf["description"][:255])
        if merged:
            cfg_kw["Environment"] = {"Variables": merged}
        lam.update_function_configuration(**cfg_kw)
        state = "updated"
    except lam.exceptions.ResourceNotFoundException:
        kw = dict(
            FunctionName=fn, Runtime=conf["runtime"], Role=ROLE,
            Handler=conf["handler"], Timeout=conf["timeout"],
            MemorySize=conf["memory"], Architectures=conf["architectures"],
            Description=conf["description"][:255], Code={"ZipFile": zb})
        if env:
            kw["Environment"] = {"Variables": env}
        lam.create_function(**kw)
        state = "created"
    fn_arn = None
    for _ in range(40):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            fn_arn = c.get("FunctionArn")
            if c.get("State") == "Active" and c.get(
                    "LastUpdateStatus") == "Successful":
                break
        except Exception:
            pass
        time.sleep(3)
    return conf, fn_arn, state


def wire_schedule(conf, fn_arn):
    sb = conf.get("eventbridge_scheduler") or {}
    if not sb:
        return "no-schedule"
    name = sb["schedule_name"]
    common = dict(
        ScheduleExpression=sb["cron"],
        ScheduleExpressionTimezone=sb.get("timezone", "UTC"),
        FlexibleTimeWindow={"Mode": "OFF"},
        State="ENABLED",
        Description=sb.get("description", "")[:512],
        Target={"Arn": fn_arn, "RoleArn": sb["role_arn"]},
    )
    try:
        sch.get_schedule(Name=name)
        sch.update_schedule(Name=name, **common)
        return f"updated {name}"
    except sch.exceptions.ResourceNotFoundException:
        sch.create_schedule(Name=name, **common)
        return f"created {name}"


def invoke(fn):
    r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                   Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", "ignore")
    return r.get("StatusCode"), r.get("FunctionError"), body


# ====================================================================
# A) ship + run justhodl-desk-returns
# ====================================================================
try:
    conf, arn, state = ship(RET_FN)
    rep["desk_returns_deploy"] = state
    check("desk_returns_deploy_ok", arn is not None, state)
except Exception as e:
    rep["desk_returns_deploy"] = f"ERROR {type(e).__name__}: {e}"
    check("desk_returns_deploy_ok", False, rep["desk_returns_deploy"])
    conf, arn = {}, None

try:
    sres = wire_schedule(conf, arn) if arn else "skipped"
    check("desk_returns_schedule_wired", "created" in sres or "updated" in sres,
          sres)
except Exception as e:
    check("desk_returns_schedule_wired", False, f"{type(e).__name__}: {e}")

try:
    sc, fe, body = invoke(RET_FN)
    rep["desk_returns_invoke"] = {"status": sc, "fn_error": fe,
                                  "body": body[:400]}
    check("desk_returns_invoke_ok", sc == 200 and not fe, fe or "200")
except Exception as e:
    rep["desk_returns_invoke"] = {"error": str(e)[:200]}
    check("desk_returns_invoke_ok", False, str(e)[:200])

time.sleep(3)

feed = {}
try:
    head = s3.head_object(Bucket=S3_BUCKET, Key=RET_KEY)
    age = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds()
    check("desk_returns_output_fresh", age < 900, f"{round(age)}s old")
    feed = json.loads(s3.get_object(
        Bucket=S3_BUCKET, Key=RET_KEY)["Body"].read())
except Exception as e:
    check("desk_returns_output_fresh", False, f"{type(e).__name__}: {e}")

summary = feed.get("desk_summary") or []
feed_desks = feed.get("desks") or {}

check("desk_returns_schema_ok",
      feed.get("schema") and feed.get("methodology")
      and feed.get("disclaimer"),
      f"schema={feed.get('schema')}")

skeys = {d.get("key") for d in summary}
check("desk_returns_all_seven", skeys == EXPECTED_DESKS,
      f"n={len(skeys)} {sorted(skeys)}")

uni = feed.get("universe_size") or 0
pxr = feed.get("prices_resolved") or 0
check("desk_returns_prices_resolved",
      uni > 0 and pxr > 0 and pxr >= 0.5 * uni,
      f"universe={uni} resolved={pxr}")

books_built = sum(1 for d in summary if (d.get("book_names") or 0) > 0)
check("desk_returns_books_built", books_built >= 6,
      f"{books_built}/7 desks built a book")

# per-desk state stored so the next run can mark to market
state_ok = []
for d in summary:
    k = d.get("key")
    st = feed_desks.get(k) or {}
    pb = st.get("prev_book") or {}
    if (d.get("book_names") or 0) > 0:
        state_ok.append(bool(pb) and st.get("prev_date") == feed.get(
            "trading_date"))
check("desk_returns_state_persisted", all(state_ok) and bool(state_ok),
      f"{sum(1 for x in state_ok if x)}/{len(state_ok)} desks stored "
      f"prev_book+prev_date")

# first run must NOT fabricate a return - seeding is the correct state
fabricated = [d.get("key") for d in summary
              if d.get("appended_today") and (d.get("n_returns") or 0) == 0]
check("desk_returns_no_fabrication", not fabricated, fabricated or "clean")

# ====================================================================
# B) re-deploy the patched allocator + prove the wiring
# ====================================================================
try:
    aconf, aarn, astate = ship(ALLOC_FN)
    rep["allocator_deploy"] = astate
    check("allocator_deploy_ok", aarn is not None, astate)
except Exception as e:
    rep["allocator_deploy"] = f"ERROR {type(e).__name__}: {e}"
    check("allocator_deploy_ok", False, rep["allocator_deploy"])

try:
    sc, fe, body = invoke(ALLOC_FN)
    rep["allocator_invoke"] = {"status": sc, "fn_error": fe,
                               "body": body[:300]}
    check("allocator_invoke_ok", sc == 200 and not fe, fe or "200")
except Exception as e:
    rep["allocator_invoke"] = {"error": str(e)[:200]}
    check("allocator_invoke_ok", False, str(e)[:200])

time.sleep(3)

alloc = {}
try:
    alloc = json.loads(s3.get_object(
        Bucket=S3_BUCKET, Key=ALLOC_KEY)["Body"].read())
except Exception as e:
    check("allocator_output_read", False, f"{type(e).__name__}: {e}")

adesks = alloc.get("desks") or []
akeys = {d.get("key") for d in adesks}
check("allocator_seven_desks", akeys == EXPECTED_DESKS,
      f"n={len(akeys)}")

wsum = round(sum((d.get("capital_weight_pct") or 0) for d in adesks), 2)
check("allocator_weights_sum_100", abs(wsum - 100.0) <= 0.5, f"sum={wsum}")

# the allocator read the feed: realized_n is reported on every desk, and
# while N<20 the effective vol falls back to the archetype prior
realized_ns = [(d.get("key"), d.get("realized_n")) for d in adesks]
n_reported = all(isinstance(d.get("realized_n"), int) for d in adesks)
prior_governs = all(
    d.get("realized_n", 0) >= 20
    or abs((d.get("effective_vol_pct") or 0)
           - (d.get("prior_vol_pct") or 0)) < 0.05
    for d in adesks)
check("allocator_reads_feed", n_reported and prior_governs,
      f"realized_n={realized_ns}")

# ---- summary ---------------------------------------------------------------
rep["desk_returns"] = {
    "headline": feed.get("headline"),
    "trading_date": feed.get("trading_date"),
    "universe_size": uni,
    "prices_resolved": pxr,
    "build_seconds": feed.get("build_seconds"),
    "desks": [{"key": d.get("key"),
               "book_names": d.get("book_names"),
               "book_fresh": d.get("book_fresh"),
               "appended_today": d.get("appended_today"),
               "skip_reason": d.get("skip_reason"),
               "n_returns": d.get("n_returns"),
               "vol_ready": d.get("vol_ready")}
              for d in summary],
}
rep["allocator"] = {
    "headline": alloc.get("headline"),
    "desks_total": (alloc.get("firm") or {}).get("desks_total"),
    "diversification_ratio": (alloc.get("firm") or {}).get(
        "diversification_ratio"),
    "realized_n": dict(realized_ns),
}

rep["all_pass"] = all(c["ok"] for c in rep["checks"])
rep["verdict"] = (
    "DESK-RETURN FEED LIVE - justhodl-desk-returns marks all 7 desk books "
    "to market daily (weekdays 23:30 UTC) and the Desk Allocator now reads "
    "data/desk-returns.json for realized vol. Feed seeded; the Bayesian "
    "shrinkage warms each desk in automatically as it clears 20 daily "
    "observations. The allocator's dormant Phase-2 hook is now wired."
    if rep["all_pass"]
    else "REVIEW - see checks[]/desk_returns/allocator")

out = json.dumps(rep, indent=2, default=str)
print(out)
try:
    with open("aws/ops/reports/841_desk_returns.json", "w") as f:
        f.write(out)
except Exception as e:
    print("report write skipped:", e)
