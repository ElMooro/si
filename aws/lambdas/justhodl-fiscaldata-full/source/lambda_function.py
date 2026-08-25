"""justhodl-fiscaldata-full v1.0.0 -- COMPLETE US Treasury FiscalData.

Khalid priority lane #2 (4962 evidence: auctions since 1979-11-15,
7 flagships alone = 170,736 rows; the board's 49 curated keys were a
sliver). Warehouse doctrine:

  DISCOVER  seed endpoints (4962-proven + official paths) UNION a
            live harvest of fiscaldata.treasury.gov/api-documentation
            (regex /v{1,2}/... paths); EVERY candidate is validated
            by a 1-row probe (200 + meta.total-count) before joining
            the universe -- invalid paths are named, never guessed in
  DRAIN     per endpoint: full pagination page[size]=10000, rows
            spooled to /tmp as JSONL then gz-uploaded (RAM-safe for
            100k+ row tables); one artifact per endpoint under
            data/warm/fiscaldata-full/src/{slug}.jsonl.gz
  REFRESH   on COMPLETE each run: 1-row delta probe per endpoint
            (total-count or newest record_date moved -> requeue);
            weekly force-redrain via {"redrain":true}
  CHAIN     MIDAS self-chain, save-first attempts -> quarantine at 4
State data/warm/fiscaldata-full/_state/state.json; manifest feeds
the card (fd-note-v2).
"""
import gzip
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

ENGINE_VERSION = "justhodl-fiscaldata-full v1.0.1 ops4974 seeds+20"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
API = ("https://api.fiscaldata.treasury.gov/services/api/"
       "fiscal_service")
DOCS = "https://fiscaldata.treasury.gov/api-documentation/"
ROOT = "data/warm/fiscaldata-full/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com)",
      "Accept": "application/json"}
BUDGET_S = int(os.environ.get("FD_BUDGET_S", "690"))
PAGE_SIZE = 10000
SPACING = 0.25
CHAIN_DEPTH_MAX = 60

SEEDS = [
    "v1/accounting/od/auctions_query",
    "v2/accounting/od/debt_to_penny",
    "v2/accounting/od/debt_outstanding",
    "v1/accounting/mts/mts_table_1",
    "v1/accounting/mts/mts_table_4",
    "v1/accounting/mts/mts_table_5",
    "v1/accounting/mts/mts_table_9",
    "v1/accounting/dts/operating_cash_balance",
    "v1/accounting/dts/deposits_withdrawals_operating_cash",
    "v1/accounting/dts/public_debt_transactions",
    "v2/accounting/od/avg_interest_rates",
    "v1/accounting/od/rates_of_exchange",
    "v2/accounting/od/gold_reserve",
    "v2/accounting/od/interest_expense",
    "v1/debt/mspd/mspd_table_1",
    "v1/debt/mspd/mspd_table_3",
    "v2/accounting/od/gift_contributions",
    "v1/accounting/od/record_setting_auction",
    "v2/accounting/od/savings_bonds_report",
    "v1/accounting/od/slgs_securities",
    # ops 4974 seed expansion (docs page is a JS shell; every seed
    # is probe-validated before joining the universe -- wrong
    # guesses fail-named harmlessly)
    "v1/accounting/mts/mts_table_2",
    "v1/accounting/mts/mts_table_3",
    "v1/accounting/mts/mts_table_6",
    "v1/accounting/mts/mts_table_7",
    "v1/accounting/mts/mts_table_8",
    "v1/debt/mspd/mspd_table_2",
    "v1/debt/mspd/mspd_table_4",
    "v1/debt/mspd/mspd_table_5",
    "v1/accounting/dts/adjustment_public_debt_transactions_cash_basis",
    "v1/accounting/dts/debt_subject_to_limit",
    "v1/accounting/dts/federal_tax_deposits",
    "v1/accounting/dts/income_tax_refunds_issued",
    "v1/accounting/dts/short_term_cash_investments",
    "v1/accounting/dts/inter_agency_tax_transfers",
    "v2/accounting/od/securities_sales",
    "v2/accounting/od/securities_redemptions",
    "v2/accounting/od/securities_outstanding",
    "v2/accounting/od/schedules_fed_debt_daily_activity",
    "v1/debt/top/top_state",
    "v1/debt/top/top_federal",
]

s3 = boto3.client("s3", region_name="us-east-1")
_t0 = time.time()


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _j(key, default=None):
    try:
        return json.loads(
            s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def _put_json(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, indent=1).encode(),
                  ContentType="application/json")


def _get(url, timeout=90, cap=60_000_000):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read(cap)


def slugify(ep):
    return ep.replace("/", "_")


def probe(ep):
    """1-row validation: (total, newest_date) or raises."""
    raw = _get(API + "/" + ep +
               "?page[size]=1&sort=-record_date&format=json",
               timeout=45, cap=400_000)
    js = json.loads(raw)
    tot = int((js.get("meta") or {}).get("total-count") or 0)
    d0 = (js.get("data") or [{}])
    newest = (d0[0] or {}).get("record_date") if d0 else None
    return tot, newest


def discover(state):
    cands = set(SEEDS)
    try:
        html = _get(DOCS, timeout=60, cap=6_000_000).decode(
            "utf-8", "replace")
        for m in re.findall(
                r"/(v[12]/[a-z0-9_]+(?:/[a-z0-9_]+){1,3})", html):
            cands.add(m)
    except Exception as e:
        state["failures"]["_docs_harvest"] = str(e)[:100]
    uni, invalid = {}, {}
    for ep in sorted(cands):
        if time.time() - _t0 > BUDGET_S - 120:
            state["failures"]["_discover"] = \
                "budget; validated %d so far" % len(uni)
            break
        try:
            tot, newest = probe(ep)
            if tot > 0:
                uni[ep] = {"total": tot, "newest": newest}
            else:
                invalid[ep] = "total=0"
        except urllib.error.HTTPError as e:
            invalid[ep] = "HTTP %s" % e.code
        except Exception as e:
            invalid[ep] = str(e)[:60]
        time.sleep(SPACING)
    state["universe"] = uni
    state["invalid"] = invalid
    q = state.setdefault("queue", [])
    queued = {x[0] for x in q}
    for ep in uni:
        if ep not in state.get("have", {}) and ep not in queued:
            q.append([ep, 0])
    return len(uni)


def drain_one(state, ep):
    """Full paginated pull -> /tmp JSONL -> gz upload. True=done."""
    slug = slugify(ep)
    tmp = "/tmp/%s.jsonl.gz" % slug
    rows, pages, total = 0, 0, None
    try:
        with gzip.open(tmp, "wt", encoding="utf-8") as f:
            page = 1
            while True:
                raw = _get(API + "/" + ep +
                           "?page[size]=%d&page[number]=%d"
                           "&sort=record_date&format=json"
                           % (PAGE_SIZE, page))
                js = json.loads(raw)
                meta = js.get("meta") or {}
                total = int(meta.get("total-count") or 0)
                tp = int(meta.get("total-pages") or 1)
                for r_ in (js.get("data") or []):
                    f.write(json.dumps(r_, separators=(",", ":"))
                            + "\n")
                    rows += 1
                pages = page
                if page >= tp:
                    break
                page += 1
                if time.time() - _t0 > BUDGET_S - 60:
                    raise TimeoutError(
                        "budget mid-pagination p%d/%d" % (page, tp))
                time.sleep(SPACING)
        key = ROOT + "src/%s.jsonl.gz" % slug
        s3.upload_file(tmp, BUCKET, key, ExtraArgs={
            "ContentType": "application/gzip",
            "Metadata": {"engine": "fiscaldata-full",
                         "endpoint": ep[:120],
                         "rows": str(rows)}})
        nb = os.path.getsize(tmp)
        os.remove(tmp)
        state["have"][ep] = {
            "rows": rows, "pages": pages, "bytes": nb,
            "total_at_pull": total,
            "newest": (state.get("universe", {}).get(ep) or {}
                       ).get("newest"),
            "at": _now(), "status": "fresh"}
        state["failures"].pop(ep, None)
        return True
    except TimeoutError as e:
        fl = state["failures"]
        fl[ep] = {"err": str(e), "tries":
                  (fl.get(ep) or {}).get("tries", 0)}
        try:
            os.remove(tmp)
        except Exception:
            pass
        return False          # retry from scratch next link
    except Exception as e:
        fl = state["failures"]
        tries = (fl.get(ep) or {}).get("tries", 0) + 1
        fl[ep] = {"err": str(e)[:90], "tries": tries}
        try:
            os.remove(tmp)
        except Exception:
            pass
        return tries >= 3


def refresh_check(state):
    """Delta probes on COMPLETE: requeue moved endpoints."""
    q = state.setdefault("queue", [])
    queued = {x[0] for x in q}
    checked = requeued = 0
    for ep, h in sorted((state.get("have") or {}).items()):
        if time.time() - _t0 > BUDGET_S - 90:
            break
        if ep in queued:
            continue
        try:
            tot, newest = probe(ep)
            checked += 1
            if tot != h.get("total_at_pull") or \
                    (newest and newest != h.get("newest")):
                q.append([ep, 0])
                queued.add(ep)
                requeued += 1
                state["universe"].setdefault(ep, {})["newest"] = \
                    newest
                state["universe"][ep]["total"] = tot
        except Exception:
            continue
        time.sleep(SPACING)
    state["last_refresh_check"] = {"at": _now(), "checked": checked,
                                   "requeued": requeued}
    return requeued


def write_manifest(state):
    have = state.get("have") or {}
    _put_json(MANIFEST_KEY, {
        "as_of": _now(), "engine": "justhodl-fiscaldata-full",
        "version": "1.0.0",
        "endpoints_valid": len(state.get("universe") or {}),
        "endpoints_banked": len(have),
        "rows": sum(v.get("rows") or 0 for v in have.values()),
        "bytes": sum(v.get("bytes") or 0 for v in have.values()),
        "invalid_named": len(state.get("invalid") or {}),
        "queue_left": len(state.get("queue") or []),
        "failures": len(state.get("failures") or {}),
        "phase": state.get("phase"),
        "note": ("complete FiscalData warehouse -- every validated "
                 "dataset endpoint fully paginated since inception "
                 "(auctions 1979-); delta-checked refresh + weekly "
                 "redrain")})


def lambda_handler(event, ctx=None):
    global _t0
    _t0 = time.time()
    event = event or {}
    state = _j(STATE_KEY, None) or {
        "version": "1.0.0", "phase": "DISCOVER", "queue": [],
        "have": {}, "failures": {}, "universe": {}}
    if float(state.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held"}
    state["lease_until"] = time.time() + BUDGET_S + 150
    _put_json(STATE_KEY, state)

    if event.get("redrain") and state.get("phase") == "COMPLETE":
        state["queue"] = [[ep, 0] for ep in sorted(state["have"])]
        state["phase"] = "DRAIN"

    if state["phase"] == "DISCOVER" or event.get("rediscover"):
        if discover(state):
            state["phase"] = "DRAIN"
        _put_json(STATE_KEY, state)

    if state["phase"] == "COMPLETE" and not event.get("no_refresh"):
        if refresh_check(state):
            state["phase"] = "DRAIN"
        _put_json(STATE_KEY, state)

    if state["phase"] == "DRAIN" and state["queue"]:
        while state["queue"]:
            if time.time() - _t0 > BUDGET_S - 50:
                break
            ep, att = state["queue"][0]
            if att >= 4:
                state["failures"][ep] = {
                    "err": "quarantined after %d dead attempts"
                           % att, "tries": att}
                state["queue"].pop(0)
                _put_json(STATE_KEY, state)
                continue
            state["queue"][0][1] = att + 1
            _put_json(STATE_KEY, state)
            if drain_one(state, ep):
                state["queue"].pop(0)
            _put_json(STATE_KEY, state)
            time.sleep(SPACING)
        if not state["queue"]:
            state["phase"] = "COMPLETE"

    n_missing = len(state["queue"])
    depth = int(event.get("chain_depth") or 0)
    chain = bool(n_missing and depth < CHAIN_DEPTH_MAX
                 and not event.get("no_chain"))
    state["lease_until"] = 0
    state["as_of"] = _now()
    state["n_banked"] = len(state["have"])
    state["rows_total"] = sum(v.get("rows") or 0
                              for v in state["have"].values())
    _put_json(STATE_KEY, state)
    write_manifest(state)
    if chain:
        try:
            boto3.client("lambda", region_name="us-east-1").invoke(
                FunctionName=os.environ.get(
                    "AWS_LAMBDA_FUNCTION_NAME",
                    "justhodl-fiscaldata-full"),
                InvocationType="Event",
                Payload=json.dumps(
                    {"chain_depth": depth + 1}).encode())
        except Exception:
            chain = False
    return {"ok": True, "phase": state["phase"],
            "banked": state["n_banked"],
            "rows": state["rows_total"],
            "queue_left": n_missing, "chained": chain,
            "elapsed_s": round(time.time() - _t0, 1)}
