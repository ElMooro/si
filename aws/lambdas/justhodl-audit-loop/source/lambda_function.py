"""justhodl-audit-loop v1.0 (ops 4380) — the ongoing estate audit.

Khalid's directive: audit the entire system engine-by-engine, page-by-page,
fix and improve continuously, as an ongoing loop with Perplexity verifying
and prompting continuation.

Every 2 hours this engine audits a shard (~25 engines + ~15 pages),
cycling the full estate every ~2-3 days, forever:

ENGINE CHECKS (mechanical, evidence-backed): function state, empty env
keys, schedule binding (EventBridge target OR central-manifest membership),
26h invocation count + error signatures from logs, heuristic output-feed
freshness. PAGE CHECKS: HTTP 2xx + size, referenced data feeds exist +
age, CSP presence, innerHTML-vs-escape heuristic (xss risk), bare-fetch
resilience flag.

Findings land deduped in data/audit/findings.json with auto-close when a
previously failing check passes (status:fixed). New critical/warn findings
are filed on A2A thread `audit-loop-main` as evidence-carrying propose
turns and fanned out to Perplexity for invariant-B verification. A handoff
brief (data/audit/handoff.json) plus Perplexity's NEXT_ACTIONS lines tell
Claude exactly where to continue each session; a daily Telegram nudge
tells Khalid the loop is alive.
"""
import hashlib
import json
import os
import re
import urllib.request
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config

try:
    from _sentry_lite import track_errors
except Exception:  # pragma: no cover
    def track_errors(f):
        return f

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
SITE = "https://justhodl.ai/"
ENG_N = int(os.environ.get("AUDIT_ENGINES_PER_RUN", "25"))
PAGE_N = int(os.environ.get("AUDIT_PAGES_PER_RUN", "15"))
BUS = "justhodl-a2a-bus"
THREAD = "audit-loop-main"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=60, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def _now():
    return datetime.now(timezone.utc)


def _get(key, default=None):
    try:
        return json.loads(
            s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def _put(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")


def _head_age(key):
    try:
        h = s3.head_object(Bucket=BUCKET, Key=key)
        return round((_now() - h["LastModified"]).total_seconds() / 3600, 1)
    except Exception:
        return None


def _find(target, layer, check, severity, detail, evidence):
    return {"id": hashlib.sha256(f"{target}|{check}".encode())
            .hexdigest()[:12],
            "target": target, "layer": layer, "check": check,
            "severity": severity, "detail": detail[:400],
            "evidence": evidence[:4]}


_EXEMPT = None


def _exempt():
    global _EXEMPT
    if _EXEMPT is None:
        _EXEMPT = set((_get("data/audit/exemptions.json") or {})
                      .get("stale_exempt") or [])
    return _EXEMPT


def audit_engine(fn, manifest_fns):
    out = []
    try:
        cfg = lam.get_function_configuration(FunctionName=fn)
    except Exception as e:
        out.append(_find(fn, "engine", "exists", "critical",
                         f"function missing: {type(e).__name__}",
                         [{"kind": "log", "ref": "data/engine-manifest.json",
                           "snippet": fn}]))
        return out, {}
    env = (cfg.get("Environment", {}) or {}).get("Variables", {}) or {}
    empty = [k for k, v in env.items() if v == ""]
    if empty:
        out.append(_find(fn, "engine", "empty_env", "warn",
                         f"empty env keys: {empty[:6]}",
                         [{"kind": "log", "ref": "data/engine-manifest.json",
                           "snippet": fn}]))
    scheduled = fn in manifest_fns
    if not scheduled:
        try:
            arn = cfg["FunctionArn"]
            rules = ev.list_rule_names_by_target(
                TargetArn=arn).get("RuleNames", [])
            scheduled = bool(rules)
        except Exception:
            rules = []
    inv, err_sig = None, {}
    try:
        since = int((_now() - timedelta(hours=26)).timestamp() * 1000)
        ee = logs.filter_log_events(
            logGroupName=f"/aws/lambda/{fn}", startTime=since, limit=600)
        msgs = [e["message"] for e in ee.get("events", [])]
        inv = sum(1 for m in msgs if "START RequestId" in m)
        for m in msgs:
            if any(k in m for k in ("ERROR", "Task timed out", "Traceback")):
                err_sig[m.strip()[:110]] = err_sig.get(m.strip()[:110], 0) + 1
    except Exception:
        pass
    if scheduled and inv == 0:
        out.append(_find(fn, "engine", "scheduled_but_silent", "critical",
                         "bound to a schedule but 0 invocations in 26h",
                         [{"kind": "log", "ref": "data/engine-manifest.json",
                           "snippet": fn}]))
    if err_sig:
        top = sorted(err_sig.items(), key=lambda kv: -kv[1])[:2]
        out.append(_find(fn, "engine", "log_errors", "warn",
                         f"{sum(err_sig.values())} error lines 26h; "
                         f"top: {top}",
                         [{"kind": "log", "ref": "data/engine-manifest.json",
                           "snippet": fn}]))
    guess = fn.replace("justhodl-", "")
    feed_age = _head_age(f"data/{guess}.json")
    if (scheduled and feed_age is not None and feed_age > 26
            and f"data/{guess}.json" not in _exempt()):
        out.append(_find(fn, "engine", "stale_feed", "warn",
                         f"data/{guess}.json age {feed_age}h with live "
                         "schedule",
                         [{"kind": "log", "ref": f"data/{guess}.json"}]))
    return out, {"invocations_26h": inv, "scheduled": scheduled,
                 "feed_age_h": feed_age}


PROTECTED_ARTIFACTS = {"crisis.html": "claude", "liquidity.html": "claude",
                       "plumbing.html": "claude"}


def check_protected(path):
    """Flag protected-artifact changes by non-owners (Khalid ownership law).
    Direct push can't be git-blocked without branch protection, so the loop
    reports any modification to a protected page as a P0 for Khalid."""
    fname = path.split("/")[-1]
    owner = PROTECTED_ARTIFACTS.get(fname)
    if not owner:
        return []
    # who last touched it? read git blame proxy via the patches ledger +
    # a marker file the owner writes. Heuristic: check data/a2a/patches.json
    # for a non-owner PR touching this file recently.
    out = []
    patches = _get("data/a2a/patches.json", {"patches": []}).get("patches", [])
    for pt in patches[-30:]:
        if fname in (pt.get("files") or []) and pt.get("agent") != owner:
            out.append(_find(fname, "governance",
                             f"protected_modified_by_{pt.get('agent')}",
                             "critical",
                             f"{fname} is {owner}-owned+Khalid-protected but "
                             f"patch {pt.get('patch_id')} by "
                             f"{pt.get('agent')} touched it (PR "
                             f"#{pt.get('pr')}) — Khalid ruling required",
                             [{"kind": "log", "ref": "data/a2a/patches.json",
                               "snippet": pt.get("patch_id", "")}]))
    return out


def audit_page(path):
    out = []
    url = SITE + path.lstrip("/")
    try:
        req = urllib.request.Request(url, headers={"User-Agent":
                                                   "justhodl-audit-loop"})
        with urllib.request.urlopen(req, timeout=10) as r:
            status = r.status
            body = r.read(400000).decode("utf-8", "replace")
    except Exception as e:
        out.append(_find(path, "page", "reachable", "critical",
                         f"fetch failed: {type(e).__name__}: {str(e)[:80]}",
                         [{"kind": "url", "ref": url}]))
        return out, {}
    if status != 200 or len(body) < 500:
        out.append(_find(path, "page", "reachable", "critical",
                         f"status {status}, bytes {len(body)}",
                         [{"kind": "url", "ref": url}]))
        return out, {"status": status}
    feeds = sorted(set(re.findall(
        r'(?:justhodl-dashboard-live\.s3\.amazonaws\.com/)?'
        r'(data/[a-z0-9\-_/]+\.json)', body)))[:3]
    for f in feeds:
        age = _head_age(f)
        if age is None:
            out.append(_find(path, "page", f"feed_missing:{f}", "critical",
                             f"references {f} which does not exist on S3",
                             [{"kind": "url", "ref": url, "snippet": f}]))
        elif age > 48 and f not in _exempt():
            out.append(_find(path, "page", f"feed_stale:{f}", "warn",
                             f"{f} is {age}h old",
                             [{"kind": "log", "ref": f}]))
    inner = body.count("innerHTML")
    escs = body.count("escape(") + body.count("esc(")
    if inner > 0 and escs == 0:
        out.append(_find(path, "page", "xss_heuristic", "warn",
                         f"{inner} innerHTML sites, zero escape helpers",
                         [{"kind": "url", "ref": url,
                           "snippet": "innerHTML"}]))
    if "Content-Security-Policy" not in body and inner > 0:
        out.append(_find(path, "page", "no_csp", "info",
                         "no CSP meta on a page with dynamic HTML",
                         [{"kind": "url", "ref": url}]))
    if "await fetch(" in body and "AbortSignal" not in body:
        out.append(_find(path, "page", "bare_fetch", "info",
                         "fetch without timeout/abort",
                         [{"kind": "url", "ref": url,
                           "snippet": "await fetch("}]))
    return out, {"bytes": len(body), "feeds": feeds}


def bus_call(payload):
    try:
        inv = lam.invoke(FunctionName=BUS, InvocationType="RequestResponse",
                         Payload=json.dumps(payload).encode())
        b = json.loads(inv["Payload"].read().decode())
        return json.loads(b["body"]) if isinstance(b, dict) and "body" in b \
            else b
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}


def telegram_nudge(text, state):
    last = state.get("last_telegram")
    if last:
        try:
            if (_now() - datetime.fromisoformat(last)).total_seconds() < 86000:
                return False
        except Exception:
            pass
    try:
        tok = ssm.get_parameter(Name="/justhodl/telegram/bot_token",
                                WithDecryption=True)["Parameter"]["Value"]
        chat = ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                 WithDecryption=True)["Parameter"]["Value"]
        urllib.request.urlopen(urllib.request.Request(
            f"https://api.telegram.org/bot{tok}/sendMessage",
            data=json.dumps({"chat_id": chat, "text": text}).encode(),
            headers={"Content-Type": "application/json"}), timeout=10)
        state["last_telegram"] = _now().isoformat()
        return True
    except Exception as e:
        print("telegram err:", str(e)[:80])
        return False


@track_errors
def lambda_handler(event, context):
    inv = _get("data/audit/inventory.json") or {}
    engines = inv.get("engines") or []
    pages = inv.get("pages") or []
    if not engines:
        man = _get("data/engine-manifest.json") or {}
        rows = man.get("engines") or man.get("functions") or man
        if isinstance(rows, dict):
            rows = list(rows.values())
        engines = sorted({(r.get("function_name") or r.get("name"))
                          for r in rows if isinstance(r, dict)
                          and (r.get("function_name") or r.get("name"))})
        inv["engines"] = engines
    manifest_fns = set(inv.get("manifest_fns") or [])
    if not manifest_fns:
        man2 = _get("config/schedule-manifest.json") or {}
        ent = man2.get("schedules") or man2.get("entries") or man2
        if isinstance(ent, dict):
            manifest_fns = set(ent.keys())
        elif isinstance(ent, list):
            manifest_fns = {e.get("function") or e.get("fn") or e.get("name")
                            for e in ent if isinstance(e, dict)}
        inv["manifest_fns"] = sorted(x for x in manifest_fns if x)

    cur = _get("data/audit/cursor.json") or {"eng_i": 0, "page_i": 0,
                                             "cycle": 1, "runs": 0}
    ei, pi = cur["eng_i"], cur["page_i"]
    eng_shard = engines[ei:ei + ENG_N]
    page_shard = pages[pi:pi + PAGE_N]

    findings, health = [], {}
    for fn in eng_shard:
        f, h = audit_engine(fn, manifest_fns)
        findings += f
        health[fn] = h
    for pg in page_shard:
        f, h = audit_page(pg)
        findings += f
        findings += check_protected(pg)

    store = _get("data/audit/findings.json") or {"findings": {}}
    fmap = store["findings"]
    now = _now().isoformat(timespec="seconds")
    shard_ids = set()
    new_items = []
    for f in findings:
        shard_ids.add(f["id"])
        if f["id"] in fmap:
            fmap[f["id"]]["last_seen"] = now
            fmap[f["id"]]["status"] = "open"
        else:
            fmap[f["id"]] = {**f, "first_seen": now, "last_seen": now,
                             "status": "open"}
            new_items.append(fmap[f["id"]])
    audited_targets = set(eng_shard) | set(page_shard)
    for fid, f in fmap.items():
        if f["target"] in audited_targets and fid not in shard_ids \
                and f["status"] == "open":
            f["status"] = "fixed"
            f["fixed_at"] = now
    store["updated"] = now
    _put("data/audit/findings.json", store)

    filed = []
    to_file = [f for f in new_items
               if f["severity"] in ("critical", "warn")][:3]
    if to_file:
        if not bus_call({"action": "get_thread",
                         "thread_id": THREAD}).get("thread"):
            bus_call({"action": "open_thread", "thread_id": THREAD,
                      "topic": "Perpetual estate audit — findings for "
                               "verification (loop v1)"})
        for f in to_file:
            r = bus_call({"action": "post_turn", "thread_id": THREAD,
                          "from": "claude-audit", "to": "perplexity",
                          "kind": "propose",
                          "content": f"[{f['severity']}] {f['layer']}:"
                                     f"{f['target']} — {f['check']}: "
                                     f"{f['detail']} (id {f['id']}). "
                                     "Verify or refute with evidence.",
                          "evidence": f["evidence"]})
            filed.append({"id": f["id"], "posted": r.get("ok"),
                          "err": r.get("error")})
        bus_call({"action": "fanout_pending"})

    ne = ei + len(eng_shard)
    np_ = pi + len(page_shard)
    wrapped = ne >= len(engines) and np_ >= len(pages)
    cur = {"eng_i": 0 if wrapped else ne,
           "page_i": 0 if wrapped else np_,
           "cycle": cur["cycle"] + (1 if wrapped else 0),
           "runs": cur["runs"] + 1,
           "last_run": now, "last_telegram": cur.get("last_telegram")}

    open_f = [f for f in fmap.values() if f["status"] == "open"]
    crit = [f for f in open_f if f["severity"] == "critical"]
    thread_doc = bus_call({"action": "get_thread",
                           "thread_id": THREAD}).get("thread") or {}
    next_actions = [x["content"][x["content"].find("NEXT_ACTIONS"):][:400]
                    for x in (thread_doc.get("turns") or [])
                    if x.get("from") == "perplexity"
                    and "NEXT_ACTIONS" in (x.get("content") or "")][-3:]
    handoff = {"updated": now,
               "coverage": {"engines": f"{min(ne, len(engines))}/"
                                       f"{len(engines)}",
                            "pages": f"{min(np_, len(pages))}/{len(pages)}",
                            "cycle": cur["cycle"], "runs": cur["runs"]},
               "open_findings": len(open_f), "critical": len(crit),
               "top_open": sorted(open_f, key=lambda f:
                                  (f["severity"] != "critical",
                                   f["last_seen"]))[:10],
               "perplexity_next_actions": next_actions,
               "note": "Claude: read this + data/a2a/inbox/claude.json "
                       "each session; drain criticals first; post fix "
                       "turns with evidence; Perplexity verifies; resolve."}
    _put("data/audit/handoff.json", handoff)

    nudged = telegram_nudge(
        f"🔁 Audit loop c{cur['cycle']}: engines "
        f"{handoff['coverage']['engines']}, pages "
        f"{handoff['coverage']['pages']} · {len(open_f)} open "
        f"({len(crit)} crit) · brief: data/audit/handoff.json", cur)
    _put("data/audit/cursor.json", cur)

    res = {"ok": True, "shard": {"engines": len(eng_shard),
                                 "pages": len(page_shard)},
           "new_findings": len(new_items), "filed_to_bus": filed,
           "open_total": len(open_f), "critical": len(crit),
           "cursor": cur, "telegram": nudged}
    if not inv.get("_persisted"):
        inv["_persisted"] = True
        _put("data/audit/inventory.json", inv)
    print(json.dumps({k: res[k] for k in
                      ("shard", "new_findings", "open_total", "critical")}))
    return {"statusCode": 200, "body": json.dumps(res, default=str)}
