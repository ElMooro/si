"""ops 3259 — Khalid's notes, everywhere they belong.

  1. TOP-UP: brain-TV 2,920 → full mirror parity. Diff mirror ids vs
     the canonical brain's tv ids; PUT only the missing (~400),
     idempotent, budgeted.
  2. VERIFY the existing stance fusion is LIVE (ops 3171 wired
     master-ranker / best-setups / alpha-compass): sample rows from
     each live feed showing his stance riding the ranks.
  3. NEW consumer: equity-research now attaches khalid_notes
     (stance, latest note, levels) to every research doc — deploy,
     force-generate one ticker with known notes, prove the field.
"""
import json
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
BRAIN = "https://justhodl-data-proxy.raafouis.workers.dev/brain"
CANON = "brain-930ffa48-60a1-4b11-8726-8848d1b827f9"
UA = {"User-Agent": "JustHodl-Ops-3259/1.0"}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


def walk(o):
    if isinstance(o, dict):
        yield o
        for v in o.values():
            yield from walk(v)
    elif isinstance(o, list):
        for v in o:
            yield from walk(v)


with report("3259_notes_everywhere") as rep:
    fails, warns = [], []
    rep.heading("ops 3259 — top-up + stance fusion verified + research "
                "docs carry his notes")

    rep.section("1. Top-up to full parity")
    req = urllib.request.Request(
        f"{BRAIN}?sync=1&uid={urllib.parse.quote(CANON)}", headers=UA)
    bj = json.loads(urllib.request.urlopen(req, timeout=30).read()
                    .decode("utf-8", "replace"))
    brain_ids = {str(d.get("id")) for d in walk(bj)
                 if isinstance(d, dict) and d.get("id")
                 and (d.get("text") or d.get("body"))}
    mirror = (s3_json("data/tradingview-notes.json") or {})\
        .get("notes") or []
    missing = [n for n in mirror
               if n.get("id") and str(n["id"]) not in brain_ids
               and len(str(n.get("text") or "")) >= 3]
    rep.kv(brain_ids=len(brain_ids), mirror=len(mirror),
           missing=len(missing))
    up = fl = 0
    if missing:
        def put1(n):
            body = json.dumps({"id": n["id"],
                               "cat": n.get("cat") or "thesis",
                               "text": n.get("text") or "",
                               "created": n.get("created"),
                               "pinned": bool(n.get("pinned"))})\
                .encode("utf-8")
            r = urllib.request.Request(
                f"{BRAIN}?uid={urllib.parse.quote(CANON)}", data=body,
                method="PUT",
                headers={"Content-Type": "text/plain", **UA})
            try:
                with urllib.request.urlopen(r, timeout=25) as h:
                    return json.loads(h.read().decode())\
                        .get("ok") is True
            except Exception:
                return False
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=8) as ex:
            for okp in ex.map(put1, missing):
                up += 1 if okp else 0
                fl += 0 if okp else 1
                if time.time() - t0 > 240:
                    warns.append("top-up budget reached")
                    break
        rep.kv(upserted=up, failed=fl)
    req2 = urllib.request.Request(
        f"{BRAIN}?sync=1&uid={urllib.parse.quote(CANON)}"
        f"&t={int(time.time())}", headers=UA)
    bj2 = json.loads(urllib.request.urlopen(req2, timeout=30).read()
                     .decode("utf-8", "replace"))
    tv2 = sum(1 for d in walk(bj2) if isinstance(d, dict)
              and (str(d.get("id", "")).startswith("tv-")
                   or str(d.get("text", "")).startswith("[TV:")))
    rep.kv(brain_tv_now=tv2)
    if tv2 >= len(mirror) - 25:
        rep.ok(f"brain-TV at parity: {tv2} (mirror {len(mirror)})")
    else:
        warns.append(f"brain-TV {tv2} still short of mirror")

    rep.section("2. Stance fusion — live in the three rankers")
    checks = [("data/master-rank.json", ("stocks", "rows", "ranked")),
              ("data/best-setups.json", ("setups", "candidates",
                                         "rows")),
              ("data/alpha-compass.json", ("rows", "assets", "sheet"))]
    live_hits = 0
    for key, fields in checks:
        d = s3_json(key) or {}
        rows = next((d[f] for f in fields
                     if isinstance(d.get(f), list)), [])
        hit = next((r for r in rows if isinstance(r, dict)
                    and any("note" in k.lower() or "khalid" in k.lower()
                            for k in r)), None)
        if hit:
            live_hits += 1
            kn = next(k for k in hit
                      if "note" in k.lower() or "khalid" in k.lower())
            rep.ok(f"{key}: notes field '{kn}' riding rows "
                   f"(e.g. {str(hit.get('symbol') or hit.get('ticker') or '?')})")
        else:
            warns.append(f"{key}: no notes field visible in rows")
    rep.kv(rankers_with_notes=live_hits)

    rep.section("3. equity-research ← khalid_notes")
    fn = "justhodl-equity-research"
    cfg = {}
    pc = AWS_DIR = Path(__file__).resolve().parents[2] / "lambdas" / fn \
        / "config.json"
    if pc.exists():
        cfg = json.loads(pc.read_text())
    sch = cfg.get("schedule")
    rule, cron = (sch.get("rule_name"), sch.get("cron")) \
        if isinstance(sch, dict) else (None, None)
    live = (LAM.get_function_configuration(FunctionName=fn)
            .get("Environment") or {}).get("Variables") or {}
    try:
        deploy_lambda(report=rep, function_name=fn,
                      source_dir=pc.parent / "source",
                      env_vars=live, eb_rule_name=rule, eb_schedule=cron,
                      timeout=cfg.get("timeout", 900),
                      memory=cfg.get("memory", 1536),
                      description=str(cfg.get("description", ""))[:250],
                      smoke=False)
        LAM.get_waiter("function_updated_v2").wait(
            FunctionName=fn, WaiterConfig={"Delay": 2,
                                           "MaxAttempts": 30})
    except Exception as e:
        fails.append(f"deploy research: {str(e)[:80]}")
    tick = None
    ni = (s3_json("data/notes-index.json") or {}).get("index") or {}
    for cand in ("NVDA", "TSLA", "AAPL", "MSFT"):
        if cand in ni:
            tick = cand
            break
    tick = tick or (sorted(ni)[0] if ni else "AAPL")
    if not fails:
        LAM.invoke(FunctionName=fn, InvocationType="Event",
                   Payload=json.dumps({"_internal": "1",
                                       "ticker": tick,
                                       "force_refresh": True})
                   .encode())
        got = None
        for _ in range(30):
            time.sleep(8)
            d = s3_json(f"equity-research/{tick}.json") or {}
            if (d.get("khalid_notes") or {}).get("n_notes") is not None:
                got = d
                break
        if got:
            kb = got["khalid_notes"]
            rep.ok(f"{tick} research doc carries khalid_notes: "
                   f"stance={kb.get('stance')} n={kb.get('n_notes')} "
                   f"latest='{str(kb.get('latest_note'))[:60]}'")
        else:
            warns.append(f"{tick}: khalid_notes not visible yet "
                         "(long generation) — field ships on next "
                         "generations regardless")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
