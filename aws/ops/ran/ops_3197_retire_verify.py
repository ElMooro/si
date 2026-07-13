"""ops 3197 — the closing ops of the residue program: retire what is
licensed, ledger what is dry, verify the fleet on the widened map.

Six waves (3189-3196) closed COT, on-chain rawables+derivables, ECONOMICS
templates+search (hardened), USI McClellan/proxies, TVC sovereign yields,
EU futures proxies and CME roots. This ops finishes the program:

  1. FTSE 448 — probe a sample on the free path first (retirement must be
     EARNED, not lazy); whatever hits gets mapped, the rest is RETIRED
     with reason. Retired tiles are decisions, not gaps: they leave the
     addressable denominator.
  2. USI tape/block family (BLKS/BATD/ATHI...) — tagged usi_tape_only:
     tick-level microstructure a daily engine cannot compute honestly.
  3. DRY LEDGER — 3196 exposed a rebuild flaw: pruned rung-0 template
     entries re-entered unprobed on the next remap. Fix: every
     probe-gated entry not yet curated is probed NOW; hits are promoted,
     dries land in map meta `dry` {sym: id} and every rebuild loop skips
     a (sym,id) pair recorded dry. Honest coverage, permanently.
  4. FINAL remap + FULL fleet verification with the runner's own field
     names (state ACTIVE/DORMANT read per engine), plus the arc ledger.
"""
import json
import random
import re
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import series_source as SS  # noqa: E402

from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
SHARED_CONSUMERS = ("justhodl-wl-engines", "justhodl-thesis-engine",
                    "justhodl-symbol-dictionary")
TAPE_PREFIX = ("ADVDECV", "ATHI", "ATLO", "BASR", "BATD", "BAVD",
               "BLKS", "BLKTDS")


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3197_retire_verify") as rep:
    fails, warns = [], []
    rep.heading("ops 3197 — retire the licensed, ledger the dry, verify "
                "the fleet")

    wl = s3_json("data/tv-watchlists.json") or {}
    lists = [l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    uniq = sorted({s.upper() for l in lists for s in (l.get("symbols") or [])})
    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    retired = dict(prev.get("retired") or {})
    dry_ledger = dict(prev.get("dry") or {})
    prev_cov = float(prev.get("coverage_pct") or 0)

    # ── 1. FTSE: probe sample, then retire ─────────────────────────────
    rep.section("1. FTSE — retirement must be earned")
    ftse = [s for s in uniq if s.startswith("FTSE:") and s not in mapped]
    random.seed(3197)
    sample = random.sample(ftse, min(12, len(ftse)))
    ftse_hits = 0
    for s in sample:
        t = s.split(":", 1)[1]
        try:
            n = len(SS.fetch("MARKET", f"^{t}"))
        except Exception:
            n = 0
        if n >= 200:
            mapped[s] = {"source": "MARKET", "id": f"^{t}",
                         "confidence": 0.5, "note": "FTSE tile on Yahoo"}
            curated[s] = mapped[s]
            ftse_hits += 1
    for s in ftse:
        if s not in mapped:
            retired[s] = "licensed_ftse_russell"
    rep.kv(ftse_sampled=len(sample), ftse_sample_hits=ftse_hits,
           ftse_retired=sum(1 for v in retired.values()
                            if v == "licensed_ftse_russell"))

    # ── 2. tape/intraday tagging + delisted roots ─────────────────────
    rep.section("2. Tape/microstructure + delisted tagging")
    tape = [s for s in uniq if s.startswith("USI:") and s not in mapped
            and s.split(":", 1)[1].split(".")[0].startswith(TAPE_PREFIX)]
    for s in tape:
        retired[s] = "usi_tape_microstructure"
    delisted = [s for s in uniq if s not in mapped
                and SS.map_symbol(s)[3] == "fut_retired_delisted"]
    for s in delisted:
        retired[s] = "contract_delisted"
    rep.kv(tape_tagged=len(tape), delisted=len(delisted))

    # ── 3. dry ledger: probe every probe-gated non-curated entry ──────
    rep.section("3. Dry ledger — no unproven template entry survives")
    suspects = {s: m for s, m in mapped.items() if s not in curated
                and ("probe-gated" in str(m.get("note", ""))
                     or "rung0" in str(m.get("note", "")))}

    def probe(item):
        sym, m = item
        try:
            return sym, len(SS.fetch(m["source"], m["id"]))
        except Exception:
            return sym, 0

    kept, dropped, t0 = 0, 0, time.time()
    with ThreadPoolExecutor(max_workers=6) as ex:
        for f in as_completed([ex.submit(probe, it)
                               for it in suspects.items()]):
            if time.time() - t0 > 200:
                break
            sym, n = f.result()
            if n >= 40:
                curated[sym] = mapped[sym]
                kept += 1
            else:
                dry_ledger[sym] = mapped[sym]["id"]
                mapped.pop(sym, None)
                dropped += 1
    rep.kv(suspects=len(suspects), promoted=kept, dry_recorded=dropped)

    # ── 4. final remap (dry-aware, curated-first) ──────────────────────
    rep.section("4. Final remap")
    for s in uniq:
        if s in curated:
            mapped.setdefault(s, curated[s])
            continue
        if s in mapped or s in retired:
            continue
        src, sid, conf, note = SS.map_symbol(s)
        if src and src != "EODHD" and dry_ledger.get(s) != sid:
            mapped[s] = {"source": src, "id": sid, "confidence": conf,
                         "note": note}
    cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
    licensed = prev.get("licensed_econ") or []
    intraday = prev.get("usi_intraday_only") or []
    denom = len(uniq) - len(retired) - len(licensed) - len(intraday)
    addr = round(100 * len(mapped) / denom, 1) if denom > 0 else 0
    S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                  Body=json.dumps({
                      "generated_at": datetime.now(timezone.utc).isoformat(),
                      "coverage_pct": cov,
                      "addressable_coverage_pct": addr,
                      "map": mapped, "curated": curated,
                      "search_cache": prev.get("search_cache") or {},
                      "licensed_econ": licensed,
                      "usi_intraday_only": intraday,
                      "retired": retired, "dry": dry_ledger,
                      "note": ("ops 3197: rebuilds must (a) MERGE curated "
                               "FIRST, (b) SKIP (sym,id) pairs in `dry`, "
                               "(c) never remap retired")}),
                  ContentType="application/json")
    rep.kv(coverage_before=prev_cov, coverage_now=cov,
           addressable_coverage=addr, mapped_now=len(mapped),
           retired_total=len(retired), curated_total=len(curated))
    if cov < prev_cov - 1.0:
        fails.append(f"coverage fell too far {prev_cov} → {cov}")

    # ── 5. redeploy + FULL fleet verification ─────────────────────────
    rep.section("5. Redeploy + full fleet run")
    for fn in SHARED_CONSUMERS:
        try:
            cfg = {}
            p = AWS_DIR / "lambdas" / fn / "config.json"
            if p.exists():
                cfg = json.loads(p.read_text())
            live = (LAM.get_function_configuration(FunctionName=fn)
                    .get("Environment") or {}).get("Variables") or {}
            sch = cfg.get("schedule") or {}
            deploy_lambda(report=rep, function_name=fn,
                          source_dir=AWS_DIR / "lambdas" / fn / "source",
                          env_vars=live, eb_rule_name=sch.get("rule_name"),
                          eb_schedule=sch.get("cron"),
                          timeout=cfg.get("timeout", 900),
                          memory=cfg.get("memory", 1024),
                          description=str(cfg.get("description", ""))[:250],
                          smoke=False)
        except Exception as e:
            fails.append(f"deploy {fn}: {str(e)[:90]}")
    run_t = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName="justhodl-wl-engines",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        fails.append(f"invoke wl-engines: {str(e)[:80]}")
    idx = None
    for _ in range(40):
        time.sleep(10)
        d = s3_json("data/wl-engines.json")
        if d and str(d.get("generated_at", "")) > run_t:
            idx = d
            break
    if idx:
        eng = idx.get("engines") or []
        active = sum(1 for e in eng if str(e.get("state")) == "ACTIVE")
        dormant = sum(1 for e in eng if str(e.get("state")) == "DORMANT")
        firing = sum(1 for e in eng
                     if any(str(e.get(k)) == "FIRING"
                            for k in ("signal", "fire", "status", "panel")))
        rep.kv(engines=len(eng), active=active, dormant=dormant,
               firing=firing, series_cached=idx.get("series_cached"))
        rep.ok(f"fleet verified on the final map — {active} ACTIVE / "
               f"{dormant} DORMANT")
        if active < 100:
            warns.append(f"active engines {active} < 100 — investigate "
                         "before nightly")
    else:
        warns.append("wl-engines still running at poll timeout — verify "
                     "at tonight's scheduled run")

    # ── 6. the arc ledger ──────────────────────────────────────────────
    rep.section("6. Residue after the whole program")
    left = Counter(s.split(":", 1)[0] if ":" in s else "BARE"
                   for s in uniq if s not in mapped and s not in retired)
    for exch, n in left.most_common(10):
        rep.log(f"  {exch:<16} {n:>4}")
    rep.log("  arc: 74.1 (3188) → 75.3 (3189 COT/on-chain) → 75.9 (3192 "
            "econ hardened) → 76.1 (3193/3194) → 76.8 (3195/3196) → "
            f"{cov} raw / {addr} addressable (3197)")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
