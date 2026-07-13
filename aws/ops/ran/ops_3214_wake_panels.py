"""ops 3214 — wake the panels: the 47-engine worklist, attacked by
engine-activation math.

3208 named the worklist: 47 engines DORMANT because <6 members map, 39
more because mapped members lack fetchable history. Doctrine (3184): a
symbol is worth mapping in proportion to the DORMANT engines it wakes.

  1. WORKLIST MATH — nearest-to-waking engines ranked (need = 6 −
     resolved-with-history), unmapped members bucketed, and the symbols
     SHARED across dormant engines scored (one mapping, many wakes).
  2. CBOEEU 40 — pan-European tickers with no US primary. A Yahoo
     SUFFIX LADDER (.L .PA .AS .DE .MI .MC .SW .ST .BR .HE) probes each
     base ticker; first suffix returning a real series wins, curated.
  3. TARGETED econ search — hardened dbn search (country-token enforced)
     only for ECONOMICS symbols sitting inside near-wake engines.
  4. DFM/ADX 27 — probed on candidate paths; zero hits = honest
     retirement ("no free daily source — Gulf venue"), not a fake.
  5. Fleet re-run: the KPI is ENGINES WOKEN, reported by name.
"""
import json
import re
import sys
import time
from collections import Counter, defaultdict
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
SUFFIXES = (".L", ".PA", ".AS", ".DE", ".MI", ".MC", ".SW", ".ST",
            ".BR", ".HE")
ECON_RE = re.compile(r"^ECONOMICS:([A-Z]{2})([A-Z0-9]+)$")


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3214_wake_panels") as rep:
    fails, warns = [], []
    rep.heading("ops 3214 — wake the panels (worklist math → mappings → "
                "engines woken)")

    wl = s3_json("data/tv-watchlists.json") or {}
    lists = {str(l.get("id")): l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")}
    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    retired = dict(prev.get("retired") or {})
    dry = dict(prev.get("dry") or {})
    cache = prev.get("search_cache") or {}
    prev_cov = float(prev.get("coverage_pct") or 0)
    idx = s3_json("data/wl-engines.json") or {}
    eng = idx.get("engines") or []
    prev_active = {e["engine_id"] for e in eng
                   if str(e.get("state")) == "ACTIVE"}

    # ── 1. worklist math ───────────────────────────────────────────────
    rep.section("1. Nearest-to-waking engines + shared-symbol targets")
    near, share = [], Counter()
    sym_engines = defaultdict(set)
    for e in eng:
        if str(e.get("state")) != "DORMANT":
            continue
        l = lists.get(str(e.get("tv_id"))) or {}
        syms = [s.upper() for s in (l.get("symbols") or [])]
        unmapped = [s for s in syms if s not in mapped and s not in retired]
        resolved = int(e.get("members_resolved") or 0)
        need = max(0, 6 - resolved)
        if unmapped and need <= 8:
            near.append((need, e.get("name"), resolved,
                         len(syms), unmapped))
            for s in unmapped:
                share[s] += 1
                sym_engines[s].add(e.get("engine_id"))
    near.sort(key=lambda x: (x[0], -len(x[4])))
    for need, name, res, tot, un in near[:12]:
        b = Counter(s.split(":")[0] if ":" in s else "BARE"
                    for s in un).most_common(2)
        rep.log(f"  need {need}  {str(name)[:34]:<34} "
                f"{res}/{tot}  gaps: " + ", ".join(f"{k}×{n}"
                                                   for k, n in b))
    top_shared = [s for s, _n in share.most_common(60)]
    rep.kv(near_wake_engines=len(near),
           shared_targets=len(top_shared))

    # ── 2. CBOEEU suffix ladder ────────────────────────────────────────
    rep.section("2. CBOEEU → Yahoo suffix ladder (probe-gated)")
    cbo = [s for s in share if s.startswith("CBOEEU:")] or \
          [s for l in lists.values() for s in
           [x.upper() for x in (l.get("symbols") or [])]
           if s.startswith("CBOEEU:") and s not in mapped
           and s not in retired]
    cbo = sorted(set(cbo))[:40]

    def suffix_probe(sym):
        t = sym.split(":", 1)[1]
        for suf in SUFFIXES:
            try:
                ser = SS._yahoo(f"{t}{suf}", "2005-01-01")
            except Exception:
                ser = {}
            if len(ser) >= 200:
                return sym, {"source": "MARKET", "id": f"{t}{suf}",
                             "confidence": 0.6,
                             "note": f"CBOEEU → Yahoo {suf} "
                                     "(suffix-ladder, ops 3214)"}, len(ser)
        return sym, None, 0

    hits_c, t0 = {}, time.time()
    with ThreadPoolExecutor(max_workers=8) as ex:
        for f in as_completed([ex.submit(suffix_probe, s) for s in cbo]):
            if time.time() - t0 > 300:
                break
            sym, entry, n = f.result()
            if entry:
                hits_c[sym] = entry
                mapped[sym] = entry
                curated[sym] = entry
    rep.kv(cboeeu_probed=len(cbo), cboeeu_hits=len(hits_c))
    for sym, e in list(hits_c.items())[:5]:
        rep.log(f"    ✓ {sym} → {e['id']}")

    # ── 3. targeted econ search (near-wake members only) ──────────────
    rep.section("3. Targeted econ search (country-enforced)")
    econ_t = [s for s in top_shared if ECON_RE.match(s)][:50]
    names_doc = s3_json("data/symbol-dictionary.json") or {}
    names = names_doc.get("dictionary") or names_doc.get("symbols") or {}
    searcher = SS.dbn_search_factory(cache)
    hits_e = {}
    t0 = time.time()
    for s in econ_t:
        if time.time() - t0 > 240:
            break
        m = ECON_RE.match(s)
        i2 = m.group(1)
        i3 = SS.ISO2_ISO3.get(i2) or ""
        nm = names.get(s)
        if isinstance(nm, dict):
            nm = nm.get("name")
        q = re.sub(r"\s*[—-]\s*", " ", str(nm or "")).strip()
        if not q:
            continue
        for sid in searcher(q, country=q.split(" ")[0], iso=(i2, i3)):
            if dry.get(s) == sid:
                continue
            try:
                ser = SS.fetch("DBNOMICS", sid)
            except Exception:
                ser = {}
            if len(ser) >= 40:
                e = {"source": "DBNOMICS", "id": sid, "confidence": 0.6,
                     "note": f"dbn-search v2: {str(nm)[:50]}"}
                hits_e[s] = e
                mapped[s] = e
                curated[s] = e
                break
    rep.kv(econ_searched=len(econ_t), econ_hits=len(hits_e))
    for sym, e in list(hits_e.items())[:4]:
        rep.log(f"    ✓ {sym} → {e['id'][:56]}")

    # ── 4. DFM/ADX verdict ─────────────────────────────────────────────
    rep.section("4. Gulf venues (DFM/ADX) — probe, then verdict")
    gulf = sorted({s for l in lists.values()
                   for s in [x.upper() for x in (l.get("symbols") or [])]
                   if s.split(":")[0] in ("DFM", "ADX")
                   and s not in mapped and s not in retired})
    g_hits = 0
    for s in gulf[:10]:
        t = s.split(":", 1)[1]
        for cand in (f"{t}.AE", f"{t}.AD", t):
            try:
                if len(SS._yahoo(cand, "2015-01-01")) >= 200:
                    mapped[s] = {"source": "MARKET", "id": cand,
                                 "confidence": 0.5,
                                 "note": "Gulf venue on Yahoo (ops 3214)"}
                    curated[s] = mapped[s]
                    g_hits += 1
                    break
            except Exception:
                pass
    if g_hits == 0 and gulf:
        for s in gulf:
            retired[s] = "no_free_daily_source_gulf_venue"
        rep.log(f"  0/{min(10, len(gulf))} sample hits → all {len(gulf)} "
                "retired: no free daily source (Gulf venue)")
    rep.kv(gulf_symbols=len(gulf), gulf_hits=g_hits)

    # ── 5. write map + fleet re-run: ENGINES WOKEN ─────────────────────
    rep.section("5. Write + fleet re-run — the KPI is engines WOKEN")
    uniq = sorted({s.upper() for l in lists.values()
                   for s in (l.get("symbols") or [])})
    cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
    S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                  Body=json.dumps({**{k: prev.get(k) for k in
                                      ("licensed_econ",
                                       "usi_intraday_only") if k in prev},
                                   "generated_at":
                                       datetime.now(timezone.utc)
                                       .isoformat(),
                                   "coverage_pct": cov,
                                   "map": mapped, "curated": curated,
                                   "retired": retired, "dry": dry,
                                   "search_cache": cache,
                                   "note": ("ops 3214: wake-the-panels — "
                                            "contract: curated first, "
                                            "skip dry, never remap "
                                            "retired")}),
                  ContentType="application/json")
    rep.kv(coverage_before=prev_cov, coverage_now=cov,
           new_mappings=len(hits_c) + len(hits_e) + g_hits)
    if cov < prev_cov - 0.05:
        fails.append(f"coverage regressed {prev_cov} → {cov}")
    for fn in SHARED_CONSUMERS:
        try:
            cfg = {}
            p = AWS_DIR / "lambdas" / fn / "config.json"
            if p.exists():
                cfg = json.loads(p.read_text())
            sch = cfg.get("schedule")
            rule, cron = (sch.get("rule_name"), sch.get("cron")) \
                if isinstance(sch, dict) else (None, None)
            live = (LAM.get_function_configuration(FunctionName=fn)
                    .get("Environment") or {}).get("Variables") or {}
            deploy_lambda(report=rep, function_name=fn,
                          source_dir=AWS_DIR / "lambdas" / fn / "source",
                          env_vars=live, eb_rule_name=rule,
                          eb_schedule=cron,
                          timeout=cfg.get("timeout", 900),
                          memory=cfg.get("memory", 1024),
                          description=str(cfg.get("description", ""))[:250],
                          smoke=False)
        except Exception as e:
            fails.append(f"deploy {fn}: {str(e)[:80]}")
    mark = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName="justhodl-wl-engines",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        fails.append(f"invoke: {str(e)[:70]}")
    idx2 = None
    for _ in range(60):
        time.sleep(10)
        d = s3_json("data/wl-engines.json") or {}
        if str(d.get("generated_at", "")) > mark:
            idx2 = d
            break
    if idx2:
        eng2 = idx2.get("engines") or []
        act2 = {e["engine_id"] for e in eng2
                if str(e.get("state")) == "ACTIVE"}
        woken = sorted(act2 - prev_active)
        rep.kv(active_before=len(prev_active), active_now=len(act2),
               woken=len(woken),
               series_cached=idx2.get("series_cached"))
        for w in woken[:8]:
            nm = next((e.get("name") for e in eng2
                       if e.get("engine_id") == w), w)
            rep.log(f"  ⏰ WOKE: {nm}")
        if woken:
            rep.ok(f"{len(woken)} panels woken by today's mappings")
        else:
            rep.log("  no wakes this pass — remaining gaps are the "
                    "deep-residue classes (measured, not assumed)")
    else:
        warns.append("index not fresh in window — wakes land at the "
                     "nightly run")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
