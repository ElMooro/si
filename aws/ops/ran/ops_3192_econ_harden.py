"""ops 3192 — econ hardening: purge the poison, learn from the hits,
retire the licensed.

3191's free-text resolver shipped 11 entries — and at least four are
WRONG-COUNTRY or WRONG-CONCEPT (BRCLI/CNCLI → the Australia CLI series;
MA/TN "INTR" → BIS debt-securities). Real-data doctrine says a wrong
series is worse than a missing one. Three moves:

  1. PURGE: every dbn-search curated entry is re-audited — the symbol's
     country token must appear in the series code AND the dataset must
     match the family concept. Failures are deleted, named, and counted.
  2. LEARN: the hits that WERE right revealed exact dataset shapes.
     They are now TEMPLATES in ECON_DBN (CLI → OECD KEI, lending rates →
     ECB MIR / IMF IFS, permits → Eurostat STS) and this ops probes them
     across every country in those families. Templates cannot cross
     countries by construction.
  3. RETIRE: MPMI / LEI class tiles are S&P Global / Conference Board
     LICENSED data — no free primary exists. Tagged `licensed_econ` in
     the map meta so every future census counts them as decisions, not
     gaps.

Gate: after this ops, ZERO curated dbn entries may fail the country
check — that assertion is the pass/fail line.
"""
import json
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
ECON_RE = re.compile(r"^ECONOMICS:([A-Z]{2})([A-Z0-9]+)$")
MIN_POINTS = 40
FAMILY_CONCEPT = {  # dataset code must contain one of these tokens
    "CLI": ("KEI", "CLI", "MEI"), "BLR": ("MIR", "IFS"),
    "INTR": ("MIR", "IFS"), "BP": ("COBP", "BP", "PERMIT"),
}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


def country_ok(sym, sid):
    m = ECON_RE.match(sym)
    if not m:
        return False
    i2 = m.group(1)
    i3 = SS.ISO2_ISO3.get(i2) or ""
    toks = set(str(sid).upper().replace("-", ".").split(".")) \
        | set(str(sid).upper().split("/"))
    return i2 in toks or (i3 and i3 in toks)


def concept_ok(sym, sid):
    m = ECON_RE.match(sym)
    fam = m.group(2) if m else ""
    kws = FAMILY_CONCEPT.get(fam)
    if not kws:
        return True                      # unknown family — country gate only
    up = str(sid).upper()
    return any(k in up for k in kws)


with report("3192_econ_harden") as rep:
    fails, warns = [], []
    rep.heading("ops 3192 — purge cross-country poison, learn templates, "
                "retire licensed")

    wl = s3_json("data/tv-watchlists.json") or {}
    lists = [l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    uniq = sorted({s.upper() for l in lists for s in (l.get("symbols") or [])})
    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    cache = prev.get("search_cache") or {}
    prev_cov = float(prev.get("coverage_pct") or 0)

    # ── 1. purge ───────────────────────────────────────────────────────
    rep.section("1. Audit + purge 3191 search entries")
    purged = []
    for sym in [s for s, e in list(curated.items())
                if str(e.get("note", "")).startswith("dbn-search")]:
        sid = curated[sym]["id"]
        if not (country_ok(sym, sid) and concept_ok(sym, sid)):
            purged.append((sym, sid))
            curated.pop(sym, None)
            mapped.pop(sym, None)
    for sym, sid in purged:
        rep.log(f"  ✗ PURGED {sym} → {sid[:58]} (country/concept mismatch)")
    rep.kv(audited=len(purged) + sum(
        1 for e in curated.values()
        if str(e.get("note", "")).startswith("dbn-search")),
        purged=len(purged))

    # ── 2. template pass across the learned families ──────────────────
    rep.section("2. Learned-template probe (CLI/BLR/INTR/BP, all countries)")
    todo = [s for s in uniq if s not in mapped and ECON_RE.match(s)
            and ECON_RE.match(s).group(2) in ("CLI", "BLR", "INTR", "BP")]

    def probe_ladder(sym):
        m = ECON_RE.match(sym)
        i2, ind = m.groups()
        i3 = SS.ISO2_ISO3.get(i2) or i2
        for rung, tpl in enumerate(SS.ECON_DBN.get(ind, [])):
            sid = tpl.format(i2=i2, i3=i3)
            try:
                ser = SS.fetch("DBNOMICS", sid)
            except Exception:
                ser = {}
            if len(ser) >= MIN_POINTS and country_ok(sym, sid):
                return sym, {"source": "DBNOMICS", "id": sid,
                             "confidence": 0.75,
                             "note": f"econ template {ind} rung{rung} "
                                     "(probe-proven)"}, len(ser)
        return sym, None, 0

    hits, dry, t0 = {}, [], time.time()
    with ThreadPoolExecutor(max_workers=6) as ex:
        for f in as_completed([ex.submit(probe_ladder, s) for s in todo]):
            if time.time() - t0 > 300:
                break
            sym, entry, n = f.result()
            (hits.__setitem__(sym, (entry, n)) if entry else dry.append(sym))
    for sym, (entry, _n) in hits.items():
        mapped[sym] = entry
        curated[sym] = entry
    fam_h = Counter(ECON_RE.match(s).group(2) for s in hits)
    fam_todo = Counter(ECON_RE.match(s).group(2) for s in todo)
    for fmly in ("CLI", "BLR", "INTR", "BP"):
        rep.log(f"  {fmly:<6} probed {fam_todo.get(fmly, 0):>3}"
                f"  hit {fam_h.get(fmly, 0):>3}")
    for sym, (entry, n) in sorted(hits.items(),
                                  key=lambda kv: -kv[1][1])[:5]:
        rep.log(f"    ✓ {sym} → {entry['id'][:58]}  ({n} pts)")
    rep.kv(template_probed=len(todo), template_hits=len(hits))

    # ── 3. retire licensed families ────────────────────────────────────
    rep.section("3. Licensed retirement (MPMI/LEI class)")
    licensed = sorted(
        s for s in uniq if s not in mapped and ECON_RE.match(s)
        and ECON_RE.match(s).group(2) in SS.ECON_LICENSED)
    rep.kv(licensed_econ=len(licensed))
    for s in licensed[:6]:
        rep.log(f"  retired: {s} (S&P Global / Conference Board — "
                "no free primary)")

    # ── 4. write, verify the assertion, redeploy, kick ────────────────
    rep.section("4. Write + hard assertion")
    cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
    bad = [s for s, e in curated.items()
           if str(e.get("note", "")).startswith(("dbn-search",
                                                 "econ template"))
           and not country_ok(s, e["id"])]
    if bad:
        fails.append(f"{len(bad)} curated econ entries STILL fail the "
                     f"country check: {bad[:3]}")
    S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                  Body=json.dumps({
                      "generated_at": datetime.now(timezone.utc).isoformat(),
                      "coverage_pct": cov, "map": mapped,
                      "curated": curated, "search_cache": cache,
                      "licensed_econ": licensed,
                      "retired": prev.get("retired") or {},
                      "note": ("ops 3192: poison purged, templates learned, "
                               "licensed_econ tagged; curated MERGES FIRST "
                               "on rebuild")}),
                  ContentType="application/json")
    rep.kv(coverage_before=prev_cov, coverage_now=cov,
           curated_total=len(curated))
    if cov < prev_cov - 0.4:
        fails.append(f"coverage regressed beyond the purge {prev_cov}→{cov}")
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
    try:
        LAM.invoke(FunctionName="justhodl-wl-engines",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        warns.append(f"fleet kick: {str(e)[:70]}")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
