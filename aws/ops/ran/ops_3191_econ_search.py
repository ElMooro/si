"""ops 3191 — ECONOMICS long tail: name-driven DBnomics search resolver.

3190's family-ladder guess measured ZERO overlap with the real residue —
the 383 leftover ECONOMICS tiles span 174 micro-families (CA, INTR, IPYY,
MPMI, RSYY, GFCF, LEI...), ~2 countries each. Hand-curating that shape is
wrong. The right tool is the one the platform already trusts for FRED:
a SEARCH resolver — but against DBnomics (IMF/OECD/BIS/ECB/Eurostat/WB),
driven by the HUMAN NAMES ops 3183 put in data/symbol-dictionary.json
("Japan — Current Account" beats decoding 'JPCA').

Every candidate is probed with a real fetch; winners land in the map AND
the `curated` carry-forward section (3190 convention) so they survive every
rebuild. Budget-capped: whatever the window doesn't reach stays honestly
unmapped and is counted.
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
BUDGET_S = 640
MIN_POINTS = 40
ECON_RE = re.compile(r"^ECONOMICS:([A-Z]{2})([A-Z0-9]+)$")


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


def dict_names(doc):
    """symbol → human name, defensive against dictionary shapes."""
    if not isinstance(doc, dict):
        return {}
    body = doc.get("symbols") or doc.get("dictionary") or doc
    out = {}
    if isinstance(body, dict):
        for k, v in body.items():
            if isinstance(v, str):
                out[k.upper()] = v
            elif isinstance(v, dict):
                n = v.get("name") or v.get("title") or v.get("desc")
                if n:
                    out[k.upper()] = str(n)
    return out


with report("3191_econ_search") as rep:
    fails, warns = [], []
    rep.heading("ops 3191 — ECONOMICS long tail via DBnomics name-search "
                "(probe-gated)")

    wl = s3_json("data/tv-watchlists.json") or {}
    lists = [l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    uniq = sorted({s.upper() for l in lists for s in (l.get("symbols") or [])})
    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    cache = prev.get("search_cache") or {}
    prev_cov = float(prev.get("coverage_pct") or 0)
    names = dict_names(s3_json("data/symbol-dictionary.json") or {})
    econ_left = [s for s in uniq if s not in mapped
                 and s.startswith("ECONOMICS:")]
    named = sum(1 for s in econ_left if s in names)
    rep.section("1. Inputs")
    rep.kv(economics_unmapped=len(econ_left), with_human_name=named)
    searcher = SS.dbn_search_factory(cache)

    rep.section("2. Search → probe (budgeted)")

    def resolve(sym):
        m = ECON_RE.match(sym)
        i2 = m.group(1) if m else ""
        nm = names.get(sym) or ""
        q = re.sub(r"\s*[—-]\s*", " ", nm).strip()
        if not q:
            return sym, None, 0, "no_name"
        for sid in searcher(q, country=q.split(" ")[0]):
            try:
                ser = SS.fetch("DBNOMICS", sid)
            except Exception:
                ser = {}
            if len(ser) >= MIN_POINTS:
                return sym, {"source": "DBNOMICS", "id": sid,
                             "confidence": 0.6,
                             "note": f"dbn-search: {nm[:60]}"}, len(ser), "hit"
        return sym, None, 0, "dry"

    t0, hits, dry, skipped = time.time(), {}, [], 0
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(resolve, s): s for s in econ_left}
        for f in as_completed(futs):
            if time.time() - t0 > BUDGET_S:
                skipped += 1
                continue
            sym, entry, n, verdict = f.result()
            if entry:
                hits[sym] = (entry, n)
            elif verdict == "dry":
                dry.append(sym)
    for sym, (entry, _n) in hits.items():
        mapped[sym] = entry
        curated[sym] = entry
    fam_h = Counter(ECON_RE.match(s).group(2) for s in hits
                    if ECON_RE.match(s))
    for fmly, n in fam_h.most_common(12):
        rep.log(f"  {fmly:<10} hit {n}")
    for sym, (entry, n) in sorted(hits.items(),
                                  key=lambda kv: -kv[1][1])[:6]:
        rep.log(f"    ✓ {sym} → {entry['id']}  ({n} pts)")
    rep.kv(searched=len(econ_left) - skipped, survivors=len(hits),
           dry=len(dry), budget_skipped=skipped)
    if econ_left and not hits:
        warns.append("search resolver found nothing — check API response "
                     "shape in the log before a second pass")

    rep.section("3. Write map + redeploy + kick fleet")
    cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
    S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                  Body=json.dumps({
                      "generated_at": datetime.now(timezone.utc).isoformat(),
                      "coverage_pct": cov, "map": mapped,
                      "curated": curated, "search_cache": cache,
                      "note": ("ops 3191: +dbn-search resolved ECONOMICS; "
                               "`curated` MERGES FIRST on every rebuild")}),
                  ContentType="application/json")
    rep.kv(coverage_before=prev_cov, coverage_now=cov,
           curated_total=len(curated))
    if cov < prev_cov - 0.05:
        fails.append(f"coverage regressed {prev_cov} → {cov}")
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
