"""ops 3137 — Compass follow-ups: zip-injection proof + evidence-vocab forensics.

Three questions from the 3135/3136 close:

  A. Does the patched build_zip now bundle aws/shared/*.py (local wins)?
     Proven here by building a zip for a shim-LESS importer and asserting
     _sentry_lite.py is inside. Fleet-wide fix — every future helper deploy
     inherits it.

  B. Why did the magdist tier match 0/7 cards? Forensics against the LIVE
     feeds: stack vocabulary vs each conviction setup's three candidate
     sets (engine names / families / engine-map expansion), best Jaccard
     per setup, and which scorecard tokens actually hit at Tier B.
     Output = the exact mapping table ops 3138 needs.

  C. RORO chip vanished from the regime strip — live risk-regime.json
     field dump confirms the real label key (source says `risk_regime` +
     `risk_regime_score`) before 3138 edits fuse_regime.

READ-ONLY on AWS (S3 gets + local zip build). No deploys.
"""

import io
import json
import sys
import zipfile
from collections import Counter
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import build_zip

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
HERE = Path(__file__).resolve().parent
AWS_DIR = HERE.parents[1]

S3 = boto3.client("s3", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


def toks(it):
    return {str(x).strip().lower() for x in it if x}


with report("3137_compass_forensics") as rep:
    fails, warns = [], []
    rep.heading("ops 3137 — zip-injection proof + evidence-vocab forensics")

    # ── A. build_zip shared-injection proof ────────────────────────────
    rep.section("A. build_zip bundles aws/shared (fleet fix proof)")
    probe_fn = "justhodl-ai-chat"  # imports _sentry_lite, has NO local copy
    src = AWS_DIR / "lambdas" / probe_fn / "source"
    if not (src / "_sentry_lite.py").exists():
        zb = build_zip(src)
        names = set(zipfile.ZipFile(io.BytesIO(zb)).namelist())
        if "_sentry_lite.py" in names:
            rep.ok(f"{probe_fn} zip now contains _sentry_lite.py "
                   f"({len(names)} files, {len(zb)} bytes)")
        else:
            fails.append("patched build_zip did NOT inject _sentry_lite.py")
        pyc = [n for n in names if "__pycache__" in n or n.endswith(".pyc")]
        if pyc:
            fails.append(f"zip polluted with pycache: {pyc[:3]}")
        # local-wins property: compass dir HAS a local shim — assert only 1
        czb = build_zip(AWS_DIR / "lambdas" / "justhodl-alpha-compass" / "source")
        cnames = zipfile.ZipFile(io.BytesIO(czb)).namelist()
        n_shim = sum(1 for n in cnames if n == "_sentry_lite.py")
        if n_shim == 1:
            rep.ok("local-copy-wins verified (exactly one shim in compass zip)")
        else:
            fails.append(f"compass zip has {n_shim} shim entries")
    else:
        warns.append(f"{probe_fn} unexpectedly has a local shim — pick "
                     "another probe next time")

    # ── B. magdist / scorecard vocabulary forensics ────────────────────
    rep.section("B. Evidence-vocab forensics (live feeds)")
    conviction = s3_json("data/conviction.json")
    magdist = s3_json("data/magnitude-distributions.json")
    scorecard = s3_json("data/signal-scorecard.json")
    emap = s3_json("data/engine-signal-map.json")

    stacks = magdist.get("stacks") or []
    stack_vocab = Counter()
    horizons = Counter()
    for s in stacks:
        horizons[s.get("horizon_days")] += 1
        for sig in s.get("signals") or []:
            stack_vocab[str(sig).strip().lower()] += 1
    rep.kv(n_stacks=len(stacks),
           horizons=json.dumps(dict(horizons)),
           vocab_size=len(stack_vocab))
    rep.log("stack vocab (top 25): " + ", ".join(
        f"{t}×{c}" for t, c in stack_vocab.most_common(25)))

    sc_rows = scorecard.get("scorecard") or []
    sc_keys = toks(r.get("signal_type") for r in sc_rows
                   if isinstance(r, dict))
    rep.kv(scorecard_signal_types=len(sc_keys))
    rep.log("scorecard keys: " + ", ".join(sorted(sc_keys)[:30]))

    by_family = emap.get("by_family") or {}
    rep.log(f"engine-map families ({len(by_family)}): "
            + ", ".join(sorted(by_family)[:20]))

    conv_families = set()
    for s in conviction.get("setups") or []:
        for e in s.get("contributing_engines") or []:
            if e.get("family"):
                conv_families.add(str(e["family"]).lower())
    overlap_fam = conv_families & toks(by_family.keys())
    rep.kv(conviction_families=len(conv_families),
           families_in_engine_map=len(overlap_fam))
    if not overlap_fam:
        rep.log("→ conviction families are DISJOINT from engine-map "
                "families — mapped-expansion set is always empty")

    rep.section("B2. Per-setup matching replay")
    for s in conviction.get("setups") or []:
        engines = s.get("contributing_engines") or []
        names = toks(e.get("engine") for e in engines)
        fams = toks(e.get("family") for e in engines)
        mapped = set()
        for f in fams:
            mapped |= toks(by_family.get(f) or [])
        best_j, best_sig = 0.0, None
        for cand in (mapped, names, fams):
            if not cand:
                continue
            for st in stacks:
                mem = toks(st.get("signals") or [])
                if not mem:
                    continue
                inter = cand & mem
                if not inter:
                    continue
                j = len(inter) / len(cand | mem)
                if j > best_j:
                    best_j, best_sig = j, sorted(inter)[:4]
        sc_hits = sorted((names | fams | mapped) & sc_keys)
        rep.log(f"· {s.get('subject')}: names={sorted(names)} "
                f"fams={sorted(fams)} mapped={len(mapped)} | "
                f"best_stack_jaccard={round(best_j, 3)} via={best_sig} | "
                f"scorecard_hits={sc_hits}")

    # ── C. RORO + sizer live field dump ────────────────────────────────
    rep.section("C. RORO + sizer live fields (for 3138 fuse_regime edit)")
    roro = s3_json("data/risk-regime.json")
    rep.log(f"risk_regime={roro.get('risk_regime')!r} "
            f"score={roro.get('risk_regime_score')!r}")
    post = roro.get("posture")
    rep.log(f"posture type={type(post).__name__} "
            f"keys={sorted(post)[:10] if isinstance(post, dict) else post}")
    sizer = s3_json("portfolio/sizer-v2.json")
    rep.log(f"sizer risk_multiplier={sizer.get('risk_multiplier')!r} "
            f"decisive_call={sizer.get('decisive_call')!r}")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    sys.exit(0 if not fails else 1)
