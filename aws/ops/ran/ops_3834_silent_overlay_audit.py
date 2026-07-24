"""
ops_3834 — FLEET AUDIT: which overlays are silently multiplying by 1.0?

WHY. ops 3832 found that master-ranker's nowcast AND liquidity regime tilts had
been no-ops on ~24 of 25 ranked names — very likely since they were built —
because _ticker_sector returned None. ops 3833 then found justhodl-risk-regime
had NO SCHEDULE at all and had only ever run manually; had its feed aged past
max_age_h the whole RORO overlay would have stood down fleet-wide with no error.

Both were INVISIBLE. A multiplier stuck at 1.0 and a fresh-by-luck feed produce
perfectly plausible output. Nothing in the stack complains.

So: measure the scale before building anything permanent (audit-first). This ops
writes no engine code. It walks every data/*.json artifact, finds row arrays
carrying *_mult / *_scalar / *_multiplier fields, and reports what fraction are
actually doing anything.

VERDICTS
  SILENT      — field present on rows, 0% ever != 1.0  -> almost certainly dead
  NEAR_SILENT — <10% active                            -> suspect, worth a look
  SPARSE      — <40% active                            -> may be legitimate
  ACTIVE      — >=40% active
A SILENT overlay is not proof of a bug (a neutral regime legitimately yields 1.0
everywhere — RORO at score 5.4 is exactly that), so this RANKS suspects for
human judgement rather than declaring failures.
"""
import json
import re
import sys
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
PAT = re.compile(r"^[a-z0-9_]+(_mult|_multiplier|_scalar)$")
MAX_BYTES = 4_000_000
MAX_ARTIFACTS = 400


def row_arrays(doc, depth=0):
    """Yield (path, list-of-dicts) for every plausible row collection."""
    if depth > 3:
        return
    if isinstance(doc, dict):
        for k, v in doc.items():
            if isinstance(v, list) and len(v) >= 5 and isinstance(v[0], dict):
                yield k, v
            elif isinstance(v, (dict, list)):
                for p, a in row_arrays(v, depth + 1):
                    yield f"{k}.{p}", a
    elif isinstance(doc, list):
        for i, v in enumerate(doc[:3]):
            for p, a in row_arrays(v, depth + 1):
                yield p, a


def main():
    with report("3834_silent_overlay_audit") as rep:
        rep.heading("ops 3834 — fleet audit: silently-neutral overlays")

        rep.section("1. Enumerate artifacts")
        keys, tok = [], None
        while True:
            kw = {"Bucket": BUCKET, "Prefix": "data/", "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            r = s3.list_objects_v2(**kw)
            for o in r.get("Contents", []):
                if o["Key"].endswith(".json") and o["Size"] <= MAX_BYTES:
                    keys.append(o["Key"])
            tok = r.get("NextContinuationToken")
            if not tok or len(keys) >= MAX_ARTIFACTS:
                break
        rep.ok(f"  {len(keys)} artifacts under {MAX_BYTES//1_000_000}MB")

        rep.section("2. Scan for overlay fields")
        findings, scanned, errors, stale_reports = [], 0, 0, []
        for k in keys:
            try:
                doc = json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
            except Exception:
                errors += 1
                continue
            scanned += 1
            # single pass: harvest feed_freshness here rather than re-reading
            # every artifact a second time (the double-load made this ops run
            # for 20+ minutes on a bucket this size)
            fh = doc.get("feed_freshness") if isinstance(doc, dict) else None
            if isinstance(fh, list):
                bad = [x for x in fh if isinstance(x, dict)
                       and (x.get("stale") or x.get("missing"))]
                if bad:
                    stale_reports.append((k.replace("data/", ""), bad))
            for path, arr in row_arrays(doc):
                fields = set()
                for row in arr[:40]:
                    fields |= {f for f in row if PAT.match(str(f))}
                for f in fields:
                    vals = [row.get(f) for row in arr]
                    present = [v for v in vals if isinstance(v, (int, float))]
                    if len(present) < 5:
                        continue
                    active = [v for v in present if v != 1.0]
                    pct = 100.0 * len(active) / len(present)
                    findings.append({
                        "artifact": k.replace("data/", ""), "path": path,
                        "field": f, "n_rows": len(vals), "n_numeric": len(present),
                        "n_active": len(active), "pct_active": round(pct, 1),
                        "verdict": ("SILENT" if pct == 0 else
                                    "NEAR_SILENT" if pct < 10 else
                                    "SPARSE" if pct < 40 else "ACTIVE"),
                    })
        rep.ok(f"  scanned {scanned} artifacts ({errors} unreadable) · "
               f"{len(findings)} overlay fields found")

        rep.section("3. SILENT — present on every row, never once != 1.0")
        sil = sorted([f for f in findings if f["verdict"] == "SILENT"],
                     key=lambda x: -x["n_numeric"])
        for f in sil[:25]:
            rep.warn(f"  {f['artifact']:<34} {f['path']}.{f['field']:<26} "
                     f"0/{f['n_numeric']} active")
        if not sil:
            rep.ok("  none")

        rep.section("4. NEAR_SILENT — <10% active")
        ns = sorted([f for f in findings if f["verdict"] == "NEAR_SILENT"],
                    key=lambda x: x["pct_active"])
        for f in ns[:20]:
            rep.warn(f"  {f['artifact']:<34} {f['path']}.{f['field']:<26} "
                     f"{f['n_active']}/{f['n_numeric']} = {f['pct_active']}%")
        if not ns:
            rep.ok("  none")

        rep.section("5. Healthy overlays (context — what normal looks like)")
        act = sorted([f for f in findings if f["verdict"] == "ACTIVE"],
                     key=lambda x: -x["pct_active"])[:10]
        for f in act:
            rep.log(f"  {f['artifact']:<34} {f['path']}.{f['field']:<26} "
                    f"{f['pct_active']}%")

        rep.section("6. Feeds excluded by staleness (where published)")
        n_stale = 0
        for name, bad in stale_reports:
            n_stale += len(bad)
            rep.warn(f"  {name}: {len(bad)} feed(s) dropped -> "
                     f"{[b.get('key','?').replace('data/','') for b in bad][:8]}")
        if not n_stale:
            rep.ok("  no published feed_freshness block reports drops")

        counts = {v: sum(1 for f in findings if f["verdict"] == v)
                  for v in ("SILENT", "NEAR_SILENT", "SPARSE", "ACTIVE")}
        rep.kv(artifacts_scanned=scanned, overlay_fields=len(findings),
               **{k.lower(): v for k, v in counts.items()},
               stale_feed_drops=n_stale)
        # The audit must FAIL when the audit itself is broken — otherwise an
        # empty scan reads as "fleet is clean", which is the same silent-success
        # failure mode this ops exists to hunt.
        if scanned == 0:
            rep.fail("scanned 0 artifacts — audit could not run")
            sys.exit(1)
        if errors > scanned:
            rep.fail(f"more unreadable ({errors}) than scanned ({scanned})")
            sys.exit(1)
        if not findings:
            rep.fail("0 overlay fields found across the whole fleet — the matcher "
                     "is broken, not the fleet (15 engines carry *_mult by grep)")
            sys.exit(1)
        rep.ok("AUDIT COMPLETE — suspects ranked for judgement, nothing auto-changed")


if __name__ == "__main__":
    main()
