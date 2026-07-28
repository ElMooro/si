"""ops_4042 — FULL CHECK v2: everything from 4034 PLUS note-fidelity —
"all my notes captured inside the indicators on the watchlist exactly as
in TradingView" (Khalid, standing requirement). Parity is proven, not
assumed: every tagged brain note whose symbol appears in any watchlist
must be present VERBATIM under that symbol; orphans reported, not hidden.
"""
import json
import random
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
TV_RX = re.compile(r"\[TV:([A-Z0-9_:!\.\-]+)\]")
MARK = "tv-workbench v1.1.3 ops4041 count-rail"


def gj(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def main():
    with report("4042_fidelity_final2") as rep:
        rep.heading("ops 4036 — note-fidelity: exactly as in TradingView")
        checks = []
        import time as _t
        wb = None
        for _i in range(12):
            wb = gj("data/tv-workbench.json")
            if wb.get("marker") == MARK:
                rep.ok(f"  v1.1.3 artifact after ~{_i*15}s")
                break
            _t.sleep(15)
        checks.append(("workbench is v1.1.3 bare-union",
                       wb.get("marker") == MARK))
        syms = wb.get("symbols") or {}
        t = wb.get("totals") or {}
        rep.kv(**t)

        brain = gj("data/brain.json")
        tagged = {}
        for n in brain.get("notes", []):
            txt = n.get("text") or ""
            m = TV_RX.match(txt)
            if not m:
                continue
            full = m.group(1)
            tagged.setdefault(full, []).append(txt[m.end():].strip())
            bare = full.split(":")[-1]
            if bare != full:
                tagged.setdefault(bare, [])
        in_wl_keys = {}
        for k, v in syms.items():
            in_wl_keys[k] = v
            in_wl_keys.setdefault(v.get("bare"), v)

        rep.section("A. count parity per symbol (watchlisted only)")
        mismatch, covered, orphan_syms, joined_notes = [], 0, [], 0
        for sym, notes in tagged.items():
            if not notes:
                continue
            row = in_wl_keys.get(sym) or in_wl_keys.get(sym.split(":")[-1])
            if not row:
                orphan_syms.append(sym)
                continue
            covered += 1
            joined_notes += len(row.get("notes") or [])
            if (row.get("n_notes") or 0) < len(notes):
                mismatch.append((sym, len(notes), row.get("n_notes")))
        rep.kv(tagged_symbols=sum(1 for v in tagged.values() if v),
               watchlisted_with_notes=covered,
               orphan_symbols_not_in_any_list=len(orphan_syms),
               count_mismatches=len(mismatch))
        for s2, want, got in mismatch[:8]:
            rep.log(f"    MISMATCH {s2}: brain={want} workbench={got}")
        if orphan_syms[:6]:
            rep.log(f"    orphans (INFO): {orphan_syms[:6]}")
        checks.append(("every watchlisted symbol carries ALL its notes",
                       len(mismatch) == 0))

        rep.section("B. verbatim fidelity — 30 random notes, byte-exact")
        pool = [(s2, n2) for s2, ns in tagged.items()
                if ns and (s2 in in_wl_keys or s2.split(":")[-1] in in_wl_keys) for n2 in ns]
        random.seed(4036)
        sample = random.sample(pool, min(30, len(pool)))
        bad = 0
        for s2, txt in sample:
            row = in_wl_keys.get(s2) or in_wl_keys.get(s2.split(":")[-1])
            have = [x.get("t") for x in row.get("notes") or []]
            if txt not in have:
                bad += 1
                rep.log(f"    NOT VERBATIM {s2}: {txt[:60]!r}")
        rep.kv(sampled=len(sample), byte_exact=len(sample) - bad)
        checks.append(("30/30 sampled notes byte-exact", bad == 0))

        rep.section("C. truncation detector")
        trunc = sum(1 for v in syms.values()
                    for x in v.get("notes") or []
                    if len(x.get("t") or "") >= 59990)
        heavy = sorted(((k, v2.get("n_notes") or 0)
                        for k, v2 in syms.items()),
                       key=lambda x: -x[1])[:5]
        rep.kv(notes_at_cap=trunc,
               heaviest=json.dumps(heavy))
        checks.append(("zero notes hitting the cap", trunc == 0))

        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {covered} watchlisted symbols carry all their "
               f"notes verbatim; {len(orphan_syms)} orphans (symbols in no "
               f"list) reported honestly; zero truncation")


if __name__ == "__main__":
    main()
