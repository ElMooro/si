"""ops_4095 — DIAGNOSTIC: is the TradingView mirror actually complete?

Khalid: tradingview.html shows nothing when he clicks a category, and
both it and data-census.html should carry EVERY watchlist, EVERY
indicator, and every note inside each one — the reason the extension was
built in the first place.

justhodl-tv-workbench already claims to do exactly that. So before
building anything, three measurements:

  A. MIRROR COMPLETENESS — does data/tv-workbench.json really hold all
     491 watchlists and all 10,319 symbols with their notes, or only the
     vault subset? If it is complete, the fix is a page problem, not an
     engine problem, and building a second mirror would be duplication.
  B. THE CATEGORY BUG — tradingview.html filters rows on r.category and
     builds its chips from by_category_counts. If those two disagree on
     spelling or case, every chip yields an empty table. Compare them.
  C. THE CFTC FETCH — ops 4094 still reports zero cftc values. Dump the
     actual vault row for a known COT symbol to separate "never admitted"
     from "admitted but failed to resolve". No more guessing between them.
"""
import json, sys
from collections import Counter
from pathlib import Path
import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

def gj(k, d=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception as e:
        print(f"miss {k}: {str(e)[:70]}"); return d

def main():
    with report("4095_mirror_audit") as rep:
        rep.heading("ops 4095 — mirror completeness · category bug · cftc")

        # ── A. mirror completeness ──
        rep.section("A. is the mirror complete?")
        wl = gj("data/tv-watchlists.json", {}) or {}
        lists = wl.get("watchlists") or wl.get("lists") or []
        truth_syms = {str(x).strip() for l in lists for x in (l.get("symbols") or []) if str(x).strip()}
        rep.log(f"  TRUTH (import): {len(lists)} watchlists · {len(truth_syms)} unique symbols")

        wb = gj("data/tv-workbench.json", {}) or {}
        wb_lists = wb.get("watchlists") or []
        wb_syms = wb.get("symbols") or {}
        with_notes = sum(1 for v in wb_syms.values() if (v.get("n_notes") or 0) > 0)
        with_value = sum(1 for v in wb_syms.values() if v.get("value") is not None)
        total_notes = sum((v.get("n_notes") or 0) for v in wb_syms.values())
        rep.log(f"  MIRROR (tv-workbench.json):")
        rep.log(f"    watchlists      : {len(wb_lists)}")
        rep.log(f"    symbols         : {len(wb_syms)}")
        rep.log(f"    with >=1 note   : {with_notes}   (total notes {total_notes})")
        rep.log(f"    with a value    : {with_value}")
        rep.log(f"    generated_at    : {wb.get('generated_at')}")
        missing = truth_syms - set(wb_syms)
        rep.log(f"    symbols MISSING from the mirror: {len(missing)}")
        if missing:
            rep.log(f"    sample missing: {sorted(missing)[:10]}")
        rep.kv(truth_lists=len(lists), truth_symbols=len(truth_syms),
               mirror_lists=len(wb_lists), mirror_symbols=len(wb_syms),
               mirror_with_notes=with_notes, mirror_missing=len(missing))
        if wb_lists:
            rep.log("  biggest lists in the mirror:")
            for w in sorted(wb_lists, key=lambda x: -(x.get("n") or 0))[:8]:
                rep.log(f"    {w.get('n'):5d}  {str(w.get('name'))[:52]}")
        rep.log("  → if lists/symbols match TRUTH, the mirror is FINE and the")
        rep.log("    problem is entirely in the pages, not the engines.")

        # ── B. the category bug ──
        rep.section("B. why does clicking a category show nothing?")
        v = gj("data/tradingview.json", {}) or {}
        rows = v.get("symbols") or []
        bcc = v.get("by_category_counts") or {}
        actual = Counter(str(r.get("category")) for r in rows)
        rep.log(f"  vault rows: {len(rows)}")
        rep.log(f"  by_category_counts keys (chips the page renders):")
        for k, n in sorted(bcc.items(), key=lambda x: -x[1]):
            rep.log(f"    chip '{k}' claims {n}  ·  rows actually matching: {actual.get(k,0)}")
        orphan = [k for k in bcc if actual.get(k, 0) == 0]
        unchipped = [k for k in actual if k not in bcc]
        rep.log(f"  chips that match ZERO rows : {orphan}")
        rep.log(f"  categories with NO chip    : {unchipped[:12]}")
        rep.kv(chips=len(bcc), orphan_chips=len(orphan), unchipped=len(unchipped))
        if orphan:
            rep.log("  ✗ CONFIRMED: those chips are unclickable-empty — the page")
            rep.log("    filters on r.category but the counts were built from a")
            rep.log("    different field or a pre-filter population.")

        # ── C. the cftc fetch ──
        rep.section("C. cftc: admitted, or failing to resolve?")
        sa = gj("data/symbol-aliases.json", {}) or {}
        cot_aliases = {k: r for k, r in (sa.get("detail") or {}).items()
                       if r.get("route") == "cot-verified"}
        rep.log(f"  cot aliases generated: {len(cot_aliases)}")
        idx = {r.get("symbol"): r for r in rows}
        present = [k for k in cot_aliases if k in idx]
        rep.log(f"  ...present as vault rows: {len(present)}")
        if not present:
            rep.log("  ✗ NOT ADMITTED — the expansion budget has not reached")
            rep.log("    them yet. Nothing wrong with the adapter; it has never")
            rep.log("    been asked to resolve anything.")
        for k in present[:6]:
            r = idx[k]
            rep.log(f"    {k[:26]:26} status={r.get('status')} "
                    f"value={r.get('value')} via={r.get('resolved_via')} "
                    f"note={str(r.get('resolution_note'))[:40]}")
        rep.kv(cot_aliases=len(cot_aliases), cot_in_vault=len(present))

        rep.section("VERDICT")
        rep.log(f"  mirror {len(wb_syms)}/{len(truth_syms)} symbols, "
                f"{len(wb_lists)}/{len(lists)} lists")
        rep.log(f"  orphan chips {len(orphan)} · cot admitted {len(present)}"
                f"/{len(cot_aliases)}")
        rep.log("  DIAGNOSTIC ONLY — no code written.")

if __name__ == "__main__":
    main()
