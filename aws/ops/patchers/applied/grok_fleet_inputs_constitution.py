#!/usr/bin/env python3
from pathlib import Path
TARGET = Path(__file__).resolve().parents[2] / "lambdas/justhodl-ai/source/fleet_inputs.py"

def main():
    t = TARGET.read_text()
    if "CONSTITUTION_FEED" in t:
        print("already patched")
        return 0
    a = '    r"probability|confidence|risk|phase|decision|allow|veto|rating)$",'
    b = '    r"probability|confidence|risk|phase|decision|allow|veto|rating|"\n    r"profile|themes|rules|emphasis)$",'
    if a not in t:
        raise SystemExit("signal regex miss")
    t = t.replace(a, b, 1)
    a = 'PRIVATE_TOKENS = ("brain", "note", "journal", "portfolio", "credential", "secret")\n'
    b = a + 'CONSTITUTION_FEED = "data/brain-constitution.json"\n'
    if a not in t:
        raise SystemExit("tokens miss")
    t = t.replace(a, b, 1)
    a = '''def _is_private(row: Dict[str, Any]) -> bool:
    feed = str(row.get("feed") or "").lstrip("/")
    if ((_canonical_private_source is not None and _canonical_private_source(feed))'''
    b = '''def _is_private(row: Dict[str, Any]) -> bool:
    feed = str(row.get("feed") or "").lstrip("/")
    if feed == CONSTITUTION_FEED:
        return False
    if ((_canonical_private_source is not None and _canonical_private_source(feed))'''
    if a not in t:
        raise SystemExit("is_private miss")
    t = t.replace(a, b, 1)
    a = '''        item["private"] = item["private"] or _is_private(row)

    feeds: List[Dict[str, Any]] = []'''
    b = '''        item["private"] = item["private"] or _is_private(row)

    if CONSTITUTION_FEED not in by_feed:
        by_feed[CONSTITUTION_FEED] = {
            "feed": CONSTITUTION_FEED,
            "engines": ["brain-sync"],
            "pages": ["brain"],
            "titles": ["Brain constitution"],
            "schema_versions": ["1.0"],
            "declared_classes": ["WIRED"],
            "private": False,
        }

    feeds: List[Dict[str, Any]] = []'''
    if a not in t:
        raise SystemExit("by_feed miss")
    t = t.replace(a, b, 1)
    TARGET.write_text(t)
    print("patched", TARGET)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
