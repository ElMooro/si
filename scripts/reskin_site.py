#!/usr/bin/env python3
"""Deploy-time reskin: legacy neon/slate palette -> Amber Terminal (single accent).

Runs in pages.yml against _site/ AFTER the homepage bake. Rewrites exact legacy
color literals (census-derived, 2026-07-06) in every built page + chrome JS.
Sources are never touched — rollback is removing this step. index.html is
skipped (natively Amber); anything under/with 'screener' is skipped (PROTECTED).
Semantics preserved: greens stay green (calmed), reds stay red (calmed);
cyan/purple/orange accents consolidate into amber per the single-accent system.
Never fails a deploy.
"""
import re
import sys
from pathlib import Path

HEX = {
    # structure
    "#0a0e14": "#0b0906", "#0f131a": "#0b0906", "#0a0a0a": "#0b0906",
    "#11161f": "#141008", "#1c2433": "#2a2318",
    # text scale
    "#e6eaf2": "#e8e2d4", "#a8b3c7": "#b5ad99", "#6f7b91": "#8a836f",
    # primary accents -> amber (single-accent consolidation)
    "#00d4ff": "#f5b93e", "#22d3ee": "#f5b93e", "#46c8ff": "#f5b93e",
    "#ff7a18": "#f5b93e", "#3b82f6": "#d99a2b", "#a78bfa": "#d99a2b",
    # amber-family normalization
    "#fbbf24": "#f5b93e", "#facc15": "#f5b93e", "#ffd266": "#ffd479",
    "#f59e0b": "#d99a2b",
    # semantic greens (calmed, meaning preserved)
    "#26ffaf": "#6fce8a", "#00ff88": "#6fce8a", "#3fd68c": "#6fce8a",
    "#22c55e": "#6fce8a",
    # semantic reds/pinks (calmed)
    "#ff5577": "#e0685f", "#ef4444": "#e0685f",
}
RGBA = {  # prefix -> prefix (alpha term untouched)
    r"rgba\(\s*0\s*,\s*212\s*,\s*255": "rgba(245,185,62",
    r"rgba\(\s*34\s*,\s*211\s*,\s*238": "rgba(245,185,62",
    r"rgba\(\s*167\s*,\s*139\s*,\s*250": "rgba(217,154,43",
    r"rgba\(\s*38\s*,\s*255\s*,\s*175": "rgba(111,206,138",
    r"rgba\(\s*0\s*,\s*230\s*,\s*118": "rgba(111,206,138",
    r"rgba\(\s*255\s*,\s*85\s*,\s*119": "rgba(224,104,95",
    r"rgba\(\s*239\s*,\s*68\s*,\s*68": "rgba(224,104,95",
}
CHROME_JS = {"jh-nav-drawer.js", "jh-page-ai.js", "wss-client.js"}


def reskin_text(s):
    n = 0
    for old, new in HEX.items():
        s, k = re.subn(re.escape(old), new, s, flags=re.I)
        n += k
    for pat, new in RGBA.items():
        s, k = re.subn(pat, new, s, flags=re.I)
        n += k
    return s, n


def main(root):
    root = Path(root)
    targets = [p for p in root.glob("*.html") if p.name != "index.html"]
    targets += [root / j for j in CHROME_JS if (root / j).exists()]
    total_f = total_r = 0
    for p in targets:
        if "screener" in str(p).lower():
            continue
        try:
            s = p.read_text(encoding="utf-8", errors="replace")
            s2, n = reskin_text(s)
            if n:
                p.write_text(s2, encoding="utf-8")
                total_f += 1
                total_r += n
        except Exception as e:
            print(f"reskin WARN {p.name}: {e}")
    print(f"reskin: {total_r} color literals rewritten across {total_f} files "
          f"(index + screener untouched)")


if __name__ == "__main__":
    try:
        main(sys.argv[1] if len(sys.argv) > 1 else "_site")
    except Exception as e:
        print("reskin WARN (deploy proceeds unskinned):", e)
    sys.exit(0)
