#!/usr/bin/env python3
"""Deploy-time reskin v2 — GENERALIZED hue engine (replaces the literal map).

Why v2: the legacy site spans build eras with 400+ distinct cool-toned color
literals plus 1,000+ rgb()/hsl() forms; enumerating them is unwinnable. This
engine parses EVERY color literal in EVERY format and classifies by HSL:

  cool band (hue 165–300, cyan→blue→purple):
      saturated  -> amber accent tier by lightness (#f5b93e / #d99a2b / #8a6a25)
      desaturated (slate/tinted grays) -> warm structural ramp by lightness
  neon greens  -> calm semantic green (#6fce8a / #3f7d55)
  pinks/reds   -> calm semantic red   (#e0685f / #b04a43)
  amber family -> normalized (#f5b93e / #d99a2b / #ffd479)
  true neutrals (sat<6%) -> untouched

Formats: #rgb #rgba #rrggbb #rrggbbaa, rgb()/rgba() (int+%), hsl()/hsla().
Alpha is always preserved. Idempotent (amber outputs are fixed points).
Scope: recursive _site/**/*.{html,css,js,svg}. Excluded: index.html (native),
service-worker.js, jh-theme.css (token source), any path containing
'screener' (PROTECTED). Sources untouched; artifact-only; never fails deploys.
"""
import colorsys
import re
import sys
from pathlib import Path

WARM_RAMP = [(0.10, "0b0906"), (0.16, "141008"), (0.24, "1a150b"),
             (0.34, "2a2318"), (0.46, "3a3428"), (0.60, "6a6455"),
             (0.75, "8a836f"), (0.86, "b5ad99"), (2.00, "e8e2d4")]
SKIP_NAMES = {"index.html", "service-worker.js", "jh-theme.css"}


def hsl(r, g, b):
    h, l, s = colorsys.rgb_to_hls(r / 255, g / 255, b / 255)
    return h * 360, s, l


def ramp(l):
    for cut, hexv in WARM_RAMP:
        if l < cut:
            return hexv
    return "e8e2d4"


def map_rgb(r, g, b):
    """Return new (r,g,b) hex-string (no '#') or None to leave untouched."""
    h, s, l = hsl(r, g, b)
    if s < 0.06:
        return None                                   # true neutral
    if 165 <= h <= 300:                               # the legacy cool band
        if s >= 0.45:                                 # accent
            return "f5b93e" if l >= 0.55 else ("d99a2b" if l >= 0.35 else "8a6a25")
        return ramp(l)                                # slate / tinted gray
    if 90 <= h < 165 and s > 0.35:                    # greens -> calm semantic
        return "6fce8a" if l >= 0.40 else "3f7d55"
    if (h >= 300 or h < 15) and s > 0.40:             # pinks/reds -> calm semantic
        return "e0685f" if l >= 0.45 else "b04a43"
    if 15 <= h < 65 and s >= 0.50:                    # oranges/ambers normalize
        return "ffd479" if l >= 0.72 else ("f5b93e" if l >= 0.45 else "d99a2b")
    return None


def hex_to_rgb(hs):
    hs = hs.lstrip("#")
    if len(hs) in (3, 4):
        hs = "".join(c * 2 for c in hs[: 3 if len(hs) == 3 else 4])
    return int(hs[0:2], 16), int(hs[2:4], 16), int(hs[4:6], 16), hs[6:8]


RE_HEX = re.compile(r"#([0-9a-fA-F]{8}|[0-9a-fA-F]{6}|[0-9a-fA-F]{4}|[0-9a-fA-F]{3})\b")
RE_FN = re.compile(r"\b(rgba?|hsla?)\(\s*([^)]{3,60})\)")


def sub_hex(m):
    r, g, b, a = hex_to_rgb(m.group(0))
    new = map_rgb(r, g, b)
    return m.group(0) if new is None else "#" + new + a.lower()


def sub_fn(m):
    kind, body = m.group(1).lower(), m.group(2)
    parts = [p.strip() for p in re.split(r"[,/]", body) if p.strip()]
    try:
        if kind.startswith("hsl"):
            h = float(re.sub(r"[^\d.\-]", "", parts[0]))
            s = float(parts[1].rstrip("%")) / 100
            l = float(parts[2].rstrip("%")) / 100
            r, g, b = (round(c * 255) for c in colorsys.hls_to_rgb(h / 360, l, s))
        else:
            def val(p):
                return round(float(p.rstrip("%")) * 2.55) if p.endswith("%") else round(float(p))
            r, g, b = val(parts[0]), val(parts[1]), val(parts[2])
        alpha = parts[3] if len(parts) > 3 else None
        new = map_rgb(r, g, b)
        if new is None:
            return m.group(0)
        nr, ng, nb = int(new[0:2], 16), int(new[2:4], 16), int(new[4:6], 16)
        return (f"rgba({nr},{ng},{nb},{alpha})" if alpha is not None
                else f"rgb({nr},{ng},{nb})")
    except Exception:
        return m.group(0)


def reskin_text(s):
    s2 = RE_HEX.sub(sub_hex, s)
    s2 = RE_FN.sub(sub_fn, s2)
    return s2


def main(root):
    root = Path(root)
    total_f = changed = 0
    for p in root.rglob("*"):
        if p.suffix.lower() not in (".html", ".css", ".js", ".svg"):
            continue
        if p.name in SKIP_NAMES or "screener" in str(p).lower():
            continue
        total_f += 1
        try:
            s = p.read_text(encoding="utf-8", errors="replace")
            s2 = reskin_text(s)
            if s2 != s:
                p.write_text(s2, encoding="utf-8")
                changed += 1
        except Exception as e:
            print(f"reskin WARN {p.name}: {e}")
    print(f"reskin v2 (hue engine): {changed}/{total_f} files rewritten "
          f"(index/screener/sw/theme untouched)")


if __name__ == "__main__":
    try:
        main(sys.argv[1] if len(sys.argv) > 1 else "_site")
    except Exception as e:
        print("reskin WARN (deploy proceeds unskinned):", e)
    sys.exit(0)
