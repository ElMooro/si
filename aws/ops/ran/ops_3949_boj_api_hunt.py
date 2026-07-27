"""
ops_3949 — BOJ API BASE HUNT (Khalid go: bring JPLG home from the primary
source). Four prongs, report-only:
  1. PDF TEXT LAYER done right: zlib streams -> collapse Tj strings AND TJ
     kerning arrays [(f)(r)(a)(g)]TJ into contiguous text, then hunt url-ish
     fragments (api / stat-search / getData / http even when split).
  2. JP-language notice page (EN page had no link; JP often does).
  3. Portal index pages EN+JP (a launched service usually adds an API nav
     link) — shift_jis-aware.
  4. Common REST-pattern probes: api.stat-search.boj.or.jp, /api/v1/, /api/,
     /ssi/api/, api.boj.or.jp — status + head each.
Anything found gets one follow-probe. Wire happens NEXT ops on verified
endpoints only.
"""
import re, sys, urllib.request, zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"}


def fetch(url, timeout=25):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def pdf_text(pdf_bytes):
    """Collapse Tj strings and TJ arrays from every FlateDecode stream."""
    chunks = []
    for m in re.finditer(rb"stream\r?\n(.*?)endstream", pdf_bytes, re.S):
        try:
            txt = zlib.decompress(m.group(1))
        except Exception:
            continue
        # TJ arrays: [(fr)(ag)-12(ment)]TJ -> join paren pieces
        for arr in re.findall(rb"\[((?:\([^)]*\)|[^\]\)])*)\]\s*TJ", txt):
            pieces = re.findall(rb"\(([^)]*)\)", arr)
            if pieces:
                chunks.append(b"".join(pieces))
        # plain Tj
        for s_ in re.findall(rb"\(([^)]*)\)\s*Tj", txt):
            chunks.append(s_)
    return b"\n".join(chunks).decode("latin1", "ignore")


def main():
    with report("3949_boj_api_hunt") as rep:
        rep.heading("ops 3949 — BOJ API base hunt (4 prongs)")

        rep.section("1. PDF text layer (manual + notice)")
        found_urlish = set()
        for name, url in (("manual", "https://www.stat-search.boj.or.jp/info/api_manual_en.pdf"),
                          ("notice", "https://www.stat-search.boj.or.jp/info/api_notice_en.pdf")):
            try:
                st, body = fetch(url, 35)
                text = pdf_text(body)
                rep.log(f"  {name}: {len(body)}b pdf -> {len(text)} chars of text")
                # url-ish hunts, tolerant of kerning splits already collapsed
                for pat in (r"https?://[^\s\)\"<>]{8,}",
                            r"[A-Za-z0-9\.\-]*stat-search[^\s\)\"<>]*",
                            r"[A-Za-z0-9\.\-/]*api[A-Za-z0-9\.\-/\?\=_]{3,}"):
                    for hit in re.findall(pat, text):
                        if len(hit) > 8 and "adobe" not in hit.lower():
                            found_urlish.add(hit[:160])
                # also show contexts around 'api'
                for m in list(re.finditer(r"(?i)api", text))[:6]:
                    ctx = text[max(0, m.start()-70):m.start()+120].replace("\n", " ")
                    rep.log(f"  {name} ctx: …{ctx}…")
            except Exception as e:
                rep.log(f"  {name}: {str(e)[:110]}")
        for h in sorted(found_urlish)[:15]:
            rep.log(f"  url-ish: {h}")

        rep.section("2. JP-language notice page")
        try:
            st, body = fetch("https://www.boj.or.jp/statistics/outline/notice_2026/not260218a.htm", 25)
            html = body.decode("utf-8", "ignore")
            if len(html) < 400:
                html = body.decode("shift_jis", "ignore")
            links = [u for u in re.findall(r'href="([^"]+)"', html)
                     if "api" in u.lower() and "font" not in u.lower()][:10]
            rep.log(f"  HTTP {st}, {len(body)}b; api links: {links}")
            for frag in re.findall(r'https?://[^\s"<>]*api[^\s"<>]*', html)[:6]:
                rep.log(f"  inline: {frag[:130]}")
                found_urlish.add(frag)
        except Exception as e:
            rep.log(f"  jp notice: {str(e)[:110]}")

        rep.section("3. portal index pages (EN+JP) — API nav link?")
        for name, url in (("index_en", "https://www.stat-search.boj.or.jp/index_en.html"),
                          ("index_jp", "https://www.stat-search.boj.or.jp/index.html"),
                          ("info_en", "https://www.stat-search.boj.or.jp/info/index_en.html")):
            try:
                st, body = fetch(url, 25)
                html = body.decode("shift_jis", "ignore")
                if "html" not in html[:200].lower():
                    html = body.decode("utf-8", "ignore")
                hits = sorted({u for u in re.findall(r'href="([^"]+)"', html)
                               if "api" in u.lower()})[:10]
                rep.log(f"  {name}: HTTP {st}, {len(body)}b; api hrefs: {hits}")
                for h in hits:
                    found_urlish.add(h)
            except Exception as e:
                rep.log(f"  {name}: {str(e)[:100]}")

        rep.section("4. common REST-pattern probes")
        for base in ("https://api.stat-search.boj.or.jp/",
                     "https://www.stat-search.boj.or.jp/api/v1/",
                     "https://www.stat-search.boj.or.jp/api/",
                     "https://www.stat-search.boj.or.jp/ssi/api/",
                     "https://api.boj.or.jp/"):
            try:
                st, b = fetch(base, 15)
                rep.log(f"  {base} -> HTTP {st}, {len(b)}b, head {b[:110]!r}")
            except Exception as e:
                rep.log(f"  {base} -> {str(e)[:90]}")

        rep.section("5. follow-probe the best candidates")
        cands = [h for h in found_urlish
                 if ("stat-search" in h or "boj" in h) and "api" in h.lower()][:5]
        for c in cands:
            u = c if c.startswith("http") else "https://www.stat-search.boj.or.jp/" + c.lstrip("/")
            try:
                st, b = fetch(u, 15)
                rep.ok(f"  {u[:110]} -> HTTP {st}, {len(b)}b, head {b[:120]!r}")
            except Exception as e:
                rep.log(f"  {u[:110]} -> {str(e)[:90]}")

        rep.ok("HUNT COMPLETE — wire only on a verified endpoint (next ops)")
        if False: sys.exit(1)


if __name__ == "__main__":
    main()
