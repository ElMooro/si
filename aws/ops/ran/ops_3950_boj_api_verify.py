"""
ops_3950 — BOJ API VERIFY + JPLG code hunt (base found in 3949's manual
text layer: /api/v1/getDataCode?format=json&lang=en&db=..&startDate=..&
endDate=..&code=..). (a) Run the manual's EXACT example live -> schema.
(b) JP-language manual (1MB, likely richer text layer): same Tj/TJ collapse
-> grep endpoint names, db= table, and 'loan' mentions. (c) Sibling
endpoint probes (getCode/getSeries/searchDataCode/getDbList). (d) If any
code-search exists, query loans and print candidates. Report-only; JPLG
wires next ops on a verified code.
"""
import re, sys, urllib.request, zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"}
BASE = "https://www.stat-search.boj.or.jp/api/v1"


def fetch(url, timeout=25):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def pdf_text(pdf_bytes):
    chunks = []
    for m in re.finditer(rb"stream\r?\n(.*?)endstream", pdf_bytes, re.S):
        try:
            txt = zlib.decompress(m.group(1))
        except Exception:
            continue
        for arr in re.findall(rb"\[((?:\([^)]*\)|[^\]\)])*)\]\s*TJ", txt):
            pieces = re.findall(rb"\(([^)]*)\)", arr)
            if pieces:
                chunks.append(b"".join(pieces))
        for s_ in re.findall(rb"\(([^)]*)\)\s*Tj", txt):
            chunks.append(s_)
    return b"\n".join(chunks).decode("latin1", "ignore")


def main():
    with report("3950_boj_api_verify") as rep:
        rep.heading("ops 3950 — BOJ API verify + JPLG code hunt")

        rep.section("a. run the manual's exact example")
        ex = (f"{BASE}/getDataCode?format=json&lang=en&db=CO&startDate=202501"
              f"&endDate=202504&code=TK99F1000601GCQ01000")
        try:
            st, b = fetch(ex, 30)
            body = b.decode("utf-8", "ignore")
            rep.ok(f"  HTTP {st}, {len(b)}b")
            rep.log(f"  SCHEMA HEAD: {body[:900]}")
        except Exception as e:
            rep.log(f"  example: {str(e)[:140]}")

        rep.section("b. JP manual — endpoints, db table, loan mentions")
        try:
            st, b = fetch("https://www.stat-search.boj.or.jp/info/api_manual.pdf", 40)
            text = pdf_text(b)
            rep.log(f"  jp manual: {len(b)}b -> {len(text)} chars")
            eps = sorted(set(re.findall(r"/api/v1/([A-Za-z]+)", text)))
            rep.log(f"  endpoints seen: {eps}")
            dbs = sorted(set(re.findall(r"db=([A-Z0-9]{1,4})", text)))
            rep.log(f"  db codes seen: {dbs}")
            for m in list(re.finditer(r"(?i)loan", text))[:6]:
                rep.log(f"  loan ctx: …{text[max(0,m.start()-60):m.start()+120]}…")
            for m in list(re.finditer(r"getData[A-Za-z]*|getCode[A-Za-z]*|search[A-Za-z]*", text))[:10]:
                rep.log(f"  fn ctx: …{text[max(0,m.start()-40):m.start()+90]}…")
        except Exception as e:
            rep.log(f"  jp manual: {str(e)[:120]}")

        rep.section("c. sibling endpoint probes")
        for ep in ("getCode", "getSeries", "searchDataCode", "getDbList",
                   "getDataList", "getMetaData"):
            try:
                st, b = fetch(f"{BASE}/{ep}?format=json&lang=en", 15)
                rep.log(f"  {ep}: HTTP {st}, {len(b)}b, head {b[:130]!r}")
            except Exception as e:
                rep.log(f"  {ep}: {str(e)[:80]}")

        rep.section("d. loans code hunt — try MD/DL db guesses with the data endpoint")
        # BOJ 'Principal Figures of Financial Institutions' family often db-coded;
        # cheap probes: wrong db returns an error json that itself is informative.
        for db in ("MD", "DL", "LA", "PF", "MA"):
            try:
                st, b = fetch(f"{BASE}/getDataCode?format=json&lang=en&db={db}"
                              f"&startDate=202501&endDate=202506&code=X", 15)
                rep.log(f"  db={db}: HTTP {st}, {len(b)}b, head "
                        f"{b[:150].decode('utf-8','ignore')!r}")
            except Exception as e:
                rep.log(f"  db={db}: {str(e)[:80]}")

        rep.ok("VERIFY COMPLETE — wire JPLG next ops on confirmed code")
        if False: sys.exit(1)


if __name__ == "__main__":
    main()
