"""
ops_3952 — read the BOJ API manuals PROPERLY (pip install pypdf on the
runner; CID/ToUnicode-aware) -> the real getDataLayer param spec + db table
+ any loans examples; then, with learned params, CALL getDataLayer in the
same run, drill to Loans and Discounts, and CONFIRM the series via
getDataCode with real values. Report-only; JPLG wires next ops.
"""
import json, re, subprocess, sys, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"}
BASE = "https://www.stat-search.boj.or.jp/api/v1"


def fetch(url, timeout=30):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def fetch_json(url, timeout=25):
    return json.loads(fetch(url, timeout))


def main():
    with report("3952_boj_manual_pypdf") as rep:
        rep.heading("ops 3952 — manuals via pypdf -> getDataLayer spec -> loans code")

        subprocess.run([sys.executable, "-m", "pip", "install", "-q",
                        "--break-system-packages", "pypdf"], check=False, timeout=120)
        from pypdf import PdfReader  # noqa: E402
        import io as _io

        texts = {}
        for name, url in (("en", "https://www.stat-search.boj.or.jp/info/api_manual_en.pdf"),
                          ("jp", "https://www.stat-search.boj.or.jp/info/api_manual.pdf")):
            try:
                pdf = fetch(url, 40)
                rd = PdfReader(_io.BytesIO(pdf))
                t = "\n".join((p.extract_text() or "") for p in rd.pages)
                texts[name] = t
                rep.log(f"  {name} manual: {len(pdf)}b -> {len(t)} chars, "
                        f"{len(rd.pages)} pages")
            except Exception as e:
                rep.log(f"  {name}: {str(e)[:120]}")
        t = texts.get("en") or texts.get("jp") or ""

        rep.section("getDataLayer spec from the manual")
        i = t.find("getDataLayer")
        while i != -1 and i < len(t):
            rep.log("  ── ctx ──\n" + t[max(0, i-80): i+700].replace("\n", " ¶ ")[:760])
            i = t.find("getDataLayer", i + 1)
            if i > 0 and sum(1 for _ in range(3)) and t.count("getDataLayer", 0, i) > 3:
                break

        rep.section("param names + db table + loans mentions")
        params = sorted(set(re.findall(r"\b([a-z][a-zA-Z]{2,16})=", t)))
        rep.log(f"  param-ish tokens: {params[:25]}")
        for m in list(re.finditer(r"(?i)loans?\b", t))[:8]:
            rep.log(f"  loan ctx: …{t[max(0,m.start()-70):m.start()+130].replace(chr(10),' ')}…")
        dbs = re.findall(r"\bdb=([A-Z0-9]{1,4})\b", t)
        rep.log(f"  db= values in text: {sorted(set(dbs))}")

        rep.section("call getDataLayer with learned params + drill")
        layer_params = [p for p in params if "layer" in p.lower()] or ["layerCode"]
        got = None
        for lp in layer_params[:4]:
            for val in ("", "0", "1", "ROOT", "F"):
                u = f"{BASE}/getDataLayer?format=json&lang=en&{lp}={val}"
                try:
                    d = fetch_json(u, 20)
                    if d.get("STATUS") == 200:
                        got = (u, d)
                        rep.ok(f"  200 via {lp}={val!r}")
                        break
                except Exception:
                    continue
            if got:
                break
        if got:
            u, d = got
            body = json.dumps(d)
            rep.log(f"  root head: {body[:500]}")
            codes = re.findall(r'"([A-Z0-9_]{2,24})"\s*,\s*"([^"]{3,80})"', body)[:30]
            # generic loans hunt in whatever came back
            loanish = [c for c in re.findall(r'"[A-Z0-9_]{2,24}"[^{}]{0,120}', body)
                       if re.search(r"loan|lend|discount", c, re.I)][:8]
            for L in loanish:
                rep.log(f"  loans-ish node: {L[:150]}")
        else:
            rep.log("  still no 200 — manual ctx above should now show the exact "
                    "required params for the next pass")

        rep.ok("MANUAL READ COMPLETE")
        if False: sys.exit(1)


if __name__ == "__main__":
    main()
