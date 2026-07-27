"""
ops_3988 — inventory + v1.8 close (runner-side, no invoke).

Khalid: total data pulled, data per source, all ENGINES listed with what
each does, all PAGES listed with what each does — on the census page.

Audit first: engines already exist (scripts/gen_engine_manifest.py uploads
data/engine-manifest.json on every ops push — 733 engines w/ descriptions).
WIRED. Pages: site-catalog.json has 231 title-only rows of ~410 pages and
zero purposes — insufficient, so this op COMPILES data/page-manifest.json
from the repo's own HTML (title tag + meta description / first subtitle).

Also gates the v1.8 clean-values artifact (fired 21:50:30; ordering bug
meant no verifier ever caught it) and checks page v4 at the edge.
"""
import html
import json
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
REPO = ROOT.parents[0]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
MARK = "data-census v1.8 ops3987 clean-values"
PAGE = "https://justhodl.ai/data-census.html"
PAGE_MARKS = ["v4-ops3988", "Data points pulled", "what it is supposed to do",
              "id=\"engt\"", "id=\"pgt\""]
T_RX = re.compile(r"<title>(.*?)</title>", re.I | re.S)
MD_RX = re.compile(r'<meta\s+(?:name|property)=["\'](?:og:)?description["\']\s+content=["\'](.*?)["\']', re.I | re.S)
SUB2 = re.compile(r'class=["\'][^"\']*(?:sub|dim|lead|tagline)[^"\']*["\'][^>]*>(.*?)<', re.I | re.S)
P_RX = re.compile(r"<p[^>]*>(.*?)</p>", re.I | re.S)
SUB_RX = re.compile(r'<div class="dim"[^>]*>(.*?)</div>', re.I | re.S)
SUB2 = re.compile(r"(?:sub|dim|lead|tagline)[^>]*>([^<]{10,300})<", re.I)
P_RX = re.compile(r"<p[^>]*>(.*?)</p>", re.I | re.S)
TAGS = re.compile(r"<[^>]+>")


def clean(t, n=220):
    return html.unescape(TAGS.sub(" ", t or "")).split("Generated")[0].strip()[:n]


def main():
    with report("3988_inventory_close") as rep:
        rep.heading("ops 3988 — fleet inventory + v1.8 close")
        checks = []

        rep.section("A. the v1.8 artifact (fired 21:50:30, never gated)")
        doc = json.loads(s3.get_object(Bucket=BUCKET,
                                       Key="data/data-census.json")["Body"].read())
        t = doc.get("totals") or {}
        bs = doc.get("by_source") or {}
        md = doc.get("metric_directory") or []
        per_src = {k: (v or {}).get("n") for k, v in bs.items()}
        total_dp = sum(v or 0 for v in per_src.values())
        rep.kv(marker=doc.get("marker"), generated_at=doc.get("generated_at"),
               scalar_paths=t.get("scalar_paths"),
               total_data_points_by_source=total_dp,
               per_source=json.dumps(per_src))
        fred = ((bs.get("FRED") or {}).get("metrics")) or []
        for m in fred[:6]:
            rep.log(f"  FRED: {m['name']} = {m['value']} live={m['live']} "
                    f"from {str(m['pulled_from'])[:30]} via {m['engine']}")
        us10 = (next((m for m in fred
                      if str(m.get("name")).upper() == "US10Y"), None)
                or next((m for m in fred
                         if "DGS10" in str(m.get("pulled_from", "")).upper()
                         and "VOL" not in str(m.get("name")).upper()), None))
        rep.log(f"  matched us10 -> {json.dumps(us10)[:200]}")
        rep.log(f"  fred head names: {[str(m.get('name'))[:28] for m in fred[:5]]}")
                or next((m for m in fred
                         if "DGS10" in str(m.get("pulled_from", "")).upper()
                         and "VOL" not in str(m.get("name")).upper()), None))
        rep.kv(us10=json.dumps(us10) if us10 else None)
        checks += [
            ("artifact is v1.8", doc.get("marker") == MARK),
            (">=6 source families", len(bs) >= 6),
            ("FRED >=150 metrics", len(fred) >= 150),
            ("US10Y is a yield 0<v<25",
             bool(us10) and 0 < abs(us10.get("value", 0)) < 25),
            ("US10Y from DGS10 via the vault engine",
             bool(us10) and "DGS10" in str(us10.get("pulled_from", "")).upper()
             and "tradingview" in str(us10.get("engine", ""))),
            ("no tried_at junk anywhere in the directory",
             not any("tried_at" in str(m.get("path", "")) for m in md)),
        ]

        rep.section("B. WIRE engines (already generated fleet-wide)")
        em = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/engine-manifest.json")["Body"].read())
        eng = em.get("engines") or []
        with_desc = sum(1 for e in eng if (e.get("description") or "").strip())
        rep.kv(n_engines=len(eng), with_description=with_desc)
        checks += [("engine manifest >=600 engines", len(eng) >= 600),
                   (">=70% engines carry a description",
                    with_desc >= 0.7 * max(1, len(eng)))]

        rep.section("C. COMPILE data/page-manifest.json from the repo HTML")
        pages = []
        for f in sorted(REPO.glob("*.html")):
            try:
                raw = f.read_text(errors="ignore")[:60000]
            except Exception:
                continue
            title = clean((T_RX.search(raw) or [None, ""])[1], 120)
            purpose = ""
            for rx in (MD_RX, SUB_RX, SUB2, P_RX):
                m0 = rx.search(raw)
                if m0:
                    cand = clean(m0.group(1), 240)
                    if len(cand) >= 25:
                        purpose = cand
                        break
                    if cand and not purpose:
                        purpose = cand or \
                clean((SUB_RX.search(raw) or [None, ""])[1], 240)
            pages.append({"page": f.name, "title": title, "purpose": purpose})
        with_purpose = sum(1 for g in pages if g["purpose"])
        s3.put_object(Bucket=BUCKET, Key="data/page-manifest.json",
                      Body=json.dumps({"generated_at":
                                       datetime.now(timezone.utc).isoformat(),
                                       "source": "ops 3988 — compiled from repo HTML "
                                                 "(title + meta description / subtitle)",
                                       "n_pages": len(pages), "pages": pages},
                                      default=str),
                      ContentType="application/json", CacheControl="max-age=900")
        rep.kv(n_pages=len(pages), with_purpose=with_purpose)
        for g in pages[:6]:
            rep.log(f"  {g['page']:28s} | {g['title'][:38]:38s} | {g['purpose'][:60]}")
        checks += [("page manifest >=350 pages", len(pages) >= 350),
                   (">=40% pages carry a purpose (best-effort from real content; "
                    "empty stays honestly empty)",
                    with_purpose >= 0.4 * max(1, len(pages)))]

        rep.section("D. page v4 at the edge")
        got, htm = 0, ""
        for i in range(10):
            try:
                req = urllib.request.Request(PAGE + f"?cb={int(time.time())}",
                                             headers={"User-Agent": "Mozilla/5.0",
                                                      "Cache-Control": "no-cache"})
                htm = urllib.request.urlopen(req, timeout=25).read().decode("utf8",
                                                                            "ignore")
                got = sum(1 for m in PAGE_MARKS if m in htm)
                rep.log(f"  [{i}] {len(htm)}B {got}/{len(PAGE_MARKS)}")
                if got == len(PAGE_MARKS):
                    break
            except Exception as e:
                rep.log(f"  [{i}] {type(e).__name__}")
            time.sleep(25)
        checks.append(("page v4 live at edge", got == len(PAGE_MARKS)))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — v1.8 whole: {total_dp} data points across "
               f"{len(bs)} sources (FRED {len(fred)}); engines {len(eng)} "
               f"({with_desc} described); pages {len(pages)} "
               f"({with_purpose} with purpose); page v4 {got}/{len(PAGE_MARKS)}")


if __name__ == "__main__":
    main()

# regate nonce

# regate2
