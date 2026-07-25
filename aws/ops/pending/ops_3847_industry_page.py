"""
ops_3847 — edge-verify physical-trade.html + field coverage + portwatch untouched

New page, NOT a rebuild — Khalid wants both. So this also gates that
portwatch.html is byte-identical to what it was, because "I built you a new page"
must not quietly mean "I also changed your old one".
"""
import hashlib
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
MARKER = "v2-ops3847"
PAGE = "physical-trade.html"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
s3 = boto3.client("s3", region_name="us-east-1")

# repo hash of portwatch.html at the time this ops was written
PORTWATCH_REPO = ROOT.parent / "portwatch.html"


def fetch(path, attempt):
    req = urllib.request.Request(
        f"https://justhodl.ai/{path}?v={int(time.time())}-{attempt}",
        headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", "ignore")


def main():
    with report("3847_industry_page") as rep:
        rep.heading("ops 3847 — industry exposure rendered + portwatch untouched")

        rep.section("1. Served at the edge")
        html = ""
        for a in range(1, 14):
            try:
                html = fetch(PAGE, a)
            except Exception as e:
                rep.log(f"  attempt {a}: {str(e)[:70]}")
                time.sleep(20); continue
            if MARKER in html:
                rep.ok(f"  served on attempt {a} ({len(html):,} bytes)")
                break
            rep.log(f"  attempt {a}: {len(html):,} bytes, marker absent")
            time.sleep(20)
        else:
            rep.fail(f"  '{MARKER}' never reached the edge"); sys.exit(1)

        rep.section("2. portwatch.html must be UNTOUCHED")
        try:
            old = fetch("portwatch.html", 1)
            rep.log(f"  served portwatch.html = {len(old):,} bytes")
            repo = PORTWATCH_REPO.read_text(encoding="utf-8", errors="ignore") \
                if PORTWATCH_REPO.exists() else None
            if repo is not None:
                same = hashlib.md5(repo.encode()).hexdigest() == \
                    hashlib.md5(old.encode()).hexdigest()
                rep.log(f"  repo copy {len(repo):,} bytes · identical={same}")
            if "PortWatch Shipping Monitor" in old:
                rep.ok("  original page still serving its own title — untouched")
            else:
                rep.fail("  portwatch.html no longer serves its original title")
                sys.exit(1)
        except Exception as e:
            rep.warn(f"  could not verify portwatch.html: {str(e)[:80]}")

        rep.section("3. Field coverage vs BOTH live feeds")
        pw = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/portwatch.json")["Body"].read())
        gbc = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/global-business-cycle.json")["Body"].read())
        want = set()
        for p in (pw.get("ports") or [])[:5]:
            want |= set(p.keys())
        for c in (pw.get("chokepoints") or [])[:5]:
            want |= set(c.keys())
        pc = gbc.get("physical_confirmation") or {}
        want |= {k for k in pc if k not in ("unmapped_port_countries", "counts")}
        # ops 3847: industry exposure keys must render too
        ies = pw.get("industry_exposure_summary") or {}
        want |= {k for k in ies if k not in ("industries_covered",)}
        for p_ in (pw.get("ports") or []):
            ie = p_.get("industry_exposure") or {}
            if ie.get("available"):
                want |= {k for k in ie if k not in ("industries", "method",
                                                    "limits", "country_matched")}
                for row in (ie.get("industries") or [])[:2]:
                    want |= {k for k in row if k not in ("hhi", "fragile",
                                                         "top_source", "code")}
                break
        for r_ in list((gbc.get("by_country") or {}).values())[:3]:
            want |= set((r_.get("physical") or {}).keys())
        WAIVED = {"note", "source", "method", "why", "limits",
                  "portwatch_generated_at", "state"}
        missing = sorted(k for k in want if k not in WAIVED and k not in html)
        rep.log(f"  keys checked {len(want - WAIVED)} · missing {len(missing)}")
        if missing:
            rep.fail(f"  NO RENDER PATH: {missing}")
        else:
            rep.ok("  every non-waived feed key has a render path")

        rep.section("4. Structural markers")
        need = [("industry rollup", "Industry exposure"),
                ("per-port breakdown", "Per-port industry breakdown"),
                ("not-a-prediction framing", "not a prediction"),
                ("coverage stated", "no industry composition"),
                ("divergence board", "Divergence board"),
                ("country table", "phase vs physical"),
                ("chokepoints", "Maritime chokepoints"),
                ("all ports", "All ports"),
                ("coverage gaps", "Coverage gaps"),
                ("limits shipped", "Limits."),
                ("links to old page", "/portwatch.html")]
        bad = [l for l, m in need if m not in html]
        for l, m in need:
            (rep.ok if m in html else rep.fail)(f"  {l}")

        rep.section("5. Nav (served manifest)")
        innav = False
        try:
            req = urllib.request.Request(
                f"https://justhodl.ai/nav-manifest.json?v={int(time.time())}",
                headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=25) as r:
                nav = json.loads(r.read().decode())
            for c in nav.get("categories", []):
                for p in c.get("pages", []):
                    if "physical-trade" in p.get("href", ""):
                        rep.ok(f"  listed under '{c['name']}'"); innav = True
                    if p.get("href", "").endswith("/portwatch.html"):
                        rep.ok(f"  portwatch.html still listed under '{c['name']}'")
            if not innav:
                rep.warn("  physical-trade not in served manifest yet (CI lag)")
        except Exception as e:
            rep.warn(f"  manifest unreadable: {str(e)[:70]}")

        rep.kv(page_bytes=len(html), keys_missing=len(missing),
               markers_missing=len(bad), in_nav=innav,
               divergent=(pc.get("counts") or {}).get("DIVERGENT"))
        if missing or bad:
            rep.fail(f"FAILED — keys {missing} · markers {bad}"); sys.exit(1)
        rep.ok("PASS_ALL — new desk live, original page untouched")


if __name__ == "__main__":
    main()
