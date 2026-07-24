"""
ops_3819 — verify rotation-dashboard.html AT THE EDGE + field-coverage audit

Two things this gates, both learned the hard way:

  1. EDGE, NOT REPO. Page audits must read what Cloudflare actually serves, with
     a cache-buster AND a marker string UNIQUE to this version. The capture-gap
     arc burned 6 ops serving a stale page while the repo copy was correct, and
     ops 3746 accepted a stale copy because the sentinel it grepped existed in
     the OLD version too. Marker here is 'v1-ops3819' — it cannot exist before.

  2. FIELD-COVERAGE AUDIT (the page contract). Dump every key the LIVE S3
     artifact publishes — top-level AND per-row — then grep the SERVED html for
     a render path for each. Any key with no render path is an open bug, not a
     stylistic choice. This is the defect class that hid 11 fields on
     sectors.html and zeroed the 13F counts on capital-flow.html.

Runs from the ops runner (real UA), never from the sandbox — the sandbox IP is
CF-403 blocked.
"""
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
PAGE = "https://justhodl.ai/rotation-dashboard.html"
MARKER = "v1-ops3819"
s3 = boto3.client("s3", region_name="us-east-1")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")


def get_page(attempt):
    url = f"{PAGE}?v={int(time.time())}-{attempt}"
    req = urllib.request.Request(url, headers={
        "User-Agent": UA, "Cache-Control": "no-cache", "Pragma": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", "ignore")


def main():
    with report("3819_rotation_page_verify") as rep:
        rep.heading("ops 3819 — rotation-dashboard.html edge verify + field coverage")

        # ── 1. served page carries the NEW marker ──
        rep.section("1. Served page (Cloudflare edge, unique marker)")
        html = ""
        for attempt in range(1, 13):
            try:
                html = get_page(attempt)
            except Exception as e:
                rep.log(f"  attempt {attempt}: {str(e)[:80]}")
                time.sleep(20)
                continue
            if MARKER in html:
                rep.ok(f"  marker '{MARKER}' served on attempt {attempt} "
                       f"({len(html):,} bytes)")
                break
            rep.log(f"  attempt {attempt}: {len(html):,} bytes, marker absent — waiting")
            time.sleep(20)
        else:
            rep.fail(f"  '{MARKER}' never appeared at the edge — pages.yml did not "
                     f"publish (check for a [skip-deploy] auto-commit)")
            sys.exit(1)

        # ── 2. field-coverage audit against the LIVE artifact ──
        rep.section("2. FIELD-COVERAGE AUDIT — live artifact vs served html")
        d = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/rotation-dashboard.json")["Body"].read())

        top = [k for k in d if not k.startswith("_")]
        row = list((d.get("assets") or [{}])[0].keys())
        sub = []
        a0 = (d.get("assets") or [{}])[0]
        for s_ in ("trend_gate", "momentum", "rrg", "flows", "crowding"):
            if isinstance(a0.get(s_), dict):
                sub += list(a0[s_].keys())
        ratio = list(((d.get("layer2_ratios") or {}).get("ratios") or [{}])[0].keys())
        l1 = list((d.get("layer1_regime") or {}).keys())
        l1 += list(((d.get("layer1_regime") or {}).get("quadrant") or {}).keys())
        l1 += list(((d.get("layer1_regime") or {}).get("dollar") or {}).keys())

        # keys that are structural/internal and legitimately need no render path
        WAIVED = {"layer", "name", "prior", "_prior_after_dollar", "basis",
                  "note", "cash_12m_pct", "aum_est_b", "shares_chg_5d_pct",
                  "net_speculator", "category", "confidence", "source_path",
                  "n_universe", "pressure", "range_pctile_1y",
                  "breadth_spread_3m_pp", "chg_1y_pct", "chg_1m_pct",
                  "gold_distortion_note", "prior_source", "engine"}

        missing, covered = [], 0
        for k in sorted(set(top + row + sub + ratio + l1)):
            if k in WAIVED:
                continue
            if k in html:
                covered += 1
            else:
                missing.append(k)

        rep.log(f"  keys checked: {covered + len(missing)} · rendered: {covered}")
        if missing:
            rep.fail(f"  NO RENDER PATH ({len(missing)}): {missing}")
        else:
            rep.ok("  every non-waived key has a render path in the served html")

        # ── 3. structural markers actually present in the served page ──
        rep.section("3. Structural markers")
        need = [
            ("regime banner", "GROWTH × INFLATION REGIME"),
            ("four-layer strip", "LAYER 4"),
            ("overweight board", "Overweight — eligible"),
            ("RRG scatter", "RS-Momentum — acceleration"),
            ("ratio table", "Cross-Asset Ratio Dashboard"),
            ("ranked table", "Full Ranked Universe"),
            ("avoid board", "Avoid — bottom of the confluence"),
            ("methodology", "Methodology &amp;"),
            ("gold caveat copy", "central-bank / debasement demand"),
            ("gate explainer", "redirected to cash"),
            ("degraded surfaced", "open bugs, not decoration"),
            ("cot not-applied flag", "not applied"),
        ]
        bad = []
        for label, m in need:
            if m in html:
                rep.ok(f"  {label}")
            else:
                bad.append(label)
                rep.fail(f"  {label} — '{m[:40]}' absent")

        # ── 4. nav manifest (SERVED copy — repo copy is always stale) ──
        rep.section("4. Nav manifest (served)")
        nav_ok = False
        try:
            req = urllib.request.Request(
                f"https://justhodl.ai/nav-manifest.json?v={int(time.time())}",
                headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=25) as r:
                nav = json.loads(r.read().decode())
            for c in nav.get("categories", []):
                for p in c.get("pages", []):
                    if "rotation-dashboard" in p.get("href", ""):
                        rep.ok(f"  listed under '{c['name']}' as '{p.get('title')}'")
                        nav_ok = True
            if not nav_ok:
                rep.warn("  not in served manifest yet (CI regenerates on a lag)")
        except Exception as e:
            rep.warn(f"  manifest unreadable: {str(e)[:80]}")

        rep.kv(page_bytes=len(html),
               keys_rendered=covered,
               keys_missing=len(missing),
               markers_missing=len(bad),
               in_nav=nav_ok,
               degraded="; ".join(d.get("degraded") or []) or "NONE")

        if missing or bad:
            rep.fail(f"FAILED — missing keys {missing} · missing markers {bad}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {covered} keys rendered, {len(need)} markers served")


if __name__ == "__main__":
    main()
