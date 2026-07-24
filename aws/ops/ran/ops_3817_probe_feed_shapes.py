"""
ops_3817 — PROBE ONLY (writes no engine code)

Closes the two `degraded` entries ops 3816 shipped honestly:
  (a) dollar-radar: 'chg_3m_pct' exists in the PRODUCER source (G0 proved it)
      but is absent from the LIVE artifact -> L1 dollar tilt runs neutral.
      Find where the 3m change actually lives in the live document.
  (b) cftc-all-cache: 'contracts' is a COUNT not a collection. Find the real
      row container and the field names so the crowding cap can be wired.

Also dumps the FULL key inventory of data/rotation-dashboard.json — top-level
AND per-row — so the page can be built against real keys rather than memory
(the POST-DEPLOY FIELD-COVERAGE AUDIT contract).

Probe ops write NO code. A wire ops follows once the shapes are known.
"""
import json
import sys
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def load(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        return {"__error__": str(e)}


def shape(v, depth=0):
    if isinstance(v, dict):
        return f"dict[{len(v)}] keys={list(v)[:8]}"
    if isinstance(v, list):
        inner = shape(v[0], depth + 1) if v else "empty"
        return f"list[{len(v)}] of {inner}"
    return type(v).__name__


def main():
    with report("3817_probe_feed_shapes") as rep:
        rep.heading("ops 3817 — PROBE: dollar-radar 3m, cftc shape, artifact keys")

        # ── (a) dollar-radar ────────────────────────────────────────────────
        rep.section("A. dollar-radar.json — where does the 3m change live?")
        dr = load("data/dollar-radar.json")
        if "__error__" in dr:
            rep.fail(f"  unreadable: {dr['__error__']}")
        else:
            rep.log(f"  top-level: {list(dr)[:24]}")
            hits = []

            def walk(o, path=""):
                if len(hits) > 25:
                    return
                if isinstance(o, dict):
                    for k, v in o.items():
                        p = f"{path}.{k}" if path else k
                        kl = k.lower()
                        if ("chg" in kl or "change" in kl or "pct" in kl
                                or "3m" in kl or "mom" in kl):
                            if isinstance(v, (int, float, str)):
                                hits.append((p, v))
                        walk(v, p)
                elif isinstance(o, list) and o:
                    walk(o[0], f"{path}[0]")

            walk(dr)
            for p, v in hits[:25]:
                rep.log(f"    {p} = {v}")
            if not hits:
                rep.warn("  NO change-like field anywhere in the live document")
            for k in ("dxy_synth", "dxy", "breadth_verdict", "components", "canaries"):
                if k in dr:
                    rep.log(f"  {k}: {shape(dr[k])}")

        # ── (b) cftc-all-cache ──────────────────────────────────────────────
        rep.section("B. cftc-all-cache.json — real row container + field names")
        c = load("data/cftc-all-cache.json")
        if "__error__" in c:
            rep.fail(f"  unreadable: {c['__error__']}")
        elif isinstance(c, dict):
            for k, v in list(c.items())[:30]:
                rep.log(f"  {k}: {shape(v)}")
            container = None
            for k, v in c.items():
                vv = list(v.values()) if isinstance(v, dict) else v
                if isinstance(vv, list) and vv and isinstance(vv[0], dict):
                    container = k
                    rep.ok(f"  ROW CONTAINER = '{k}' ({len(vv)} rows)")
                    rep.log(f"    row keys: {list(vv[0])[:26]}")
                    rep.log(f"    sample:   {json.dumps(vv[0], default=str)[:420]}")
                    break
            if not container:
                rep.warn("  no list-of-dicts container found at top level")
        else:
            rep.log(f"  document is {shape(c)}")

        # ── (c) rotation-dashboard field-coverage inventory ─────────────────
        rep.section("C. rotation-dashboard.json — full key inventory for the page")
        d = load("data/rotation-dashboard.json")
        if "__error__" in d:
            rep.fail(f"  unreadable: {d['__error__']}")
            sys.exit(1)
        rep.log(f"  generated_at={d.get('generated_at')} version={d.get('version')}")
        for k, v in d.items():
            rep.log(f"  {k}: {shape(v)}")
        assets = d.get("assets") or []
        if assets:
            a = assets[0]
            rep.log(f"  ── per-asset keys ({len(assets)} rows) ──")
            for k, v in a.items():
                rep.log(f"    assets[].{k}: {shape(v)}")
            for sub in ("trend_gate", "momentum", "rrg", "flows", "crowding"):
                if isinstance(a.get(sub), dict):
                    rep.log(f"    assets[].{sub}.*: {list(a[sub])}")
        r0 = (d.get("layer2_ratios") or {}).get("ratios") or []
        if r0:
            rep.log(f"    layer2.ratios[].*: {list(r0[0])}")
        rep.kv(scored=d["layer3_layer4"]["n_scored"],
               eligible=d["layer3_layer4"]["n_eligible"],
               regime=(d["layer1_regime"].get("quadrant") or {}).get("regime"),
               degraded="; ".join(d.get("degraded") or []) or "none")

        rep.ok("PROBE COMPLETE — no code written, shapes recorded for the wire ops")


if __name__ == "__main__":
    main()
