"""
ops_3838 — PROBE: what per-country HARD data can confirm global-recession?

global-recession v1.2.1 ships honestly: CONFIRMED 0 / UNCONFIRMED 32, with
84.3% of the headline resting on equity-momentum phase labels. It stays unwired
until an independent leg exists. OECD CLI is 30 months stale and the FRED CCI/BCI
supplements are empty, so both prior candidates are dead.

Remaining fleet candidates carry PHYSICAL and CREDIT data — genuinely
independent of equity momentum, which is the whole point:
  portwatch        trade/port volumes  (18 country refs in source)
  china-liquidity  credit impulse      (57 impulse refs — CHN is 18% of world GDP
                                        and the single largest contributor)
  trade-nowcast / freight-pulse / air-cargo

Dumps real country coverage and field names. Writes no engine code — the last
two confirmation attempts failed on assumed shapes (OECD staleness, a defaulted
supplement value of 0.0 that manufactured a false CONFIRMED), so this one gets
measured before anything is built.
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

CANDIDATES = ["data/portwatch.json", "data/trade-nowcast.json",
              "data/china-liquidity.json", "data/freight-pulse.json",
              "data/air-cargo.json", "data/global-business-cycle.json"]
ISO_HINT = ("iso3", "iso", "country", "country_code", "economy", "nation")


def shape(v):
    if isinstance(v, dict):
        return f"dict[{len(v)}] {list(v)[:7]}"
    if isinstance(v, list):
        return f"list[{len(v)}] of {type(v[0]).__name__ if v else 'empty'}"
    return f"{type(v).__name__}={str(v)[:40]}"


def main():
    with report("3838_probe_hard_legs") as rep:
        rep.heading("ops 3838 — PROBE: per-country hard legs for confirmation")
        usable = 0

        for key in CANDIDATES:
            rep.section(f"── {key}")
            try:
                d = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            except Exception as e:
                rep.warn(f"  unreadable: {str(e)[:90]}")
                continue
            rep.log(f"  generated_at={d.get('generated_at')} "
                    f"top-level={list(d)[:12]}")

            # find country-keyed containers
            hits = []
            for k, v in d.items():
                if isinstance(v, dict) and len(v) >= 5:
                    ks = list(v)[:6]
                    if all(isinstance(x, str) and 2 <= len(x) <= 4 for x in ks):
                        hits.append((k, "dict-keyed-by-code", v))
                elif isinstance(v, list) and v and isinstance(v[0], dict):
                    if any(h in v[0] for h in ISO_HINT):
                        hits.append((k, "list-of-rows", v))
            if not hits:
                rep.warn("  no country-keyed container found")
                continue
            for name, kind, cont in hits[:3]:
                if kind == "dict-keyed-by-code":
                    codes = list(cont)
                    sample = cont[codes[0]]
                    rep.ok(f"  '{name}' {kind} · {len(codes)} codes: {codes[:14]}")
                    if isinstance(sample, dict):
                        rep.log(f"    per-country fields: {list(sample)[:16]}")
                        rep.log(f"    sample {codes[0]}: "
                                f"{json.dumps(sample, default=str)[:260]}")
                        usable += 1
                else:
                    rep.ok(f"  '{name}' {kind} · {len(cont)} rows")
                    rep.log(f"    row fields: {list(cont[0])[:16]}")
                    rep.log(f"    sample: {json.dumps(cont[0], default=str)[:260]}")
                    usable += 1

            # numeric change-like fields are what a confirmation leg needs
            chg = []

            def walk(o, p=""):
                if len(chg) > 12:
                    return
                if isinstance(o, dict):
                    for k2, v2 in o.items():
                        kl = str(k2).lower()
                        if isinstance(v2, (int, float)) and any(
                                s in kl for s in ("yoy", "impulse", "chg", "growth",
                                                  "_3m", "trend", "z")):
                            chg.append((f"{p}.{k2}".lstrip("."), v2))
                        walk(v2, f"{p}.{k2}".lstrip("."))
                elif isinstance(o, list) and o:
                    walk(o[0], f"{p}[0]")
            walk(d)
            for pth, val in chg[:12]:
                rep.log(f"    change-like: {pth} = {val}")

        rep.section("Verdict")
        if usable == 0:
            rep.fail("no candidate exposes a country-keyed container — a per-country "
                     "hard leg cannot be built from these feeds")
            sys.exit(1)
        rep.ok(f"{usable} usable country-keyed container(s) found — a confirmation "
               f"leg is buildable; wire ops follows")
        rep.kv(usable_containers=usable, candidates=len(CANDIDATES))


if __name__ == "__main__":
    main()
