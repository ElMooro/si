"""
ops_3951 — BOJ getDataLayer walk -> the JPLG series code. getDataCode is
VERIFIED (Tankan example returned real values). getDataLayer = the tree
browser. Param names unknown -> permutation probes, then a GENERIC drill:
parse whatever JSON comes back, follow any entry whose name mentions
loans/lending/discounts, print candidate SERIES_CODEs, and CONFIRM the best
one via getDataCode with real values. Report-only; wire next ops.
"""
import json, re, sys, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"}
BASE = "https://www.stat-search.boj.or.jp/api/v1"


def fetch_json(url, timeout=25):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def walk_strings(obj, out=None, depth=0):
    """Collect (name-ish string, code-ish sibling) pairs generically."""
    if out is None:
        out = []
    if isinstance(obj, dict):
        name = next((v for k, v in obj.items()
                     if isinstance(v, str) and "NAME" in k.upper()), None)
        code = next((v for k, v in obj.items()
                     if isinstance(v, str) and ("CODE" in k.upper() or "LAYER" in k.upper())
                     and v != name), None)
        if name:
            out.append((name, code, obj))
        for v in obj.values():
            walk_strings(v, out, depth + 1)
    elif isinstance(obj, list):
        for v in obj[:400]:
            walk_strings(v, out, depth + 1)
    return out


def main():
    with report("3951_boj_layer_walk") as rep:
        rep.heading("ops 3951 — getDataLayer walk to the loans series code")

        rep.section("1. param permutation probes")
        root = None
        root_url = None
        for q in ("format=json&lang=en",
                  "format=json&lang=en&db=CO",
                  "format=json&lang=en&layer=",
                  "format=json&lang=en&layerCode=",
                  "format=json&lang=en&hierarchyLevel=1"):
            u = f"{BASE}/getDataLayer?{q}"
            try:
                d = fetch_json(u)
                head = json.dumps(d)[:260]
                rep.log(f"  {q}: STATUS={d.get('STATUS')} head={head}")
                if d.get("STATUS") == 200 and root is None:
                    root, root_url = d, u
            except Exception as e:
                rep.log(f"  {q}: {str(e)[:100]}")
        if root is None:
            rep.fail("no getDataLayer permutation returned 200 — dump above guides next")
            sys.exit(1)
        rep.ok(f"  ROOT via {root_url}")

        rep.section("2. root layer inventory")
        pairs = walk_strings(root)
        for name, code, _ in pairs[:40]:
            rep.log(f"  {str(code)[:28]:28s} {str(name)[:80]}")

        rep.section("3. drill toward loans/lending/discounts")
        frontier = [(n, c, o) for n, c, o in pairs if c]
        found_series = []
        seen = set()
        for hop in range(3):
            targets = [t for t in frontier
                       if re.search(r"loan|lend|discount|deposit", t[0], re.I)] or frontier[:6]
            nxt = []
            for name, code, _ in targets[:8]:
                if code in seen:
                    continue
                seen.add(code)
                for param in ("layerCode", "layer", "code", "parentCode"):
                    u = f"{BASE}/getDataLayer?format=json&lang=en&{param}={code}"
                    try:
                        d = fetch_json(u, 20)
                        if d.get("STATUS") != 200:
                            continue
                        sub = walk_strings(d)
                        hit_names = [s for s in sub
                                     if re.search(r"loan|lend|discount", s[0], re.I)][:6]
                        if hit_names:
                            rep.log(f"  [{hop}] {name[:40]} ({param}={code[:20]}):")
                            for hn, hc, ho in hit_names:
                                rep.log(f"      -> {str(hc)[:30]} {hn[:85]}")
                                sc = next((v for k, v in ho.items()
                                           if k.upper() == "SERIES_CODE"), None)
                                if sc:
                                    found_series.append((sc, hn))
                        nxt.extend([s for s in sub if s[1]])
                        break
                    except Exception:
                        continue
            frontier = nxt
            if found_series:
                break

        rep.section("4. confirm the best candidate via getDataCode")
        for sc, nm in found_series[:5]:
            try:
                d = fetch_json(f"{BASE}/getDataCode?format=json&lang=en"
                               f"&startDate=202501&endDate=202506&code={sc}", 25)
                rs = (d.get("RESULTSET") or [{}])[0]
                vals = (rs.get("VALUES") or {})
                rep.ok(f"  {sc} :: {rs.get('NAME_OF_TIME_SERIES','')[:80]} "
                       f":: unit={rs.get('UNIT')} freq={rs.get('FREQUENCY')} "
                       f"vals={str(vals.get('VALUES'))[:60]}")
            except Exception as e:
                rep.log(f"  {sc}: {str(e)[:90]}")
        if not found_series:
            rep.log("  no loan-named SERIES_CODE surfaced in 3 hops — dump above "
                    "shows the tree vocabulary for a targeted pass next")

        rep.ok("LAYER WALK COMPLETE")
        if False: sys.exit(1)


if __name__ == "__main__":
    main()
