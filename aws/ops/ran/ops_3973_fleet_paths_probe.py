"""
ops_3973 — PROBE (read-only): what the FLEET already holds for the dead
vault symbols — and a suspected data-integrity bug.

Khalid: "look at other engines and previous chats for them."

The Asia Arc (ops 3582-3634) already landed three legs the vault is still
calling NO_FREE_SOURCE: Taiwan export orders (US$89.47B May-2026, MOEA
GetPointData), Korea 1-20 flash (+52.3% YoY), China TSF (¥20.84tn H1-2026).
justhodl-boj-detail carries cpi_yoy_pct and jgb_yield. So before hunting any
more external endpoints, read what the fleet already produces.

SUSPECTED BUG (found by grep, must be confirmed against the live artifact):
    "JPEXPYY": "fleet:data/asia-leads.json:korea_exports.yoy_pct"
JPEXPYY is JAPAN exports YoY. It is wired to KOREA's export YoY. If that
resolves, the vault has been publishing one country's number under another
country's symbol — and since it is LIVE it has been voting in the MACRO
barometer and carrying his Japan notes. This op confirms or clears it.

Read-only. Writes nothing.
"""
import json
import sys
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def gj(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        return {"__err__": f"{type(e).__name__}: {str(e)[:90]}"}


def walk(o, pre="", depth=0, out=None, maxd=3):
    """Flatten to dot-paths with scalar leaves so aliases can be written exactly."""
    if out is None:
        out = {}
    if depth > maxd:
        return out
    if isinstance(o, dict):
        for k, v in o.items():
            walk(v, f"{pre}.{k}" if pre else str(k), depth + 1, out, maxd)
    elif isinstance(o, (int, float)) and not isinstance(o, bool):
        out[pre] = o
    elif isinstance(o, str) and len(o) < 40:
        out[pre] = o
    return out


def main():
    with report("3973_fleet_paths_probe") as rep:
        rep.heading("ops 3973 — fleet paths for dead vault symbols + JPEXPYY integrity")
        checks = []

        rep.section("A. CONFIRM/CLEAR the JPEXPYY mislabel")
        al = gj("data/asia-leads.json")
        vault = gj("data/tradingview.json")
        vidx = {r["symbol"]: r for r in (vault.get("symbols") or [])}
        jp = vidx.get("JPEXPYY") or {}
        kr_yoy = ((al.get("korea_exports") or {}) or {}).get("yoy_pct")
        rep.log(f"  JPEXPYY in vault : status={jp.get('status')} value={jp.get('value')} "
                f"src={jp.get('source')} n_notes={jp.get('n_notes')}")
        rep.log(f"  asia-leads korea_exports.yoy_pct = {kr_yoy}")
        same = (jp.get("value") is not None and kr_yoy is not None
                and abs(float(jp["value"]) - float(kr_yoy)) < 1e-9)
        rep.kv(jpexpyy_equals_korea=same)
        if same:
            rep.fail("  CONFIRMED — Japan's symbol is publishing KOREA's export YoY")
        else:
            rep.log("  not identical — record the real values above before concluding")
        checks.append(("JPEXPYY integrity checked", True))

        rep.section("B. asia-leads.json — every scalar path")
        if "__err__" in al:
            rep.fail(f"  {al['__err__']}")
        else:
            rep.log(f"  top-level keys: {sorted(al.keys())[:25]}")
            for p, v in sorted(walk(al).items()):
                if any(t in p.lower() for t in ("taiwan", "korea", "china", "yoy",
                                                "value", "level", "impulse", "orders",
                                                "exports", "asof", "period")):
                    rep.log(f"    {p} = {v}")

        rep.section("C. boj-detail.json — Japan CPI / JGB")
        bd = gj("data/boj-detail.json")
        if "__err__" in bd:
            rep.fail(f"  {bd['__err__']}")
        else:
            rep.log(f"  top-level keys: {sorted(bd.keys())[:25]}")
            for p, v in sorted(walk(bd).items()):
                if any(t in p.lower() for t in ("cpi", "jgb", "yield", "yoy", "rate",
                                                "policy", "asof")):
                    rep.log(f"    {p} = {v}")

        rep.section("D. china-liquidity.json — TSF / credit impulse")
        cl = gj("data/china-liquidity.json")
        if "__err__" in cl:
            rep.fail(f"  {cl['__err__']}")
        else:
            rep.log(f"  top-level keys: {sorted(cl.keys())[:25]}")
            for p, v in sorted(walk(cl).items()):
                if any(t in p.lower() for t in ("tsf", "impulse", "m1", "m2", "yoy",
                                                "value", "flow", "period", "ppi")):
                    rep.log(f"    {p} = {v}")

        rep.section("E. candidate wirings for still-dead symbols")
        dead = [r["symbol"] for r in (vault.get("symbols") or [])
                if r.get("status") != "LIVE"]
        rep.kv(n_dead=len(dead))
        want = ["TWEXPYY", "JPIRYY", "CNPPIYY", "CNEAI", "CNINTR", "CNLIVRR",
                "CN10Y", "TWINTR", "JPCIND", "JPJV", "JPMTO", "JPGDG", "TOPIX",
                "USFER", "EUFER", "CHFER", "USCF", "USTOT", "USBCOI", "USSBSI"]
        rep.log(f"  still dead among the targets: {[s for s in want if s in dead]}")
        rep.log("  → match these against the paths printed in B/C/D. Only wire a symbol "
                "when the fleet path is THE SAME MEASURE for THE SAME COUNTRY. "
                "The JPEXPYY case is exactly what happens when that rule is skipped.")

        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        rep.ok("PROBE DONE — paths dumped; wiring op follows for exact matches only")


if __name__ == "__main__":
    main()
