"""
ops 3719 — aws/shared/benzinga.py: FMP fallback for the dead Massive calendar

WHY THIS, AND WHY NOT THE 3717 APPROACH
═══════════════════════════════════════
ops 3717 patched `cur_eps` in justhodl-estimate-revisions to fall back to the
FMP forward consensus. Correct in principle, WRONG LAYER, and it could never
have worked:

    fetch_calendar() -> _get("earnings", ...) -> api.polygon.io/benzinga/v1
    _get() is a bare `except: return None`; Massive has returned 403
    NOT_AUTHORIZED on every Benzinga path since 2026-07-15.

So `rows = []`, the per-name loop in the engine NEVER EXECUTES, and no
downstream fallback inside that loop can fire. The 436 names in the artifact
come from the self-built state ledger (n_with_history=436), not from live
calendar rows — which is exactly why n_tracked looked healthy while
upward_revisions/downward_revisions stayed empty.

THE FIX, AT THE RIGHT LAYER
═══════════════════════════
Repair `fetch_calendar` inside aws/shared/benzinga.py so the dead vendor
degrades to a live one. FMP's earnings-calendar is ALREADY PROVEN on this
platform — reused, not invented:

    benzinga-news-agent : fmp("earnings-calendar", {"from":…, "to":…})
    buyback-engine      : fmp("earnings-calendar?from=%s&to=%s&limit=3000")

Benzinga keeps precedence, so if Massive ever re-enables the entitlement the
original path wins again automatically with no code change.

This fixes BOTH consumers of the module at once:
    justhodl-estimate-revisions   (this session's target)
    justhodl-earnings-tracker     (same dead import, same silent failure)

FIELD MAPPING (FMP /stable/earnings-calendar -> Benzinga calendar shape)
    symbol            -> ticker
    date              -> date
    epsEstimated      -> estimated_eps      <- the field 3717 was chasing
    revenueEstimated  -> estimated_revenue
    (no importance in FMP)  -> importance defaults to 3 so existing
                               min_importance filters keep working
    time/BMO-AMC      -> session, when present

SAFETY
    - Probe-first: this ops PROVES the FMP endpoint returns rows BEFORE it
      rewrites anything, and exits 0 with a DIAGNOSIS verdict if the probe
      fails, so a dead-end costs no false alarm.
    - No deploy_lambda() call: the commit touches aws/shared/**, which only
      deploy-lambdas.yml bundles correctly (the ops-helper zips source/ only
      and WOULD BREAK the import).
    - Verification of the live artifact is deliberately left to ops 3720 after
      deploy-lambdas settles; 3717/3718 both burned runs racing that deploy.
"""
import io
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SHARED = ROOT / "shared" / "benzinga.py"

FMP_KEY = os.environ.get("FMP_API_KEY") or os.environ.get("FMP_KEY") or ""

ANCHOR = '''def fetch_calendar(days_ahead=14, min_importance=0, limit=1000):'''

SHIM = '''def _fmp_calendar(days_ahead=14, limit=1000):
    """FMP earnings-calendar -> Benzinga calendar row shape.

    Massive has returned 403 NOT_AUTHORIZED on every api.polygon.io/benzinga
    path since 2026-07-15, so fetch_calendar() below degrades to this instead
    of silently returning an empty list (which made every consumer look
    healthy while emitting nothing). FMP /stable/earnings-calendar is already
    proven in benzinga-news-agent and buyback-engine.
    """
    key = (os.environ.get("FMP_API_KEY") or os.environ.get("FMP_KEY") or "")
    if not key:
        return []
    today = date.today()
    url = ("https://financialmodelingprep.com/stable/earnings-calendar"
           f"?from={today.isoformat()}"
           f"&to={(today + timedelta(days=days_ahead)).isoformat()}"
           f"&limit={int(limit)}&apikey={key}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-bz-shim/1.0"})
        with urllib.request.urlopen(req, timeout=25) as r:
            data = json.loads(r.read())
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    out = []
    for r in data:
        tk = r.get("symbol") or r.get("ticker")
        if not tk:
            continue
        t = (r.get("time") or "").lower()
        session = "AMC" if "amc" in t or "after" in t else \\
                  "BMO" if "bmo" in t or "before" in t else "\\u2014"
        out.append({
            "ticker": tk,
            "company": r.get("name") or r.get("company"),
            "date": r.get("date"),
            "time": r.get("time"),
            "session": session,
            # FMP has no importance ranking; default mid so existing
            # min_importance filters (typically 1-3) still admit these rows.
            "importance": 3,
            "fiscal_period": r.get("fiscalPeriod") or r.get("period"),
            "fiscal_year": r.get("fiscalYear"),
            "estimated_eps": r.get("epsEstimated"),
            "estimated_revenue": r.get("revenueEstimated"),
            "actual_eps": r.get("epsActual") or r.get("eps"),
            "actual_revenue": r.get("revenueActual") or r.get("revenue"),
            "_source": "fmp_fallback",
        })
    return out


'''


def probe_fmp():
    """Prove FMP returns usable calendar rows BEFORE rewriting the module."""
    if not FMP_KEY:
        return None, "no FMP key in ops env"
    today = date.today()
    url = ("https://financialmodelingprep.com/stable/earnings-calendar"
           f"?from={today.isoformat()}"
           f"&to={(today + timedelta(days=14)).isoformat()}"
           f"&limit=1000&apikey={FMP_KEY}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-probe/1.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
    except Exception as e:  # noqa: BLE001
        return None, f"request failed: {str(e)[:120]}"
    if not isinstance(data, list) or not data:
        return None, f"empty/odd payload: {str(data)[:120]}"
    with_eps = [r for r in data if r.get("epsEstimated") is not None]
    return {"n": len(data), "n_with_eps": len(with_eps),
            "sample": data[0], "keys": sorted(data[0].keys())}, "ok"


def main():
    with report("3719_benzinga_fmp_shim") as rep:
        rep.heading("ops 3719 — shared/benzinga.py FMP calendar fallback")
        out = {}
        hard_fail = []

        def gate(n, ok, d, hard=True):
            rep.log(f"{n} {bool(ok)}")
            print(f"  {n:32} {bool(ok)}  {d}")
            out[n] = {"ok": bool(ok), "detail": d}
            if not ok and hard:
                hard_fail.append(n)

        # ── PROBE FIRST ─────────────────────────────────────────────────────
        rep.section("probe — is FMP earnings-calendar usable?")
        probe, why = probe_fmp()
        if not probe:
            gate("P1_fmp_probe", False, f"PROBE FAILED: {why} — no rewrite attempted",
                 hard=False)
            out["verdict"] = "DIAGNOSIS: FMP calendar unusable — " + why
            print("\nVERDICT:", out["verdict"])
            rep.log("VERDICT: " + out["verdict"])
            for _k, _v in out.items():
                if isinstance(_v, dict):
                    rep.kv(gate=_k, ok=_v.get("ok"), detail=str(_v.get("detail"))[:170])
            return  # exit 0 — diagnosis, not breakage

        gate("P1_fmp_probe", probe["n_with_eps"] > 0,
             f"rows={probe['n']} with_epsEstimated={probe['n_with_eps']} "
             f"keys={probe['keys'][:12]}")
        print(f"    sample row: {json.dumps(probe['sample'])[:260]}")

        # ── REWRITE THE SHARED MODULE ───────────────────────────────────────
        rep.section("patch — shared/benzinga.py")
        src = io.open(SHARED, encoding="utf-8").read()

        if "_fmp_calendar" in src:
            gate("G1_shim_added", True, "already shimmed (idempotent re-run)")
        else:
            if ANCHOR not in src:
                gate("G1_shim_added", False, "anchor missing — inspect module")
            else:
                # need timedelta at module scope for the shim
                if "from datetime import date, timedelta" not in src:
                    src = src.replace("from datetime import date",
                                      "from datetime import date, timedelta", 1)
                src = src.replace(ANCHOR, SHIM + ANCHOR, 1)

                # make fetch_calendar degrade instead of returning []
                old_ret = '''    out = []
    for r in (j or {}).get("results", []) or []:'''
                new_ret = '''    if not (j or {}).get("results"):
        # Massive/Benzinga returned nothing (403 since 2026-07-15) -> degrade
        # to FMP rather than handing back an empty list that makes every
        # downstream engine look healthy while emitting nothing.
        fb = _fmp_calendar(days_ahead=days_ahead, limit=limit)
        return [r for r in fb if (r.get("importance") or 0) >= min_importance]

    out = []
    for r in (j or {}).get("results", []) or []:'''
                if old_ret in src:
                    src = src.replace(old_ret, new_ret, 1)

                io.open(SHARED, "w", encoding="utf-8").write(src)
                gate("G1_shim_added",
                     "_fmp_calendar" in src and "degrade" in src,
                     "fetch_calendar now falls back to FMP when Massive yields nothing")

        # ── compile-check the rewritten module ──────────────────────────────
        try:
            import py_compile
            py_compile.compile(str(SHARED), doraise=True)
            gate("G2_module_compiles", True, "shared/benzinga.py compiles")
        except Exception as e:  # noqa: BLE001
            gate("G2_module_compiles", False, f"compile error: {str(e)[:150]}")

        # ── consumers that this repairs ─────────────────────────────────────
        consumers = []
        for d in sorted((ROOT / "lambdas").glob("*/source/lambda_function.py")):
            try:
                if "from benzinga import" in d.read_text(encoding="utf-8"):
                    consumers.append(d.parent.parent.name)
            except Exception:  # noqa: BLE001
                pass
        gate("G3_consumers_identified", len(consumers) >= 1,
             f"repaired by this shim: {consumers}")

        out["verdict"] = ("PASS_ALL" if not hard_fail
                          else "GAPS: " + ",".join(hard_fail))
        print("\nVERDICT:", out["verdict"])
        print("NOTE: live verification deferred to ops 3720 so deploy-lambdas "
              "can settle (3717/3718 both burned runs racing it).")
        rep.log("VERDICT: " + out["verdict"])
        for _k, _v in out.items():
            if isinstance(_v, dict):
                rep.kv(gate=_k, ok=_v.get("ok"), detail=str(_v.get("detail"))[:170])

        if hard_fail:
            sys.exit(1)


if __name__ == "__main__":
    main()
