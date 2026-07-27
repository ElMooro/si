"""
ops_3971 — SNB series identification (read-only).

ops 3970 overturned a standing assumption: the SNB rendoblid cube is NOT
unparseable. It returns 200, 5.6MB, 22 timeseries with 7,534 observations
each. What was missing was (a) knowing WHICH of the 22 series is the 2Y and
the 3Y, and (b) freshness — the default pull ends 2025-07-31, a year stale,
which matches the registry note that the plain cube is stale while
?fromDate= is fresh.

This op prints every series header in full and re-pulls with fromDate so the
wiring op can alias CH02Y / CH03Y to exact series indices with a current
value. Read-only.
"""
import json
import sys
import urllib.request
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0",
      "Accept-Encoding": "identity"}


def get(url, timeout=60):
    try:
        r = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, resp.read()
    except Exception as e:
        return getattr(e, "code", 0), str(e).encode()[:300]


def main():
    with report("3971_snb_identify") as rep:
        rep.heading("ops 3971 — identify SNB CH02Y / CH03Y series + freshness")
        checks = []

        rep.section("A. every series header in the rendoblid cube")
        st, body = get("https://data.snb.ch/api/cube/rendoblid/data/json/en")
        rep.kv(status=st, bytes=len(body))
        d = json.loads(body)
        ts = d.get("timeseries") or []
        idx2y = idx3y = None
        for i, t in enumerate(ts):
            hdr = t.get("header") or []
            desc = " | ".join(f"{h.get('dim')}={h.get('dimItem')}" for h in hdr)
            vals = t.get("values") or []
            last = vals[-1] if vals else None
            rep.log(f"  [{i:2d}] n={len(vals):<6} last={str(last)[:34]}  {desc[:150]}")
            low = desc.lower()
            if idx2y is None and ("2 year" in low or "2-year" in low or "2y" in low):
                idx2y = i
            if idx3y is None and ("3 year" in low or "3-year" in low or "3y" in low):
                idx3y = i
        rep.kv(index_2y=idx2y, index_3y=idx3y, n_series=len(ts))
        checks.append(("2Y and 3Y series located", idx2y is not None and idx3y is not None))

        rep.section("B. freshness — re-pull with fromDate")
        frm = (date.today() - timedelta(days=90)).isoformat()
        st2, body2 = get(f"https://data.snb.ch/api/cube/rendoblid/data/json/en?fromDate={frm}")
        rep.kv(status=st2, bytes=len(body2), fromDate=frm)
        fresh = {}
        try:
            d2 = json.loads(body2)
            ts2 = d2.get("timeseries") or []
            rep.log(f"  series returned: {len(ts2)}")
            for i, t in enumerate(ts2):
                vals = [v for v in (t.get("values") or []) if v.get("value") is not None]
                if not vals:
                    continue
                hdr = " | ".join(f"{h.get('dimItem')}" for h in (t.get("header") or []))
                fresh[i] = (hdr, vals[-1])
                if i < 24:
                    rep.log(f"  [{i:2d}] last={vals[-1]}  {hdr[:120]}")
        except Exception as e:
            rep.log(f"  parse: {type(e).__name__}: {str(e)[:140]}")
        checks.append(("fromDate pull returns data", bool(fresh)))

        rep.section("C. verdict")
        if idx2y is not None:
            rep.log(f"  CH02Y -> rendoblid series index {idx2y}")
        if idx3y is not None:
            rep.log(f"  CH03Y -> rendoblid series index {idx3y}")
        rep.log("  NOTE: index position is not a stable contract. The wiring op must "
                "match on the HEADER TEXT at fetch time, never on a hardcoded index, "
                "or an SNB reordering silently repoints CH02Y at another tenor.")

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"IDENTIFIED — 2Y idx {idx2y}, 3Y idx {idx3y}, fresh series {len(fresh)}")


if __name__ == "__main__":
    main()
