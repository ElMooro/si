"""wl_series — the series-level fusion bridge (ops 3244).

Any engine can attach the fleet's own curated tile data as a direct
model input WITHOUT new fetch load: the wl-engines runner already
fetches, throttles, tombstones and caches every curated series weekly in
data/thesis-state-v2.json.gz. This module reads that cache once per
invocation (memoized) and returns per-series stats blocks.

Contract:
    block({"btp_bund": ("TVC:BTPBUND", "BTP–Bund 10y spread"), ...},
          composite=False)
      → {"asof": iso, "series": {field: {"label", "last", "z_1y",
         "chg_13w_pct", "n_weeks"}}, ["composite_z": float]}

NEVER raises — any failure returns {}. Additive-only by design: callers
attach the result as one extra payload field and their existing model
is untouched.
"""
import gzip
import json
import math

_CACHE = {"state": None}


def _weekly():
    if _CACHE["state"] is not None:
        return _CACHE["state"]
    try:
        import boto3
        b = boto3.client("s3", region_name="us-east-1").get_object(
            Bucket="justhodl-dashboard-live",
            Key="data/thesis-state-v2.json.gz")["Body"].read()
        st = json.loads(gzip.decompress(b))
        _CACHE["state"] = (st.get("weekly") or {}, str(st.get("stamp")))
    except Exception:
        _CACHE["state"] = ({}, "")
    return _CACHE["state"]


def _stats(w):
    try:
        ks = sorted(w)
        vs = [float(w[k]) for k in ks]
        n = len(vs)
        if n < 8:
            return None
        last = vs[-1]
        tail = vs[-52:] if n >= 52 else vs
        mu = sum(tail) / len(tail)
        var = sum((v - mu) ** 2 for v in tail) / max(1, len(tail) - 1)
        sd = math.sqrt(var)
        z = round((last - mu) / sd, 2) if sd > 1e-12 else 0.0
        base = vs[-14] if n >= 14 else vs[0]
        chg = round(100.0 * (last - base) / abs(base), 2) \
            if abs(base) > 1e-12 else None
        return {"last": round(last, 4), "z_1y": z,
                "chg_13w_pct": chg, "n_weeks": n,
                "asof_week": ks[-1]}
    except Exception:
        return None


def block(spec, composite=False):
    """spec: {field: (TILE_SYMBOL, label)}. Never raises."""
    try:
        weekly, stamp = _weekly()
        out, zs = {}, []
        for field, (sym, label) in spec.items():
            w = weekly.get(str(sym).upper()) or {}
            st = _stats(w)
            if st:
                st["label"] = label
                st["symbol"] = sym
                out[field] = st
                zs.append(st["z_1y"])
        if not out:
            return {}
        res = {"asof": stamp, "series": out,
               "note": "series-level fusion via wl_series (fleet cache; "
                       "additive)"}
        if composite and zs:
            res["composite_z"] = round(sum(zs) / len(zs), 2)
            res["composite_n"] = len(zs)
        return res
    except Exception:
        return {}
