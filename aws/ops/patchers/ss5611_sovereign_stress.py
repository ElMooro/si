#!/usr/bin/env python3
"""Restore justhodl-sovereign-stress from last good blob, apply ops 5611 fixes."""
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-sovereign-stress/source/lambda_function.py"
BLOB = "442bbe4344dce20488cd6953d7a1de55e97f2aac"


def main() -> None:
    raw = subprocess.check_output(["git", "cat-file", "-p", BLOB], cwd=ROOT)
    t = raw.decode("utf-8")
    if "CISS_HEADLINE" not in t:
        raise SystemExit("blob is not sovereign-stress source")

    t = t.replace('VERSION = "2.4.6"', 'VERSION = "2.4.7"')

    old_pct = """def pct_change(obs, days):
    base = val_days_ago(obs, days)
    if base in (None, 0) or not obs:
        return None
    return round((obs[0][1] - base) / abs(base) * 100, 2)
"""
    new_pct = """def pct_change(obs, days):
    base = val_days_ago(obs, days)
    if base in (None, 0) or not obs:
        return None
    if abs(base) < 0.02:
        return None
    return round((obs[0][1] - base) / abs(base) * 100, 2)
"""
    if old_pct not in t:
        raise SystemExit("pct_change block missing")
    t = t.replace(old_pct, new_pct)

    old_wgb = """    return {
        "bond10y_pct": num("bond10y"),
        "cds_bp": num("lastCds"),
        "cds_default_prob_pct": num("lastCdsDefaultProb"),
        "spread_vs_bund_bp": num("mainSpreadValue"),
        "rating": d.get("lastRatingValue"),
        "cb_rate_pct": num("cbRateNumber"),
        "as_of": d.get("lastDataValDesc"),
    }
"""
    new_wgb = """    cds = num("lastCds")
    dprob = num("lastCdsDefaultProb")
    if cds is None:
        dprob = None
    return {
        "bond10y_pct": num("bond10y"),
        "cds_bp": cds,
        "cds_default_prob_pct": dprob,
        "spread_vs_bund_bp": num("mainSpreadValue"),
        "rating": d.get("lastRatingValue"),
        "cb_rate_pct": num("cbRateNumber"),
        "as_of": d.get("lastDataValDesc"),
    }
"""
    if old_wgb not in t:
        raise SystemExit("wgb return block missing")
    t = t.replace(old_wgb, new_wgb)

    old_ciss = """    for name, key in CISS_HEADLINE.items():
        obs = None
        # primary series (SS_CI works for EA/US); fall through to fallback (SS_CIN, which
        # is where china/UK live) on EITHER an empty result OR a 404/exception.
        try:
            obs = ecb_ciss(key)
        except Exception:
            obs = None
        if not obs and name in CISS_HEADLINE_FALLBACK:
            try:
                obs = ecb_ciss(CISS_HEADLINE_FALLBACK[name])
            except Exception as e:
                errors.append(f"CISS/{name}: fallback {str(e)[:40]}")
                continue
        if not obs:
            errors.append(f"CISS/{name}: empty")
            continue
"""
    new_ciss = """    for name, key in CISS_HEADLINE.items():
        obs = None
        cands = []
        for k in (key, CISS_HEADLINE_FALLBACK.get(name)):
            if not k:
                continue
            try:
                o = ecb_ciss(k)
            except Exception as e:
                errors.append(f"CISS/{name}/{k[-12:]}: {str(e)[:40]}")
                o = None
            if o:
                cands.append(o)
        if cands:
            obs = max(cands, key=lambda o: o[0][0])
        if not obs:
            errors.append(f"CISS/{name}: empty")
            continue
"""
    if old_ciss not in t:
        raise SystemExit("ciss fetch block missing")
    t = t.replace(old_ciss, new_ciss)

    old_rank = """    sov_ranked = sorted(
        ((k, v) for k, v in sov.items()
         if k != \"euro_area\" and v.get(\"percentile_5y\") is not None),
        key=lambda kv: kv[1][\"percentile_5y\"], reverse=True)
    most_stressed_sov = sov_ranked[0][0] if sov_ranked else None
"""
    # written with real quotes below
    old_rank = (
        "    sov_ranked = sorted(\n"
        "        ((k, v) for k, v in sov.items()\n"
        "         if k != \"euro_area\" and v.get(\"percentile_5y\") is not None),\n"
        "        key=lambda kv: kv[1][\"percentile_5y\"], reverse=True)\n"
        "    most_stressed_sov = sov_ranked[0][0] if sov_ranked else None\n"
    )
    old_rank = '''    sov_ranked = sorted(
        ((k, v) for k, v in sov.items()
         if k != "euro_area" and v.get("percentile_5y") is not None),
        key=lambda kv: kv[1]["percentile_5y"], reverse=True)
    most_stressed_sov = sov_ranked[0][0] if sov_ranked else None
'''
    new_rank = '''    def _obs_fresh(as_of, max_days=120):
        if not as_of:
            return False
        try:
            s = str(as_of)
            d = datetime.fromisoformat(s[:10] if len(s) >= 10 else s + "-01")
        except Exception:
            return False
        return (now.replace(tzinfo=None) - d).days <= max_days

    sov_ranked = sorted(
        ((k, v) for k, v in sov.items()
         if k != "euro_area" and v.get("percentile_5y") is not None
         and _obs_fresh(v.get("as_of"), 150)),
        key=lambda kv: kv[1]["percentile_5y"], reverse=True)
    most_stressed_sov = sov_ranked[0][0] if sov_ranked else None
'''
    if old_rank not in t:
        raise SystemExit("sov rank block missing")
    t = t.replace(old_rank, new_rank)

    old_worst = '''    worst_country = (max(country_scores, key=country_scores.get)
                     if country_scores else None)
'''
    new_worst = '''    worst_country = (max(country_scores, key=country_scores.get)
                     if country_scores else None)
    if most_stressed_sov:
        worst_country = most_stressed_sov
'''
    if old_worst not in t:
        raise SystemExit("worst_country block missing")
    t = t.replace(old_worst, new_worst)

    old_out = '''        "most_stressed_sovereign": most_stressed_sov,
        "equity_market_stress": equity,
'''
    new_out = '''        "most_stressed_sovereign": most_stressed_sov,
        "quality": {
            "observation_date": (ciss.get("euro_area") or {}).get("as_of"),
            "publication_date": now.date().isoformat(),
            "frequency": "mixed",
            "freshness_basis": "observation",
            "status": (
                "fresh" if _obs_fresh((ciss.get("euro_area") or {}).get("as_of"), 21)
                else "stale" if ciss else "unavailable"
            ),
            "ciss_ea_as_of": (ciss.get("euro_area") or {}).get("as_of"),
            "sovciss_ranking_eligible": bool(sov_ranked),
        },
        "equity_market_stress": equity,
'''
    if old_out not in t:
        raise SystemExit("out block missing")
    t = t.replace(old_out, new_out)

    compile(t, str(TARGET), "exec")
    TARGET.write_text(t)
    print("wrote", TARGET, "bytes", len(t.encode()))


if __name__ == "__main__":
    main()
