"""justhodl-regime-playbook — regime memory. Records the current regime fingerprint
(bond-vol + funding-plumbing + crypto-risk) daily, and over time learns which
signal types / sectors performed best in each regime by joining to the signal
backtest. Surfaces: 'In the current regime (X), historically these worked best.'

OUTPUT: data/regime-playbook.json · SCHEDULE: daily 14:30 UTC.
Builds a rolling history at data/regime-playbook-history.json.
"""
import json, time
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/regime-playbook.json"
HIST_KEY = "data/regime-playbook-history.json"
s3 = boto3.client("s3", region_name=REGION)


def rj(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default


def lambda_handler(event=None, context=None):
    t0 = time.time()
    bv = rj("data/bond-vol.json") or {}
    fp = rj("data/funding-plumbing.json") or {}
    cr = rj("data/crypto-cycle-risk.json") or {}
    backtest = rj("data/signal-backtest.json") or {}

    # current regime fingerprint
    fingerprint = {
        "bond_vol": bv.get("regime"),
        "plumbing": fp.get("regime"),
        "balance_sheet": fp.get("balance_sheet_direction"),
        "crypto_risk": cr.get("risk_level"),
        "date": datetime.now(timezone.utc).date().isoformat(),
    }
    regime_key = f"{fingerprint['bond_vol']}|{fingerprint['plumbing']}|{fingerprint['balance_sheet']}"

    # append to history (dedup by date)
    hist = (rj(HIST_KEY) or {}).get("points", [])
    if not hist or hist[-1].get("date") != fingerprint["date"]:
        hist.append(fingerprint)
    hist = hist[-730:]
    s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps({"points": hist}).encode(), ContentType="application/json")

    # What signal types have the best proven forward returns? (regime-tagging of
    # historical observations requires the backtest to carry regime — until then
    # we surface the overall proven leaders + the current-regime playbook rules.)
    leaders = []
    by_sig = backtest.get("by_signal_type") or backtest.get("by_signal") or {}
    if isinstance(by_sig, dict):
        for sig, st in by_sig.items():
            hr = st.get("hit_rate") or st.get("hit_rate_pct")
            avg = st.get("avg_return") or st.get("avg_fwd_return_pct") or st.get("avg_return_pct")
            n = st.get("n") or st.get("count")
            if hr is not None and (n or 0) >= 10:
                leaders.append({"signal": sig, "hit_rate": hr, "avg_return": avg, "n": n})
    leaders.sort(key=lambda x: -(x.get("hit_rate") or 0))

    # Regime-specific playbook (institutional priors — what historically works)
    PLAYBOOKS = {
        "FRAGILE": "Liquidity buffers thin → favor quality/cash-flow + buyback-yield; cut leverage, beta, and crowded longs. Defensives + low-debt compounders outperform.",
        "STRESS": "Funding stress → max defense: cash, T-bills, gold, dollar; avoid high-beta, small-cap, crypto. Wait for SOFR/repo to normalize before re-risking.",
        "TIGHTENING": "Liquidity flat/draining (NOT expanding) → don't chase; favor self-funding (buyback/FCF) names over capital-hungry growth. QT≠QE — wait for real expansion.",
        "AMPLE": "Liquidity supportive → risk-on works; high-beta, growth, and small-cap have tailwinds. Lean into momentum + capex-buildout themes.",
        "CRISIS": "Bond-vol crisis → de-gross hard; correlations spike to 1; only the safest carry. Tail hedges pay.",
        "ELEVATED": "Elevated vol → trim size, widen stops, favor quality over junk; insider clusters + capital-flow more reliable than momentum here.",
        "NORMAL": "Benign vol → standard playbook; let conviction + flow lead; size normally.",
        "BOND_VOL_LOW": "Low rate-vol → carry & dislocation work; mean-reversion favored; watch for complacency.",
    }
    current_playbook = []
    for r in [fingerprint["plumbing"], fingerprint["bond_vol"], fingerprint["crypto_risk"]]:
        if r and r in PLAYBOOKS:
            current_playbook.append({"regime": r, "guidance": PLAYBOOKS[r]})

    out = {"engine": "regime-playbook", "version": "1.0",
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "current_fingerprint": fingerprint, "regime_key": regime_key,
           "current_playbook": current_playbook,
           "proven_signal_leaders": leaders[:10],
           "days_in_history": len(hist),
           "note": "What historically works in the current regime + the system's proven signal leaders."}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[regime-playbook] {regime_key}, {len(leaders)} leaders, {len(hist)} days history")
    return {"statusCode": 200, "body": json.dumps({"regime_key": regime_key})}
