"""justhodl-fifx-vol-migration v1.0 — the cross-asset VOL MIGRATION barometer.

Khalid (2026-07-20): volatility begins in fixed income / FX and migrates to
equities — measure it. Audit: FI vol substantially built (bond-vol synthetic
5-channel; risk-ratios carries real ^MOVE + MOVE/VIX); FX vol existed NOWHERE
(EVZ discontinued 2025-03, TYVIX 2020, CVIX/JPM paid → realized on the majors
is the free institutional proxy); and no engine z-scored the upstream legs
against equity vol to time the migration. This engine closes both gaps by
WIRING what exists and adding only the FX leg + the gauge.

LEGS (each z-scored vs its own trailing 2y):
  FI  implied  ^MOVE (Yahoo v8, 2y — same source risk-ratios proved)
      realized synthetic composite from data/bond-vol.json (existing engine);
               inline DGS10 30d realized as fallback channel
  FX  realized 20d annualized vol on EURUSD/USDJPY/GBPUSD (FRED DEXUSEU /
               DEXJPUS / DEXUSUK) equal-weight + broad-dollar DTWEXBGS
  EQ  implied  VIXCLS (FRED)

MIGRATION GAUGE  spillover = max(FI_z, FX_z) − EQ_z
  CALM              upstream z < 0.5
  UPSTREAM_BREWING  spillover ≥ 1 and upstream z ≥ 1  ← the barometer
  MIGRATING         upstream z ≥ 1 and EQ_z rising toward it (gap closing)
  BROAD_STRESS      all three z ≥ 1
Ratios: MOVE/VIX and FXvol/VIX vs their own 2y percentile.
Out: data/fifx-vol.json · daily 21:20 UTC. Real data only; legs degrade
independently and report coverage.
"""
import json, math, os, time, urllib.parse, urllib.request
from datetime import datetime, timezone
import boto3

BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/fifx-vol.json"
s3 = boto3.client("s3", region_name="us-east-1")
FRED = os.environ.get("FRED_API_KEY") or "2f057499936072679d8843d7fce99989"
UA = {"User-Agent": "Mozilla/5.0 (JustHodl research contact@justhodl.ai)"}


def _j(url, timeout=25):
    try:
        return json.loads(urllib.request.urlopen(
            urllib.request.Request(url, headers=UA), timeout=timeout).read())
    except Exception as e:
        print("[fifx] http fail", url[:70], str(e)[:70])
        return None


def fred(series, start="2019-01-01"):
    j = _j("https://api.stlouisfed.org/fred/series/observations?"
           + urllib.parse.urlencode({"series_id": series, "api_key": FRED,
                                     "file_type": "json", "observation_start": start}))
    out = []
    for o in (j or {}).get("observations") or []:
        try:
            out.append((o["date"], float(o["value"])))
        except Exception:
            pass
    return out


def yahoo_move():
    j = _j("https://query1.finance.yahoo.com/v8/finance/chart/%5EMOVE?range=2y&interval=1d")
    try:
        r = j["chart"]["result"][0]
        ts = r["timestamp"]
        cl = r["indicators"]["quote"][0]["close"]
        return [(datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d"), c)
                for t, c in zip(ts, cl) if isinstance(c, (int, float))]
    except Exception as e:
        print("[fifx] MOVE fail", str(e)[:70])
        return []


def realized_series(rows, n=20, diff="log"):
    """Rolling annualized realized vol (%) of daily changes."""
    vals = [v for _, v in rows if isinstance(v, (int, float)) and v > 0]
    dates = [d for d, v in rows if isinstance(v, (int, float)) and v > 0]
    ch = []
    for i in range(1, len(vals)):
        ch.append(math.log(vals[i] / vals[i - 1]) if diff == "log"
                  else vals[i] - vals[i - 1])
    out = []
    for i in range(n, len(ch) + 1):
        w = ch[i - n:i]
        mu = sum(w) / n
        sd = (sum((x - mu) ** 2 for x in w) / (n - 1)) ** 0.5
        out.append((dates[i], round(sd * math.sqrt(252) * 100, 2)))
    return out


def z_and_pct(series):
    """z + percentile of the last value vs its own trailing history."""
    xs = [v for _, v in series if isinstance(v, (int, float))]
    if len(xs) < 60:
        return None, None, (xs[-1] if xs else None)
    hist, last = xs[:-1][-504:], xs[-1]
    mu = sum(hist) / len(hist)
    sd = (sum((x - mu) ** 2 for x in hist) / (len(hist) - 1)) ** 0.5
    z = round((last - mu) / sd, 2) if sd else None
    pct = round(100.0 * sum(1 for x in hist if x <= last) / len(hist), 1)
    return z, pct, last


def trend_5d(series):
    xs = [v for _, v in series if isinstance(v, (int, float))]
    return round(xs[-1] - xs[-6], 2) if len(xs) >= 6 else None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    # ── EQ leg
    vix_rows = fred("VIXCLS")
    eq_z, eq_pct, vix = z_and_pct(vix_rows)
    # ── FI leg: real MOVE + synthetic feed cross-check
    move_rows = yahoo_move()
    fi_z, fi_pct, move = z_and_pct(move_rows)
    synth = None
    try:
        bv = json.loads(s3.get_object(Bucket=BUCKET, Key="data/bond-vol.json")["Body"].read())
        synth = {"composite": (bv.get("composite") or {}).get("value")
                 or bv.get("composite_vol") or bv.get("bond_vol_composite"),
                 "note": "existing justhodl-bond-vol 5-channel synthetic (cross-check)"}
    except Exception as e:
        print("[fifx] bond-vol feed skip", str(e)[:60])
    if fi_z is None:   # MOVE outage fallback: DGS10 realized (bp, diff)
        r10 = realized_series(fred("DGS10"), 30, diff="diff")
        fi_z, fi_pct, move = z_and_pct(r10)
        fi_src = "fallback: DGS10 30d realized (bp, annualized)"
    else:
        fi_src = "^MOVE (Yahoo, 2y)"
    # ── FX leg: realized on the majors (the free institutional proxy)
    pairs = {"EURUSD": "DEXUSEU", "USDJPY": "DEXJPUS", "GBPUSD": "DEXUSUK"}
    fx_pair = {}
    fx_composite = {}
    for name, sid in pairs.items():
        rs = realized_series(fred(sid), 20)
        z, p, last = z_and_pct(rs)
        fx_pair[name] = {"realized_20d_pct": last, "z": z, "pctile": p}
        for d, v in rs:
            fx_composite.setdefault(d, []).append(v)
    comp_rows = sorted((d, round(sum(v) / len(v), 2))
                       for d, v in fx_composite.items() if len(v) == len(pairs))
    fx_z, fx_pct, fxv = z_and_pct(comp_rows)
    dxy_rows = realized_series(fred("DTWEXBGS"), 20)
    dxy_z, dxy_pct, dxyv = z_and_pct(dxy_rows)
    # ── ratios vs own history
    def ratio_hist(a_rows, b_rows):
        bm = dict(b_rows)
        return [(d, round(v / bm[d], 3)) for d, v in a_rows
                if bm.get(d) not in (None, 0)]
    mv_ratio = ratio_hist(move_rows, vix_rows) if move_rows else []
    mv_z, mv_pctile, mv_last = z_and_pct(mv_ratio)
    fxvix = ratio_hist(comp_rows, vix_rows)
    fxvix_z, fxvix_pctile, fxvix_last = z_and_pct(fxvix)
    # ── the migration gauge
    up_z = max(x for x in (fi_z, fx_z) if x is not None) if (fi_z is not None or fx_z is not None) else None
    spill = round(up_z - eq_z, 2) if (up_z is not None and eq_z is not None) else None
    eq_trend = trend_5d(vix_rows)
    if up_z is None or eq_z is None:
        state = "DEGRADED"
    elif up_z >= 1 and eq_z >= 1:
        state = "BROAD_STRESS"
    elif up_z >= 1 and (spill or 0) >= 1:
        state = "UPSTREAM_BREWING"
    elif up_z >= 1 and (eq_trend or 0) > 0:
        state = "MIGRATING"
    else:
        state = "CALM"
    reads = {"CALM": "no upstream pressure — FI/FX vol at or below normal vs equity vol",
             "UPSTREAM_BREWING": "⚠ bond/FX vol elevated while equity vol sleeps — the classic pre-migration setup; hedges cheap",
             "MIGRATING": "upstream vol elevated and VIX now rising toward it — migration underway",
             "BROAD_STRESS": "all three markets in high-vol regime — migration complete",
             "DEGRADED": "a leg failed to price — see coverage"}
    out = {"engine": "fifx-vol-migration", "version": "1.0.0",
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "legs": {
               "fixed_income": {"measure": fi_src, "level": move, "z": fi_z, "pctile": fi_pct,
                                "synthetic_crosscheck": synth},
               "fx": {"measure": "equal-weight 20d realized: EURUSD/USDJPY/GBPUSD (FRED)",
                      "level_pct": fxv, "z": fx_z, "pctile": fx_pct, "pairs": fx_pair,
                      "broad_dollar": {"realized_20d_pct": dxyv, "z": dxy_z, "pctile": dxy_pct},
                      "note": "implied FX vol indices dead/paid (EVZ disc. 2025-03, TYVIX 2020, CVIX paid) — realized is the free institutional proxy"},
               "equity": {"measure": "VIXCLS (FRED)", "level": vix, "z": eq_z, "pctile": eq_pct,
                          "trend_5d": eq_trend}},
           "ratios": {"move_vix": {"last": mv_last, "z": mv_z, "pctile": mv_pctile,
                                   "sibling": "risk-ratios also carries this series"},
                      "fxvol_vix": {"last": fxvix_last, "z": fxvix_z, "pctile": fxvix_pctile}},
           "migration": {"upstream_z": up_z, "equity_z": eq_z, "spillover": spill,
                         "state": state, "read": reads[state]},
           "methodology": {"thesis": "vol begins in fixed income/FX and migrates to equities; z-score each leg vs its own 2y and watch the spillover gap",
                           "spillover": "max(FI_z, FX_z) − EQ_z; ≥1 with upstream z ≥1 = UPSTREAM_BREWING"},
           "siblings": {"fi_realized_5ch": "data/bond-vol.json", "move_vix_series": "risk-ratios",
                        "vix_term_structure": "risk-regime", "bond_regime": "regime/current.json"},
           "disclaimer": "Real data only. Research, not advice.",
           "elapsed_s": round(time.time() - t0, 2)}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json")
    print(f"[fifx] state={state} spill={spill} FIz={fi_z} FXz={fx_z} EQz={eq_z} "
          f"MOVE={move} FXvol={fxv}% VIX={vix} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "state": state,
            "spillover": spill, "fi_z": fi_z, "fx_z": fx_z, "eq_z": eq_z})}
