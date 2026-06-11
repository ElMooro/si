"""
justhodl-rotation-radar v1.0 — Altseason & Leadership-Rotation Cascades
=======================================================================
Khalid's pattern, measured then generalized: "ETH/BTC hitting bottom and
ripping has preceded every altseason." A ratio instrument bottoming and
thrusting is the season-starter for its higher-beta complex — in crypto
(ETH/BTC → alts) and in equities (high-beta/low-vol, small/large, equal/cap,
XBI, ARKK → spec leadership).

Doctrine: event-study FIRST. For every historical bottom→thrust on each
ratio (new 180d low, then ≥X% off-low within 60d), measure the real forward
returns of the complex it allegedly ignites (alt basket in BTC & USD terms;
IWM/SPY relative and XBI absolute) with n. Live detectors score today's
state; fresh thrusts log to the closed loop at measured confidence; a
server-side Claude strategist narrates ONLY these tables.
"""
import json, os, time, re, urllib.request
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/rotation-radar.json"
POLY_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
VERSION = "1.1.1"
UA = {"User-Agent": "JustHodl Research admin@justhodl.ai"}


def cg(path, tries=3):
    for i in range(tries):
        try:
            req = urllib.request.Request(
                f"https://api.coingecko.com/api/v3{path}", headers=UA)
            return json.loads(urllib.request.urlopen(req, timeout=45).read())
        except Exception as e:
            if i == tries - 1:
                print(f"[cg] {path[:40]}: {str(e)[:50]}")
                DIAG.append(f"coingecko {path[:38]}: {str(e)[:60]}")
                return None
            time.sleep(8)


def cg_daily(coin, vs="btc"):
    j = cg(f"/coins/{coin}/market_chart?vs_currency={vs}&days=max&interval=daily")
    if not j:
        return []
    out = {}
    for ts, px in j.get("prices", []):
        out[datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date().isoformat()] = px
    return sorted(out.items())


DIAG = []


def cb_daily(product, start="2016-06-01"):
    """Coinbase Exchange public daily candles, paginated (300/req), no key."""
    out = {}
    try:
        cur = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_all = datetime.now(timezone.utc)
        while cur < end_all:
            seg_end = min(cur + timedelta(days=295), end_all)
            u = (f"https://api.exchange.coinbase.com/products/{product}/candles"
                 f"?granularity=86400&start={cur.strftime('%Y-%m-%dT00:00:00Z')}"
                 f"&end={seg_end.strftime('%Y-%m-%dT00:00:00Z')}")
            req = urllib.request.Request(u, headers=UA)
            j = json.loads(urllib.request.urlopen(req, timeout=40).read())
            if isinstance(j, list):
                for row in j:
                    try:
                        d = datetime.fromtimestamp(row[0], tz=timezone.utc).date().isoformat()
                        out[d] = float(row[4])
                    except Exception:
                        pass
            else:
                DIAG.append(f"coinbase {product}: {str(j)[:60]}")
            cur = seg_end
            time.sleep(0.13)
    except Exception as e:
        DIAG.append(f"coinbase {product}: {str(e)[:70]}")
    return sorted(out.items())


def crypto_history():
    """Source ladder for the crypto leg; every failure lands in DIAG."""
    ethbtc = cb_daily("ETH-BTC", "2016-06-01")
    DIAG.append(f"coinbase ETH-BTC: {len(ethbtc)} pts")
    btc_usd = dict(cb_daily("BTC-USD", "2015-01-15"))
    eth_usd = dict(cb_daily("ETH-USD", "2016-06-01"))
    DIAG.append(f"coinbase BTC-USD/ETH-USD: {len(btc_usd)}/{len(eth_usd)} pts")
    alts = {}
    for prod, since in (("LTC-USD", "2016-09-01"), ("LINK-USD", "2019-07-01"),
                         ("ADA-USD", "2021-03-20"), ("DOGE-USD", "2021-06-03"),
                         ("SOL-USD", "2021-06-01")):
        s = dict(cb_daily(prod, since))
        ab = {d: v / btc_usd[d] for d, v in s.items() if btc_usd.get(d)}
        if len(ab) > 400:
            alts[prod.split("-")[0]] = ab
    DIAG.append(f"alt basket coins (USD/BTC-derived): {len(alts)}")
    if len(ethbtc) < 500:
        cgs = cg_daily("ethereum", "btc")
        DIAG.append(f"coingecko ETH/BTC fallback: {len(cgs)} pts")
        if len(cgs) > len(ethbtc):
            ethbtc = cgs
    if len(ethbtc) < 500:
        pe, pb = dict(poly_closes("X:ETHUSD")), dict(poly_closes("X:BTCUSD"))
        ks = sorted(set(pe) & set(pb))
        pr = [(k, pe[k] / pb[k]) for k in ks if pb[k]]
        DIAG.append(f"polygon X-pair fallback: {len(pr)} pts")
        if len(pr) > len(ethbtc):
            ethbtc = pr
        if not eth_usd:
            eth_usd = pe
        if not btc_usd:
            btc_usd = pb
    return ethbtc, alts, btc_usd, eth_usd


def poly_closes(t, days=4200):
    end = datetime.now(timezone.utc).date().isoformat()
    start = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
    u = (f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/{start}/{end}"
         f"?adjusted=true&sort=asc&limit=50000&apiKey={POLY_KEY}")
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=50).read())
        return [(datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).date().isoformat(),
                 float(r["c"])) for r in (j.get("results") or [])]
    except Exception as e:
        print(f"[poly] {t}: {str(e)[:40]}")
        return []


def thrust_events(series, low_win=180, thrust_pct=25.0, thrust_win=60, cooldown=120):
    """Bottom→rip detector: makes a `low_win` low, then closes ≥thrust_pct above
    that low within thrust_win sessions. Returns event indices at THRUST
    confirmation (tradeable moment, no lookahead)."""
    v = [x for _, x in series]
    n = len(v)
    evs = []
    i = low_win
    while i < n:
        if v[i] <= min(v[i - low_win:i]):           # fresh low
            lo, j = v[i], i + 1
            limit = min(i + thrust_win, n)
            while j < limit:
                if v[j] <= lo:
                    lo = v[j]
                if v[j] >= lo * (1 + thrust_pct / 100):
                    evs.append(j)
                    i = j + cooldown
                    break
                j += 1
            else:
                i = j
        else:
            i += 1
    return evs


def fwd(series_map, dates, i, w, ref=None):
    d0 = dates[i]
    tgt = (datetime.strptime(d0, "%Y-%m-%d") + timedelta(days=w)).date().isoformat()
    keys = sorted(series_map)
    if not keys:
        return None
    import bisect
    j0 = bisect.bisect_left(keys, d0)
    j1 = bisect.bisect_left(keys, tgt)
    if j0 >= len(keys) or j1 >= len(keys):
        return None
    def near(k, dd, tol=6):
        return abs((datetime.strptime(k, "%Y-%m-%d")
                     - datetime.strptime(dd, "%Y-%m-%d")).days) <= tol
    if not near(keys[j0], d0) or not near(keys[j1], tgt):
        return None  # event predates coverage or target beyond it — honest null
    a, b = series_map[keys[j0]], series_map[keys[j1]]
    return round((b / a - 1) * 100, 1) if a else None


def stats(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return None
    xs.sort()
    return {"n": len(xs), "median_pct": xs[len(xs) // 2],
            "pos_pct": round(100 * sum(1 for x in xs if x > 0) / len(xs), 1)}


def live_state(series):
    d = [x for x, _ in series]
    v = [x for _, x in series]
    if len(v) < 200:
        return None
    lo180 = min(v[-180:])
    hi365 = max(v[-365:]) if len(v) >= 365 else max(v)
    lo_idx = len(v) - 180 + v[-180:].index(lo180)
    return {"value": v[-1], "as_of": d[-1],
            "off_180d_low_pct": round((v[-1] / lo180 - 1) * 100, 1),
            "days_since_180d_low": len(v) - 1 - lo_idx,
            "vs_365d_high_pct": round((v[-1] / hi365 - 1) * 100, 1),
            "mom_20d_pct": round((v[-1] / v[-21] - 1) * 100, 1) if len(v) > 21 else None,
            "thrust_live": bool((v[-1] / lo180 - 1) * 100 >= 25
                                 and len(v) - 1 - lo_idx <= 60)}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    out = {"engine": "rotation-radar", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat()}

    # ── CRYPTO: ETH/BTC + alt basket (in BTC and USD) ──
    DIAG.clear()
    ethbtc, alts, btc_usd, eth_usd = crypto_history()
    crypto = {"available": bool(len(ethbtc) > 500 and alts)}
    if crypto["available"]:
        dates = [d for d, _ in ethbtc]
        # equal-weight alt basket in BTC terms (normalized at each coin's first common date)
        basket = {}
        for d in dates:
            vals = []
            for c, m in alts.items():
                if d in m:
                    base = next(iter(sorted(m.items())))[1]
                    vals.append(m[d] / base)
            if len(vals) >= 3:
                basket[d] = sum(vals) / len(vals)
        evs = thrust_events(ethbtc)
        rows = []
        for i in evs:
            rows.append({"thrust_date": dates[i],
                          "ethbtc_at_thrust": round(ethbtc[i][1], 5),
                          "alts_btc_fwd": {w: fwd(basket, dates, i, w) for w in (30, 90, 180)},
                          "alts_usd_proxy_eth_fwd": {w: fwd(eth_usd, dates, i, w)
                                                      for w in (30, 90, 180)},
                          "btc_usd_fwd_90": fwd(btc_usd, dates, i, 90)})
        crypto["event_study"] = {
            "definition": "ETH/BTC makes a 180d low then closes ≥25% off that low "
                           "within 60 sessions (thrust = tradeable confirmation)",
            "events": rows,
            "alts_in_btc": {str(w): stats([r["alts_btc_fwd"][w] for r in rows])
                             for w in (30, 90, 180)},
            "eth_usd": {str(w): stats([r["alts_usd_proxy_eth_fwd"][w] for r in rows])
                         for w in (30, 90, 180)}}
        crypto["live"] = {"ethbtc": live_state(ethbtc)}
        # BTC-dominance proxy: ETH/BTC slope + basket/BTC slope combined
        b_ser = sorted(basket.items())
        crypto["live"]["alt_basket_btc"] = live_state(b_ser)
    crypto["diagnostics"] = list(DIAG)
    out["crypto"] = crypto

    # ── EQUITIES: appetite ratios → spec-complex sequels ──
    legs = {}
    for t in ("SPHB", "SPLV", "RSP", "SPY", "IWM", "XBI", "ARKK", "QQQ"):
        legs[t] = dict(poly_closes(t))
    def ratio(a, b):
        ks = sorted(set(legs[a]) & set(legs[b]))
        return [(k, legs[a][k] / legs[b][k]) for k in ks]
    RATIOS = {"highbeta_lowvol": ratio("SPHB", "SPLV"),
               "smallcap_large": ratio("IWM", "SPY"),
               "equalweight_cap": ratio("RSP", "SPY"),
               "biotech_mkt": ratio("XBI", "SPY"),
               "spec_growth": ratio("ARKK", "QQQ")}
    iwm_rel = dict(ratio("IWM", "SPY"))
    eq = {}
    for name, ser in RATIOS.items():
        if len(ser) < 600:
            continue
        dts = [d for d, _ in ser]
        evs = thrust_events(ser, thrust_pct=12.0)   # equities: 12% ratio thrust
        rows = [{"thrust_date": dts[i],
                  "iwm_rel_fwd": {w: fwd(iwm_rel, dts, i, w) for w in (30, 90, 180)},
                  "xbi_fwd_90": fwd(legs["XBI"], dts, i, 90),
                  "spy_fwd_90": fwd(legs["SPY"], dts, i, 90)} for i in evs]
        eq[name] = {"events": rows,
                     "iwm_rel": {str(w): stats([r["iwm_rel_fwd"][w] for r in rows])
                                  for w in (30, 90, 180)},
                     "xbi_90": stats([r["xbi_fwd_90"] for r in rows]),
                     "live": live_state(ser)}
    out["equity"] = {"definition": "ratio makes 180d low then ≥12% off-low within "
                                     "60 sessions; sequels on IWM/SPY relative + XBI",
                      "ratios": eq}

    # composite ignition scores
    def score(lv):
        if not lv:
            return None
        s = 0
        if lv["thrust_live"]:
            s += 55
        s += max(0, min(25, lv["off_180d_low_pct"]))
        if (lv["mom_20d_pct"] or 0) > 0:
            s += 10
        if lv["days_since_180d_low"] <= 90:
            s += 10
        return min(100, round(s))
    out["scores"] = {
      "crypto_altseason": score((crypto.get("live") or {}).get("ethbtc")),
      "equity_rotation": (round(sum(filter(None, [score(v.get("live"))
                            for v in eq.values()])) / max(1, len([1 for v in eq.values()
                            if v.get("live")])), 1) if eq else None)}

    # closed loop: fresh thrusts (≤3 sessions old)
    n_logged = 0
    nowt = datetime.now(timezone.utc)
    d0 = nowt.strftime("%Y-%m-%d")
    def log(sid, why, conf, px, bench):
        nonlocal n_logged
        try:
            DDB.Table("justhodl-signals").put_item(Item={
                "signal_id": sid, "signal_type": "rotation_thrust",
                "signal_value": why[:40], "predicted_direction": "UP",
                "confidence": Decimal(str(conf)), "measure_against": "ticker",
                "baseline_price": str(px), "benchmark": bench,
                "check_windows": ["day_21", "day_63"],
                "check_timestamps": {f"day_{w}": (nowt + timedelta(days=w)).isoformat()
                                      for w in (21, 63)},
                "outcomes": {}, "accuracy_scores": {}, "logged_at": nowt.isoformat(),
                "logged_epoch": int(nowt.timestamp()), "status": "pending",
                "schema_version": "2", "horizon_days_primary": 63,
                "regime_at_log": "ROTATION",
                "ttl": int(nowt.timestamp()) + 150 * 86400,
                "metadata": {"engine": "rotation-radar", "v": VERSION},
                "rationale": why})
            n_logged += 1
        except Exception as e:
            print(f"[log] {str(e)[:60]}")
    ce = (crypto.get("event_study") or {}).get("events") or []
    if ce and ce[-1]["thrust_date"] >= (nowt - timedelta(days=4)).date().isoformat():
        st90 = crypto["event_study"]["eth_usd"].get("90") or {}
        px = list(eth_usd.values())[-1] if eth_usd else None
        if px and st90.get("n"):
            log(f"altseason-thrust#ETH#{d0}",
                f"ETH/BTC bottom→thrust confirmed {ce[-1]['thrust_date']}; historical "
                f"sequel: ETH/USD +90d median {st90['median_pct']}% ({st90['pos_pct']}%+, "
                f"n={st90['n']})",
                round(min(0.66, 0.40 + (st90["pos_pct"] / 100) * 0.3), 2), px, "SPY")
    for name, v in eq.items():
        rows = v.get("events") or []
        if rows and rows[-1]["thrust_date"] >= (nowt - timedelta(days=4)).date().isoformat():
            st = (v.get("iwm_rel") or {}).get("90") or {}
            px = list(legs["IWM"].values())[-1]
            if st.get("n"):
                log(f"rotation-thrust-{name}#IWM#{d0}",
                    f"{name} ratio thrust {rows[-1]['thrust_date']}; IWM/SPY +90d median "
                    f"{st['median_pct']}% ({st['pos_pct']}%+, n={st['n']})",
                    round(min(0.64, 0.40 + (st["pos_pct"] / 100) * 0.3), 2), px, "SPY")
    out["signals_logged"] = n_logged

    # ── server-side Claude strategist (tables only) ──
    ai = {"error": None}
    try:
        if ANTHROPIC_KEY:
            compact = {"scores": out["scores"],
                        "crypto_live": crypto.get("live"),
                        "crypto_sequel_tables": (crypto.get("event_study") or {}).get("alts_in_btc"),
                        "crypto_events_tail": ce[-3:],
                        "equity_live": {k: v.get("live") for k, v in eq.items()},
                        "equity_iwm_tables": {k: v.get("iwm_rel") for k, v in eq.items()}}
            prompt = ("You are a CROSS-ASSET ROTATION STRATEGIST. Using ONLY the measured "
                       "event-study tables and live states below, write JSON keys: verdict "
                       "(<=160 chars), altseason_read (is the ETH/BTC season-starter armed, "
                       "cite off-low %, days-since-low, and the sequel table n), "
                       "equity_rotation_read (which appetite ratio is closest to thrust), "
                       "what_confirms (the exact numeric trigger), what_kills, watch_next "
                       "(array of 3). Never invent history beyond the tables. <360 words. "
                       "JSON only.\n\nDATA:\n" + json.dumps(compact, default=str))
            req = urllib.request.Request(
                "https://api.anthropic.com/v1/messages",
                data=json.dumps({"model": "claude-haiku-4-5-20251001", "max_tokens": 2000,
                                  "messages": [{"role": "user", "content": prompt}]}).encode(),
                headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01",
                         "content-type": "application/json"})
            rj = json.loads(urllib.request.urlopen(req, timeout=90).read())
            txt = "".join(b.get("text", "") for b in rj.get("content", [])
                           if b.get("type") == "text")
            txt = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", txt)
            ai.update(json.loads(txt[txt.find("{"):txt.rfind("}") + 1]))
        else:
            ai["error"] = "no ANTHROPIC_API_KEY"
    except Exception as e:
        ai["error"] = str(e)[:120]
    out["ai_brief"] = ai

    out["methodology"] = (
        "Season-starter doctrine, measured: bottom→thrust events on ETH/BTC (180d low, "
        "≥25% off-low ≤60 sessions) and five equity appetite ratios (≥12%), each with "
        "real forward sequels (alt basket in BTC, ETH/USD, IWM/SPY relative, XBI) and n. "
        "Live detectors score thrust-armed states; fresh thrusts log to the closed loop "
        "at sequel-table confidence. Coinbase Exchange daily candles (primary) with CoinGecko/Polygon fallbacks — "
        "coverage honest.")
    out["duration_s"] = round(time.time() - t0, 1)
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[rotation] scores={out['scores']} crypto_ev={len(ce)} "
          f"eq={[(k, len(v.get('events') or [])) for k, v in eq.items()]} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps(out["scores"])}
