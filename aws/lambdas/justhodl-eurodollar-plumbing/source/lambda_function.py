"""
justhodl-eurodollar-plumbing — offshore U.S. dollar funding monitor.

The existing justhodl-eurodollar-stress engine is a generic financial-stress
composite (VIX, OAS, broad dollar). This engine tracks the ACTUAL eurodollar
plumbing: can non-U.S. banks/funds/corporates roll dollar funding cheaply through
repo, CP/CD, FX swaps and central-bank backstops? A dollar shortage can occur even
while the dollar falls, so price alone is useless — you watch the funding system.

LAYERS (all real, free, authoritative — FRED/NY Fed/FMP):
  1. US money-market core   SOFR, SOFR99, EFFR, OBFR, IORB, SOFR-IORB, ON RRP, reserves, TGA
  2. Bank/short-term funding 3M financial CP-OIS spread, nonfinancial CP, 3M bill
  3. Credit backdrop         HY OAS, IG OAS
  4. Central-bank backstops  Fed central-bank liquidity swaps (SWPT) — THE smoking-gun tell
  5. Settlement plumbing     Treasury fails percentile (reuses data/settlement-fails.json)
  6. FX / offshore strain    broad dollar trend (true cross-currency basis needs an FX-swap
                             forward feed not in entitlements — flagged, not fabricated)
  7. Country hubs            Hong Kong USD/HKD peg-band position, USD/JPY (FMP spot)

Each metric → green/yellow/red vs institutional thresholds → composite plumbing-health
0-100. GLM (tier=reason; Claude credits exhausted) scans the board → FUNCTIONING /
STRAINED / SEIZING verdict + short-term lean. Output data/eurodollar-plumbing.json.
"""
import os, json, time, urllib.request, urllib.parse, datetime, statistics

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
FMP_KEY = os.environ.get("FMP_API_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
S3 = boto3.client("s3", region_name="us-east-1")
OUT_KEY = "data/eurodollar-plumbing.json"
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"


def http_get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-eurodollar/2.0"})
    return urllib.request.urlopen(req, timeout=timeout).read()


def num(v):
    try:
        return None if v in (None, "", ".") else float(v)
    except (TypeError, ValueError):
        return None


def fred(series_id, days=2000):
    start = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()
    qs = urllib.parse.urlencode({"series_id": series_id, "api_key": FRED_KEY, "file_type": "json",
                                 "observation_start": start, "sort_order": "asc"})
    try:
        d = json.loads(http_get(FRED_BASE + "?" + qs).decode())
        out = [(o["date"], num(o["value"])) for o in d.get("observations", []) if num(o["value"]) is not None]
        return out
    except Exception as e:
        print("[ed] fred %s: %s" % (series_id, e))
        return []


def latest(series):
    return series[-1] if series else (None, None)


def pctile(value, hist):
    h = sorted([x for x in hist if x is not None])
    if not h or value is None:
        return None
    below = sum(1 for x in h if x <= value)
    return round(below / len(h) * 100.0, 1)


def fmp_fx(symbol):
    """Latest FX rate from FMP quote (e.g. USDHKD, USDJPY). Returns float or None."""
    try:
        d = json.loads(http_get("https://financialmodelingprep.com/stable/quote?symbol=%s&apikey=%s"
                                % (symbol, FMP_KEY)).decode())
        row = d[0] if isinstance(d, list) and d else (d if isinstance(d, dict) else {})
        return num(row.get("price"))
    except Exception as e:
        print("[ed] fmp %s: %s" % (symbol, e))
        return None


def gj(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def flag(value, green_max, yellow_max, higher_is_worse=True):
    """green/yellow/red against absolute thresholds."""
    if value is None:
        return "unknown"
    if higher_is_worse:
        return "green" if value <= green_max else "yellow" if value <= yellow_max else "red"
    return "green" if value >= green_max else "yellow" if value >= yellow_max else "red"


def metric(mid, label, value, unit, status, detail, hist_pctile=None, asof=None):
    return {"id": mid, "label": label, "value": value, "unit": unit, "status": status,
            "detail": detail, "pctile": hist_pctile, "asof": asof}


def build_layers():
    layers = {}

    # ---- Layer 1: US money-market core ----
    sofr = fred("SOFR"); iorb = fred("IORB"); effr = fred("EFFR"); obfr = fred("OBFR")
    rrp = fred("RRPONTSYD"); res = fred("WRESBAL"); tga = fred("WTREGEN"); sofr99 = fred("SOFR99")
    core = []
    sd, sv = latest(sofr); idt, iv = latest(iorb)
    if sv is not None and iv is not None:
        spread = round((sv - iv) * 100, 1)  # bps
        core.append(metric("sofr_iorb", "SOFR − IORB (repo richness)", spread, "bp",
                            flag(spread, 5, 15), "Overnight repo vs the Fed floor; sustained >15bp = repo pressure / reserve scarcity", asof=sd))
    s9d, s9v = latest(sofr99)
    if s9v is not None and iv is not None:
        t = round((s9v - iv) * 100, 1)
        core.append(metric("sofr99_iorb", "SOFR 99th pct − IORB (repo tail)", t, "bp",
                            flag(t, 15, 40), "Tail of the repo distribution — squeezed dealers pay up here first", asof=s9d))
    rd, rv = latest(rrp)
    if rv is not None:
        core.append(metric("on_rrp", "ON RRP balances", round(rv, 1), "$bn",
                            flag(rv, 50, 5, higher_is_worse=False),
                            "Cash parked at the Fed; a drained RRP (→0) removes the liquidity buffer above reserves", asof=rd))
    wd, wv = latest(res)
    if wv is not None:
        res_hist = [v for _, v in res]
        core.append(metric("reserves", "Bank reserve balances", round(wv / 1000, 1), "$tn",
                            flag(pctile(wv, res_hist), 40, 20, higher_is_worse=False),
                            "System reserves; scarcity is what turns RRP drain into repo spikes", pctile(wv, res_hist), wd))
    td, tv = latest(tga)
    if tv is not None:
        core.append(metric("tga", "Treasury General Account", round(tv / 1000, 2), "$tn", "info",
                            "A rising TGA (e.g. post-debt-ceiling rebuild) drains reserves dollar-for-dollar", asof=td))
    if effr and obfr and iv is not None:
        ed, ev = latest(effr)
        core.append(metric("effr_iorb", "EFFR − IORB", round((ev - iv) * 100, 1), "bp",
                            flag(abs((ev - iv) * 100), 3, 8), "Fed funds vs floor — corridor position", asof=ed))
    layers["us_core"] = {"title": "US money-market core", "metrics": core}

    # ---- Layer 2: bank / short-term funding ----
    cp = fred("DCPF3M"); cpn = fred("DCPN3M"); bill = fred("DTB3")
    bank = []
    cd, cv = latest(cp); _, sv2 = latest(sofr)
    if cv is not None and sv2 is not None:
        cpois = round((cv - sv2) * 100, 1)
        bank.append(metric("cp_ois", "3M financial CP − SOFR", cpois, "bp",
                            flag(cpois, 25, 75), "Foreign/large banks' short-term dollar cost over the risk-free rate; >100bp = CP market freezing", asof=cd))
    nd, nv = latest(cpn)
    if nv is not None and sv2 is not None:
        bank.append(metric("cpn_ois", "3M nonfinancial CP − SOFR", round((nv - sv2) * 100, 1), "bp",
                            flag(round((nv - sv2) * 100, 1), 20, 60), "Corporate short-term dollar funding premium", asof=nd))
    bd, bv = latest(bill)
    if bv is not None and sv2 is not None:
        bank.append(metric("bill_ois", "SOFR − 3M T-bill", round((sv2 - bv) * 100, 1), "bp",
                            flag(round((sv2 - bv) * 100, 1), 20, 45), "Wide spread = flight-to-bills / collateral scramble", asof=bd))
    layers["bank_funding"] = {"title": "Bank & short-term funding", "metrics": bank}

    # ---- Layer 3: credit backdrop ----
    hy = fred("BAMLH0A0HYM2"); ig = fred("BAMLC0A0CM")
    credit = []
    for sid, lab, ser, gy, yy in [("hy_oas", "HY credit OAS", hy, 400, 600), ("ig_oas", "IG credit OAS", ig, 120, 175)]:
        dd, vv = latest(ser)
        if vv is not None:
            credit.append(metric(sid, lab, round(vv, 0), "bp", flag(vv, gy, yy),
                                  "Credit risk premium; funding stress and credit stress reinforce", pctile(vv, [v for _, v in ser]), dd))
    layers["credit"] = {"title": "Credit backdrop", "metrics": credit}

    # ---- Layer 4: central-bank backstops (THE tell) ----
    swp = fred("SWPT")
    back = []
    swd, swv = latest(swp)
    if swv is not None:
        swvb = round(swv / 1000, 2)  # millions → $bn
        back.append(metric("fed_swaps", "Fed central-bank liquidity swaps", swvb, "$bn",
                            flag(swvb, 1, 10), "Dollars lent to ECB/BoE/BoJ/SNB/BoC. ~0 normally; ANY sustained rise = offshore dollar shortage (peaked ~$450bn in 2020)", asof=swd))
    layers["backstops"] = {"title": "Central-bank backstops", "metrics": back}

    # ---- Layer 5: settlement plumbing (reuse fails engine) ----
    fails = gj("data/settlement-fails.json") or {}
    plumb = []
    try:
        f = (fails.get("headline") or fails.get("ust_ex_tips") or {})
        pct = f.get("percentile") or (fails.get("regime") or {}).get("percentile")
        val = f.get("latest") or f.get("value")
        if pct is not None:
            plumb.append(metric("ust_fails", "Treasury settlement fails", round(val, 0) if val else None, "$bn",
                                 flag(pct, 80, 95), "Fails-to-deliver/receive; collateral hard to source when this spikes", round(pct, 1)))
    except Exception:
        pass
    layers["settlement"] = {"title": "Settlement plumbing", "metrics": plumb}

    # ---- Layer 6: FX / offshore strain ----
    dollar = fred("DTWEXBGS")
    fx = []
    dd, dv = latest(dollar)
    if dv is not None:
        dh = [v for _, v in dollar]
        fx.append(metric("broad_dollar", "Broad trade-weighted USD", round(dv, 2), "idx",
                         flag(pctile(dv, dh), 75, 90), "A surging dollar tightens offshore funding; proxy for cross-currency strain", pctile(dv, dh), dd))
    fx.append(metric("xccy_basis", "Cross-currency basis (EUR/JPY/GBP…)", None, "bp", "unavailable",
                     "True basis needs an FX forward/swap-points feed (not in current data entitlements). Proxied above by broad-dollar strain; a dedicated CIP feed is the upgrade path."))
    layers["fx"] = {"title": "FX & offshore strain", "metrics": fx}

    # ---- Layer 7: country hubs ----
    hubs = []
    hkd = fmp_fx("USDHKD")
    if hkd is not None:
        # peg band 7.75 (strong) – 7.85 (weak). Near 7.85 = HKD weak, liquidity tightening.
        st = "green" if hkd <= 7.82 else "yellow" if hkd <= 7.848 else "red"
        hubs.append(metric("hk_peg", "Hong Kong USD/HKD (band 7.75–7.85)", round(hkd, 4), "", st,
                            "At the weak-side 7.85, HKMA sells USD/buys HKD → Aggregate Balance shrinks, HIBOR jumps"))
    jpy = fmp_fx("USDJPY")
    if jpy is not None:
        hubs.append(metric("jpy", "USD/JPY (hedging-cost context)", round(jpy, 2), "", "info",
                            "Japan is the largest offshore dollar borrower; basis/hedge cost not directly sourced, spot shown for context"))
    layers["hubs"] = {"title": "Country hubs", "metrics": hubs}

    return layers


def composite(layers):
    pts = {"green": 0, "yellow": 1, "red": 2}
    weight = {"us_core": 1.0, "bank_funding": 1.1, "credit": 0.6, "backstops": 1.8,
              "settlement": 1.0, "fx": 0.7, "hubs": 0.8}
    num_, den_, reds, yellows = 0.0, 0.0, [], []
    for lk, lv in layers.items():
        w = weight.get(lk, 1.0)
        for m in lv["metrics"]:
            if m["status"] in pts:
                num_ += pts[m["status"]] * w
                den_ += 2 * w
                if m["status"] == "red":
                    reds.append(m["label"])
                elif m["status"] == "yellow":
                    yellows.append(m["label"])
    stress = (num_ / den_ * 100) if den_ else 0
    health = round(100 - stress, 1)
    verdict = ("FUNCTIONING" if health >= 78 else "MILD STRAIN" if health >= 60
               else "STRAINED" if health >= 40 else "SEIZING")
    return health, verdict, reds, yellows


def ai_scan(layers, health, verdict, reds, yellows):
    rows = []
    for lk, lv in layers.items():
        for m in lv["metrics"]:
            if m["value"] is not None:
                rows.append("%s: %s%s [%s]" % (m["label"], m["value"], m["unit"], m["status"]))
    board = "\n".join(rows)
    prompt = ("You are a money-market desk strategist. Below is today's offshore U.S. dollar "
              "funding board (green=normal, yellow=watch, red=stress; the Fed central-bank liquidity "
              "swap line is the key backstop tell — near zero is healthy). Composite plumbing-health "
              "is %s/100 (%s). Reds: %s. Yellows: %s.\n\nBOARD:\n%s\n\n"
              "Return ONLY JSON: {\"state\": \"FUNCTIONING|STRAINED|SEIZING\", "
              "\"summary\": \"<=3 sentences, plain, is the eurodollar market working or seizing and why\", "
              "\"short_term\": \"<=2 sentences on the likely 1-4 week direction (improving/stable/deteriorating) and the trigger to watch\", "
              "\"key_drivers\": [\"metric: why\", ...]}"
              % (health, verdict, ", ".join(reds) or "none", ", ".join(yellows) or "none", board))
    try:
        from llm_router import complete
        raw = complete(prompt, tier="reason", max_tokens=1500)
        import re
        m = re.search(r"\{.*\}", raw, re.S)
        return json.loads(m.group(0)) if m else {"error": "no json", "raw": (raw or "")[:160]}
    except Exception as e:
        return {"error": str(e)[:160]}


def lambda_handler(event, context):
    t0 = time.time()
    layers = build_layers()
    health, verdict, reds, yellows = composite(layers)
    ai = ai_scan(layers, health, verdict, reds, yellows)
    payload = {
        "engine": "justhodl-eurodollar-plumbing", "version": "1.0",
        "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "plumbing_health": health, "verdict": verdict,
        "red_flags": reds, "yellow_flags": yellows,
        "ai": ai,
        "layers": layers,
        "methodology": "Offshore dollar funding plumbing across 7 layers from FRED/NY Fed/FMP. Each metric "
                       "graded green/yellow/red on institutional thresholds; composite health 0-100 weights the "
                       "central-bank swap-line backstop and bank-funding layers most. True cross-currency basis "
                       "requires an FX forward/swap feed not in current entitlements and is proxied by broad-dollar strain.",
        "honesty": "A dollar shortage can occur even while the dollar falls — this watches the funding system, not price. "
                   "Empirical/threshold readings, not a guarantee; analysis, not investment advice.",
        "duration_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, indent=2, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=900")
    print("[ed] done %.1fs health=%s verdict=%s reds=%d" % (payload["duration_s"], health, verdict, len(reds)))
    return {"statusCode": 200, "body": json.dumps({"ok": True, "health": health, "verdict": verdict})}
