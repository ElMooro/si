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
  7. Country hubs            HK USD/HKD peg + HKMA funding; offshore-yuan CNH-CNY escape-valve
                             + CNH HIBOR (TMA) overnight/3M; USD/JPY context

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


TMA_CNH_URL = "https://www.tma.org.hk/en_market_more_ib.aspx"


def cnh_hibor():
    """Latest CNH HIBOR fixings (offshore-yuan funding rate, TMA-administered) scraped from
    the TMA benchmark page, which server-renders a table of the last ~5 business days x 8
    tenors. We take the most-recent column. Returns {date, on, m1, m3} in %, or {} on failure.
    A spiking overnight CNH HIBOR is the PBoC-squeeze tell (2016 hit >60%); paired with a wide
    CNH-CNY spot gap it confirms a deliberate offshore-liquidity drain."""
    import re
    try:
        req = urllib.request.Request(TMA_CNH_URL, headers={"User-Agent":
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"})
        html = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "ignore")
    except Exception as e:
        print("[ed] cnh hibor fetch: %s" % e)
        return {}
    text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))  # strip tags → token stream
    out = {}
    md = re.search(r"(\d{2}/\d{2}/\d{4})", text)
    if md:
        out["date"] = md.group(1)
    for key, label in (("on", "ON"), ("m1", "1M"), ("m3", "3M")):
        m = re.search(r"\b%s\b\s+([0-9]+\.[0-9]+)" % re.escape(label), text)
        if m:
            v = float(m.group(1))
            if 0 < v < 200:  # sanity (CNH HIBOR can spike to 60%+ but not beyond)
                out[key] = v
    return out


def ofr_fsi():
    """OFR Financial Stress Index (financialresearch.gov, free daily CSV, no key). Columns:
    Date, OFR FSI, Credit, Equity valuation, Safe assets, Funding, Volatility, United States,
    Other advanced economies, Emerging markets. Returns {date, fsi, funding, other_adv, em} from
    the last row, or {}. FSI is signed (>0 = above-average stress); category cols are contributions."""
    try:
        txt = http_get("https://www.financialresearch.gov/financial-stress-index/data/fsi.csv").decode("utf-8", "ignore")
    except Exception as e:
        print("[ed] ofr fsi: %s" % e)
        return {}
    lines = [l for l in txt.splitlines() if l.strip()]
    if len(lines) < 2:
        return {}
    hdr = [h.strip().lower() for h in lines[0].split(",")]
    def idx(name):
        return hdr.index(name) if name in hdr else None
    i_fsi, i_fund = idx("ofr fsi"), idx("funding")
    i_oa, i_em = idx("other advanced economies"), idx("emerging markets")
    for ln in reversed(lines[1:]):
        c = ln.split(",")
        if i_fund is None or len(c) <= i_fund:
            continue
        if num(c[i_fund]) is not None:
            return {"date": c[0], "fsi": num(c[i_fsi]) if i_fsi is not None else None,
                    "funding": num(c[i_fund]),
                    "other_adv": num(c[i_oa]) if (i_oa is not None and len(c) > i_oa) else None,
                    "em": num(c[i_em]) if (i_em is not None and len(c) > i_em) else None}
    return {}


def ecb_ilm(key, n=8):
    """ECB ILM weekly series via SDMX CSV (data-api.ecb.europa.eu). Returns [(period, value_EUR_mn)]
    oldest→newest, or []. Same API path the platform already uses for ECB CISS."""
    try:
        txt = http_get("https://data-api.ecb.europa.eu/service/data/ILM/%s?format=csvdata&lastNObservations=%d"
                       % (key, n)).decode("utf-8", "ignore")
    except Exception as e:
        print("[ed] ecb ilm %s: %s" % (key, e))
        return []
    lines = [l for l in txt.splitlines() if l.strip()]
    if len(lines) < 2:
        return []
    hdr = lines[0].split(",")
    if "TIME_PERIOD" not in hdr or "OBS_VALUE" not in hdr:
        return []
    di, vi = hdr.index("TIME_PERIOD"), hdr.index("OBS_VALUE")
    out = []
    for ln in lines[1:]:
        c = ln.split(",")
        if len(c) > max(di, vi) and num(c[vi]) is not None:
            out.append((c[di], num(c[vi])))
    return out


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
        trend = ""
        if len(res) > 66 and res[-66][1]:
            trend = " (%+.0f$bn over 13wk)" % ((wv - res[-66][1]) / 1e3)
        core.append(metric("reserves", "Bank reserve balances", round(wv / 1e6, 2), "$tn", "info",
                            "System reserves%s; scarcity — not level — is the signal, and SOFR-IORB above is still the cleanest tell" % trend, asof=wd))
    td, tv = latest(tga)
    if tv is not None:
        core.append(metric("tga", "Treasury General Account", round(tv / 1e6, 2), "$tn", "info",
                            "A rising TGA (e.g. post-debt-ceiling rebuild) drains bank reserves dollar-for-dollar", asof=td))
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
    fsi = ofr_fsi()
    if fsi.get("funding") is not None:
        fc = round(fsi["funding"], 3)
        st = "green" if fc <= 0 else "yellow" if fc <= 0.5 else "red"
        bank.append(metric("ofr_fsi_funding", "OFR FSI — funding stress contribution", fc, "σ", st,
                           "Office of Financial Research daily stress index, funding category — its signed contribution "
                           "to total stress (>0 = above-average funding stress). Independent 33-variable cross-check; "
                           "headline OFR FSI %s, other-advanced %s, EM %s."
                           % (("%.2f" % fsi["fsi"]) if fsi.get("fsi") is not None else "n/a",
                              ("%.2f" % fsi["other_adv"]) if fsi.get("other_adv") is not None else "n/a",
                              ("%.2f" % fsi["em"]) if fsi.get("em") is not None else "n/a"), asof=fsi.get("date")))
    layers["bank_funding"] = {"title": "Bank & short-term funding", "metrics": bank}

    # ---- Layer 3: credit backdrop ----
    hy = fred("BAMLH0A0HYM2"); ig = fred("BAMLC0A0CM")
    credit = []
    for sid, lab, ser, gy, yy in [("hy_oas", "HY credit OAS", hy, 400, 600), ("ig_oas", "IG credit OAS", ig, 120, 175)]:
        dd, vv = latest(ser)
        if vv is not None:
            bps = round(vv * 100, 0)  # FRED OAS series are in percent
            credit.append(metric(sid, lab, bps, "bp", flag(bps, gy, yy),
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
    fima = fred("H41RESPPALGTRFNWW")  # FIMA: Fed repo lending to foreign official accounts (H.4.1, weekly, $mn)
    fimd, fimv = latest(fima)
    if fimv is not None:
        fimb = round(fimv / 1000, 2)  # $mn → $bn
        back.append(metric("fima_repo", "Fed FIMA repo (foreign-official $ borrowing)", fimb, "$bn",
                            flag(fimb, 1, 10), "Foreign central banks borrowing dollars from the Fed against their USTs "
                            "rather than selling them. ~0 normally; ANY sustained use = offshore dollar shortage (the "
                            "quieter alternative to dumping custody holdings).", asof=fimd))
    srf = fred("WORAL")  # total Fed repo lending incl Standing Repo Facility take-up (H.4.1, weekly, $mn)
    srd, srv = latest(srf)
    if srv is not None:
        srb = round(srv / 1000, 2)
        back.append(metric("fed_repo_srf", "Fed repo / SRF take-up", srb, "$bn",
                            flag(srb, 5, 50), "Total Fed repo lending incl. the Standing Repo Facility. ~0 in normal "
                            "times; a spike = domestic repo stress / dealers tapping the backstop (Sept-2019 hit $100bn+).",
                            asof=srd))
    ecb_usd = ecb_ilm("W.U2.C.A030000.U2.Z06")  # Eurosystem FX(USD) claims on EA residents = ECB USD lending to EA banks
    if ecb_usd:
        ep, ev = ecb_usd[-1]
        evb = round(ev / 1000.0, 1)  # €mn → €bn
        back.append(metric("ecb_usd_provision", "ECB USD lending to euro-area banks", evb, "€bn",
                            flag(evb, 40, 100), "Eurosystem foreign-currency (mostly USD) claims on euro-area residents — "
                            "dollars the ECB has lent to euro-area banks via its USD operations (funded by the Fed swap "
                            "line). Near-zero in calm times; balloons when euro-area banks can't source dollars in the "
                            "market (~€130bn in Mar-2020). The euro-area mirror of the Fed swap-line tell.", asof=ep))
    layers["backstops"] = {"title": "Central-bank backstops", "metrics": back}

    # ---- Layer 5: settlement plumbing (reuse fails engine) ----
    fails = gj("data/settlement-fails.json") or {}
    plumb = []
    hl = fails.get("headline") or {}
    sig = fails.get("signal") or {}
    val = hl.get("combined_bn")
    pct = hl.get("pctile")
    if val is not None and pct is not None:
        plumb.append(metric("ust_fails", "Treasury settlement fails (UST ex-TIPS)",
                            round(val, 0), "$bn", flag(pct, 80, 95),
                            "Fails-to-deliver + receive; collateral becomes hard to source when this spikes (NY Fed FR2004 regime: %s)"
                            % sig.get("regime", "?"),
                            round(pct, 1), asof=fails.get("as_of")))
    # per-class FTD/FTR detail (corporates + agency MBS are the secondary tells)
    for cls in (fails.get("classes") or []):
        lab = cls.get("label", "")
        st = cls.get("stats") or {}
        if lab and lab != hl.get("label") and st.get("latest") is not None and st.get("pctile") is not None:
            plumb.append(metric("fails_%s" % lab[:6], "Fails · %s" % lab, round(st["latest"], 0), "$bn",
                                flag(st["pctile"], 80, 95), "Secondary collateral-sourcing tell", round(st["pctile"], 1)))
    layers["settlement"] = {"title": "Settlement plumbing", "metrics": plumb}

    # ---- Layer 6: FX / offshore strain ----
    dollar = fred("DTWEXBGS")
    fx = []
    dd, dv = latest(dollar)
    if dv is not None:
        dh = [v for _, v in dollar]
        fx.append(metric("broad_dollar", "Broad trade-weighted USD", round(dv, 2), "idx",
                         flag(pctile(dv, dh), 75, 90), "A surging dollar tightens offshore funding; proxy for cross-currency strain", pctile(dv, dh), dd))
    # Stablecoins as an offshore-dollar proxy: USDT/USDC are dollar liabilities circulating
    # largely OUTSIDE the US banking system. Net minting = offshore dollar CREATION (eases global
    # $ funding); contraction = offshore $ drain — a genuine, real-time eurodollar tell.
    # Read from the crypto-liquidity engine (small + reliable) rather than re-fetching DefiLlama.
    try:
        _cl = gj("data/crypto-liquidity.json") or {}
        _ss = _cl.get("stablecoin_supply") or {}
        flow30 = _ss.get("chg_30d_pct"); tot = _ss.get("total_usd")
        if flow30 is not None:
            st = "green" if flow30 >= 0 else "yellow" if flow30 >= -4 else "red"
            fx.append(metric("stablecoin_offshore_usd", "Stablecoin supply (offshore-$ proxy, 30d Δ)",
                             round(float(flow30), 2), "%", st,
                             "Stablecoins (USDT/USDC) are dollar liabilities circulating largely OFFSHORE. "
                             "Net minting = offshore-dollar CREATION (eases global $ funding); contraction = "
                             "offshore-dollar drain / tightening.%s"
                             % ((" Total supply $%.0fbn." % (tot / 1e9)) if tot else "")))
    except Exception as _sce:
        print("[eurodollar] stablecoin offshore-$ metric failed:", str(_sce)[:80])
    cust = fred("WMTSECL1")  # FRBNY custody of marketable USTs held for foreign official/intl accounts (weekly)
    cd2, cv2 = latest(cust)
    if cv2 is not None and len(cust) > 13 and cust[-14][1]:
        chg = round((cv2 / cust[-14][1] - 1) * 100, 2)  # ~13-week % change
        st = "green" if chg >= -1 else "yellow" if chg >= -4 else "red"
        fx.append(metric("foreign_custody", "Fed custody of USTs for foreign central banks (13wk Δ)", chg, "%", st,
                         "FRBNY-held Treasuries for foreign official accounts (level $%.2ftn). A sharp DROP = foreign "
                         "central banks selling/repo-ing USTs to raise scarce dollars — the classic dollar-shortage "
                         "tell (fell hard in 2015-16, 2018-19, Mar-2020, 2022)." % (cv2 / 1e6), asof=cd2))
    netdue = fred("NDFACBW027SBOG")  # Net due to related foreign offices, all commercial banks (H.8, weekly, $bn)
    ndd, ndv = latest(netdue)
    if ndv is not None and len(netdue) > 13 and netdue[-14][1] is not None:
        sw = round(ndv - netdue[-14][1], 0)  # 13-week $bn swing
        st = "green" if abs(sw) < 80 else "yellow" if abs(sw) < 160 else "red"
        direction = "inflow to US offices" if sw > 0 else "outflow — foreign offices pulling dollar funding"
        fx.append(metric("net_due_foreign", "Net due to related foreign offices (13wk swing)", sw, "$bn", st,
                         "Net dollars US-located banks (incl. foreign branches) owe their foreign parents — the "
                         "balance-sheet footprint of eurodollar interbank funding. Level $%.0fbn; %+.0f$bn over 13wk "
                         "(%s). Large rapid swings = cross-border dollar repositioning/stress." % (ndv, sw, direction),
                         asof=ndd))
    fx.append(metric("xccy_basis", "Cross-currency basis (EUR/JPY/GBP…)", None, "bp", "unavailable",
                     "True basis needs an FX forward/swap-points feed (not in current data entitlements). Proxied above by broad-dollar strain; a dedicated CIP feed is the upgrade path."))
    layers["fx"] = {"title": "FX & offshore strain", "metrics": fx}

    # ---- Layer 7: country hubs (HK funding plumbing from HKMA Open API) ----
    hubs = []
    hk = gj("data/hkma.json") or {}
    by_id = {m.get("id"): m for m in (hk.get("metrics") or [])}
    if by_id:
        for mid, lab in (("agg_balance", "HK Aggregate Balance"),
                         ("usd_hkd", "HK USD/HKD vs 7.85 weak side"),
                         ("hibor_sofr", "HK HIBOR − SOFR")):
            m = by_id.get(mid)
            if m and m.get("value") is not None:
                hubs.append(metric("hk_" + mid, lab, m["value"], m.get("unit", ""), m.get("status", "info"),
                                    m.get("detail", ""), m.get("pctile"), m.get("asof")))
    else:
        hkd = fmp_fx("USDHKD")
        if hkd is not None:
            st = "green" if hkd <= 7.82 else "yellow" if hkd <= 7.848 else "red"
            hubs.append(metric("hk_peg", "Hong Kong USD/HKD (band 7.75–7.85)", round(hkd, 4), "", st,
                                "At the weak-side 7.85, HKMA sells USD/buys HKD → Aggregate Balance shrinks, HIBOR jumps"))
    # China hub: offshore-yuan (CNH) vs onshore (CNY) escape-valve. 3/4 of CNH trades in HK,
    # so it sits with the HK plumbing. CNH weaker than CNY = offshore-USD demand / yuan-depreciation
    # pressure — the canonical squeeze tell (2015-16: 3M CNH-CNY blew out ~700bp and Jan-2016 o/n
    # CNH HIBOR was driven >60% as the PBoC made offshore yuan scarce to crush shorts; Aug-2023 redux).
    usdcnh = fmp_fx("USDCNH"); usdcny = fmp_fx("USDCNY")
    if usdcnh is not None and usdcny is not None:
        gap_pips = round((usdcnh - usdcny) * 10000, 0)
        st = "red" if gap_pips >= 600 else "yellow" if gap_pips >= 200 else "green"
        hubs.append(metric("cnh_cny", "Offshore yuan CNH−CNY (escape valve)", gap_pips, "pips", st,
                            "Offshore (CNH %.4f) minus onshore (CNY %.4f) USD/RMB, in pips. Positive = offshore "
                            "yuan weaker = offshore-USD demand / depreciation pressure; a persistent wide gap "
                            "alongside a CNH funding spike is the PBoC-squeeze / dollar-stress signature."
                            % (usdcnh, usdcny)))
    elif usdcnh is not None:
        hubs.append(metric("cnh", "Offshore yuan USD/CNH", round(usdcnh, 4), "", "info",
                            "Offshore yuan spot; onshore CNY unavailable from FMP this run, so the CNH−CNY escape-valve gap could not be computed"))
    # CNH funding leg: overnight + 3M HIBOR (TMA). The spot escape-valve gap above + a CNH
    # funding spike here = the squeeze signature; overnight is the most squeeze-sensitive tenor.
    ch = cnh_hibor()
    if ch.get("on") is not None:
        on = round(ch["on"], 3)
        st = "green" if on <= 5 else "yellow" if on <= 15 else "red"
        hubs.append(metric("cnh_hibor_on", "CNH HIBOR overnight (offshore-yuan funding)", on, "%", st,
                            "Offshore-yuan overnight funding cost. Routine 1-3%; a spike (quarter-ends, or a deliberate "
                            "PBoC squeeze — Jan-2016 drove it past 60%) drains offshore liquidity. Paired with a wide "
                            "CNH-CNY gap above, it confirms a squeeze rather than drift.", asof=ch.get("date")))
    if ch.get("m3") is not None:
        m3 = round(ch["m3"], 3)
        st3 = "green" if m3 <= 4 else "yellow" if m3 <= 8 else "red"
        hubs.append(metric("cnh_hibor_3m", "CNH HIBOR 3-month (term offshore-yuan)", m3, "%", st3,
                            "Term offshore-yuan funding cost. Elevated 3M alongside an overnight spike = sustained "
                            "(not one-day) offshore liquidity stress.", asof=ch.get("date")))
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


def _massive_fx_block():
    try:
        fx = json.loads(S3.get_object(Bucket=BUCKET, Key="data/polygon-fx-regime.json")["Body"].read())
        pd = fx.get("pair_data") or {}
        rm = fx.get("regime_metrics") or {}
        g = lambda p, k: (pd.get(p) or {}).get(k)
        return {
            "regime_signals": fx.get("regime_signals") or [],
            "usd_synthetic_20d_pct": rm.get("usd_synthetic_20d_pct"),
            "em_fx_mean_20d_pct": rm.get("em_fx_mean_20d_pct"),
            "usdjpy": g("USD_JPY", "latest_price"), "usdjpy_20d_pct": g("USD_JPY", "return_20d_pct"),
            "eurusd": g("EUR_USD", "latest_price"), "usdcnh": g("USD_CNH", "latest_price"),
            "asof": fx.get("generated_at"), "source": "Massive polygon-fx-regime (live FX majors)",
        }
    except Exception as e:
        return {"error": str(e)[:80]}


def ai_scan(layers, health, verdict, reds, yellows, fx_context=None):
    rows = []
    for lk, lv in layers.items():
        for m in lv["metrics"]:
            if m["value"] is not None:
                rows.append("%s: %s%s [%s]" % (m["label"], m["value"], m["unit"], m["status"]))
    board = "\n".join(rows)
    if fx_context and not fx_context.get("error"):
        board += ("\n\nLIVE FX (Massive): broad USD 20d %s%% · USDJPY %s (%s%% 20d) · EURUSD %s · CNH %s · signals: %s"
                  % (fx_context.get("usd_synthetic_20d_pct"), fx_context.get("usdjpy"),
                     fx_context.get("usdjpy_20d_pct"), fx_context.get("eurusd"), fx_context.get("usdcnh"),
                     ", ".join(fx_context.get("regime_signals") or []) or "none"))
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
        raw = complete(prompt, tier="reason", max_tokens=3500)
        import re
        txt = (raw or "").replace("```json", "").replace("```", "")
        m = re.search(r"\{.*\}", txt, re.S)
        return json.loads(m.group(0)) if m else {"error": "no json", "raw": (raw or "")[:200]}
    except Exception as e:
        return {"error": str(e)[:200]}


def lambda_handler(event, context):
    t0 = time.time()
    layers = build_layers()
    health, verdict, reds, yellows = composite(layers)
    massive_fx = _massive_fx_block()
    ai = ai_scan(layers, health, verdict, reds, yellows, fx_context=massive_fx)
    payload = {
        "engine": "justhodl-eurodollar-plumbing", "version": "1.0",
        "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "wl_research": __import__("wl_fusion").block(('RATES',)),
        "plumbing_health": health, "verdict": verdict,
        # --- compatibility adapter: mirror the legacy data/eurodollar-stress.json schema in
        #     STRESS polarity (composite_score = 100 - health) so migrated crisis/risk consumers
        #     read composite_score/score/stress_score with zero inversion risk ---
        "composite_score": (round(100 - health, 1) if health is not None else None),
        "score": (round(100 - health, 1) if health is not None else None),
        "stress_score": (round(100 - health, 1) if health is not None else None),
        "severity": (("CRITICAL" if health <= 15 else "ELEVATED" if health <= 30 else "MODERATE"
                      if health <= 50 else "CALM" if health <= 70 else "ABUNDANT")
                     if health is not None else "UNKNOWN"),
        "stress_regime": (("ELEVATED_STRESS" if health <= 30 else "MODERATE_STRESS" if health <= 50
                           else "CALM" if health <= 70 else "ABUNDANT_LIQUIDITY")
                          if health is not None else "UNKNOWN"),
        "compat_note": "composite_score/score/stress_score = 100 - plumbing_health (stress polarity); "
                       "mirrors the legacy eurodollar-stress.json so consumers migrate without inverting.",
        "red_flags": reds, "yellow_flags": yellows,
        "massive_fx": massive_fx,
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
