"""justhodl-nyfed-pd — PRIMARY DEALER NET POSITIONS (the missing ledger column).

Ops-2727 proved the fleet never had real PD positions data (dealer-survey =
FOMC link tracker; the Aug-2025 shim never wrote a feed). This engine is the
real thing: NY Fed markets API Primary Dealer statistics — weekly NET
OUTRIGHT POSITIONS by security class, straight from the dealers' own
regulatory reporting. It is BOTH the institutional treasuries/credit read
and an independent cross-check on CFTC spec positioning.

  API        markets.newyorkfed.org/api/pd
               /list/timeseries.json -> {pd:{timeseries:[{keyid,description}]}}
               /get/{keyid}.json     -> {pd:{timeseries:[{asofdate,value}]}}
             (values may be "" or "*" — skip; unit = $ MILLIONS)
  SPEC       data/config/nyfed-pd-spec.json — catalog-discovered keyids per
             class (self-heals: if absent/empty, rediscovers from catalog).
  OUTPUT     data/nyfed-primary-dealer.json:
               net_positions_usd_b per class, net_treasury_total_b (flat
               alias the footprint desk reads), wow_b, z_52w, as_of, series.
             History: data/history/nyfed-pd.json (per-class, last 400 weeks).
  CONSUMERS  institutional-footprint asset ledger (TREASURIES pd column +
             primary_dealer_net), bond-desk cross-checks (future).
"""
import json, os, re, time, urllib.request, statistics
from datetime import datetime, timezone
import boto3

BUCKET, OUT = "justhodl-dashboard-live", "data/nyfed-primary-dealer.json"
SPEC_KEY, HIST_KEY = "data/config/nyfed-pd-spec.json", "data/history/nyfed-pd.json"
BASE = "https://markets.newyorkfed.org/api/pd"
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com", "Accept": "application/json"}
s3 = boto3.client("s3", region_name="us-east-1")

# Catalog truth (ops-2728 probe): descriptions are generic ("SECURITY NET
# SETTLED POSITION"); the class + tenor live in the KEYID grammar:
#   PDSI{2,3,5,7,10,20,30}NSP  -> nominal Treasury COUPONS, tenor-bucket ladder
#   PDST{5,10,30}NSP           -> TIPS (exact TIPS tenors)
#   PDFRN{2}NSP                -> Floating Rate Notes
#   *NSPC                      -> change-from-prior variants (skip; WoW computed here)
# The API exposes the Treasury family only — but BY TENOR, i.e. dealer CURVE
# positioning, which is richer than flat class totals.
KEYID_RX = re.compile(r"^PD(SI|ST|FRN)(\d+)NSP$")
FAM = {"SI": "TREASURY_COUPONS", "ST": "TIPS", "FRN": "TREASURY_FRN"}

def _j(k, d=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception: return d

def _get(url, timeout=25):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
        return json.loads(r.read())

def _discover():
    cat = _get(BASE + "/list/timeseries.json", 35)
    rows = (cat or {}).get("pd", {}).get("timeseries", [])
    spec = {}
    for r in rows:
        kid = str(r.get("keyid") or "")
        m = KEYID_RX.match(kid)
        if not m: continue
        cls, tenor = FAM[m.group(1)], int(m.group(2))
        spec.setdefault(cls, []).append({"keyid": kid, "tenor_y": tenor,
                                         "desc": str(r.get("description") or "")[:60]})
    for v in spec.values():
        v.sort(key=lambda x: x["tenor_y"])
    doc = {"classes": spec, "discovered": datetime.now(timezone.utc).isoformat(),
           "unit": "USD millions", "n_series": sum(len(v) for v in spec.values())}
    s3.put_object(Bucket=BUCKET, Key=SPEC_KEY, Body=json.dumps(doc).encode(),
                  ContentType="application/json")
    print("[pd] discovered %d series across %d classes" % (doc["n_series"], len(spec)))
    return doc

# ── ops 3301: PDPOS* NET OUTRIGHT POSITIONS — the FR2004 headline family
# (the Bloomberg/Crisil corporate-dealer-short dataset, Jul-2026). Grammar
# confirmed from the live catalog: PDPOS{CLASS}-{BUCKET} (e.g. PDPOSCS-TOT
# = corporate total, SBN2024 break). Discovery is description-driven so
# bucket keyids never need hardcoding; corporate maturity buckets are
# stitched (summed IG+HY per bucket) for the dealer credit-inventory read.
POS_RX = re.compile(r"^PDPOS([A-Z]+)-")
CORP_BUCKET = [
    (re.compile(r"COMMERCIAL PAPER"), "cp"),
    (re.compile(r"13 MONTHS OR LESS|LESS THAN OR EQUAL TO 13 MONTHS|DUE IN 13"), "u13m"),
    (re.compile(r"GREATER THAN 13|13 MONTHS BUT"), "m13m_5y"),
    (re.compile(r"GREATER THAN 5|5 YEARS BUT"), "y5_10"),
    (re.compile(r"GREATER THAN 10|MORE THAN 10"), "y10p"),
]

def _pos_layer(spec_doc):
    pos = (spec_doc or {}).get("pos")
    if not pos or not pos.get("corp") or "fin" not in pos:
        cat = _get(BASE + "/list/timeseries.json", 35).get("pd", {}).get("timeseries", [])
        ledger, corp, fin, txn = {}, [], {"in": [], "out": []}, {}
        TXN_RX = re.compile(r"^PDTR([A-Z]+)-TOT$|^PDTR(GS)-")
        for r in cat:
            kid = str(r.get("keyid") or "")
            desc = str(r.get("description") or "").upper()
            m = POS_RX.match(kid)
            if m:
                cls = m.group(1)
                if kid.endswith("-TOT") and cls != "CS":
                    ledger.setdefault(cls, []).append(kid)
                if cls == "CS" or "CORPORATE" in desc:
                    b = None
                    for rx, name in CORP_BUCKET:
                        if rx.search(desc):
                            b = name
                            break
                    corp.append({"keyid": kid, "bucket": b,
                                 "tot": kid.endswith("-TOT"),
                                 "desc": desc[:90]})
                continue
            # ops 3302c: dealer FINANCING — true grammar from catalog recon:
            #   PDSIRRA-*TOT  reverse repo (securities IN, cash LENT)
            #   PDSORA-*TOT   repo         (securities OUT, cash BORROWED)
            #   PDSIOSB/PDSOOS-*TOT  securities borrowed / lent
            if kid.startswith("PDSIRRA-") and kid.endswith("TOT"):
                fin["in"].append(kid)
            elif kid.startswith("PDSORA-") and kid.endswith("TOT"):
                fin["out"].append(kid)
            elif kid.startswith("PDSIOSB-") and kid.endswith("TOT"):
                fin.setdefault("sec_borrowed", []).append(kid)
            elif kid.startswith("PDSOOS-") and kid.endswith("TOT"):
                fin.setdefault("sec_lent", []).append(kid)
            # ops 3302: TRANSACTION volumes per class (weekly turnover)
            mt = TXN_RX.match(kid)
            if mt and ("TRANSACTION" in desc or kid.startswith("PDTR")):
                txn.setdefault(mt.group(1) or "GS", []).append(kid)
        pos = {"ledger": ledger, "corp": corp, "fin": fin, "txn": txn}
        spec_doc["pos"] = pos
        s3.put_object(Bucket=BUCKET, Key=SPEC_KEY,
                      Body=json.dumps(spec_doc).encode(),
                      ContentType="application/json")
        print("[pd] pos discovery: %d corp (%d bucketed), %d ledger, "
              "fin in/out %d/%d, txn classes %d"
              % (len(corp), sum(1 for c in corp if c["bucket"]),
                 len(ledger), len(fin["in"]), len(fin["out"]), len(txn)))
    return pos

def _fetch_series(kid):
    try:
        obs = _get("%s/get/%s.json" % (BASE, kid)).get("pd", {}).get("timeseries", [])
    except Exception as e:
        print("[pd] pos %s err %s" % (kid, str(e)[:60]))
        return {}
    out = {}
    for o in obs:
        v, d = o.get("value"), o.get("asofdate")
        if v in (None, "", "*") or not d:
            continue
        try:
            out[d] = float(v)
        except Exception:
            pass
    return out


def _read(tsy, wow_b, tenor_latest):
    cp = (tenor_latest or {}).get("TREASURY_COUPONS") or {}
    curve = ""
    if len(cp) >= 4:
        lo = max(cp.items(), key=lambda kv: kv[1])
        hi = min(cp.items(), key=lambda kv: kv[1])
        curve = "; curve: long %sy %+.1fB / short %sy %+.1fB" % (lo[0], lo[1], hi[0], hi[1])
    wow = wow_b.get("TREASURY_COUPONS")
    return "Dealer SETTLED book net %s $%.1fB UST%s%s" % (
        "LONG" if tsy > 0 else "SHORT", abs(tsy),
        "" if wow is None else ", coupons WoW %+.1fB" % wow, curve)


def lambda_handler(event=None, context=None):
    spec = _j(SPEC_KEY) or {}
    if not spec.get("classes") or (event or {}).get("rediscover"):
        spec = _discover()
    classes = spec["classes"]
    per_class = {}
    series_used = {}
    tenor_latest = {}   # cls -> {tenor: $B}
    for cls, items in classes.items():
        agg = {}   # asofdate -> summed value ($M)
        used = []
        for it in items[:14]:
            kid = it["keyid"]; tenor = it.get("tenor_y")
            try:
                obs = _get("%s/get/%s.json" % (BASE, kid)).get("pd", {}).get("timeseries", [])
            except Exception as e:
                print("[pd] %s fetch err %s" % (kid, str(e)[:60])); continue
            n = 0
            for o in obs:
                v, d = o.get("value"), o.get("asofdate")
                if v in (None, "", "*") or not d: continue
                try: agg[d] = agg.get(d, 0.0) + float(v); n += 1
                except Exception: continue
            if n:
                used.append(kid)
                last_d = max(d for d in agg)  # after this series merged
                # per-tenor latest: recompute from this series' own last obs
                own = [(o.get("asofdate"), o.get("value")) for o in obs
                       if o.get("value") not in (None, "", "*")]
                if own and tenor is not None:
                    own.sort()
                    try:
                        tenor_latest.setdefault(cls, {})[tenor] = round(float(own[-1][1]) / 1e3, 1)
                    except Exception:
                        pass
            time.sleep(0.2)
        if agg:
            per_class[cls] = dict(sorted(agg.items()))
            series_used[cls] = used
    assert per_class, "no PD series parsed — catalog drift?"

    hist = _j(HIST_KEY, {}) or {}
    net_b, wow_b, z52 = {}, {}, {}
    as_of = None
    for cls, ser in per_class.items():
        dates = sorted(ser)
        vals_b = [ser[d] / 1e3 for d in dates]           # $M -> $B
        net_b[cls] = round(vals_b[-1], 1)
        wow_b[cls] = round(vals_b[-1] - vals_b[-2], 1) if len(vals_b) >= 2 else None
        w = vals_b[-52:]
        z52[cls] = round((vals_b[-1] - statistics.mean(w)) / statistics.stdev(w), 2) \
                   if len(w) >= 20 and statistics.stdev(w) > 0 else None
        as_of = max(as_of or dates[-1], dates[-1])
        hist[cls] = {d: round(ser[d] / 1e3, 1) for d in dates[-400:]}
    tsy = round(sum(net_b.get(c, 0) for c in
                    ("TREASURY_BILLS", "TREASURY_COUPONS", "TIPS", "TREASURY_FRN")), 1)

    # ── ops 3301: corporate dealer positioning + full-ledger POS layer ──
    corporate, ledger_out, financing, transactions = None, {}, None, {}
    try:
        pos = _pos_layer(spec if isinstance(spec, dict) else {})
        bser = {}
        for it in sorted(pos.get("corp", []), key=lambda x: x["keyid"]):
            if it.get("tot") or not it.get("bucket"):
                continue
            s = _fetch_series(it["keyid"]); time.sleep(0.15)
            tgt = bser.setdefault(it["bucket"], {})
            for d, v in s.items():
                tgt[d] = tgt.get(d, 0.0) + v
        tot_kid = next((it["keyid"] for it in pos.get("corp", [])
                        if it.get("tot")), None)
        tot_ser = _fetch_series(tot_kid) if tot_kid else {}
        bond_b = [b for b in ("u13m", "m13m_5y", "y5_10", "y10p") if b in bser]
        dates = sorted(set().union(*[set(bser[b]) for b in bond_b])) if bond_b else []
        totals, u5y, y5p = {}, {}, {}
        for d in dates:
            a = sum(bser[b].get(d, 0.0) for b in ("u13m", "m13m_5y") if b in bser)
            c = sum(bser[b].get(d, 0.0) for b in ("y5_10", "y10p") if b in bser)
            u5y[d], y5p[d], totals[d] = a, c, a + c
        if not totals and tot_ser:
            totals = dict(tot_ser)
        ds = sorted(totals)
        if ds:
            vb = lambda x: round(x / 1e3, 2)
            latest_d = ds[-1]; latest = totals[latest_d]
            ytd = [totals[d] for d in ds if d.startswith(latest_d[:4])]
            neg = [d for d in ds[:-1] if totals[d] < 0]
            v52 = [totals[d] for d in ds[-52:]]
            zc = (round((latest - statistics.mean(v52)) / statistics.stdev(v52), 2)
                  if len(v52) >= 20 and statistics.stdev(v52) > 0 else None)
            mn_d = min(ds, key=lambda d: totals[d])
            mx_d = max(ds, key=lambda d: totals[d])
            regime = ("UNPRECEDENTED_NET_SHORT" if latest < 0 and not neg else
                      "NET_SHORT" if latest < 0 else
                      "DE_INVENTORYING" if (zc is not None and zc <= -1.5) else
                      "RE_INVENTORYING" if (zc is not None and zc >= 1.5) else
                      "NORMAL")
            cs = _j("data/credit-stress.json") or {}
            pct_hist = round(100.0 * sum(1 for d in ds if totals[d] <= latest)
                             / len(ds), 1)
            fy = y5p.get(latest_d, 0.0) if y5p else 0.0
            fu = u5y.get(latest_d, 0.0) if u5y else 0.0
            if latest < 0:
                rd = ("Dealers are NET SHORT $%.1fB of corporate bonds (%s in "
                      "available history since %s) — 5y+ book %+.1fB vs "
                      "short-end %+.1fB. The market's shock absorber is gone; "
                      "a rally forces covering into supply that pension/"
                      "insurance holders rarely sell."
                      % (abs(vb(latest)),
                         "FIRST net short" if not neg else "net short again",
                         ds[0][:4], vb(fy), vb(fu)))
            else:
                rd = ("Dealer corporate-bond inventory %+.1fB (%.0fth pctile "
                      "since %s), 5y+ %+.1fB / <5y %+.1fB — buffer intact."
                      % (vb(latest), pct_hist, ds[0][:4], vb(fy), vb(fu)))
            corporate = {
                "as_of": latest_d, "history_start": ds[0], "n_weeks": len(ds),
                "net_bonds_b": vb(latest),
                "net_5yplus_b": vb(fy) if y5p else None,
                "net_under5y_b": vb(fu) if u5y else None,
                "cp_b": (vb(bser["cp"][latest_d])
                         if "cp" in bser and latest_d in bser["cp"] else None),
                "total_series_b": (vb(tot_ser[latest_d])
                                   if latest_d in tot_ser else None),
                "wow_b": vb(latest - totals[ds[-2]]) if len(ds) >= 2 else None,
                "d13w_b": vb(latest - totals[ds[-14]]) if len(ds) >= 14 else None,
                "ytd_avg_b": vb(sum(ytd) / len(ytd)) if ytd else None,
                "z_52w": zc,
                "all_time_min_b": vb(totals[mn_d]), "all_time_min_date": mn_d,
                "all_time_max_b": vb(totals[mx_d]), "all_time_max_date": mx_d,
                "pctile_history": pct_hist,
                "prior_negative_date": (neg[-1] if neg else None),
                "regime": regime,
                "spreads_regime": cs.get("composite_regime"),
                "squeeze_setup": bool(latest < 0),
                "buckets_latest_b": {b: vb(bser[b][latest_d]) for b in bser
                                     if latest_d in bser[b]},
                "read": rd,
            }
            corp_hist = {"generated_at": datetime.now(timezone.utc)
                         .isoformat(timespec="seconds"),
                         "unit": "USD billions",
                         "totals": {d: vb(totals[d]) for d in ds},
                         "u5y": {d: vb(u5y[d]) for d in sorted(u5y)},
                         "y5plus": {d: vb(y5p[d]) for d in sorted(y5p)},
                         "buckets": {b: {d: vb(v) for d, v in
                                         sorted(bser[b].items())[-1560:]}
                                     for b in bser}}
            s3.put_object(Bucket=BUCKET, Key="data/history/nyfed-pd-corp.json",
                          Body=json.dumps(corp_hist,
                                          separators=(",", ":")).encode(),
                          ContentType="application/json",
                          CacheControl="public, max-age=3600")
            try:
                prev = (_j(OUT) or {}).get("corporate") or {}
                tok = os.environ.get("TELEGRAM_BOT_TOKEN")
                chat = os.environ.get("TELEGRAM_CHAT_ID")
                flipped = (prev.get("net_bonds_b") is not None
                           and (prev["net_bonds_b"] < 0) != (vb(latest) < 0))
                first = (regime == "UNPRECEDENTED_NET_SHORT"
                         and prev.get("regime") != regime)
                if tok and chat and (flipped or first):
                    msg = ("🏦 PRIMARY DEALERS: corporate-bond book %s — net "
                           "%+.1fB (5y+ %+.1fB / <5y %+.1fB). "
                           "justhodl.ai/primary-dealers.html"
                           % (regime.replace("_", " "), vb(latest), vb(fy),
                              vb(fu)))
                    urllib.request.urlopen(urllib.request.Request(
                        "https://api.telegram.org/bot%s/sendMessage" % tok,
                        data=json.dumps({"chat_id": chat,
                                         "text": msg}).encode(),
                        headers={"Content-Type": "application/json"}),
                        timeout=10).read()
                    print("[pd] telegram tripwire sent")
            except Exception as e:
                print("[pd] telegram skip %s" % str(e)[:60])
        LEDGER_NAME = {"MBS": "AGENCY_MBS", "ABS": "ABS",
                       "FGS": "AGENCY_DEBT", "SMGO": "MUNIS",
                       "GST": "TREASURY_EXTIPS"}
        for cls, kids in (pos.get("ledger") or {}).items():
            s = _fetch_series(sorted(kids)[-1]); time.sleep(0.15)
            if not s:
                continue
            dd = sorted(s); vv = [s[d] / 1e3 for d in dd]
            w = vv[-52:]
            zz = (round((vv[-1] - statistics.mean(w)) / statistics.stdev(w), 2)
                  if len(w) >= 20 and statistics.stdev(w) > 0 else None)
            name = LEDGER_NAME.get(cls, cls)
            ledger_out[name] = {"latest_b": round(vv[-1], 1), "as_of": dd[-1],
                                "wow_b": (round(vv[-1] - vv[-2], 1)
                                          if len(vv) >= 2 else None),
                                "z_52w": zz}
            hist["POS_" + name] = {d: round(s[d] / 1e3, 1) for d in dd[-400:]}

        # ── ops 3302: FINANCING (securities in = reverse-repo lending,
        # securities out = repo borrowing) + TRANSACTION volumes ──
        financing = None
        try:
            def _sum_family(kids, cap=12, grab=None):
                agg, grabbed = {}, {}
                for kid in sorted(kids)[:cap]:
                    s = _fetch_series(kid); time.sleep(0.12)
                    for d, v in s.items():
                        agg[d] = agg.get(d, 0.0) + v
                    if grab and grab in kid and s:
                        grabbed = s
                return agg, grabbed
            fi, fi_cd = _sum_family((pos.get("fin") or {}).get("in") or [],
                                    grab="CDTOT")
            fo, fo_cd = _sum_family((pos.get("fin") or {}).get("out") or [],
                                    grab="CDTOT")
            sl, _ = _sum_family((pos.get("fin") or {}).get("sec_lent")
                                or [], cap=10)
            sb, _ = _sum_family((pos.get("fin") or {}).get("sec_borrowed")
                                or [], cap=10)
            if fi and fo:
                dd2 = sorted(set(fi) & set(fo))
                if dd2:
                    ld = dd2[-1]
                    financing = {
                        "as_of": ld,
                        "reverse_repo_in_b": round(fi[ld] / 1e3, 1),
                        "repo_out_b": round(fo[ld] / 1e3, 1),
                        "securities_in_b": round(fi[ld] / 1e3, 1),
                        "securities_out_b": round(fo[ld] / 1e3, 1),
                        "net_lend_b": round((fi[ld] - fo[ld]) / 1e3, 1),
                        "sec_lent_b": (round(sl[max(sl)] / 1e3, 1)
                                       if sl else None),
                        "sec_borrowed_b": (round(sb[max(sb)] / 1e3, 1)
                                           if sb else None),
                        "corp_rev_repo_in_b": (round(fi_cd[max(fi_cd)]
                                                     / 1e3, 1)
                                               if fi_cd else None),
                        "corp_repo_out_b": (round(fo_cd[max(fo_cd)]
                                                  / 1e3, 1)
                                            if fo_cd else None),
                        "in_wow_b": (round((fi[ld] - fi[dd2[-2]]) / 1e3, 1)
                                     if len(dd2) >= 2 else None),
                        "out_wow_b": (round((fo[ld] - fo[dd2[-2]]) / 1e3, 1)
                                      if len(dd2) >= 2 else None),
                        "read": "Dealer financing book: $%.2fT reverse "
                                "repo IN (cash lent against collateral) "
                                "vs $%.2fT repo OUT (cash borrowed) — "
                                "the leverage engine behind every "
                                "position on this page."
                                % (fi[ld] / 1e6, fo[ld] / 1e6)}
                    hist["FIN_SEC_IN"] = {d: round(fi[d] / 1e3, 1)
                                          for d in sorted(fi)[-400:]}
                    hist["FIN_SEC_OUT"] = {d: round(fo[d] / 1e3, 1)
                                           for d in sorted(fo)[-400:]}
        except Exception as e:
            print("[pd] financing skip %s" % str(e)[:80])
        transactions = {}
        try:
            TXN_NAME = {"CS": "CORPORATE", "MBS": "AGENCY_MBS", "AB": "ABS",
                        "ABS": "ABS", "FGSXM": "AGENCY_DEBT", "GS": "TREASURY",
                        "SMGO": "MUNIS"}
            for cls, kids in sorted((pos.get("txn") or {}).items()):
                s = _fetch_series(sorted(kids)[-1]); time.sleep(0.12)
                if not s:
                    continue
                dd3 = sorted(s)
                name = TXN_NAME.get(cls, cls)
                avg4 = (sum(s[d] for d in dd3[-4:]) / min(4, len(dd3))) / 1e3
                transactions[name] = {
                    "weekly_b": round(s[dd3[-1]] / 1e3, 1),
                    "avg_4w_b": round(avg4, 1), "as_of": dd3[-1]}
            if corporate and transactions.get("CORPORATE"):
                tv = transactions["CORPORATE"]["avg_4w_b"]
                inv = abs(corporate.get("net_bonds_b") or 0)
                if tv and inv is not None:
                    corporate["turnover_velocity"] = round(tv / max(inv, 0.5), 1)
                    corporate["weekly_volume_b"] = transactions["CORPORATE"]["weekly_b"]
        except Exception as e:
            print("[pd] transactions skip %s" % str(e)[:80])

    except Exception as e:
        print("[pd] pos layer error: %s" % str(e)[:200])

    s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist).encode(),
                  ContentType="application/json")
    doc = {"engine": "justhodl-nyfed-pd", "version": "3.0.0",
           "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "as_of": as_of,
           "net_treasury_total_b": tsy,                  # flat alias for footprint _find
           "net_positions_usd_b": net_b, "wow_usd_b": wow_b, "z_52w": z52,
           "by_tenor_usd_b": tenor_latest,
           "corporate": corporate,                        # ops 3301
           "positions_ledger": ledger_out,                # ops 3301
           "financing": financing,                        # ops 3302
           "transactions": transactions,                  # ops 3302
           "corp_history_key": "data/history/nyfed-pd-corp.json",
           "series_used": series_used,
           "source": "NY Fed Primary Dealer Statistics (markets.newyorkfed.org/api/pd): net "
                     "settled positions, weekly, $B (reported $M). API exposes the Treasury "
                     "family by tenor, plus the PDPOS* outright family: corporate bonds by "
                     "maturity bucket (stitched) and the full dealer ledger.",
           "metric": "NET SETTLED POSITIONS (settled inventory book; +-$B scale — "
                     "distinct from headline net outright positions)",
           "read": _read(tsy, wow_b, tenor_latest)}
    s3.put_object(Bucket=BUCKET, Key=OUT, Body=json.dumps(doc, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    return {"ok": True, "as_of": as_of, "classes": len(net_b),
            "net_treasury_total_b": tsy, "net_b": net_b,
            "corp_net_bonds_b": (corporate or {}).get("net_bonds_b"),
            "corp_regime": (corporate or {}).get("regime"),
            "ledger_classes": len(ledger_out),
            "financing_ok": bool(financing), "txn_classes": len(transactions)}
