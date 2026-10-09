"""justhodl-esma-ratings — ESMA European Rating Platform (registers.esma.europa.eu), free, keyless.

Every credit rating issued or endorsed by an EU-registered credit rating agency
(S&P, Moody's, Fitch, DBRS, Scope, …) is published on ESMA's Rating Platform under
Article 11a CRA Regulation. The platform's public Solr index exposes two document
types: `radar` (one per rating: issuer, country, CRA, current value/outlook) and
`action` (children: every upgrade / downgrade / affirmation / outlook change with a
validity timestamp).

This engine keeps the SOVEREIGN slice: long-term issuer ratings of type
"Sovereign and public finance / State rating" — ~3,500 records across ~170
countries — and their full action history for the main agencies. Output:

  * per country × agency: current rating (foreign-currency long-term, local as
    fallback), outlook/watch status, last action type and date;
  * a numeric consensus scale (AAA=21 … D=1) per country and its history;
  * the most recent actions tape (upgrades, downgrades, outlook changes);
  * series: `SCORE_<ISO3>` consensus notch by date, `N_UPGRADES_M`, `N_DOWNGRADES_M`.

Descriptive only (no call). Daily.
"""
from __future__ import annotations

import json
import re
import time
import urllib.parse
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import risk_sources as RS

SLUG = "esma-ratings"
SOLR = "https://registers.esma.europa.eu/solr/esma_registers_radar/select"
PORTAL = "https://registers.esma.europa.eu/publication/searchRegister?core=esma_registers_radar"
PARENT_Q = 'entity_type:radar AND ratingTypeSubtype:*Sovereign* AND ratedObjectCode:ISR AND timeHorizonDescr:Long*'
PARENT_FL = ("id,craName,respCraLeiName,issuerName,ratingValueLabel,lastActionTypeLabel,ratingStatusLabel,"
             "localForeignCurrencyCode,countryCode,countryName,timestamp,ratingTypeSubtype,ratingIssuanceLocationDesc")
ACTION_FL = "parent_id,actionsActionTypeLabel,actionsRatingValueLabel,actionsRacValidityDatetime,actionsDefaultFlag"
MAJOR = {  # craName prefix -> short label
    "Standard & Poor": "S&P", "S&P": "S&P", "Moody": "Moody's", "Fitch": "Fitch", "DBRS": "DBRS", "Scope": "Scope",
}
ALL_AGENCIES_MAX = 30
# generic long-term scale → 21..1 (S&P/Fitch letters, Moody's, DBRS)
SCALE = ["AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-", "BB+", "BB", "BB-", "B+", "B", "B-",
         "CCC+", "CCC", "CCC-", "CC", "C", "D"]
MOODYS = {"Aaa": "AAA", "Aa1": "AA+", "Aa2": "AA", "Aa3": "AA-", "A1": "A+", "A2": "A", "A3": "A-", "Baa1": "BBB+", "Baa2": "BBB",
          "Baa3": "BBB-", "Ba1": "BB+", "Ba2": "BB", "Ba3": "BB-", "B1": "B+", "B2": "B", "B3": "B-", "Caa1": "CCC+", "Caa2": "CCC",
          "Caa3": "CCC-", "Ca": "CC", "C": "C"}


def notch(label):
    if not label:
        return None
    s = str(label).strip().replace(" ", "")
    s = re.sub(r"\(sf\)|u$|\*|pi$", "", s)
    if s in MOODYS:
        s = MOODYS[s]
    m = re.fullmatch(r"([ABC]{1,3}|D)\((high|low|H|L)\)", s, re.I)   # DBRS AA(high)
    if m:
        s = m.group(1).upper() + ("+" if m.group(2).lower() in ("high", "h") else "-")
    s = s.upper()
    if s in ("SD", "RD", "DDD", "DD", "D"):
        return 1
    if s in ("WD", "WR", "NR"):
        return None
    try:
        return 22 - (SCALE.index(s) + 1)
    except ValueError:
        return None


ISO2_TO_3 = None


def iso3(cc):
    global ISO2_TO_3  # noqa: PLW0603
    if ISO2_TO_3 is None:
        try:
            import cds_long_context as LC  # shared with the CDS desk
            ISO2_TO_3 = dict(getattr(LC, "ECB_ISO2_TO_ISO3", {}))
        except Exception:  # noqa: BLE001
            ISO2_TO_3 = {}
        ISO2_TO_3.update(_ISO2_3)
    return ISO2_TO_3.get(cc, cc)


_ISO2_3 = {"US": "USA", "GB": "GBR", "JP": "JPN", "CN": "CHN", "DE": "DEU", "FR": "FRA", "IT": "ITA", "ES": "ESP", "PT": "PRT", "GR": "GRC",
           "IE": "IRL", "NL": "NLD", "BE": "BEL", "AT": "AUT", "FI": "FIN", "SE": "SWE", "NO": "NOR", "DK": "DNK", "CH": "CHE", "PL": "POL",
           "CZ": "CZE", "HU": "HUN", "RO": "ROU", "BG": "BGR", "HR": "HRV", "SI": "SVN", "SK": "SVK", "LT": "LTU", "LV": "LVA", "EE": "EST",
           "CY": "CYP", "MT": "MLT", "LU": "LUX", "IS": "ISL", "TR": "TUR", "RU": "RUS", "UA": "UKR", "KZ": "KAZ", "GE": "GEO", "AM": "ARM",
           "AZ": "AZE", "RS": "SRB", "ME": "MNE", "MK": "MKD", "AL": "ALB", "BA": "BIH", "XK": "XKX", "MD": "MDA", "BY": "BLR", "CA": "CAN",
           "MX": "MEX", "BR": "BRA", "AR": "ARG", "CL": "CHL", "CO": "COL", "PE": "PER", "UY": "URY", "PY": "PRY", "BO": "BOL", "EC": "ECU",
           "VE": "VEN", "PA": "PAN", "CR": "CRI", "GT": "GTM", "HN": "HND", "SV": "SLV", "NI": "NIC", "DO": "DOM", "JM": "JAM", "TT": "TTO",
           "BS": "BHS", "BB": "BRB", "BZ": "BLZ", "SR": "SUR", "GY": "GUY", "CU": "CUB", "HT": "HTI", "AU": "AUS", "NZ": "NZL", "KR": "KOR",
           "TW": "TWN", "HK": "HKG", "SG": "SGP", "MY": "MYS", "TH": "THA", "ID": "IDN", "PH": "PHL", "VN": "VNM", "IN": "IND", "PK": "PAK",
           "BD": "BGD", "LK": "LKA", "NP": "NPL", "MN": "MNG", "KH": "KHM", "LA": "LAO", "MM": "MMR", "MV": "MDV", "BT": "BTN", "PG": "PNG",
           "FJ": "FJI", "MO": "MAC", "SA": "SAU", "AE": "ARE", "QA": "QAT", "KW": "KWT", "BH": "BHR", "OM": "OMN", "IL": "ISR", "JO": "JOR",
           "LB": "LBN", "IQ": "IRQ", "IR": "IRN", "EG": "EGY", "MA": "MAR", "TN": "TUN", "DZ": "DZA", "LY": "LBY", "ZA": "ZAF", "NG": "NGA",
           "KE": "KEN", "GH": "GHA", "CI": "CIV", "SN": "SEN", "CM": "CMR", "ET": "ETH", "TZ": "TZA", "UG": "UGA", "RW": "RWA", "ZM": "ZMB",
           "AO": "AGO", "MZ": "MOZ", "NA": "NAM", "BW": "BWA", "MU": "MUS", "GA": "GAB", "CG": "COG", "CD": "COD", "BJ": "BEN", "TG": "TGO",
           "ML": "MLI", "BF": "BFA", "NE": "NER", "TD": "TCD", "SD": "SDN", "SC": "SYC", "CV": "CPV", "MW": "MWI", "ZW": "ZWE", "LS": "LSO",
           "SZ": "SWZ", "MG": "MDG", "SL": "SLE", "LR": "LBR", "GM": "GMB", "GN": "GIN", "GQ": "GNQ", "DJ": "DJI", "SO": "SOM", "ER": "ERI",
           "SS": "SSD", "UZ": "UZB", "TJ": "TJK", "KG": "KGZ", "TM": "TKM", "AF": "AFG", "AD": "AND", "LI": "LIE", "MC": "MCO", "SM": "SMR",
           "VA": "VAT", "GI": "GIB", "IM": "IMN", "JE": "JEY", "GG": "GGY", "BM": "BMU", "KY": "CYM", "AW": "ABW", "CW": "CUW", "PR": "PRI",
           "GL": "GRL", "FO": "FRO", "SB": "SLB", "VU": "VUT", "WS": "WSM", "TO": "TON", "ST": "STP", "KM": "COM", "SY": "SYR", "YE": "YEM",
           "PS": "PSE", "BN": "BRN", "TL": "TLS", "KP": "PRK", "NC": "NCL", "PF": "PYF", "RE": "REU", "GP": "GLP", "MQ": "MTQ", "GF": "GUF",
           "AG": "ATG", "LC": "LCA", "VC": "VCT", "GD": "GRD", "KN": "KNA", "DM": "DMA", "TC": "TCA", "VG": "VGB", "AI": "AIA", "MS": "MSR",
           "SX": "SXM", "BQ": "BES", "ML": "MLI", "MR": "MRT", "GW": "GNB", "CF": "CAF", "BI": "BDI", "EH": "ESH", "NR": "NRU", "TV": "TUV",
           "KI": "KIR", "MH": "MHL", "FM": "FSM", "PW": "PLW", "CK": "COK", "NU": "NIU", "WF": "WLF", "AS": "ASM", "GU": "GUM", "MP": "MNP"}


def solr(q, fl, rows=1000, start=0, sort=None, timeout=90):
    params = {"q": q, "fl": fl, "rows": rows, "start": start, "wt": "json"}
    if sort:
        params["sort"] = sort
    url = SOLR + "?" + urllib.parse.urlencode(params)
    d = RS.http_json(url, timeout=timeout)
    if "response" not in d:
        raise RuntimeError(f"Solr error: {str(d)[:200]}")
    return d["response"]


def fetch_parents():
    out, start = [], 0
    while True:
        r = solr(PARENT_Q, PARENT_FL, rows=2000, start=start)
        out.extend(r["docs"])
        start += 2000
        if start >= r["numFound"] or not r["docs"]:
            break
        time.sleep(0.2)
    return [d for d in out if "State rating" in (d.get("ratingTypeSubtype") or "")]


def agency(doc):
    name = doc.get("respCraLeiName") or doc.get("craName") or ""
    for k, v in MAJOR.items():
        if name.startswith(k) or k in name:
            return v, True
    return re.sub(r"\s*\(.*?\)", "", name).strip()[:28], False


def fetch_actions(parent_ids):
    acts = defaultdict(list)
    ids = list(parent_ids)
    for i in range(0, len(ids), 40):
        chunk = ids[i:i + 40]
        q = "entity_type:action AND parent_id:(" + " OR ".join(f'"{x}"' for x in chunk) + ")"
        start = 0
        while True:
            r = solr(q, ACTION_FL, rows=2000, start=start, sort="actionsRacValidityDatetime asc")
            for a in r["docs"]:
                acts[a.get("parent_id")].append(a)
            start += 2000
            if start >= r["numFound"] or not r["docs"]:
                break
        time.sleep(0.15)
    return acts


def build(event):
    pk = RS.Packet(
        SLUG, "ESMA Rating Platform", "ESMA European Rating Platform — sovereign ratings and rating actions",
        "Every sovereign long-term issuer rating published by an EU-registered or certified credit rating agency "
        "(S&P, Moody's, Fitch, DBRS, Scope and ~25 smaller agencies), with the full upgrade / downgrade / outlook "
        "action history ESMA requires under the CRA Regulation. Free, timestamped, no vendor licence. The consensus "
        "notch (AAA = 21 … D = 1) averages the main agencies' foreign-currency ratings.",
        {"provider": "ESMA — European Securities and Markets Authority", "url": PORTAL,
         "docs": "https://www.esma.europa.eu/credit-rating-agencies/european-rating-platform",
         "license": "ESMA legal notice — public register, free reuse with attribution", "cadence": "continuous; daily read"},
        cadence="daily")
    parents = fetch_parents()
    pk.raw("sovereign_state_ratings_parents.json", json.dumps(parents, separators=(",", ":")).encode(),
           "application/json", url=SOLR + "?q=" + urllib.parse.quote(PARENT_Q))
    # choose the records to track actions for: main agencies, all currencies, active (not withdrawn) first
    tracked = []
    for d in parents:
        ag, major = agency(d)
        d["_agency"], d["_major"] = ag, major
        d["_iso3"] = iso3(d.get("countryCode"))
        if major and (d.get("ratingStatusLabel") or "") != "Withdrawal":
            tracked.append(d)
    # one record per country × agency × currency (DBRS lists several parallel records): keep the freshest two
    grouped = defaultdict(list)
    for d in tracked:
        grouped[(d["_iso3"], d["_agency"], d.get("localForeignCurrencyCode"))].append(d)
    tracked = []
    for recs in grouped.values():
        recs.sort(key=lambda r: r.get("timestamp") or "", reverse=True)
        tracked.extend(recs[:2])
    acts = fetch_actions([d["id"] for d in tracked])
    # current rating per country × agency (prefer FC)
    cur = {}
    tape = []
    monthly_up, monthly_dn = defaultdict(int), defaultdict(int)
    hist = defaultdict(lambda: defaultdict(dict))   # iso3 -> agency -> date -> notch
    for d in tracked:
        key = (d["_iso3"], d["_agency"])
        A = sorted(acts.get(d["id"], []), key=lambda a: a.get("actionsRacValidityDatetime") or "")
        last_val, last_val_date, outlook, outlook_date, last_type, last_date = None, None, None, None, None, None
        for a in A:
            t = a.get("actionsActionTypeLabel") or ""
            dt = (a.get("actionsRacValidityDatetime") or "")[:10]
            v = a.get("actionsRatingValueLabel")
            if v:
                last_val, last_val_date = v, dt
                n = notch(v)
                if n is not None:
                    hist[d["_iso3"]][d["_agency"]][dt] = n
            if "outlook" in t.lower() or "watch" in t.lower():
                outlook, outlook_date = t, dt
            last_type, last_date = t, dt
            if t in ("Upgrade", "Downgrade") and dt:
                tape.append([dt, d["_iso3"], d.get("countryName"), d["_agency"], t, v, d.get("localForeignCurrencyCode")])
        rec = {"iso3": d["_iso3"], "country": d.get("countryName"), "agency": d["_agency"],
               "rating": last_val or d.get("ratingValueLabel"), "rating_date": last_val_date,
               "notch": notch(last_val or d.get("ratingValueLabel")),
               "outlook": outlook or d.get("ratingStatusLabel"), "outlook_date": outlook_date,
               "last_action": last_type or d.get("lastActionTypeLabel"), "last_action_date": last_date,
               "currency": d.get("localForeignCurrencyCode"), "issuer": d.get("issuerName"), "n_actions": len(A)}
        prev = cur.get(key)
        if prev is None or (rec["currency"] == "FC" and prev["currency"] != "FC") or \
           (rec["currency"] == prev["currency"] and (rec["rating_date"] or "") > (prev["rating_date"] or "")):
            cur[key] = rec
    # consensus per country
    by_c = defaultdict(list)
    for (c, ag), rec in cur.items():
        by_c[c].append(rec)
    grid_rows = []
    agencies = ["S&P", "Moody's", "Fitch", "DBRS", "Scope"]
    consensus = {}
    for c, recs in sorted(by_c.items()):
        notches = [r["notch"] for r in recs if r["notch"] is not None]
        cons = round(sum(notches) / len(notches), 2) if notches else None
        name = recs[0]["country"]
        cells = {r["agency"]: r for r in recs}
        last = max((r["last_action_date"] or "" for r in recs), default="")
        neg = sum(1 for r in recs if "negative" in (r["outlook"] or "").lower())
        pos = sum(1 for r in recs if "positive" in (r["outlook"] or "").lower())
        consensus[c] = {"iso3": c, "country": name, "consensus_notch": cons,
                        "consensus_label": SCALE[int(round(21 - cons))] if cons else None,
                        "n_agencies": len(recs), "negative_outlooks": neg, "positive_outlooks": pos,
                        "last_action_date": last,
                        "ratings": {r["agency"]: {"rating": r["rating"], "outlook": r["outlook"], "date": r["rating_date"],
                                                 "last_action": r["last_action"], "last_action_date": r["last_action_date"]} for r in recs}}
        grid_rows.append([c, name, consensus[c]["consensus_label"], cons] +
                         [f"{cells[a]['rating']} · {_short_outlook(cells[a]['outlook'])}" if a in cells else "" for a in agencies] +
                         [neg, pos, last])
    pk.add_table("grid", ["ISO3", "Country", "Consensus", "Notch"] + agencies + ["Neg. outlooks", "Pos. outlooks", "Last action"],
                 grid_rows, title="Sovereign long-term ratings by agency (foreign currency where available)",
                 note="Current value per agency from the latest rating action on ESMA's platform; outlook / watch from the latest outlook action. Notch scale AAA = 21 … D = 1; consensus is the mean of available main-agency notches.")
    # one tape line per country × agency × action × day (FC and LC legs are usually announced together)
    seen, uniq = set(), []
    for r in sorted(tape, key=lambda r: (r[0], r[6] != "FC")):
        k = (r[0], r[1], r[3], r[4])
        if k in seen:
            continue
        seen.add(k)
        uniq.append(r)
        (monthly_up if r[4] == "Upgrade" else monthly_dn)[r[0][:7]] += 1
    tape = uniq
    tape.sort(key=lambda r: r[0], reverse=True)
    pk.add_table("actions", ["Date", "ISO3", "Country", "Agency", "Action", "Rating", "Ccy"], tape[:400],
                 title="Sovereign upgrades and downgrades — most recent 400", note="Validity timestamp of the rating action as published to ESMA.")
    # series: consensus notch history per country (step series at action dates)
    for c, agmap in hist.items():
        dates = sorted({d for m in agmap.values() for d in m})
        pts, cur_n = [], {}
        for dt in dates:
            for ag, m in agmap.items():
                if dt in m:
                    cur_n[ag] = m[dt]
            pts.append([dt, round(sum(cur_n.values()) / len(cur_n), 2)])
        if pts:
            pk.add_series(f"SCORE_{c}", f"{consensus.get(c, {}).get('country', c)} — consensus notch (AAA=21)", pts,
                          unit="notch", freq="event", group="country", iso3=c)
    months = sorted(set(monthly_up) | set(monthly_dn))
    today = datetime.now(timezone.utc).date().isoformat()
    mdate = lambda m: min(RS.period_to_date(m), today)   # the running month is dated today, not at its (future) month end  # noqa: E731
    pk.add_series("N_UPGRADES_M", "Sovereign upgrades per month (main agencies)", [[mdate(m), monthly_up.get(m, 0)] for m in months], unit="count", freq="monthly", group="tape")
    pk.add_series("N_DOWNGRADES_M", "Sovereign downgrades per month (main agencies)", [[mdate(m), monthly_dn.get(m, 0)] for m in months], unit="count", freq="monthly", group="tape")
    n_dn_90 = sum(1 for r in tape if r[4] == "Downgrade" and r[0] >= (datetime.now(timezone.utc) - timedelta(days=90)).date().isoformat())
    n_up_90 = sum(1 for r in tape if r[4] == "Upgrade" and r[0] >= (datetime.now(timezone.utc) - timedelta(days=90)).date().isoformat())
    pk.kpi("Sovereigns rated", str(len(consensus)), f"{len(parents):,} long-term state ratings on the platform", "info")
    pk.kpi("Main-agency ratings tracked", str(len(tracked)), f"{sum(len(v) for v in acts.values()):,} actions read", "info")
    pk.kpi("Downgrades, 90d", str(n_dn_90), "S&P · Moody's · Fitch · DBRS · Scope", "neg" if n_dn_90 > n_up_90 else "info")
    pk.kpi("Upgrades, 90d", str(n_up_90), "", "pos" if n_up_90 >= n_dn_90 else "info")
    pk.kpi("Negative outlooks", str(sum(c["negative_outlooks"] for c in consensus.values())), "across tracked ratings", "warning")
    pk.extra["consensus"] = consensus
    pk.note("ESMA's platform lists ratings issued in the EU or endorsed into the EU; for the big three this covers their sovereign books. Withdrawn ratings are excluded from the grid. "
            "Labels are the agencies' own; DBRS 'AA (high)' maps to AA+, Moody's 'Aa1' to AA+.")
    return pk


def _short_outlook(o):
    o = (o or "").lower()
    if "negative" in o:
        return "neg" + (" watch" if "watch" in o else "")
    if "positive" in o:
        return "pos" + (" watch" if "watch" in o else "")
    if "stable" in o:
        return "stable"
    if "evolving" in o or "developing" in o:
        return "evolving"
    return o[:12] if o else "—"


def lambda_handler(event, context=None):
    return RS.run(SLUG, build, event)
