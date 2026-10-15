"""Corp bonds measurements. Descriptive only: no calls, no sizing.

Three public layers, each labelled with what it actually covers:

* United States — FINRA TRACE corporate and Rule 144A breadth, sentiment and
  capped volume. That is the whole disseminated tape, rolled up by grade.
  The public aggregate API does not include a CUSIP blotter (CUSIP licence).
* Euro area — every bond on the ECB's weekly CSPP/PEPP holdings list
  (ISIN, issuer, coupon, maturity, purchasing national central bank).
* International indices — ICE BofA corporate OAS and yield sleeves on FRED
  for the markets that do not publish an issue-level public tape.
"""
from __future__ import annotations

import csv
import io
import math
import re
from datetime import date, datetime

VERSION = "1.3.0"

# kind "oas" is stored by FRED in percent; the packet converts it to basis points.
# kind "yield" stays in percent. A missing series is skipped, never invented.
SLEEVES = (
    {"id": "us_ig", "label": "US IG", "region": "United States", "market": "corporate", "kind": "oas", "fred": "BAMLC0A0CM"},
    {"id": "us_aaa", "label": "US AAA", "region": "United States", "market": "corporate", "kind": "oas", "fred": "BAMLC0A1CAAA"},
    {"id": "us_aa", "label": "US AA", "region": "United States", "market": "corporate", "kind": "oas", "fred": "BAMLC0A2CAA"},
    {"id": "us_a", "label": "US A", "region": "United States", "market": "corporate", "kind": "oas", "fred": "BAMLC0A3CA"},
    {"id": "us_bbb", "label": "US BBB", "region": "United States", "market": "corporate", "kind": "oas", "fred": "BAMLC0A4CBBB"},
    {"id": "us_hy", "label": "US HY", "region": "United States", "market": "high yield", "kind": "oas", "fred": "BAMLH0A0HYM2"},
    {"id": "us_bb", "label": "US BB", "region": "United States", "market": "high yield", "kind": "oas", "fred": "BAMLH0A1HYBB"},
    {"id": "us_b", "label": "US B", "region": "United States", "market": "high yield", "kind": "oas", "fred": "BAMLH0A2HYB"},
    {"id": "us_ccc", "label": "US CCC", "region": "United States", "market": "high yield", "kind": "oas", "fred": "BAMLH0A3HYC"},
    {"id": "us_ig_yield", "label": "US IG yield", "region": "United States", "market": "corporate", "kind": "yield", "fred": "BAMLC0A0CMEY"},
    {"id": "us_hy_yield", "label": "US HY yield", "region": "United States", "market": "high yield", "kind": "yield", "fred": "BAMLH0A0HYM2EY"},
    {"id": "us_1_3", "label": "US IG 1-3Y", "region": "United States", "market": "tenor", "kind": "oas", "fred": "BAMLC1A0C13Y"},
    {"id": "us_3_5", "label": "US IG 3-5Y", "region": "United States", "market": "tenor", "kind": "oas", "fred": "BAMLC2A0C35Y"},
    {"id": "us_5_7", "label": "US IG 5-7Y", "region": "United States", "market": "tenor", "kind": "oas", "fred": "BAMLC3A0C57Y"},
    {"id": "us_7_10", "label": "US IG 7-10Y", "region": "United States", "market": "tenor", "kind": "oas", "fred": "BAMLC4A0C710Y"},
    {"id": "us_10_15", "label": "US IG 10-15Y", "region": "United States", "market": "tenor", "kind": "oas", "fred": "BAMLC7A0C1015Y"},
    {"id": "us_15p", "label": "US IG 15Y+", "region": "United States", "market": "tenor", "kind": "oas", "fred": "BAMLC8A0C15PY"},
    {"id": "euro_hy", "label": "Euro HY", "region": "Euro area", "market": "high yield", "kind": "oas", "fred": "BAMLHE00EHYIOAS"},
    {"id": "euro_hy_yield", "label": "Euro HY yield", "region": "Euro area", "market": "high yield", "kind": "yield", "fred": "BAMLHE00EHYIEY"},
    {"id": "em", "label": "EM corporate", "region": "Emerging markets", "market": "corporate", "kind": "oas", "fred": "BAMLEMCBPIOAS"},
    {"id": "em_hg", "label": "EM high grade", "region": "Emerging markets", "market": "corporate", "kind": "oas", "fred": "BAMLEMIBHGCRPIOAS"},
    {"id": "asia_usd", "label": "Asia USD", "region": "Asia", "market": "corporate", "kind": "oas", "fred": "BAMLEMALLCRPIASIAUSOAS"},
    {"id": "em_euro_yield", "label": "EM euro yield", "region": "Emerging markets", "market": "corporate", "kind": "yield", "fred": "BAMLEMEBCRPIEEY"},
    {"id": "sterling", "label": "Sterling corporate", "region": "United Kingdom", "market": "corporate", "kind": "oas", "fred": "BAMLSE00ECLIOAS"},
)

_STOP = {
    "INC", "INCORPORATED", "CORP", "CORPORATION", "CO", "COMPANY", "LTD", "LIMITED",
    "PLC", "GROUP", "THE", "SA", "SPA", "NV", "AG", "SE", "BV", "SARL", "SRL",
    "HOLDINGS", "HOLDING", "INTERNATIONAL", "FINANCE", "FINANCIAL", "AND",
}


def _num(value):
    if type(value) not in (int, float) or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _round(value, places=2):
    number = _num(value)
    if number is None:
        return None
    return round(number, places)


def parse_fred_csv(text):
    """Return sorted (iso date, float) observations. Dots and blanks are gaps."""
    rows = []
    if not text or text.lstrip().startswith("<"):
        return rows
    for line in text.splitlines():
        if not line or line.startswith("DATE") or "," not in line:
            continue
        day, raw = line.split(",", 1)
        raw = raw.strip().strip('"')
        if len(day) != 10 or raw in ("", "."):
            continue
        try:
            value = float(raw)
            date.fromisoformat(day)
        except ValueError:
            continue
        if math.isfinite(value):
            rows.append((day, value))
    rows.sort()
    return rows


def _lag(points, sessions):
    if len(points) <= sessions:
        return None
    return points[-1][1] - points[-1 - sessions][1]


def sleeve_stats(spec, points, today_iso):
    """Changes are in the same unit as `level` (bp for OAS, percent for yield)."""
    if not points:
        return None
    day, last = points[-1]
    scale = 100.0 if spec["kind"] == "oas" else 1.0
    level = last * scale
    window = [p for p in points if p[0] >= _shift_years(day, 5)]
    base = window or points
    rank = sum(1 for _, value in base if value <= last)
    pct = (100.0 * rank / len(base)) if base else None
    def chg(sessions):
        delta = _lag(points, sessions)
        return _round(delta * scale, 2) if delta is not None else None
    return {
        "id": spec["id"],
        "label": spec["label"],
        "region": spec["region"],
        "market": spec["market"],
        "kind": spec["kind"],
        "unit": "bp" if spec["kind"] == "oas" else "pct",
        "fred": spec["fred"],
        "as_of": day,
        "level": _round(level, 2),
        "chg_1d": chg(1),
        "chg_5d": chg(5),
        "chg_21d": chg(21),
        "chg_63d": chg(63),
        "chg_252d": chg(252),
        "pct_5y": _round(pct, 1),
        "n": len(points),
        "stale_days": _day_gap(day, today_iso),
    }


def _shift_years(day_iso, years):
    y, m, d = (int(x) for x in day_iso.split("-"))
    try:
        return date(y - years, m, d).isoformat()
    except ValueError:
        return date(y - years, m, 28).isoformat()


def _day_gap(left, right):
    try:
        return (date.fromisoformat(right) - date.fromisoformat(left)).days
    except ValueError:
        return None


def parse_ecb_holdings(text):
    """ECB CSPP/PEPP weekly list. Header: NCB,ISIN,ISSUER,MATURITY DATE,COUPON RATE."""
    if not text:
        return []
    sample = text.lstrip()[:80]
    if "ISIN" not in sample.upper() or sample.startswith("<"):
        return []
    out = []
    seen = set()
    reader = csv.reader(io.StringIO(text))
    header = next(reader, None)
    if not header:
        return []
    for row in reader:
        if len(row) < 5:
            continue
        ncb, isin, issuer, maturity, coupon = (cell.strip() for cell in row[:5])
        if len(isin) < 12 or isin in seen:
            continue
        try:
            date.fromisoformat(maturity)
        except ValueError:
            continue
        try:
            coupon_value = float(coupon) if coupon not in ("", ".") else None
        except ValueError:
            coupon_value = None
        seen.add(isin)
        out.append({
            "country": ncb.upper(),
            "isin": isin.upper(),
            "issuer": issuer,
            "maturity": maturity,
            "coupon": coupon_value,
        })
    return out


def _years(maturity, today_iso):
    try:
        return round((date.fromisoformat(maturity) - date.fromisoformat(today_iso)).days / 365.25, 2)
    except ValueError:
        return None


def euro_book(bonds, today_iso):
    """Issue-level euro book plus country, issuer and maturity-wall rolls."""
    countries = {}
    issuers = {}
    wall = {}
    packed = []
    for bond in bonds:
        years = _years(bond["maturity"], today_iso)
        row = dict(bond)
        row["years"] = years
        packed.append(row)
        countries[bond["country"]] = countries.get(bond["country"], 0) + 1
        year = bond["maturity"][:4]
        wall[year] = wall.get(year, 0) + 1
        bucket = issuers.setdefault(bond["issuer"], {
            "issuer": bond["issuer"], "country": bond["country"], "n": 0,
            "coupon_min": None, "coupon_max": None, "nearest": None, "furthest": None,
        })
        bucket["n"] += 1
        if bond["country"] and bucket["country"] != bond["country"]:
            bucket["country"] = "multi"
        coupon = bond.get("coupon")
        if coupon is not None:
            bucket["coupon_min"] = coupon if bucket["coupon_min"] is None else min(bucket["coupon_min"], coupon)
            bucket["coupon_max"] = coupon if bucket["coupon_max"] is None else max(bucket["coupon_max"], coupon)
        if bucket["nearest"] is None or bond["maturity"] < bucket["nearest"]:
            bucket["nearest"] = bond["maturity"]
        if bucket["furthest"] is None or bond["maturity"] > bucket["furthest"]:
            bucket["furthest"] = bond["maturity"]
    packed.sort(key=lambda item: (item["maturity"], item["issuer"], item["isin"]))
    issuer_rows = sorted(issuers.values(), key=lambda item: (-item["n"], item["issuer"]))
    return {
        "n_bonds": len(packed),
        "n_issuers": len(issuer_rows),
        "n_countries": len(countries),
        "by_country": [{"country": key, "n": countries[key]} for key in sorted(countries, key=lambda k: -countries[k])],
        "maturity_wall": [{"year": year, "n": wall[year]} for year in sorted(wall)],
        "issuers": issuer_rows,
        "bonds": packed,
    }



def _tokens(name):
    cleaned = re.sub(r"[^A-Z0-9 ]", " ", (name or "").upper())
    return [tok for tok in cleaned.split() if tok not in _STOP and len(tok) > 2]


def _cds_rows(packet):
    groups = (packet or {}).get("groups") or {}
    rows = []
    for key in ("us_corp", "global_corp"):
        block = groups.get(key) or {}
        for row in block.get("rows") or []:
            if isinstance(row, dict) and row.get("name"):
                rows.append(row)
    return rows


def attach_cds(issuer_rows, cds_packet):
    """Unique token overlap only. A tie is left unmatched rather than guessed."""
    catalog = []
    for row in _cds_rows(cds_packet):
        tokens = _tokens(row.get("name"))
        if tokens:
            catalog.append((tokens, row))
    matched = 0
    for issuer in issuer_rows:
        issuer.pop("cds_bp", None)
        issuer.pop("cds_name", None)
        issuer.pop("cds_sector", None)
        want = _tokens(issuer.get("issuer"))
        if not want:
            continue
        scored = []
        want_set = set(want)
        for tokens, row in catalog:
            overlap = want_set & set(tokens)
            if not overlap or not any(len(tok) >= 4 for tok in overlap):
                continue
            score = len(overlap) / max(len(want_set), 1)
            if score >= 0.6:
                scored.append((score, row.get("name"), row))
        scored.sort(key=lambda item: (-item[0], item[1] or ""))
        if len(scored) == 1 or (len(scored) > 1 and scored[0][0] > scored[1][0]):
            row = scored[0][2]
            spread = _num(row.get("spread_bp"))
            if spread is None:
                continue
            issuer["cds_bp"] = _round(spread, 1)
            issuer["cds_name"] = row.get("name")
            issuer["cds_sector"] = row.get("sector")
            matched += 1
    return matched


def _field(row, *names):
    for name in names:
        if isinstance(row, dict) and row.get(name) is not None:
            return row.get(name)
    return None


def _date_of(row):
    value = _field(row, "tradeReportDate", "tradeDate", "reportDate")
    if isinstance(value, str) and len(value) >= 10:
        return value[:10]
    return None


def latest_slice(rows):
    """Rows from the newest observation date. None when the feed did not answer."""
    if not isinstance(rows, list) or not rows:
        return None
    dated = [( _date_of(row), row) for row in rows if isinstance(row, dict) and _date_of(row)]
    if not dated:
        return {"as_of": None, "rows": rows[:40], "n_returned": len(rows)}
    day = max(item[0] for item in dated)
    chosen = [item[1] for item in dated if item[0] == day]
    return {"as_of": day, "rows": chosen, "n_returned": len(rows)}


def _category(row):
    return str(_field(row, "productCategory", "productType", "gradeCode", "category") or "unspecified")


def compact_breadth(snapshot):
    if not snapshot:
        return None
    out = []
    for row in snapshot["rows"]:
        out.append({
            "category": _category(row),
            "advances": _num(_field(row, "advances", "numberOfIssuesAdvancing")),
            "declines": _num(_field(row, "declines", "numberOfIssuesDeclining")),
            "unchanged": _num(_field(row, "unchanged", "numberOfIssuesUnchanged")),
            "trades": _num(_field(row, "totalTrades", "numberOfTrades")),
            "volume": _num(_field(row, "totalVolume", "parValueTraded")),
            "high_52w": _num(_field(row, "fiftyTwoWeekHigh")),
            "low_52w": _num(_field(row, "fiftyTwoWeekLow")),
        })
    out.sort(key=lambda item: item["category"])
    return {"as_of": snapshot["as_of"], "rows": out, "volume_unit": "FINRA source units, not rescaled"}


def compact_sentiment(snapshot):
    if not snapshot:
        return None
    out = []
    for row in snapshot["rows"]:
        out.append({
            "category": _category(row),
            "side": str(_field(row, "tradeType", "side") or "all"),
            "trades": _num(_field(row, "totalTrades")),
            "transactions": _num(_field(row, "totalTransactions")),
            "volume": _num(_field(row, "totalVolume")),
        })
    out.sort(key=lambda item: (item["category"], item["side"]))
    return {"as_of": snapshot["as_of"], "rows": out, "volume_unit": "FINRA source units, not rescaled"}


def compact_capped(snapshot):
    """Keep the latest day's grade/maturity buckets. Drop the long history."""
    if not snapshot:
        return None
    out = []
    for row in snapshot["rows"]:
        item = {"grade": _category(row)}
        for key, value in row.items():
            if key in ("tradeReportDate", "tradeDate", "reportDate", "recordLoadTimestamp"):
                continue
            number = _num(value)
            if number is not None and key != "productCategory":
                item[key] = number
            elif key in ("gradeCode", "productCategory") and isinstance(value, str):
                item["grade"] = value
        out.append(item)
    return {"as_of": snapshot["as_of"], "rows": out[:80]}


def read_line(sleeves, trace):
    """One descriptive sentence. Not a trade."""
    bits = []
    by_id = {row["id"]: row for row in sleeves}
    hy, ig = by_id.get("us_hy"), by_id.get("us_ig")
    if hy and hy.get("level") is not None:
        pct = hy.get("pct_5y")
        where = "inside its own 5-year range" if pct is None else "at the %s percentile of its own 5-year range" % _round(pct, 0)
        bits.append("US high-yield OAS is %s bp, %s" % (hy["level"], where))
    if ig and ig.get("level") is not None and hy and hy.get("level") is not None:
        bits.append("IG is %s bp, so the HY-IG gap is %s bp" % (ig["level"], _round(hy["level"] - ig["level"], 1)))
    breadth = ((trace or {}).get("corporate") or {}).get("breadth") or {}
    all_row = next((row for row in breadth.get("rows") or [] if row.get("category") == "all securities"), None)
    if all_row and all_row.get("advances") is not None and all_row.get("declines") is not None:
        bits.append("TRACE corporate issues %s up / %s down on %s" % (
            int(all_row["advances"]), int(all_row["declines"]), breadth.get("as_of") or "the last tape"))
    if not bits:
        return "Feeds have not filled this read yet."
    return ". ".join(bits) + "."


def coverage_block():
    return {
        "us": "FINRA TRACE corporate and Rule 144A market breadth, sentiment and capped volume. This is the whole disseminated US corporate tape rolled up by grade, not a sample. Individual CUSIPs are not in the public aggregate API.",
        "euro_issues": "Every bond on the ECB weekly CSPP/PEPP corporate holdings list: ISIN, issuer, coupon, maturity and the purchasing national central bank. It is the Eurosystem book, not every euro corporate outstanding.",
        "international_indices": "ICE BofA option-adjusted spread and yield sleeves on FRED for US ratings and tenors, Euro high yield, emerging-market corporates, EM high grade and Asia USD. A sleeve that FRED does not publish is omitted, not zeroed.",
        "companies": "Individual bonds inside public daily holdings: iShares broad USD investment-grade and high-yield (plus the liquid and short books), EM corporate and EM high-yield, euro investment-grade and high-yield, convertibles, floating-rate, the iShares Core euro corporate UCITS book, corporate lines of the iShares core international aggregate (local market currency: euro, sterling, Canada, yen and others), and SPDR international corporate. Industry is the fund sector. Yield is yield-to-worst when the file prints it, otherwise yield-to-maturity. Duration is modified duration when the file prints it, otherwise the duration column, and it is not estimated. Price and yield changes use the previous date that same holdings feed lists, usually the prior month-end. A longer window uses the earliest earlier date that feed still lists, usually the prior year-end, and only when both files print the bond. Demand is market value held by those funds, not TRACE volume and not the amount outstanding. Demand change is that market value minus the same sum on the previous holdings date. A bond the previous file held and this file does not is marked left; its price is the last print, not a live price. Versus-industry is the bond yield minus its industry's demand-weighted yield, not an option-adjusted spread. CUSIPs are not republished.",
        "not_issue_level": [
            "TRACE CUSIP blotter (public aggregate API has no prints; no CUSIP licence)",
            "A yield the holdings file does not print (left blank, not estimated)",
            "A duration the holdings file does not print (not estimated from coupon and maturity)",
            "A price or yield move for a bond that was not in the previous holdings date",
            "A demand change when that fund's previous holdings file did not load",
            "Option-adjusted spread on a single bond",
            "Bonds no scanned fund holds",
        ],
    }


def build_packet(generated_at, today_iso, sleeves, trace, euro, cds_matched, sources, issues=None):
    live = [row for row in sleeves if row]
    as_of_candidates = [row["as_of"] for row in live if row.get("as_of")]
    if euro and euro.get("bonds"):
        as_of_candidates.append(today_iso)
    breadth = ((trace.get("corporate") or {}).get("breadth") or {})
    if breadth.get("as_of"):
        as_of_candidates.append(breadth["as_of"])
    if issues and issues.get("as_of"):
        as_of_candidates.append(issues["as_of"])
    packet = {
        "engine": "justhodl-corp-bonds",
        "version": VERSION,
        "generated_at": generated_at,
        "as_of": max(as_of_candidates) if as_of_candidates else today_iso,
        "read": read_line(live, trace),
        "decision": {"call": None, "sizing_eligible": False, "basis": "descriptive public measurements only"},
        "coverage": coverage_block(),
        "sleeves": live,
        "trace": trace,
        "euro": euro,
        "cds_matched_issuers": cds_matched,
        "sources": sources,
    }
    if issues:
        packet["issues"] = issues
        count = issues.get("n_bonds") or 0
        if count:
            extra = " The company book lists %s bonds from %s issuers." % (count, issues.get("n_issuers") or 0)
            changed = issues.get("n_with_change") or 0
            moved = issues.get("n_with_move") or 0
            if moved:
                extra += " Price moves versus %s cover %s bonds." % (issues.get("move_as_of") or "the previous holdings date", moved)
            elif changed:
                extra += " Yield changes versus the previous snapshot cover %s bonds." % changed
            else:
                extra += " Price and yield moves stay blank until a previous holdings date is available."
            if issues.get("n_with_ytd") and issues.get("ytd_as_of"):
                extra += " The longer window versus %s covers %s bonds." % (issues.get("ytd_as_of"), issues.get("n_with_ytd"))
            if issues.get("n_with_flow"):
                extra += " Demand changes cover %s bonds." % issues.get("n_with_flow")
            if issues.get("n_left"):
                extra += " %s bonds left the scanned funds." % issues.get("n_left")
            if issues.get("n_with_dur"):
                extra += " Modified duration is printed on %s bonds." % issues.get("n_with_dur")
            packet["read"] = packet["read"].rstrip() + extra
    return packet


def iso_now(now=None):

    moment = now or datetime.utcnow()
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ"), moment.date().isoformat()

