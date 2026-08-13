"""justhodl-catalyst v1.0.0 (ops 4654)

Khalid's catalyst doctrine as an engine: "a stock can have
beautiful fundamentals and do nothing — you need something
capable of forcing investors to reconsider its value."

Fuses in-fleet primary evidence into per-ticker catalyst stacks:
  PR/news tape (deal-scanner)  -> classified by taxonomy regex
  backlog engine (XBRL)        -> MAJOR_BACKLOG, EARNINGS_INFLECTION
  fundamental census matrix    -> LARGE_BUYBACK, DEBT_REDUCTION,
                                  MARGIN_TURNAROUND
  industry-boom league         -> INDUSTRY_TAILWIND / RECOVERY
  liquidity/blackswan rows     -> COMMODITY_INFLECTION (sector-
                                  mapped), RATE_CUTS (macro flag)

Every catalyst carries class, evidence text, source, and weight.
Nothing inferred without a named source; absences are absences.
"""
import json
import re
from datetime import datetime, timezone

import boto3

BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/catalyst.json"
s3 = boto3.client("s3", region_name="us-east-1")


def s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None


TAXONOMY = [
    ("FDA_APPROVAL", r"fda (approval|clearance|approves)|"
     r"breakthrough (therapy|device)|phase (2|3|ii|iii) "
     r"(data|results|trial)", 3.0),
    ("MAJOR_CONTRACT", r"(contract|order|award)s? (worth|"
     r"valued|of) \$|awarded (a |an )?\$|multi-?year "
     r"(contract|agreement)|task order", 2.5),
    ("MAJOR_CUSTOMER", r"(selected by|partners? with|"
     r"partnership with|customer win|expands? .* with) "
     r"(amazon|aws|microsoft|azure|google|apple|meta|nvidia|"
     r"tesla|boeing|walmart|openai)", 2.5),
    ("AI_DATACENTER", r"\bai\b|artificial intelligence|"
     r"data ?center|gpu|accelerator|inference|hyperscaler",
     2.0),
    ("NEW_PRODUCT", r"launch(es|ed)?|unveil|introduc(es|ing)|"
     r"next-?gen|new (product|platform|chip|drug|device)",
     1.5),
    ("BUYBACK_ANNOUNCED", r"(share|stock) (repurchase|buyback)"
     r"|buyback program|repurchase authorization", 2.0),
    ("GUIDANCE_RAISE", r"raises? (full.?year |annual |fy )?"
     r"guidance|guidance raised|hikes? (outlook|forecast)|"
     r"above (prior )?guidance", 2.5),
    ("RESTRUCTURING", r"restructur|cost (cut|reduction) "
     r"program|streamlin|workforce reduction", 1.5),
    ("NEW_MANAGEMENT", r"appoints? (new )?(ceo|cfo|coo|chief)"
     r"|names? .* (ceo|cfo)|steps down", 1.5),
    ("DEBT_REDUCTION", r"(repay|redeem|retire)s? .*debt|"
     r"debt (reduction|repayment)|deleverag", 1.5),
    ("ACTIVIST_STAKE", r"activist|13-?d|starboard|elliott|"
     r"icahn|third point|jana partners", 2.0),
    ("SPINOFF_DIVEST", r"spin-?off|divestiture|divests?|"
     r"separat(e|ion) of|carve-?out", 1.8),
    ("MA_EVENT", r"acquir(es|ing|ed)|merger|to be acquired|"
     r"takeover|buyout", 2.2),
]


def classify_title(title):
    t = (title or "").lower()
    hits = []
    for cls, pat, w in TAXONOMY:
        if re.search(pat, t):
            hits.append((cls, w))
    return hits


def g(row, *names):
    for n in names:
        v = row.get(n)
        if isinstance(v, (int, float)):
            return float(v)
    return None


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc).isoformat(
        timespec="seconds")
    deals = (s3_json("data/deal-scanner.json") or {}
             ).get("deals") or []
    bx = (s3_json("data/backlog.json") or {}
          ).get("by_ticker") or {}
    boom = (s3_json("data/industry-boom.json") or {})
    league = {str(x.get("industry", "")).lower(): x
              for x in boom.get("league") or []}
    mx = s3_json("data/fundamental-census-matrix.json") or {}
    cols = mx.get("cols") or {}
    ticks = mx.get("tickers") or []
    inds = mx.get("industries") or []
    sects = mx.get("sectors") or []

    def col(name):
        c = cols.get(name)
        return c if isinstance(c, list) else [None] * len(ticks)

    by = {}

    def add(t, cls, ev, src, w):
        t = str(t or "").upper()
        if not t:
            return
        e = by.setdefault(t, {"catalysts": [], "score": 0.0})
        if any(c["class"] == cls and c["evidence"] == ev
               for c in e["catalysts"]):
            return
        e["catalysts"].append({"class": cls,
                               "evidence": ev[:140],
                               "src": src, "ts": now,
                               "weight": w})
        e["score"] = round(e["score"] + w, 2)

    # 1) PR tape classification
    for d in deals:
        sym = d.get("symbol") or d.get("ticker")
        title = d.get("title") or d.get("headline") or ""
        for cls, w in classify_title(title):
            add(sym, cls, title, "deal-scanner PR tape", w)
        ec = str(d.get("event_class") or "")
        if ec and not classify_title(title):
            add(sym, "EVENT_" + ec.upper()[:18], title,
                "deal-scanner class", 1.2)

    # 2) backlog engine: major backlog + earnings inflection
    for t, x in bx.items():
        ryoy = g(x, "rpo_yoy")
        rq = g(x, "rpo_qoq")
        if ryoy is not None and ryoy >= 15:
            add(t, "MAJOR_BACKLOG",
                "RPO +%.1f%% YoY (XBRL %s)"
                % (ryoy, x.get("rpo_asof") or ""),
                "backlog engine", 2.2)
        elif rq is not None and rq >= 8:
            add(t, "BACKLOG_ACCEL",
                "RPO +%.1f%% QoQ" % rq, "backlog engine", 1.6)
        eq = g(x, "eps_qoq")
        ey = g(x, "eps_yoy")
        if eq is not None and eq >= 20 and (ey or 0) >= 0:
            add(t, "EARNINGS_INFLECTION",
                "EPS +%.1f%% QoQ (XBRL diluted)" % eq,
                "backlog engine EPS", 2.0)

    # 3) census matrix: buyback / debt / margin turnaround
    nb = col("net_buyback_yield_pct")
    sc_ = col("share_count_yoy_pct")
    dbt = col("net_debt_to_ebitda")
    dch = col("net_debt_yoy_pct")
    om = col("operating_margin_pct_chg")
    om2 = col("operating_margin_chg_pp")
    for i, t in enumerate(ticks):
        v = nb[i] if i < len(nb) else None
        if isinstance(v, (int, float)) and v >= 4:
            add(t, "LARGE_BUYBACK",
                "net buyback yield %.1f%%/yr (census)" % v,
                "fundamental census", 1.8)
        d1 = dch[i] if i < len(dch) else None
        if isinstance(d1, (int, float)) and d1 <= -15:
            add(t, "DEBT_REDUCTION",
                "net debt %.1f%% YoY (census)" % d1,
                "fundamental census", 1.5)
        mv = None
        for arr in (om, om2):
            if i < len(arr) and isinstance(arr[i],
                                           (int, float)):
                mv = arr[i]
                break
        if mv is not None and mv >= 2.5:
            add(t, "MARGIN_TURNAROUND",
                "op margin +%.1fpp (census)" % mv,
                "fundamental census", 1.8)

    # 4) industry boom: tailwind per member ticker
    for i, t in enumerate(ticks):
        ind = str(inds[i] if i < len(inds) else "").lower()
        row = league.get(ind)
        if row and isinstance(row.get("boom_score"),
                              (int, float)) \
                and row["boom_score"] >= 75:
            add(t, "INDUSTRY_TAILWIND",
                "%s boom %.1f (top-decile heat)"
                % (row.get("industry"), row["boom_score"]),
                "industry-boom", 1.2)

    # 5) macro layer: commodity inflection + rate path
    macro = {}
    lq = (s3_json("data/liquidity-reversal.json") or {}
          ).get("rows") or []
    lqmap = {x.get("symbol"): x for x in lq}
    for sym, label, sect_t in (
            ("CAPITALCOM:COPPER", "copper", "Basic Materials"),
            ("TVC:USOIL", "oil", "Energy"),
            ("TVC:GOLD", "gold", "Basic Materials")):
        x = lqmap.get(sym) or {}
        z = x.get("move_z")
        if isinstance(z, (int, float)) and abs(z) >= 1.5:
            macro[label + "_z"] = z
            for i, t in enumerate(ticks):
                if (sects[i] if i < len(sects) else ""
                        ) == sect_t:
                    add(t, "COMMODITY_INFLECTION",
                        "%s z=%.2f (%s)" % (label, z,
                                            "up" if z > 0
                                            else "down"),
                        "liquidity rows", 1.0)
    ff = None
    for cand in ("FRED:DFF", "FRED:FEDFUNDS",
                 "FRED:DFEDTARU"):
        x = lqmap.get(cand)
        if x and x.get("trend_state"):
            ff = x
            break
    if ff:
        macro["fedfunds_trend"] = ff.get("trend_state")
        macro["rate_cuts"] = ff.get("trend_state") == "DOWN"

    for t, e in by.items():
        e["catalysts"].sort(key=lambda c: -c["weight"])
        e["catalysts"] = e["catalysts"][:6]
        e["top_class"] = e["catalysts"][0]["class"] \
            if e["catalysts"] else None

    census = {}
    for e in by.values():
        for c in e["catalysts"]:
            census[c["class"]] = census.get(c["class"], 0) + 1

    payload = {
        "engine": "justhodl-catalyst",
        "as_of": now,
        "doctrine": "catalysts force revaluation; every entry "
                    "carries class+evidence+source+weight from "
                    "in-fleet primary stores; macro flags are "
                    "context, never fabricated per-ticker",
        "taxonomy": [c for c, _, _ in TAXONOMY] + [
            "MAJOR_BACKLOG", "BACKLOG_ACCEL",
            "EARNINGS_INFLECTION", "LARGE_BUYBACK",
            "DEBT_REDUCTION", "MARGIN_TURNAROUND",
            "INDUSTRY_TAILWIND", "COMMODITY_INFLECTION"],
        "macro": macro,
        "class_census": census,
        "n_tickers": len(by),
        "by_ticker": by}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload).encode(),
                  ContentType="application/json")
    return {"tickers": len(by), "classes": len(census)}
