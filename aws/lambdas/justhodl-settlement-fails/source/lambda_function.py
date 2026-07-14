"""
justhodl-settlement-fails — dealer settlement fails (fails to deliver & fails to
receive) from the NY Fed Primary Dealer Statistics (FR 2004).

Settlement fails = trades that don't settle on time because the security can't be
sourced (or cash/collateral can't be moved). They spike during:
  • collateral squeezes / specials pressure in repo,
  • forced deleveraging and operational gridlock (2008, Mar-2020),
  • sharp risk-off bond routs (2022 UK-LDI / gilt-UST stress).
That makes the fails series a clean plumbing-stress / black-swan / market-top tell:
when fails blow out, the financial plumbing is jamming.

Reports BOTH sides across 6 asset classes: U.S. Treasury (ex-TIPS), TIPS,
corporate securities, agency MBS, agency debt, and other (non-agency) MBS.

OUTPUT: data/settlement-fails.json     SCHEDULE: daily 21:30 UTC (PD posts weekly Thu)
Real official data only — not investment advice.
"""
import json
import urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/settlement-fails.json"
NY = "https://markets.newyorkfed.org/api/pd"
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com"}

# class key -> (fails-to-deliver series, fails-to-receive series, label)
CLASSES = [
    ("ust_ex_tips", "PDFTD-USTET", "PDFTR-USTET", "U.S. Treasury (ex-TIPS)"),
    ("tips",        "PDFTD-UST",   "PDFTR-UST",   "TIPS"),
    ("corporate",   "PDFTD-CS",    "PDFTR-CS",    "Corporate securities"),
    ("agency_mbs",  "PDFTD-FGM",   "PDFTR-FGM",   "Agency MBS"),
    ("agency_debt", "PDFTD-FGEM",  "PDFTR-FGEM",  "Agency debt (ex-MBS)"),
    ("other_mbs",   "PDFTD-OM",    "PDFTR-OM",    "Other (non-agency) MBS"),
]


def _get(url, t=45):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=t) as r:
            return json.loads(r.read().decode("utf-8", "ignore"))
    except Exception as e:
        print("fetch fail %s: %s" % (url[-40:], e))
        return None


def fetch(key):
    """Return [[asofdate, $bn]] ascending, dropping masked (*) values."""
    j = _get(NY + "/get/%s.json" % key)
    out = []
    if j:
        for t in j.get("pd", {}).get("timeseries", []):
            v = t.get("value")
            if v not in ("*", "", None):
                try:
                    out.append([t["asofdate"], round(float(v) / 1000.0, 2)])  # $m -> $bn
                except Exception:
                    pass
    out.sort(key=lambda x: x[0])
    return out


def combine(a, b):
    """Sum two date-keyed series (deliver + receive) on shared dates."""
    db = dict(b)
    return [[d, round(v + db[d], 2)] for d, v in a if d in db]


def sum_series(list_of_series):
    agg = {}
    for s in list_of_series:
        for d, v in s:
            agg[d] = round(agg.get(d, 0) + v, 2)
    return sorted(([d, v] for d, v in agg.items()), key=lambda x: x[0])


def stats(pts):
    vs = [p[1] for p in pts]
    if not vs:
        return {}
    n = len(vs); latest = vs[-1]
    mean = sum(vs) / n
    var = sum((x - mean) ** 2 for x in vs) / n
    sd = var ** 0.5
    z = round((latest - mean) / sd, 2) if sd else 0.0
    pctile = round(sum(1 for x in vs if x <= latest) / n * 100, 1)
    avg52 = round(sum(vs[-52:]) / min(52, n), 1)
    return {"latest": round(latest, 1), "mean": round(mean, 1), "max": round(max(vs), 1),
            "min": round(min(vs), 1), "z": z, "pctile": pctile, "avg_52w": avg52,
            "n_obs": n, "start": pts[0][0], "as_of": pts[-1][0],
            "spike": bool(z >= 2 or pctile >= 95)}


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc).isoformat()

    classes = []
    ftd_all, ftr_all = [], []
    for key, dk, rk, label in CLASSES:
        ftd = fetch(dk); ftr = fetch(rk)
        if not ftd and not ftr:
            continue
        comb = combine(ftd, ftr)
        if ftd:
            ftd_all.append(ftd)
        if ftr:
            ftr_all.append(ftr)
        deep, seen = [], set()
        for d, v in comb or ftd:
            mk = d[:7]
            if mk not in seen:
                deep.append([d, v])
                seen.add(mk)
        classes.append({
            "key": key, "label": label,
            "deep": deep, "deep_start": (deep[0][0] if deep else None),
            "ftd": ftd[-720:], "ftr": ftr[-720:], "combined": comb[-720:],
            "ftd_latest": (ftd[-1][1] if ftd else None),
            "ftr_latest": (ftr[-1][1] if ftr else None),
            "stats": stats(comb if comb else ftd),
        })

    total_ftd = sum_series(ftd_all)
    total_ftr = sum_series(ftr_all)
    total_comb = combine(total_ftd, total_ftr)

    head = next((c for c in classes if c["key"] == "ust_ex_tips"), None)
    hs = head["stats"] if head else {}

    # regime from the Treasury-fails percentile / z (the canonical plumbing tell)
    pct = hs.get("pctile", 0); z = hs.get("z", 0)
    if pct >= 97 or z >= 2.5:
        regime, score = "CRISIS", min(100, 80 + (pct - 97) * 6)
    elif pct >= 90 or z >= 1.5:
        regime, score = "STRESS", 60 + (pct - 90) * 2
    elif pct >= 70 or z >= 0.7:
        regime, score = "ELEVATED", 40 + (pct - 70)
    else:
        regime, score = "CALM", round(pct * 0.5)

    drivers = []
    if head:
        drivers.append("Treasury (ex-TIPS) settlement fails $%.0fbn combined (deliver $%.0fbn + receive $%.0fbn), %.0f%%ile / z %+.1f \u2014 %s"
                       % (head["stats"].get("latest", 0), head["ftd_latest"] or 0, head["ftr_latest"] or 0,
                          pct, z, regime.lower()))
    for c in classes:
        st = c["stats"]
        if c["key"] != "ust_ex_tips" and st.get("spike"):
            drivers.append("SPIKE: %s fails $%.0fbn (%.0f%%ile, z %+.1f)" % (c["label"], st.get("latest", 0), st.get("pctile", 0), st.get("z", 0)))
    if total_comb:
        drivers.append("All-asset fails $%.0fbn combined across 6 classes" % total_comb[-1][1])
    if regime == "CALM":
        drivers.append("Plumbing settling normally \u2014 no collateral-sourcing stress in the fails data")

    out = {
        "engine": "settlement-fails", "version": "1.0.0", "generated_at": now,
        "as_of": (head["stats"]["as_of"] if head else (total_comb[-1][0] if total_comb else None)),
        "signal": {"regime": regime, "score": round(score), "drivers": drivers},
        "headline": {"label": "U.S. Treasury (ex-TIPS)",
                     "ftd_bn": (head["ftd_latest"] if head else None),
                     "ftr_bn": (head["ftr_latest"] if head else None),
                     "combined_bn": (hs.get("latest") if head else None),
                     "z": z, "pctile": pct, "max_bn": hs.get("max"),
                     "combined": (head["combined"] if head else [])},
        "classes": classes,
        "totals": {"ftd": total_ftd[-720:], "ftr": total_ftr[-720:], "combined": total_comb[-720:]},
        "source": "NY Fed Primary Dealer Statistics (FR 2004) \u2014 dealer financing settlement fails, weekly, $bn par",
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    return {"statusCode": 200, "body": json.dumps({
        "regime": regime, "score": round(score), "as_of": out["as_of"],
        "ust_combined_bn": hs.get("latest"), "ust_pctile": pct,
        "classes": len(classes), "total_combined_bn": (total_comb[-1][1] if total_comb else None)})}
