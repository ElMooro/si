"""justhodl-fdic-bankfind — FDIC BankFind Suite API (api.fdic.gov/banks), free, keyless.

Bank-level balance-sheet fragility for every FDIC-insured institution, straight
from the quarterly Call Report fields the FDIC publishes:

  uninsured deposit share      DEPUNA / DEP
  HTM unrealised loss / equity (SCHA - SCHF) / EQ        (SVB 2022Q4: 98%)
  CRE concentration / Tier 1   (LNRECONS + LNREMULT + LNRENRES) / RBCT1J
  brokered deposit share       BRO / DEP
  FHLB + other borrowings      (OTHBFHLB + OTHBOR) / ASSET
  noncurrent loan ratio        NCLNLSR
  CET1 ratio                   IDT1CER

Outputs: system aggregates as quarterly series (last N quarters), a screen of the
most fragile banks (descriptive ranking by the three classic run-risk metrics),
size-bucket breakdowns, and the raw latest-quarter extract mirrored under
data/warm/fdic-bankfind/src/. Descriptive only — no call, no sizing.
"""
from __future__ import annotations

import json
import os
import time
from datetime import date

import risk_sources as RS

SLUG = "fdic-bankfind"
API = "https://api.fdic.gov/banks"
FIELDS = ("CERT,REPDTE,NAME,STALP,CITY,ASSET,DEP,DEPUNA,DEPINS,EQ,SC,SCHA,SCHF,SCAF,BRO,"
          "LNRECONS,LNREMULT,LNRENRES,LNLSNET,RBCT1J,IDT1CER,IDT1RWAJR,NCLNLS,NCLNLSR,NAASSET,"
          "ROA,ROE,NETINC,OTHBFHLB,OTHBOR,CHBAL,LIAB")
N_QUARTERS = int(os.environ.get("N_QUARTERS", "28"))
SCREEN_MIN_ASSET = 1_000_000          # $1bn (API values are $ thousands)
SCREEN_ROWS = 60


def _q(d):
    return d.replace("-", "")


def quarter_ends(n, latest):
    y, m = int(latest[:4]), int(latest[4:6])
    out = []
    for _ in range(n):
        out.append(f"{y}{m:02d}{(date(y + (m // 12), (m % 12) + 1, 1) - date.resolution).day:02d}")
        m -= 3
        if m <= 0:
            m += 12
            y -= 1
    return out


def fetch_quarter(repdte, limit=10000):
    url = (f"{API}/financials?filters=REPDTE%3A{repdte}&fields={FIELDS}"
           f"&limit={limit}&format=json")
    d = RS.http_json(url, timeout=120)
    return [x["data"] for x in d.get("data", [])]


def latest_repdte():
    d = RS.http_json(f"{API}/financials?filters=CERT%3A3511&fields=REPDTE&sort_by=REPDTE&sort_order=DESC&limit=1&format=json")
    return d["data"][0]["data"]["REPDTE"]


def _f(x):
    v = RS.parse_num(x)
    return v if v is not None else 0.0


def _r2(x):
    v = RS.parse_num(x)
    return None if v is None else round(v, 2)


def bank_metrics(r):
    dep = _f(r.get("DEP")); eq = _f(r.get("EQ")); asset = _f(r.get("ASSET")); t1 = _f(r.get("RBCT1J"))
    htm_loss = _f(r.get("SCHA")) - _f(r.get("SCHF"))      # positive = unrealised loss
    cre = _f(r.get("LNRECONS")) + _f(r.get("LNREMULT")) + _f(r.get("LNRENRES"))
    return {
        "cert": r.get("CERT"), "name": r.get("NAME"), "state": r.get("STALP"), "city": r.get("CITY"),
        "asset_musd": round(asset / 1000.0, 1),
        "uninsured_share": round(100.0 * _f(r.get("DEPUNA")) / dep, 1) if dep else None,
        "htm_loss_to_equity": round(100.0 * htm_loss / eq, 1) if eq else None,
        "htm_loss_musd": round(htm_loss / 1000.0, 1),
        "cre_to_tier1": round(100.0 * cre / t1, 0) if t1 else None,
        "brokered_share": round(100.0 * _f(r.get("BRO")) / dep, 1) if dep else None,
        "wholesale_to_assets": round(100.0 * (_f(r.get("OTHBFHLB")) + _f(r.get("OTHBOR"))) / asset, 1) if asset else None,
        "noncurrent_ratio": _r2(r.get("NCLNLSR")),
        "cet1_ratio": _r2(r.get("IDT1CER")),
        "roa": _r2(r.get("ROA")),
        "equity_to_assets": round(100.0 * eq / asset, 1) if asset else None,
    }


def aggregate(rows):
    S = lambda k: sum(_f(r.get(k)) for r in rows)  # noqa: E731
    dep, asset, eq, t1 = S("DEP"), S("ASSET"), S("EQ"), S("RBCT1J")
    # only institutions >= $1bn report estimated uninsured deposits (DEPUNA); measure the share on that reporting base
    rep = [r for r in rows if _f(r.get("ASSET")) >= SCREEN_MIN_ASSET and _f(r.get("DEP")) > 0]
    rep_dep = sum(_f(r.get("DEP")) for r in rep)
    htm_loss = S("SCHA") - S("SCHF")
    cre = S("LNRECONS") + S("LNREMULT") + S("LNRENRES")
    lns = S("LNLSNET")
    big_loss = [r for r in rows if _f(r.get("EQ")) > 0 and (_f(r.get("SCHA")) - _f(r.get("SCHF"))) / _f(r.get("EQ")) > 0.5]
    high_unins = [r for r in rows if _f(r.get("DEP")) > 0 and _f(r.get("DEPUNA")) / _f(r.get("DEP")) > 0.5 and _f(r.get("ASSET")) >= SCREEN_MIN_ASSET]
    high_cre = [r for r in rows if _f(r.get("RBCT1J")) > 0 and (_f(r.get("LNRECONS")) + _f(r.get("LNREMULT")) + _f(r.get("LNRENRES"))) / _f(r.get("RBCT1J")) > 3.0]
    return {
        "n_banks": len(rows),
        "assets_tn": round(asset / 1e9, 3),
        "deposits_tn": round(dep / 1e9, 3),
        "uninsured_share": round(100.0 * sum(_f(r.get("DEPUNA")) for r in rep) / rep_dep, 2) if rep_dep else None,
        "htm_loss_bn": round(htm_loss / 1e6, 1),
        "htm_loss_to_equity": round(100.0 * htm_loss / eq, 2) if eq else None,
        "cre_to_tier1": round(100.0 * cre / t1, 1) if t1 else None,
        "brokered_share": round(100.0 * S("BRO") / dep, 2) if dep else None,
        "wholesale_to_assets": round(100.0 * (S("OTHBFHLB") + S("OTHBOR")) / asset, 2) if asset else None,
        "noncurrent_ratio": round(100.0 * S("NCLNLS") / lns, 2) if lns else None,
        "equity_to_assets": round(100.0 * eq / asset, 2) if asset else None,
        "n_htm_loss_gt50_equity": len(big_loss),
        "n_uninsured_gt50_1bn": len(high_unins),
        "n_cre_gt300_tier1": len(high_cre),
    }


AGG_SERIES = [
    ("uninsured_share", "Uninsured deposits / total deposits — banks ≥ $1bn (reporting base)", "%"),
    ("htm_loss_to_equity", "HTM unrealised loss / equity — all FDIC banks", "%"),
    ("htm_loss_bn", "HTM unrealised loss — all FDIC banks", "$bn"),
    ("cre_to_tier1", "CRE loans / Tier 1 capital — all FDIC banks", "%"),
    ("brokered_share", "Brokered deposits / total deposits", "%"),
    ("wholesale_to_assets", "FHLB + other borrowings / assets", "%"),
    ("noncurrent_ratio", "Noncurrent loans / net loans", "%"),
    ("equity_to_assets", "Equity / assets", "%"),
    ("n_banks", "Institutions filing", "count"),
    ("n_htm_loss_gt50_equity", "Banks with HTM loss > 50% of equity", "count"),
    ("n_uninsured_gt50_1bn", "Banks ≥$1bn with uninsured deposits > 50%", "count"),
    ("n_cre_gt300_tier1", "Banks with CRE > 300% of Tier 1", "count"),
    ("assets_tn", "Total assets — all FDIC banks", "$tn"),
    ("deposits_tn", "Total deposits — all FDIC banks", "$tn"),
]


def build(event):
    pk = RS.Packet(
        SLUG, "FDIC BankFind", "FDIC BankFind — bank-level balance-sheet fragility",
        "Every FDIC-insured institution's quarterly Call Report, read from the free BankFind Suite API: "
        "uninsured deposit share, held-to-maturity unrealised losses against equity, commercial real estate "
        "concentration against Tier 1 capital, brokered and wholesale funding, noncurrent loans. System "
        "aggregates by quarter plus the banks that screen highest on the three classic run-risk measures.",
        {"provider": "FDIC — Federal Deposit Insurance Corporation", "url": "https://api.fdic.gov/banks",
         "docs": "https://api.fdic.gov/banks/docs/", "license": "Public domain (U.S. federal government)",
         "cadence": "quarterly Call Reports, ~55 days after quarter end"},
        cadence="daily check / quarterly data")
    latest = latest_repdte()
    quarters = quarter_ends(N_QUARTERS, latest)
    per_q = {}
    latest_rows = None
    for q in quarters:
        try:
            rows = fetch_quarter(q)
        except RuntimeError as e:
            pk.file_status(f"financials REPDTE={q}", f"{API}/financials?filters=REPDTE:{q}", f"error: {str(e)[:80]}")
            continue
        if not rows:
            continue
        per_q[q] = aggregate(rows)
        if latest_rows is None:
            latest_rows = rows
            pk.raw(f"financials_{q}.json", json.dumps(rows, separators=(",", ":")).encode(), "application/json",
                   url=f"{API}/financials?filters=REPDTE:{q}&fields={FIELDS}")
        time.sleep(0.3)
    if not per_q:
        raise RuntimeError("FDIC API returned no quarters")
    iso = lambda q: f"{q[:4]}-{q[4:6]}-{q[6:]}"  # noqa: E731
    for key, label, unit in AGG_SERIES:
        pts = [[iso(q), per_q[q][key]] for q in sorted(per_q) if per_q[q].get(key) is not None]
        pk.add_series(f"SYS_{key.upper()}", label, pts, unit=unit, freq="quarterly", group="system")
    # size buckets at latest quarter
    buckets = [("over_250bn", 250e9), ("50_250bn", 50e9), ("10_50bn", 10e9), ("1_10bn", 1e9), ("under_1bn", 0)]
    bucket_rows = []
    for name, floor in buckets:
        ceil = {"over_250bn": 1e18, "50_250bn": 250e9, "10_50bn": 50e9, "1_10bn": 10e9, "under_1bn": 1e9}[name]
        sel = [r for r in latest_rows if floor <= _f(r.get("ASSET")) * 1000 < ceil]
        if sel:
            a = aggregate(sel)
            bucket_rows.append([name.replace("_", " "), a["n_banks"], a["assets_tn"], a["uninsured_share"],
                                a["htm_loss_to_equity"], a["cre_to_tier1"], a["brokered_share"], a["noncurrent_ratio"]])
    pk.add_table("size_buckets", ["Size bucket", "Banks", "Assets $tn", "Uninsured %", "HTM loss / equity %",
                                  "CRE / Tier 1 %", "Brokered %", "Noncurrent %"], bucket_rows,
                 title=f"By asset size — {iso(latest)}",
                 note="Aggregates of the latest Call Report quarter; thresholds in the screen follow FDIC/OCC guidance (CRE > 300% of capital) and the 2023 run episodes (uninsured > 50%).")
    # fragility screen (banks >= $1bn)
    mets = [bank_metrics(r) for r in latest_rows if _f(r.get("ASSET")) >= SCREEN_MIN_ASSET]

    def rank(key, reverse=True):
        vals = sorted([m[key] for m in mets if m[key] is not None], reverse=reverse)
        return {v: i for i, v in enumerate(vals)}
    r_un, r_htm, r_cre = rank("uninsured_share"), rank("htm_loss_to_equity"), rank("cre_to_tier1")
    n = max(1, len(mets))
    for m in mets:
        parts = []
        for key, rk in (("uninsured_share", r_un), ("htm_loss_to_equity", r_htm), ("cre_to_tier1", r_cre)):
            parts.append(100.0 * (1 - rk.get(m[key], n) / n) if m[key] is not None else 0.0)
        m["screen_score"] = round(sum(parts) / 3.0, 1)
    mets.sort(key=lambda m: -m["screen_score"])
    cols = ["Bank", "State", "Assets $m", "Uninsured %", "HTM loss / equity %", "CRE / Tier 1 %", "Brokered %",
            "Wholesale / assets %", "Noncurrent %", "CET1 %", "Screen"]
    rows_out = [[m["name"], m["state"], m["asset_musd"], m["uninsured_share"], m["htm_loss_to_equity"], m["cre_to_tier1"],
                 m["brokered_share"], m["wholesale_to_assets"], m["noncurrent_ratio"], m["cet1_ratio"], m["screen_score"]]
                for m in mets[:SCREEN_ROWS]]
    pk.add_table("fragility_screen", cols, rows_out,
                 title=f"Run-risk screen — banks ≥ $1bn, {iso(latest)}",
                 note="Screen = mean percentile rank across uninsured share, HTM loss / equity and CRE / Tier 1 among banks ≥ $1bn. A descriptive ordering of public filings, not an assessment of any institution.")
    # largest banks snapshot
    big = sorted(mets, key=lambda m: -m["asset_musd"])[:30]
    pk.add_table("largest_banks", cols[:-1], [[m["name"], m["state"], m["asset_musd"], m["uninsured_share"], m["htm_loss_to_equity"],
                                               m["cre_to_tier1"], m["brokered_share"], m["wholesale_to_assets"], m["noncurrent_ratio"], m["cet1_ratio"]] for m in big],
                 title="30 largest institutions")
    a = per_q[latest]
    pk.kpi("Call Report quarter", iso(latest), f"{a['n_banks']:,} institutions", "info")
    pk.kpi("Uninsured deposits", f"{a['uninsured_share']}%", "share of all deposits", "warning" if (a['uninsured_share'] or 0) > 40 else "info")
    pk.kpi("HTM unrealised loss", f"${a['htm_loss_bn']}bn", f"{a['htm_loss_to_equity']}% of equity", "neg" if (a['htm_loss_to_equity'] or 0) > 10 else "info")
    pk.kpi("CRE / Tier 1", f"{a['cre_to_tier1']}%", f"{a['n_cre_gt300_tier1']} banks above 300%", "info")
    pk.kpi("Banks HTM loss > 50% equity", str(a["n_htm_loss_gt50_equity"]), "SVB was 98% in 2022Q4", "warning" if a["n_htm_loss_gt50_equity"] else "pos")
    pk.note("Values are from the FDIC's quarterly Call Report extract (RIS). HTM unrealised loss = amortised cost (SCHA) minus fair value (SCHF). "
            "Uninsured deposits are the FDIC's estimate (DEPUNA). Totals are simple sums across all filers, so they are dominated by the largest banks.")
    pk.extra["latest_quarter"] = iso(latest)
    pk.extra["aggregates"] = {iso(q): v for q, v in per_q.items()}
    return pk


def lambda_handler(event, context=None):
    return RS.run(SLUG, build, event)
