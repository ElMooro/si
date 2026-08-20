"""Local ground-truth harness for justhodl-floor-audit (house-mandatory
before any engine push). Pure-function identities against hand-computed
fixtures shaped exactly like real SEC companyfacts. No network, no AWS
calls -- boto3 client construction only.

Covers the burned-lesson classes:
  - field-level binds (boom silent-join, ops 4817): custody crypto tags
    must NEVER bind; backlog join breakage must surface, not zero.
  - decomposition identity: asset_driven + residual == dd exactly.
  - NLAV identity: sum(legs) - debt == nlav to the cent.
  - verdict ladder incl. Khalid's BTBT case and the honest ASSET_DRIVEN
    (dump-makes-sense) branch.
  - G0 gate rejects hollow output.
Exit 1 on any failure.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "source"))
import lambda_function as L  # noqa: E402

TH = dict(L.DEFAULT_CONFIG["thresholds"])
FAIL = []


def check(name, cond, detail=""):
    tag = "PASS" if cond else "FAIL"
    print("  [%s] %s %s" % (tag, name, detail))
    if not cond:
        FAIL.append(name)


def facts_fixture(crypto_total=None, crypto_cur=None, crypto_non=None,
                  custody=None):
    def inst(tag, rows):
        return {tag: {"units": {"USD": [
            {"val": v, "end": e, "form": f, "filed": fd}
            for v, e, f, fd in rows]}}}
    gaap = {}
    gaap.update(inst("CashAndCashEquivalentsAtCarryingValue",
                     [(180e6, "2026-03-31", "10-Q", "2026-05-08"),
                      (150e6, "2025-12-31", "10-K", "2026-03-02")]))
    gaap.update(inst("ShortTermInvestments",
                     [(40e6, "2026-03-31", "10-Q", "2026-05-08")]))
    gaap.update(inst("AccountsReceivableNetCurrent",
                     [(20e6, "2026-03-31", "10-Q", "2026-05-08")]))
    gaap.update(inst("LongTermDebtNoncurrent",
                     [(60e6, "2026-03-31", "10-Q", "2026-05-08")]))
    gaap.update(inst("LongTermDebtCurrent",
                     [(10e6, "2026-03-31", "10-Q", "2026-05-08")]))
    gaap.update(inst("RevenueRemainingPerformanceObligation",
                     [(90e6, "2026-03-31", "10-Q", "2026-05-08")]))
    if crypto_total is not None:
        gaap.update(inst("CryptoAssetFairValue",
                         [(crypto_total, "2026-03-31", "10-Q",
                           "2026-05-08")]))
    if crypto_cur is not None:
        gaap.update(inst("CryptoAssetFairValueCurrent",
                         [(crypto_cur, "2026-03-31", "10-Q",
                           "2026-05-08")]))
    if crypto_non is not None:
        gaap.update(inst("CryptoAssetFairValueNoncurrent",
                         [(crypto_non, "2026-03-31", "10-Q",
                           "2026-05-08")]))
    if custody is not None:
        gaap.update(inst("CryptoAssetHeldForPlatformUserFairValue",
                         [(custody, "2026-03-31", "10-Q", "2026-05-08")]))
    dei = {"EntityCommonStockSharesOutstanding": {"units": {"shares": [
        {"val": 100e6, "end": "2026-05-01", "form": "10-Q",
         "filed": "2026-05-08"},
        {"val": 92e6, "end": "2026-02-01", "form": "10-K",
         "filed": "2026-03-02"},
    ]}}}
    return {"facts": {"us-gaap": gaap, "dei": dei}}


print("== 1. XBRL binds ==")
fx = facts_fixture(crypto_total=300e6)
v, p = L.xbrl_usd(fx, ["CashAndCashEquivalentsAtCarryingValue"])
check("cash latest-instant", v == 180e6 and p["end"] == "2026-03-31",
      "(%s @ %s)" % (v, p["end"]))
sh = L.shares_series(fx)
check("shares series sorted+latest", sh[-1] == ("2026-05-01", 100e6))

print("== 2. crypto ownership doctrine ==")
cv, cp = L.crypto_fv(facts_fixture(crypto_total=300e6, custody=5e9))
check("total tag binds, custody ignored",
      cv == 300e6 and "custody" in cp["doctrine"])
cv2, cp2 = L.crypto_fv(facts_fixture(crypto_cur=100e6, crypto_non=250e6))
check("current+noncurrent summed", cv2 == 350e6,
      "(%s)" % cp2["tag"])
cv3, _ = L.crypto_fv(facts_fixture(custody=5e9))
check("custody-ONLY filer -> None (never a floor)", cv3 is None)

print("== 3. NLAV identity ==")
st = L.floor_stack(fx, TH["ar_haircut"], 300e6,
                   {"tag": "CryptoAssetFairValue", "end": "2026-03-31"})
# hand math: 180 + 40 + 0 + 300 + 0.85*20 - (60+10) = 467
check("nlav to the cent", abs(st["nlav"] - 467e6) < 0.01,
      "(nlav=%.2f)" % st["nlav"])
legs_sum = sum(x["value"] for x in st["legs"] if x["value"] is not None)
check("legs signed-sum == nlav", abs(legs_sum - st["nlav"]) < 0.01)
check("AR haircut applied",
      any(x["name"] == "receivables" and x["value"] == 17e6
          for x in st["legs"]))

print("== 4. drawdowns + decomposition identity ==")
closes = [100.0] * 40 + [100 - i * 2.25 for i in range(1, 21)]  # -45% /20
dd = L.drawdowns(closes, [5, 20, 60])
check("dd20 == -45%", abs(dd["20"] - (-0.45)) < 1e-9, "(%s)" % dd["20"])
ad, res, expl = L.decompose(-0.45, 0.34, -0.20)
check("identity ad+res==dd", abs(ad + res - (-0.45)) < 1e-12,
      "(ad=%s res=%s)" % (ad, res))
check("BTBT math: 34%% ETH x -20%% ETH explains 6.8pts",
      abs(ad - (-0.068)) < 1e-9 and abs(res - (-0.382)) < 1e-9)
check("explained_frac ~15%%", abs(expl - 0.1511) < 1e-3, "(%s)" % expl)
adX, resX, explX = L.decompose(-0.20, 0.90, -0.35)
check("over-explained clamps to 1.0 (res goes positive)",
      explX == 1.0 and abs(resX - 0.115) < 1e-9, "(res=%s)" % resX)

print("== 5. verdict ladder ==")
dmap = {"5": -0.05, "20": -0.45, "60": -0.30, "120": -0.30}


def dcp(cov, aret):
    return {w: L.decompose(dmap[w], cov, aret) for w in dmap}


v1 = L.verdict(dmap, dcp(0.55, -0.20), 0.55, TH)
check("SENSELESS at cov .55 res -.34", v1["verdict"] ==
      "SENSELESS_DRAWDOWN" and v1["severity"] == "HIGH",
      "(%s sense=%s)" % (v1["verdict"], v1["sense_score"]))
v2 = L.verdict(dmap, dcp(0.34, -0.20), 0.34, TH)
check("STRETCHED at cov .34 (Khalid BTBT crypto-only case)",
      v2["verdict"] == "STRETCHED" and v2["severity"] == "MEDIUM")
v3 = L.verdict(dmap, dcp(1.12, -0.20), 1.12, TH)
check("BELOW_LIQUID_FLOOR -> CRITICAL", v3["verdict"] ==
      "BELOW_LIQUID_FLOOR" and v3["severity"] == "CRITICAL")
v4 = L.verdict({"5": -0.02, "20": -0.30, "60": -0.10, "120": -0.10},
               {"20": L.decompose(-0.30, 0.80, -0.35),
                "5": (None, None, None), "60": (0, -0.10, 0.0),
                "120": (0, -0.10, 0.0)}, 0.80, TH)
check("ASSET_DRIVEN: dump makes sense -> INFO, sense>=60",
      v4["verdict"] == "ASSET_DRIVEN" and v4["severity"] == "INFO" and
      v4["sense_score"] >= 60, "(sense=%s)" % v4["sense_score"])
v5 = L.verdict({"5": -0.01, "20": -0.05, "60": -0.05, "120": -0.05},
               {}, 0.40, TH)
check("quiet tape -> IN_LINE", v5["verdict"] == "IN_LINE")
v6 = L.verdict(dmap, dcp(0.55, -0.20), None, TH)
check("no floor data honest", v6["verdict"] == "NO_FLOOR_DATA")

print("== 6. G0 gate ==")
good_rec = {"status": "OK", "ticker": "BTBT", "mcap_usd": 1.0,
            "shares": 1, "last_close": 1,
            "floor": {"legs": [{"name": "cash"}]}, "coverage": 0.5,
            "drawdowns": {}, "decomposition": {},
            "verdict": {"severity": "NONE"}, "why_block": {}}
pl = {"engine": "justhodl-floor-audit", "version": L.VERSION,
      "as_of": "x", "alerts": [],
      "tickers": {t: dict(good_rec, ticker=t)
                  for t in ("A", "B", "C", "D")}}
check("G0 passes clean payload", L.g0_validate(pl) == 4)
bad = dict(pl)
bad["tickers"] = dict(pl["tickers"])
bad["tickers"]["A"] = dict(good_rec, mcap_usd=None)
try:
    L.g0_validate(bad)
    check("G0 rejects missing mcap", False)
except AssertionError as e:
    check("G0 rejects missing mcap", "mcap" in str(e), "(%s)" % e)
hollow = dict(pl)
hollow["tickers"] = {t: {"status": "ERROR"} for t in "ABCDE"}
try:
    L.g0_validate(hollow)
    check("G0 rejects <60% OK", False)
except AssertionError:
    check("G0 rejects <60% OK", True)

print("== 7. v1.0.1 quarantine ladder (integration slice) ==")
# fund wrapper: structural test on the audit-side classification inputs
cfgq = {"fund_blocklist": ["IBIT"], "thresholds": TH}
def classify(tk, coverage, crypto_cov, mcap, vd):
    is_fund = tk in set(cfgq.get("fund_blocklist") or []) or \
        (coverage is not None and 0.94 <= coverage <= 1.08 and
         crypto_cov >= 0.90)
    if is_fund:
        return "FUND_WRAPPER"
    if coverage is not None and (coverage > 10 or mcap < 3e6):
        return "SUSPECT_INPUTS"
    return vd
check("unknown ETF caught structurally (cov 1.00, crypto 99%)",
      classify("ZZZC", 1.003, 0.99, 5e9, "BELOW_LIQUID_FLOOR")
      == "FUND_WRAPPER")
check("blocklisted IBIT caught even off-band",
      classify("IBIT", 1.2, 0.99, 5e10, "BELOW_LIQUID_FLOOR")
      == "FUND_WRAPPER")
check("GLXY-class 3e6x coverage quarantined",
      classify("GLXY", 3040406.0, 118054.0, 2e3, "BELOW_LIQUID_FLOOR")
      == "SUSPECT_INPUTS")
check("real DAT discount 1.73x passes through (UPXI-class)",
      classify("UPXI", 1.73, 1.53, 61e6, "BELOW_LIQUID_FLOOR")
      == "BELOW_LIQUID_FLOOR")
check("real operator 6.6x still passes (<=10x band honored)",
      classify("AIFC", 6.6, 6.1, 73e6, "BELOW_LIQUID_FLOOR")
      == "BELOW_LIQUID_FLOOR")
# shares form filter: S-1 placeholder row must not win
fx2 = facts_fixture()
fx2["facts"]["dei"]["EntityCommonStockSharesOutstanding"]["units"][
    "shares"].append({"val": 100, "end": "2026-06-01", "form": "S-1",
                      "filed": "2026-06-02"})
sh2 = L.shares_series(fx2)
check("S-1 placeholder (100 sh) never wins the series",
      sh2[-1] == ("2026-05-01", 100e6), "(latest=%s)" % (sh2[-1],))

print("== 8. v1.0.2 binds ==")
# cross-namespace crypto (BTBT-class entity tag)
fx8 = facts_fixture()
fx8["facts"]["btbt"] = {"CryptoAssetEthereumFairValue": {"units": {
    "USD": [{"val": 250e6, "end": "2026-06-30", "form": "10-Q",
             "filed": "2026-08-10"}]}}}
cv8, cp8 = L.crypto_fv(fx8)
if cv8 is None:
    cv8, cp8 = L.crypto_fv_crossns(fx8)
check("cross-ns entity tag binds with cited ns:tag",
      cv8 == 250e6 and cp8["tag"] == "btbt:CryptoAssetEthereumFairValue",
      "(%s)" % (cp8 or {}).get("tag"))
fx8b = facts_fixture()
fx8b["facts"]["hood"] = {"CryptoAssetHeldForPlatformUserFairValue":
    {"units": {"USD": [{"val": 9e9, "end": "2026-06-30",
                        "form": "10-Q", "filed": "2026-08-10"}]}}}
cv8b, _ = L.crypto_fv_crossns(fx8b)
check("cross-ns scan still blocks custody patterns", cv8b is None)
fx8c = facts_fixture()
fx8c["facts"]["us-gaap"]["CashSegregatedUnderFederalAndOtherRegulations"] =     {"units": {"USD": [{"val": 5e9, "end": "2026-06-30",
                        "form": "10-Q", "filed": "2026-08-10"}]}}
check("broker markers detected (HOOD-class), cited ns:tag",
      L.broker_balance_sheet(fx8c) ==
      ["us-gaap:CashSegregatedUnderFederalAndOtherRegulations"])
# watchlist exemption from the structural wrapper test
wl = set(L.DEFAULT_CONFIG["watchlist"])
def classify2(tk, coverage, crypto_cov, mcap, broker, vd):
    in_watchlist = tk in wl
    is_fund = tk in set(L.DEFAULT_CONFIG["fund_blocklist"]) or \
        (not in_watchlist and coverage is not None and
         0.94 <= coverage <= 1.08 and crypto_cov >= 0.90)
    if broker and not in_watchlist:
        return "BROKER_BALANCE_SHEET"
    if is_fund:
        return "FUND_WRAPPER"
    if not in_watchlist and coverage is not None and \
            (coverage > 10 or mcap < 3e6):
        return "SUSPECT_INPUTS"
    return vd
check("BMNR (watchlist, cov .97, crypto .94) NOT a wrapper",
      classify2("BMNR", 0.9726, 0.9443, 2e10, [], "IN_LINE")
      == "IN_LINE")
check("unknown at same shape IS a wrapper",
      classify2("ZETF", 0.9726, 0.9443, 2e9, [], "IN_LINE")
      == "FUND_WRAPPER")
check("HOOD-class broker quarantined",
      classify2("HOOD", 1.62, 0.05, 9e10,
                ["CashSegregatedUnderFederalAndOtherRegulations"],
                "BELOW_LIQUID_FLOOR") == "BROKER_BALANCE_SHEET")
check("watchlist name with broker markers still audited",
      classify2("GLXY", 1.2, 0.8, 8e9, ["PayablesToCustomers"],
                "BELOW_LIQUID_FLOOR") == "BELOW_LIQUID_FLOOR")

print("== 9. v1.0.3 recency-first bind (ops-4916 BTBT failure) ==")


def inst_rows(tag, rows):
    return {tag: {"units": {"USD": [
        {"val": v, "end": e, "form": f, "filed": fd}
        for v, e, f, fd in rows]}}}


# exact BTBT tape from the ops-4916 tag inventory: stale $2.3M parent
# (2026-03-31) must NOT shadow fresh $120.1M+$120.1M splits (2026-06-30)
fx9 = facts_fixture()
fx9["facts"]["us-gaap"].update(inst_rows(
    "CryptoAssetFairValue",
    [(2296509, "2026-03-31", "10-Q", "2026-05-08")]))
fx9["facts"]["us-gaap"].update(inst_rows(
    "CryptoAssetFairValueCurrent",
    [(120149000, "2026-06-30", "10-Q", "2026-08-10")]))
fx9["facts"]["us-gaap"].update(inst_rows(
    "CryptoAssetFairValueNoncurrent",
    [(120149000, "2026-06-30", "10-Q", "2026-08-10")]))
cv9, cp9 = L.crypto_fv(fx9)
check("BTBT replica: fresh splits sum, stale parent superseded",
      cv9 == 240298000 and cp9["end"] == "2026-06-30",
      "(val=%s end=%s)" % (cv9, (cp9 or {}).get("end")))
check("BTBT replica: superseded parent cited in provenance",
      (cp9.get("superseded_parent") or {}).get("end") == "2026-03-31"
      and (cp9.get("superseded_parent") or {}).get("val") == 2296509)
check("BTBT replica: equal-split doctrine note present",
      "split_equal_note" in cp9)

# parent fresh at the same end as splits -> parent authoritative
fx9b = facts_fixture()
fx9b["facts"]["us-gaap"].update(inst_rows(
    "CryptoAssetFairValue",
    [(500e6, "2026-06-30", "10-Q", "2026-08-10")]))
fx9b["facts"]["us-gaap"].update(inst_rows(
    "CryptoAssetFairValueCurrent",
    [(100e6, "2026-03-31", "10-Q", "2026-05-08")]))
cv9b, cp9b = L.crypto_fv(fx9b)
check("fresh parent beats stale split",
      cv9b == 500e6 and cp9b["tag"] == "CryptoAssetFairValue")

# only ONE split is fresh -> stale sibling excluded from the sum
fx9c = facts_fixture()
fx9c["facts"]["us-gaap"].update(inst_rows(
    "CryptoAssetFairValueCurrent",
    [(200e6, "2026-06-30", "10-Q", "2026-08-10")]))
fx9c["facts"]["us-gaap"].update(inst_rows(
    "CryptoAssetFairValueNoncurrent",
    [(300e6, "2026-03-31", "10-Q", "2026-05-08")]))
cv9c, cp9c = L.crypto_fv(fx9c)
check("lone fresh split binds alone, stale sibling excluded",
      cv9c == 200e6 and cp9c["tag"] == "CryptoAssetFairValueCurrent")

# extension-namespace tag fresher than every us-gaap fact -> ext wins
fx9d = facts_fixture(crypto_total=100e6)  # parent @2026-03-31
fx9d["facts"]["btbt"] = inst_rows(
    "CryptoAssetEthereumFairValue",
    [(400e6, "2026-06-30", "10-Q", "2026-08-10")])
cv9d, cp9d = L.crypto_fv(fx9d)
check("fresher extension tag beats stale us-gaap parent",
      cv9d == 400e6
      and cp9d["tag"] == "btbt:CryptoAssetEthereumFairValue"
      and "recency-first" in cp9d["doctrine"])

print("== 10. v1.0.3 broker pattern scan (HOOD tag-miss fix) ==")
fx10 = facts_fixture()
fx10["facts"]["us-gaap"][
    "CashAndSecuritiesSegregatedUnderFederalAndOtherRegulations"] = \
    {"units": {"USD": [{"val": 4e9, "end": "2026-06-30",
                        "form": "10-Q", "filed": "2026-08-06"}]}}
check("HOOD real segregation tag caught by pattern",
      L.broker_balance_sheet(fx10) ==
      ["us-gaap:CashAndSecuritiesSegregatedUnderFederalAndOther"
       "Regulations"])
fx10b = facts_fixture()
fx10b["facts"]["hood"] = {"PayablesToUsers": {"units": {"USD": [
    {"val": 6e9, "end": "2026-06-30", "form": "10-Q",
     "filed": "2026-08-06"}]}}}
check("entity-namespace PayablesToUsers caught",
      L.broker_balance_sheet(fx10b) == ["hood:PayablesToUsers"])
check("clean DAT fixture has zero broker hits",
      L.broker_balance_sheet(facts_fixture(crypto_total=300e6)) == [])

print("== 9. v1.1 contract floor (backlog/orders leg) ==")
dd_none = {"5": -0.02, "20": -0.03, "60": -0.05, "120": -0.08}
dec_none = {w: (0.0, 0.0, None) for w in ("5", "20", "60", "120")}
dd_dump = {"5": -0.10, "20": -0.34, "60": -0.40, "120": -0.45}
dec_dump = {"5": (0.0, -0.10, 0.0), "20": (-0.02, -0.32, 0.06),
            "60": (-0.02, -0.38, 0.05), "120": (-0.03, -0.42, 0.07)}
v = L.verdict(dd_none, dec_none, 0.22, TH, 2.4)
check("no dump + committed 2.4x mcap -> BACKLOG_FLOOR/INFO",
      v["verdict"] == "BACKLOG_FLOOR" and v["severity"] == "INFO",
      "(%s/%s)" % (v["verdict"], v["severity"]))
v = L.verdict(dd_none, dec_none, 0.22, TH, 0.9)
check("no dump + committed 0.9x -> IN_LINE (below floor threshold)",
      v["verdict"] == "IN_LINE")
v = L.verdict(dd_dump, dec_dump, 0.18, TH, 2.0)
check("dump + committed 2.0x -> CONTRACT_BACKED_DUMP/MEDIUM",
      v["verdict"] == "CONTRACT_BACKED_DUMP" and
      v["severity"] == "MEDIUM", "(%s/%s)" % (v["verdict"],
                                              v["severity"]))
v = L.verdict(dd_dump, dec_dump, 0.18, TH, 4.1)
check("dump + committed 4.1x -> CONTRACT_BACKED_DUMP/HIGH",
      v["verdict"] == "CONTRACT_BACKED_DUMP" and
      v["severity"] == "HIGH", "(%s)" % v["severity"])
v = L.verdict(dd_dump, dec_dump, 1.4, TH, 4.1)
check("liquid floor still outranks the order book (hard assets win)",
      v["verdict"] == "BELOW_LIQUID_FLOOR")
v = L.verdict(dd_dump, dec_dump, 0.62, TH, 4.1)
check("crypto SENSELESS still outranks CONTRACT_BACKED_DUMP",
      v["verdict"] == "SENSELESS_DRAWDOWN")
v = L.verdict(dd_dump, dec_dump, 0.18, TH, None)
check("no backlog data -> no contract verdict invented",
      v["verdict"] not in ("BACKLOG_FLOOR", "CONTRACT_BACKED_DUMP"),
      "(%s)" % v["verdict"])
check("committed_coverage echoed on the verdict for fusion",
      L.verdict(dd_dump, dec_dump, 0.18, TH, 2.0)[
          "committed_coverage"] == 2.0)

print("== 10. v2.0 durability, quality, tiers ==")
check("cap tiers ladder", [L.cap_tier(x) for x in
      (3e11, 5e10, 5e9, 1e9, 1e8, 1e7)] ==
      ["mega", "large", "mid", "small", "micro", "nano"])
f_cash = {"legs": [{"name": "cash", "value": 100e6, "sign": 1},
                   {"name": "receivables", "value": 0, "sign": 1},
                   {"name": "debt", "value": -10e6, "sign": -1}],
          "gross_liquid_assets": 100e6, "debt_total": 10e6,
          "nlav": 90e6}
f_soft = {"legs": [{"name": "cash", "value": 10e6, "sign": 1},
                   {"name": "receivables", "value": 90e6, "sign": 1},
                   {"name": "debt", "value": 0, "sign": -1}],
          "gross_liquid_assets": 100e6, "debt_total": 0.0,
          "nlav": 100e6}
check("all-cash floor scores 100 quality", L.asset_quality(f_cash) == 100)
check("receivable-heavy floor scores far lower",
      L.asset_quality(f_soft) == 55, "(%s)" % L.asset_quality(f_soft))
rw, st = L.runway_months(f_cash, -60e6)
check("runway = cash-like / monthly burn (100M / 5M = 20 months)",
      rw == 20.0 and st == "burning", "(%s,%s)" % (rw, st))
rw2, st2 = L.runway_months(f_cash, +5e6)
check("profitable company gets a state, not a number",
      rw2 is None and st2 == "self_funding")
d_ok, _ = L.durability(None, "self_funding", 0.02, 10e6, 100e6)
d_melt, fl = L.durability(4.0, "burning", 0.0, 0.0, 100e6)
check("self-funding scores high", d_ok >= 85, "(%s)" % d_ok)
check("4-month runway scores near zero + says why",
      d_melt <= 10 and any("6 months" in x for x in fl), "(%s)" % d_melt)
d_dil, fl2 = L.durability(None, "self_funding", 0.60, 0.0, 100e6)
check("60% YoY dilution penalised even when self-funding",
      d_dil <= 60 and any("diluted" in x for x in fl2), "(%s)" % d_dil)
check("discount score monotonic in coverage",
      L.discount_score(0.3) < L.discount_score(1.0) <
      L.discount_score(1.8))
check("mispricing score scales with unexplained residual",
      L.mispricing_score(-0.45) > L.mispricing_score(-0.15) >
      L.mispricing_score(0.0) == 0)

print("== 11. v2.0 recommendation vetoes + ladder ==")
def R(**kw):
    ctx = {"verdict": "SENSELESS_DRAWDOWN", "coverage": 1.2,
           "crypto_coverage": 0.1, "committed_cov": None,
           "durability": 80, "durability_flags": [],
           "asset_quality": 95, "premium_to_nav": 0.83,
           "dilution_yoy": 0.02, "worst_residual": -0.35}
    ctx.update(kw)
    return L.recommend(ctx, TH)
check("full house -> BUY", R()["action"] == "BUY", "(%s)" % R()["action"])
check("melting floor VETOES a real discount",
      R(durability=15)["action"] == "AVOID")
check("veto is recorded, not hidden",
      "durability" in R(durability=15)["vetoes"])
check("heavy dilution vetoes too",
      R(dilution_yoy=0.60)["action"] == "AVOID")
check("premium to NAV on a treasury -> REDUCE (sell side)",
      R(coverage=0.4, crypto_coverage=0.35,
        premium_to_nav=2.5)["action"] == "REDUCE")
check("weak-but-clean setup -> WATCH or PASS, never BUY",
      R(coverage=0.35, worst_residual=-0.05,
        asset_quality=60)["action"] in ("WATCH", "PASS"))
check("structural quarantine -> NO_CALL with a plain reason",
      R(verdict="FUND_WRAPPER")["action"] == "NO_CALL" and
      "fund or trust" in R(verdict="FUND_WRAPPER")["plain"])
check("every call carries plain text, risks and an invalidation",
      all(R(**k).get("plain") and R(**k).get("invalidation")
          for k in ({}, {"durability": 15}, {"coverage": 0.2})))
check("crypto-heavy floor is flagged as a risk to the retail reader",
      any("crypto" in x for x in R(crypto_coverage=0.55)["risks"]))
check("conviction bounded 0-99",
      all(0 <= R(**k)["conviction"] <= 99
          for k in ({}, {"durability": 5}, {"coverage": 3.0})))

print("== 12. v2.0 market prescreen shape ==")
check("PRESCREEN_TAGS covers cash/investments/crypto/debt + IFRS",
      {t[0] for t in L.PRESCREEN_TAGS} >=
      {"cash", "cash_ifrs", "st_inv", "lt_inv", "crypto", "debt_nc"})
check("debt legs carry sign -1 (subtracted, never added)",
      all(t[3] == -1 for t in L.PRESCREEN_TAGS
          if t[0] in ("debt_nc", "debt_c", "st_borrow")))
check("sweep is config-gated and disabled cleanly",
      L.market_prescreen({"market_sweep": {"enabled": False}}, {},
                         set()) == ([], {}))

print("== 13. v2.0.1 frame density (ops 4921 trap) ==")
newest = {1: 10.0, 2: 20.0}          # current quarter: 2 early filers
prior = {1: 9.0, 3: 30.0, 4: 40.0}   # completed quarter: dense
m = L.merge_frames([newest, prior])
check("newest filing wins per company", m[1] == 10.0)
check("older frame fills the companies that have not filed yet",
      m[3] == 30.0 and m[4] == 40.0)
check("union is denser than either frame alone", len(m) == 4)
check("empty input is empty, never a fabricated row",
      L.merge_frames([]) == {})

print("== 14. v2.1 retail guards (debt bind, tradability, premium) ==")
def R2(**kw):
    ctx = {"verdict": "SENSELESS_DRAWDOWN", "coverage": 1.2,
           "crypto_coverage": 0.1, "committed_cov": None,
           "durability": 80, "durability_flags": [],
           "asset_quality": 95, "premium_to_nav": 0.83,
           "dilution_yoy": 0.02, "worst_residual": -0.35,
           "debt_bound": True, "adv_usd": 5e6}
    ctx.update(kw)
    return L.recommend(ctx, TH)
check("clean setup still BUYs", R2()["action"] == "BUY")
u = R2(debt_bound=False)
check("unbound debt blocks the BUY", u["action"] != "BUY",
      "(%s)" % u["action"])
check("and says the floor may be overstated",
      any("upper bound" in x for x in u["risks"]))
check("unbound debt is recorded as a veto", "debt_unbound" in u["vetoes"])
check("low-coverage names are unaffected by the debt gate",
      "debt_unbound" not in R2(debt_bound=False, coverage=0.3)["vetoes"])
tn = R2(adv_usd=80000.0)
check("a stock trading $80k/day is never a BUY",
      tn["action"] != "BUY", "(%s)" % tn["action"])
check("and the reader is told the actual daily volume",
      any("thin market" in x for x in tn["risks"]))
check("thin ACCUMULATE is demoted to WATCH",
      R2(coverage=0.7, composite_hint=None, adv_usd=50000.0,
         worst_residual=-0.25)["action"] in ("WATCH", "PASS"))
check("liquid names keep their call", R2(adv_usd=9e6)["action"] == "BUY")
dj = R2(coverage=0.07, crypto_coverage=0.30, premium_to_nav=14.3)
check("tiny floor + some coins is NOT a wrapper-premium REDUCE",
      dj["action"] != "REDUCE", "(%s)" % dj["action"])
tr = R2(coverage=0.45, crypto_coverage=0.40, premium_to_nav=2.4)
check("a real treasury premium still REDUCEs",
      tr["action"] == "REDUCE", "(%s)" % tr["action"])
check("missing ADV data never invents a liquidity problem",
      R2(adv_usd=None)["action"] == "BUY")

print()
if FAIL:
    print("HARNESS RED: %d failures: %s" % (len(FAIL), FAIL))
    sys.exit(1)
print("HARNESS GREEN: all identities hold against hand math.")
sys.exit(0)
