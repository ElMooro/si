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

print()
if FAIL:
    print("HARNESS RED: %d failures: %s" % (len(FAIL), FAIL))
    sys.exit(1)
print("HARNESS GREEN: all identities hold against hand math.")
sys.exit(0)
