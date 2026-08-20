"""Local push gate for justhodl-usd-funding. No network, no AWS.

Fixtures are shaped from the ACTUAL payloads observed in ops 4927/4928
(NY Fed 9-field refRates, BIS 25-column LBS CSV, FRED observations
JSON), because a harness built against an imagined shape proves
nothing. Run: python3 harness.py
"""
import json
import sys
import types
from datetime import date, timedelta

FAILS = []
CHECKS = [0]


def check(cond, label):
    CHECKS[0] += 1
    if not cond:
        FAILS.append(label)
        print("  ✗ %s" % label)
    else:
        print("  ✓ %s" % label)


# ───────────────────────── stub boto3 ────────────────────────────────
PUTS = {}


class _S3:
    def put_object(self, Bucket=None, Key=None, Body=None, **kw):
        PUTS[Key] = Body
        return {}


boto3_stub = types.ModuleType("boto3")
boto3_stub.client = lambda *a, **k: _S3()
sys.modules["boto3"] = boto3_stub

sys.path.insert(0, "source")
import lambda_function as ENG  # noqa: E402

ENG.FRED_KEY = "TEST"


# ───────────────────────── fixtures ──────────────────────────────────
def _days(n, step=1):
    d0 = date(2026, 8, 18)
    return [(d0 - timedelta(days=i * step)).isoformat()
            for i in range(n)][::-1]


NY_FIELDS = ["effectiveDate", "type", "percentRate", "percentPercentile1",
             "percentPercentile25", "percentPercentile75",
             "percentPercentile99", "volumeInBillions", "revisionIndicator"]

BIS_HDR = ("FREQ,L_MEASURE,L_POSITION,L_INSTR,L_DENOM,L_CURR_TYPE,"
           "L_PARENT_CTY,L_REP_BANK_TYPE,L_REP_CTY,L_CP_SECTOR,"
           "L_CP_COUNTRY,L_POS_TYPE,DECIMALS,UNIT_MEASURE,UNIT_MULT,"
           "AVAILABILITY,TITLE_GRP,TIME_FORMAT,COLLECTION,ORG_VISIBILITY,"
           "TIME_PERIOD,OBS_VALUE,OBS_STATUS,OBS_CONF,OBS_PRE_BREAK")


def bis_row(pos, rep, cp_ctry, sector, period, val, denom="USD"):
    return ("Q,S,%s,A,%s,F,5J,A,%s,%s,%s,R,3,USD,6,K,,,S,E,%s,%s,A,F,"
            % (pos, denom, rep, sector, cp_ctry, period, val))


STATE = {"mode": "happy"}


def fake_get(url, timeout=45, cap=None, headers=None):
    if "markets.newyorkfed.org" in url:
        rid = url.split("/")[-3]
        base = {"sofr": 3.65, "tgcr": 3.63, "bgcr": 3.63,
                "effr": 3.63, "obfr": 3.63}[rid]
        vol = {"sofr": 3010, "tgcr": 1203, "bgcr": 1234,
               "effr": 89, "obfr": 214}[rid]
        rows = []
        for i, d in enumerate(_days(400)):
            drift = 0.02 if i > 380 else 0.0
            rows.append({"effectiveDate": d, "type": rid.upper(),
                         "percentRate": round(base + drift, 4),
                         "percentPercentile1": round(base - 0.05, 4),
                         "percentPercentile25": round(base - 0.02, 4),
                         "percentPercentile75": round(base + 0.03, 4),
                         "percentPercentile99": round(base + 0.07, 4),
                         "volumeInBillions": vol,
                         "revisionIndicator": ""})
        return json.dumps({"refRates": rows})

    if "stlouisfed.org" in url:
        sid = url.split("series_id=")[1].split("&")[0]
        lvl = {"SOFR": 3.65, "IORB": 3.60}.get(sid, 1.0)
        if sid.startswith("RIFSPP"):
            lvl = 3.75 if "A2P2" in sid else 3.68
        if sid in ("COMPOUT", "FINCP", "NFINCP", "ABCOMP"):
            lvl = {"COMPOUT": 1180.0, "FINCP": 420.0,
                   "NFINCP": 310.0, "ABCOMP": 290.0}[sid]
        if sid.startswith("SOFR") and "DAY" in sid:
            lvl = 3.66
        if sid in ("LTDACBW027SBOG", "DPSACBW027SBOG",
                   "ODSACBW027SBOG", "NDFACBW027SBOG"):
            lvl = {"LTDACBW027SBOG": 1420.0, "DPSACBW027SBOG": 17800.0,
                   "ODSACBW027SBOG": 16380.0,
                   "NDFACBW027SBOG": -240.0}[sid]
        if sid == "H8B3094NCBA":
            lvl = 1950000.0
        obs = [{"date": d, "value": "%.4f" % (lvl + (i % 7) * 0.001)}
               for i, d in enumerate(_days(400))]
        obs.append({"date": "2026-08-19", "value": "."})  # FRED "." gap
        return json.dumps({"observations": obs})

    if "stats.bis.org" in url:
        if STATE["mode"] == "bis_no_aggregate":
            body = [BIS_HDR,
                    bis_row("C", "GB", "5J", "A", "2026-Q1", "1000")]
            return "\n".join(body)
        pos = "C" if "Q.S.C." in url or "Q..C." in url else "L"
        rows = [BIS_HDR]
        for i, per in enumerate(("2024-Q1", "2024-Q2", "2024-Q3",
                                 "2024-Q4", "2025-Q1")):
            v = (18_000_000 if pos == "C" else 16_000_000) + i * 100_000
            rows.append(bis_row(pos, "5A", "5J", "A", per, str(v)))
            # decoys that MUST NOT be summed in
            rows.append(bis_row(pos, "GB", "5J", "A", per, "900000"))
            rows.append(bis_row(pos, "5A", "US", "A", per, "800000"))
            rows.append(bis_row(pos, "5A", "5J", "B", per, "700000"))
            rows.append(bis_row(pos, "5A", "5J", "A", per, "999999",
                                denom="EUR"))
        return "\n".join(rows)
    raise RuntimeError("unexpected url %s" % url)


ENG._get = fake_get

# ───────────────────────── run ───────────────────────────────────────
print("\n=== 1. happy path ===")
p = ENG.build()

check(p["version"] == "1.0.0", "version stamped")
check(p["status"] in ("GREEN", "PARTIAL"), "status=%s" % p["status"])

print("\n--- gap 1+2: TGCR/BGCR present with volume + percentile fan ---")
rr = p["reference_rates"]
for rid in ("sofr", "tgcr", "bgcr", "effr", "obfr"):
    check(rr.get(rid, {}).get("ok"), "%s collected" % rid)
check(abs(rr["tgcr"]["rate"] - 3.65) < 1e-6,
      "TGCR rate bound to latest obs (3.63 base + 0.02 fixture drift)")
check(rr["tgcr"]["volume_bn"] == 1203, "TGCR volume carried (FRED has none)")
check(rr["bgcr"]["volume_bn"] == 1234, "BGCR volume carried")
check(rr["sofr"]["fan_bp"] is not None, "percentile fan p99-p1 computed")
check(rr["sofr"]["iqr_bp"] is not None, "percentile IQR p75-p25 computed")
check("_hist" not in rr["sofr"], "internal _hist stripped from payload")

print("\n--- gap 1: spreads vs IORB incl the two missing legs ---")
sp = p["spreads_vs_iorb"]
for rid in ("sofr", "tgcr", "bgcr", "obfr", "effr"):
    check(rid in sp, "%s-IORB spread present" % rid)
check(sp["tgcr"]["z"] is not None, "TGCR-IORB z computed")
check(sp["tgcr"]["n_hist"] >= 60, "TGCR z has >=60 aligned obs")

print("\n--- gap 3: H.8 wholesale funding ---")
h8 = p["bank_wholesale_funding"]
for k in ("large_time_deposits", "borrowings", "deposits",
          "other_deposits", "net_due_foreign"):
    check(k in h8 and h8[k].get("value") is not None, "H.8 %s" % k)
check(h8["large_time_deposits"]["series_id"] == "LTDACBW027SBOG",
      "large time deposits bound to proven id")
check(h8["borrowings"]["series_id"] == "H8B3094NCBA",
      "borrowings bound to proven id")

print("\n--- gap 4: CP quantities + tenor grid ---")
cp = p["commercial_paper"]
for k in ("cp_total", "cp_financial", "cp_nonfinancial", "cp_abcp"):
    check(cp["outstandings"][k].get("value") is not None, "CP out %s" % k)
check(cp.get("abcp_share_pct") is not None, "ABCP share computed")
check(len(cp["rates"]) == 13, "13 CP rate cells (4 tiers x tenors)")
check(all("a2p2" in k or "aa_" in k or "abcp" in k for k in cp["rates"]),
      "tiers cover AA fin / AA nonfin / ABCP / A2-P2")
check(cp.get("quality_spread_30d_bp") is not None,
      "A2/P2 - AA quality spread computed")
check(cp["rates"]["aa_fin_90d"]["spread_to_sofr_bp"] is not None,
      "CP-SOFR spread computed")

print("\n--- gap 5: SOFR term structure ---")
t = p["sofr_term_structure"]
for k in ("sofr_30d_avg", "sofr_90d_avg", "sofr_180d_avg", "sofr_index"):
    check(t[k].get("value") is not None, "term %s" % k)
check("term_slope_bp" in t, "term slope computed")
check("BACKWARD-looking" in t.get("term_slope_note", ""),
      "term slope honestly labelled backward-looking")

print("\n--- gaps 6+7: honest not-entitled rows ---")
ne = {x["id"]: x for x in p["not_entitled"]}
check("sofr_futures" in ne and "xccy_basis" in ne, "both gaps declared")
check(all(("source" in v and "upgrade" in v and "why" in v)
          for v in ne.values()), "each gap carries source+why+upgrade")
check(all("value" not in v for v in ne.values()),
      "not-entitled rows carry NO fabricated value")

print("\n--- BIS LBS USD: most-aggregate only, never summed ---")
b = p["bis_lbs_usd"]
check(b["ok"], "BIS answered")
check(b["positions"]["claims"]["latest_usd_bn"] == 18400.0,
      "claims = ONLY the 5A/5J/A USD row (decoys excluded), got %s"
      % b["positions"]["claims"]["latest_usd_bn"])
check(b["positions"]["liabilities"]["latest_usd_bn"] == 16400.0,
      "liabilities likewise, got %s"
      % b["positions"]["liabilities"]["latest_usd_bn"])
check(b.get("net_usd_bn") == 2000.0, "net USD gap = claims - liabilities")
check(b.get("structural") is True, "BIS flagged structural")
check("quarterly" in b.get("cadence", ""), "BIS cadence disclosed")
check(b.get("key_used"), "the key that answered is recorded")

print("\n--- z-score composite ---")
z = p["stress_z"]
check(z["sum_z"] is not None, "sum_z computed")
check(len(z["legs"]) >= 4, "%d legs entered" % len(z["legs"]))
check(any("TGCR" in x["leg"] for x in z["legs"]), "TGCR leg in composite")
check(any("BGCR" in x["leg"] for x in z["legs"]), "BGCR leg in composite")
check(z["reading"] in ("CALM", "NORMAL", "TIGHTENING", "STRESSED",
                       "ACUTE"), "reading=%s" % z["reading"])
check("EXCLUDED" in z["structural_note"],
      "quarterly BIS explicitly excluded from live composite")

print("\n--- FRED '.' missing-value rows must not become 0.0 ---")
s = ENG.fred("SOFR")
check(all(isinstance(v, float) for _, v in s), "no None leaked")
check(not any(v == 0.0 for _, v in s), "'.' rows dropped, not zeroed")

print("\n=== 2. BIS returns no aggregate row (honest failure) ===")
STATE["mode"] = "bis_no_aggregate"
p2 = ENG.build()
check(not p2["bis_lbs_usd"]["ok"], "BIS reports NOT ok")
check(p2["bis_lbs_usd"]["positions"] == {}, "no fabricated aggregate")
check(any("bis" in e for e in p2["errors"]), "BIS failure surfaced in errors")
check("not found" in json.dumps(p2["bis_lbs_usd"]["attempts"]),
      "attempt log states aggregate not found")
STATE["mode"] = "happy"

print("\n=== 3. NY Fed down — TGCR/BGCR must fail loud, not silently ===")
_orig = ENG._get


def ny_down(url, **kw):
    if "newyorkfed" in url:
        raise RuntimeError("503")
    return fake_get(url, **kw)


ENG._get = ny_down
p3 = ENG.build()
check(not p3["reference_rates"]["tgcr"]["ok"], "TGCR marked not-ok")
check("error" in p3["reference_rates"]["tgcr"], "error string carried")
check(any("tgcr" in e for e in p3["errors"]), "surfaced in errors[]")
check(not any("TGCR" in x["leg"] for x in p3["stress_z"]["legs"]),
      "dead TGCR leg EXCLUDED from composite (not zero-filled)")
check("TGCR-IORB" in p3["stress_z"]["legs_missing"],
      "missing leg named explicitly")
ENG._get = _orig

print("\n=== 4. handler writes both live + history keys ===")
ENG.lambda_handler({}, None)
check("data/usd-funding.json" in PUTS, "live key written")
check(any(k.startswith("data/usd-funding/history/") for k in PUTS),
      "history key written")
body = json.loads(PUTS["data/usd-funding.json"])
check(body["engine"] == "justhodl-usd-funding", "engine stamped")

print("\n=== 5. consumer contract (data.html reads these) ===")
for k in ("reference_rates", "spreads_vs_iorb", "bank_wholesale_funding",
          "commercial_paper", "sofr_term_structure", "bis_lbs_usd",
          "not_entitled", "stress_z", "status", "generated", "errors"):
    check(k in body, "top-level key %s" % k)

print("\n" + "=" * 58)
if FAILS:
    print("HARNESS RED — %d/%d failed:" % (len(FAILS), CHECKS[0]))
    for f in FAILS:
        print("   - %s" % f)
    sys.exit(1)
print("HARNESS GREEN — %d/%d checks passed" % (CHECKS[0], CHECKS[0]))
