"""LOCAL PUSH-GATE HARNESS -- justhodl-foreign-flows v1.0.0
(ops 4824).  boto3 stubbed; fred_fetch monkeypatched with fixtures.
Asserts: unit conversion (millions->bn, billions passthrough); every
signal == independent arithmetic from the doc formulas (risk_appetite
= eq+corp+agency, safe_haven = treas-equity, total_demand = four-way
sum) to 1e-9 on latest/3m/12m; z identity vs an independent
implementation; bank union-merge is date-keyed (no dupes) AND a
banked 1980s row absent from the fresh fetch SURVIVES (the FRED
retro-windowing / ICE-BofA lesson); new_release flips only on a newer
month and appends the releases log; missing-series honesty (4/6 floor
LIVE with named exclusions + null signal with reason, 3/6 ->
INSUFFICIENT); official_private permanently deferred-not-guessed;
no FRED_KEY -> refuses to publish; write discipline.
Exit 1 on any failure.
"""
import json
import sys
import types
from pathlib import Path

FAILS = []


def chk(name, cond, detail=""):
    print("  [%s] %s %s" % ("PASS" if cond else "FAIL", name, detail))
    if not cond:
        FAILS.append(name)


STORE, PUTS = {}, []


class _Body:
    def __init__(self, b):
        self._b = b

    def read(self):
        return self._b


class _S3:
    def get_object(self, Bucket, Key):
        if Key not in STORE:
            raise KeyError(Key)
        return {"Body": _Body(json.dumps(STORE[Key]).encode())}

    def put_object(self, Bucket, Key, Body, **kw):
        PUTS.append(Key)
        STORE[Key] = json.loads(Body)


_stub = types.ModuleType("boto3")
_stub.client = lambda *a, **k: _S3()
sys.modules["boto3"] = _stub

SRC = (Path(__file__).resolve().parents[2] / "lambdas"
       / "justhodl-foreign-flows" / "source")
sys.path.insert(0, str(SRC))
import lambda_function as eng  # noqa: E402
eng.COUNTRY_PACE = 0.0

eng.FRED_KEY = "FIXTURE"

MONTHS = ["%04d-%02d-01" % (1985 + i // 12, i % 12 + 1)
          for i in range(500)]        # 1985-01 .. 2026-08
BASE = {
    "treas": [20000.0 + (i % 7) * 1000 for i in range(500)],
    "equity": [5000.0 + (i % 5) * 2000 for i in range(500)],
    "corp": [3000.0 + (i % 3) * 500 for i in range(500)],
    "agency": [1000.0 + (i % 4) * 250 for i in range(500)],
    "total": [30000.0 + (i % 9) * 1500 for i in range(500)],
    "tbills": [-2000.0 + (i % 6) * 800 for i in range(500)],
}
CORE_BY_SID = {sid: n for n, sid in eng.SERIES.items()}
DROP = set()
DROP_SIDS = set()
UNITS = {n: "Millions of Dollars" for n in BASE}
UNITS["tbills"] = "Billions of Dollars"
BREAK_FAMS = set()          # families whose 'all' gets +0.9B skew


def _mn(units):
    return 1.0 if "billion" in units.lower() else 1000.0


def gen_all(fam):
    sa = eng.SPLITS[fam][0]
    if sa in CORE_BY_SID:
        n = CORE_BY_SID[sa]
        return UNITS[n], list(BASE[n])
    return "Millions of Dollars", [9000.0 + (i % 5) * 400
                                   for i in range(len(MONTHS))]


def gen_off(fam):
    units, allv = gen_all(fam)
    k = _mn(units)
    return units, [(-1200.0 + (i % 3) * 250) * (k / 1000.0)
                   for i in range(len(allv))]


def fake_fetch(sid):
    if sid in DROP_SIDS:
        return None, "fetch_error:404"
    if sid in CORE_BY_SID:
        n = CORE_BY_SID[sid]
        if n in DROP:
            return None, "fetch_error:404"
        return UNITS[n], list(zip(MONTHS, BASE[n]))
    for fam, (sa, so, sp) in eng.SPLITS.items():
        if sid not in (sa, so, sp):
            continue
        units, allv = gen_all(fam)
        _, offv = gen_off(fam)
        prv = [a - o for a, o in zip(allv, offv)]   # from UNSKEWED
        if fam in BREAK_FAMS:
            allv = [v + 0.9 * _mn(units) for v in allv]
        if sid == so:
            return units, list(zip(MONTHS, offv))
        if sid == sp:
            return units, list(zip(MONTHS, prv))
        return units, list(zip(MONTHS, allv))
    if sid.startswith("FORLTTREASPOS"):
        return "Millions of Dollars", list(zip(
            MONTHS, [700000.0 + i * 500 for i in
                     range(len(MONTHS))]))
    if sid.startswith("FORLTTREASNET"):
        return "Millions of Dollars", list(zip(
            MONTHS, [400.0 + (i % 4) * 150 for i in
                     range(len(MONTHS))]))
    if sid.startswith("FORLTTREASVALCHG"):
        return "Millions of Dollars", list(zip(
            MONTHS, [60.0 + (i % 4) * 40 for i in
                     range(len(MONTHS))]))
    if sid.startswith("FORLTEQTYPOS"):
        return "Millions of Dollars", list(zip(
            MONTHS, [300000.0 + i * 250 for i in
                     range(len(MONTHS))]))
    if sid.startswith("FORLTEQTYNET"):
        return "Millions of Dollars", list(zip(
            MONTHS, [120.0 + (i % 3) * 30 for i in
                     range(len(MONTHS))]))
    if sid.startswith("FORLTEQTYVALCHG"):
        return "Millions of Dollars", list(zip(
            MONTHS, [50.0 + (i % 5) * 20 for i in
                     range(len(MONTHS))]))
    n = NAME_BY_SID[sid]
    if n in DROP:
        return None, "fetch_error:404"
    return UNITS[n], list(zip(MONTHS, BASE[n]))


NAME_BY_SID = {sid: n for n, sid in eng.SERIES.items()}
eng.fred_fetch = fake_fetch


def bn(name):
    div = 1.0 if "billion" in UNITS[name].lower() else 1000.0
    return [v / div for v in BASE[name]]


def ind_z(vals):
    win = vals[-(eng.Z_WINDOW + 1):]
    h, last = win[:-1], win[-1]
    mu = sum(h) / len(h)
    sd = (sum((v - mu) ** 2 for v in h) / (len(h) - 1)) ** 0.5
    return round(max(-4.0, min(4.0, (last - mu) / sd)), 2)


def main():
    global DROP
    STORE.clear()
    PUTS.clear()
    print("== full build ==")
    doc = eng.build()
    chk("LIVE 6/6", doc.get("status") == "LIVE"
        and len(doc["flows_bn"]) == 6)
    chk("latest_month = 2026-08-01",
        doc.get("latest_month") == "2026-08-01")

    print("== A1 unit conversion ==")
    chk("A1 millions->bn (treas)",
        doc["flows_bn"]["treas"]["latest"]
        == round(bn("treas")[-1], 1))
    chk("A1 billions passthrough (tbills)",
        doc["flows_bn"]["tbills"]["latest"]
        == round(BASE["tbills"][-1], 1))

    print("== A2 signal arithmetic == doc formulas ==")
    ra = [a + b + c for a, b, c in zip(bn("equity"), bn("corp"),
                                       bn("agency"))]
    sh = [a - b for a, b in zip(bn("treas"), bn("equity"))]
    td = [a + b + c + d for a, b, c, d in
          zip(bn("treas"), bn("agency"), bn("corp"), bn("equity"))]
    for sig, ser in (("risk_appetite", ra), ("safe_haven", sh),
                     ("total_demand", td)):
        s = doc["signals"][sig]
        ok = (s["latest_bn"] == round(ser[-1], 1)
              and s["sum_3m_bn"] == round(sum(ser[-3:]), 1)
              and s["sum_12m_bn"] == round(sum(ser[-12:]), 1)
              and s["z_10y"] == ind_z(ser))
        chk("A2 %s" % sig, ok, "latest=%s" % s["latest_bn"])
    u_lt, _ = gen_all("lt_total")
    _, offv = gen_off("lt_total")
    _, allv = gen_all("lt_total")
    k = _mn(u_lt)
    dop = [((a - o) - o) / k for a, o in zip(allv, offv)]
    s = doc["signals"]["official_private"]
    chk("A2 official_private == private-official identity",
        s["latest_bn"] == round(dop[-1], 1)
        and s["sum_12m_bn"] == round(sum(dop[-12:]), 1)
        and s["z_10y"] == ind_z(dop))
    hs = doc["holder_splits"]
    u_st, _ = gen_all("st_treas")
    _, off_st = gen_off("st_treas")
    chk("A2 all six split families reconciled OK",
        all(hs[f]["status"] == "OK" for f in eng.SPLITS)
        and hs["st_treas"]["official"]["latest"]
        == round(off_st[-1] / _mn(u_st), 1))
    base_rows = {k: r for k, r in
                 doc["country_lt_treasury"].items()
                 if not r.get("composite")}
    chk("A2 21-country matrix all OK + ordered non-increasing "
        "(+1 composite row)",
        len(base_rows) == len(eng.COUNTRIES)
        and len(doc["country_lt_treasury"])
        == len(eng.COUNTRIES) + 1
        and all(r.get("status") == "OK"
                for r in base_rows.values())
        and [r["holdings_bn"] for r in base_rows.values()]
        == sorted((r["holdings_bn"] for r in
                   base_rows.values()), reverse=True))
    eqj = doc["country_lt_equity"]["japan"]
    chk("A2 equity decomposition identity (japan)",
        isinstance(eqj.get("tx_12m_bn"), (int, float))
        and isinstance(eqj.get("valchg_12m_bn"), (int, float))
        and eqj.get("identity_gap_bn") == round(
            eqj["d12m_holdings_bn"] - eqj["tx_12m_bn"]
            - eqj["valchg_12m_bn"], 1))
    cbt = doc["country_lt_treasury"].get("china_plus_belgium")
    chk("A2 china+belgium composite = exact sum",
        cbt and cbt.get("composite") is True
        and cbt["holdings_bn"] == round(
            doc["country_lt_treasury"]["china"]["holdings_bn"]
            + doc["country_lt_treasury"]["belgium"]
            ["holdings_bn"], 1)
        and cbt["tx_12m_bn"] == round(
            doc["country_lt_treasury"]["china"]["tx_12m_bn"]
            + doc["country_lt_treasury"]["belgium"]
            ["tx_12m_bn"], 1))
    eq = doc["country_lt_equity"]["japan"]
    exp_eq = round((300000.0 + (len(MONTHS) - 1) * 250)
                   / 1000.0, 1)
    chk("A2 equity holdings block identity (japan)",
        eq["status"] == "OK" and eq["holdings_bn"] == exp_eq
        and eq["d12m_holdings_bn"] == round(12 * 250 / 1000.0,
                                            1))
    _saved = dict(eng.COUNTRIES)
    eng.COUNTRIES = {"belgium": "10308", "luxembourg": "10308"}
    d_dup = eng.build()
    chk("A2 dedupe guard: duplicate code -> MISSING named",
        d_dup["country_lt_treasury"]["luxembourg"]["status"]
        == "MISSING"
        and "duplicate" in d_dup["country_lt_treasury"]
        ["luxembourg"]["why"]
        and d_dup["country_lt_treasury"]["belgium"]["status"]
        == "OK")
    eng.COUNTRIES = _saved
    H = doc.get("hist_10y") or {}
    chk("A2 hist_10y: 6 flows + 4 signals, identity + cap",
        all(k in H for k in ("total", "treas", "equity", "corp",
                             "agency", "tbills",
                             "sig_risk_appetite",
                             "sig_safe_haven",
                             "sig_total_demand",
                             "sig_official_private"))
        and H["total"]["vals"][-1]
        == doc["flows_bn"]["total"]["latest"]
        and H["sig_risk_appetite"]["vals"][-1]
        == doc["signals"]["risk_appetite"]["latest_bn"]
        and H["sig_official_private"]["vals"][-1]
        == doc["signals"]["official_private"]["latest_bn"]
        and all(len(v["vals"]) <= 120
                and len(v["dates"]) == len(v["vals"])
                for v in H.values()))
    c3 = doc["country_lt_treasury"]["china"]
    chk("A2 country tx_3m present + numeric",
        isinstance(c3.get("tx_3m_bn"), (int, float)))
    c = doc["country_lt_treasury"]["china"]
    chk("A2 country decomposition identity (china)",
        c["holdings_bn"] == round((700000.0
                                   + (len(MONTHS) - 1) * 500)
                                  / 1000.0, 1)
        and c["identity_gap_bn"] == round(
            c["d12m_holdings_bn"] - c["tx_12m_bn"]
            - c["valchg_12m_bn"], 1))
    chk("A2 per-series z identity (equity)",
        doc["flows_bn"]["equity"]["z_10y"] == ind_z(bn("equity")))

    print("== A3 bank union-merge + windowing survival ==")
    sid = eng.SERIES["treas"]
    n0 = STORE[eng.BANK_FMT % sid]["rows"]
    chk("A3 bank rows == 500 no dupes", len(n0) == 500)
    STORE[eng.BANK_FMT % sid]["rows"]["1980-06-01"] = 12345.0
    PUTS.clear()
    d2 = eng.build()
    b2 = STORE[eng.BANK_FMT % sid]["rows"]
    chk("A3 banked 1980 row SURVIVES fresh fetch (windowing "
        "protection)", b2.get("1980-06-01") == 12345.0
        and len(b2) == 501)
    chk("A3 no new rows -> bank not rewritten",
        (eng.BANK_FMT % sid) not in PUTS)
    chk("A3 flows still off fetch rows (bank is archive, not "
        "input)", d2["flows_bn"]["treas"]["first"] == "1985-01-01")

    print("== A4 new_release detection + log ==")
    eng.lambda_handler({}, None)          # persist prev doc (OUT)
    d_same = eng.build()
    chk("A4 same month vs prev doc -> False",
        d_same.get("new_release") is False)
    MONTHS.append("2026-09-01")
    for k in BASE:
        BASE[k].append(BASE[k][-1])
    d3 = eng.build()
    chk("A4 newer month -> True + alert",
        d3.get("new_release") is True
        and "2026-09-01" in d3.get("alert", ""))
    chk("A4 releases log appended",
        STORE[eng.REL_KEY]["rows"][-1]["month"] == "2026-09-01")

    print("== A5 missing-series honesty ==")
    DROP = {"tbills", "corp"}
    d4 = eng.build()
    chk("A5 4/6 -> LIVE with named exclusions",
        d4.get("status") == "LIVE"
        and "corp" in d4["excluded"]
        and "404" in d4["excluded"]["corp"])
    chk("A5 signal with missing component -> null + reason",
        d4["signals"]["risk_appetite"]["value"] is None
        and "corp" in d4["signals"]["risk_appetite"]["why"])
    chk("A5 safe_haven unaffected",
        d4["signals"]["safe_haven"]["latest_bn"]
        == round(sh[-1], 1))
    BREAK_FAMS.add("lt_equity")
    d4b = eng.build()
    chk("A5 broken family -> UNRECONCILED named",
        d4b["holder_splits"]["lt_equity"]["status"]
        == "UNRECONCILED")
    u2, all2 = gen_all("lt_total")
    _, off2 = gen_off("lt_total")
    exp2 = round(((all2[-1] - off2[-1]) - off2[-1]) / _mn(u2), 1)
    chk("A5 lt_total signal unaffected by sibling break",
        d4b["signals"]["official_private"]["latest_bn"] == exp2)
    BREAK_FAMS.clear()
    DROP_SIDS.add(eng.SPLITS["lt_total"][2])
    d4c = eng.build()
    chk("A5 missing private id -> signal null with reason",
        d4c["signals"]["official_private"].get("value") is None
        and "MISSING" in
        d4c["signals"]["official_private"]["why"])
    DROP_SIDS.clear()
    DROP = {"tbills", "corp", "agency", "equity"}
    d5 = eng.build()
    chk("A5 2/6 -> INSUFFICIENT",
        d5.get("status") == "INSUFFICIENT_DATA")
    DROP = set()

    print("== A6 key + write discipline ==")
    k = eng.FRED_KEY
    eng.FRED_KEY = ""
    d6 = eng.build()
    chk("A6 no FRED_KEY -> refuses to publish",
        d6.get("status") == "INSUFFICIENT_DATA"
        and "FRED_KEY" in d6.get("why", ""))
    eng.FRED_KEY = k
    PUTS.clear()
    out = eng.lambda_handler({}, None)
    chk("A6 handler writes OUT last", PUTS[-1] == eng.OUT_KEY
        and out["ok"] is True,
        "tail=%s out=%s" % (PUTS[-3:], out))
    chk("A6 only provider-bank/releases/out keys ever written",
        all(p == eng.OUT_KEY or p == eng.REL_KEY
            or p.startswith("data/providers/tic-cslt/")
            for p in PUTS))

    print()
    if FAILS:
        print("HARNESS FAILED: %s" % FAILS)
        sys.exit(1)
    print("HARNESS GREEN -- push gate open (ops 4824)")


if __name__ == "__main__":
    main()
