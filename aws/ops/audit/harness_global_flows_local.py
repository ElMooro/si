"""LOCAL PUSH-GATE HARNESS -- justhodl-global-flows v1.1.0
(ops 4839).  v1.1 adds Taiwan: CBC label-locate (+decoy-resistant
debt rule, width guard, "-" nulls, +1 offset) and TWSE hot-money
ledger (foreign-row sum, accrual honesty, backfill event).  bcrp_fetch seam stubbed.  Asserts: string->float parse
incl 'n.d.' nulls; per-series latest/sum_4q/z == independent
recompute; bank union-merge survival of a pre-2012 row; THIN when
<3 series usable; INSUFFICIENT on fetch death; deferred block ships
verbatim with unlock reasons; composites honestly null; write
discipline.  Exit 1 on any failure.
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
       / "justhodl-global-flows" / "source")
sys.path.insert(0, str(SRC))
import lambda_function as eng  # noqa: E402

MOF_HDR = (",,,,,,,,,,,,,,,,,,,\u5358\u4f4d\uff1a \u5104\u5186,,,\n"
           '"\u671f\u9593\nPeriod",x,,,,,,,,,,,,,,,,,,,,,\n')


def mof_row(period, base):
    c = [""] * 23
    c[0] = period
    c[3] = str(base + 10)        # out_eq (oku)
    c[6] = str(base + 20)        # out_lt
    c[10] = "\u25b3" + str(base)  # out_st negative
    c[11] = str(base + 30)
    c[14] = str(base + 40)       # in_eq
    c[17] = str(base + 50)       # in_lt
    c[21] = str(base)
    c[22] = str(base + 60)
    return ",".join(c) + "\n"


MOFN = {"n": 30}


def fake_mof():
    from datetime import date, timedelta
    txt = MOF_HDR
    d0 = date(2024, 1, 7)
    for i in range(MOFN["n"]):
        e = d0 + timedelta(days=7 * i)
        s = e - timedelta(days=6)
        txt += mof_row("%d\uff0e%d\uff0e%d\uff5e%d\uff0e%d"
                       % (s.year, s.month, s.day,
                          e.month, e.day), 100 + i * 10)
    txt += "(Note 3),x,,,,,,,,,,,,,,,,,,,,,\n"
    return txt, None


eng.mof_fetch = fake_mof


def seed_tic_jp(n=30, sign_seq=None):
    rows = {}
    for i in range(n):
        y, m = 2024 + (i // 12), 1 + (i % 12)
        v = (1000.0 if (sign_seq or [True] * n)[i]
             else -1000.0) * (1 + i * 0.01)
        rows["%04d-%02d-01" % (y, m)] = v
    STORE[eng.TIC_JP_BANK] = {"rows": rows}

QS = ["T%d.%02d" % (q, y) for y in range(12, 27)
      for q in range(1, 5)][:57]
VALS = {
    "portfolio_total": [100.0 + 7 * (i % 5) for i in range(57)],
    "portfolio_equity": [20.0 + 3 * (i % 4) for i in range(57)],
    "portfolio_fixed_income": [80.0 + 5 * (i % 3)
                               for i in range(57)],
    "gov_bonds_nonresident": [200.0 + 50 * (i % 6)
                              for i in range(57)],
}
MODE = {"drop": set(), "dead": False, "nd_at": None}


def fake_fetch():
    if MODE["dead"]:
        return None, "fetch_error:boom"
    out = {}
    for k, vs in VALS.items():
        if k in MODE["drop"]:
            out[k] = []
            continue
        rows = []
        for i, v in enumerate(vs):
            if MODE["nd_at"] == (k, i):
                continue                    # engine skips n.d.
            rows.append((QS[i], v))
        out[k] = rows
    return out, None


eng.bcrp_fetch = fake_fetch

# ---- Taiwan fixtures ----
CBC_LABELS = (["Current account-Net Value",
               "Direct investment-Debt instruments-Liabilities"]
              + ["pad%d" % i for i in range(2, 159)]
              + ["Portfolio investment-Balance",
                 "Portfolio investment-Assets",
                 "Portfolio investment-Liabilities",          # 161
                 "Portfolio investment-Equity and investment "
                 "fund shares-Balance",
                 "Portfolio investment-Equity and investment "
                 "fund shares-Assets",
                 "Portfolio investment-Equity and investment "
                 "fund shares-Liabilities",                   # 164
                 "pad165",
                 "Debt securities-Balance",
                 "Debt securities-Assets",
                 "Debt securities-Liabilities",               # 168
                 "Other investment-Liabilities",
                 "Debt securities-Liabilities"])              # 170 decoy
CBC_QS = ["%dQ%d" % (1984 + i // 4, i % 4 + 1) for i in range(30)]
CBC_MODE = {"dead": False, "shift": False, "ragged": False}


def cbc_val(qi, li):
    if li == 161:
        return 1000.0 + 10 * qi
    if li == 164:
        return 300.0 + 5 * qi
    if li == 168:
        return 700.0 + 5 * qi if qi != 2 else None    # "-" null
    return float(li)


def fake_cbc():
    if CBC_MODE["dead"]:
        return None, "fetch_error:boom"
    labels = list(CBC_LABELS)
    if CBC_MODE["shift"]:
        labels[161] = "Portfolio investment-LiabilitiesX"
    rows = []
    for qi, q in enumerate(CBC_QS):
        vals = [cbc_val(qi, li) for li in range(len(labels))]
        if CBC_MODE["ragged"] and qi == len(CBC_QS) - 1:
            vals = vals[:-5]
        rows.append((q, vals))
    return labels, rows


eng.cbc_fetch = fake_cbc

def fake_twse(day=None):
    if TW_MODE["dead"]:
        return None, "stat=NO"
    if day is None:
        day = TW_DAYS[-1]
    if day not in TW_NET:
        return None, "stat=NO"          # weekend/holiday
    return day, TW_NET[day]





def ind_z(vals):
    h, last = vals[:-1], vals[-1]
    mu = sum(h) / len(h)
    sd = (sum((v - mu) ** 2 for v in h) / (len(h) - 1)) ** 0.5
    return round(max(-4.0, min(4.0, (last - mu) / sd)), 2)


def main():
    STORE.clear()
    PUTS.clear()
    print("== full build ==")
    doc = eng.build()
    pe = doc["countries"]["peru"]
    chk("LIVE + peru LIVE", doc["status"] == "LIVE"
        and pe["status"] == "LIVE"
        and pe["latest_period"] == QS[-1])
    print("== A1 per-series identities ==")
    for k, vs in VALS.items():
        s = pe["series"][k]
        ok = (s["latest"] == round(vs[-1], 1)
              and s["sum_4q"] == round(sum(vs[-4:]), 1)
              and s["z_all"] == ind_z(vs)
              and s["n_obs"] == 57 and s["first"] == QS[0])
        chk("A1 %s" % k, ok, "latest=%s z=%s" % (s["latest"],
                                                 s["z_all"]))
    print("== A2 deferred + composites honest ==")
    chk("A2 deferrals now korea/chile/imf only",
        set(doc["deferred"]) == {"korea", "chile", "imf_layer"}
        and "INDICATOR" in doc["deferred"]["imf_layer"]["why"])
    chk("A2 composites null with reasons",
        doc["composites"]["cfi"]["value"] is None
        and "partial" in doc["composites"]["cfi"]["why"]
        and doc["composites"]["hot_money"]["value"] is None)
    print("== A3 bank merge + survival ==")
    sid = eng.PE_SERIES["portfolio_total"]
    chk("A3 bank n=57", len(STORE[eng.BANK_FMT % sid]["rows"])
        == 57)
    STORE[eng.BANK_FMT % sid]["rows"]["T4.05"] = 999.0
    PUTS.clear()
    eng.build()
    chk("A3 pre-2012 banked row survives",
        STORE[eng.BANK_FMT % sid]["rows"]["T4.05"] == 999.0)
    chk("A3 no-new -> bank not rewritten",
        (eng.BANK_FMT % sid) not in PUTS)
    print("== A4 honesty paths ==")
    MODE["drop"] = {"portfolio_equity", "gov_bonds_nonresident"}
    d2 = eng.build({})
    chk("A4 2/4 -> peru THIN (doc LIVE via taiwan)",
        d2["countries"]["peru"]["status"] == "THIN"
        and d2["status"] == "LIVE")
    MODE["drop"] = set()
    MODE["dead"] = True
    d3 = eng.build({})
    chk("A4 peru fetch death -> peru MISSING, doc LIVE via "
        "taiwan", d3["countries"]["peru"]["status"] == "MISSING"
        and d3["status"] == "LIVE")
    MODE["dead"] = False
    print("== T1 Taiwan CBC label-bind identities ==")
    tw = doc["countries"]["taiwan"]
    m = tw["macro"]
    chk("T1 macro LIVE latest_period", m["status"] == "LIVE"
        and m["latest_period"] == CBC_QS[-1])
    tot = m["series"]["portfolio_liab_total"]
    eq = m["series"]["portfolio_liab_equity"]
    db = m["series"]["portfolio_liab_debt"]
    chk("T1 exact-label indices 161/164",
        tot["label_index"] == 161 and eq["label_index"] == 164)
    chk("T1 debt = FIRST bare label AFTER equity (168, not "
        "decoy 170)", db["label_index"] == 168)
    exp_tot = [cbc_val(q, 161) for q in range(30)]
    chk("T1 total latest/sum/z", tot["latest"]
        == round(exp_tot[-1], 1)
        and tot["sum_4q"] == round(sum(exp_tot[-4:]), 1)
        and tot["z_all"] == ind_z(exp_tot))
    chk("T1 '-' null dropped from debt series",
        db["n_obs"] == 29 and db["latest"]
        == round(cbc_val(29, 168), 1))
    chk("T1 taiwan bank written",
        len(STORE[eng.CBC_BANK_FMT % "portfolio_liab_total"]
            ["rows"]) == 30)

    jp = d2["countries"]["japan"]
    chk("T2 japan MOF weekly LIVE, oku->bn, both directions",
        jp["status"] == "LIVE" and jp["n_weeks"] == 30
        and jp["series"]["inward_lt_bonds"]["latest"]
        == round((100 + 29 * 10 + 50) / 10.0, 1)
        and jp["series"]["outward_lt_bonds"]["latest"]
        == round((100 + 29 * 10 + 20) / 10.0, 1)
        and jp["series"]["outward_lt_bonds"]["sum_4q"]
        == round(sum((100 + i * 10 + 20) / 10.0
                     for i in range(26, 30)), 1)
        and jp["window_label"] == "4w sum")
    chk("T2 mof bank written (30 periods)",
        len(STORE[eng.MOF_BANK]["rows"]) == 30)
    chk("T2 month attribution: rollover week -> Jan next year",
        eng.month_of_period("2026\uff0e12\uff0e28\uff5e1"
                            "\uff0e3") == "2027-01"
        and eng.month_of_period("2026\uff0e8\uff0e2\uff5e8"
                                "\uff0e8") == "2026-08")
    cc = jp["tic_concordance"]
    chk("T2 concordance honest-missing without tic bank",
        cc["status"] == "MISSING" and "bank rows=0"
        in cc["why"])
    seed_tic_jp(40)
    MOFN["n"] = 140
    d_cc = eng.build({})
    MOFN["n"] = 30
    cc2 = d_cc["countries"]["japan"]["tic_concordance"]
    mof_m = {}
    for per, r in STORE[eng.MOF_BANK]["rows"].items():
        mm = eng.month_of_period(per)
        if mm:
            mof_m[mm] = round(mof_m.get(mm, 0)
                              + r["out_lt"], 1)
    tic_m = {d[:7]: round(v / 1000.0, 2)
             for d, v in STORE[eng.TIC_JP_BANK]["rows"]
             .items()}
    common = sorted(set(mof_m) & set(tic_m))
    exp_agree = round(100.0 * sum(
        (mof_m[m] > 0) == (tic_m[m] > 0)
        for m in common) / len(common), 1) if len(common) \
        >= 24 else None
    if len(common) >= 24:
        chk("T2 concordance LIVE == independent recompute",
            cc2["status"] == "LIVE"
            and cc2["n_months"] == len(common)
            and cc2["sign_agree_pct"] == exp_agree
            and cc2["corr_lag0"] == eng._pearson(
                [mof_m[m] for m in common],
                [tic_m[m] for m in common]))
    else:
        chk("T2 concordance honest on thin overlap (%d)"
            % len(common),
            cc2["status"] == "MISSING"
            and "aligned months" in cc2["why"])
    _mf = eng.mof_fetch
    eng.mof_fetch = lambda: (None, "fetch_error:boom")
    d_mo = eng.build({})
    chk("T2 mof dead -> japan MISSING named, doc still LIVE",
        d_mo["countries"]["japan"]["status"] == "MISSING"
        and "boom" in d_mo["countries"]["japan"]["why"]
        and d_mo["status"] == "LIVE")
    eng.mof_fetch = _mf
    print("== T2 hot-money MOVED pointer ==")
    chk("T2 hot_money = MOVED -> data/hot-money.json",
        tw["hot_money"]["status"] == "MOVED"
        and "hot-money" in tw["hot_money"]["see"])

    print("== T4 Taiwan honesty paths ==")
    CBC_MODE["shift"] = True
    d_s = eng.build({})
    chk("T4 moved label -> macro MISSING (refuses positional "
        "bind)", d_s["countries"]["taiwan"]["macro"]["status"]
        == "MISSING")
    CBC_MODE["shift"] = False
    CBC_MODE["ragged"] = True
    d_r = eng.build({})
    chk("T4 width mismatch -> macro MISSING",
        d_r["countries"]["taiwan"]["macro"]["status"]
        == "MISSING")
    CBC_MODE["ragged"] = False
    CBC_MODE["dead"] = True
    d_d = eng.build({})
    chk("T4 cbc dead -> taiwan MISSING (macro-only), doc LIVE "
        "on peru", d_d["countries"]["taiwan"]["status"]
        == "MISSING" and d_d["status"] == "LIVE")
    CBC_MODE["dead"] = False

    print("== A5b endq_prev unit ==")
    from datetime import datetime as _dt, timezone as _tz
    cases = {(2026, 8): "2026-2", (2026, 1): "2025-4",
             (2026, 4): "2026-1", (2026, 12): "2026-3"}
    ok = all(eng.endq_prev(_dt(y, m, 15, tzinfo=_tz.utc)) == v
             for (y, m), v in cases.items())
    chk("A5b endq = previous completed quarter (4 cases)", ok)

    print("== A5 write discipline ==")
    PUTS.clear()
    out = eng.lambda_handler({}, None)
    chk("A5 handler writes OUT last", PUTS[-1] == eng.OUT_KEY
        and out["ok"] is True)
    chk("A5 only bcrp/cbc/mof banks + out written (TWSE ledger "
        "NEVER touched)",
        all(p == eng.OUT_KEY
            or p.startswith("data/providers/bcrp/")
            or p.startswith("data/providers/cbc/")
            or p == eng.MOF_BANK
            for p in PUTS))
    print()
    if FAILS:
        print("HARNESS FAILED: %s" % FAILS)
        sys.exit(1)
    print("HARNESS GREEN -- push gate open (ops 4854)")


if __name__ == "__main__":
    main()
