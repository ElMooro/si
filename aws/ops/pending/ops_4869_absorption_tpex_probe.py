"""ops/4869 -- field-dump probe for wave-2A build 2 (report-only).
 (1) TPEx OTC foreign net: dump row0 keys + 2 sample rows from
     BOTH proven endpoints; identify the foreign-net field by
     name; sum it across all rows for today's market total.
 (2) TreasuryDirect auctions: dump ONE full auction row verbatim
     (all fields) so the indirect-bidder math binds real names.
 (3) FiscalData MSPD: confirm the exact 'Total Marketable' class
     label + last 3 monthly held-by-public levels.
Hard-fail only if TPEx foreign-net field cannot be identified.
"""
import json
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 justhodl-ops-4869",
      "Accept": "application/json,text/*;q=0.8"}


def get(url, timeout=60):
    with urllib.request.urlopen(
            urllib.request.Request(url, headers=UA),
            timeout=timeout) as r:
        return r.status, r.read()


def num(x):
    try:
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return None


def main():
    with report("ops 4869 -- absorption + tpex field dump") \
            as rep:
        rep.heading("1. TPEx openapi list")
        found = False
        try:
            st, raw = get("https://www.tpex.org.tw/openapi/v1/"
                          "tpex_3insti_daily_trading")
            j = json.loads(raw)
            r0 = j[0] if isinstance(j, list) and j else {}
            rep.ok("  HTTP %d rows=%d" % (st, len(j)))
            rep.log("  row0 keys: %s" % sorted(r0)[:30])
            rep.log("  row0: %s"
                    % json.dumps(r0, ensure_ascii=False)[:400])
            cands = [k for k in r0
                     if "foreign" in k.lower()
                     and ("net" in k.lower()
                          or "diff" in k.lower())]
            rep.log("  foreign-net key candidates: %s" % cands)
            for k in cands[:2]:
                tot = sum(v for v in (num(row.get(k))
                                      for row in j)
                          if v is not None)
                rep.ok("  SUM(%s) over %d rows = %+.0f" % (
                    k, len(j), tot))
                found = True
        except Exception as e:  # noqa: BLE001
            rep.warn("  openapi died: %s" % str(e)[:80])
        time.sleep(0.5)

        rep.heading("2. TPEx en-us result.php")
        try:
            st, raw = get("https://www.tpex.org.tw/web/stock/"
                          "3insti/daily_trade/3itrade_hedge_"
                          "result.php?l=en-us&t=D&o=json")
            j = json.loads(raw)
            tb = (j.get("tables") or [{}])[0]
            rep.ok("  HTTP %d date=%s totalCount=%s"
                   % (st, tb.get("date"),
                      tb.get("totalCount")))
            rep.log("  fields: %s" % (tb.get("fields")
                                      or [])[:26])
            data = tb.get("data") or []
            if data:
                rep.log("  data row0: %s"
                        % json.dumps(data[0],
                                     ensure_ascii=False)[:340])
        except Exception as e:  # noqa: BLE001
            rep.warn("  result.php died: %s" % str(e)[:80])
        time.sleep(0.5)

        rep.heading("3. TreasuryDirect full row")
        try:
            st, raw = get("https://www.treasurydirect.gov/TA_WS/"
                          "securities/auctioned?format=json"
                          "&days=30&type=Note")
            j = json.loads(raw)
            r0 = j[0] if j else {}
            rep.ok("  HTTP %d rows=%d" % (st, len(j)))
            rep.log("  FULL row0: %s" % json.dumps(r0)[:900])
            rep.log("  row0 keys: %s" % sorted(r0))
        except Exception as e:  # noqa: BLE001
            rep.warn("  treasurydirect died: %s" % str(e)[:80])
        time.sleep(0.5)

        rep.heading("4. MSPD Total Marketable")
        try:
            st, raw = get(
                "https://api.fiscaldata.treasury.gov/services/"
                "api/fiscal_service/v1/debt/mspd/mspd_table_1"
                "?fields=record_date,security_type_desc,"
                "security_class_desc,debt_held_public_mil_amt"
                "&sort=-record_date&page%5Bsize%5D=40")
            j = json.loads(raw)
            classes = sorted({d.get("security_class_desc")
                              for d in j.get("data") or []})
            rep.ok("  HTTP %d classes=%s" % (st, classes))
            tot = [d for d in j.get("data") or []
                   if "total" in str(
                       d.get("security_class_desc", "")).lower()
                   and "market" in str(
                       d.get("security_type_desc", "")).lower()]
            for d in tot[:3]:
                rep.log("  %s | %s | held_public=%sM"
                        % (d["record_date"],
                           d["security_class_desc"],
                           d["debt_held_public_mil_amt"]))
        except Exception as e:  # noqa: BLE001
            rep.warn("  mspd died: %s" % str(e)[:80])

        rep.heading("verdict")
        if not found:
            rep.fail("TPEx foreign-net field unresolved")
            sys.exit(1)
        rep.ok("fields dumped -- wires bind ONLY what is above")


if __name__ == "__main__":
    main()
