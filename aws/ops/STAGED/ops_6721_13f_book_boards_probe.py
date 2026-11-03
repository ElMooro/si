"""ops 6721 - READ-ONLY: compute the institutional-book boards from the native 13F fund artifacts (design audit).

Follow-up to ops 6720 (the live desk feed carries no boards: most_bought/most_sold are empty, transaction
inference is "not_observed_in_13f"). The redesigned 13F page needs boards that are DEFENSIBLE from the
disclosures alone, so this probe derives them only from retained data:

  per fund (all 18, from data/holdings-research/funds/<sha>.json, contract holdings-native-fund.v1):
    - current period: equity rows (quantity_type SH, no put/call) vs option rows (PUT/CALL)
    - total reported value (current and prior period), position counts, top-10 by reported value and the
      share of the book they carry, rows newly present / absent / increased / decreased by reported value
      (increase/decrease = reported QUANTITY change between the two dated disclosures; the $ shown is the
      reported value change, which also carries the price move - labelled as such)
  across the current cohort (funds whose current period == required period):
    - by native identity (CUSIP + class): managers holding, sum of reported value, managers newly present /
      absent / increased / decreased; boards for widely held, newly present, dropped, increased, decreased
    - the same boards for the CONCENTRATED managers (<= 100 equity positions) so the hedge books
      (thousands of rows) do not drown Berkshire/Baupost/Elliott/Lone Pine/Coatue/Tiger
  No tickers are inferred (symbol stays None in the artifacts); issuer names are the filers' own text.

Reads S3 only. Invokes nothing. Writes nothing but this report (boards embedded as one JSON block).
"""
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
DESK_KEY = "data/13f-desk.json"
CONCENTRATED_MAX = 100
TOP = 10
BOARD = 20


def dec(v):
    try:
        return Decimal(str(v))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(0)


def f(v):
    return float(v) if v is not None else None


def is_equity(identity):
    return (identity or {}).get("quantity_type") == "SH" and not (identity or {}).get("put_call")


def fund_boards(detail):
    """Per-fund boards from one native artifact. Pure (dict in, dict out)."""
    periods = detail.get("periods") or {}
    cur_p = detail.get("current_holdings_period")
    pri_p = detail.get("prior_holdings_period")
    cur = (periods.get(cur_p) or {}).get("positions") or {}
    pri = (periods.get(pri_p) or {}).get("positions") or {}
    eq = {k: v for k, v in cur.items() if is_equity(v.get("identity"))}
    opt = {k: v for k, v in cur.items() if not is_equity(v.get("identity"))}
    eq_prior = {k: v for k, v in pri.items() if is_equity(v.get("identity"))}
    total = sum((dec(v.get("reported_value_usd")) for v in eq.values()), Decimal(0))
    total_prior = sum((dec(v.get("reported_value_usd")) for v in eq_prior.values()), Decimal(0))
    puts = sum((dec(v.get("reported_value_usd")) for v in opt.values() if (v.get("identity") or {}).get("put_call") == "PUT"), Decimal(0))
    calls = sum((dec(v.get("reported_value_usd")) for v in opt.values() if (v.get("identity") or {}).get("put_call") == "CALL"), Decimal(0))

    def row(pid, pos, extra=None):
        ident = pos.get("identity") or {}
        r = {"id": pid[:12], "issuer": (pos.get("issuer_names") or [""])[0], "class": ident.get("class"),
             "cusip": ident.get("cusip"), "value_usd": f(dec(pos.get("reported_value_usd"))),
             "qty": f(dec(pos.get("reported_quantity"))),
             "pct_of_book": f(round(100 * dec(pos.get("reported_value_usd")) / total, 2)) if total else None}
        if extra:
            r.update(extra)
        return r

    comp = {r["position_id"]: r for r in ((detail.get("comparison") or {}).get("rows") or [])}
    top = sorted(eq.items(), key=lambda kv: -dec(kv[1].get("reported_value_usd")))[:TOP]
    top_rows = []
    for pid, pos in top:
        c = comp.get(pid) or {}
        top_rows.append(row(pid, pos, {"status": c.get("status"), "qty_change": f(dec(c.get("reported_quantity_change"))) if c.get("reported_quantity_change") is not None else None,
                                       "value_change_usd": f(dec(c.get("reported_value_change_usd"))) if c.get("reported_value_change_usd") is not None else None}))
    top10_pct = f(round(100 * sum((dec(p.get("reported_value_usd")) for _, p in top), Decimal(0)) / total, 1)) if total else None

    new_rows, absent_rows, inc_rows, dec_rows = [], [], [], []
    for pid, c in comp.items():
        st = c.get("status")
        if st == "newly_present_in_public_disclosure" and pid in eq:
            new_rows.append(row(pid, eq[pid]))
        elif st == "not_present_in_current_public_disclosure" and pid in eq_prior:
            p = eq_prior[pid]
            absent_rows.append({"id": pid[:12], "issuer": (p.get("issuer_names") or [""])[0], "class": (p.get("identity") or {}).get("class"),
                                "cusip": (p.get("identity") or {}).get("cusip"), "prior_value_usd": f(dec(p.get("reported_value_usd"))),
                                "prior_qty": f(dec(p.get("reported_quantity")))})
        elif st in ("reported_quantity_increased", "reported_quantity_decreased") and pid in eq and pid in eq_prior:
            p0 = eq_prior[pid]
            q0 = dec(p0.get("reported_quantity"))
            qc = dec(c.get("reported_quantity_change"))
            r = row(pid, eq[pid], {"qty_change": f(qc), "qty_change_pct": f(round(100 * qc / q0, 1)) if q0 else None,
                                   "value_change_usd": f(dec(c.get("reported_value_change_usd")))})
            (inc_rows if st == "reported_quantity_increased" else dec_rows).append(r)
    new_rows.sort(key=lambda r: -(r["value_usd"] or 0))
    absent_rows.sort(key=lambda r: -(r["prior_value_usd"] or 0))
    inc_rows.sort(key=lambda r: -(r["value_change_usd"] or 0))
    dec_rows.sort(key=lambda r: (r["value_change_usd"] or 0))
    counts = (detail.get("comparison") or {}).get("counts") or {}
    return {
        "current_period": cur_p, "prior_period": pri_p,
        "n_equity_positions": len(eq), "n_option_rows": len(opt), "n_prior_equity_positions": len(eq_prior),
        "total_value_usd": f(total), "total_value_prior_usd": f(total_prior),
        "put_value_usd": f(puts), "call_value_usd": f(calls),
        "top10_pct_of_book": top10_pct, "top": top_rows,
        "newly_present": new_rows[:TOP], "absent": absent_rows[:TOP], "increased": inc_rows[:TOP], "decreased": dec_rows[:TOP],
        "counts": counts,
        "shared_discretion_positions": sum(1 for v in eq.values() if v.get("other_managers")),
    }, eq, eq_prior, comp


def cross_boards(members):
    """members: list of (fund_key, eq, eq_prior, comp). Pure."""
    agg = {}
    for fk, eq, eq_prior, comp in members:
        for pid, pos in eq.items():
            ident = pos.get("identity") or {}
            a = agg.setdefault(pid, {"issuer": (pos.get("issuer_names") or [""])[0], "class": ident.get("class"), "cusip": ident.get("cusip"),
                                     "holders": [], "value_usd": Decimal(0), "new": [], "absent": [], "inc": [], "dec": [], "flat": []})
            a["holders"].append(fk)
            a["value_usd"] += dec(pos.get("reported_value_usd"))
            st = (comp.get(pid) or {}).get("status")
            if st == "newly_present_in_public_disclosure":
                a["new"].append(fk)
            elif st == "reported_quantity_increased":
                a["inc"].append(fk)
            elif st == "reported_quantity_decreased":
                a["dec"].append(fk)
            elif st == "reported_quantity_unchanged":
                a["flat"].append(fk)
        for pid, c in comp.items():
            if c.get("status") == "not_present_in_current_public_disclosure" and pid in eq_prior:
                p = eq_prior[pid]
                ident = p.get("identity") or {}
                a = agg.setdefault(pid, {"issuer": (p.get("issuer_names") or [""])[0], "class": ident.get("class"), "cusip": ident.get("cusip"),
                                         "holders": [], "value_usd": Decimal(0), "new": [], "absent": [], "inc": [], "dec": [], "flat": []})
                a["absent"].append(fk)

    def out(a, pid):
        return {"id": pid[:12], "issuer": a["issuer"], "class": a["class"], "cusip": a["cusip"], "n_holders": len(a["holders"]),
                "value_usd": f(a["value_usd"]), "holders": sorted(a["holders"]), "new": sorted(a["new"]), "absent": sorted(a["absent"]),
                "inc": sorted(a["inc"]), "dec": sorted(a["dec"]), "n_new": len(a["new"]), "n_absent": len(a["absent"]),
                "n_inc": len(a["inc"]), "n_dec": len(a["dec"]), "n_flat": len(a["flat"])}
    items = list(agg.items())
    boards = {
        "widely_held": [out(a, p) for p, a in sorted(items, key=lambda kv: (-len(kv[1]["holders"]), -kv[1]["value_usd"]))[:BOARD]],
        "largest_value": [out(a, p) for p, a in sorted(items, key=lambda kv: -kv[1]["value_usd"])[:BOARD]],
        "newly_present": [out(a, p) for p, a in sorted(items, key=lambda kv: (-len(kv[1]["new"]), -kv[1]["value_usd"])) if a["new"]][:BOARD],
        "dropped": [out(a, p) for p, a in sorted(items, key=lambda kv: (-len(kv[1]["absent"]), -len(kv[1]["holders"]))) if a["absent"]][:BOARD],
        "increased": [out(a, p) for p, a in sorted(items, key=lambda kv: (-len(kv[1]["inc"]), -kv[1]["value_usd"])) if a["inc"]][:BOARD],
        "decreased": [out(a, p) for p, a in sorted(items, key=lambda kv: (-len(kv[1]["dec"]), -kv[1]["value_usd"])) if a["dec"]][:BOARD],
        "n_identities": len(agg),
    }
    return boards


def selftest():
    detail = {"current_holdings_period": "2026-06-30", "prior_holdings_period": "2026-03-31",
              "periods": {"2026-06-30": {"positions": {
                  "a": {"identity": {"cusip": "037833100", "class": "COM", "quantity_type": "SH", "put_call": None}, "issuer_names": ["APPLE INC"], "reported_quantity": "100", "reported_value_usd": "20000"},
                  "b": {"identity": {"cusip": "594918104", "class": "COM", "quantity_type": "SH", "put_call": None}, "issuer_names": ["MICROSOFT CORP"], "reported_quantity": "10", "reported_value_usd": "5000"},
                  "o": {"identity": {"cusip": "594918104", "class": "COM", "quantity_type": "SH", "put_call": "PUT"}, "issuer_names": ["MICROSOFT CORP"], "reported_quantity": "10", "reported_value_usd": "700"}}},
                          "2026-03-31": {"positions": {
                  "a": {"identity": {"cusip": "037833100", "class": "COM", "quantity_type": "SH", "put_call": None}, "issuer_names": ["APPLE INC"], "reported_quantity": "80", "reported_value_usd": "15000"},
                  "c": {"identity": {"cusip": "88160R101", "class": "COM", "quantity_type": "SH", "put_call": None}, "issuer_names": ["TESLA INC"], "reported_quantity": "5", "reported_value_usd": "1000"}}}},
              "comparison": {"rows": [
                  {"position_id": "a", "status": "reported_quantity_increased", "reported_quantity_change": "20", "reported_value_change_usd": "5000"},
                  {"position_id": "b", "status": "newly_present_in_public_disclosure"},
                  {"position_id": "c", "status": "not_present_in_current_public_disclosure"}], "counts": {"x": 1}}}
    b, eq, eqp, comp = fund_boards(detail)
    assert b["n_equity_positions"] == 2 and b["n_option_rows"] == 1 and b["total_value_usd"] == 25000.0 and b["put_value_usd"] == 700.0
    assert b["top"][0]["issuer"] == "APPLE INC" and b["top"][0]["pct_of_book"] == 80.0 and b["top10_pct_of_book"] == 100.0
    assert b["newly_present"][0]["issuer"] == "MICROSOFT CORP" and b["absent"][0]["issuer"] == "TESLA INC"
    assert b["increased"][0]["qty_change_pct"] == 25.0
    x = cross_boards([("F1", eq, eqp, comp), ("F2", eq, eqp, comp)])
    assert x["widely_held"][0]["n_holders"] == 2 and x["dropped"][0]["issuer"] == "TESLA INC" and x["n_identities"] == 3
    print("selftest OK")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=10, read_timeout=120)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    rc = 0

    def get(key):
        obj = s3.get_object(Bucket=LIVE, Key=key)
        return json.loads(obj["Body"].read() or b"{}"), obj["ContentLength"]

    with report("ops_6721_13f_book_boards_probe") as r:
        r.heading("ops 6721 - 13F institutional-book boards from the native fund artifacts (read-only) %s" % now.isoformat(timespec="minutes"))
        desk, size = get(DESK_KEY)
        required = desk.get("required_period_for_current_cohort")
        r.log("desk generated_at %s; required period %s; funds %s/%s; source_generated_at %s" % (
            desk.get("generated_at"), required, desk.get("funds_parsed"), desk.get("funds_total"), desk.get("source_generated_at")))
        r.log("desk.quality = %s" % json.dumps(desk.get("quality"), separators=(",", ":"))[:800])
        r.log("desk.limitations = %s" % json.dumps(desk.get("limitations"), ensure_ascii=False)[:1500])
        r.log("desk.report_period_cohorts = %s" % json.dumps(desk.get("report_period_cohorts"), separators=(",", ":"))[:1200])
        r.log("desk.ticker_identity = %s" % json.dumps(desk.get("ticker_identity"), ensure_ascii=False)[:600])
        r.log("desk.security_indexes = %s" % json.dumps(desk.get("security_indexes"), ensure_ascii=False)[:600])
        r.log("desk.view = %s | research = %s" % (json.dumps(desk.get("view"), ensure_ascii=False)[:400], json.dumps(desk.get("research"), ensure_ascii=False)[:400]))

        per_fund, members, members_conc = {}, [], []
        by_fund = desk.get("by_fund") or {}
        r.section("A. per-fund boards (%d artifacts)" % len(by_fund))
        for fk in sorted(by_fund):
            meta = by_fund[fk] or {}
            key = ((meta.get("detail") or {}).get("key"))
            if not key:
                r.warn("%s: no detail key" % fk)
                continue
            try:
                detail, size = get(key)
            except Exception as exc:  # noqa: BLE001
                r.warn("%s: %s unreadable: %s" % (fk, key, str(exc)[:160]))
                rc = 1
                continue
            boards, eq, eq_prior, comp = fund_boards(detail)
            boards.update({"name": meta.get("name"), "cik": meta.get("cik"), "latest_submission": meta.get("latest_submission"),
                           "chain": meta.get("current_chain_status"), "cohort": bool(meta.get("current_cohort_eligible")),
                           "observation_age_days": meta.get("observation_age_days"), "artifact_bytes": size,
                           "valuation_reviews": meta.get("valuation_reviews")})
            per_fund[fk] = boards
            r.log("%s: %s | period %s (prior %s) | equity %d (prior %d) option rows %d | value $%.3fB (prior $%.3fB) | puts $%.2fB calls $%.2fB | top10 %s%% | new %d absent %d inc %d dec %d | cohort %s | %d bytes" % (
                fk, boards["name"], boards["current_period"], boards["prior_period"], boards["n_equity_positions"], boards["n_prior_equity_positions"],
                boards["n_option_rows"], (boards["total_value_usd"] or 0) / 1e9, (boards["total_value_prior_usd"] or 0) / 1e9,
                (boards["put_value_usd"] or 0) / 1e9, (boards["call_value_usd"] or 0) / 1e9, boards["top10_pct_of_book"],
                len(boards["newly_present"]), len(boards["absent"]), len(boards["increased"]), len(boards["decreased"]), boards["cohort"], size))
            if boards["cohort"] and boards["current_period"] == required:
                members.append((fk, eq, eq_prior, comp))
                if boards["n_equity_positions"] <= CONCENTRATED_MAX:
                    members_conc.append((fk, eq, eq_prior, comp))

        r.section("B. cross-manager boards")
        cohort = cross_boards(members)
        conc = cross_boards(members_conc)
        cohort_total = sum((per_fund[fk]["total_value_usd"] or 0) for fk, *_ in members)
        r.log("cohort: %d managers (%s), %d identities, equity value $%.1fB" % (len(members), ", ".join(fk for fk, *_ in members), cohort["n_identities"], cohort_total / 1e9))
        r.log("concentrated (<= %d equity positions): %d managers (%s), %d identities" % (CONCENTRATED_MAX, len(members_conc), ", ".join(fk for fk, *_ in members_conc), conc["n_identities"]))
        for name, b in (("cohort", cohort), ("concentrated", conc)):
            for board in ("widely_held", "largest_value", "newly_present", "dropped", "increased", "decreased"):
                r.log("%s.%s top5: %s" % (name, board, "; ".join("%s (%s) h%d n%d a%d i%d d%d $%.2fB" % (
                    x["issuer"], x["cusip"], x["n_holders"], x["n_new"], x["n_absent"], x["n_inc"], x["n_dec"], (x["value_usd"] or 0) / 1e9) for x in b[board][:5])))

        r.section("C. boards JSON (for the page design; trimmed to board sizes)")
        payload = {"generated_at": now.isoformat(timespec="seconds"), "desk_generated_at": desk.get("generated_at"),
                   "required_period": required, "cohort_members": [fk for fk, *_ in members],
                   "concentrated_members": [fk for fk, *_ in members_conc], "cohort_equity_value_usd": cohort_total,
                   "per_fund": per_fund, "cohort": cohort, "concentrated": conc}
        text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False, default=str)
        r.log("payload bytes: %d" % len(text))
        r.lines.append("")
        r.lines.append("```json")
        r.lines.append(text)
        r.lines.append("```")
        r.lines.append("")
        if rc == 0:
            r.ok("read-only; nothing invoked, nothing written but this report")
        else:
            r.fail("one or more artifacts unreadable")
    sys.exit(rc)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
