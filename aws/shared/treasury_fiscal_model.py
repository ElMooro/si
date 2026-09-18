"""Pure FiscalData compiler: exact decimals, dimensional identity, dated evidence.

These are current-vintage fiscal measurements, not signals or spot FX. A replay
of retained responses does not establish historical publication availability.
"""
import hashlib
import json
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

CONTRACT = "treasury-fiscal-warehouse.v2"
BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
DATASETS = {
    "debt_to_penny": dict(path="/v2/accounting/od/debt_to_penny", dimensions=[],
        fields={"tot_pub_debt_out_amt": "USD", "debt_held_public_amt": "USD", "intragov_hold_amt": "USD"},
        cadence="business_daily", max_age_days=7, headline="tot_pub_debt_out_amt"),
    "rates_of_exchange": dict(path="/v1/accounting/od/rates_of_exchange",
        dimensions=["country", "currency", "country_currency_desc", "effective_date"],
        fields={"exchange_rate": "foreign_currency_units_per_USD"}, cadence="quarterly", max_age_days=150,
        usage="Treasury reporting/accounting rate; not spot FX or a portfolio valuation quote"),
    "avg_interest_rates": dict(path="/v2/accounting/od/avg_interest_rates",
        dimensions=["security_type_desc", "security_desc"], fields={"avg_interest_rate_amt": "percent_per_annum"},
        cadence="monthly", max_age_days=75, usage="Average interest rate by Treasury security category; not a market yield"),
    "interest_expense": dict(path="/v2/accounting/od/interest_expense",
        dimensions=["expense_catg_desc", "expense_group_desc", "expense_type_desc"],
        fields={"month_expense_amt": "USD_monthly_flow", "fytd_expense_amt": "USD_fiscal_year_to_date_flow"},
        cadence="monthly", max_age_days=75),
    "debt_outstanding": dict(path="/v2/accounting/od/debt_outstanding", dimensions=[],
        fields={"debt_outstanding_amt": "USD"}, cadence="fiscal_annual", max_age_days=430, headline="debt_outstanding_amt"),
    "tga_operating_cash": dict(path="/v1/accounting/dts/operating_cash_balance", dimensions=["account_type"],
        fields={"open_today_bal": "USD_millions", "close_today_bal": "USD_millions",
                "open_month_bal": "USD_millions", "open_fiscal_year_bal": "USD_millions"},
        cadence="business_daily", max_age_days=7, headline="open_today_bal"),
}
TGA_CLOSING = "Treasury General Account (TGA) Closing Balance"
TGA_OPENING = "Treasury General Account (TGA) Opening Balance"
TGA_DEPOSITS = "Total TGA Deposits (Table II)"
TGA_WITHDRAWALS = "Total TGA Withdrawals (Table II) (-)"
TGA_ACCOUNT_TYPES = (TGA_OPENING, TGA_DEPOSITS, TGA_WITHDRAWALS, TGA_CLOSING)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def number(value):
    if value is None or isinstance(value, bool) or str(value).strip().lower() in ("", "null", "nan", "n/a"):
        return None
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except InvalidOperation:
        return None


def identity(dataset, row):
    cfg = DATASETS[dataset]
    stamp = row.get("record_date")
    if not isinstance(stamp, str) or date.fromisoformat(stamp).isoformat() != stamp:
        raise ValueError("invalid observation date")
    dims = {k: row.get(k) for k in cfg["dimensions"]}
    if any(not isinstance(v, str) or not v.strip() or v.lower() == "null" for v in dims.values()):
        raise ValueError("required dataset dimension missing")
    return encoded([stamp, dims]).decode()


def series_id(dataset, field, dimensions):
    # effective_date distinguishes a revision within a quarterly FX observation;
    # it is not a new currency time series at every release.
    stable = {k: v for k, v in dimensions.items() if k != "effective_date"}
    return "TREASURY:" + dataset + ":" + digest([field, stable])


def is_headline(dataset, row, field):
    cfg = DATASETS[dataset]
    return (cfg.get("headline") == field and
            (dataset != "tga_operating_cash" or row.get("account_type") == TGA_CLOSING))


def build(dataset, previous, pages, generated_at, previous_evidence=None):
    """Merge retained originals by the full key; previous legacy rows stay archived.

    Each page contains the decoded original response and its read-verified raw
    receipt. Conflicting duplicates within this acquisition fail the whole unit.
    CAS publication is the runtime's responsibility.
    """
    cfg = DATASETS[dataset]
    today = datetime.fromisoformat(generated_at.replace("Z", "+00:00")).date()
    records = {}
    sources = {}
    old = previous if isinstance(previous, dict) else {}
    if old.get("contract") == CONTRACT:
        if old.get("dataset") != dataset:
            raise ValueError("previous dataset identity differs")
        sources.update(old.get("sources") or {})
        for item in old.get("records", []):
            key = identity(dataset, item["row"])
            if key in records or item["source_key"] not in sources:
                raise ValueError("previous warehouse identity invalid")
            records[key] = item
    incoming = {}
    coverage = []
    for page in pages:
        doc, receipt = page["document"], page["receipt"]
        acquired_at = page["acquired_at"]
        clock = datetime.fromisoformat(acquired_at)
        if clock.tzinfo is None or clock > datetime.fromisoformat(generated_at.replace("Z", "+00:00")):
            raise ValueError("invalid acquisition clock")
        if receipt.get("contract") != "raw-snapshot.v2" or receipt.get("captured_bytes_verified") is not True:
            raise ValueError("read-verified original response required")
        if receipt.get("provider") != "treasury" or not receipt.get("source_url", "").startswith(BASE + cfg["path"] + "?"):
            raise ValueError("provider dataset request differs")
        rows = doc.get("data")
        meta = doc.get("meta") or {}
        if not isinstance(rows, list) or int(meta.get("count", -1)) != len(rows):
            raise ValueError("provider row count differs")
        total = int(meta.get("total-count", -1))
        if total < len(rows):
            raise ValueError("provider total count invalid")
        key = receipt["key"]
        sources[key] = receipt
        coverage.append({"source_key": key, "rows_returned": len(rows), "matching_query_total": total,
                         "query_complete": len(rows) == total})
        for index, row in enumerate(rows):
            ident = identity(dataset, row)
            if date.fromisoformat(row["record_date"]) > today:
                raise ValueError("future observation date")
            item = {"row": row, "source_key": key, "row_index": index, "acquired_at": acquired_at}
            if ident in incoming and incoming[ident]["row"] != row:
                raise ValueError("conflicting dimensional duplicate in acquisition")
            incoming[ident] = item
    if not pages:
        raise ValueError("no original response supplied")
    for ident, item in incoming.items():
        existing = records.get(ident)
        if existing:
            old_clock = datetime.fromisoformat(existing["acquired_at"])
            new_clock = datetime.fromisoformat(item["acquired_at"])
            if new_clock < old_clock:
                continue
            if new_clock == old_clock and item["row"] != existing["row"]:
                raise ValueError("conflicting values at one capture time")
        records[ident] = item
    ordered = [records[k] for k in sorted(records)]
    # Only sources still used by a row need copying into the current warehouse.
    used = {item["source_key"] for item in ordered}
    sources = {k: sources[k] for k in sorted(used)}
    out = {"contract": CONTRACT, "version": "2.0.0", "dataset": dataset, "generated_at": generated_at,
           "source_url": BASE + cfg["path"], "cadence": cfg["cadence"], "max_age_days": cfg["max_age_days"],
           "dimensions": cfg["dimensions"], "field_units": cfg["fields"], "records": ordered, "sources": sources,
           "n_records": len(ordered), "acquisition": coverage, "previous_evidence": previous_evidence,
           "history_scope": "retained current-vintage rows; query completeness is reported per acquisition",
           "publication_time_verified": False, "calls_eligible": False, "sizing_eligible": False,
           "legacy_rows_promoted": 0, "usage": cfg.get("usage", "Fiscal measurement; no standalone investment recommendation")}
    # Compatibility projection exists only for an explicitly defined single series.
    obs = []
    for item in ordered:
        row = item["row"]
        field = cfg.get("headline")
        if field and is_headline(dataset, row, field):
            val = number(row.get(field))
            if val is not None:
                obs.append({"date": row["record_date"], "value": float(val), "value_decimal": str(val),
                            "source_key": item["source_key"], "row_index": item["row_index"]})
    out.update(observations=obs, n_obs=len(obs), unit=cfg["fields"].get(cfg.get("headline")))
    out["latest"] = latest(out, generated_at)
    return out


def measurements(warehouse, latest_only=True):
    ds = warehouse["dataset"]
    cfg = DATASETS[ds]
    records = warehouse.get("records", [])
    latest_date = max((item["row"]["record_date"] for item in records), default=None)
    result = []
    for item in records:
        row = item["row"]
        if latest_only and row["record_date"] != latest_date:
            continue
        dims = {k: row[k] for k in cfg["dimensions"]}
        for field, unit in cfg["fields"].items():
            val = number(row.get(field))
            result.append({"series_id": series_id(ds, field, dims), "field": field, "dimensions": dims,
                           "as_of": row["record_date"], "value": float(val) if val is not None else None,
                           "value_decimal": str(val) if val is not None else None, "unit": unit,
                           "source_key": item["source_key"], "row_index": item["row_index"],
                           "headline": is_headline(ds, row, field)})
    return result


def latest(warehouse, now):
    ds = warehouse["dataset"]
    cfg = DATASETS[ds]
    rows = measurements(warehouse)
    stamp = rows[0]["as_of"] if rows else None
    today = datetime.fromisoformat(now.replace("Z", "+00:00")).date()
    age = (today - date.fromisoformat(stamp)).days if stamp else None
    status = "unavailable" if age is None or age < 0 else "stale" if age > cfg["max_age_days"] else "fresh"
    heads = [row for row in rows if row["headline"]]
    headline = heads[0] if len(heads) == 1 else None
    check = {"status": "not_applicable"}
    if ds == "debt_to_penny":
        vals = {r["field"]: number(r["value_decimal"]) for r in rows}
        keys = ("tot_pub_debt_out_amt", "debt_held_public_amt", "intragov_hold_amt")
        if all(vals.get(k) is not None for k in keys):
            diff = vals[keys[0]] - vals[keys[1]] - vals[keys[2]]
            check = {"status": "reconciled" if diff == 0 else "mismatch", "difference_decimal": str(diff),
                     "unit": "USD", "formula": "total_public_debt - debt_held_by_public - intragovernmental_holdings"}
        else:
            check = {"status": "unavailable"}
    elif ds == "tga_operating_cash":
        vals = {r["dimensions"]["account_type"]: number(r["value_decimal"]) for r in rows if r["field"] == "open_today_bal"}
        if all(vals.get(k) is not None for k in TGA_ACCOUNT_TYPES):
            diff = vals[TGA_CLOSING] - (vals[TGA_OPENING] + vals[TGA_DEPOSITS] - vals[TGA_WITHDRAWALS])
            check = {"status": "within_reported_rounding" if abs(diff) <= 2 else "mismatch",
                     "difference_decimal": str(diff), "unit": "USD_millions", "rounding_tolerance": 2,
                     "formula": "closing - (opening + deposits - withdrawals)",
                     "tolerance_basis": "four whole-million reported inputs; up to 0.5 million rounding each; reported values never adjusted"}
        else:
            check = {"status": "unavailable", "reason": "all four exact modern TGA account types required"}
    if check["status"] in ("mismatch", "unavailable") and ds in ("debt_to_penny", "tga_operating_cash"):
        status = "partial" if status == "fresh" else status
    if not any(row["value"] is not None for row in rows):
        status = "unavailable"
    return {"dataset": ds, "date": stamp, "current": headline["value"] if headline else None,
            "value_decimal": headline["value_decimal"] if headline else None,
            "unit": headline["unit"] if headline else None, "headline": headline, "measurements": rows,
            "scalar_selection": "explicit_defined_series" if headline else "dimension_selection_required",
            "freshness": {"status": status, "age_days": age, "max_age_days": cfg["max_age_days"],
                          "cadence": cfg["cadence"], "basis": "observation_age_ceiling; publication_calendar_not_verified"},
            "reconciliation": check, "n_records": warehouse["n_records"], "n_obs": warehouse["n_obs"],
            "usage": warehouse["usage"], "sizing_eligible": False}


def series_rows(warehouse, selector=None):
    """Exact chart series only. Bare mixed datasets cannot become a line chart."""
    if warehouse.get("contract") != CONTRACT:
        raise ValueError("legacy dimensionless Treasury history is unverified")
    rows = measurements(warehouse, latest_only=False)
    if selector:
        rows = [r for r in rows if r["series_id"] == selector]
    else:
        rows = [r for r in rows if r["headline"]]
    if not rows:
        raise ValueError("select an explicit Treasury series/dimension")
    ids = {r["series_id"] for r in rows}
    dates = [r["as_of"] for r in rows]
    if len(ids) != 1 or len(dates) != len(set(dates)):
        raise ValueError("ambiguous Treasury observation/vintage; select a unique series")
    return sorted(rows, key=lambda r: r["as_of"])
