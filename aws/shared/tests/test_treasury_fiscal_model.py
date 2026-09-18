import copy
import unittest
from treasury_fiscal_model import *

NOW = "2026-09-18T22:00:00+00:00"


def page(ds, rows, clock="2026-09-18T20:00:00+00:00", total=None):
    sha = digest(rows)
    return {"document": {"data": rows, "meta": {"count": len(rows), "total-count": len(rows) if total is None else total}},
            "receipt": {"key": "data/raw/v2/treasury/" + "0" * 64 + "/" + sha + ".bin.gz",
                        "contract": "raw-snapshot.v2", "captured_bytes_verified": True,
                        "provider": "treasury", "source_url": BASE + DATASETS[ds]["path"] + "?page%5Bsize%5D=2500",
                        "captured_at": clock, "sha256": sha}, "acquired_at": clock}


def debt(stamp="2026-09-17", total="40093343468150.50"):
    return {"record_date": stamp, "tot_pub_debt_out_amt": total,
            "debt_held_public_amt": "32392786557372.46", "intragov_hold_amt": "7700556910778.04"}


class FiscalModelTests(unittest.TestCase):
    def test_exact_debt_cents_reconcile_and_original_fields_survive(self):
        raw = debt(); raw["new_provider_dimension"] = "retained"
        out = build("debt_to_penny", None, [page("debt_to_penny", [raw])], NOW)
        self.assertEqual(out["latest"]["value_decimal"], "40093343468150.50")
        self.assertEqual(out["latest"]["reconciliation"]["status"], "reconciled")
        self.assertEqual(out["records"][0]["row"], raw)
        self.assertEqual(out["latest"]["headline"]["row_index"], 0)

    def test_currencies_are_not_one_arbitrary_scalar_or_chart(self):
        rows = [{"record_date": "2026-06-30", "country": c, "currency": u,
                 "country_currency_desc": c + "-" + u, "effective_date": "2026-06-30", "exchange_rate": v}
                for c, u, v in [("Afghanistan", "Afghani", "65.09"), ("Albania", "Lek", "82.4")]]
        out = build("rates_of_exchange", None, [page("rates_of_exchange", rows)], NOW)
        self.assertIsNone(out["latest"]["current"])
        self.assertEqual(len(out["latest"]["measurements"]), 2)
        self.assertEqual(out["latest"]["freshness"]["status"], "fresh")
        self.assertIn("not spot FX", out["usage"])
        with self.assertRaises(ValueError): series_rows(out)
        selected = series_rows(out, out["latest"]["measurements"][0]["series_id"])
        self.assertEqual(len(selected), 1)

    def test_monthly_and_fiscal_year_to_date_expense_cannot_share_series(self):
        row = {"record_date": "2026-08-31", "expense_catg_desc": "PUBLIC", "expense_group_desc": "ACCRUED",
               "expense_type_desc": "Notes", "month_expense_amt": "10.20", "fytd_expense_amt": "90.30"}
        out = build("interest_expense", None, [page("interest_expense", [row])], NOW)
        a, b = out["latest"]["measurements"]
        self.assertNotEqual(a["series_id"], b["series_id"])
        self.assertNotEqual(a["unit"], b["unit"])

    def test_tga_opening_deposits_withdrawals_and_closing_do_not_mix(self):
        rows = [{"record_date": "2026-09-17", "account_type": k, "open_today_bal": v, "close_today_bal": "null"}
                for k, v in zip(TGA_ACCOUNT_TYPES, ["991708", "283312", "302346", "972675"])]
        out = build("tga_operating_cash", None, [page("tga_operating_cash", rows)], NOW)
        self.assertEqual(out["latest"]["current"], 972675)
        self.assertEqual(out["n_obs"], 1)
        self.assertEqual(out["latest"]["reconciliation"]["difference_decimal"], "1")
        self.assertEqual(out["latest"]["reconciliation"]["status"], "within_reported_rounding")
        rows[-1]["open_today_bal"] = "972700"
        bad = build("tga_operating_cash", None, [page("tga_operating_cash", rows)], NOW)
        self.assertEqual(bad["latest"]["freshness"]["status"], "partial")
        self.assertEqual(bad["latest"]["current"], 972700)  # never adjust provider values

    def test_annual_debt_uses_annual_observation_age_not_daily_collector_clock(self):
        row = {"record_date": "2025-09-30", "debt_outstanding_amt": "37637553494935.61"}
        out = build("debt_outstanding", None, [page("debt_outstanding", [row])], NOW)
        self.assertEqual(out["latest"]["freshness"]["cadence"], "fiscal_annual")
        self.assertEqual(out["latest"]["freshness"]["status"], "fresh")
        self.assertEqual(latest(out, "2027-09-18T00:00:00+00:00")["freshness"]["status"], "stale")

    def test_missing_zero_invalid_and_negative_flow_are_distinct(self):
        self.assertIsNone(number("NaN")); self.assertIsNone(number("Infinity")); self.assertIsNone(number(True))
        self.assertEqual(number("0"), 0); self.assertEqual(number("-12.50"), Decimal("-12.50"))
        row = {"record_date": "2025-09-30", "debt_outstanding_amt": "null"}
        out = build("debt_outstanding", None, [page("debt_outstanding", [row])], NOW)
        self.assertIsNone(out["latest"]["current"])
        self.assertEqual(out["latest"]["freshness"]["status"], "unavailable")

    def test_full_identity_duplicates_and_provider_count_fail_closed(self):
        ds = "debt_to_penny"
        p = page(ds, [debt(), debt(total="999")])
        with self.assertRaisesRegex(ValueError, "conflicting"): build(ds, None, [p], NOW)
        p = page(ds, [debt()]); p["document"]["meta"]["count"] = 2
        with self.assertRaisesRegex(ValueError, "count"): build(ds, None, [p], NOW)
        with self.assertRaises(ValueError): build(ds, None, [page(ds, [debt("2026-09-19")])], NOW)
        row = {"record_date": "2026-08-31", "security_desc": "Notes", "avg_interest_rate_amt": "3"}
        with self.assertRaises(ValueError): build("avg_interest_rates", None, [page("avg_interest_rates", [row])], NOW)

    def test_deep_history_survives_current_refresh_and_old_acquisition_cannot_revert_new(self):
        ds = "debt_to_penny"
        old = build(ds, None, [page(ds, [debt("2020-01-02"), debt()], clock="2026-09-18T19:00:00+00:00")], NOW)
        new = build(ds, old, [page(ds, [debt(total="42")])], NOW)
        self.assertEqual(new["n_records"], 2)
        again = build(ds, new, [page(ds, [debt()], clock="2026-09-18T18:00:00+00:00")], NOW)
        self.assertEqual(again["latest"]["value_decimal"], "42")

    def test_provider_reversion_to_previously_archived_body_uses_new_acquisition_clock(self):
        ds = "debt_to_penny"
        old = build(ds, None, [page(ds, [debt(total="42")])], NOW)
        p = page(ds, [debt()], clock="2026-09-18T10:00:00+00:00")
        p["acquired_at"] = "2026-09-18T21:00:00+00:00"
        new = build(ds, old, [p], NOW)
        self.assertEqual(new["latest"]["value_decimal"], debt()["tot_pub_debt_out_amt"])

    def test_legacy_history_archived_but_never_promoted_and_no_sizing(self):
        old = {"observations": [{"date": "2020-01-02", "value": 123}]}
        proof = {"key": "retained-old-warehouse"}
        out = build("debt_to_penny", old, [page("debt_to_penny", [debt()])], NOW, proof)
        self.assertEqual(out["n_records"], 1)
        self.assertEqual(out["previous_evidence"], proof)
        self.assertEqual(out["legacy_rows_promoted"], 0)
        self.assertFalse(out["sizing_eligible"]); self.assertFalse(out["publication_time_verified"])
        self.assertEqual(out["latest"]["freshness"]["age_days"], 1)

    def test_source_identity_and_explicit_partial_query_coverage(self):
        p = page("debt_to_penny", [debt()], total=9000)
        out = build("debt_to_penny", None, [p], NOW)
        self.assertFalse(out["acquisition"][0]["query_complete"])
        p["receipt"]["provider"] = "other"
        with self.assertRaises(ValueError): build("debt_to_penny", None, [p], NOW)


if __name__ == "__main__": unittest.main()
