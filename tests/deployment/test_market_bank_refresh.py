"""Pure-function checks for scripts/refresh_market_banks.py (no network, no AWS)."""
import os, sys, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))
import refresh_market_banks as m

BANK = {"bars": [[1790947800, 333.26, 334.54, 330.61, 333.69, 33245500.0]]}   # 2026-10-02 09:30 ET


class T(unittest.TestCase):
    def test_us_ticker(self):
        self.assertEqual(m.us_ticker("NASDAQ:AAPL"), "AAPL")
        self.assertEqual(m.us_ticker("NYSE:BRK.B"), "BRK.B")
        self.assertIsNone(m.us_ticker("TVC:DXY"))
        self.assertIsNone(m.us_ticker("NASDAQ:AAPL/NASDAQ:MSFT"))

    def test_session_stamp_matches_bank_convention(self):
        self.assertEqual(m.session_ts("2026-10-02"), 1790947800)
        self.assertEqual(m.session_ts("2026-12-01") % 86400, 14 * 3600 + 1800)   # EST: 14:30 UTC

    def test_appends_only_later_sessions(self):
        s = {"2026-10-02": {"AAPL": {"o": 1, "h": 2, "l": .5, "c": 1.5, "v": 1}},
             "2026-10-05": {"AAPL": {"o": 333, "h": 334.5, "l": 330, "c": 332.89, "v": 10}}}
        rows, why = m.plan_append(BANK, s, "AAPL")
        self.assertEqual(why, "append 1")
        self.assertEqual(rows, [[m.session_ts("2026-10-05"), 333.0, 334.5, 330.0, 332.89, 10.0]])

    def test_identity_guard_and_invalid_rows(self):
        s = {"2026-10-05": {"AAPL": {"o": 33, "h": 34, "l": 32, "c": 33.3, "v": 1}}}
        rows, why = m.plan_append(BANK, s, "AAPL")
        self.assertEqual(rows, []); self.assertTrue(why.startswith("identity guard"))
        bad = {"2026-10-05": {"AAPL": {"o": 333, "h": 300, "l": 330, "c": 332, "v": 1}}}   # high < open
        self.assertEqual(m.plan_append(BANK, bad, "AAPL")[0], [])

    def test_current_bank_is_left_alone(self):
        s = {"2026-10-02": {"AAPL": {"o": 333, "h": 335, "l": 330, "c": 333.69, "v": 1}}}
        self.assertEqual(m.plan_append(BANK, s, "AAPL"), ([], "current"))


if __name__ == "__main__":
    unittest.main()
