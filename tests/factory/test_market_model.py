"""The quantitative market model (scripts/factory_market_model.py): embargo, scale-free features, exam-identical grading."""
import random, runpy, sys, unittest
from datetime import date, timedelta
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'aws/shared')); sys.path.insert(0, str(ROOT / 'scripts'))
M = runpy.run_path(str(ROOT / 'scripts/factory_market_model.py'))
SEASON = {"flat_thresholds": {s: 0.003 for s in M["SYMBOLS"]}, "crisis_drawdown_thresholds": {s: 0.05 for s in M["SYMBOLS"]}, "weights": {"direction": 0.5, "regime": 0.3, "crisis": 0.2}}


def series(n, seed, start=date(2019, 6, 3)):
    r = random.Random(seed); px, vol, d, out = 100.0, 0.01, start, []
    for _ in range(n):
        vol = max(0.004, min(0.06, vol * (1 + r.gauss(0, 0.08))))
        o = px; c = px * (1 + r.gauss(0.0003, vol)); h = max(o, c) * 1.002; l = min(o, c) * 0.998
        while d.weekday() >= 5: d += timedelta(days=1)
        out.append({"date": d.isoformat(), "o": o, "h": h, "l": l, "c": c, "v": 1e6}); px = c; d += timedelta(days=1)
    return out


class MarketModelTests(unittest.TestCase):
    def test_no_training_window_touches_a_holdout_crisis_or_its_embargo(self):
        samples = M["windows_from_history"]({"SPY": series(700, 1)}, SEASON)          # 2019-06 .. 2022-02: spans covid-2020
        self.assertTrue(samples)
        self.assertFalse(any(M["embargoed"](d) for _, _, _, d in samples))
        self.assertTrue(M["embargoed"]("2020-03-01")); self.assertTrue(M["embargoed"]("2019-12-20")); self.assertFalse(M["embargoed"]("2019-11-01"))

    def test_features_are_scale_free(self):
        rows = series(20, 3)
        big = [{k: (v * 37.0 if k in ("o", "h", "l", "c") else v) for k, v in r.items()} for r in rows]
        a, b = M["features"](M["rebase"](rows)), M["features"](M["rebase"](big))
        self.assertEqual(len(a), len(M["FEATURES"]))
        self.assertTrue(all(abs(x - y) < 1e-9 for x, y in zip(a, b)))

    def test_train_predict_grade_round_trip_uses_the_exams_baselines(self):
        model = M["train"](M["windows_from_history"]({s: series(900, i, date(2010, 1, 4)) for i, s in enumerate(M["SYMBOLS"])}, SEASON))
        H = series(300, 42, date(2012, 1, 2)); drills = []
        for i in range(0, len(H) - 25, 5):
            win, fut = H[i:i + 20], H[i + 20:i + 25]
            lab = M["labels_from_prices"]("SPY", fut[0]["o"], [r["c"] for r in fut], SEASON)
            drills.append({"bars": M["rebase"](win), "labels": {"direction": lab["direction"], "regime": lab["regime"], "crisis": bool(lab["crisis"])}, "flat_threshold": 0.003})
        g = M["grade"](model, drills, SEASON)
        self.assertEqual(set(g), {"model", "prior", "momentum"}); self.assertEqual(g["model"]["n"], len(drills))
        p = model.predict(M["features"](drills[0]["bars"]))
        self.assertAlmostEqual(sum(p["direction_probabilities"].values()), 1.0, places=6); self.assertTrue(0.0 <= p["crisis_probability"] <= 1.0)


if __name__ == '__main__':
    unittest.main()
