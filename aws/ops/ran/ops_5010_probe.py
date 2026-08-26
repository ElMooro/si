"""ops_5010_probe -- print the real generated AAOI/NVDA quant numbers for
eyeball verification of ops 5010 (idempotent, read-only)."""
import json
import sys
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

s3c = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"

with report("ops_5010_probe") as rep:
    rep.heading("ops 5010 probe -- real generated numbers")
    for t in ("AAOI", "NVDA"):
        doc = json.loads(s3c.get_object(
            Bucket=B, Key="equity-research/%s.json" % t)["Body"].read())
        pa = doc.get("price_analytics") or {}
        rk = pa.get("risk") or {}
        cls = doc.get("classification") or {}
        ind = doc.get("industry_growth") or {}
        hz = (pa.get("expectations") or {}).get("horizons") or {}
        rep.section(t)
        rep.log("generated_at=%s schema=%s price=%s history_years=%s"
                % (doc.get("generated_at"), doc.get("schema_version"),
                   pa.get("last_price"), pa.get("history_years")))
        rep.log("beta_2y=%s corr=%s vol30=%s vol1y=%s ddown1y=%s dd5y=%s"
                % (rk.get("beta_2y"), rk.get("spy_corr_2y"),
                   rk.get("vol_30d_pct"), rk.get("vol_1y_pct"),
                   rk.get("max_dd_1y_pct"), rk.get("max_dd_5y_pct")))
        for k in ("ema200", "ema250"):
            e = pa.get(k) or {}
            rep.log("%s value=%s dist=%s%% above=%s slope20d=%s%%"
                    % (k, e.get("value"), e.get("dist_pct"),
                       e.get("above"), e.get("slope_20d_pct")))
        rep.log("cap=%s (%s) style=%s blue=%s score=%s"
                % (cls.get("cap_tier"), cls.get("market_cap"),
                   cls.get("style"), cls.get("is_blue_chip"),
                   cls.get("blue_chip_score")))
        for c in cls.get("checklist") or []:
            rep.log("  [%s] %s -- %s" % ("PASS" if c.get("pass") else
                                         "fail", c.get("name"),
                                         c.get("detail")))
        for h in ("1M", "3M", "1Y"):
            e = hz.get(h) or {}
            rep.log("%s: n=%s median=%s p10=%s p90=%s win=%s vol1s=%s"
                    % (h, e.get("n_windows"), e.get("median_pct"),
                       e.get("p10_pct"), e.get("p90_pct"),
                       e.get("win_rate_pct"),
                       e.get("vol_implied_1sigma_pct")))
        rep.log("industry avail=%s peers=%s ind_vol=%s ind_dd=%s q_pts=%s"
                % (ind.get("available"), ind.get("peers"),
                   ((ind.get("risk_compare") or {}).get("industry_median")
                    or {}).get("vol_1y_pct"),
                   ((ind.get("risk_compare") or {}).get("industry_median")
                    or {}).get("max_dd_1y_pct"),
                   len(ind.get("stock_quarters") or [])))
        for q in (ind.get("stock_quarters") or [])[-4:]:
            rep.log("  %s yoy=%s qoq=%s" % (q.get("q"), q.get("yoy"),
                                            q.get("qoq")))
    rep.ok("probe complete")
