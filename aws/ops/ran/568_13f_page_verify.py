#!/usr/bin/env python3
"""568 — Verify /13f.html upgrades land via GH Pages."""
import io, json, os, time as _time, urllib.request
from datetime import datetime, timezone

REPORT = "aws/ops/reports/568_13f_page_verify.json"


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # GH Pages takes 30-90s to rebuild. Allow time.
    _time.sleep(60)

    try:
        req = urllib.request.Request("https://justhodl.ai/13f.html",
                                       headers={"User-Agent": "JustHodl.AI ops/568",
                                                  "Cache-Control": "no-cache, no-store"})
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", "replace")
        out["size"] = len(html)
        checks = {
            "action_spotlight_section": "Action Spotlight" in html,
            "famous_fund_section": "Famous-Fund Activity" in html,
            "spotlight_grid_div": 'id="spotlight"' in html,
            "famous_funds_div": 'id="famous-funds"' in html,
            "modal_backdrop": 'id="modal-backdrop"' in html,
            "modal_table": 'fund-act-tbl' in html,
            "render_spotlight_fn": "function renderSpotlight" in html,
            "render_famous_fn": "function renderFamousFunds" in html,
            "openTicker_fn": "function openTicker" in html,
            "closeModal_fn": "function closeModal" in html,
            "bs_score_class": "bs-score" in html,
            "famous_funds_array": "FAMOUS_FUNDS" in html and "BERKSHIRE" in html and "SCION" in html,
            "load_calls_spotlight": "renderSpotlight(data)" in html,
            "load_calls_famous": "renderFamousFunds(data.by_fund)" in html,
            "loads_13f_sidecar": "data/13f-positions.json" in html,
            "drill_down_kbd_esc": "if(e.key === 'Escape')" in html,
        }
        out["checks"] = checks
        out["all_passed"] = all(checks.values())
        # Count CSS rules for new sections to spot-check they shipped
        out["spot_css_present"] = ".spot{" in html or ".spot.buy" in html
        out["famous_css_present"] = ".famous-card" in html
        out["modal_css_present"] = ".modal-backdrop" in html
        # Count <section> tags
        out["section_count"] = html.count("<section ")
    except Exception as e:
        out["err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
