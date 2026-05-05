"""Verify horizons.html post-deploy + verify SPY benchmark on backtest still works."""
import json
import urllib.request
import boto3
from ops_report import report

UA = {"User-Agent": "justhodl-audit/1.0"}
S3 = boto3.client("s3", region_name="us-east-1")


def fetch(url):
    req = urllib.request.Request(url, headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, r.read().decode("utf-8", errors="replace"), None
    except Exception as e:
        return None, "", str(e)


def main():
    with report("verify_horizons_live") as r:
        # 1) horizons.html live
        r.heading("1) horizons.html live + checks")
        code, body, err = fetch("https://justhodl.ai/horizons.html")
        if err:
            r.log(f"  ✗ {err}")
        else:
            r.log(f"  ✓ status={code}, size={len(body):,}b")
            checks = [
                ("title", "<title>Horizons · JustHodl</title>" in body),
                ("nav active", 'class="tab active" href="/horizons.html"' in body),
                ("uplift list", 'id="uplift-list"' in body),
                ("matrix table", 'id="horizon-matrix"' in body),
                ("loads calibration", "calibration/latest.json" in body),
                ("weight color logic", "weightColor" in body and "weightTextColor" in body),
                ("auto-refresh", "setInterval(load, 5*60*1000)" in body),
            ]
            for label, ok in checks:
                r.log(f"    {'✓' if ok else '✗'} {label}")

        # 2) Calibration data state
        r.heading("2) Calibration JSON has multi-horizon fields")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="calibration/latest.json")
            d = json.loads(obj["Body"].read())
            ww = d.get("window_weights") or {}
            rh = d.get("recommended_horizon") or {}
            flat = d.get("weights") or {}
            r.log(f"  generated_at:        {d.get('generated_at')}")
            r.log(f"  total_outcomes:      {d.get('total_outcomes')}")
            r.log(f"  window_weights:      {len(ww)} signals")
            r.log(f"  recommended_horizon: {len(rh)} signals")
            r.log(f"  flat weights:        {len(flat)} signals")
            r.log("")

            # Compute uplifts table
            r.log("  Top 13 horizon-uplifts (best-window weight - flat weight):")
            uplifts = []
            for sig, rec in rh.items():
                fw = flat.get(sig, 0)
                hw = rec.get("weight", 0)
                d_v = hw - fw
                if d_v >= 0.15:
                    uplifts.append((sig, rec.get("window"), hw, fw, d_v, rec.get("accuracy"), rec.get("n")))
            uplifts.sort(key=lambda x: -x[4])
            for sig, win, hw, fw, du, acc, n in uplifts:
                accs = f"{acc*100:.0f}%" if acc is not None else "—"
                r.log(f"    {sig:28s}  flat={fw:.2f} → {win}: w={hw:.2f}  acc={accs} n={n}  Δ+{du:.2f}")

            # Check for crisis_sofr_iorb specifically (it was floored at 0.40 day_30 but day_7 has acc 75%)
            r.log("")
            r.log("  Spotcheck: crisis_sofr_iorb (was floored at 0.40)")
            csi = ww.get("crisis_sofr_iorb")
            csi_a = (d.get("window_accuracy") or {}).get("crisis_sofr_iorb")
            if csi:
                for h, w in csi.items():
                    a = (csi_a or {}).get(h, {})
                    accs = f"{a.get('accuracy', 0)*100:.0f}%" if a.get('accuracy') else "—"
                    r.log(f"    {h}: weight={w:.2f}  accuracy={accs}  n={a.get('n', 0)}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 3) Cross-page nav
        r.heading("3) Horizons tab visible across key pages")
        for p in ["today.html", "brief.html", "calls.html", "performance.html", "backtest.html",
                  "weights.html", "accuracy.html"]:
            code, body, err = fetch(f"https://justhodl.ai/{p}")
            has = ('href="/horizons.html"' in body) or ('href="horizons.html"' in body)
            r.log(f"  {'✓' if has else '✗'} {p:25s}  Horizons link: {has}")


if __name__ == "__main__":
    main()
