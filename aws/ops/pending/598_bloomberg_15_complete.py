"""ops/598 — final post-deploy verify after Bloomberg 15/15 roadmap completion.

Verifies:
1. justhodl-sector-heatmap deployed + invokes + writes sidecar
2. ESI fix works (now reads by_indicator from macro-surprise)
3. /intelligence/ page returns 200 with all 17 tabs + 17 renderers
4. Quick sanity scan of all 16 sidecar URLs in the dashboard
"""
import json, os, time, base64, urllib.request
import boto3
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def get_param(name):
    try:
        return ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return None


def fetch_sidecar(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        return {"_error": f"{type(e).__name__}: {str(e)[:200]}"}


def force_invoke(fname):
    try:
        r = lam.invoke(FunctionName=fname, InvocationType="RequestResponse",
                        Payload=b"{}", LogType="Tail")
        log = base64.b64decode(r.get("LogResult", b"")).decode("utf-8", errors="replace") if r.get("LogResult") else ""
        body = r["Payload"].read().decode("utf-8", errors="replace") if r.get("Payload") else ""
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                "response_preview": body[:400], "log_tail": log[-1800:]}
    except Exception as e:
        return {"status": "error", "err": str(e)[:300]}


def http_head(url, timeout=10):
    try:
        req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return {"status": r.status, "size_kb": int(r.headers.get("content-length", 0))/1024}
    except Exception as e:
        return {"err": str(e)[:200]}


def http_get(url, timeout=10):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        return f"ERR: {e}"


def main():
    report = {"started": datetime.now(timezone.utc).isoformat()}

    # ============================================================
    # 1. Sector heatmap Lambda
    # ============================================================
    print("=== A: sector-heatmap ===")
    sh = {}
    # Wait up to 3 min for it to deploy
    found = False
    for _ in range(12):
        try:
            cfg = lam.get_function_configuration(FunctionName="justhodl-sector-heatmap")
            sh["last_modified"] = cfg.get("LastModified")
            sh["memory"] = cfg.get("MemorySize")
            sh["timeout"] = cfg.get("Timeout")
            cur_env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
            sh["env_keys"] = sorted(cur_env.keys())
            found = True
            break
        except Exception:
            time.sleep(15)
    sh["exists"] = found
    if found:
        # Ensure env vars present
        cur_env = (lam.get_function_configuration(FunctionName="justhodl-sector-heatmap")
                   .get("Environment") or {}).get("Variables", {}) or {}
        if "FMP_KEY" not in cur_env:
            fmp = get_param("/justhodl/fmp-key") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
            cur_env["FMP_KEY"] = fmp
            cur_env.setdefault("TELEGRAM_TOKEN", get_param("/justhodl/telegram/bot_token") or "")
            cur_env.setdefault("TELEGRAM_CHAT_ID", get_param("/justhodl/telegram/chat_id") or "")
            lam.update_function_configuration(FunctionName="justhodl-sector-heatmap",
                                              Environment={"Variables": cur_env})
            time.sleep(3)
            sh["env_patched"] = True
        sh["invoke"] = force_invoke("justhodl-sector-heatmap")
        sh["sidecar"] = fetch_sidecar("data/sector-heatmap.json")
        sc = sh["sidecar"]
        if isinstance(sc, dict) and "_error" not in sc:
            sh["sidecar_size_kb"] = round(len(json.dumps(sc, default=str))/1024, 1)
            mr = sc.get("market_regime", {})
            sh["summary"] = {
                "n_tickers_total": sc.get("n_tickers_total"),
                "n_with_1d": sc.get("n_tickers_with_1d"),
                "regime": mr.get("regime"),
                "breadth_pct": mr.get("breadth_pct"),
                "weighted_1d_pct": mr.get("weighted_return_1d_pct"),
                "n_sectors": len(sc.get("sectors") or {}),
                "top_sector": sc.get("sector_rank_1d", [{}])[0].get("sector"),
                "alerts": len(sc.get("alerts") or []),
            }
    report["A_sector_heatmap"] = sh

    # ============================================================
    # 2. ESI re-invoke
    # ============================================================
    print("=== B: ESI ===")
    e = {}
    e["invoke"] = force_invoke("justhodl-esi")
    e["sidecar"] = fetch_sidecar("data/esi.json")
    sc = e["sidecar"]
    if isinstance(sc, dict) and "_error" not in sc:
        e["summary"] = {
            "composite_60d": sc.get("composite_60d"),
            "composite_30d": sc.get("composite_30d"),
            "composite_7d": sc.get("composite_7d"),
            "regime": sc.get("regime"),
            "n_events_60d": sc.get("n_events_60d"),
            "n_events_30d": sc.get("n_events_30d"),
            "n_events_7d": sc.get("n_events_7d"),
            "err": sc.get("err"),
        }
    report["B_esi"] = e

    # ============================================================
    # 3. /intelligence/ page health
    # ============================================================
    print("=== C: /intelligence/ page ===")
    page = {}
    page_html = http_get("https://justhodl.ai/intelligence/")
    if isinstance(page_html, str) and not page_html.startswith("ERR"):
        page["status"] = "200"
        page["size_kb"] = round(len(page_html)/1024, 1)
        # Count tabs + renderers
        import re
        tabs = re.findall(r'class="tab( active)?" data-pane="([^"]+)"', page_html)
        panes = re.findall(r'class="tab-pane( active)?" id="pane-([^"]+)"', page_html)
        renders = re.findall(r'function render([A-Za-z]+)\(', page_html)
        page["n_tabs"] = len(tabs)
        page["n_panes"] = len(panes)
        page["n_renders"] = len(renders)
        page["renders"] = renders
    else:
        page["err"] = page_html
    report["C_page"] = page

    # ============================================================
    # 4. All sidecar URLs sanity
    # ============================================================
    print("=== D: all sidecar URLs ===")
    SIDECARS = [
        "data/khalid-adaptive.json", "data/stress-scenarios.json",
        "data/political-trades.json", "data/reversal-radar.json",
        "data/auction-grades.json", "data/repo-lending.json",
        "data/esi.json", "data/vol-surface.json",
        "data/market-internals.json", "data/sector-heatmap.json",
        "data/cds-proxy.json", "data/bond-trace.json",
        "data/tic-flows.json", "data/analyst-consensus.json",
        "data/sellside-views.json", "data/seasonality.json",
        "data/liquidity-profile.json",
    ]
    sc_status = {}
    for key in SIDECARS:
        url = f"https://justhodl-dashboard-live.s3.amazonaws.com/{key}"
        sc_status[key] = http_head(url)
    report["D_sidecars"] = sc_status

    sc_present = sum(1 for v in sc_status.values() if v.get("status") == 200)
    sc_total = len(sc_status)

    # Summary
    report["summary"] = {
        "sector_heatmap_alive": sh.get("exists") and sh.get("invoke",{}).get("status") == 200
                                   and not sh.get("invoke",{}).get("fn_error"),
        "esi_has_events": (e.get("summary",{}) or {}).get("n_events_60d",0) > 0,
        "intelligence_tabs": page.get("n_tabs"),
        "intelligence_renderers": page.get("n_renders"),
        "sidecars_present": f"{sc_present}/{sc_total}",
    }

    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/598_bloomberg_15_complete.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nDONE - wrote 598. Summary: {report['summary']}")
    return report


if __name__ == "__main__":
    main()
