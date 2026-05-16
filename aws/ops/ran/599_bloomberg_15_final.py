"""ops/599 — Bloomberg 15/15 + Intelligence Dashboard FINAL VERIFY.

Captures all current state in one report:
  A) Sector-heatmap Lambda + sidecar
  B) ESI invoke + sidecar
  C) /intelligence/ page health + n_tabs/n_panes/n_renderers
  D) All 17 sidecar URLs HEAD check
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
        return {"_error": f"{type(e).__name__}: {str(e)[:150]}"}


def force_invoke(fname):
    try:
        r = lam.invoke(FunctionName=fname, InvocationType="RequestResponse",
                        Payload=b"{}", LogType="Tail")
        log = base64.b64decode(r.get("LogResult", b"")).decode("utf-8", errors="replace") if r.get("LogResult") else ""
        body = r["Payload"].read().decode("utf-8", errors="replace") if r.get("Payload") else ""
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                "response_preview": body[:500], "log_tail": log[-2000:]}
    except Exception as e:
        return {"status": "error", "err": str(e)[:300]}


def http_head(url, timeout=10):
    try:
        req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return {"status": r.status, "size_kb": round(int(r.headers.get("content-length", 0))/1024, 1),
                    "last_modified": r.headers.get("last-modified")}
    except urllib.error.HTTPError as e:
        return {"status": e.code, "err": str(e)[:120]}
    except Exception as e:
        return {"err": str(e)[:120]}


def http_get(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        return f"ERR: {e}"


def main():
    report = {"started": datetime.now(timezone.utc).isoformat()}

    # ============================================================
    # A. Sector-heatmap Lambda + ensure env + invoke + sidecar
    # ============================================================
    print("=== A: sector-heatmap Lambda ===")
    sh = {}
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-sector-heatmap")
        sh["exists"] = True
        sh["last_modified"] = cfg.get("LastModified")
        sh["memory"] = cfg.get("MemorySize")
        sh["timeout"] = cfg.get("Timeout")
        env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
        sh["env_keys"] = sorted(env.keys())
        # Patch env if needed
        need = []
        if "FMP_KEY" not in env: need.append("FMP_KEY")
        if "TELEGRAM_TOKEN" not in env: need.append("TELEGRAM_TOKEN")
        if "TELEGRAM_CHAT_ID" not in env: need.append("TELEGRAM_CHAT_ID")
        if need:
            env["FMP_KEY"] = get_param("/justhodl/fmp-key") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
            env["TELEGRAM_TOKEN"] = get_param("/justhodl/telegram/bot_token") or ""
            env["TELEGRAM_CHAT_ID"] = get_param("/justhodl/telegram/chat_id") or ""
            lam.update_function_configuration(FunctionName="justhodl-sector-heatmap",
                                              Environment={"Variables": env})
            time.sleep(4)
            sh["env_patched"] = need
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
                "top_sector_1d": (sc.get("sector_rank_1d", [{}])[0] or {}).get("sector"),
                "top_sector_return_1d": (sc.get("sector_rank_1d", [{}])[0] or {}).get("weighted_return_1d_pct"),
                "alerts": len(sc.get("alerts") or []),
                "alerts_preview": (sc.get("alerts") or [])[:4],
            }
    except Exception as e:
        sh["exists"] = False
        sh["preflight_err"] = str(e)[:200]
    report["A_sector_heatmap"] = sh

    # ============================================================
    # B. ESI re-invoke + sidecar
    # ============================================================
    print("=== B: ESI Lambda ===")
    e = {"invoke": force_invoke("justhodl-esi"),
         "sidecar": fetch_sidecar("data/esi.json")}
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
            "top_keys": list(sc.keys())[:12],
        }
    report["B_esi"] = e

    # ============================================================
    # C. /intelligence/ dashboard health
    # ============================================================
    print("=== C: /intelligence/ page ===")
    p = {}
    page_html = http_get("https://justhodl.ai/intelligence/")
    if isinstance(page_html, str) and not page_html.startswith("ERR"):
        p["status"] = 200
        p["size_kb"] = round(len(page_html)/1024, 1)
        import re
        tabs = re.findall(r'data-pane=["\']([^"\']+)["\']', page_html)
        panes = re.findall(r'id=["\']pane-([^"\']+)["\']', page_html)
        renders = re.findall(r'function render([A-Za-z][A-Za-z0-9_]*)\(', page_html)
        sidecar_refs = re.findall(r'data/([a-z0-9-]+\.json)', page_html)
        p["n_tabs"] = len(set(tabs))
        p["n_panes"] = len(set(panes))
        p["n_renders"] = len(set(renders))
        p["tabs"] = sorted(set(tabs))
        p["panes"] = sorted(set(panes))
        p["renders"] = sorted(set(renders))
        p["sidecar_refs"] = sorted(set(sidecar_refs))
    else:
        p["err"] = page_html
    report["C_page"] = p

    # ============================================================
    # D. All 17 sidecars HEAD check
    # ============================================================
    print("=== D: 17 sidecar URLs ===")
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

    # ============================================================
    # Summary
    # ============================================================
    report["summary"] = {
        "sector_heatmap_alive": (sh.get("exists") and
                                  sh.get("invoke", {}).get("status") == 200 and
                                  not sh.get("invoke", {}).get("fn_error")),
        "sector_heatmap_regime": (sh.get("summary") or {}).get("regime"),
        "esi_works": (e.get("summary", {}) or {}).get("n_events_60d", 0) > 0,
        "esi_regime": (e.get("summary", {}) or {}).get("regime"),
        "intelligence_n_tabs": p.get("n_tabs"),
        "intelligence_n_renders": p.get("n_renders"),
        "intelligence_size_kb": p.get("size_kb"),
        "sidecars_alive": f"{sc_present}/{sc_total}",
        "sidecars_missing": [k for k, v in sc_status.items() if v.get("status") != 200],
    }

    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/599_bloomberg_15_final.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nDONE -> aws/ops/reports/599_bloomberg_15_final.json")
    print(f"Summary: {json.dumps(report['summary'], indent=2)}")
    return report


if __name__ == "__main__":
    main()
