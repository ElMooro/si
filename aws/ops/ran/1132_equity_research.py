"""1132 — discover equity-research Lambda URL, smoke-test it, patch why.html."""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import boto3
import urllib.request

REPORT = "aws/ops/reports/1132_equity_research.json"
lam = boto3.client("lambda", region_name="us-east-1")
s3  = boto3.client("s3", region_name="us-east-1")


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
        return r
    except Exception as e:
        out["phases"].append({"name": name, "status": "ERROR", "error": str(e)[:300],
                                "traceback": traceback.format_exc()[:1500]})
        return None


def get_lambda_url():
    """Fetch the function URL for the new Lambda."""
    r = lam.get_function_url_config(FunctionName="justhodl-equity-research")
    return {"url": r["FunctionUrl"], "auth_type": r.get("AuthType")}


def get_function_config():
    """Verify ANTHROPIC_API_KEY is set."""
    r = lam.get_function_configuration(FunctionName="justhodl-equity-research")
    env = (r.get("Environment") or {}).get("Variables") or {}
    return {
        "runtime":       r.get("Runtime"),
        "memory":        r.get("MemorySize"),
        "timeout":       r.get("Timeout"),
        "env_keys":      list(env.keys()),
        "has_anthropic": "ANTHROPIC_API_KEY" in env,
        "has_fmp":       "FMP_KEY" in env,
    }


def smoke_test(url, ticker="AAPL"):
    """Hit the Lambda URL with a real ticker; measure latency + check key fields."""
    full_url = f"{url}?ticker={ticker}"
    req = urllib.request.Request(full_url, headers={"User-Agent": "ops-1132-smoke/1.0"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=180) as r:
        body = r.read()
        elapsed = round(time.time() - t0, 1)
    d = json.loads(body)
    return {
        "elapsed_s":   elapsed,
        "size_bytes":  len(body),
        "status_code": r.status,
        "ticker":      d.get("ticker"),
        "company":     (d.get("company") or {}).get("name"),
        "rating":      (d.get("verdict") or {}).get("rating"),
        "conviction":  (d.get("verdict") or {}).get("conviction_grade"),
        "price_target":(d.get("verdict") or {}).get("price_target_12m"),
        "upside_pct":  (d.get("verdict") or {}).get("upside_pct"),
        "from_cache":  d.get("from_cache"),
        "has_exec_summary":   bool(d.get("executive_summary")),
        "has_thesis":         bool((d.get("investment_thesis") or {}).get("thesis_paragraph")),
        "has_risk":           bool((d.get("risk_factors") or {}).get("risk_paragraph")),
        "has_valuation":      bool(d.get("valuation_assessment")),
        "has_health":         bool(d.get("financial_health_summary")),
        "n_catalysts":        len(d.get("catalysts_12m") or []),
        "n_triggers":         len(d.get("invalidation_triggers") or []),
        "income_years":       len((d.get("statements") or {}).get("income_annual") or []),
        "balance_years":      len((d.get("statements") or {}).get("balance_annual") or []),
        "cashflow_years":     len((d.get("statements") or {}).get("cashflow_annual") or []),
        "claude_elapsed":     (d.get("metadata") or {}).get("claude_elapsed_sec"),
        "fmp_endpoints_ok":   (d.get("metadata") or {}).get("data_sources_loaded"),
        "fmp_endpoints_total":(d.get("metadata") or {}).get("data_sources_total"),
        "fmp_endpoints_failed": (d.get("metadata") or {}).get("fmp_endpoints_failed"),
    }


def patch_why_html(url):
    """Replace the CONFIGURE_AFTER_DEPLOY placeholder with the actual URL."""
    path = pathlib.Path("why.html")
    src = path.read_text()
    placeholder = "https://CONFIGURE_AFTER_DEPLOY.lambda-url.us-east-1.on.aws/"
    if placeholder not in src:
        return {"patched": False, "reason": "placeholder not found"}
    new = src.replace(placeholder, url)
    path.write_text(new)
    return {"patched": True, "new_url": url}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}

    cfg = phase(out, "function_config", get_function_config)
    url_info = phase(out, "function_url", get_lambda_url)

    if url_info and "url" in url_info:
        # Patch the HTML first so subsequent commit can deploy it
        phase(out, "patch_why_html", lambda: patch_why_html(url_info["url"]))
        # Now smoke test
        phase(out, "smoke_test_AAPL", lambda: smoke_test(url_info["url"], "AAPL"))

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1132] DONE")


if __name__ == "__main__":
    main()
