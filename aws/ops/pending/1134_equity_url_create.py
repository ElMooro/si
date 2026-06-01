"""1134 — create function URL directly via boto3, then patch HTML + smoke test."""
import json
import pathlib
import time
import traceback
from datetime import datetime, timezone

import boto3
import urllib.request

REPORT = "aws/ops/reports/1134_equity_url_create.json"
FN = "justhodl-equity-research"
REGION = "us-east-1"

lam = boto3.client("lambda", region_name=REGION)


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
        return r
    except Exception as e:
        out["phases"].append({
            "name": name, "status": "ERROR",
            "error": str(e)[:400],
            "traceback": traceback.format_exc()[:1500],
        })
        return None


def list_existing_urls():
    """See if there's ANY URL config for this function, perhaps with a qualifier."""
    try:
        r = lam.list_function_url_configs(FunctionName=FN)
        return {"configs": r.get("FunctionUrlConfigs", [])}
    except Exception as e:
        return {"error": str(e)[:200]}


def ensure_url():
    """Idempotently create the function URL config."""
    try:
        existing = lam.get_function_url_config(FunctionName=FN)
        return {
            "action": "found_existing",
            "url": existing["FunctionUrl"],
            "auth_type": existing.get("AuthType"),
        }
    except lam.exceptions.ResourceNotFoundException:
        pass

    # Create it
    cors = {
        "AllowCredentials": False,
        "AllowHeaders":     ["*"],
        "AllowMethods":     ["GET", "POST", "OPTIONS"],
        "AllowOrigins":     ["*"],
        "ExposeHeaders":    ["*"],
        "MaxAge":           86400,
    }
    r = lam.create_function_url_config(
        FunctionName=FN,
        AuthType="NONE",
        Cors=cors,
    )
    return {
        "action": "created",
        "url":     r["FunctionUrl"],
        "auth_type": r.get("AuthType"),
        "creation_time": str(r.get("CreationTime")),
    }


def add_invoke_permission():
    """Allow public invocation of the function URL. Idempotent."""
    try:
        lam.add_permission(
            FunctionName=FN,
            StatementId="FunctionURLAllowPublicAccess",
            Action="lambda:InvokeFunctionUrl",
            Principal="*",
            FunctionUrlAuthType="NONE",
        )
        return {"added": True}
    except lam.exceptions.ResourceConflictException:
        return {"added": False, "reason": "permission already exists"}


def ensure_env_vars():
    """Make sure FMP_KEY is set in env (the code has a fallback but better to set it)."""
    r = lam.get_function_configuration(FunctionName=FN)
    env_vars = (r.get("Environment") or {}).get("Variables") or {}
    if "FMP_KEY" in env_vars:
        return {"changed": False, "current_keys": list(env_vars.keys())}

    # Add FMP_KEY
    env_vars["FMP_KEY"] = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
    lam.update_function_configuration(
        FunctionName=FN,
        Environment={"Variables": env_vars},
    )
    # Wait for update to settle
    waiter = lam.get_waiter("function_updated")
    waiter.wait(FunctionName=FN, WaiterConfig={"Delay": 2, "MaxAttempts": 30})
    return {"changed": True, "new_keys": list(env_vars.keys())}


def patch_html(url):
    p = pathlib.Path("why.html")
    src = p.read_text()
    placeholder = "https://CONFIGURE_AFTER_DEPLOY.lambda-url.us-east-1.on.aws/"
    if placeholder in src:
        new = src.replace(placeholder, url)
        p.write_text(new)
        return {"patched": True, "new_url": url, "occurrences": src.count(placeholder)}
    return {"patched": False, "reason": "placeholder already replaced or absent"}


def smoke(url, ticker="AAPL"):
    full = f"{url}?ticker={ticker}"
    req = urllib.request.Request(full, headers={"User-Agent": "ops-1134/1.0"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=180) as r:
        body = r.read()
        elapsed = round(time.time() - t0, 1)
    d = json.loads(body)
    metadata = d.get("metadata") or {}
    verdict = d.get("verdict") or {}
    company = d.get("company") or {}
    statements = d.get("statements") or {}
    quote = d.get("quote") or {}
    return {
        "status_code":   r.status,
        "elapsed_s":     elapsed,
        "size_kb":       round(len(body) / 1024, 1),
        "ticker":        d.get("ticker"),
        "company":       company.get("name"),
        "sector":        company.get("sector"),
        "price":         quote.get("price"),
        "from_cache":    d.get("from_cache"),
        "rating":        verdict.get("rating"),
        "conviction":    verdict.get("conviction_grade"),
        "price_target":  verdict.get("price_target_12m"),
        "upside_pct":    verdict.get("upside_pct"),
        "confidence":    verdict.get("confidence_pct"),
        "rationale":     (verdict.get("verdict_rationale") or "")[:200],
        "exec_summary_chars": len(d.get("executive_summary") or ""),
        "thesis_chars":  len(((d.get("investment_thesis") or {}).get("thesis_paragraph")) or ""),
        "risk_chars":    len(((d.get("risk_factors") or {}).get("risk_paragraph")) or ""),
        "n_catalysts":   len(d.get("catalysts_12m") or []),
        "n_triggers":    len(d.get("invalidation_triggers") or []),
        "income_years":  len(statements.get("income_annual") or []),
        "balance_years": len(statements.get("balance_annual") or []),
        "cf_years":      len(statements.get("cashflow_annual") or []),
        "claude_elapsed": metadata.get("claude_elapsed_sec"),
        "fmp_loaded":    f"{metadata.get('data_sources_loaded')}/{metadata.get('data_sources_total')}",
        "fmp_failed":    metadata.get("fmp_endpoints_failed"),
    }


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}

    phase(out, "list_existing_urls", list_existing_urls)
    phase(out, "ensure_env_vars",    ensure_env_vars)
    url_info = phase(out, "ensure_url", ensure_url)
    phase(out, "add_permission",     add_invoke_permission)

    if url_info and url_info.get("url"):
        url = url_info["url"]
        # Save URL to tracked file for future ops
        try:
            pathlib.Path(f"aws/lambdas/{FN}/.function-url").write_text(url + "\n")
        except Exception:
            pass

        phase(out, "patch_html", lambda: patch_html(url))
        # Quick wait for the URL to be globally propagated
        time.sleep(3)
        phase(out, "smoke_AAPL", lambda: smoke(url, "AAPL"))

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1134] DONE")


if __name__ == "__main__":
    main()
