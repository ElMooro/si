"""
Debug FMP API access — figure out why tier-classifier got 0 fundamentals.
Tests:
  1. Direct FMP call to /profile/AAPL with the key
  2. Direct FMP call to /key-metrics-ttm/AAPL
  3. FMP API response shapes
  4. Lambda direct invoke with single ticker
"""
import json
import time
import urllib.request
import urllib.error
import os

REPORT = []


def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"- `{ts}`   {msg}")
    REPORT.append(f"- `{ts}`   {msg}")


def section(title):
    print(f"\n# {title}\n")
    REPORT.append(f"\n# {title}\n")


FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"


def test_fmp(path, params=None, timeout=15):
    qs = f"apikey={FMP_KEY}"
    if params:
        for k, v in params.items():
            qs += f"&{k}={v}"
    url = f"https://financialmodelingprep.com/api/v3{path}?{qs}"
    log(f"GET {path}{('?' + '&'.join(f'{k}={v}' for k,v in (params or {}).items())) if params else ''}")

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-fmp-debug/1.0"})
        started = time.time()
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            txt = resp.read().decode("utf-8", errors="replace")
            dur = round((time.time() - started) * 1000, 0)
            log(f"  → status={resp.status} duration={dur}ms size={len(txt)}b")
            try:
                data = json.loads(txt)
                if isinstance(data, list):
                    log(f"  → list with {len(data)} items")
                    if data:
                        log(f"  → first item keys: {sorted(data[0].keys())[:15] if isinstance(data[0], dict) else data[0]}")
                        if isinstance(data[0], dict):
                            for k in ["symbol", "companyName", "mktCap", "price", "peRatioTTM", "priceToSalesRatioTTM", "freeCashFlowYieldTTM"]:
                                if k in data[0]:
                                    log(f"     {k}: {data[0][k]}")
                elif isinstance(data, dict):
                    log(f"  → dict keys: {sorted(data.keys())[:10]}")
                    if "Error Message" in data:
                        log(f"  → ERROR: {data['Error Message']}")
                return data
            except json.JSONDecodeError:
                log(f"  → not JSON, first 200 chars: {txt[:200]}")
                return None
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:400]
        log(f"  → HTTPError {e.code}: {body[:300]}")
        return None
    except urllib.error.URLError as e:
        log(f"  → URLError: {e.reason}")
        return None
    except Exception as e:
        log(f"  → {type(e).__name__}: {e}")
        return None


def main():
    section("1) FMP — fetch /profile/AAPL")
    profile = test_fmp("/profile/AAPL")

    section("2) FMP — fetch /key-metrics-ttm/AAPL")
    ktm = test_fmp("/key-metrics-ttm/AAPL", params={"limit": "1"})

    section("3) FMP — fetch /ratios-ttm/AAPL")
    rtm = test_fmp("/ratios-ttm/AAPL")

    section("4) FMP — try /profile/MU (memory leader)")
    mu_profile = test_fmp("/profile/MU")

    section("5) FMP — try v3 /quote/MU")
    quote = test_fmp("/quote/MU")

    section("6) Try fetching through the deployed Lambda — /tmp/AAPL invoke")
    import boto3
    lam = boto3.client("lambda", region_name="us-east-1")
    # We can't easily trigger fetch_fundamentals('AAPL') without a custom payload,
    # but we can check the Lambda's environment to see if FMP_KEY is set correctly.
    cfg = lam.get_function_configuration(FunctionName="justhodl-theme-tier-classifier")
    env_vars = cfg.get("Environment", {}).get("Variables", {})
    log(f"Lambda env vars: {list(env_vars.keys())}")
    log(f"  FMP_KEY length: {len(env_vars.get('FMP_KEY', ''))}")
    log(f"  FMP_KEY starts: {env_vars.get('FMP_KEY', '')[:8]}...")

    section("7) Inspect tier-classifier source — fetch_fundamentals function")
    # Read the deployed code via update test
    import io, zipfile
    import urllib.request as ur
    code_url_resp = lam.get_function(FunctionName="justhodl-theme-tier-classifier")
    code_url = code_url_resp["Code"]["Location"]
    log(f"Lambda code url retrieved ({len(code_url)} chars)")
    with ur.urlopen(code_url, timeout=20) as resp:
        zip_bytes = resp.read()
    log(f"Lambda code zip size: {len(zip_bytes):,}b")
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        with z.open("lambda_function.py") as f:
            code = f.read().decode("utf-8")
    # Find fetch_fundamentals
    if "fetch_fundamentals" in code:
        idx = code.index("def fetch_fundamentals")
        snippet = code[idx:idx+1500]
        log("── deployed fetch_fundamentals ──")
        for line in snippet.split("\n")[:40]:
            log(f"  {line}")


if __name__ == "__main__":
    main()
    out_dir = "aws/ops/reports/latest"
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "debug_fmp_tier_classifier.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("\n[report written]")
