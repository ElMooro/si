"""1152 — retest CF proxy with cache busting + direct S3 access."""
import json, pathlib, time, random
from datetime import datetime, timezone
import urllib.request, urllib.error

REPORT = "aws/ops/reports/1152_cf_retest.json"


def test(url, label):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-1152/1.0",
                                                 "Cache-Control": "no-cache",
                                                 "Pragma": "no-cache"})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read()
            return {"label": label, "http": r.status, "elapsed_s": round(time.time()-t0, 2),
                    "size_kb": round(len(body)/1024, 1),
                    "cache_hit": r.headers.get("CF-Cache-Status") or r.headers.get("x-cache")}
    except urllib.error.HTTPError as e:
        return {"label": label, "http_error": e.code, "msg": e.reason,
                "elapsed_s": round(time.time()-t0, 2)}
    except Exception as e:
        return {"label": label, "error": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "tests": []}
    nonce = str(random.randint(100000, 999999))

    # 1. CF proxy with cache-bust query param
    for t in ["UBER", "CRWD", "ZM", "AAPL"]:
        url = f"https://justhodl-data-proxy.raafouis.workers.dev/equity-research/{t}.json?cb={nonce}"
        out["tests"].append(test(url, f"CF (cb={nonce}): {t}"))

    # 2. Direct S3 URL (bypasses CF entirely)
    for t in ["UBER", "CRWD", "ZM", "AAPL"]:
        url = f"https://justhodl-dashboard-live.s3.amazonaws.com/equity-research/{t}.json"
        out["tests"].append(test(url, f"S3 direct: {t}"))

    # 3. After cache-bust, normal CF URLs again (should now cache the 200)
    time.sleep(2)
    for t in ["UBER", "CRWD"]:
        url = f"https://justhodl-data-proxy.raafouis.workers.dev/equity-research/{t}.json"
        out["tests"].append(test(url, f"CF (post-bust): {t}"))

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1152] DONE")


if __name__ == "__main__":
    main()
