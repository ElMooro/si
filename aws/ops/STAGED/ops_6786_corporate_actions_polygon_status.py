"""ops 6786 - READ-ONLY: which HTTP status is Polygon returning to justhodl-corporate-actions? (daily health run 2026-11-11)

The engine has failed every run since Nov 9 (ops 6767: PARTLY_FAILING; ops 6785: three invocations a day, every one an
error, today's a 300 s timeout) and its log says only `[corpact] http failed https://api.polygon.io/v3/reference/splits:
HTTPError` - the status code is never printed, so the health runs have carried "needs reading from the account" for
three days. This op reads it, without printing any secret:

  1. resolves the key exactly as the engine does (managed_secret: env POLYGON_API_KEY / POLYGON_KEY / POLY_KEY on the
     function, then SSM /justhodl/polygon/api-key) - from the FUNCTION'S configuration (names only, never values), and
     says whether an env value equals the SSM value (boolean);
  2. issues the engine's three request shapes for one ticker (AAPL: v3 splits, v3 dividends, v2 unadjusted aggs for the
     last session) with the engine's own User-Agent, and prints status code + the body's `status` / `error` / `message`
     fields (the key is redacted from every string before it is written);
  3. for contrast, the same splits call with the key the polygon-bank engine resolves (if it differs), so a dead key is
     distinguished from a plan that lacks the endpoint;
  4. the function's last 6 invocations (duration, status) from CloudWatch Logs REPORT lines.
Invokes nothing; changes nothing; writes nothing but this report.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import re
import sys
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))

REGION = "us-east-1"
FN = "justhodl-corporate-actions"
CONTRAST_FN = "justhodl-polygon-bank"
ENV_NAMES = ("POLYGON_API_KEY", "POLYGON_KEY", "POLY_KEY")
SSM_NAME = "/justhodl/polygon/api-key"
UA = "justhodl.ai/1.0 (corporate-actions; contact: ops@justhodl.ai)"   # the engine's own
TICKER = "AAPL"
SPLITS = "https://api.polygon.io/v3/reference/splits?ticker=%s&limit=1000&apiKey=%s"
DIVS = "https://api.polygon.io/v3/reference/dividends?ticker=%s&limit=1000&apiKey=%s"
AGGS = "https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s?adjusted=false&limit=5&apiKey=%s"


def scrub(text, secrets):
    """Every secret value replaced by *** - pure, for the selftest."""
    out = str(text)
    for s in secrets:
        if s:
            out = out.replace(s, "***")
    return re.sub(r"(?i)(apiKey=)[^&\s\"']+", r"\1***", out)


def probe(url, secrets):
    """(status, summary) for one GET; the body is summarised, never dumped."""
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=25) as resp:
            body = resp.read().decode("utf-8", "replace")
            code = resp.status
    except urllib.error.HTTPError as e:
        code = e.code
        try:
            body = e.read().decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            body = ""
    except Exception as exc:  # noqa: BLE001
        return None, "%s: %s" % (type(exc).__name__, scrub(str(exc), secrets)[:160])
    try:
        doc = json.loads(body or "{}")
        summary = {k: doc.get(k) for k in ("status", "error", "message", "resultsCount", "count", "queryCount") if k in doc}
        if isinstance(doc.get("results"), list):
            summary["results"] = len(doc["results"])
    except ValueError:
        summary = {"body": body[:160]}
    return code, scrub(json.dumps(summary, default=str), secrets)[:300]


def selftest():
    assert scrub("x?ticker=AAPL&apiKey=abc123&limit=1", ["abc123"]) == "x?ticker=AAPL&apiKey=***&limit=1"
    assert scrub("key abc123 end", ["abc123"]) == "key *** end"
    assert scrub("apiKey=zzz", []) == "apiKey=***"
    assert scrub("plain", [""]) == "plain"
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=10, read_timeout=60)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    ssm = boto3.client("ssm", region_name=REGION, config=cfg)
    logs = boto3.client("logs", region_name=REGION, config=cfg)
    secrets = []
    facts = {}
    with report("ops_6786_corporate_actions_polygon_status") as r:
        r.heading("ops 6786 - the HTTP status Polygon returns to justhodl-corporate-actions (read-only, no secret printed)")

        r.section("1. the key the engine resolves (names and equality only)")
        try:
            ssm_val = ssm.get_parameter(Name=SSM_NAME, WithDecryption=True)["Parameter"]["Value"]
        except Exception as exc:  # noqa: BLE001
            ssm_val = ""
            r.warn("SSM %s unreadable from the runner: %s" % (SSM_NAME, type(exc).__name__))
        secrets.append(ssm_val)
        r.log("SSM %s: %s (%d chars)" % (SSM_NAME, "present" if ssm_val else "EMPTY", len(ssm_val)))
        engine_key = ""
        try:
            conf = lam.get_function_configuration(FunctionName=FN)
            env = (conf.get("Environment") or {}).get("Variables") or {}
            present = [n for n in ENV_NAMES if env.get(n)]
            r.log("function env has %d variables; Polygon names present: %s | timeout %ss memory %sMB | runtime %s | last modified %s | state %s/%s" % (
                len(env), present or "none", conf.get("Timeout"), conf.get("MemorySize"), conf.get("Runtime"), conf.get("LastModified"),
                conf.get("State"), conf.get("LastUpdateStatus")))
            for n in present:
                secrets.append(env[n])
                r.log("  env %s equals SSM: %s (%d chars)" % (n, env[n] == ssm_val, len(env[n])))
            engine_key = next((env[n] for n in ENV_NAMES if env.get(n)), "") or ssm_val   # managed_secret order: env first, then SSM
            facts["engine_key_source"] = "env:%s" % present[0] if present else ("ssm" if ssm_val else "NONE")
        except Exception as exc:  # noqa: BLE001
            r.warn("get_function_configuration %s: %s" % (FN, type(exc).__name__))
        contrast_key = ""
        try:
            cenv = (lam.get_function_configuration(FunctionName=CONTRAST_FN).get("Environment") or {}).get("Variables") or {}
            contrast_key = next((cenv[n] for n in ENV_NAMES if cenv.get(n)), "") or ssm_val
            secrets.append(contrast_key)
            r.log("%s resolves %s; same key as the engine: %s" % (CONTRAST_FN, "an env key" if any(cenv.get(n) for n in ENV_NAMES) else "the SSM key",
                                                                 contrast_key == engine_key))
            facts["same_key_as_polygon_bank"] = contrast_key == engine_key
        except Exception as exc:  # noqa: BLE001
            r.warn("get_function_configuration %s: %s" % (CONTRAST_FN, type(exc).__name__))
        if not engine_key:
            r.fail("the engine resolves NO Polygon key (no env name set, SSM empty) - every call would be a 401")
            facts["engine_key_source"] = "NONE"

        r.section("2. the engine's three request shapes for %s" % TICKER)
        day = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        while day.weekday() >= 5:
            day -= timedelta(days=1)
        shapes = (("v3 splits", SPLITS % (TICKER, engine_key)), ("v3 dividends", DIVS % (TICKER, engine_key)),
                  ("v2 aggs %s unadjusted" % day, AGGS % (TICKER, day, day, engine_key)))
        codes = {}
        for label, url in shapes:
            code, summary = probe(url, secrets)
            codes[label] = code
            (r.log if code == 200 else r.warn)("%s -> HTTP %s | %s" % (label, code, summary))
        facts["splits_status"] = codes.get("v3 splits")
        facts["dividends_status"] = codes.get("v3 dividends")
        facts["aggs_status"] = codes.get("v2 aggs %s unadjusted" % day)

        r.section("3. contrast: the same splits call with the key polygon-bank resolves")
        if contrast_key and contrast_key != engine_key:
            code, summary = probe(SPLITS % (TICKER, contrast_key), secrets)
            r.log("polygon-bank's key, v3 splits -> HTTP %s | %s" % (code, summary))
            facts["contrast_splits_status"] = code
        else:
            r.log("same key as the engine (or unreadable) - the section-2 answer stands for both")

        r.section("4. the engine's last invocations (REPORT lines)")
        try:
            streams = logs.describe_log_streams(logGroupName="/aws/lambda/" + FN, orderBy="LastEventTime", descending=True, limit=4).get("logStreams", [])
            n = 0
            for s_ in streams:
                for e in logs.get_log_events(logGroupName="/aws/lambda/" + FN, logStreamName=s_["logStreamName"], limit=60, startFromHead=False).get("events", []):
                    m = e["message"]
                    if m.startswith("REPORT RequestId") and n < 6:
                        n += 1
                        stamp = datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).strftime("%m-%d %H:%M")
                        dur = re.search(r"Duration: ([\d.]+) ms", m)
                        st = re.search(r"Status: (\w+)", m)
                        r.log("  %s duration %.0f s%s" % (stamp, float(dur.group(1)) / 1000 if dur else -1, " status " + st.group(1) if st else ""))
        except Exception as exc:  # noqa: BLE001
            r.warn("logs: %s: %s" % (type(exc).__name__, scrub(str(exc), secrets)[:120]))

        r.kv(**{k: str(v) for k, v in facts.items()})
        bad = [k for k, v in codes.items() if v != 200]
        if bad:
            r.warn("Polygon answers %s for the engine's key - the status codes above are the finding; nothing was changed" % {k: codes[k] for k in bad})
        else:
            r.ok("all three shapes answer 200 for the engine's key - the failure is not the key or the plan; look at pacing / the universe size")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
