"""
ops_3888 — redeploy news-wire + news-sentiment with the CORRECTED inherit_env
source. ops 3886 pointed both at justhodl-confluence-meta based on a
deploy-lambdas.yml comment calling it "the known-good source of all keys" —
ops 3887 proved that comment wrong: confluence-meta's live environment has
only FMP_KEY/FRED_KEY/POLYGON_KEY, zero Anthropic key. justhodl-equity-research
has a real 108-char key (confirmed the same run), and justhodl-flows-ai-analysis
— the actually-proven-working reference engine — is live right now BECAUSE it
inherits from equity-research, not confluence-meta. This ops re-points both
fixes there and re-gates on the same falsifiable claims 3886 used.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=890, retries={"max_attempts": 0}))
logs = boto3.client("logs", region_name="us-east-1")


def wait_for_env_key(fn_name, env_key, rep, tries=30, sleep_s=15):
    for attempt in range(1, tries + 1):
        try:
            cfg = lam.get_function_configuration(FunctionName=fn_name)
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            val = env.get(env_key, "")
            if val and "PLACEHOLDER" not in val and len(val) > 20:
                rep.ok(f"  {fn_name}: {env_key} is live and non-placeholder "
                       f"(len={len(val)}) on attempt {attempt}")
                return True
            rep.log(f"  {fn_name}: attempt {attempt}, "
                    f"{env_key}={'empty' if not val else 'still placeholder/short'}")
        except Exception as e:
            rep.log(f"  {fn_name}: attempt {attempt}, {str(e)[:90]}")
        time.sleep(sleep_s)
    rep.fail(f"  {fn_name}: {env_key} never became a real live value after {tries} attempts")
    return False


def zip_settle(fn_name, marker, rep, tries=30, sleep_s=15):
    for attempt in range(1, tries + 1):
        try:
            loc = lam.get_function(FunctionName=fn_name)["Code"]["Location"]
            blob = urllib.request.urlopen(loc, timeout=60).read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                src = z.read("lambda_function.py").decode("utf-8", "ignore")
            if marker in src:
                rep.ok(f"  {fn_name}: new artifact live on attempt {attempt}")
                return True
        except Exception as e:
            rep.log(f"  {fn_name}: attempt {attempt}, {str(e)[:90]}")
        time.sleep(sleep_s)
    rep.fail(f"  {fn_name}: deploy never landed")
    return False


def s3_snapshot(key):
    try:
        o = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"]
    except Exception:
        return None, None


def recent_log_text(fn_name, n=60):
    try:
        streams = logs.describe_log_streams(
            logGroupName=f"/aws/lambda/{fn_name}",
            orderBy="LastEventTime", descending=True, limit=1)["logStreams"]
        if not streams:
            return ""
        events = logs.get_log_events(
            logGroupName=f"/aws/lambda/{fn_name}",
            logStreamName=streams[0]["logStreamName"], limit=n)["events"]
        return "\n".join(e["message"] for e in events)
    except Exception:
        return ""


def main():
    with report("3888_anthropic_key_redeploy") as rep:
        rep.heading("ops 3888 — redeploy with the CORRECTED inherit_env source (equity-research, verified real)")

        rep.section("1. news-wire — env-key check against the corrected source")
        if wait_for_env_key("justhodl-news-wire", "ANTHROPIC_API_KEY", rep):
            lam.invoke(FunctionName="justhodl-news-wire", InvocationType="RequestResponse", Payload=b"{}")
            time.sleep(3)
            log_nw = recent_log_text("justhodl-news-wire")
            rep.log(f"  recent log tail: {log_nw[-1200:]}")
            nw_ok = "401" not in log_nw and "Unauthorized" not in log_nw
        else:
            nw_ok, log_nw = False, ""

        rep.section("2. news-sentiment — zip-settle + env-key + invoke + gate on real scoring")
        settled_ns = zip_settle("justhodl-news-sentiment", 'os.environ.get("ANTHROPIC_API_KEY"', rep)
        env_ok_ns = wait_for_env_key("justhodl-news-sentiment", "ANTHROPIC_API_KEY", rep) if settled_ns else False
        ns_after = None
        if settled_ns and env_ok_ns:
            _, blm_ns = s3_snapshot("sentiment/data.json")
            lam.invoke(FunctionName="justhodl-news-sentiment", InvocationType="Event", Payload=b"{}")
            for attempt in range(1, 25):
                time.sleep(15)
                doc, lm = s3_snapshot("sentiment/data.json")
                if doc and (blm_ns is None or lm > blm_ns):
                    ns_after = doc
                    rep.ok(f"  sentiment/data.json rewritten on attempt {attempt}")
                    break
            log_ns = recent_log_text("justhodl-news-sentiment")
            rep.log(f"  recent log tail: {log_ns[-1200:]}")
            ns_400_free = "400" not in log_ns.split("claude err")[-1][:50] if "claude err" in log_ns else True
        else:
            log_ns, ns_400_free = "", False

        rep.section("3. THE HARD GATE")
        checks = [("news-wire: no 401/Unauthorized after the corrected fix", nw_ok)]
        if ns_after:
            b, be = ns_after.get("bullish_count", 0), ns_after.get("bearish_count", 0)
            n_neu = ns_after.get("neutral_count", 0)
            rep.kv(news_sentiment_bullish=b, news_sentiment_bearish=be, news_sentiment_neutral=n_neu)
            checks.append(("news-sentiment: no 400 in log tail", ns_400_free))
            checks.append(("news-sentiment: at least some non-neutral scores (was 0/0/503)", (b + be) > 0))
        else:
            checks.append(("news-sentiment: produced fresh output", False))

        for label, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {label}")
        failed = [l for l, ok in checks if not ok]
        if failed:
            rep.fail(f"FAILED {len(failed)}: {failed}")
            sys.exit(1)
        rep.ok("PASS_ALL — both Anthropic-key fixes verified against real deployed evidence")


if __name__ == "__main__":
    main()
