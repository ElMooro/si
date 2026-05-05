"""Re-invoke theme-detector after polygon adapter fix and capture full logs."""
import boto3
import json
import time
import base64
from ops_report import report

REGION = "us-east-1"
LAM = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)


def main():
    with report("reinvoke_theme_detector_v2") as r:
        # Wait for Lambda to be active
        for _ in range(30):
            cfg = LAM.get_function(FunctionName="justhodl-theme-detector")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.log(f"  ✓ active, code mod: {cfg.get('LastModified')}")
                break
            time.sleep(2)

        r.heading("1) Re-invoke with LogType=Tail (captures full last 4KB)")
        t0 = time.time()
        resp = LAM.invoke(
            FunctionName="justhodl-theme-detector",
            InvocationType="RequestResponse",
            LogType="Tail",
        )
        body = resp["Payload"].read().decode()
        r.log(f"  duration: {time.time()-t0:.1f}s, status: {resp.get('StatusCode')}")

        log_result = resp.get("LogResult")
        if log_result:
            log_text = base64.b64decode(log_result).decode("utf-8", errors="replace")
            r.log("")
            r.log("  ── Tail logs (last 4KB) ────────────────────────")
            for line in log_text.split("\n"):
                if not line.strip() or line.startswith("START") or line.startswith("END") or line.startswith("REPORT") or line.startswith("INIT"):
                    continue
                r.log(f"    {line[:240]}")
            r.log("  ────────────────────────────────────────────────")

        r.heading("2) Parse response body")
        try:
            outer = json.loads(body)
            if "errorType" in outer:
                r.log(f"  ✗ ERROR: {outer.get('errorType')}: {outer.get('errorMessage')}")
                return
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"  n_themes:           {inner.get('n_themes')}")
            r.log(f"  duration_s:         {inner.get('duration_s')}")
            r.log(f"  phase_distribution: {inner.get('phase_distribution')}")
            r.log(f"  HOTTEST:            {inner.get('hottest')}")
            r.log(f"  TIER-2 grounds:     {inner.get('tier2_hunt')}")
            r.log(f"  EMERGING:           {inner.get('emerging')}")
            r.log(f"  DYING:              {inner.get('dying')}")
        except Exception as e:
            r.log(f"  ✗ {e}, body[:500]: {body[:500]}")

        r.heading("3) Pull fresh CloudWatch log stream (more than 4KB tail)")
        try:
            logs = boto3.client("logs", region_name=REGION)
            streams = logs.describe_log_streams(
                logGroupName="/aws/lambda/justhodl-theme-detector",
                orderBy="LastEventTime",
                descending=True,
                limit=1,
            )
            if streams.get("logStreams"):
                stream = streams["logStreams"][0]
                r.log(f"  stream: {stream['logStreamName']}")
                events = logs.get_log_events(
                    logGroupName="/aws/lambda/justhodl-theme-detector",
                    logStreamName=stream["logStreamName"],
                    limit=300,
                    startFromHead=True,
                )
                r.log("")
                r.log("  Full log events (max 300):")
                poly_lines = []
                other_lines = []
                for e in events.get("events", []):
                    msg = e["message"].rstrip()
                    if not msg or msg.startswith(("START", "END", "REPORT", "INIT")):
                        continue
                    if "[poly]" in msg:
                        poly_lines.append(msg)
                    else:
                        other_lines.append(msg)
                r.log("")
                r.log(f"  ── [poly] lines (errors): {len(poly_lines)} ──")
                # Show first 30 polygon error lines (probably all the same root cause)
                for line in poly_lines[:30]:
                    r.log(f"    {line[:240]}")
                r.log("")
                r.log(f"  ── Other lines: {len(other_lines)} ──")
                for line in other_lines[:30]:
                    r.log(f"    {line[:240]}")
        except Exception as e:
            r.log(f"  ✗ logs: {e}")

        r.heading("4) S3 themes-detected.json contents")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/themes-detected.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  generated_at: {d.get('generated_at')}")
            r.log(f"  n_themes:     {len(d.get('themes', []))}")
            r.log(f"  fetch_stats:  {d.get('fetch_stats')}")
            r.log(f"  summary:      {json.dumps(d.get('summary'), indent=2)[:600]}")
            themes = d.get("themes") or []
            if themes:
                r.log("")
                r.log("  Top 8 themes (EXTENDED + ACCELERATING first):")
                shown = 0
                for t in themes:
                    if shown >= 8:
                        break
                    if t.get("phase") not in ("EXTENDED", "ACCELERATING"):
                        continue
                    m = t.get("metrics", {})
                    r.log(f"    {t['etf']:5s} {t['name']:32s} {t['phase']:13s} score={t['phase_score']}")
                    r.log(f"      30d={m.get('ret_30d')}% 90d={m.get('ret_90d')}% 180d={m.get('ret_180d')}% 365d={m.get('ret_365d')}%")
                    r.log(f"      → {t.get('interpretation')}")
                    shown += 1
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
