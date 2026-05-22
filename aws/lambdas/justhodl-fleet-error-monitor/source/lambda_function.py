import json, os, boto3, urllib.request
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

REGION = "us-east-1"
SNS_ARN = os.environ["SNS_ARN"]
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
ERROR_RATE_THRESHOLD = float(os.environ.get("ERROR_RATE_THRESHOLD", "5.0"))  # %
MIN_INVOCATIONS = int(os.environ.get("MIN_INVOCATIONS", "5"))  # ignore noise
LOOKBACK_MINUTES = int(os.environ.get("LOOKBACK_MINUTES", "15"))

lam = boto3.client("lambda", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
sns = boto3.client("sns", region_name=REGION)


def get_metrics(name, start, end):
    out = {"name": name}
    for metric in ["Invocations", "Errors"]:
        try:
            r = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName=metric,
                Dimensions=[{"Name": "FunctionName", "Value": name}],
                StartTime=start, EndTime=end, Period=900, Statistics=["Sum"],
            )
            out[metric.lower()] = int(sum(p["Sum"] for p in r["Datapoints"]))
        except Exception:
            out[metric.lower()] = 0
    return out


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=LOOKBACK_MINUTES)

    funcs = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        funcs.extend(page["Functions"])

    metrics = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = [ex.submit(get_metrics, fn["FunctionName"], start, now) for fn in funcs]
        for fut in as_completed(futures):
            metrics.append(fut.result())

    alerts = []
    for m in metrics:
        inv = m.get("invocations", 0)
        err = m.get("errors", 0)
        if inv >= MIN_INVOCATIONS and err > 0:
            rate = err / inv * 100
            if rate >= ERROR_RATE_THRESHOLD:
                alerts.append({
                    "lambda": m["name"], "inv": inv, "err": err, "rate": round(rate, 1)
                })

    alerts.sort(key=lambda x: -x["rate"])

    summary = {
        "checked_at": now.isoformat(),
        "lookback_min": LOOKBACK_MINUTES,
        "lambdas_scanned": len(metrics),
        "lambdas_alerting": len(alerts),
        "alerts": alerts[:30],
        "threshold_pct": ERROR_RATE_THRESHOLD,
    }

    if alerts:
        lines = [f"\u26a0\ufe0f *JustHodl Fleet Alert*",
                 f"_{len(alerts)} Lambdas erroring above {ERROR_RATE_THRESHOLD}% over {LOOKBACK_MINUTES} min_",
                 ""]
        for a in alerts[:10]:
            lines.append(f"\u2022 `{a['lambda']}`: {a['err']}/{a['inv']} = {a['rate']}%")
        if len(alerts) > 10:
            lines.append(f"...and {len(alerts) - 10} more")
        msg = "\n".join(lines)

        # SNS publish
        try:
            sns.publish(TopicArn=SNS_ARN, Subject=f"Fleet alert: {len(alerts)} Lambdas erroring",
                        Message=msg)
        except Exception as e:
            summary["sns_error"] = str(e)[:200]

        # Telegram direct
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                data = json.dumps({
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": msg,
                    "parse_mode": "Markdown",
                }).encode()
                req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
                urllib.request.urlopen(req, timeout=10)
            except Exception as e:
                summary["telegram_error"] = str(e)[:200]

    return summary
