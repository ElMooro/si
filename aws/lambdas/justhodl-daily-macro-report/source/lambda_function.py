import json, os, boto3, requests
from datetime import datetime

S3_BUCKET = os.getenv("S3_BUCKET", "justhodl-historical-data-1758485495")
SES_SENDER = os.getenv("SES_SENDER", "reports@justhodl.ai")

def fetch_data():
    url = "https://api.justhodl.ai/"
    resp = requests.post(url, json={"operation":"data"})
    return resp.json()

def interpret_metrics(data):
    notes = []
    # Example: FRA-OIS
    fra_ois = data.get("enhanced-repo-agent",{}).get("fra_ois",42)
    if fra_ois > 40:
        notes.append(f"FRA-OIS at {fra_ois}bps → bank credit stress rising (negative for EM rollover).")
    # Extend: MOVE, ON RRP, TGA, SovCISS, ILM, spreads, etc.
    return notes

def lambda_handler(event, context):
    data = fetch_data()
    notes = interpret_metrics(data)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    report = {
        "generated": now,
        "notes": notes,
        "data": data
    }
    # Save to S3
    s3 = boto3.client("s3")
    key_json = f"reports/macro/{now}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key_json, Body=json.dumps(report).encode(), ContentType="application/json")
    # Email summary
    ses = boto3.client("ses")
    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses":[SES_SENDER]},
        Message={
            "Subject":{"Data":f"Macro Report {now}"},
            "Body":{"Text":{"Data":"\n".join(notes)}}
        }
    )
    return {"ok": True, "s3_key": key_json, "notes": notes}
