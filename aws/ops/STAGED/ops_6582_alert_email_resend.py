"""ops 6582 - fleet-alerts e-mail: is it confirmed?  If not, send the confirmation again (Khalid, 2026-10-20 22:10 ET).

The address is never written here: it is copied from the AWS Budget's own EMAIL subscriber inside the runner (ops 6558
pattern).  SNS e-mail subscriptions need the recipient's click on "Confirm subscription"; the link lives three days.
Also subscribes the same address to the governor's own topic (justhodl-spend-governor: the runaway alarms' notices) if
it is not there yet, so one click per topic confirms both.  Read-only apart from the subscribe calls.
"""
from pathlib import Path
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
BUDGET = "JustHodl-Monthly-Hard-Cap-600"
TOPICS = {"fleet-alerts": "arn:aws:sns:us-east-1:%s:justhodl-fleet-alerts" % ACCOUNT,
          "governor": "arn:aws:sns:us-east-1:%s:justhodl-spend-governor" % ACCOUNT}
CFG = Config(retries={"max_attempts": 10, "mode": "standard"})


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def mask(addr):
    return addr[:2] + "***" + addr[addr.find("@"):] if "@" in addr else "***"


def main():
    b, sns = c("budgets"), c("sns")
    with report("ops_6582_alert_email_resend") as r:
        r.heading("ops 6582 - alert e-mail subscriptions: status, re-send if unconfirmed")
        emails = set()
        for n in b.describe_notifications_for_budget(AccountId=ACCOUNT, BudgetName=BUDGET).get("Notifications", []):
            for s in b.describe_subscribers_for_notification(AccountId=ACCOUNT, BudgetName=BUDGET, Notification=n)["Subscribers"]:
                if s["SubscriptionType"] == "EMAIL":
                    emails.add(s["Address"])
        r.log("budget e-mail subscribers: %s" % [mask(e) for e in sorted(emails)])
        status = {}
        for name, arn in TOPICS.items():
            try:
                subs = sns.list_subscriptions_by_topic(TopicArn=arn).get("Subscriptions", [])
            except Exception as ex:  # noqa: BLE001
                r.log("%s: %s" % (name, type(ex).__name__))
                continue
            have = {s["Endpoint"]: s["SubscriptionArn"] for s in subs if s["Protocol"] == "email"}
            for e in sorted(emails):
                sarn = have.get(e)
                if sarn and sarn != "PendingConfirmation":
                    status["%s/%s" % (name, mask(e))] = "confirmed"
                else:
                    sns.subscribe(TopicArn=arn, Protocol="email", Endpoint=e)
                    status["%s/%s" % (name, mask(e))] = "confirmation e-mail sent just now (was %s)" % ("pending" if sarn else "absent")
            others = [(s["Protocol"], "confirmed" if s["SubscriptionArn"] != "PendingConfirmation" else "pending") for s in subs if s["Protocol"] != "email"]
            r.log("%s: other subscriptions %s" % (name, others))
        r.log("e-mail status: %s" % json.dumps(status))
        r.kv(**{k.replace("/", "_").replace("*", "").replace("@", "_at_").replace(".", "_"): v for k, v in status.items()})
        if all(v == "confirmed" for v in status.values()) and status:
            r.ok("every e-mail subscription is confirmed")
        else:
            r.ok("confirmation e-mail(s) sent; the click is Khalid's (link lives 3 days)")


if __name__ == "__main__":
    main()
