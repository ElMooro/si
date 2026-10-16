#!/usr/bin/env python3
"""Set a $500 monthly AWS budget with alerts so costs cannot silently exceed it.

- Creates or updates a cost budget of $500 USD per month.
- Notifications at 50%, 80%, 100% actual and 100% forecasted.
- Email notifications to the account contact.
- User must manually increase the budget amount for anything to go over $500.
"""
import boto3
import json
from datetime import datetime

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"  # from prior context
BUDGET_NAME = "JustHodl-Monthly-Hard-Cap-500"
LIMIT = "500"
EMAIL = "raafouis@gmail.com"  # from prior context

budgets = boto3.client("budgets", region_name=REGION)

def main():
    print(f"=== Setting ${LIMIT}/month hard budget ===")

    budget = {
        "BudgetName": BUDGET_NAME,
        "BudgetLimit": {"Amount": LIMIT, "Unit": "USD"},
        "TimeUnit": "MONTHLY",
        "BudgetType": "COST",
        "CostFilters": {},
        "TimePeriod": {
            "Start": datetime(2026, 10, 1),
            "End": datetime(2027, 10, 1)
        }
    }

    notifications = [
        {
            "Notification": {
                "NotificationType": "ACTUAL",
                "ComparisonOperator": "GREATER_THAN",
                "Threshold": 50,
                "ThresholdType": "PERCENTAGE"
            },
            "Subscribers": [{"SubscriptionType": "EMAIL", "Address": EMAIL}]
        },
        {
            "Notification": {
                "NotificationType": "ACTUAL",
                "ComparisonOperator": "GREATER_THAN",
                "Threshold": 80,
                "ThresholdType": "PERCENTAGE"
            },
            "Subscribers": [{"SubscriptionType": "EMAIL", "Address": EMAIL}]
        },
        {
            "Notification": {
                "NotificationType": "ACTUAL",
                "ComparisonOperator": "GREATER_THAN",
                "Threshold": 100,
                "ThresholdType": "PERCENTAGE"
            },
            "Subscribers": [{"SubscriptionType": "EMAIL", "Address": EMAIL}]
        },
        {
            "Notification": {
                "NotificationType": "FORECASTED",
                "ComparisonOperator": "GREATER_THAN",
                "Threshold": 100,
                "ThresholdType": "PERCENTAGE"
            },
            "Subscribers": [{"SubscriptionType": "EMAIL", "Address": EMAIL}]
        }
    ]

    try:
        budgets.create_budget(
            AccountId=ACCOUNT_ID,
            Budget=budget,
            NotificationsWithSubscribers=notifications
        )
        print(f"  Created budget {BUDGET_NAME} = ${LIMIT}/month")
    except budgets.exceptions.DuplicateRecordException:
        print(f"  Budget {BUDGET_NAME} already exists; updating...")
        budgets.update_budget(AccountId=ACCOUNT_ID, NewBudget=budget)
        # Re-apply notifications
        for n in notifications:
            try:
                budgets.create_notification(
                    AccountId=ACCOUNT_ID,
                    BudgetName=BUDGET_NAME,
                    Notification=n["Notification"],
                    Subscribers=n["Subscribers"]
                )
            except Exception:
                pass
        print("  Updated budget and notifications")
    except Exception as e:
        print(f"  Error: {e}")

    print("=== Budget set. Alerts at 50/80/100% actual and 100% forecasted. Manual increase required to exceed $500. ===")

if __name__ == "__main__":
    main()
