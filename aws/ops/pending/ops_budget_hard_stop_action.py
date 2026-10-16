#!/usr/bin/env python3
"""Add stronger enforcement to the $500 monthly budget.

Creates a restrictive IAM policy that denies high-cost actions
(Lambda Invoke, S3 PutObject, EventBridge PutRule, etc.)
and attaches it via a Budget Action when the budget hits 100% actual.

This provides a hard stop beyond alerts.
"""
import boto3
import json

REGION = 'us-east-1'
ACCOUNT_ID = '857687956942'
BUDGET_NAME = 'JustHodl-Monthly-Hard-Cap-500'
POLICY_NAME = 'JustHodl-Budget-Exceeded-Deny'
ROLE_NAME = 'JustHodl-Budgets-Action-Role'  # must exist or be created with trust for budgets.amazonaws.com

# Restrictive policy that blocks further high-cost activity
DENY_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Deny",
            "Action": [
                "lambda:InvokeFunction",
                "lambda:UpdateFunctionConfiguration",
                "s3:PutObject",
                "s3:DeleteObject",
                "events:PutRule",
                "events:EnableRule",
                "scheduler:CreateSchedule",
                "scheduler:UpdateSchedule"
            ],
            "Resource": "*"
        }
    ]
}

def main():
    iam = boto3.client('iam', region_name=REGION)
    budgets = boto3.client('budgets', region_name=REGION)

    print('=== Setting up stronger budget enforcement ===')

    # 1. Create or update the deny policy
    try:
        policy = iam.create_policy(
            PolicyName=POLICY_NAME,
            PolicyDocument=json.dumps(DENY_POLICY),
            Description='Applied by Budget Action when JustHodl $500 monthly budget is exceeded'
        )
        policy_arn = policy['Policy']['Arn']
        print(f'Created deny policy: {policy_arn}')
    except iam.exceptions.EntityAlreadyExistsException:
        # Get existing
        policy_arn = f'arn:aws:iam::{ACCOUNT_ID}:policy/{POLICY_NAME}'
        # Update versions if needed (simplified)
        print(f'Using existing deny policy: {policy_arn}')

    # 2. Ensure an execution role exists for Budgets (best-effort; may already exist)
    # In practice this role must trust budgets.amazonaws.com and have permissions to attach policies.
    # For this ops we assume it exists or will be created separately if missing.

    # 3. Create Budget Action (automatic) at 100% actual
    try:
        action = budgets.create_budget_action(
            AccountId=ACCOUNT_ID,
            BudgetName=BUDGET_NAME,
            NotificationType='ACTUAL',
            ActionType='APPLY_IAM_POLICY',
            ActionThreshold={
                'ActionThresholdValue': 100.0,
                'ActionThresholdType': 'PERCENTAGE'
            },
            Definition={
                'IamActionDefinition': {
                    'PolicyArn': policy_arn,
                    'Roles': ['lambda-execution-role', 'justhodl-scheduler-role']  # adjust to actual roles used by the fleet
                }
            },
            ExecutionRoleArn=f'arn:aws:iam::{ACCOUNT_ID}:role/{ROLE_NAME}',
            ApprovalModel='AUTOMATIC',
            Subscribers=[
                {
                    'SubscriptionType': 'EMAIL',
                    'Address': 'raafouis@gmail.com'
                }
            ]
        )
        print(f'Created Budget Action: {action.get("ActionId", "created")}')
    except Exception as e:
        print(f'Budget Action error (may need ExecutionRole): {e}')

    print('\n=== Stronger enforcement configured ===')
    print('When the $500 budget is exceeded, the deny policy will be applied to key roles,')
    print('blocking further Lambda invokes, S3 writes, and schedule changes.')
    print('To reverse, manually detach the policy or increase the budget and wait for reset.')

if __name__ == '__main__':
    main()
