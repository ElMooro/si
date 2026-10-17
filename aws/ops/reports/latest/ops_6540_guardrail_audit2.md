# ops 6540 - follow-up audit for the runaway guardrails (read-only)

**Status:** success  
**Duration:** 5.4s  
**Finished:** 2026-10-17T01:37:58+00:00  

## Log
## 1. Did the old guard alarms fire?

- `01:37:53` justhodl-guard-lambda-concurrency: period=60 eval=5 threshold=150.0 actions=['arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts'] ok_actions=['arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts']
- `01:37:53`   state changes since Oct 1: [('10-10 16:33', 'Alarm updated from OK to ALARM'), ('10-10 16:37', 'Alarm updated from ALARM to OK'), ('10-12 09:18', 'Alarm updated from OK to ALARM'), ('10-16 19:48', 'Alarm updated from ALARM to OK')]
- `01:37:53`   actions since Oct 1: [('10-10 16:33', 'Successfully executed action arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts'), ('10-10 16:37', 'Successfully executed action arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts'), ('10-12 09:18', 'Successfully executed action arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts'), ('10-16 19:48', 'Successfully executed action arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts')]
- `01:37:53` justhodl-guard-lambda-invocations-1h: period=3600 eval=1 threshold=20000.0 actions=['arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts'] ok_actions=['arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts']
- `01:37:53`   state changes since Oct 1: [('10-05 00:25', 'Alarm updated from OK to ALARM'), ('10-05 01:12', 'Alarm updated from ALARM to OK'), ('10-12 09:19', 'Alarm updated from OK to ALARM'), ('10-16 20:42', 'Alarm updated from ALARM to OK')]
- `01:37:53`   actions since Oct 1: [('10-05 00:25', 'Successfully executed action arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts'), ('10-05 01:12', 'Successfully executed action arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts'), ('10-12 09:19', 'Successfully executed action arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts'), ('10-16 20:42', 'Successfully executed action arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts')]
## 2. Cost Anomaly subscription

- `01:37:54` subscription Default-Services-Subscription arn=...b8ca2cdd04b5 freq=DAILY expr={"And": [{"Dimensions": {"Key": "ANOMALY_TOTAL_IMPACT_ABSOLUTE", "Values": ["100.0"], "MatchOptions": ["GREATER_THAN_OR_EQUAL"]}}, {"Dimensions": {"Key": "ANOMALY_TOTAL_IMPACT_PERCENTAGE", "Values": ["40.0"], "MatchOptions": ["GREATER_THAN_OR_EQUAL"]}}]}
## 3. Prefixes that may rely on version history

- `01:37:54` backups/        keys(sampled)=   3 multi-version=   0 max_versions=  1 truncated=False newest=2026-08-01 sample=['backups/opensearch/openbb-simple-working-20260801.json', 'backups/ops-4231-rollback.json', 'backups/scrapeMacroData-env-20260801.json']
- `01:37:54` audit-private/  keys(sampled)=1000 multi-version=   0 max_versions=  1 truncated=True newest=2026-09-09 sample=['audit-private/20260909-originals/000c8c780a52323e7d79825f170a4d3f66dd129509065123b214965df7c5212b.json', 'audit-private/20260909-originals/001024c38e333d3f753d44be8ad6371e91ba966432cafdc670cb4c9ca96aa3af.json', 'audit-private/20260909-originals/00142f15be42962cc8f3b7aef8a1bbf6ea13f7466173ac771b40c9e1941b1a45.json', 'audit-private/20260909-originals/001b54cfbce9265bc95eb4c6940e243f0dc97ee47ac63adb575de95ec5d2a8e5.json']
- `01:37:55` _audit/         keys(sampled)=   7 multi-version=   0 max_versions=  1 truncated=False newest=2026-04-25 sample=['_audit/broken_lambdas_2026-04-25.md', '_audit/cost_audit_2026-04-25.md', '_audit/ddb_pre_delete_20260425_015658.json', '_audit/feature_audit_2026-04-25.md']
- `01:37:55` archive/        keys(sampled)=1000 multi-version=   0 max_versions=  1 truncated=True newest=2026-08-04 sample=['archive/2026-02-17.html', 'archive/2026-02-17.json', 'archive/2026-02-18.json', 'archive/intelligence/2026/02/23/0815.json']
- `01:37:55` config/         keys(sampled)=  12 multi-version=   4 max_versions= 12 truncated=False newest=2026-10-16 sample=['config/ai-brief-contexts.json', 'config/artifact-producers.json', 'config/contract-exemptions.json', 'config/engine-contracts.json']
- `01:37:55` state/          keys(sampled)=   1 multi-version=   1 max_versions= 78 truncated=False newest=2026-10-16 sample=['state/universe-discovery-snapshot.json']
- `01:37:55` khalid/         keys(sampled)=   1 multi-version=   0 max_versions=  1 truncated=False newest=2026-02-27 sample=['khalid/index.html']
- `01:37:55` ops/            keys(sampled)=  15 multi-version=   0 max_versions=  1 truncated=False newest=2026-10-01 sample=['ops/hibernate-disabled-rules.json', 'ops/reports/1029_live_audit.json', 'ops/reports/1051.json', 'ops/reports/1053.json']
- `01:37:55` _ops/           keys(sampled)=   1 multi-version=   0 max_versions=  1 truncated=False newest=2026-05-16 sample=['_ops/reports/719_scorecard_fix_verify.json']
- `01:37:55` learning/       keys(sampled)=   4 multi-version=   2 max_versions= 77 truncated=False newest=2026-10-16 sample=['learning/improvement_log.json', 'learning/last_log_run.json', 'learning/morning_run_log.json', 'learning/prompt_templates.json']
## 4. SES

- `01:37:56` SES get_account: ClientError
- `01:37:56` ⚠ ses failed: ClientError: An error occurred (AccessDeniedException) when calling the ListEmailIdentities operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: ses:ListEmailIdentities on resource: * because no identity-
## 5. a2a pair

- `01:37:56` justhodl-a2a-bus: reserved=0 invocations/day last 30d (non-zero)=[('10-05', 958), ('10-06', 942), ('10-07', 939), ('10-08', 939), ('10-09', 942), ('10-10', 936), ('10-11', 952), ('10-12', 662), ('10-13', 555), ('10-14', 497), ('10-15', 596), ('10-16', 796)]
- `01:37:57` justhodl-backend-agent: reserved=0 invocations/day last 30d (non-zero)=[('10-05', 96), ('10-06', 96), ('10-07', 96), ('10-08', 96), ('10-09', 96), ('10-10', 96), ('10-11', 96), ('10-12', 66), ('10-13', 54), ('10-14', 48), ('10-15', 56), ('10-16', 76)]
- `01:37:57` cloudtrail lookup: AccessDeniedException
## 6. SNS policies

- `01:37:58` governor topic policy: {"Version":"2012-10-17","Statement":[{"Sid":"Owner","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::857687956942:root"},"Action":["SNS:Publish","SNS:Subscribe","SNS:GetTopicAttributes","SNS:SetTopicAttributes","SNS:ListSubscriptionsByTopic","SNS:DeleteTopic"],"Resource":"arn:aws:sns:us-east-1:857687956942:justhodl-spend-governor"},{"Sid":"BudgetsPublish","Effect":"Allow","Principal":{"Service":"budgets.amazonaws.com"},"Action":"SNS:Publish","Resource":"arn:aws:sns:us-east-1:857687956942:justhodl-spend-governor","Condition":{"StringEquals":{"aws:SourceAccount":"857687956942"},"ArnLike":{"aws:SourceArn":"arn:aws:budgets::857687956942:*"}}}]}
- `01:37:58` fleet-alerts policy principals: [('__default_statement_ID', {'AWS': '*'}, ['SNS:GetTopicAttributes', 'SNS:SetTopicAttributes', 'SNS:AddPermission', 'SNS:RemovePermission', 'SNS:DeleteTopic', 'SNS:Subscribe', 'SNS:ListSubscriptionsByTopic', 'SNS:Publish'])]
## 7. Metric filters

- `01:37:58` /aws/lambda/justhodl-event-coordinator metric filters: []
- `01:37:58` /aws/lambda/justhodl-spend-governor metric filters: []
- `01:37:58` ⚠ ses: ClientError: An error occurred (AccessDeniedException) when calling the ListEmailIdentities operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: ses:ListEmailIdentities on resource: * because no identity-
- `01:37:58` ✅ audit 2 complete
