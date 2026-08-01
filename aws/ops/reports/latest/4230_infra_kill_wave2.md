# ops 4230 — standby infrastructure, wave 2

**Status:** success  
**Duration:** 25.4s  
**Finished:** 2026-08-01T13:59:39+00:00  

## Data

| action | count | cpu_avg | cursor | docs | domain | evidence | exprs | function | id | instance | name | net_out | requests_14d | searches_14d | section | status | svc | targets | type | universe | url | usd_mo | version |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | 504 |  |  |  |  |  |  |  |  |  |  |  | census |  |  |  |  | 498 |  |  | 1.11.0 |
|  | 17 |  |  |  |  |  | cron(53 17 * * ? *), cron(55 14 * * ? *), cron(6 18 * * ? *), cron(24 21 * * ? *), cron(0  | justhodl-scheduler |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 6 |  |  |  |  |  | cron(0 21 * * ? *), cron(0 9 * * ? *), cron(30 10 * * ? *), cron(30 14 * * ? *), cron(0 12 | justhodl-ai-brief-router |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 5 |  |  |  |  |  | rate(3 hours), rate(3 hours), rate(3 hours), rate(3 hours), rate(3 hours) | justhodl-fleet-freshness-monitor |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 4 |  |  |  |  |  | rate(6 hours), rate(6 hours), rate(6 hours), rate(6 hours) | justhodl-fleet-monitor |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 4 |  |  |  |  |  | cron(40 12 * * ? *), cron(40 22 ? * TUE-SAT *), cron(40 22 ? * TUE-SAT *), cron(15 21 ? *  | justhodl-market-internals |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(30 22 * * ? *), cron(30 22 * * ? *), cron(20 22 ? * MON-FRI *) | justhodl-capital-flow-radar |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | rate(4 hours), rate(4 hours), cron(0 13 * * ? *) | justhodl-carry-surface |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(40 13 ? * MON-FRI *), cron(40 17 ? * MON-FRI *), cron(0 12 * * ? *) | justhodl-estimate-revisions |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(30 11 * * ? *), cron(30 11 * * ? *), cron(30 11 * * ? *) | justhodl-alpha-daily-brief |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(0 22 ? * MON-FRI *), cron(0 22 ? * MON-FRI *), cron(30 21 * * ? *) | justhodl-breadth-thrust |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(55 21 * * ? *), cron(35 21 ? * * *), cron(40 12 * * ? *) | justhodl-crypto-exchange-flows |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(40 21 ? * MON-FRI *), cron(0 14 ? * WED *), cron(0 14 ? * WED *) | justhodl-dark-pool |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(7 13-21 ? * MON-FRI *), cron(7 13-21 ? * MON-FRI *), cron(35 20 ? * MON-FRI *) | justhodl-dealer-gex |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(35 13 ? * MON *), cron(35 13 ? * MON *), cron(0 6 ? * SUN *) | justhodl-factor-decomposition |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(40 20 ? * MON-FRI *), cron(30 23 * * ? *), cron(30 23 * * ? *) | justhodl-money-flow-state |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(0 13 ? * TUE-SAT *), cron(0 13 ? * TUE-SAT *), cron(30 21 ? * MON-FRI *) | justhodl-options-analytics |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(20 * * * ? *), cron(20 * * * ? *), cron(20 * * * ? *) | justhodl-options-confluence |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(29 21 * * ? *), cron(29 21 * * ? *), cron(0 23 * * ? *) | justhodl-outcome-checker |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(0 22 * * ? *), cron(0 22 * * ? *), cron(20 21 ? * MON-FRI *) | justhodl-sector-emergence |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(12 18 * * ? *), cron(38 * * * ? *), cron(5 21 ? * MON-FRI *) | justhodl-sector-rotation |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(30 13 ? * TUE-SAT *), cron(30 13 ? * TUE-SAT *), cron(40 21 ? * MON-FRI *) | justhodl-squeeze-fuel |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(45 13 ? * TUE-SAT *), cron(45 13 ? * TUE-SAT *), cron(55 21 ? * * *) | justhodl-supply-chain-graph |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(0 13 ? * TUE-SAT *), cron(0 13 ? * TUE-SAT *), cron(35 21 * * ? *) | justhodl-tail-risk |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(25 21 ? * MON-FRI *), cron(30 13 * * ? *), cron(30 13 * * ? *) | justhodl-theme-rotation |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
|  | 3 |  |  |  |  |  | cron(30 13 ? * TUE-SAT *), cron(30 13 ? * TUE-SAT *), cron(45 21 ? * MON-FRI *) | justhodl-treasury-noise |  |  |  |  |  |  | dupe_schedule |  |  |  |  |  |  |  |  |
| HOLD (irreversible) |  |  |  | 2 | openbb-financial-search |  |  |  |  | t3.small.search |  |  |  | 1259 | opensearch |  |  |  |  |  |  |  |  |
| HOLD (irreversible) |  |  |  | 6206 | openbb-simple-working |  |  |  |  | t3.small.search |  |  |  | 1260 | opensearch |  |  |  |  |  |  |  |  |
| HOLD (irreversible) |  |  |  |  |  |  |  |  |  |  | openbb-prod-alb |  | 0 |  | elb |  |  | 1 |  |  |  |  |  |
| HOLD (irreversible) |  |  |  |  |  |  |  |  |  |  | openbb-basic-alb |  | 0 |  | elb |  |  | 0 |  |  |  |  |  |
|  |  | 0.94 |  |  |  |  |  |  | i-0a38d50ce030fed84 |  | web server | 4181653 |  |  | ec2 |  |  |  | t2.micro |  |  |  |  |
|  |  | 2.38 |  |  |  |  |  |  | i-0236308c6be77cee8 |  | OpenBB Server | 10990588 |  |  | ec2 |  |  |  | t2.micro |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | openbb-api |  |  |  | apprunner | RUNNING |  |  |  |  | ped8gafyuz.us-east-1.awsapprunner.com |  |  |
|  |  |  |  |  |  | 14d searches=1259, 2 docs indexed |  |  | openbb-financial-search |  |  |  |  |  | held |  | opensearch |  |  |  |  | 51 |  |
|  |  |  |  |  |  | 14d searches=1260, 6206 docs indexed |  |  | openbb-simple-working |  |  |  |  |  | held |  | opensearch |  |  |  |  | 51 |  |
|  |  |  |  |  |  | 14d requests=0, 1 registered targets |  |  | openbb-prod-alb |  |  |  |  |  | held |  | elbv2 |  |  |  |  | 32 |  |
|  |  |  |  |  |  | 14d requests=0, 0 registered targets |  |  | openbb-basic-alb |  |  |  |  |  | held |  | elbv2 |  |  |  |  | 32 |  |

## Log
## 1. Did the ops-4229 census fix hold?

- `13:59:14` ✅ cursor artifact EXISTS — the drain-loop ran and persisted
- `13:59:14`    {"cursor": 504, "universe": 498, "depth": 3, "version": "1.11.0", "at": "2026-08-01T06:42:54.629247+00:00"}
- `13:59:14`    universe=498 -> OLD design needed 63 chained links (AWS kills at 16, so ~25.4% of the walk ever ran)
- `13:59:14` RecursiveInvocationsDropped since deploy (10h) = 0
## 2. Duplicate schedule census (fleet-wide)

- `13:59:32` functions carrying MORE THAN ONE enabled schedule: 168
- `13:59:32`    justhodl-scheduler                         x17  cron(53 17 * * ? *), cron(55 14 * * ? *), cron(6 18 * * ? *), cron(24 
- `13:59:32`    justhodl-ai-brief-router                   x6  cron(0 21 * * ? *), cron(0 9 * * ? *), cron(30 10 * * ? *), cron(30 14
- `13:59:32`    justhodl-fleet-freshness-monitor           x5  rate(3 hours), rate(3 hours), rate(3 hours), rate(3 hours), rate(3 hou
- `13:59:32`    justhodl-fleet-monitor                     x4  rate(6 hours), rate(6 hours), rate(6 hours), rate(6 hours)
- `13:59:32`    justhodl-market-internals                  x4  cron(40 12 * * ? *), cron(40 22 ? * TUE-SAT *), cron(40 22 ? * TUE-SAT
- `13:59:32`    justhodl-capital-flow-radar                x3  cron(30 22 * * ? *), cron(30 22 * * ? *), cron(20 22 ? * MON-FRI *)
- `13:59:32`    justhodl-carry-surface                     x3  rate(4 hours), rate(4 hours), cron(0 13 * * ? *)
- `13:59:32`    justhodl-estimate-revisions                x3  cron(40 13 ? * MON-FRI *), cron(40 17 ? * MON-FRI *), cron(0 12 * * ? 
- `13:59:32`    justhodl-alpha-daily-brief                 x3  cron(30 11 * * ? *), cron(30 11 * * ? *), cron(30 11 * * ? *)
- `13:59:32`    justhodl-breadth-thrust                    x3  cron(0 22 ? * MON-FRI *), cron(0 22 ? * MON-FRI *), cron(30 21 * * ? *
- `13:59:32`    justhodl-crypto-exchange-flows             x3  cron(55 21 * * ? *), cron(35 21 ? * * *), cron(40 12 * * ? *)
- `13:59:32`    justhodl-dark-pool                         x3  cron(40 21 ? * MON-FRI *), cron(0 14 ? * WED *), cron(0 14 ? * WED *)
- `13:59:32`    justhodl-dealer-gex                        x3  cron(7 13-21 ? * MON-FRI *), cron(7 13-21 ? * MON-FRI *), cron(35 20 ?
- `13:59:32`    justhodl-factor-decomposition              x3  cron(35 13 ? * MON *), cron(35 13 ? * MON *), cron(0 6 ? * SUN *)
- `13:59:32`    justhodl-money-flow-state                  x3  cron(40 20 ? * MON-FRI *), cron(30 23 * * ? *), cron(30 23 * * ? *)
- `13:59:32`    justhodl-options-analytics                 x3  cron(0 13 ? * TUE-SAT *), cron(0 13 ? * TUE-SAT *), cron(30 21 ? * MON
- `13:59:32`    justhodl-options-confluence                x3  cron(20 * * * ? *), cron(20 * * * ? *), cron(20 * * * ? *)
- `13:59:32`    justhodl-outcome-checker                   x3  cron(29 21 * * ? *), cron(29 21 * * ? *), cron(0 23 * * ? *)
- `13:59:32`    justhodl-sector-emergence                  x3  cron(0 22 * * ? *), cron(0 22 * * ? *), cron(20 21 ? * MON-FRI *)
- `13:59:32`    justhodl-sector-rotation                   x3  cron(12 18 * * ? *), cron(38 * * * ? *), cron(5 21 ? * MON-FRI *)
- `13:59:32`    justhodl-squeeze-fuel                      x3  cron(30 13 ? * TUE-SAT *), cron(30 13 ? * TUE-SAT *), cron(40 21 ? * M
- `13:59:32`    justhodl-supply-chain-graph                x3  cron(45 13 ? * TUE-SAT *), cron(45 13 ? * TUE-SAT *), cron(55 21 ? * *
- `13:59:32`    justhodl-tail-risk                         x3  cron(0 13 ? * TUE-SAT *), cron(0 13 ? * TUE-SAT *), cron(35 21 * * ? *
- `13:59:32`    justhodl-theme-rotation                    x3  cron(25 21 ? * MON-FRI *), cron(30 13 * * ? *), cron(30 13 * * ? *)
- `13:59:32`    justhodl-treasury-noise                    x3  cron(30 13 ? * TUE-SAT *), cron(30 13 ? * TUE-SAT *), cron(45 21 ? * M
- `13:59:32` 
- `13:59:32` auto-dedupe: MONITORS only (keep 1, disable the rest)
- `13:59:32` ✅    justhodl-fleet-error-monitor KEEP scheduler/fleet-error-monitor-sched (rate(1 hour))
- `13:59:32`       disabled events/justhodl-fleet-error-6h (rate(1 hour))
- `13:59:32` ✅    justhodl-fleet-monitor KEEP scheduler/fleet-monitor-sched (rate(6 hours))
- `13:59:32`       disabled events/fleet-monitor-3h (rate(6 hours))
- `13:59:32`       disabled events/justhodl-fleet-monitor-6h (rate(6 hours))
- `13:59:32`       disabled events/justhodl-fleet-monitor-sched (rate(6 hours))
- `13:59:32` ✅    justhodl-event-flow-monitor KEEP events/event-flow-monitor-hourly (rate(6 hours))
- `13:59:32` ✅    justhodl-fleet-freshness-monitor KEEP scheduler/fleet-freshness-monitor-sched (rate(3 hours))
- `13:59:32`       disabled events/fleet-freshness-monitor-30min (rate(3 hours))
- `13:59:32`       disabled events/justhodl-fleet-freshness-hourly (rate(3 hours))
- `13:59:32`       disabled events/justhodl-fleet-freshness-monitor-sched (rate(3 hours))
- `13:59:32`       disabled events/justhodl-freshness-30m (rate(3 hours))
## 3. Standby infrastructure inventory

- `13:59:32` OpenSearch domains: 2
- `13:59:33` ⚠   openbb-financial-search  t3.small.search x1  14d searches=1259 docs=2
- `13:59:33`      endpoint=search-openbb-financial-search-pjxaw2cqqeqfilppjyxkhfgwue.us-east-1.es.amazonaws.com created=True
- `13:59:34` ⚠   openbb-simple-working    t3.small.search x1  14d searches=1260 docs=6206
- `13:59:34`      endpoint=search-openbb-simple-working-oi5qjg5nan4a73ifgopmhfp234.us-east-1.es.amazonaws.com created=True
- `13:59:34`   SM app default              type=JupyterLab status=InService
- `13:59:35` ⚠   ELB openbb-prod-alb            14d requests=0 targets=1 dns=openbb-prod-alb-180258533.us-east-1.elb.
- `13:59:35` ⚠   ELB openbb-basic-alb           14d requests=0 targets=0 dns=openbb-basic-alb-989296070.us-east-1.elb
- `13:59:36` ⚠   EC2 i-0a38d50ce030fed84  t2.micro    name=web server       14d avgCPU=0.94% netOut=4181653B
- `13:59:37` ⚠   EC2 i-0236308c6be77cee8  t2.micro    name=OpenBB Server    14d avgCPU=2.38% netOut=10990588B
- `13:59:37`   EIP 100.48.244.171   -> eni-084068b954d7a70e7
- `13:59:37`   EIP 44.223.130.247   -> eni-00e4d6c6a852ea8c3
- `13:59:37`   EIP 52.206.134.240   -> eni-02b416228751e541b
- `13:59:37` NAT gateways: 0   interface VPC endpoints: 0
- `13:59:37` ⚠   AppRunner openbb-api             status=RUNNING url=ped8gafyuz.us-east-1.awsapprunner.com
## 4. Execute REVERSIBLE stops

- `13:59:37` ✗   sagemaker_app: Parameter validation failed:
Invalid type for parameter UserProfileName, value: None, type: <class 'NoneType'>, valid types: <class 'str'>
- `13:59:38` ✅   EC2 i-0a38d50ce030fed84 (web server) STOPPED — 14d avg CPU 0.94%; EBS preserved, `aws ec2 start-instances` to undo
- `13:59:38` ✅   EC2 i-0236308c6be77cee8 (OpenBB Server) STOPPED — 14d avg CPU 2.38%; EBS preserved, `aws ec2 start-instances` to undo
- `13:59:38` ✅   AppRunner openbb-api PAUSED (resume anytime)
- `13:59:38` 
- `13:59:38` reversible stops executed -> ~$42/mo removed
## 5. Budget -> ACTUAL alerts at 50/80/100%

- `13:59:39`   budget My Monthly Cost Budget   limit=$150.0
- `13:59:39` ✅      ACTUAL > 50% added ($75)
- `13:59:39` ✅      ACTUAL > 80% added ($120)
- `13:59:39`      ACTUAL 100% already present
## 6. HELD — irreversible, needs an explicit call

- `13:59:39`   $ 51.00/mo  opensearch       openbb-financial-search     14d searches=1259, 2 docs indexed
- `13:59:39`   $ 51.00/mo  opensearch       openbb-simple-working       14d searches=1260, 6206 docs indexed
- `13:59:39`   $ 32.00/mo  elbv2            openbb-prod-alb             14d requests=0, 1 registered targets
- `13:59:39`   $ 32.00/mo  elbv2            openbb-basic-alb            14d requests=0, 0 registered targets
- `13:59:39` 
- `13:59:39` held total: $166.00/mo   executed this run: $42.00/mo
- `13:59:39` ✅ wrote 4230_infra_kill_wave2.json
