# ops 6530 - forensics of the Oct 12-16 anomaly (read-only)

**Status:** success  
**Duration:** 36.0s  
**Finished:** 2026-10-17T00:38:55+00:00  

## Log
## 1. Cost

- `00:38:19` Oct 1-11 average: $21.35/day
- `00:38:19` 2026-10-09 $   24.32 (est)  AmazonCloudWatch $9, Simple Storage Service $7, Lambda $5, Cost Explorer $2
- `00:38:19` 2026-10-10 $   20.34 (est)  AmazonCloudWatch $9, Lambda $4, Simple Storage Service $4, Cost Explorer $2
- `00:38:19` 2026-10-11 $   20.69 (est)  AmazonCloudWatch $9, Simple Storage Service $4, Lambda $4, Cost Explorer $2
- `00:38:19` 2026-10-12 $  336.24 (est)  Lambda $286, Simple Storage Service $35, AmazonCloudWatch $7, CloudWatch Events $5
- `00:38:19` 2026-10-13 $  515.89 (est)  Lambda $444, Simple Storage Service $52, AmazonCloudWatch $8, CloudWatch Events $8
- `00:38:19` 2026-10-14 $  555.54 (est)  Lambda $451, Simple Storage Service $83, AmazonCloudWatch $8, CloudWatch Events $8
- `00:38:19` 2026-10-15 $  587.84 (est)  Lambda $450, Simple Storage Service $116, AmazonCloudWatch $9, CloudWatch Events $8
- `00:38:19` 2026-10-16 $  373.98 (est)  Lambda $327, Simple Storage Service $32, AmazonCloudWatch $6, CloudWatch Events $6
- `00:38:19` 2026-10-17 $    0.00 (est)  
- `00:38:19` October month-to-date per Cost Explorer now: $2604.33
- `00:38:20` Cost Explorer forecast for the rest of October (it does not know about the freeze): $3338.31
- `00:38:20`   Oct 12-17 Lambda-GB-Second                         $  1945.09  qty 116705441.2
- `00:38:20`   Oct 12-17 Requests-Tier1                           $   135.68  qty 27136391.0
- `00:38:20`   Oct 12-17 TimedStorage-ByteHrs                     $   133.79  qty 5804.3
- `00:38:20`   Oct 12-17 USE1-Event-64K-Chunks                    $    35.52  qty 35522152.0
- `00:38:20`   Oct 12-17 DataTransfer-Out-Bytes                   $    29.87  qty 331.9
- `00:38:20`   Oct 12-17 Requests-Tier2                           $    24.18  qty 60450982.0
- `00:38:20`   Oct 12-17 CW:Requests                              $    23.59  qty 2359459.0
- `00:38:20`   Oct 12-17 USE1-VendedLog-Bytes                     $    11.33  qty 27.4
- `00:38:20`   Oct 12-17 Requests-RBP                             $     7.99  qty 20971012.0
- `00:38:20`   Oct 12-17 Request                                  $     6.35  qty 32438664.0
- `00:38:20`   Oct 12-17 USE1-APIRequest                          $     5.12  qty 512.0
- `00:38:20`   Oct 12-17 CW:MetricMonitorUsage                    $     2.78  qty 9.3
## 2. Before the change: previous Mondays

- `00:38:21` calibrator invocations per day Sep 10 - Oct 11 (non-zero): [('09-14 00:00', 10.0), ('09-21 00:00', 16.0), ('09-28 00:00', 24.0), ('10-05 00:00', 12.0)]
- `00:38:21` coordinator invocations per day Sep 10 - Oct 11: total 16261.0, max day 835.0
- `00:38:21` Monday 09-14 09:00-11:00 UTC: calibrator 10.0 runs [('09-14 09:15', 10.0)] | coordinator 59.0 runs
- `00:38:22` Monday 09-21 09:00-11:00 UTC: calibrator 16.0 runs [('09-21 09:15', 16.0)] | coordinator 76.0 runs
- `00:38:22` Monday 09-28 09:00-11:00 UTC: calibrator 24.0 runs [('09-28 09:15', 24.0)] | coordinator 74.0 runs
- `00:38:22` Monday 10-05 09:00-11:00 UTC: calibrator 12.0 runs [('10-05 09:15', 12.0)] | coordinator 38.0 runs
- `00:38:23` Monday 10-12 09:00-11:00 UTC: calibrator 250969.0 runs [('10-12 09:15', 11111.0), ('10-12 09:20', 12587.0), ('10-12 09:25', 12406.0), ('10-12 09:30', 12396.0), ('10-12 09:35', 5565.0), ('10-12 09:40', 11979.0), ('10-12 09:45', 12922.0), ('10-12 09:50', 12486.0)] | coordinator 437734.0 runs
## 3. Start minute, Oct 12

- `00:38:23` justhodl-miss-calibrator invocations per minute from 09:00 UTC Oct 12 (first 30 non-empty minutes): [('10-12 09:15', 969.0), ('10-12 09:16', 2576.0), ('10-12 09:17', 2514.0), ('10-12 09:18', 2530.0), ('10-12 09:19', 2522.0), ('10-12 09:20', 2522.0), ('10-12 09:21', 2510.0), ('10-12 09:22', 2553.0), ('10-12 09:23', 2497.0), ('10-12 09:24', 2505.0), ('10-12 09:25', 2451.0), ('10-12 09:26', 2487.0), ('10-12 09:27', 2512.0), ('10-12 09:28', 2493.0), ('10-12 09:29', 2463.0), ('10-12 09:30', 2501.0), ('10-12 09:31', 2519.0), ('10-12 09:32', 2449.0), ('10-12 09:33', 2498.0), ('10-12 09:34', 2429.0), ('10-12 09:35', 630.0), ('10-12 09:36', 831.0), ('10-12 09:37', 1051.0), ('10-12 09:38', 1366.0), ('10-12 09:39', 1687.0), ('10-12 09:40', 1989.0), ('10-12 09:41', 2313.0), ('10-12 09:42', 2558.0), ('10-12 09:43', 2523.0), ('10-12 09:44', 2596.0)]
- `00:38:23` justhodl-event-coordinator invocations per minute from 09:00 UTC Oct 12 (first 30 non-empty minutes): [('10-12 09:15', 1027.0), ('10-12 09:16', 4343.0), ('10-12 09:17', 4417.0), ('10-12 09:18', 4349.0), ('10-12 09:19', 4305.0), ('10-12 09:20', 4400.0), ('10-12 09:21', 4457.0), ('10-12 09:22', 4237.0), ('10-12 09:23', 4412.0), ('10-12 09:24', 4365.0), ('10-12 09:25', 4327.0), ('10-12 09:26', 4295.0), ('10-12 09:27', 4253.0), ('10-12 09:28', 4357.0), ('10-12 09:29', 4430.0), ('10-12 09:30', 4200.0), ('10-12 09:31', 4150.0), ('10-12 09:32', 4493.0), ('10-12 09:33', 4354.0), ('10-12 09:34', 3905.0), ('10-12 09:35', 1198.0), ('10-12 09:36', 1403.0), ('10-12 09:37', 1839.0), ('10-12 09:38', 2504.0), ('10-12 09:39', 3097.0), ('10-12 09:40', 3746.0), ('10-12 09:41', 4278.0), ('10-12 09:42', 4419.0), ('10-12 09:43', 4169.0), ('10-12 09:44', 4077.0)]
- `00:38:23` calibrator concurrent executions per minute: [('10-12 09:15', 625.0), ('10-12 09:16', 716.0), ('10-12 09:17', 682.0), ('10-12 09:18', 678.0), ('10-12 09:19', 679.0), ('10-12 09:20', 665.0), ('10-12 09:21', 664.0), ('10-12 09:22', 676.0), ('10-12 09:23', 680.0), ('10-12 09:24', 663.0), ('10-12 09:25', 669.0), ('10-12 09:26', 670.0), ('10-12 09:27', 680.0), ('10-12 09:28', 663.0), ('10-12 09:29', 668.0), ('10-12 09:30', 663.0), ('10-12 09:31', 689.0), ('10-12 09:32', 660.0), ('10-12 09:33', 679.0), ('10-12 09:34', 684.0), ('10-12 09:35', 496.0), ('10-12 09:36', 218.0), ('10-12 09:37', 295.0), ('10-12 09:38', 379.0), ('10-12 09:39', 474.0), ('10-12 09:40', 551.0), ('10-12 09:41', 624.0), ('10-12 09:42', 672.0), ('10-12 09:43', 681.0), ('10-12 09:44', 685.0)]
## 4. Trajectory Oct 12-17 + collateral

- `00:38:26` justhodl-miss-calibrator invocations per day: {'10-12': 1779920, '10-13': 2611486, '10-14': 2711595, '10-15': 2704800, '10-16': 2307774}
- `00:38:26` justhodl-miss-calibrator peak concurrency per day: {'10-12': 716.0, '10-13': 737.0, '10-14': 732.0, '10-15': 720.0, '10-16': 721.0}
- `00:38:26` justhodl-miss-calibrator avg duration ms/day [('10-12 00:00', 15347.6), ('10-13 00:00', 15327.9), ('10-14 00:00', 15340.6), ('10-15 00:00', 15341.2), ('10-16 00:00', 15349.8)] | errors [('10-12 00:00', 282.0), ('10-13 00:00', 340.0), ('10-14 00:00', 544.0), ('10-15 00:00', 488.0), ('10-16 00:00', 417.0)] | throttles [('10-12 00:00', 33005.0), ('10-13 00:00', 41723.0), ('10-14 00:00', 43498.0), ('10-15 00:00', 44081.0), ('10-16 00:00', 35470.0)]
- `00:38:26` justhodl-miss-calibrator async queue: received [('10-12 00:00', 3082393.0), ('10-13 00:00', 4525096.0), ('10-14 00:00', 4692317.0), ('10-15 00:00', 4679538.0), ('10-16 00:00', 3989510.0)] | oldest event age s [('10-12 00:00', 21600006.0), ('10-13 00:00', 21600002.0), ('10-14 00:00', 21600008.0), ('10-15 00:00', 21600001.0), ('10-16 00:00', 21600020.0)] | dropped [('10-12 00:00', 179694.0), ('10-13 00:00', 2077531.0), ('10-14 00:00', 1958998.0), ('10-15 00:00', 2041086.0), ('10-16 00:00', 2598080.0)]
- `00:38:29` justhodl-event-coordinator invocations per day: {'10-12': 3082441, '10-13': 4525179, '10-14': 4692506, '10-15': 4679621, '10-16': 3989618}
- `00:38:29` justhodl-event-coordinator peak concurrency per day: {'10-12': 387.0, '10-13': 438.0, '10-14': 418.0, '10-15': 439.0, '10-16': 397.0}
- `00:38:29` justhodl-event-coordinator avg duration ms/day [('10-12 00:00', 4091.6), ('10-13 00:00', 5518.7), ('10-14 00:00', 4990.9), ('10-15 00:00', 5024.1), ('10-16 00:00', 4534.7)] | errors [('10-12 00:00', 0.0), ('10-13 00:00', 6.0), ('10-14 00:00', 1.0), ('10-15 00:00', 0.0), ('10-16 00:00', 4.0)] | throttles [('10-12 00:00', 56446.0), ('10-13 00:00', 70929.0), ('10-14 00:00', 73952.0), ('10-15 00:00', 75600.0), ('10-16 00:00', 60135.0)]
- `00:38:29` justhodl-event-coordinator async queue: received [('10-12 00:00', 5337636.0), ('10-13 00:00', 7833919.0), ('10-14 00:00', 8133387.0), ('10-15 00:00', 8113100.0), ('10-16 00:00', 6923499.0)] | oldest event age s [('10-12 00:00', 21600000.0), ('10-13 00:00', 21600035.0), ('10-14 00:00', 21600266.0), ('10-15 00:00', 21600028.0), ('10-16 00:00', 21600283.0)] | dropped [('10-12 00:00', 320582.0), ('10-13 00:00', 3594978.0), ('10-14 00:00', 3407921.0), ('10-15 00:00', 3541235.0), ('10-16 00:00', 4507522.0)]
- `00:38:31` account-wide Throttles per day: [('10-05 00:00', 421.0), ('10-06 00:00', 423.0), ('10-07 00:00', 437.0), ('10-08 00:00', 486.0), ('10-09 00:00', 417.0), ('10-10 00:00', 479.0), ('10-11 00:00', 395.0), ('10-12 00:00', 90653.0), ('10-13 00:00', 113787.0), ('10-14 00:00', 118788.0), ('10-15 00:00', 120534.0), ('10-16 00:00', 96799.0), ('10-17 00:00', 254.0)]
- `00:38:31` account-wide ConcurrentExecutions per day: [('10-05 00:00', 64.0), ('10-06 00:00', 114.0), ('10-07 00:00', 75.0), ('10-08 00:00', 321.0), ('10-09 00:00', 93.0), ('10-10 00:00', 654.0), ('10-11 00:00', 50.0), ('10-12 00:00', 767.0), ('10-13 00:00', 773.0), ('10-14 00:00', 776.0), ('10-15 00:00', 775.0), ('10-16 00:00', 777.0), ('10-17 00:00', 1.0)]
- `00:38:31` account-wide Invocations per day: [('10-05 00:00', 88251.0), ('10-06 00:00', 25707.0), ('10-07 00:00', 41947.0), ('10-08 00:00', 23748.0), ('10-09 00:00', 24269.0), ('10-10 00:00', 17035.0), ('10-11 00:00', 15523.0), ('10-12 00:00', 4886191.0), ('10-13 00:00', 7155216.0), ('10-14 00:00', 7427168.0), ('10-15 00:00', 7401289.0), ('10-16 00:00', 6313085.0), ('10-17 00:00', 1.0)]
- `00:38:31` account-wide UnreservedConcurrentExecutions per day: [('10-05 00:00', 64.0), ('10-06 00:00', 114.0), ('10-07 00:00', 75.0), ('10-08 00:00', 321.0), ('10-09 00:00', 93.0), ('10-10 00:00', 654.0), ('10-11 00:00', 50.0), ('10-12 00:00', 764.0), ('10-13 00:00', 764.0), ('10-14 00:00', 764.0), ('10-15 00:00', 764.0), ('10-16 00:00', 769.0)]
## 5. Logs

- `00:38:31` log groups: {'justhodl-miss-calibrator': [{'name': '/aws/lambda/justhodl-miss-calibrator', 'retention_days': 14, 'stored_gb': 8.8}], 'justhodl-event-coordinator': [{'name': '/aws/lambda/justhodl-event-coordinator', 'retention_days': 14, 'stored_gb': 6.7}]}
- `00:38:33`   CAL 2026-10-12 09:15:12.902 [miss-cal] telegram: HTTP Error 401: Unauthorized
- `00:38:33`   CAL 2026-10-12 09:15:12.903 [miss-cal] 3 proposals, 50 universe candidates, 0 skipped deprecated
- `00:38:33`   CAL 2026-10-12 09:15:13.030 REPORT RequestId: d628354d-1380-45b3-b4ea-bf5f1931ac51	Duration: 1054.03 ms	Billed Duration: 1473 ms	Memory Size: 512 MB	Max Memory Used: 112 MB	Init Duration: 
- `00:38:33`   CAL 2026-10-12 09:15:14.667 [miss-cal] telegram: HTTP Error 401: Unauthorized
- `00:38:33`   CAL 2026-10-12 09:15:14.667 [miss-cal] 3 proposals, 50 universe candidates, 0 skipped deprecated
- `00:38:33`   CAL 2026-10-12 09:15:14.749 REPORT RequestId: db67dd26-3bfc-4d68-896a-33a618e46673	Duration: 715.19 ms	Billed Duration: 716 ms	Memory Size: 512 MB	Max Memory Used: 114 MB	
XRAY TraceId: 1-
- `00:38:33`   CAL 2026-10-12 09:15:15.206 [miss-cal] ssm put fail /justhodl/calibration/miss_threshold_proposals: An error occurred (TooManyUpdates) when calling the PutParameter operation:
- `00:38:33`   CAL 2026-10-12 09:15:15.252 [miss-cal] ssm put fail /justhodl/calibration/miss_recurring_tickers: An error occurred (TooManyUpdates) when calling the PutParameter operation:
- `00:38:33`   CAL 2026-10-12 09:15:15.503 [miss-cal] telegram: HTTP Error 401: Unauthorized
- `00:38:33`   CAL 2026-10-12 09:15:15.504 [miss-cal] 3 proposals, 50 universe candidates, 0 skipped deprecated
- `00:38:33`   CAL 2026-10-12 09:15:15.575 [miss-cal] 3 proposals, 50 universe candidates, 0 skipped deprecated
- `00:38:33`   CAL 2026-10-12 09:15:15.575 [miss-cal] telegram: HTTP Error 401: Unauthorized
- `00:38:33`   CAL 2026-10-12 09:15:15.589 REPORT RequestId: be4d9b76-8b1f-406e-b886-e80af8eaaca5	Duration: 617.64 ms	Billed Duration: 618 ms	Memory Size: 512 MB	Max Memory Used: 115 MB	
XRAY TraceId: 1-
- `00:38:33`   CAL 2026-10-12 09:15:15.686 REPORT RequestId: df0dd401-d38b-4db9-b718-481b8f302e7d	Duration: 954.05 ms	Billed Duration: 1478 ms	Memory Size: 512 MB	Max Memory Used: 112 MB	Init Duration: 5
- `00:38:33`   CAL 2026-10-12 09:15:16.067 [miss-cal] ssm put fail /justhodl/calibration/miss_threshold_proposals: An error occurred (TooManyUpdates) when calling the PutParameter operation:
- `00:38:33`   CAL 2026-10-12 09:15:16.130 [miss-cal] ssm put fail /justhodl/calibration/miss_recurring_tickers: An error occurred (TooManyUpdates) when calling the PutParameter operation:
- `00:38:33`   CAL 2026-10-12 09:15:16.378 [miss-cal] 3 proposals, 50 universe candidates, 0 skipped deprecated
- `00:38:33`   CAL 2026-10-12 09:15:16.378 [miss-cal] telegram: HTTP Error 401: Unauthorized
- `00:38:33`   CAL 2026-10-12 09:15:16.409 [miss-cal] telegram: HTTP Error 401: Unauthorized
- `00:38:33`   CAL 2026-10-12 09:15:16.409 [miss-cal] 3 proposals, 50 universe candidates, 0 skipped deprecated
- `00:38:36`   COORD 2026-10-12 09:15:14.534 [coordinator] routed event=near_miss.extreme invokes=1
- `00:38:36`   COORD 2026-10-12 09:15:14.537 REPORT RequestId: a1c5d731-4de8-45ec-8602-7a620958737f	Duration: 739.13 ms	Billed Duration: 1199 ms	Memory Size: 256 MB	Max Memory Used: 105 MB	Init Duration: 4
- `00:38:36`   COORD 2026-10-12 09:15:14.618 [coordinator] routed event=near_miss.extreme invokes=1
- `00:38:36`   COORD 2026-10-12 09:15:14.620 REPORT RequestId: 55db7a48-285e-40d3-9ac9-494a8ddb427e	Duration: 762.43 ms	Billed Duration: 1182 ms	Memory Size: 256 MB	Max Memory Used: 105 MB	Init Duration: 4
- `00:38:36`   COORD 2026-10-12 09:15:14.642 [coordinator] routed event=near_miss.extreme invokes=1
- `00:38:36`   COORD 2026-10-12 09:15:14.644 REPORT RequestId: fa4205bd-cbcb-4fcf-9109-bc7d212c5376	Duration: 786.80 ms	Billed Duration: 1317 ms	Memory Size: 256 MB	Max Memory Used: 105 MB	Init Duration: 5
- `00:38:36`   COORD 2026-10-12 09:15:15.234 [coordinator] routed event=near_miss.extreme invokes=1
- `00:38:36`   COORD 2026-10-12 09:15:15.236 REPORT RequestId: 485d262b-e50a-4ed5-90b7-d3add49b52e9	Duration: 395.63 ms	Billed Duration: 396 ms	Memory Size: 256 MB	Max Memory Used: 105 MB	
XRAY TraceId: 1-
- `00:38:36`   COORD 2026-10-12 09:15:15.257 [coordinator] routed event=near_miss.extreme invokes=1
- `00:38:36`   COORD 2026-10-12 09:15:15.259 REPORT RequestId: cdad8947-4430-4076-98fa-70769d98f809	Duration: 425.93 ms	Billed Duration: 426 ms	Memory Size: 256 MB	Max Memory Used: 105 MB	
XRAY TraceId: 1-
- `00:38:36`   COORD 2026-10-12 09:15:15.270 [coordinator] routed event=near_miss.extreme invokes=1
- `00:38:36`   COORD 2026-10-12 09:15:15.272 REPORT RequestId: 407ecf0f-54b6-44a5-ba16-34d4d02b16d2	Duration: 440.02 ms	Billed Duration: 441 ms	Memory Size: 256 MB	Max Memory Used: 105 MB	
XRAY TraceId: 1-
- `00:38:36`   COORD 2026-10-12 09:15:16.089 [coordinator] routed event=near_miss.extreme invokes=1
- `00:38:36`   COORD 2026-10-12 09:15:16.091 REPORT RequestId: 4036489e-c526-4c66-abb6-be6273f40954	Duration: 394.10 ms	Billed Duration: 395 ms	Memory Size: 256 MB	Max Memory Used: 105 MB	
XRAY TraceId: 1-
- `00:38:36`   COORD 2026-10-12 09:15:16.108 [coordinator] routed event=near_miss.extreme invokes=1
- `00:38:36`   COORD 2026-10-12 09:15:16.110 REPORT RequestId: ae908817-00a1-41f8-b361-d3a7401d19af	Duration: 422.10 ms	Billed Duration: 423 ms	Memory Size: 256 MB	Max Memory Used: 105 MB	
XRAY TraceId: 1-
- `00:38:40` oct15_1200 justhodl-miss-calibrator: [{'runs': '17493', 'avg_ms': '15347.0758', 'p50_ms': '15268.0411', 'p95_ms': '21679.3738', 'max_mem_mb': '124', 'mem_mb': '512'}] | kinds [{'kind': 'ssm put fail', 'n': '33280'}, {'kind': 'telegram', 'n': '17490'}, {'kind': 'proposals,', 'n': '17490'}, {'kind': 'SlowDown', 'n': '2'}] (scanned 28.9+28.9 MB)
- `00:38:45` oct15_1200 justhodl-event-coordinator: [{'runs': '31015', 'avg_ms': '5880.5591', 'p50_ms': '3967.4873', 'p95_ms': '14125.7718', 'max_mem_mb': '172', 'mem_mb': '256'}] | kinds [{'kind': 'telegram', 'n': '30992'}, {'kind': 'routed event=near_miss.extreme', 'n': '31013'}, {'kind': 'audit', 'n': '12421'}] (scanned 26.0+26.0 MB)
- `00:38:50` oct05_monday justhodl-miss-calibrator: [{'runs': '12', 'avg_ms': '752.0567', 'p50_ms': '695.99', 'p95_ms': '1100.07', 'max_mem_mb': '116', 'mem_mb': '512'}] | kinds [{'kind': 'telegram', 'n': '12'}, {'kind': 'proposals,', 'n': '12'}, {'kind': 'ssm put fail', 'n': '2'}] (scanned 0.0+0.0 MB)
- `00:38:54` oct05_monday justhodl-event-coordinator: [{'runs': '38', 'avg_ms': '172.6029', 'p50_ms': '1.65', 'p95_ms': '691.25', 'max_mem_mb': '106', 'mem_mb': '256'}] | kinds [{'kind': 'routed event=jhsignal.batch_published', 'n': '2'}, {'kind': 'routed event=near_miss.extreme', 'n': '11'}, {'kind': 'telegram', 'n': '11'}] (scanned 0.0+0.0 MB)
## 6. Branching factor (miss-summary history)

- `00:38:55` now (written 2026-10-15T06:54:29+00:00): 5 signal types, 3 with >= 50 near-misses: {'screener_top_pick': 185, 'momentum_top_pick': 141, 'valuation_composite': 128}
- `00:38:55` 2 stored versions; per day (time, signal types >= 50, max count): [('10-14 06:59', 3, 185), ('10-15 06:54', 3, 185)]
- `00:38:55` ✅ wrote aws/ops/reports/6530.json
