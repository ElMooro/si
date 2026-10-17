# ops 6531 - where a storm-time run spent its time (read-only)

**Status:** success  
**Duration:** 16.9s  
**Finished:** 2026-10-17T00:46:08+00:00  

## Log
## Calibrator runs, Oct 15 12:00 UTC

- `00:45:56` justhodl-miss-calibrator run (REPORT RequestId: f6901f45-d53e-41b9-be59-d3aca914d8ce Duration: 17760.90 ms Billed Duration: 17761 ms Memory ):
- `00:45:56`     +  6880 ms (step   6880 ms)  [miss-cal] ssm put fail /justhodl/calibration/miss_threshold_proposals: An error occurred (ThrottlingException
- `00:45:56`     + 17430 ms (step  10550 ms)  [miss-cal] ssm put fail /justhodl/calibration/miss_recurring_tickers: An error occurred (ThrottlingException) 
- `00:45:56`     + 17677 ms (step    247 ms)  [miss-cal] telegram: HTTP Error 401: Unauthorized
- `00:45:56`     + 17677 ms (step      0 ms)  [miss-cal] 3 proposals, 50 universe candidates, 0 skipped deprecated
- `00:45:56` justhodl-miss-calibrator run (REPORT RequestId: 1dc08ac8-95dc-4cfe-8059-17dc9fafb9e6 Duration: 6989.69 ms Billed Duration: 6990 ms Memory Si):
- `00:45:56`     +  6646 ms (step   6646 ms)  [miss-cal] ssm put fail /justhodl/calibration/miss_recurring_tickers: An error occurred (ThrottlingException) 
- `00:45:56`     +  6908 ms (step    262 ms)  [miss-cal] 3 proposals, 50 universe candidates, 0 skipped deprecated
- `00:45:56`     +  6908 ms (step      0 ms)  [miss-cal] telegram: HTTP Error 401: Unauthorized
- `00:45:58` justhodl-miss-calibrator run (REPORT RequestId: 54bcc25d-edc9-41a7-9bfb-4495466c849a Duration: 17628.42 ms Billed Duration: 17629 ms Memory ):
- `00:45:58`     + 10644 ms (step  10644 ms)  [miss-cal] ssm put fail /justhodl/calibration/miss_threshold_proposals: An error occurred (ThrottlingException
- `00:45:58`     + 17278 ms (step   6634 ms)  [miss-cal] ssm put fail /justhodl/calibration/miss_recurring_tickers: An error occurred (ThrottlingException) 
- `00:45:58`     + 17536 ms (step    258 ms)  [miss-cal] 3 proposals, 50 universe candidates, 0 skipped deprecated
- `00:45:58`     + 17536 ms (step      0 ms)  [miss-cal] telegram: HTTP Error 401: Unauthorized
- `00:45:58` justhodl-miss-calibrator run (REPORT RequestId: ef237ff5-ad9f-4038-b0b6-bf86765cdc5f Duration: 10760.67 ms Billed Duration: 10761 ms Memory ):
- `00:45:58`     +  9671 ms (step   9671 ms)  [miss-cal] ssm put fail /justhodl/calibration/miss_threshold_proposals: An error occurred (ThrottlingException
- `00:45:58`     + 10641 ms (step    970 ms)  [miss-cal] telegram: HTTP Error 401: Unauthorized
- `00:45:58`     + 10641 ms (step      0 ms)  [miss-cal] 3 proposals, 50 universe candidates, 0 skipped deprecated
- `00:46:01` justhodl-miss-calibrator run (REPORT RequestId: e85fd10c-bbfe-4852-aa58-14187fd755a1 Duration: 14868.15 ms Billed Duration: 14869 ms Memory ):
- `00:46:01`     +  6170 ms (step   6170 ms)  [miss-cal] ssm put fail /justhodl/calibration/miss_threshold_proposals: An error occurred (ThrottlingException
- `00:46:01`     + 14536 ms (step   8366 ms)  [miss-cal] ssm put fail /justhodl/calibration/miss_recurring_tickers: An error occurred (ThrottlingException) 
- `00:46:01`     + 14781 ms (step    245 ms)  [miss-cal] telegram: HTTP Error 401: Unauthorized
- `00:46:01`     + 14781 ms (step      0 ms)  [miss-cal] 3 proposals, 50 universe candidates, 0 skipped deprecated
- `00:46:01` justhodl-miss-calibrator run (REPORT RequestId: 95f420f0-eeae-493b-9dfa-92190355e055 Duration: 18809.04 ms Billed Duration: 18810 ms Memory ):
- `00:46:01`     + 10050 ms (step  10050 ms)  [miss-cal] ssm put fail /justhodl/calibration/miss_threshold_proposals: An error occurred (ThrottlingException
- `00:46:01`     + 18477 ms (step   8427 ms)  [miss-cal] ssm put fail /justhodl/calibration/miss_recurring_tickers: An error occurred (ThrottlingException) 
- `00:46:01`     + 18735 ms (step    258 ms)  [miss-cal] telegram: HTTP Error 401: Unauthorized
- `00:46:01`     + 18735 ms (step      0 ms)  [miss-cal] 3 proposals, 50 universe candidates, 0 skipped deprecated
## Coordinator runs, Oct 15 12:00 UTC

- `00:46:05` justhodl-event-coordinator run (REPORT RequestId: 04e5a06c-0d5c-4f1b-8243-a6c30fdfc5f8 Duration: 968.71 ms Billed Duration: 969 ms Memory Size):
- `00:46:05`     +   298 ms (step    298 ms)  [coordinator] telegram err: HTTP Error 401: Unauthorized
- `00:46:05`     +   968 ms (step    670 ms)  [coordinator] routed event=near_miss.extreme invokes=1
- `00:46:05` justhodl-event-coordinator run (REPORT RequestId: 8b87fa25-a106-4e76-88e2-7bc8ec059e08 Duration: 1036.45 ms Billed Duration: 1037 ms Memory Si):
- `00:46:05`     +   329 ms (step    329 ms)  [coordinator] telegram err: HTTP Error 401: Unauthorized
- `00:46:05`     +  1035 ms (step    706 ms)  [coordinator] routed event=near_miss.extreme invokes=1
- `00:46:08` justhodl-event-coordinator run (REPORT RequestId: a1d6c6ed-b564-4cbe-998d-5e63ed50d6dd Duration: 1086.66 ms Billed Duration: 1087 ms Memory Si):
- `00:46:08`     +   324 ms (step    324 ms)  [coordinator] telegram err: HTTP Error 401: Unauthorized
- `00:46:08`     +  1085 ms (step    761 ms)  [coordinator] routed event=near_miss.extreme invokes=1
- `00:46:08` justhodl-event-coordinator run (REPORT RequestId: 1bf677c6-eeee-4765-be03-ded55cca6347 Duration: 1132.65 ms Billed Duration: 1133 ms Memory Si):
- `00:46:08`     +   320 ms (step    320 ms)  [coordinator] telegram err: HTTP Error 401: Unauthorized
- `00:46:08`     +  1131 ms (step    811 ms)  [coordinator] routed event=near_miss.extreme invokes=1
## When runs slowed down (Oct 12)

- `00:46:08` justhodl-miss-calibrator avg Duration ms per 5 min: [('09:15', 15373), ('09:20', 15419), ('09:25', 15366), ('09:30', 15407), ('09:35', 14941), ('09:40', 15357), ('09:45', 15381), ('09:50', 15366), ('09:55', 15392), ('10:00', 15433), ('10:05', 15454), ('10:10', 15419), ('10:15', 15428), ('10:20', 15467), ('10:25', 15352), ('10:30', 15473), ('10:35', 15443), ('10:40', 15383), ('10:45', 15336), ('10:50', 15352), ('10:55', 15445)]
- `00:46:08` justhodl-event-coordinator avg Duration ms per 5 min: [('09:15', 1348), ('09:20', 1510), ('09:25', 1667), ('09:30', 1580), ('09:35', 971), ('09:40', 1435), ('09:45', 1368), ('09:50', 1484), ('09:55', 1461), ('10:00', 1260), ('10:05', 1355), ('10:10', 1720), ('10:15', 1742), ('10:20', 1747), ('10:25', 1923), ('10:30', 2046), ('10:35', 2169), ('10:40', 1822), ('10:45', 1837), ('10:50', 1797), ('10:55', 1807)]
- `00:46:08` ✅ wrote aws/ops/reports/6531.json
