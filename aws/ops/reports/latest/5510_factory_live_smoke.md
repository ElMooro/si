- `14:43:00` ✅ Live code, roles, independent result, dual state, schedule, wall and Brain verified. No training or endpoints created.
**Status:** success  
**Duration:** 5.3s  
**Finished:** 2026-09-13T14:43:00+00:00  

## Data

| coding | functions | generation | permissions_denied | schedule |
|---|---|---|---|---|
| {'candidate': {'critical_failures': 0, 'evaluation_id': 'code-identity-v1-d804f37d7cca68c9', 'held_out': True, 'independent': True, 'n': 64, 'passed': 64, 'score': 1.0}, 'baseline': {'critical_failures': 48, 'evaluation_id': 'code-identity-v1-d804f37d7cca68c9', 'held_out': True, 'independent': True, 'n': 64, 'passed': 16, 'score': 0.25}, 'source_sha256': '857565f178505b22a11477463a55b6cdebab4f6a4b4c3cd28340db5e70e297f9', 'scope': 'one bounded repair family, not model training'} | {'justhodl-ai': {'arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-ai', 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'code_sha256': 'HbYsehtForuUbBbzrph4V927CybYZeCFsQmkQsKqhhw=', 'source_commit': 'cb195e5143a4a5fb91a51d848aea2d732bc9b81b', 'memory': 3008, 'timeout': 900, 'package_parity': True}, 'justhodl-factory-grader': {'arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-factory-grader', 'role': 'arn:aws:iam::857687956942:role/justhodl-factory-grader-role', 'code_sha256': 'LCVAMzpoaUtLTO9Au6ihBnihlxQEJy0Rz10TmCUGmMg=', 'source_commit': 'bdbbe91f61da0d7387a49108ab69f34bbd11b05a', 'memory': 256, 'timeout': 45, 'package_parity': True}, 'justhodl-student-rsi': {'arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-student-rsi', 'role': 'arn:aws:iam::857687956942:role/justhodl-student-rsi-role', 'code_sha256': 'Cgij/YDrcoGiFZr5Dax8/RzfnWIcdHziNImnLjLogLQ=', 'source_commit': 'bdbbe91f61da0d7387a49108ab69f34bbd11b05a', 'memory': 512, 'timeout': 120, 'package_parity': True}} | 1 | 11 | {'Name': 'justhodl-student-rsi-1m', 'State': 'ENABLED', 'ScheduleExpression': 'rate(1 minute)', 'service': 'EventBridge Scheduler'} |

## Log

