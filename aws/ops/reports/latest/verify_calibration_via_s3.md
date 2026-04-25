# Verify Loop 1 integration in justhodl-intelligence (via S3 + handler trace)

**Status:** success  
**Duration:** 3.2s  
**Finished:** 2026-04-25T10:56:15+00:00  

## Log
## 1. Inspect lambda_handler in source

- `10:56:12`   lambda_handler starts at L907, showing first 120 lines:
- `10:56:12`       1: def lambda_handler(event, context):
- `10:56:12`       2:     try:
- `10:56:12`       3:         print("=== MARKET INTELLIGENCE ENGINE v3.0 ===")
- `10:56:12`       4:         main, repo, pred = load_system_data()
- `10:56:12`       6:         print("Generating cross-system intelligence...")
- `10:56:12`       7:         report = generate_full_intelligence(main, repo, pred)
- `10:56:12`       9:         print(f"Publishing to {BUCKET}/intelligence-report.json")
- `10:56:12`      10:         body = json.dumps(report, default=str)
- `10:56:12`      11:         s3.put_object(Bucket=BUCKET, Key='intelligence-report.json', Body=body, ContentType='application/json', CacheControl='max-age=120')
- `10:56:12`      13:         # Archive
- `10:56:12`      14:         dk = datetime.now(timezone.utc).strftime('%Y/%m/%d/%H%M')
- `10:56:12`      15:         s3.put_object(Bucket=BUCKET, Key=f'archive/intelligence/{dk}.json', Body=body, ContentType='application/json')
- `10:56:12`      17:         print(f"=== DONE === Phase:{report['phase']} Khalid:{report['scores']['khalid_index']} Crisis:{report['scores']['crisis_distance']} Metrics:{len(report['metrics_table'])}")
- `10:56:12`      19:         return {
- `10:56:12`      20:             'statusCode': 200,
- `10:56:12`      21:             'body': json.dumps({
- `10:56:12`      22:                 'status': 'published',
- `10:56:12`      23:                 'phase': report['phase'],
- `10:56:12`      24:                 'khalid_index': report['scores']['khalid_index'],
- `10:56:12` 
  S3 put_object calls in source:
- `10:56:12`     Key: intelligence-report.json
- `10:56:12`     Key: archive/intelligence/{dk}.json
- `10:56:12` 
  f-string Key patterns:
- `10:56:12`     Key: archive/intelligence/{dk}.json
## 2. Re-invoke + inspect the actual response payload

- `10:56:15`   Raw payload (262B):
- `10:56:15`     {"statusCode": 200, "body": "{\"status\": \"published\", \"phase\": \"PRE-CRISIS\", \"khalid_index\": 43, \"crisis_distance\": 60, \"plumbing_stress\": 25, \"headline\": \"\\u26a0\\ufe0f PRE-CRISIS WARNING\", \"metrics\": 15, \"risks\": 3, \"data_sources\": 3}"}
## 3. Recent S3 keys under intelligence-related paths

- `10:56:15`     intelligence-report.json                                           4449B  age -0.0m
- `10:56:15`     intelligence-report.json                                           4449B  age -0.0m
- `10:56:15`     intelligence.html                                                 27710B  age 87997.7m
## 4. Read intelligence-report.json — find calibration fields

- `10:56:15`   intelligence-report.json: 4,449B, age -0.0m
- `10:56:15`   Top-level keys: ['action_required', 'data_sources', 'dxy', 'forecast', 'generated_at', 'headline', 'headline_detail', 'metrics_table', 'ml_intelligence', 'phase', 'phase_color', 'plumbing_flags', 'portfolio', 'regime', 'risks', 'scores', 'signals', 'stock_signals', 'swap_spreads', 'timestamp', 'version', 'yield_curve']
- `10:56:15`   No 'pred' key — output structure differs from expected
- `10:56:15`   'calibrated_composite' NOT in response
- `10:56:15`   'raw_composite' NOT in response
- `10:56:15`   'calibration_meta' NOT in response
- `10:56:15`   'is_meaningful' NOT in response
## 5. Most recent log stream from intelligence Lambda

- `10:56:15`   Stream: 2026/04/25/[$LATEST]bbc516425cae453eb41318c68a471e16
- `10:56:15`   Last 30 lines:
- `10:56:15`     INIT_START Runtime Version: python:3.12.mainlinev2.v7	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:2b830756c5b10ff133ebda3f21aaa7e31ccd9f7038e1342adb459865da7617a3
- `10:56:15`     START RequestId: 6c214fca-9ec8-48d0-b6e9-d7637be89d0f Version: $LATEST
- `10:56:15`     === MARKET INTELLIGENCE ENGINE v3.0 ===
- `10:56:15`     Loading data/report.json (current)...
- `10:56:15`     data/report.json: OK
- `10:56:15`     Loading repo-data.json...
- `10:56:15`     repo-data.json: OK
- `10:56:15`     Loading edge-data.json (for pred synthesis)...
- `10:56:15`     Loading flow-data.json (for pred synthesis)...
- `10:56:15`     Generating cross-system intelligence...
- `10:56:15`     Publishing to justhodl-dashboard-live/intelligence-report.json
- `10:56:15`     === DONE === Phase:PRE-CRISIS Khalid:43 Crisis:60 Metrics:15
- `10:56:15`     END RequestId: 6c214fca-9ec8-48d0-b6e9-d7637be89d0f
- `10:56:15`     REPORT RequestId: 6c214fca-9ec8-48d0-b6e9-d7637be89d0f	Duration: 892.07 ms	Billed Duration: 1302 ms	Memory Size: 256 MB	Max Memory Used: 115 MB	Init Duration: 409.14 ms
- `10:56:15` Done
