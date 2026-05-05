
# 1) Lambda existence + state

- `17:59:57`     ✓ exists, state=Active, mod=2026-05-05T17:47:41.000+0000
- `17:59:57`     mem=1024MB timeout=600s
- `17:59:57`     handler=lambda_function.lambda_handler

# 2) EventBridge schedule

- `17:59:58`     rule: justhodl-insider-cluster-scanner-daily  expr=cron(30 14 * * ? *)  state=ENABLED

# 3) Last CloudWatch logs (any execution attempts?)

- `17:59:58`     stream: 2026/05/05/[$LATEST]fec23db119824bb28be7141d3e40f75c  last_event: 1778003518749
- `17:59:58`       INIT_START Runtime Version: python:3.12.mainlinev2.v7	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:e4ab553846c4e081013ff7d1d608a5358d5b956bb5b81c83c66d2a31da8f6244
- `17:59:58`       START RequestId: 0ac865c3-299c-4ce6-a89e-7d9349fb422c Version: $LATEST
- `17:59:58`       [insider-cluster] starting, lookback=30d
- `17:59:58`       [insider-cluster] pulling daily index for 10 business days: 2026-05-05 → 2026-04-22
- `17:59:58`       [insider-cluster] index error 2026-05-05: HTTP Error 403: Forbidden
- `17:59:58`       [insider-cluster] found 10104 Form 4 filings across 10 days (1 index errors)
- `17:59:58`       [insider-cluster] fetching XML for 10104 filings (workers=4)
- `17:59:58`   
- `17:59:58`     stream: 2026/05/05/[$LATEST]d773cb91eb2c4500a43697251839398a  last_event: 1778003451253
- `17:59:58`       INIT_START Runtime Version: python:3.12.mainlinev2.v7	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:e4ab553846c4e081013ff7d1d608a5358d5b956bb5b81c83c66d2a31da8f6244
- `17:59:58`       START RequestId: 759992a9-648e-4ffa-85df-1d0573e438b5 Version: $LATEST
- `17:59:58`       [insider-cluster] starting, lookback=30d
- `17:59:58`       [insider-cluster] pulling daily index for 10 business days: 2026-05-05 → 2026-04-22
- `17:59:58`       [insider-cluster] index error 2026-05-05: HTTP Error 403: Forbidden
- `17:59:58`       [insider-cluster] found 10104 Form 4 filings across 10 days (1 index errors)
- `17:59:58`       [insider-cluster] fetching XML for 10104 filings (workers=4)
- `17:59:58`   

# 4) S3 output check

- `17:59:58`     no S3 output yet: ClientError