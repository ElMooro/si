# ops 6783 - deployed justhodl-ici-flows lambda_function.py (read-only)

**Status:** success  
**Duration:** 0.6s  
**Finished:** 2026-11-11T06:23:35+00:00  

## Log
- `06:23:35` lambda_function.py: 1312 bytes, sha256 c5292dcc3bfa6415b6b69ed5e27d1c37745c4105368ec5a4dbff090248af881b, 21 lines
- `06:23:35` <!-- ICI_HANDLER_BEGIN -->
```python
"""Native ICI release research. Opening the public endpoint cannot acquire data."""
import json,os
import boto3
from botocore.config import Config
from ici_store import CURRENT,run,qualified

BUCKET=os.environ.get('S3_BUCKET','justhodl-dashboard-live')

def lambda_handler(event=None,context=None):
    event=event if isinstance(event,dict) else {}
    if event.get('httpMethod') or (event.get('requestContext') or {}).get('http'):
        return {'statusCode':307,'headers':{'Location':'https://justhodl-data-proxy.raafouis.workers.dev/'+CURRENT+'?exact=1&nogen=1',
            'Cache-Control':'no-store'},'body':''}
    if event.get('validate_only') is True:
        qualified()
        return {'statusCode':200,'body':json.dumps({'contract':'ici-research.v1','compiler_validated':True,'published':False})}
    if not getattr(context,'aws_request_id',None):raise ValueError('Native AWS execution context required')
    client=boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=4,read_timeout=15,retries={'max_attempts':2}))
    result=run(client,BUCKET,str(event.get('id') or context.aws_request_id))
    return {'statusCode':200,'headers':{'Content-Type':'application/json'},
        'body':json.dumps({'engine_contract':'ici-research.v1','packet_key':CURRENT,**result},allow_nan=False)}

```
<!-- ICI_HANDLER_END -->
- `06:23:35` ✅ read-only
