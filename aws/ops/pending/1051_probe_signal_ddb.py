#!/usr/bin/env python3
"""ops 1051 — probe DDB signals/outcomes schema for signal-halflife engine design."""
import json, boto3, os
from datetime import datetime, timezone

ddb = boto3.client('dynamodb', region_name='us-east-1')

report = {'started_at': datetime.now(timezone.utc).isoformat()}

# Check tables exist + their schema
for table in ['justhodl-signals', 'justhodl-outcomes']:
    info = {}
    try:
        desc = ddb.describe_table(TableName=table)['Table']
        info['attrs'] = [a['AttributeName']+':'+a['AttributeType'] for a in desc.get('AttributeDefinitions', [])]
        info['keys'] = [k['AttributeName']+':'+k['KeyType'] for k in desc.get('KeySchema', [])]
        info['item_count_approx'] = desc.get('ItemCount')
        info['size_bytes_approx'] = desc.get('TableSizeBytes')
        info['gsi'] = [g['IndexName'] for g in desc.get('GlobalSecondaryIndexes', [])] or []
        # sample 5 items
        scan = ddb.scan(TableName=table, Limit=5)
        info['sample_items'] = [
            {k: list(v.values())[0] for k, v in item.items()}
            for item in scan.get('Items', [])
        ]
        # also get a count of unique signal_names if applicable
        all_items = []
        last_key = None
        while True:
            kwargs = {'TableName': table, 'Limit': 1000}
            if last_key: kwargs['ExclusiveStartKey'] = last_key
            r = ddb.scan(**kwargs)
            all_items.extend(r.get('Items', []))
            last_key = r.get('LastEvaluatedKey')
            if not last_key or len(all_items) > 10000:
                break
        info['actual_count'] = len(all_items)
        # Extract distinct engines
        from collections import Counter
        engines = Counter()
        for item in all_items:
            for k in ['engine', 'engine_name', 'signal_name', 'source', 'origin']:
                if k in item:
                    val = list(item[k].values())[0]
                    engines[val] += 1
                    break
        info['distinct_engines'] = dict(engines.most_common(20))
    except ddb.exceptions.ResourceNotFoundException:
        info['exists'] = False
    except Exception as e:
        info['error'] = str(e)[:300]
    report[table] = info

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1051.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str)[:4000])
