"""Inventory only the already-public prospective archive for Genealogy repair.

No learning ledger, account, recipient, source-engine body, provider request or
native invocation. This lists complete object metadata; no record body is read.
"""
from datetime import datetime,timezone
from pathlib import Path
import hashlib,json,re,sys
ROOT=Path(__file__).resolve().parents[3]
BUCKET='justhodl-dashboard-live'
PREFIXES=('data/research-forecasts/captures/','data/research-forecasts/records/')

def inventory(client,prefix,cutoff):
    if prefix not in PREFIXES or cutoff.tzinfo is None:raise ValueError('Reviewed public prefix and UTC cutoff required')
    rows=[];seen=set();later=0;pages=0
    for page in client.get_paginator('list_objects_v2').paginate(Bucket=BUCKET,Prefix=prefix):
        pages+=1
        for obj in page.get('Contents',[]):
            key=obj['Key'];stamp=obj['LastModified'];size=obj['Size']
            if not re.fullmatch(re.escape(prefix)+r'[a-f0-9]{64}\.json',key):raise ValueError('Unexpected public archive key')
            if key in seen:raise ValueError('Duplicate archive listing key')
            seen.add(key)
            if not isinstance(stamp,datetime) or stamp.tzinfo is None or type(size) is not int or size<1:raise ValueError('Invalid archive object metadata')
            if stamp>cutoff:later+=1;continue
            rows.append({'key':key,'bytes':size,'last_modified':stamp.astimezone(timezone.utc).isoformat()})
    rows.sort(key=lambda r:r['key'])
    raw=json.dumps(rows,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
    return {'prefix':prefix,'cutoff':cutoff.astimezone(timezone.utc).isoformat(),'listing_pages':pages,
      'objects':rows,'objects_at_cutoff':len(rows),'objects_after_cutoff':later,
      'total_bytes':sum(r['bytes'] for r in rows),'largest_object_bytes':max((r['bytes'] for r in rows),default=0),
      'earliest_storage_time':min((r['last_modified'] for r in rows),default=None),
      'latest_storage_time':max((r['last_modified'] for r in rows),default=None),'inventory_sha256':hashlib.sha256(raw).hexdigest(),
      'listing_complete':True,'record_bodies_verified':False,'registration_clocks_verified':False,
      'scope':'Complete listed metadata at a fixed cutoff. Storage timestamps are not signal firing times; content and registration clocks require separate verification.'}

def main():
    import boto3
    sys.path.insert(0,str(ROOT/'aws/ops'))
    from ops_report import report
    client=boto3.client('s3',region_name='us-east-1');cutoff=datetime.now(timezone.utc)
    with report('ops_6303_public_research_archive_inventory') as r:
        rows={prefix:inventory(client,prefix,cutoff) for prefix in PREFIXES}
        r.kv(inventories=rows,metadata_only=True,record_body_reads=0,learning_ledger_reads=0,
             private_account_reads=0,downstream_output_reads=0,provider_requests=0,credential_reads=0,
             native_invocations=0,public_writes=0,history_writes=0,schedule_changes=0,
             scope='Capacity and available-date inventory for the approved public prospective archive. No current engine output or private learning ledger was read; this is not chronology or forecast qualification.')

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
