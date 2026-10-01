"""Offline benchmark: pass a directory containing public khalid/fortress/katlin JSON captures.
Does not fetch sources or reconstruct earlier Katlin inputs from a later publication.
"""
import collections
import hashlib
import json
from pathlib import Path
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'aws/lambdas/justhodl-khalid/source'))
from user_scope_evidence import project
root=Path(sys.argv[1]);payloads={k:json.loads((root/(k+'.json')).read_bytes()) for k in ['khalid','fortress','katlin']}
output=payloads['khalid'];before=json.dumps(output,separators=(',',':'),allow_nan=False)
start=time.perf_counter();scope=project(output,payloads);elapsed=time.perf_counter()-start
assert json.dumps(output,separators=(',',':'),allow_nan=False)==before
addition=json.dumps({'user_scope_evidence':scope},separators=(',',':'),allow_nan=False).encode();after={**output,'user_scope_evidence':scope}
report={'scope':'Actual public candidate inventory; separately captured native source snapshots. Mismatched clocks stay unavailable, not repaired.',
        'candidate_count':len(output['opportunity_radar']),'generated_at':output['generated_at'],
        'inputs':[{'file':k+'.json','sha256':hashlib.sha256((root/(k+'.json')).read_bytes()).hexdigest(),'bytes':len((root/(k+'.json')).read_bytes())} for k in payloads],
        'added_bytes':len(addition)-1,'exceeds_1MB':len(addition)-1>1000000,'projection_ms':round(elapsed*1000,3),
        'legacy_packet_byte_equal':True,'checks':{d['id']:dict(collections.Counter(r['checks'][i][0] for r in scope['rows'])) for i,d in enumerate(scope['definitions'])},
        'sources':scope['sources']}
(root/'with-scope.json').write_text(json.dumps(after,separators=(',',':'),allow_nan=False));print(json.dumps(report,indent=2))
