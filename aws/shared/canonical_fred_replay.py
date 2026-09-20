"""Read and reconstruct selected canonical FRED measurements from pinned originals.

The caller supplies a bounded IAM reader. This module has no network client,
credential lookup, publication path or investment authority.
"""
from concurrent.futures import ThreadPoolExecutor
from decimal import localcontext,ROUND_HALF_EVEN
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
import hashlib,json,re
import report_observations
from research_brief_model import clock
from evidence_store import public_source_url

MAX=32*1024*1024
PREFIX='data/report-research/'
def sha(raw):return hashlib.sha256(raw).hexdigest()

def pinned_report(packet,read):
    if not isinstance(packet,dict) or packet.get('contract')!=report_observations.CONTRACT:
        raise ValueError('Canonical macro measurement contract required')
    ref=packet.get('replay') or {};key=ref.get('manifest_key','')
    if not re.fullmatch(re.escape(PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('Canonical run path required')
    raw=read(key)
    if not isinstance(raw,bytes) or len(raw)>MAX or key!=PREFIX+'runs/'+sha(raw)+'.json':raise ValueError('Canonical run bytes differ')
    manifest=json.loads(raw)
    if manifest.get('contract')!='report-research-replay.v1' or manifest.get('generated_at')!=packet.get('generated_at'):
        raise ValueError('Canonical run contract or clock differs')
    clock(packet['generated_at'])
    digest=report_observations.digest({k:v for k,v in packet.items() if k!='replay'})
    if manifest.get('output_sha256')!=digest or ref.get('output_sha256')!=digest:raise ValueError('Canonical output differs')
    compiler=Path(report_observations.__file__).read_bytes();digest=sha(compiler)
    expected={'key':PREFIX+'compilers/'+digest+'.py','sha256':digest}
    if manifest.get('compiler')!=expected or read(expected['key'])!=compiler:raise ValueError('Matching reviewed macro compiler required')
    return manifest

def original(ref,sid,kind,generated_at,read):
    url=ref.get('source_url','');parsed=urlsplit(url);query=parse_qs(parsed.query)
    expected_path='/fred/series'+('/observations' if kind=='observations' else '')
    if (kind not in ('definition','observations') or parsed.scheme!='https' or parsed.netloc!='api.stlouisfed.org'
        or parsed.path!=expected_path or query.get('series_id')!=[sid] or public_source_url(url)!=url
        or any(k.lower() in ('api_key','apikey','token') for k in query)):
        raise ValueError('Canonical FRED request identity differs')
    digest=ref.get('sha256','');request=sha(url.encode())
    if (ref.get('contract')!='source-evidence.v1' or ref.get('captured') is not True or ref.get('provider')!='fred'
        or not re.fullmatch('[a-f0-9]{64}',digest) or ref.get('key')!='data/evidence/fred/'+request+'/'+digest+'.bin.gz'
        or type(ref.get('bytes')) is not int or not 0<ref['bytes']<=MAX):
        raise ValueError('Canonical FRED original identity differs')
    if clock(ref['first_received_at'])>clock(generated_at):raise ValueError('Future original receipt')
    # Reader returns bounded decompressed provider bytes, not the gzip container.
    raw=read(ref['key'])
    if not isinstance(raw,bytes) or len(raw)!=ref['bytes'] or sha(raw)!=digest:raise ValueError('Canonical FRED original bytes differ')
    return json.loads(raw)

def restore(packet,series,read):
    series=tuple(series)
    if not series or len(series)>100 or len(set(series))!=len(series) or any(not re.fullmatch('[A-Z0-9]+',sid) for sid in series):
        raise ValueError('Unique reviewed FRED series identities required')
    manifest=pinned_report(packet,read)
    def one(sid):
        row=packet.get('measurements',{}).get(sid)
        if row is None:return sid,None
        entry=manifest['inputs'][sid]
        if set(entry['evidence'])!={'definition','observations'}:raise ValueError('Definition and observation originals required')
        acquired=entry['acquired_at']
        if clock(acquired)>clock(packet['generated_at']):raise ValueError('Future source acquisition')
        out={'evidence':entry['evidence'],'acquired_at':acquired}
        for kind,ref in entry['evidence'].items():out[kind]=original(ref,sid,kind,packet['generated_at'],read)
        with localcontext() as arithmetic:
            arithmetic.prec=28;arithmetic.rounding=ROUND_HALF_EVEN
            rebuilt=report_observations.measurement(sid,out['definition'],out['observations'],out['evidence'],packet['generated_at'],acquired)
        if rebuilt!=row:raise ValueError('Original measurement reconstruction differs: '+sid)
        return sid,out
    with ThreadPoolExecutor(max_workers=4) as pool:return dict(pool.map(one,series))
