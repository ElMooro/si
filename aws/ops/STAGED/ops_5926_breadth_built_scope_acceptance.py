"""Verify cached breadth-scope closure, exact deployed packages and final page bytes.

No consumer or producer invocation and no private account reads. Reuse the already
accepted native publication, checking its exact bytes and source-replay identity.

Source hashes bind the revision consumed by Pages. Served hashes bind the built
manifest after the reviewed metadata/theme pipeline; they are not source hashes.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,hashlib,io,json,re,subprocess,sys,urllib.request,urllib.error,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/lambdas/justhodl-market-internals/source')]
from ops_report import report
from release_package_evidence import shared_imports
from breadth_research_store import bounded
import breadth_research_model as model
BUCKET='justhodl-dashboard-live';FN='justhodl-market-internals'
COMMIT='345f294e5ca1b83f4ce8373e525b7269c71621cd'
CONSUMERS=('justhodl-playbook-engine','justhodl-symbol-dictionary','justhodl-thesis-engine','justhodl-wl-engines')
EXPECTED={**{fn:COMMIT for fn in CONSUMERS},FN:'245e5e386bca56324da8db2a5468296fc27972ba',
    'justhodl-signal-board':'3a3408be40f4c60c41beab29bdbab6952894534e'}
SOURCE_ASSETS={'market-internals.html': 'ac48b95678154c962dfd819939b90b7116df8f22f130606d606d2bcd08792686', 'jh-breadth-research.js': 'f5e24d3592aed75fa81248e2b3fe8feec47a085a2ad1125872ced2b23ab8c221'}


def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as response:
        return bounded(response)


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def runtime(lam,fn):
    source=ROOT/'aws/lambdas'/fn/'source';config_path=source.parent/'config.json'
    configuration=json.loads(config_path.read_bytes()) if config_path.exists() else {}
    request={'FunctionName':fn}
    if configuration.get('release_validation') or fn in ('justhodl-engine-fusion','justhodl-khalid-risk'):request['Qualifier']='live'
    deployed=lam.get_function(**request);cfg=deployed['Configuration'];receipt=json.loads(public('data/ops/releases/'+fn+'.json'))
    expected_commit=EXPECTED[fn]
    assert receipt['commit']==expected_commit and receipt['code_sha256']==cfg['CodeSha256'],'Exact receipt required: '+fn
    assert cfg['State']=='Active' and cfg['LastUpdateStatus']=='Successful'
    archive=bounded(urllib.request.urlopen(deployed['Code']['Location'],timeout=45))
    assert base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256'],'AWS archive hash differs: '+fn
    files=[ROOT/p for p in subprocess.check_output(['git','ls-files',str(source.relative_to(ROOT))],cwd=ROOT,text=True).splitlines()]
    expected={p.relative_to(source).as_posix():p for p in files}
    expected.update({p.name:p for p in shared_imports(ROOT,files) if not (source/p.name).exists()})
    with zipfile.ZipFile(io.BytesIO(archive)) as z:
        for name,path in expected.items():assert z.read(name)==path.read_bytes(),'Packaged source differs: '+fn+'/'+name
    alias=None
    if request.get('Qualifier'):
        a=lam.get_alias(FunctionName=fn,Name='live')
        assert a['FunctionVersion']==cfg['Version'] and not (a.get('RoutingConfig') or {}).get('AdditionalVersionWeights')
        alias={'name':'live','version':cfg['Version'],'weighted_secondary_versions':0}
    if fn==FN:assert cfg['MemorySize']==3008 and cfg['Timeout']==900
    return {'commit':expected_commit,'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}


def main():
    with report('ops_5926_breadth_built_scope_acceptance') as r:
        lam=boto3.client('lambda',region_name='us-east-1');s3=boto3.client('s3',region_name='us-east-1')
        for fn in CONSUMERS:subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/fn/'tests/run_tests.py')],cwd=ROOT,check=True)
        runtimes={fn:runtime(lam,fn) for fn in EXPECTED};r.kv(runtimes=runtimes,engine_invocations=0,private_account_reads=0,notifications_sent=0,portfolio_writes=0)
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','merge-base','--is-ancestor','19fe9ef27eb534ec84c134e08d690f1fdb946df1',pages_commit],cwd=ROOT,check=True)
        for name,digest in SOURCE_ASSETS.items():
            source_at_build=subprocess.check_output(['git','show',pages_commit+':'+name],cwd=ROOT)
            assert hashlib.sha256(source_at_build).hexdigest()==digest,'Built revision has different source: '+name
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and hashlib.sha256(clean).hexdigest()==build['files_sha256'][name],'Served bytes differ from commit-bound build manifest: '+name
            if name=='jh-breadth-research.js':assert hashlib.sha256(clean).hexdigest()=='21121fbdf69a7996830b1351e71b701091bbc168ff9be98f03997707ce99c7ba','Reviewed renderer transform differs'
            if name=='market-internals.html':assert b'jh-breadth-research.js?v=20260920-native2' in served
        previous=json.loads(public('data/breadth-research-verification.json'));raw=public('data/market-internals.json');packet=json.loads(raw)
        assert previous['runtime_commit']==EXPECTED[FN] and previous['source_replay']['replayed'] is True
        assert previous['public_sha256']==model.sha(raw) and previous['replay']==packet['replay']
        assert packet['quality']['status']=='fresh' and len(packet['source_evidence'])==253
        assert model.sha(model.encoded({k:v for k,v in packet.items() if k!='replay'}))==packet['replay']['output_sha256']
        proof={'contract':'breadth-scope-closure-acceptance.v1','verified_at':datetime.now(timezone.utc).isoformat(),
            'consumer_commit':COMMIT,'runtimes':runtimes,'pages_commit':pages_commit,'source_assets':SOURCE_ASSETS,'built_assets':{name:build['files_sha256'][name] for name in SOURCE_ASSETS},
            'source_acceptance':'data/breadth-research-verification.json','source_acceptance_sha256':model.sha(model.encoded(previous)),
            'public_sha256':model.sha(raw),'replay':packet['replay'],'native_generation':packet['generated_at'],
            'cached_vendor_scope_withheld':True,'dependent_formulas_withheld':True,
            'engine_invocations':0,'private_account_reads':0,'notifications_sent':0,'portfolio_writes':0,
            'remaining':'No issuer-master or pre-collection point-in-time history claim; no forecast, sizing or execution authority.'}
        s3.put_object(Bucket=BUCKET,Key='data/breadth-scope-verification.json',Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public('data/breadth-scope-verification.json'))==proof
        r.kv(accepted=True,proof_key='data/breadth-scope-verification.json',pages_commit=pages_commit,native_generation=packet['generated_at'])


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
