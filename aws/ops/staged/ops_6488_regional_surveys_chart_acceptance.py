"""Read-only exact market source/control acceptance; no producer or credential access."""
from pathlib import Path
import hashlib,json,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from market_runtime_evidence import runtime,BUCKET
SPECS={'justhodl-symdir': {'source_hashes': {'aws/lambdas/justhodl-symdir/source/bis-fx-series.json': 'ad2bd58847a911c12d7bb0ab209009434bc901485f6adaf63286545a87bd5c27', 'aws/lambdas/justhodl-symdir/source/bis-policy-series.json': '85e7f0b36de1c2baa1ec22846a8e02e40b1ffab7482a1206438d0cc6d23f45c0', 'aws/lambdas/justhodl-symdir/source/bis_fx_cross.py': 'd595e607966fd1b4f485fa44be1364058cdd5a6dc187a19e2c38b46551297852', 'aws/lambdas/justhodl-symdir/source/bis_fx_series.py': '1d92a9f9bcb9dd7575084d738f96c373496669549890042b39f817ae9abbf126', 'aws/lambdas/justhodl-symdir/source/bis_policy_series.py': '1fce97cb3f98a95fb64d635520e9bb25b3936df03182acfd0a8a4605d82840e0', 'aws/lambdas/justhodl-symdir/source/bis_reviewed_series.py': '0d0edd667de1b940109db96cbf56b6ffc1a1af8d221217cb857258806fdeb2ea', 'aws/lambdas/justhodl-symdir/source/cboe-indices.json': '750d0e55ce141ca8ff12d068c134c66ebc16efedfa3b3493cfd1e6893a6bbafc', 'aws/lambdas/justhodl-symdir/source/cboe_index.py': 'c60af77e59ad7eec1a671bd35263430a9762f28a21d7f7dc14ca065e881c0ccc', 'aws/lambdas/justhodl-symdir/source/census-series.json': '66ab9ddcba21d189407c84affe4e78838f647e50d858254230a04e94a3d75c91', 'aws/lambdas/justhodl-symdir/source/census_series.py': 'c3e19fd938a529493383d341afb785f3122f7ef1d344600599d9c44d9dc28cab', 'aws/lambdas/justhodl-symdir/source/cftc-series.json': '5af4de91d873f415b76e50c06ec9c2d0428498394313fd87cee56020796d5294', 'aws/lambdas/justhodl-symdir/source/cftc_series.py': 'ce8435383b72aaea7ad14fe57ceb47f831272fa12362d6cc9cb54f959a475389', 'aws/lambdas/justhodl-symdir/source/defillama-tvl.json': 'd7a8b812ca91d3a45affa53067c82f4ed4cf88f15bdd4dac4b6df385803d8130', 'aws/lambdas/justhodl-symdir/source/defillama_tvl.py': '593b980142af354a810301a9064ba489350c7c453795716a5aefb98bb29e89b2', 'aws/lambdas/justhodl-symdir/source/directory_identity.py': '2022ec3d4067ac603e7e1eac5db0e8941c3f3af4beb47d973b4ebbfa9b87df51', 'aws/lambdas/justhodl-symdir/source/directory_index.py': 'a16ff08a9bf9c67a33498b6db4cb3547dafb1ef307b5c948d0fc9091d63198bb', 'aws/lambdas/justhodl-symdir/source/directory_publication.py': 'dcde7e1810e01c76daa24ebcf14b7345a8196f16a9c60e1dc28f93b46a5940a6', 'aws/lambdas/justhodl-symdir/source/directory_resident.py': '86b6ebbe5d956066b518277b72564143131be749f881ba26acb2b612cb5b5e0b', 'aws/lambdas/justhodl-symdir/source/imf-series.json': '42602c214b3b629f8f1bb2e2db5d33bfca02e00042283f0cafefe4eaba385ffd', 'aws/lambdas/justhodl-symdir/source/imf_series.py': 'a98fe7fcd51f5b40ac4b818b5e0418fc1415c3fbdda24c3d626664b419222514', 'aws/lambdas/justhodl-symdir/source/lambda_function.py': '71bfb5fe08276a4892be85178a89a905be4e3c926bbce5a43ec3a7387e70e051', 'aws/lambdas/justhodl-symdir/source/oecd-additional-series.json': '907f3d7d644f43461f2a3a78cd97b7eea7310d23d8936eab793a4604fbc926c6', 'aws/lambdas/justhodl-symdir/source/oecd-series.json': 'c07696528e2031be7fd9157187aec438950aff643f59edca248b3e4f34a14c54', 'aws/lambdas/justhodl-symdir/source/oecd_series.py': 'f0693b2ae75b91e20c8fabb945f9823026ae332d17463b2cf4a7f93be5fb1164', 'aws/lambdas/justhodl-symdir/source/regional-fed-series.json': 'fd4f0ea566aac7bd36a4a395440c3d4761bbd3a6fbf492e428e932fb36d8b719', 'aws/lambdas/justhodl-symdir/source/regional_fed.py': '40db3873f789cc1f4de973767348eb78f1be98064e646b35bac637fd748ddb7e', 'aws/lambdas/justhodl-symdir/source/regional_fed_parser.py': '025269122e95067d45e31c315059c450cf4bf0d6445e99f8d152f80f7317b62d', 'aws/lambdas/justhodl-symdir/source/regional_survey_parser.py': 'c1d1f9a3c2e049a2ab79dcdb3173f151dff37a295f2a22fdacbeb6dd103d25db', 'aws/lambdas/justhodl-symdir/source/reviewed_symbol_config.py': 'ed7db69bec9b2da4e900e5dbf1052cc327648cc955419e0ccd8fa7c0c683ed34', 'aws/lambdas/justhodl-symdir/source/tv-symbol-resolver.json': '0580a6bc5546140511cfd1b877a99aa19215bdec8281ed9b89ace8a42ac23db1', 'aws/lambdas/justhodl-symdir/source/warehouse_cache.py': '1f5f733ccd5145296c3f9488eabd2e68f133f023d66b88a1f638b4830903e461', 'aws/lambdas/justhodl-symdir/source/warehouse_routing.py': 'a050b681540df839447db270e9adc9f15a1f0260b8ee420ad48831142983bdc0', 'aws/shared/treasury_fiscal_model.py': 'aa7627c565fe088662bef508dde9cf993f59e3acdd8e855a8e75b2b42af25fa3'}, 'controls': {'function_name': 'justhodl-symdir', 'timeout': 900, 'memory_mb': 6144, 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 2048, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-symdir-build', 'state': 'ENABLED', 'expression': 'cron(40 5 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-symdir-codelists', 'state': 'ENABLED', 'expression': 'rate(20 minutes)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-symdir-fredfresh', 'state': 'ENABLED', 'expression': 'rate(1 hour)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-symdir-fredupdates', 'state': 'ENABLED', 'expression': 'rate(15 minutes)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-symdir-titles', 'state': 'ENABLED', 'expression': 'rate(1 hour)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-symdir-ustbank', 'state': 'ENABLED', 'expression': 'cron(30 21 ? * MON-FRI *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-symdir-warm', 'state': 'ENABLED', 'expression': 'rate(5 minutes)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}]}}}

class ReceiptOnly:
    def __init__(self,client,function):self.client,self.function=client,function
    def get_object(self,**kwargs):
        if kwargs!={'Bucket':BUCKET,'Key':'data/ops/releases/'+self.function+'.json'}:raise ValueError('Exact named receipt only')
        return self.client.get_object(**kwargs)

def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)

def normalized(value,function,commit):
    if not isinstance(value,dict) or value.get('function_name')!=function or function not in SPECS:raise ValueError('Native function identity differs')
    if value.get('receipt')!={'status':'matched','commit':commit}:raise ValueError('Exact source commit receipt required')
    spec=SPECS[function]
    if type(value.get('source_files_checked')) is not int or value['source_files_checked']!=len(spec['source_hashes']):raise ValueError('Exact source population required')
    if type(value.get('handler_bytes')) is not int or value['handler_bytes']<=0 or not isinstance(value.get('code_sha256'),str) or not value['code_sha256']:raise ValueError('Native code identity unavailable')
    rows=value.get('schedules')
    if not isinstance(rows,list) or not all(isinstance(row,dict) for row in rows):raise ValueError('Schedule census unavailable')
    value={**value,'schedules':sorted(rows,key=lambda row:(row.get('kind',''),row.get('group','default'),row.get('name','')))}
    expected={**spec['controls'],'schedules':sorted(spec['controls']['schedules'],key=lambda row:(row.get('kind',''),row.get('group','default'),row.get('name','')))}
    if encoded({key:value.get(key) for key in expected})!=encoded(expected):raise ValueError('Original native controls changed')
    return value

def main():
    import boto3
    from ops_report import report
    lam,s3,events,scheduler=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    all_paths=[p for spec in SPECS.values() for p in spec['source_hashes']]
    commit=subprocess.check_output(['git','log','-1','--format=%H','--',*all_paths],cwd=ROOT,text=True).strip()
    with report('ops_6488_regional_surveys_chart_acceptance') as output:
        evidence={}
        for function,spec in SPECS.items():
            for path,digest in spec['source_hashes'].items():
                if hashlib.sha256((ROOT/path).read_bytes()).hexdigest()!=digest:raise ValueError('Reviewed source changed')
            clients=(lam,ReceiptOnly(s3,function),events,scheduler)
            before=normalized(runtime(*clients,function),function,commit);after=normalized(runtime(*clients,function),function,commit)
            if encoded(before)!=encoded(after):raise ValueError('Function changed during inspection')
            evidence[function]={'native_before':before,'native_after':after,'source_hashes':spec['source_hashes']}
        output.kv(evidence={'status':'exact_regional_surveys_native_release_checked','expected_commit':commit,'functions':evidence,
          'native_invocations':0,'provider_requests':0,'engine_packet_reads':0,'private_reads':0,'account_reads':0,
          'credential_reads':0,'native_writes':0,'schedule_changes':0,'application_log_queries':0,
          'history_coverage_verified':False,'point_in_time_verified':False,
          'scope':'Only named native packages, exact release receipts and complete schedule/control census. No environment values, credentials, private packets, provider sessions, bank or index bodies.'})

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
