from pathlib import Path
import importlib.util,json,subprocess,sys

ROOT=Path(__file__).resolve().parents[2]


def test_current_options_packages_include_the_added_momentum_helper():
    path=ROOT/'aws/ops/staged/ops_6320_options_normal_publication.py'
    spec=importlib.util.spec_from_file_location('options_complete_closure',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    from release_package_evidence import shared_imports
    baselines=json.loads((ROOT/'docs/audit/2026-09-28/options-flow-original-baseline.json').read_bytes())['actual_producers']
    actual={}
    for fn,sha in module.EXPECTED.items():
        source=ROOT/'aws/lambdas'/fn/'source'
        files=[ROOT/p for p in subprocess.check_output(['git','ls-files',str(source.relative_to(ROOT))],cwd=ROOT,text=True).splitlines()]
        closure={p.relative_to(source).as_posix() for p in files}
        closure.update(p.name for p in shared_imports(ROOT,files) if not (source/p.name).exists())
        assert module.COUNTS[fn]==len(closure),(fn,module.COUNTS[fn],closure)
        cfg=baselines[fn]['runtime']
        mapping={'function_name':'FunctionName','runtime':'Runtime','handler':'Handler','timeout':'Timeout',
            'memory_mb':'MemorySize','architectures':'Architectures','role':'Role'}
        actual[fn]={k:cfg[v] for k,v in mapping.items()}
        actual[fn].update(ephemeral_storage_mb=cfg['EphemeralStorage']['Size'],schedules=baselines[fn]['schedules'],
            receipt={'status':'matched','commit':sha},source_files_checked=len(closure))
    module.check_packages(actual,baselines)
    actual['justhodl-best-ideas']['source_files_checked']=5
    try:module.check_packages(actual,baselines)
    except ValueError:pass
    else:raise AssertionError('Incomplete Best Ideas source closure accepted')
