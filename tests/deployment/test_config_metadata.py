"""Selected configuration fails early; environment strings never use delimiters."""
import json
import runpy
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
validate = runpy.run_path(str(ROOT / "scripts/validate_lambda_configs.py"))["validate_configs"]
environment = runpy.run_path(str(ROOT / "scripts/lambda_config_environment.py"))["config_environment"]


def test_reviewed_recovery_descriptions_fit_api_limit():
    names = "backend-agent domain-barometers etf-constituents eurodollar-plumbing notes-intel research-backtest sector-rotation squeeze-fuel tax-plan wealth-plan".split()
    assert validate(ROOT, ["justhodl-"+name for name in names]) == []


def test_exact_metadata_boundary_rejects_before_production_mutation_without_prose():
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        folder = root / "aws/lambdas/fixture"
        folder.mkdir(parents=True)
        path = folder / "config.json"
        path.write_text(json.dumps({"description":"x"*256}))
        assert validate(root, ["fixture", "missing"]) == []
        path.write_text(json.dumps({"description":"SYNTHETIC_PRIVATE_CANARY"+"x"*257}))
        errors = validate(root, ["fixture"])
        assert len(errors) == 1 and errors[0]["field"] == "description"
        assert "SYNTHETIC_PRIVATE_CANARY" not in json.dumps(errors)
    workflow = (ROOT / ".github/workflows/deploy-lambdas.yml").read_text()
    assert workflow.index("python3 scripts/validate_lambda_configs.py") < workflow.index("- name: Prime selected governed aliases")
    shell = (ROOT / "scripts/deploy_lambdas.sh").read_text()
    assert shell.index("python3 scripts/validate_lambda_configs.py") < shell.index("aws lambda update-function-code")


def test_config_values_keep_commas_equals_newlines_and_unicode_exactly():
    values = {"ALLOWLIST":"NVDA,MSFT", "TOKEN":"a=b,c==d", "MULTILINE":"a\nb\n", "UNICODE":"日本,東京=1"}
    def forbidden(name): raise AssertionError("No inheritance must make no source calls")
    assert environment({"env":values}, forbidden) == values
    assert environment({"environment":values}, forbidden) == values


def test_inherited_values_remain_exact_and_keep_declared_precedence():
    values = {"one":{"TOKEN":"first,value=="}, "two":{"TOKEN":"second,value=\n", "EMPTY":"", "LIST":"x,y"}}
    config = {"env":{"TOKEN":"declared", "KEEP":"as,is=", "EMPTY":"retain"}, "inherit_env":[
        {"from_function":"one", "keys":["TOKEN","MISSING"]},
        {"from_function":"two", "keys":["TOKEN","LIST","EMPTY"]}]}
    result = environment(config, lambda name:values[name])
    assert result == {"TOKEN":"second,value=\n", "KEEP":"as,is=", "EMPTY":"retain", "LIST":"x,y"}


def test_boolean_standard_inheritance_and_scalar_conversion():
    calls=[]
    def fetch(name):
        calls.append(name)
        return {"FMP_KEY":"a,b==c", "UNREQUESTED":"do-not-copy"}
    result=environment({"env":{"COUNT":3,"ENABLED":True}, "inherit_env":True}, fetch)
    assert calls == ["justhodl-confluence-meta"]
    assert result == {"COUNT":"3","ENABLED":"true","FMP_KEY":"a,b==c"}


def test_failed_inheritance_does_not_silently_deploy_partial_environment():
    def failed(name):raise RuntimeError("SYNTHETIC_SECRET_RESPONSE")
    try:environment({"inherit_env":{"from_function":"source","keys":["TOKEN"]}}, failed)
    except RuntimeError as error:assert str(error) == "Environment inheritance read failed"
    else:raise AssertionError("Failed inheritance accepted as empty environment")
    shell=(ROOT / "scripts/deploy_lambdas.sh").read_text()
    assert "cfg_env_json=$(python3 scripts/lambda_config_environment.py" in shell
    assert "env_kv=" not in shell and "tr ','" not in shell


def test_runtime_upgrade_is_explicit_and_does_not_apply_create_time_defaults():
    import tempfile,json
    from pathlib import Path
    import runpy
    root=Path(__file__).resolve().parents[2]
    validate=runpy.run_path(str(root/'scripts/validate_lambda_configs.py'))['validate_configs']
    with tempfile.TemporaryDirectory() as temp:
        folder=Path(temp)/'aws/lambdas/fixture';folder.mkdir(parents=True)
        (folder/'config.json').write_text(json.dumps({'update_runtime':True}))
        assert validate(temp,['fixture'])[0]['field']=='runtime'
        (folder/'config.json').write_text(json.dumps({'runtime':'python3.12','update_runtime':True}))
        assert validate(temp,['fixture'])==[]
    script=(root/'scripts/deploy_lambdas.sh').read_text()
    assert "jq -e '.update_runtime == true'" in script
    assert 'config_args+=(--runtime "$fn_runtime")' in script


def test_runtime_upgrade_is_explicit_and_preserves_legacy_imported_defaults():
    import json
    source=(ROOT/'scripts/deploy_lambdas.sh').read_text()
    assert '.update_runtime == true' in source and 'config_args+=(--runtime "$fn_runtime")' in source
    config=json.loads((ROOT/'aws/lambdas/fedliquidityapi/config.json').read_text())
    assert config['update_runtime'] is True and config['runtime']=='python3.12'
