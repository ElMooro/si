"""Configuration errors may contain request values; only fixed codes reach logs."""
import contextlib
import io
import json
import runpy
import subprocess
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
SCOPE = runpy.run_path(str(ROOT / "scripts/secret_lambda_config.py"))
CANARY = "INHERITED_SECRET_VALUE_NOT_GITHUB_MASKED"


def test_create_and_update_validation_errors_never_log_request_values():
    for operation in ("create-function", "update-function-configuration"):
        streams=[];calls=[]
        def mock(command, **kwargs):
            calls.append(command);streams.append(kwargs["stderr"])
            assert kwargs["stdout"] == subprocess.DEVNULL
            kwargs["stderr"].write(("An error occurred (ValidationException) when calling the Lambda operation: "
                "Value '{Variables={TOKEN="+CANARY+"}}' at 'environment' failed validation\n").encode())
            return SimpleNamespace(returncode=254)
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            status=SCOPE["request"](operation,"justhodl-fixture",["--environment","file:///tmp/private-env.json"],run=mock)
        assert status==254 and stdout.getvalue()==""
        assert CANARY not in stderr.getvalue() and "private-env.json" not in stderr.getvalue()
        report=json.loads(stderr.getvalue())
        assert report["error_code"]=="ValidationException" and report["function"]=="justhodl-fixture"
        assert all(stream.closed for stream in streams)
        assert calls[0][:5]==["aws","lambda",operation,"--function-name","justhodl-fixture"]


def test_unrecognized_error_codes_and_client_validation_text_do_not_escape_allowlist():
    assert SCOPE["error_code"](("An error occurred ("+CANARY+") when calling op").encode())=="CLIRequestFailed"
    assert SCOPE["error_code"](("Parameter validation failed: "+CANARY).encode())=="ClientParameterValidation"


def test_success_has_no_output_and_closes_anonymous_error_file():
    streams=[]
    def mock(command,**kwargs):
        streams.append(kwargs["stderr"])
        kwargs["stderr"].write(CANARY.encode())
        return SimpleNamespace(returncode=0)
    stdout,stderr=io.StringIO(),io.StringIO()
    with contextlib.redirect_stdout(stdout),contextlib.redirect_stderr(stderr):
        assert SCOPE["request"]("create-function","justhodl-fixture",[],run=mock)==0
    assert stdout.getvalue()==stderr.getvalue()=="" and all(stream.closed for stream in streams)


def test_environment_bearing_shell_requests_use_guarded_wrapper():
    shell=(ROOT/"scripts/deploy_lambdas.sh").read_text()
    assert 'scripts/secret_lambda_config.py update-function-configuration "$fn"' in shell
    assert 'scripts/secret_lambda_config.py create-function "$fn"' in shell
    # The remaining direct configuration request carries only tracing/DLQ data.
    direct=shell[shell.index("  aws lambda update-function-configuration"):]
    direct=direct[:direct.index("  aws lambda wait")]
    assert "$env_arg" not in direct and "$cfg_env_json" not in direct
    assert "2>&1 || true" in direct
