"""HTTP metadata acceptance must match browser CORS behavior."""
from email.message import Message
import io
import json
from pathlib import Path
import runpy
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
FILE='ops_5271_audit_page_api_probe.py'
SOURCE=next(p for folder in ('pending','ran') if (p:=ROOT/'aws/ops'/folder/FILE).exists())
PROBE=runpy.run_path(str(SOURCE))['probe']


def response(origins,document):
    class Response(io.BytesIO):
        status=200
        def __enter__(self):return self
        def __exit__(self,*args):self.close()
    reply=Response(json.dumps(document).encode());reply.headers=Message()
    for origin in origins:reply.headers['Access-Control-Allow-Origin']=origin
    return reply


def test_duplicate_missing_or_combined_origins_cannot_pass_http_probe():
    for origins,valid in [(['*'],True),(['https://justhodl.ai'],True),(['*','*'],False),(['*, *'],False),([],False)]:
        with patch('urllib.request.OpenerDirector.open',return_value=response(origins,{'agent':'fmp-fundamentals-agent'})):
            result=PROBE('/')
        assert result['cors_valid'] is valid


def test_probe_suppresses_error_body_and_distinguishes_provider_availability():
    doc={'agent':'fmp-fundamentals-agent','status':'UNAVAILABLE','error':'SYNTHETIC_PRIVATE_VALUE','trace':'SYNTHETIC_PRIVATE_VALUE'}
    with patch('urllib.request.OpenerDirector.open',return_value=response(['*'],doc)):result=PROBE('/')
    assert result['provider_status']=='UNAVAILABLE' and result['handler_error_present'] and result['trace_field_present']
    assert 'SYNTHETIC_PRIVATE_VALUE' not in json.dumps(result)
