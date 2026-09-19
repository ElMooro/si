import ast
from pathlib import Path
import unittest
import sys

ROOT=Path(__file__).resolve().parents[1]


class Tests(unittest.TestCase):
    def test_rejected_throttle_can_retry_but_ambiguous_execution_cannot(self):
        sys.path.insert(0,str(ROOT/'aws/ops'))
        from acceptance_invoke import invoke_when_available
        class Error(Exception):
            def __init__(self,code):self.response={'Error':{'Code':code}}
        class Client:
            def __init__(self,errors):self.errors=iter(errors);self.calls=0
            def invoke(self,**kwargs):
                self.calls+=1;error=next(self.errors,None)
                if error:raise error
                return {'StatusCode':200}
        times=[0]
        client=Client([Error('TooManyRequestsException'),Error('TooManyRequestsException')])
        result,rejected=invoke_when_available(client,{},100,lambda:times[0],lambda n:times.__setitem__(0,times[0]+n))
        self.assertEqual((result['StatusCode'],rejected,client.calls),(200,2,3))
        for error in (TimeoutError('uncertain'),ConnectionError('uncertain'),Error('AccessDeniedException')):
            client=Client([error])
            with self.assertRaises(type(error)):invoke_when_available(client,{})
            self.assertEqual(client.calls,1)
        client=Client([Error('TooManyRequestsException')]);
        with self.assertRaises(TimeoutError):invoke_when_available(client,{},0,lambda:0,lambda n:None)

    def test_transport_response_shapes_preserve_original_error_without_retry(self):
        sys.path.insert(0,str(ROOT/'aws/ops'))
        from acceptance_invoke import invoke_when_available
        for response in (None, [], 'disconnected', {}, {'Error':None}, {'Error':[]}, {'Error':'unavailable'}):
            with self.subTest(response=response):
                error=ConnectionError('invocation outcome uncertain')
                error.response=response
                class Client:
                    calls=0
                    def invoke(self,**kwargs):
                        self.calls+=1
                        raise error
                client=Client()
                with self.assertRaises(ConnectionError) as caught:
                    invoke_when_available(client,{})
                self.assertIs(caught.exception,error)
                self.assertEqual(client.calls,1)

    def test_missing_receipt_waits_but_permission_failure_does_not_hide(self):
        path=ROOT/'aws/ops/pending/ops_5834_inflection_research_acceptance.py'
        if not path.exists():path=ROOT/'aws/ops/ran'/path.name
        node=next(n for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name=='pending_receipt')
        scope={};exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),scope)
        class StorageError(Exception):
            def __init__(self,code):self.response={'Error':{'Code':code}}
        for code in ('NoSuchKey','404'):
            def missing(key):raise StorageError(code)
            self.assertIsNone(scope['pending_receipt'](missing,'fixture'))
        def denied(key):raise StorageError('AccessDenied')
        with self.assertRaises(StorageError):scope['pending_receipt'](denied,'fixture')
        self.assertEqual(scope['pending_receipt'](lambda key:{'commit':'x'},'fixture'),{'commit':'x'})


if __name__=='__main__':unittest.main()
