"""Exercise actual thin holdings handlers and research read boundaries without AWS."""
from pathlib import Path
from unittest import mock
import ast,io,json,os,sys,types,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
import etf_holdings_model as model
import etf_holdings_store as store
from test_etf_holdings_store import Storage


def actual(short,name,ns):
    p=ROOT/'aws/lambdas'/('justhodl-'+short)/'source/lambda_function.py'
    tree=ast.parse(p.read_text(encoding='utf-8'));node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name)
    exec(compile(ast.Module(body=[node],type_ignores=[]),str(p),'exec'),ns)
    return ns[name]


def check_handlers(test):
    for kind,short,contract,key in (('holdings','etf-constituents',model.CONTRACT,model.CURRENT),
            ('lookthrough','flow-lookthrough',model.LOOK_CONTRACT,model.LOOK_CURRENT)):
        db=Storage();creator=mock.Mock(return_value=db);runner=mock.Mock(return_value={'status':'complete','published':True})
        ns={'json':json,'os':os,'boto3':types.SimpleNamespace(client=creator),'Config':lambda **kw:kw,
            'CONTRACT':contract,'KIND':kind,'PUBLISHED_KEY':key,'reader':store.reader,'run':runner,'missing':store.missing}
        ns['publish_current']=actual(short,'publish_current',ns);handler=actual(short,'lambda_handler',ns)
        test.assertEqual(handler({'validate_only':True},None)['statusCode'],200);creator.assert_not_called()
        test.assertEqual(handler({'httpMethod':'GET'},None)['statusCode'],503);runner.assert_not_called()
        db.objects[key]=b'{"legacy":true}'
        test.assertEqual(handler({'httpMethod':'GET'},None)['statusCode'],503);runner.assert_not_called()
        packet={'contract':contract,'call':None};db.objects[key]=json.dumps(packet).encode()
        test.assertEqual(json.loads(handler({'action':'current_state'},None)['body']),packet);runner.assert_not_called()
        with test.assertRaises(ValueError):handler({},None)
        ctx=types.SimpleNamespace(aws_request_id='fixture-execution',get_remaining_time_in_millis=lambda:820000)
        with mock.patch.dict(os.environ,{'POLYGON_KEY':'fixture-provider-secret'}):
            test.assertEqual(handler({'request_id':'once'},ctx)['statusCode'],200)
        test.assertEqual(runner.call_args.args[2:5],(kind,'once','fixture-execution'))
        test.assertEqual(runner.call_args.kwargs['remaining_seconds'],820)
        test.assertEqual(runner.call_args.kwargs['credential'],'fixture-provider-secret' if kind=='holdings' else '')
        db=Storage();candidate={'contract':contract,'generated_at':'2026-09-21T07:00:00Z'}
        test.assertTrue(store.conditional(db,'fixture',key,candidate,ns['publish_current']))
        with test.assertRaises(ValueError):ns['publish_current'](db,'fixture','portfolio/current.json',b'{}',{'IfNoneMatch':'*'})
        with test.assertRaises(ValueError):ns['publish_current'](db,'fixture',key,b'{}',{})


class NativeHandlers(unittest.TestCase):
    def test_actual_handlers(self):check_handlers(self)
    def test_equity_confluence_rejects_legacy_holdings_flow_scores(self):
        db=Storage();canary={'top_picks':[{'ticker':'AAPL','score':100}], 'calls_eligible':True}
        db.objects['data/flow-lookthrough.json']=json.dumps(canary).encode()
        db.objects['data/unrelated.json']=json.dumps(canary).encode()
        reader=actual('equity-confluence','_read',{'s3':db,'BUCKET':'fixture','json':json})
        self.assertEqual(reader('data/flow-lookthrough.json'),{})
        self.assertEqual(reader('data/unrelated.json'),canary)


if __name__=='__main__':unittest.main(verbosity=2)
