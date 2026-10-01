import importlib.util
import json
from pathlib import Path
import unittest

SPEC=importlib.util.spec_from_file_location('baseline',Path(__file__).resolve().parents[1]/'staged/ops_6423_cloudwatch_monitor_baseline.py')
p=importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(p)
SECRET='PRIVATE_FIXTURE_SENTINEL'
class Output:
    def __init__(self):self.rows=[]
    def kv(self,**kw):self.rows.append(kw)
class Fake:
    def get_account_settings(self):return {'AccountUsage':{'FunctionCount':123,'Other':SECRET},'AccountLimit':SECRET}
    def head_object(self,**kw):
        assert kw=={'Bucket':p.BUCKET,'Key':p.KEY}
        return {'LastModified':'2020-01-01','ContentLength':12,'Metadata':{'secret':SECRET},'ETag':SECRET}
    def get_function_configuration(self,FunctionName):
        cfg=json.loads((p.ROOT/'aws/lambdas'/FunctionName/'config.json').read_text(encoding='utf-8'))
        return {'Environment':{'Variables':dict(cfg['env'],PRIVATE=SECRET)},'CodeSha256':'hash'}
class Tests(unittest.TestCase):
    def test_four_reads_and_strict_projection(self):
        fake=Fake();out=Output();reader=p.probe.Reader()
        p.inspect(fake,fake,out,reader)
        self.assertEqual(reader.calls,4)
        self.assertNotIn(SECRET,str(out.rows))
        for row in out.rows:
            if 'declared_setting_matches' in row:self.assertTrue(all(row['declared_setting_matches'].values()))
    def test_partial_environment_error_stops(self):
        class Error(Fake):
            def get_function_configuration(self,**kw):return {'Environment':{'Error':{'Message':SECRET}}}
        fake=Error();reader=p.probe.Reader()
        with self.assertRaisesRegex(p.probe.Stop,'configuration_unavailable'):p.inspect(fake,fake,Output(),reader)
        self.assertEqual(reader.calls,3)
    def test_invalid_count_stops(self):
        class Error(Fake):
            def get_account_settings(self):return {'AccountUsage':{'FunctionCount':True}}
        fake=Error();reader=p.probe.Reader()
        with self.assertRaisesRegex(p.probe.Stop,'function_count_unavailable'):p.inspect(fake,fake,Output(),reader)
        self.assertEqual(reader.calls,1)
if __name__=='__main__':unittest.main()
