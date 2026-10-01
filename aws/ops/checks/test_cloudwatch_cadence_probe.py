"""Bounded fake clients prove private fields never enter the public report."""
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location('probe', Path(__file__).resolve().parents[1] / 'staged/ops_6422_cloudwatch_cadence_probe.py')
p = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(p)
SECRET = 'PRIVATE_TEST_SENTINEL'


class Output:
    def __init__(self): self.rows = []
    def kv(self, **row): self.rows.append(row)


class Fake:
    def __init__(self, count=1, token=False): self.count, self.token = count, token
    def get_function_configuration(self, **kw):
        return {'CodeSha256':'hash', 'Runtime':'python3.12', 'Environment':{'Variables':{'KEY':SECRET}}}
    def list_rule_names_by_target(self, **kw):
        assert kw['Limit'] == 100
        return {'RuleNames':['r'+str(i) for i in range(self.count)], 'NextToken':SECRET if self.token else None}
    def describe_rule(self, **kw): return {'State':'ENABLED','ScheduleExpression':'rate(1 hour)','Description':SECRET}
    def list_targets_by_rule(self, **kw):
        assert kw['Limit'] == 100
        return {'Targets':[{'Arn':p.ARN_PREFIX+p.FUNCTIONS[0], 'Input':SECRET}], 'NextToken':SECRET if self.token else None}
    def get_schedule(self, **kw):
        return {'State':'ENABLED','ScheduleExpression':'rate(1 hour)', 'Target':{'Arn':p.ARN_PREFIX+p.FUNCTIONS[0], 'Input':SECRET}}


class Tests(unittest.TestCase):
    def test_max_calls_and_private_projection(self):
        fake, out, reader = Fake(count=12), Output(), p.Reader()
        p.inspect(fake,fake,fake,out,reader)
        self.assertEqual(reader.calls,33)
        self.assertNotIn(SECRET,str(out.rows))
        self.assertTrue(out.rows[-1]['completed'])
        with self.assertRaisesRegex(p.Stop,'read_bound_reached'):
            reader.read(lambda: {})
    def test_pagination_marks_incomplete_without_following(self):
        fake, out, reader = Fake(token=True), Output(), p.Reader()
        p.inspect(fake,fake,fake,out,reader)
        self.assertEqual(reader.calls,11)
        self.assertTrue(out.rows[-1]['pagination_incomplete'])
        self.assertNotIn(SECRET,str(out.rows))
    def test_rule_bound_stops_before_rule_reads(self):
        fake, out, reader = Fake(count=13), Output(), p.Reader()
        with self.assertRaisesRegex(p.Stop,'rule_bound_reached'):
            p.inspect(fake,fake,fake,out,reader)
        self.assertEqual(reader.calls,2)
    def test_denial_stops_and_redacts(self):
        class Denied(Exception): response={'Error':{'Code':'AccessDeniedException','Message':SECRET}}
        def deny(): raise Denied(SECRET)
        reader=p.Reader()
        with self.assertRaisesRegex(p.Stop,'^permission_denied$'):
            reader.read(deny)
        self.assertEqual(reader.calls,1)
    def test_deadline(self):
        reader=p.Reader(); reader.deadline=0
        with self.assertRaisesRegex(p.Stop,'read_bound_reached'): reader.read(lambda:{})
        self.assertEqual(reader.calls,0)


if __name__ == '__main__': unittest.main()
