"""A staged $LATEST receipt is insufficient evidence of active production code."""
import importlib.util
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('runtime_identity',ROOT/'scripts/release_runtime_identity.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


class Client:
    def __init__(self,version='7',actual='expected',weighted=False):
        self.version=version;self.actual=actual;self.weighted=weighted;self.reads=[]
    def get_alias(self,**kw):
        self.reads.append(kw)
        return {'FunctionVersion':'7','RoutingConfig':{'AdditionalVersionWeights':{'6':.1}} if self.weighted else {}}
    def get_function_configuration(self,**kw):
        self.reads.append(kw)
        return {'Version':self.version,'CodeSha256':self.actual,'Environment':{'Variables':{'PRIVATE':'never return'}}}


class Tests(unittest.TestCase):
    def test_exact_receipt_does_not_certify_an_old_or_weighted_alias(self):
        receipt={'commit':'a'*40,'code_sha256':'expected'}
        for client in (Client(actual='old'),Client(version='8'),Client(weighted=True)):
            self.assertIsNone(module.active_alias(client,'fixture',receipt))
        client=Client();proof=module.active_alias(client,'fixture',receipt)
        self.assertEqual(proof,{'function':'fixture','alias':'live','version':'7','code_sha256':'expected','commit':'a'*40})
        self.assertEqual(client.reads[-1],{'FunctionName':'fixture','Qualifier':'live'})
        self.assertNotIn('Environment',proof)


def test_active_alias_identity():
    Tests('test_exact_receipt_does_not_certify_an_old_or_weighted_alias').test_exact_receipt_does_not_certify_an_old_or_weighted_alias()


if __name__=='__main__':unittest.main()
