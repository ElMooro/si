"""Do not promote a partial, merely green, or self-qualified population."""
from copy import deepcopy
from pathlib import Path
import sys, types, unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/ops/staged'))
# resource only exposes a report metric; Windows offline tests have no Unix module.
if sys.platform=='win32':sys.modules.setdefault('resource',types.SimpleNamespace(RUSAGE_SELF=0))
import ops_6123_capital_structure_private_readiness as operation


def accepted():
    return {'status':'complete','request_id':operation.candidate.REQUEST,
        'counts':{'reported_names':1615,'provider_responses':11305,'provider_rows':59256},
        'qualification':{'all_original_rows_conserved':True,'production_measurement_formulas_imported':False,
                         'forecast_qualified':False,'sizing_qualified':False},'public_artifacts_checked':1626}


class Tests(unittest.TestCase):
    def test_only_full_independently_checked_candidate_is_accepted(self):
        good=accepted();self.assertIs(operation.accepted_population(good),good)
        changes=[('status','failed'),('request_id','other'),('public_artifacts_checked',1625),
            ('counts.provider_responses',11304),('counts.provider_rows',59255),('counts.reported_names',1614),
            ('qualification.all_original_rows_conserved',False),('qualification.production_measurement_formulas_imported',True),
            ('qualification.forecast_qualified',True),('qualification.sizing_qualified',True)]
        for key,value in changes:
            bad=deepcopy(good);target=bad;parts=key.split('.')
            for part in parts[:-1]:target=target[part]
            target[parts[-1]]=value
            with self.subTest(key=key),self.assertRaises(ValueError):operation.accepted_population(bad)

    def test_readiness_wrapper_has_no_live_mutation_or_provider_acquisition(self):
        text=Path(operation.__file__).read_text(encoding='utf-8')
        for forbidden in ('.invoke(','.update_function_code(','.update_function_configuration(','.put_rule(','.get_parameter(','.urlopen('):
            self.assertNotIn(forbidden,text)
        self.assertIn('sys.exit(1)',text)
        self.assertIn("ready['replay'] != accepted['replay']",text)
        self.assertIn('producer.raw(s3, BUCKET, producer.CURRENT) != old',text)


if __name__=='__main__':unittest.main(verbosity=2)
