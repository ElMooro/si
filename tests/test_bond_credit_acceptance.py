from pathlib import Path
import copy,hashlib,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/ops/staged')]
import ops_6171_bond_credit_candidate as op
from test_bond_credit_candidate import fixture,AT,model


def originals():
    packet=fixture();view=model.project(model.encode(packet),AT);files={}
    for row in view['comparisons'].values():
        for c in row['components']:
            values={'definition':{'seriess':[{'id':c['series_id'],'units':'Percent'}]},'observations':{'observations':[{'date':'2026-09-25','value':'1.25'} for _ in range(11)],'series':c['series_id']}}
            for kind,value in values.items():
                raw=model.encode(value);sha=hashlib.sha256(raw).hexdigest();key='protected/'+sha
                files[key]=raw;c['originals'][kind]={'key':key,'sha256':sha,'bytes':len(raw)}
    return packet,view,files


class Tests(unittest.TestCase):
    def test_rational_checks_cover_every_original_comparison(self):
        p,v,files=originals();proof=op.original_check(p,v,files.__getitem__)
        self.assertEqual(proof['comparisons_checked'],4);self.assertEqual(proof['original_rows_checked'],8);self.assertIs(proof['forecast_qualified'],False)

    def test_changed_original_unit_date_value_or_row_cannot_pass(self):
        for field in ('unit','date','value','row','derived'):
            p,v,files=originals();row=v['comparisons']['ccc_minus_bb'];c=row['components'][0]
            if field=='row':c['original_row_index']=0
            elif field=='derived':row['value_decimal']='1'
            else:
                kind='definition' if field=='unit' else 'observations';key=c['originals'][kind]['key'];doc=json.loads(files[key])
                if field=='unit':doc['seriess'][0]['units']='Basis points'
                elif field=='date':doc['observations'][-1]['date']='2026-09-24'
                else:doc['observations'][-1]['value']='1.26'
                files[key]=model.encode(doc)
            with self.assertRaises(ValueError):op.original_check(p,v,files.__getitem__)

if __name__=='__main__':unittest.main()
