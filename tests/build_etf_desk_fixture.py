"""Build browser evidence from the actual three-fund original-replay fixture."""
from pathlib import Path
from unittest import mock
import json,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
import etf_desk_store as store
from test_etf_desk_store import fixture


def main():
    with mock.patch.object(store.catalog,'DESK',('SPY','VOO','BND')),mock.patch.dict(store.flow_catalog.ETF_UNIVERSE,
            {'SPY':{'category':'broad'},'VOO':{'category':'broad'}},clear=True):
        db,inputs=fixture();read=store.reader(db,'fixture')
        with store.ArtifactWriter(db,'fixture',read) as emit:output=store.compile_output(inputs,read,emit)
        ref=store.retain(db,'fixture',inputs,output,read)
        result={'publication':{**output,'replay':ref},'artifacts':{k:v.decode('utf-8') for k,v in db.objects.items()
            if k.startswith('data/') and store.artifact(k) and k.endswith('.json')}}
        path=ROOT/'tests/fixtures/etf-desk-native.json'
        path.write_text(json.dumps(result,separators=(',',':'),ensure_ascii=False),encoding='utf-8',newline='\n')
        print(json.dumps({'path':str(path.relative_to(ROOT)),'bytes':path.stat().st_size,'public_artifacts':len(result['artifacts'])}))


if __name__=='__main__':main()
