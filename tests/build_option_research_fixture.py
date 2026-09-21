"""Build a synthetic source-bound browser fixture without network or credentials."""
from pathlib import Path
from unittest.mock import patch
import json,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared')]
import option_flow_store as store
import option_flow_research as model
from test_option_flow_research import fixture
from test_option_flow_store import S3


def build():
    with patch.object(model,'CONTINUITY',('SPY',)):
        blobs,inputs=fixture();s3=S3(blobs);read=store.reader(s3,'synthetic')
        with store.ArtifactWriter(s3,'synthetic',read) as emit:output=model.build(inputs,read,emit)
        replay=store.retain(s3,'synthetic',inputs,output,read)
    publication={**output,'replay':replay}
    public={k:v.decode('utf-8') for k,v in s3.data.items() if k.startswith(model.PREFIX) and '/compilers/' not in k and '/inputs/' not in k}
    public[model.CURRENT]=model.encoded(publication).decode('utf-8')
    return {'synthetic':True,'publication':publication,'artifacts':public}


if __name__=='__main__':
    path=ROOT/'tests/fixtures/option-native.json';raw=model.encoded(build())+b'\n'
    if '--check' in sys.argv:
        assert path.read_bytes()==raw,'Synthetic option fixture is stale'
    else:path.write_bytes(raw)
