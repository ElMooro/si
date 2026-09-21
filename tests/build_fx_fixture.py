"""Small synthetic public graph; no credentials, providers or user positions."""
from pathlib import Path
from unittest.mock import patch
import json,sys
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared')]
import fx_research_model as model
import fx_research_store as store
from test_fx_research_store import Memory
from test_fx_research_model import fixture,GENERATED
memory=Memory();originals,sources=fixture();memory.data.update(originals)
memory.data[model.LEGACY]=model.encoded({'engine':'polygon-fx-regime','version':'2.0.0','pair_data':{},'fx_roro':{}})
with patch.object(store,'now',return_value=GENERATED),patch.object(store,'collect',return_value={'sources':sources,'provider_requests':19,'source_bytes':0}):
    status=store.run(memory,'synthetic','synthetic-fx','synthetic-execution',credential='test-only')
doc={'publication':json.loads(memory.data[model.CURRENT]),'artifacts':{k:v.decode() for k,v in memory.data.items() if k.startswith(model.PREFIX) and '/compilers/' not in k}}
(ROOT/'tests/fixtures/fx-native.json').write_text(json.dumps(doc,separators=(',',':'),ensure_ascii=False)+'\n',encoding='utf-8',newline='\n')
