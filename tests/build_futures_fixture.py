"""Synthetic exact futures graph, never provider or account data."""
from pathlib import Path
from unittest.mock import patch
import json,sys
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared')]
import futures_research_model as model
import futures_research_store as store
from test_futures_research_store import Memory
from test_futures_research_model import fixture,GENERATED,add_calendar
memory=Memory();originals,sources=fixture()
for product in model.capture.PRODUCTS:add_calendar(originals,sources,product)
memory.data.update(originals)
memory.data[model.LEGACY]=model.encoded({'engine':'justhodl-polygon-futures-curves','version':'2.0.1','product_data':{},'identity':{}})
with patch.object(store,'now',return_value=GENERATED),patch.object(store,'collect',return_value={'sources':sources,'provider_requests':35,'source_bytes':0}):
    status=store.run(memory,'synthetic','synthetic-futures','synthetic-execution',credential='test-only')
doc={'publication':json.loads(memory.data[model.CURRENT]),'artifacts':{k:v.decode() for k,v in memory.data.items() if k.startswith(model.PREFIX) and '/compilers/' not in k}}
(ROOT/'tests/fixtures/futures-native.json').write_text(json.dumps(doc,separators=(',',':'),ensure_ascii=False)+'\n',encoding='utf-8',newline='\n')
