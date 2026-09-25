"""Deterministic synthetic document catalog for browser-contract regressions."""
from pathlib import Path
import json,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/ops/checks')]
from test_buyback_document_evidence import fixture
import buyback_document_evidence as model
import buyback_filing_sources as source

client,manifest,keep=fixture()
catalog=model.compile_output(keep(manifest),lambda ref:source.read(client,ref))
f=catalog['filings'][0]
base={key:catalog[key] for key in ('manifest_sha256','inspector_sha256','source_packet','reported_rows','distinct_filings','documents','original_bytes')}
base['rows']=[{**catalog['rows'][0],**{key:f[key] for key in ('issuer_cik','accession','original_sha256','original_bytes','filing_date','received_at')},'documents':len(f['documents'])}]
out={'fixture_scope':'Synthetic test issuer and documents; not live financial evidence.','catalog':catalog,'base':base}
(ROOT/'tests/fixtures/buyback-document-evidence-synthetic.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8',newline='\n')
