"""Quote evidence must stay local to the two existing private portfolio inputs."""
from pathlib import Path
import base64,gzip,hashlib,json,sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from page_sources import page_graph


def test_quote_helpers_preserve_original_page_inputs_and_primary_engines():
    graph=page_graph(ROOT,ROOT/'portfolio/index.html')
    assert graph['keys']==['portfolio/risk.json','portfolio/snapshot.json']
    assert set(graph['primary_engines'])=={'justhodl-portfolio-snapshot','justhodl-portfolio-risk'}
    assert {'jh-portfolio-quotes.js','jh-portfolio-quotes-page.js','jh-portfolio-research.js'}<=set(graph['scripts'])
    assert not graph['script_parse_errors'] and not graph['missing_scripts']


def test_complete_current_quote_browser_frames_bind_sources_and_every_provider_body():
    fixture=json.loads(gzip.decompress((ROOT/'tests/fixtures/portfolio-quotes-browser-synthetic.json.gz').read_bytes()))
    # Freeze the complete Stage 460 frame against its whole inert predecessors.
    changed={'portfolio/index.html':'index.html.txt','aws/lambdas/justhodl-portfolio-snapshot/source/lambda_function.py':'snapshot-lambda_function.py.txt','aws/lambdas/justhodl-portfolio-risk/source/portfolio_risk_model.py':'portfolio_risk_model.py.txt'}
    for path,digest in fixture['source_files'].items():
        target=ROOT/'tests/fixtures/pre-portfolio-sector-coverage'/changed[path] if path in changed else ROOT/path
        assert hashlib.sha256(target.read_bytes()).hexdigest()==digest,path
    assert set(fixture['cases'])=={'complete','partial','binary','empty','corrupt','legacy','identity'}
    for name in ('complete','partial','binary','empty'):
        case=fixture['cases'][name];snapshot=case['frame']['snapshot']
        assert len(case['writes'])==2
        for request in case['provider_attempts']:
            original=request['complete_response_body'];raw=base64.b64decode(original['body'],validate=True)
            assert len(raw)==original['bytes'] and hashlib.sha256(raw).hexdigest()==original['sha256']
            symbol=request['url'].split('/ticker/',1)[1].split('/prev',1)[0]
            retained=snapshot['accounting']['source_prices'][symbol]['source_evidence']
            assert retained['body_complete'] is True and base64.b64decode(retained['body'],validate=True)==raw
        assert snapshot['capital_book']['allows_new_entries'] is False
    full=fixture['cases']['complete'];assert len(full['complete_inputs']['watchlist'])==105 and len(full['frame']['snapshot']['accounting']['source_prices'])==106
    raw=base64.b64decode(full['frame']['snapshot']['accounting']['source_prices']['AAA']['source_evidence']['body'],validate=True)
    assert b'900719925474099312345' in raw and b'INVENTED_MARKUP' in raw
    partial=fixture['cases']['partial'];assert partial['provider_attempts']==[] and partial['frame']['snapshot']['accounting']['quote_collection']['unattempted_count']==106
    assert fixture['cases']['binary']['frame']['snapshot']['positions'][0]['market_value'] is None
    assert fixture['cases']['empty']['frame']['snapshot']['accounting']['quote_collection']['unique_requested_count']==0


def test_retained_predecessor_page_and_native_source_are_complete_and_inert():
    audit=json.loads((ROOT/'docs/audit/2026-09-30/portfolio-quote-page.json').read_bytes())
    for row in audit['fixtures'].values():
        raw=(ROOT/row['path']).read_bytes();assert len(raw)==row['bytes'] and hashlib.sha256(raw).hexdigest()==row['sha256']
    assert audit['native_snapshot_source_commit']=='51b4ea7a088e49c4d11754fc2451bdd06b90f3c4'
    assert (ROOT/'tests/fixtures/pre-portfolio-quote-page/lambda_function.py.txt').read_bytes()==(ROOT/'tests/fixtures/pre-portfolio-sector-coverage/snapshot-lambda_function.py.txt').read_bytes()
