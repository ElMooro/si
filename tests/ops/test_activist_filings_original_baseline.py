"""Reproduce original defects using extracted code and synthetic inputs only."""
from pathlib import Path
from collections import defaultdict
import ast, json, re, sys, unittest
ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks')]
import ops_6259_activist_filings_original_baseline as op
SOURCE = ROOT/'tests/fixtures/pre-activist-filings-research.py.txt'
TREE = ast.parse(SOURCE.read_bytes())
HANDLER = next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == 'lambda_handler')

def run(nodes, ns):
    exec(compile(ast.Module(body=nodes, type_ignores=[]), '<isolated original SEC function>', 'exec'), ns)
    return ns

def extracted(name, ns):
    run([next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == name)], ns)
    return ns[name]

def entry(role='Filer', name='Example', cik='123', prefix=''):
    p=prefix
    return (f'<{p}entry><{p}title>SC 13D - {name} ({cik}) ({role})</{p}title>'
            f'<{p}link href="https://www.sec.gov/Archives/edgar/data/123/000000012326000001/0000000123-26-000001-index.htm"/>'
            f'<{p}updated>2026-09-25T17:00:00Z</{p}updated><{p}summary>'+('X'*700)+f'</{p}summary></{p}entry>')

def filing_group(entries):
    node = next(n for n in HANDLER.body if isinstance(n, ast.For) and ast.unparse(n.target) == '(acc, entries)')
    ns={'by_accession':{'0000000123-26-000001':entries},'seen_accessions':set(),'filings':[],
        'cik_map':{},'universe':{},'re':re,'classify_filer':lambda name:(None,None),
        'score_filing':lambda *a:(0,'NOTABLE',[])}
    return run([node],ns)['filings'][0]

class Tests(unittest.TestCase):
    def test_complete_sources_pinned_and_private_state_out_of_scope(self):
        self.assertFalse(op.source_check('justhodl-activist-filings-scanner',SOURCE.read_bytes())['imported_or_executed'])
        raw=(ROOT/'tests/fixtures/pre-activist-compound-consumer.py.txt').read_bytes()
        self.assertFalse(op.source_check('justhodl-compound-aggregator',raw)['imported_or_executed'])
        self.assertEqual(op.KEYS,('data/activist-filings.json',))
        with self.assertRaises(ValueError):op.source_check('justhodl-activist-filings-scanner',SOURCE.read_bytes()+b'\n')
        class Denied:
            def get_object(self,**kw):raise AssertionError('Private or consumer reads forbidden')
        for key in ('data/activist-filings-state.json','data/compound-signals.json'):
            with self.assertRaises(ValueError):op.capture(Denied(),key)
        self.assertNotIn('lambda_function',sys.modules)

    def test_valid_accession_crashes_nonexistent_capture_group(self):
        node=next(n for n in HANDLER.body if isinstance(n,ast.For) and ast.unparse(n.iter)=='rss_entries')
        with self.assertRaisesRegex(IndexError,'no such group'):
            run([node],{'rss_entries':[{'link':'https://www.sec.gov/Archives/edgar/data/123/0000000123-26-000001-index.htm'}],
                        'by_accession':defaultdict(list),'re':re})

    def test_namespace_drops_entries_and_summary_is_truncated(self):
        parse=extracted('parse_atom_entries',{'re':re})
        self.assertEqual(len(parse('<feed>'+entry()+'</feed>','SC 13D')[0]['summary']),500)
        self.assertEqual(parse('<atom:feed xmlns:atom="http://www.w3.org/2005/Atom">'+entry(prefix='atom:')+'</atom:feed>','SC 13D'),[])

    def test_missing_subject_role_is_invented(self):
        row=filing_group([{'role':'Filer','name':'Reporting party only','cik':'123','fetched_form_type':'SC 13D'}])
        self.assertEqual(row['subject_name'],'Reporting party only')
        self.assertEqual(row['subject_cik'],row['filer_cik'])

    def test_last_cofiler_overwrites_first(self):
        row=filing_group([{'role':'Filer','name':'First','cik':'123','fetched_form_type':'SC 13D'},
                          {'role':'Reporting','name':'Second','cik':'456','fetched_form_type':'SC 13D'},
                          {'role':'Subject','name':'Issuer','cik':'789','fetched_form_type':'SC 13D'}])
        self.assertEqual(row['filer_name'],'Second');self.assertNotIn('all_filers',row)

    def test_share_class_ticker_is_overwritten(self):
        rows={'0':{'cik_str':123,'ticker':'CLASSA'},'1':{'cik_str':123,'ticker':'CLASSB'}}
        mapping=extracted('cik_to_ticker_map',{'json':json,'http_get':lambda *a,**kw:json.dumps(rows)})()
        self.assertEqual(mapping['0000000123']['ticker'],'CLASSB');self.assertEqual(len(mapping),1)

    def test_name_substring_is_a_hot_tier_without_identity_or_outcomes(self):
        tiers=next(n for n in TREE.body if isinstance(n,ast.Assign) and ast.unparse(n.targets[0])=='ACTIVIST_TIERS')
        ns=run([tiers],{});classify=extracted('classify_filer',ns);score=extracted('score_filing',ns)
        tier,_=classify('NOT AFFILIATED WITH PERSHING SQUARE EXAMPLE')
        self.assertEqual(score('SC 13D',tier,True)[:2],(90,'TIER_A_HOT'))

    def test_same_party_amendments_become_multi_activist(self):
        node=next(n for n in HANDLER.body if isinstance(n,ast.For) and ast.unparse(n.target)=='(ticker, fs)')
        row={'subject_company':'Issuer','filer_name':'Same party','form_type':'SC 13D/A','score':90}
        out=run([node],{'by_ticker':{'TEST':[row,dict(row)]},'multi_activist':[]})['multi_activist']
        self.assertEqual(out[0]['n_filings'],2);self.assertEqual(out[0]['filers'],['Same party'])

    def test_feed_failure_disappears_as_empty_population(self):
        def fail(*a,**kw):raise RuntimeError('synthetic provider failure')
        import urllib.parse
        fetch=extracted('fetch_atom_feed',{'urllib':__import__('urllib'),'http_get':fail,'parse_atom_entries':lambda *a:[]})
        self.assertEqual(fetch('SC 13D'),[])

if __name__=='__main__':unittest.main(verbosity=2)
