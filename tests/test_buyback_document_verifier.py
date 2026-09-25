from pathlib import Path
from unittest.mock import Mock
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'scripts'),str(ROOT/'aws/ops/checks')]
from test_buyback_document_evidence import fixture
import buyback_document_evidence as compiler
import buyback_filing_sources as source
import verify_buyback_documents as checker


def example(duplicate=False):
    client,manifest,keep=fixture(duplicate)
    catalog=compiler.compile_output(keep(manifest),lambda ref:source.read(client,ref))
    return catalog,lambda digest:client.files[source.PRIVATE+digest+'.bin']


class Tests(unittest.TestCase):
    def test_independent_original_metadata_ranges_and_all_literal_matches(self):
        catalog,read=example(True);raw=source.encoded(catalog)
        proof=checker.verify(raw,source.sha(raw),read)
        self.assertEqual((proof['verified_submissions'],proof['verified_documents'],proof['reported_rows_conserved']), (1,2,2))
        self.assertEqual(proof['literal_coordinates_checked'],2)
        self.assertFalse(proof['production_inspector_imported']);self.assertFalse(proof['authorization_amount_qualified'])
        text=Path(checker.__file__).read_text()
        self.assertNotIn('import buyback_',text);self.assertNotIn('import boto3',text)

    def test_catalog_and_whole_original_hash_failures_stop_verification(self):
        catalog,read=example();raw=source.encoded(catalog);unused=Mock()
        with self.assertRaises(ValueError):checker.verify(raw,'0'*64,unused)
        unused.assert_not_called()
        with self.assertRaises(ValueError):checker.verify(raw,source.sha(raw),lambda digest:read(digest)[:-10])

    def test_independent_checks_detect_rehashed_wrong_metadata_missing_words_and_ranges(self):
        for failure in ('accession','issuer','filing_date','filename','range','words','removed','permission','row','url'):
            catalog,read=example();filing=catalog['filings'][0];doc=filing['documents'][1]
            if failure=='accession':filing['accession']='0001193125-26-999999'
            elif failure=='issuer':filing['issuer_cik']='0000000456'
            elif failure=='filing_date':filing['filing_date']='2026-01-01'
            elif failure=='filename':doc['filename']='different.htm'
            elif failure=='range':doc['text_byte_start']+=1
            elif failure=='words':doc['literal_keyword_occurrences'].pop();catalog['literal_keyword_occurrences']-=1
            elif failure=='removed':filing['documents'].pop();catalog['documents']-=1
            elif failure=='permission':catalog['sizing_qualified']=True
            elif failure=='row':catalog['rows'][0]['filing_id']='0'*64
            else:doc['document_url']='https://example.com/changed.htm'
            raw=source.encoded(catalog)
            with self.assertRaises(ValueError):checker.verify(raw,source.sha(raw),read)


if __name__=='__main__':unittest.main(verbosity=2)
