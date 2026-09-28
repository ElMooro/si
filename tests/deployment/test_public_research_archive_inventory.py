from pathlib import Path
from datetime import datetime,timezone,timedelta
from types import SimpleNamespace
import ast,copy,runpy
ROOT=Path(__file__).resolve().parents[2]
PATH=ROOT/'aws/ops/staged/ops_6303_public_research_archive_inventory.py'
if not PATH.exists():PATH=ROOT/'aws/ops/STAGED/ops_6303_public_research_archive_inventory.py'
M=runpy.run_path(str(PATH));AT=datetime(2026,9,28,18,tzinfo=timezone.utc);PREFIX=M['PREFIXES'][0]

class Listed:
    def __init__(self,pages):self.pages=pages
    def get_paginator(self,name):
        assert name=='list_objects_v2'
        def pages(**kwargs):
            assert kwargs=={'Bucket':'justhodl-dashboard-live','Prefix':PREFIX}
            yield from self.pages
        return SimpleNamespace(paginate=pages)

def entry(letter,**changes):return {'Key':PREFIX+letter*64+'.json','Size':100,'LastModified':AT,**changes}

def test_public_archive_inventory_keeps_every_page_and_marks_a_fixed_cutoff():
    pages=[{'Contents':[entry('b'),entry('a',Size=700)]},{'Contents':[entry('c',LastModified=AT+timedelta(seconds=1))]}]
    result=M['inventory'](Listed(pages),PREFIX,AT)
    assert result['listing_pages']==2 and result['objects_at_cutoff']==2 and result['objects_after_cutoff']==1
    assert result['total_bytes']==800 and result['largest_object_bytes']==700
    assert [r['key'] for r in result['objects']]==[PREFIX+'a'*64+'.json',PREFIX+'b'*64+'.json']
    assert result['record_bodies_verified'] is False and result['registration_clocks_verified'] is False
    reordered=M['inventory'](Listed([{'Contents':[pages[0]['Contents'][1]]},{'Contents':[pages[0]['Contents'][0]]}]),PREFIX,AT)
    assert result['inventory_sha256']==reordered['inventory_sha256']

def test_public_archive_inventory_rejects_duplicates_unknown_paths_and_untyped_metadata():
    cases=[[entry('a'),entry('a')],[entry('a',Key='audit-private/ledger.json')],
           [entry('a',Size=True)],[entry('a',Size=0)],[entry('a',LastModified=AT.replace(tzinfo=None))]]
    for rows in cases:
        try:M['inventory'](Listed([{'Contents':rows}]),PREFIX,AT)
        except ValueError:pass
        else:raise AssertionError('Malformed inventory passed')
    try:M['inventory'](Listed([]),'data/',AT)
    except ValueError:pass
    else:raise AssertionError('Unreviewed prefix allowed')
    calls={n.func.attr for n in ast.walk(ast.parse(PATH.read_text(encoding='utf-8'))) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls.intersection({'get_object','head_object','scan','query','invoke','put_object','put_item','update_schedule','put_rule','urlopen'})
