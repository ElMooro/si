"""Chart's deliberate embedded-panel opt-out retains an explicit evidence route."""
from pathlib import Path
import io,sys
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'scripts'),str(ROOT/'aws/ops/checks')]
from build_page_data_contracts import require_standalone_link,STANDALONE_INSPECTION_PAGES
from audit_20260909_public_pages import Markers,inspect


def test_existing_chart_layout_uses_a_separate_inspection_link_and_no_embedded_panel():
    source=(ROOT/'chart.html').read_text()
    require_standalone_link(source,'chart.html')
    assert 'chart.html' in STANDALONE_INSPECTION_PAGES
    assert 'src="/jh-data-inspector.js' not in source
    assert 'data-jh-inspection-route="chart.html" target="_blank" rel="noopener"' in source
    for broken in [source.replace('/engine-data.html?page=chart.html','https://elsewhere.invalid/'),source.replace('data-jh-inspection-route="chart.html"','data-jh-inspection-route="calls.html"')]:
        try:require_standalone_link(broken,'chart.html')
        except ValueError:pass
        else:raise AssertionError('Unusable standalone entry was accepted')


def test_audit_requires_exact_same_origin_inspection_link_bound_to_the_route():
    for href,route,expected in [('/engine-data.html?page=chart.html','chart.html',['chart.html']),
        ('https://elsewhere.invalid/engine-data.html?page=chart.html','chart.html',[]),
        ('/engine-data.html?page=calls.html','chart.html',[]),('/engine-data.html?page=calls.html','calls.html',[])]:
        parser=Markers();parser.feed(f'<a href="{href}" data-jh-inspection-route="{route}">Data</a>')
        assert parser.standalone==expected


def test_http_audit_reports_standalone_mode_without_claiming_embedded_inspection():
    class Response(io.BytesIO):
        status=200;headers={'Content-Type':'text/html'}
    class Opener:
        def open(self,*args,**kwargs):
            return Response(('<meta name="jh-build-commit" content="'+'a'*40+'">'+
                '<a href="/engine-data.html?page=chart.html" data-jh-inspection-route="chart.html">Data</a>').encode())
    result=inspect('chart.html',Opener())
    assert result['status']=='HTML_VERIFIED' and result['inspection_mode']=='standalone'
    assert result['inspector_present'] is False
    assert inspect('calls.html',Opener())['status']=='MISSING_BUILD_OR_INSPECTOR'
