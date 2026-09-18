"""Run actual narrative consumer functions; never invoke an LLM or notification."""
import ast
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'aws/shared/tests')]
from research_brief_model import build, narrative_context
from test_research_brief_model import source_packet, NOW
from tenor_research_model import public_summary


class FixedDate(datetime):
    @classmethod
    def now(cls, tz=None): return datetime.fromisoformat(NOW)


def run(engine):
    path = ROOT/'aws/lambdas'/('justhodl-'+engine)/'source/lambda_function.py'
    name = {'ai-brief': 'compress_intel', 'ai-chat': 'build_context', 'morning-intelligence': 'extract_metrics'}[engine]
    tree = ast.parse(path.read_text(encoding='utf-8'))
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    packet = build(source_packet(), NOW)
    scope = {'datetime': FixedDate, 'timedelta': timedelta, 'timezone': timezone, 'json': json,
             'research_brief_context': narrative_context, 'tenor_research_summary': public_summary,
             '_CALIBRATION_AVAILABLE': False, 'detect_entities': lambda _: ([], []),
             'get_s3': lambda key: packet if key == 'intelligence-report.json' else {}}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), scope)
    if engine == 'ai-brief':
        result = scope[name](packet)
    elif engine == 'ai-chat':
        text = scope[name]('Show dated macro research')
        line = next(line for line in text.splitlines() if line.startswith('[DATED MACRO RESEARCH'))
        result = json.loads(line.split('] ', 1)[1])
        assert 'Score:N/A/100' not in line
    else:
        result_all = scope[name]({'intel': packet, 'main': {'khalid_index': {'score': None, 'regime': None}}}, {})
        assert result_all['khalid_adj'] is None
        assert result_all['forecast'] == ''
        assert result_all['blended_composite'] is None
        result = result_all['research_brief']
    assert result['status'] == 'research_only' and result['sizing_eligible'] is False and result['call'] is None
    claims = next(row for row in result['observations'] if row['series_id'] == 'ICSA')
    assert claims['value'] == '196000' and claims['unit'] == 'Number' and claims['observed_at'] == '2026-09-12'
    assert claims['evidence']['observations'] and claims['calendar_month_change']['baseline_date'] == '2026-08-08'
    print(engine+': actual narrative projection preserves date, units, evidence, null forecast and no sizing authority')


if __name__ == '__main__':
    for engine in ('ai-brief', 'ai-chat', 'morning-intelligence'): run(engine)
