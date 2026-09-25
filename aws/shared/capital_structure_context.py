"""Capital-structure observations cannot become unqualified investment votes."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib, json, re

CURRENT='data/share-flows.json'
PREFIX='data/capital-structure-research/'
FLAGS=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')
NOTE=('Cash repurchases, cash issuance, weighted-average EPS denominators, outstanding shares and free float '
      'measure different quantities. This input has no qualified dilution verdict, buyback alpha, independent '
      'investment vote or position size. Descriptive observations do not establish split-adjusted ownership '
      'changes, executed shares retired, insider motives or future returns.')


def context(packet):
    p=packet if isinstance(packet,dict) else {};ref=p.get('replay');available=False
    try:
        stamp=datetime.fromisoformat(p['generated_at'].replace('Z','+00:00'))
        names=p.get('issuers');quality=p.get('quality',{})
        available=(p.get('contract')=='capital-structure-original-research.v1'
            and all(p.get(k) is False for k in FLAGS) and p.get('call') is None and p.get('score') is None
            and p.get('independent_investment_votes')==0 and quality.get('original_source_bytes_replayed') is True
            and quality.get('current_sec_index_replayed') is True and quality.get('every_original_row_conserved') is True
            and quality.get('complete_planned_population') is True and quality.get('split_basis_verified') is False
            and quality.get('historical_security_continuity_verified') is False
            and isinstance(names,list) and 1<=len(names)<=5000 and len(names)==p.get('reported_names')
            and all(isinstance(v,dict) and re.fullmatch('[A-Z0-9][A-Z0-9.-]{0,15}',str(v.get('symbol',''))) for v in names)
            and len({v['symbol'] for v in names})==len(names)
            and isinstance(ref,dict) and set(ref)=={'manifest_key','output_sha256'}
            and bool(re.fullmatch(re.escape(PREFIX)+r'runs/[a-f0-9]{64}\.json',str(ref.get('manifest_key',''))))
            and bool(re.fullmatch('[a-f0-9]{64}',str(ref.get('output_sha256',''))))
            and hashlib.sha256(json.dumps({k:v for k,v in p.items() if k!='replay'},sort_keys=True,
                separators=(',',':'),allow_nan=False).encode()).hexdigest()==ref['output_sha256']
            and stamp.tzinfo is not None and stamp<=datetime.now(timezone.utc))
    except (KeyError,ValueError,TypeError,AttributeError,OverflowError):
        available=False
    return {'contract':'capital-structure-context.v1','native_reference_available':bool(available),
        'status':'descriptive_native_reference' if available else 'unqualified_legacy_or_unavailable',
        'canonical':{'key':CURRENT,'replay':deepcopy(ref)} if available else None,
        'generated_at':p.get('generated_at') if available else None,
        'reported_names':p.get('reported_names') if available else None,
        'output_digest_checked':bool(available),'original_source_replay_performed_by_consumer':False,
        'current_freshness_verified_by_consumer':False,'dilution_qualified':False,'buyback_signal_qualified':False,
        'call':None,'score':None,'independent_investment_votes':0,'portfolio_action':'WAIT','note':NOTE,
        **dict.fromkeys(FLAGS,False)}


def decision_view(packet):
    return {'research_context':context(packet),'tickers':{},'rows':[],'stocks':[],'top':[],
        'call':None,'score':None,'independent_investment_votes':0,**dict.fromkeys(FLAGS,False)}


def guard(key,packet):
    return decision_view(packet) if key in (CURRENT,'share-flows.json') else packet
