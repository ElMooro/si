"""Replay CapitalFlow, native SEC records, issuer histories and Treasury originals."""
import argparse
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/lambdas/justhodl-capital-flow/source'), str(ROOT/'scripts')]
import capital_research as model
import capital_store as store
from replay_fred_vintage import read_public
from replay_holdings_canonical import verify_current as replay_canonical
from replay_flow_desk_research import verify_current as replay_flows
from replay_etf_research import verify_current as replay_etfs
from replay_foreign_research import verify_view as replay_tic_view


def original_sources(bindings, read):
    """Derived hash checks alone do not establish original-source reproducibility."""
    _, products = replay_canonical(read, run=bindings['holdings']['manifest']['sha256'])
    holdings = products['data/13f-positions.json']
    if model.digest(holdings) != bindings['holdings']['output']['sha256']:
        raise ValueError('Original SEC replay differs from CapitalFlow input')
    flows = replay_flows(read, run=bindings['fund_flows']['manifest']['sha256'])
    if model.digest(flows) != bindings['fund_flows']['output']['sha256']:
        raise ValueError('Fund comparison replay differs from CapitalFlow input')
    etfs = replay_etfs(read, run=flows['etf_manifest']['sha256'])
    if model.digest(etfs) != flows['etf_output']['sha256']:
        raise ValueError('Original issuer replay differs from fund comparison input')
    tic = flows['tic_context']
    if tic['source_status'] == 'verified_descriptive_snapshot':
        original_view = replay_tic_view(read, run=tic['manifest']['sha256'])
        if (model.digest(original_view) != tic['output']['sha256']
                or original_view['foreign_manifest'] != tic['foreign_manifest']
                or original_view['foreign_output'] != tic['foreign_output']):
            raise ValueError('Original Treasury replay differs from fund comparison context')
    elif tic['source_status'] != 'unavailable':
        raise ValueError('Unknown Treasury context qualification')
    return holdings, flows


def verify_current(read=read_public, run=None):
    pointer = None
    if run:
        if not re.fullmatch('[a-f0-9]{64}', run): raise ValueError('Immutable capital research run hash required')
        key = model.PREFIX+'runs/'+run+'.json'; raw = read(key); ref = model.reference(key, raw)
    else:
        pointer = model.decode(read(model.CURRENT)); ref = pointer['replay']
    manifest = model.verified(ref, read, model.PREFIX, 'runs')
    original_sources(manifest['source'], read)
    output = store.replay(manifest, read)
    if pointer is not None and {k: v for k, v in pointer.items() if k != 'replay'} != output:
        raise ValueError('Current CapitalFlow differs from complete replay')
    return manifest, output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--run'); args = parser.parse_args()
    manifest, output = verify_current(run=args.run)
    print('REPRODUCED', model.digest(manifest), ';', output['counts']['disclosure_comparisons'],
          'complete manager comparisons; distinct issuer and Treasury measurements; no allocation authority')
