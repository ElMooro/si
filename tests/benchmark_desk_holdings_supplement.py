"""Offline synthetic reducer cost envelope, not a replay of live vendor evidence."""
from pathlib import Path
import argparse, json, resource, sys, time
from unittest import mock
sys.path[:0] = [str(Path(__file__).resolve().parents[1] / 'aws/shared'),
                str(Path(__file__).resolve().parents[1] / 'aws/shared/tests')]
import etf_holdings_model as model
from test_etf_ownership_summary import pair, GENERATED

EXTRAS = 'BKLN BND ECH EFNL EPU FALN FXE FXY IEFA MOAT PPLT QQQM RSP SGOV USHY VCIT'.split()


def benchmark(all_qualified=False):
    start = time.perf_counter(); acc = model.OwnershipSummary(GENERATED, 16)
    for number, ticker in enumerate(EXTRAS):
        a, b = pair()
        # Match the supplied aggregate row counts only. Identities, distribution,
        # clocks and qualification here are invented, disjoint stress inputs.
        for snapshot, total in ((a, 22809), (b, 25252)):
            n = total // 16 + (number < total % 16)
            base = snapshot['rows'][0]
            snapshot['ticker'] = ticker
            snapshot['rows'] = [{**base, 'identity_key': format(number * 100000 + j, '064x'),
                                'row_id': format(number * 100000 + j, '064x')} for j in range(n)]
            snapshot['quality']['returned_rows'] = n
            if not all_qualified and ticker != 'MOAT':
                snapshot['quality']['missing_identity_rows'] = 1
                snapshot['rows'][0]['identity_key'] = None
        acc.add(ticker, a, b, model.native.compare(a, b),
                {'synthetic': 'current'}, {'synthetic': 'prior'}, {'synthetic': 'comparison'}, {})
    objects = {}; ref = acc.finish(lambda k, v: objects.__setitem__(k, v))
    manifest = json.loads(objects[ref['manifest']['key']])
    return {'synthetic': True, 'case': 'all_qualified' if all_qualified else 'one_qualified',
            'current_rows': 22809, 'prior_rows': 25252, 'configured_funds': 16,
            'status': ref['status'], 'pages': manifest['page_count'], 'records': manifest['record_count'],
            'additional_put_attempts_per_compile': len(objects), 'additional_payload_bytes': sum(map(len, objects.values())),
            'manifest_bytes': ref['manifest']['bytes'], 'seconds': time.perf_counter() - start,
            'peak_rss_mib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
            'additional_holdings_shards': 0, 'additional_vendor_requests': 0, 'additional_invocations': 0}


def producer(enabled):
    import etf_desk_store as store
    from test_etf_desk_store import fixture
    with mock.patch.object(store.catalog,'DESK',('SPY','VOO','BND')),mock.patch.dict(store.flow_catalog.ETF_UNIVERSE,{'SPY':{'category':'broad'},'VOO':{'category':'broad'}},clear=True):
        db,inputs=fixture()
        if enabled: inputs.update(contract='etf-desk-inputs.v2',extra_holdings_summary_policy=store.model.SUPPLEMENT_POLICY)
        read=store.reader(db,'fixture'); before=set(db.objects); attempts=[]; original=db.put_object
        def put(**kw): attempts.append(kw['Key']); return original(**kw)
        db.put_object=put
        start=time.perf_counter()
        with store.ArtifactWriter(db,'fixture',read) as emit: output=store.compile_output(inputs,read,emit)
        compiled=time.perf_counter()-start
        start=time.perf_counter(); ref=store.retain(db,'fixture',inputs,output,read); retained=time.perf_counter()-start
        prior_puts=len(attempts); start=time.perf_counter()
        assert store.replay(ref,read)==output
        replayed=time.perf_counter()-start
        added={k:v for k,v in db.objects.items() if k not in before}
        return {'synthetic':True,'case':'producer-new' if enabled else 'producer-old',
                'compile_seconds':compiled,'retain_including_replay_seconds':retained,'replay_seconds':replayed,
                'put_attempts':len(attempts),'replay_put_attempts':len(attempts)-prior_puts,
                'get_attempts':len(db.reads),'unique_new_objects':len(added),'new_payload_bytes':sum(map(len,added.values())),
                'root_bytes':len(store.model.encoded(output)),
                'peak_rss_mib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024,
                'additional_vendor_requests':0,'additional_invocations':0}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--all-qualified', action='store_true')
    parser.add_argument('--producer',choices=('old','new'))
    args=parser.parse_args()
    print(json.dumps(producer(args.producer=='new') if args.producer else benchmark(args.all_qualified),sort_keys=True))
