"""Disk-backed equivalent of the accepted complete membership computation.

The input iterator must contain whole validated capture contexts in capture-key
order. No source is sampled or omitted. SQLite is a temporary computation spool,
not an authoritative source, cache or production persistence mechanism.
"""
from collections import Counter
from contextlib import closing
import hashlib,json,sqlite3
from genealogy_public_archive import canonical,clock
from genealogy_source_fold import fold_source
import genealogy_capture_timing as baseline


def compile_spooled(contexts,cutoff,database_path,output_path):
    # Caller supplies a fresh private temporary directory. Never open a supplied
    # existing database or truncate an earlier result as a side effect of audit.
    if database_path.exists() or output_path.exists():raise ValueError('fresh_computation_paths_required')
    clock(cutoff)
    with closing(sqlite3.connect(database_path)) as db:
        db.execute('PRAGMA cache_size=-4096');db.execute('PRAGMA temp_store=FILE')
        db.execute('CREATE TABLE snapshots(source TEXT, stamp TEXT, capture TEXT, body TEXT, PRIMARY KEY(source,capture))')
        db.execute('CREATE INDEX source_time ON snapshots(source,stamp,capture)')
        db.execute('CREATE TABLE intervals(instrument TEXT,direction TEXT,source TEXT,stamp TEXT,body TEXT)')
        db.execute('CREATE TABLE firsts(instrument TEXT,direction TEXT,source TEXT,body TEXT,PRIMARY KEY(instrument,direction,source))')
        db.execute('CREATE TABLE ambiguities(seq INTEGER PRIMARY KEY,body TEXT)')
        db.execute('CREATE TABLE comparisons(seq INTEGER PRIMARY KEY,body TEXT)')
        captures=0;last='';scope=Counter();excluded=Counter();inputs=hashlib.sha256();inputs.update(b'[')
        for context in contexts:
            key=context['capture_key']
            if key<=last:raise ValueError('capture_context_order_or_duplicate')
            if clock(context['generated_at'])>clock(cutoff):raise ValueError('capture_after_cutoff')
            last=key
            if captures:inputs.update(b',')
            inputs.update(canonical(context));captures+=1
            scope['complete_capture_scans' if context['candidate_scan_complete'] else 'partial_capture_scans']+=1
            for source in context['sources']:
                name=source['source_key']
                if not source['eligible']:excluded.update(source['exclusion_reasons'])
                snapshot={'members':sorted((r['instrument_id'],r['direction']) for r in source['explicit_members']),
                    'eligible':source['eligible'],'absence_eligible':source['unsupported_identity_count']==0,
                    'receipt':{'capture_key':key,'capture_sha256':context['capture_sha256'],
                        **{field:source[field] for field in ('capture_source_index','source_bytes_sha256','source_generated_at','source_received_at')}}}
                db.execute('INSERT INTO snapshots VALUES(?,?,?,?)',(name,clock(source['source_received_at']).isoformat(),key,canonical(snapshot).decode()))
        inputs.update(b']');db.commit()
        source_count=0;counts=Counter()
        # Use a separate read cursor while interval/first rows are inserted.
        for (source,) in db.execute('SELECT DISTINCT source FROM snapshots ORDER BY source'):
            source_count+=1
            def timeline():
                for (body,) in db.execute('SELECT body FROM snapshots WHERE source=? ORDER BY stamp,capture',(source,)):
                    row=json.loads(body);row['members']={tuple(x) for x in row['members']};yield row
            def emit_interval(row):
                db.execute('INSERT INTO intervals VALUES(?,?,?,?,?)',
                    (row['instrument_id'],row['direction'],source,clock(row['upper_inclusive_utc']).isoformat(),canonical(row).decode()))
            def emit_ambiguity(row):db.execute('INSERT INTO ambiguities(body) VALUES(?)',(canonical(row).decode(),))
            result=fold_source(source,timeline(),emit_interval,emit_ambiguity);counts.update(result['counts'])
            for row in result['first_observations']:
                db.execute('INSERT INTO firsts VALUES(?,?,?,?)',(row['instrument_id'],row['direction'],source,canonical(row).decode()))
        first_count=db.execute('SELECT COUNT(*) FROM firsts').fetchone()[0]
        possible=db.execute('SELECT COALESCE(SUM(n*(n-1)/2),0) FROM (SELECT COUNT(*) n FROM firsts GROUP BY instrument,direction)').fetchone()[0]
        statuses=Counter();compared=0
        query='SELECT a.body,b.body FROM firsts a JOIN firsts b ON a.instrument=b.instrument AND a.direction=b.direction AND a.source<b.source ORDER BY a.instrument,a.direction,a.source,b.source'
        for left,right in db.execute(query):
            a,b=json.loads(left),json.loads(right)
            al,bl=a['lower_exclusive_utc'],b['lower_exclusive_utc'];au,bu=a['upper_inclusive_utc'],b['upper_inclusive_utc']
            if a['status']=='ambiguous_initial_membership' or b['status']=='ambiguous_initial_membership':order='unresolved_initial_membership_ambiguity'
            elif bl is not None and clock(au)<=clock(bl):order='a_observed_presence_before_b_bounded_entry'
            elif al is not None and clock(bu)<=clock(al):order='b_observed_presence_before_a_bounded_entry'
            elif al is None or bl is None:order='unresolved_prior_observation_missing'
            else:order='overlapping_observation_intervals'
            common=sorted({r['capture_key'] for r in a['presence_receipts']} & {r['capture_key'] for r in b['presence_receipts']})
            row={'instrument_id':a['instrument_id'],'direction':a['direction'],'source_a':a['source_key'],'source_b':b['source_key'],
                'a_lower_exclusive_utc':al,'a_upper_inclusive_utc':au,'b_lower_exclusive_utc':bl,'b_upper_inclusive_utc':bu,
                'interval_order':order,'shared_presence_capture_keys':common,'causal_order_qualified':False,'independent_evidence_count':None}
            db.execute('INSERT INTO comparisons(body) VALUES(?)',(canonical(row).decode(),));statuses[order]+=1;compared+=1
        if compared!=possible:raise ValueError('complete_comparison_population_required')
        db.commit()
        arrays={'intervals':'SELECT body FROM intervals ORDER BY instrument,direction,source,stamp',
                'first_observations':'SELECT body FROM firsts ORDER BY instrument,direction,source',
                'same_timestamp_ambiguities':'SELECT body FROM ambiguities ORDER BY seq',
                'comparisons':'SELECT body FROM comparisons ORDER BY seq'}
        metadata={k:v for k,v in baseline.compile_contexts([],cutoff).items() if k not in arrays}
        metadata.update(input_contexts_sha256=inputs.hexdigest(),capture_count=captures,source_count=source_count,
            **dict(scope),**{key:counts[key] for key in ('valid_source_snapshots','ineligible_source_snapshots','identity_partial_source_snapshots','same_timestamp_duplicate_snapshots')},
            first_observed_groups=first_count,possible_comparisons=possible,
            excluded_reason_counts=dict(sorted(excluded.items())),comparison_status_counts=dict(sorted(statuses.items())))
        digest=hashlib.sha256();size=0
        with output_path.open('xb') as output:
            def write(raw):
                nonlocal size
                digest.update(raw);output.write(raw);size+=len(raw)
            write(b'{')
            for position,key in enumerate(sorted(set(metadata)|set(arrays))):
                if position:write(b',')
                write(canonical(key)+b':')
                if key not in arrays:write(canonical(metadata[key]));continue
                write(b'[')
                for index,(body,) in enumerate(db.execute(arrays[key])):
                    if index:write(b',')
                    write(body.encode())
                write(b']')
            write(b'}')
        return {'complete_output_sha256':digest.hexdigest(),'output_bytes':size,'input_contexts_sha256':inputs.hexdigest(),
                'capture_count':captures,'source_count':source_count,'first_observed_groups':first_count,
                'possible_comparisons':possible,'comparison_status_counts':dict(statuses),'database_bytes':database_path.stat().st_size,
                'scope':'Complete validated input iterator, disk-backed ordering and full canonical output. Original-body validation and durable incremental caching are separate required boundaries.'}
