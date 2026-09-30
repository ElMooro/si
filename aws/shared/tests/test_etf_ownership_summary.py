"""Synthetic-only qualification, byte replay, limits and importer regressions."""
import copy, gzip, hashlib, json, sys, unittest
from pathlib import Path
from unittest import mock
sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).parent)]
import etf_holdings_model as model
import etf_holdings_native as native
import etf_holdings_store as store
import etf_desk_store as desk
from test_etf_holdings_native import collection, row, GENERATED
from test_etf_holdings_store import fixture, Storage
ROOT = Path(__file__).resolve().parents[3]


def pair():
    a, raw = collection([row()]);b, old = collection([row(processed_date='2026-08-20', effective_date='2026-08-19')], processed='2026-08-20')
    return native.reconstruct(a, raw.__getitem__, GENERATED), native.reconstruct(b, old.__getitem__, GENERATED)


def output(pairs, generated=GENERATED):
    acc=model.OwnershipSummary(generated,len(pairs));artifacts={}
    for t,(a,b) in sorted(pairs.items()):
        a=copy.deepcopy(a);b=copy.deepcopy(b);a['ticker']=b['ticker']=t
        comparison=native.compare(a,b)
        acc.add(t,a,b,comparison,{'synthetic':'current'},{'synthetic':'prior'},{'synthetic':'comparison'},{'category':'unverified'})
    ref=acc.finish(lambda k,v:artifacts.__setitem__(k,v))
    manifest=json.loads(artifacts[ref['manifest']['key']]) if ref['status']=='complete' else None
    return ref,manifest,artifacts


def rows(manifest,artifacts,kind='current_membership'):
    return [r for g in manifest['cohorts'] if g['kind']==kind for p in g['parts'] for r in json.loads(artifacts[p['key']])['rows']]


class Summary(unittest.TestCase):
    def test_exact_membership_raw_and_lower_bound_are_separate(self):
        a,b=pair();partial=copy.deepcopy(a);partial['quality'].update(status='incomplete',pagination_complete=False)
        _,m,parts=output({'AAA':(a,b),'BBB':(partial,b)})
        g=next(g for g in m['cohorts'] if g['kind']=='current_membership');r=rows(m,parts)[0]
        self.assertEqual((g['configured_fund_count'],g['eligible_fund_count']),(2,1))
        self.assertEqual((r['raw_observed_fund_count'],r['known_presence_lower_bound'],r['qualified_fund_count']),(2,2,1))
        self.assertEqual(m['funds']['BBB']['qualification_exclusion'],'incomplete_returned_snapshot')
        self.assertFalse(m['calls_eligible']);self.assertFalse(m['corporate_actions_verified'])

    def test_lower_bound_deadline_includes_excluded_partial_contributors(self):
        a,b=pair();a['source_valid_until']='2026-09-22T08:00:00+00:00'
        partial=copy.deepcopy(a);partial['source_valid_until']='2026-09-21T08:00:00+00:00'
        partial['quality'].update(status='incomplete',pagination_complete=False)
        inputs={'AAA':(a,b),'BBB':(partial,b)}
        _,m,p=output(inputs)
        g=next(g for g in m['cohorts'] if g['kind']=='current_membership')
        self.assertEqual(g['source_valid_until'],a['source_valid_until'])
        self.assertEqual(g['lower_bound_valid_until'],partial['source_valid_until'])
        self.assertEqual(rows(m,p)[0]['known_presence_lower_bound'],2)
        self.assertEqual(rows(m,p)[0]['qualified_fund_count'],1)
        # At the exclusive deadline the retained lower bound is no longer fresh.
        for now in ('2026-09-21T08:00:00+00:00','2026-09-21T09:00:00+00:00'):
            self.assertFalse(native.clock(now)<native.clock(g['lower_bound_valid_until']))
            _,fresh,fp=output(inputs,now)
            fg=next(g for g in fresh['cohorts'] if g['kind']=='current_membership')
            self.assertEqual(rows(fresh,fp)[0]['known_presence_lower_bound'],1)
            self.assertEqual(rows(fresh,fp)[0]['raw_observed_fund_count'],2)
            self.assertEqual(fg['lower_bound_valid_until'],a['source_valid_until'])

    def test_partial_only_lower_bound_has_own_deadline_or_is_unavailable(self):
        a,b=pair();a['source_valid_until']='2026-09-21T08:00:00+00:00'
        a['quality'].update(status='incomplete',pagination_complete=False)
        _,m,p=output({'BBB':(a,b)})
        g=m['cohorts'][0];r=rows(m,p)[0]
        self.assertIsNone(g['source_valid_until']);self.assertEqual(g['eligible_fund_count'],0)
        self.assertEqual(g['lower_bound_valid_until'],a['source_valid_until'])
        self.assertEqual(r['known_presence_lower_bound'],1);self.assertIsNone(r['qualified_fund_count'])
        _,m,p=output({'BBB':(a,b)},a['source_valid_until'])
        self.assertIsNone(m['cohorts'][0]['lower_bound_valid_until'])
        self.assertEqual(rows(m,p)[0]['raw_observed_fund_count'],1)
        self.assertEqual(rows(m,p)[0]['known_presence_lower_bound'],0)
        self.assertEqual(output({'BBB':(a,b)}),output({'BBB':(a,b)}))

    def test_stale_future_mixed_missing_and_ambiguous_identity_exclusions(self):
        a,b=pair()
        cases=[('source_check_not_current',{'source_valid_until':'2020-01-01T00:00:00Z'}),
               ('source_check_not_current',{'source_acquired_at':'2099-01-01T00:00:00Z'}),
               ('mixed_or_future_effective_dates',{'effective_dates':{'2026-09-17':1,'2026-09-16':1}}),
               ('future_processing_date',{'processed_date':'2099-01-01'}),
               ('invalid_source_clocks',{'source_valid_until':None})]
        for reason,changes in cases:
            bad={**a,**changes};self.assertEqual(model.summary_reason(bad,native.clock(GENERATED)),reason)
        for field in ('missing_identity_rows','duplicate_identity_rows','rows_with_field_errors'):
            for value in (1,None,True):
                bad=copy.deepcopy(a);bad['quality'][field]=value
                self.assertEqual(model.summary_reason(bad,native.clock(GENERATED)),'identity_or_field_coverage_unresolved')
        missing=copy.deepcopy(a);missing['rows'][0]['identity_key']=None;missing['quality']['missing_identity_rows']=1
        _,m,parts=output({'AAA':(missing,b)});self.assertEqual(rows(m,parts),[])

    def test_date_cohorts_and_pairs_never_mix_or_count_same_date_revisions(self):
        a,b=pair();other=copy.deepcopy(a);other['effective_dates']={'2026-09-16':1};other['rows'][0]['effective_date']='2026-09-16'
        _,m,_=output({'AAA':(a,b),'BBB':(other,b)})
        self.assertEqual(len(m['cohorts']),4)
        self.assertTrue(all(g['eligible_fund_count']==1 for g in m['cohorts']))
        _,m,_=output({'AAA':(a,a)});self.assertEqual(len(m['cohorts']),1)
        self.assertEqual(m['funds']['AAA']['comparison_exclusion'],'incompatible_effective_dates')

    def test_zero_null_weight_quantity_asset_labels_and_duplicates_not_exposure(self):
        a,b=pair();a['rows'][0].update(shares_held_raw_decimal='0',weight_raw_decimal=None)
        raw=copy.deepcopy(a);raw['rows'].append(copy.deepcopy(raw['rows'][0]));raw['quality']['duplicate_identity_rows']=1
        _,m,parts=output({'AAA':(a,b),'BBB':(raw,b)})
        r=rows(m,parts)[0];self.assertEqual(r['raw_observed_fund_count'],2);self.assertEqual(r['qualified_fund_count'],1)
        self.assertNotIn('asset_class',r);self.assertNotIn('quantity',r);self.assertNotIn('weight',r)
        self.assertIsNone(a['rows'][0]['weight_raw_decimal']);self.assertEqual(a['rows'][0]['shares_held_raw_decimal'],'0')
        self.assertFalse(m['weight_unit_certified'])
        self.assertEqual(r['source_asset_class'], a['rows'][0]['asset_class'])
        self.assertFalse(m['source_classifications_verified'])
        unknown=copy.deepcopy(a);unknown['rows'][0].update(asset_class=None,security_type=None)
        _,um,up=output({'AAA':(unknown,b)})
        self.assertIsNone(rows(um,up)[0]['source_asset_class'])
        self.assertIsNone(rows(um,up)[0]['source_security_type'])

    def test_ticker_collision_retains_exact_identities_and_deterministic_ties(self):
        a,b=pair();x=copy.deepcopy(a);x['rows'][0]['identity_key']='f'*64
        r1,m,p=output({'AAA':(a,b),'BBB':(x,b)});r2,_,p2=output({'BBB':(x,b),'AAA':(a,b)})
        self.assertEqual(r1,r2);self.assertEqual(p,p2)
        rs=rows(m,p);self.assertEqual(len(rs),2);self.assertEqual([r['identity_key'] for r in rs],sorted(r['identity_key'] for r in rs))

    def test_all_limits_fail_summary_without_emitting_prefix(self):
        a,b=pair()
        for constant in ('SUMMARY_MAX_RECORDS','SUMMARY_MAX_OBSERVATIONS','SUMMARY_MAX_BYTES','SUMMARY_PAGE_BYTES','SUMMARY_MANIFEST_BYTES','SUMMARY_MAX_PAGES'):
            with mock.patch.object(model,constant,0):
                ref,m,parts=output({'AAA':(a,b)});self.assertEqual(ref['status'],'unavailable',constant);self.assertEqual(parts,{})


class Replay(unittest.TestCase):
    def setUp(self):
        p=mock.patch.dict(model.catalog.ETF_UNIVERSE,{'SPY':{'category':'broad'}},clear=True);p.start();self.addCleanup(p.stop)

    def build(self,policy=True):
        db,i=fixture()
        if policy:i['ownership_summary_policy']=model.OWNERSHIP_POLICY
        read=store.reader(db,'fixture');out=store.compile_output(i,read,lambda k,v:store.immutable(db,'fixture',k,v))
        ref=store.retain(db,'fixture',i,out,read)
        return db,i,out,ref

    def test_old_input_golden_replays_without_executing_retained_compilers(self):
        f=json.loads(gzip.decompress((ROOT/'tests/fixtures/holdings-summary-predecessor-synthetic.json.gz').read_bytes()))
        db=Storage();db.objects={k:v.encode() for k,v in f['objects'].items()}
        packet=f['packet'];actual=store.replay(packet['replay'],store.reader(db,'fixture'))
        self.assertEqual(actual,{k:v for k,v in packet.items() if k!='replay'})
        self.assertNotIn('ownership_summary',actual);self.assertEqual(actual['version'],'1.0.0')

    def test_new_replay_and_lookthrough_reference_not_duplicate_summary(self):
        db,i,out,ref=self.build();self.assertEqual(out['version'],'1.1.0')
        self.assertEqual(store.replay(ref,store.reader(db,'fixture')),out)
        current={**out,'replay':ref};db.objects[model.CURRENT]=model.encoded(current)
        look={'contract':'etf-lookthrough-inputs.v1','kind':'lookthrough','generated_at':GENERATED,
              'canonical_source':store.snapshot(db,'fixture',model.CURRENT),'previous':None}
        writes=[];projected=store.compile_output(look,store.reader(db,'fixture'),lambda *a:writes.append(a))
        self.assertEqual(projected['ownership_summary'],out['ownership_summary']);self.assertEqual(writes,[])

    def test_overflow_keeps_base_output_and_replays_exactly(self):
        _,_,old,_=self.build(False)
        with mock.patch.object(model,'SUMMARY_MAX_BYTES',1):
            db,i,out,ref=self.build();self.assertEqual(out['ownership_summary']['status'],'unavailable')
            self.assertEqual(out['funds'],old['funds']);self.assertEqual(out['security_directory'],old['security_directory'])
            self.assertEqual(store.replay(ref,store.reader(db,'fixture')),out)

    def test_forged_manifest_page_and_reference_metadata_fail_replay(self):
        db,i,out,ref=self.build();manifest_ref=out['ownership_summary']['manifest'];m=json.loads(db.objects[manifest_ref['key']])
        page=next(g['parts'][0] for g in m['cohorts'] if g['parts'])
        for key,field in [(manifest_ref['key'],'record_count'),(page['key'],'row_offset')]:
            saved=db.objects[key];doc=json.loads(saved);doc[field]+=1;db.objects[key]=model.encoded(doc)
            with self.assertRaises(ValueError):store.replay(ref,store.reader(db,'fixture'))
            db.objects[key]=saved
        # Even self-consistently rehashed forged output metadata must match reconstruction.
        run=json.loads(db.objects[ref['manifest_key']]);forged=copy.deepcopy(out);forged['ownership_summary']['manifest']['bytes']+=1
        raw=model.encoded(forged);digest=model.sha(raw);key=model.PREFIX+'outputs/'+digest+'.json';db.objects[key]=raw
        run['output']={'key':key,'sha256':digest,'bytes':len(raw)};run['output_sha256']=digest
        raw=model.encoded(run);key=model.PREFIX+'runs/'+model.sha(raw)+'.json';db.objects[key]=raw
        with self.assertRaises(ValueError):store.replay({'manifest_key':key,'output_sha256':digest},store.reader(db,'fixture'))

    def test_unreviewed_policy_and_compiler_pins_rejected(self):
        db,i=fixture();i['ownership_summary_policy']='invented'
        with self.assertRaisesRegex(ValueError,'Unreviewed'):store.compile_output(i,store.reader(db,'fixture'),lambda *a:None)
        db,i,out,ref=self.build();run=json.loads(db.objects[ref['manifest_key']]);run['compilers']['etf_holdings_model']['sha256']='0'*64
        with self.assertRaises(ValueError):store.verify_compilers(run,model.PREFIX,store.reader(db,'fixture'))

    def test_new_canonical_summary_is_consumed_and_replayed_by_desk(self):
        import test_etf_desk_store as ds
        with mock.patch.object(desk.catalog,'DESK',('SPY','VOO','BND')),mock.patch.dict(model.catalog.ETF_UNIVERSE,{'SPY':{'category':'broad'},'VOO':{'category':'broad'}},clear=True):
            db,inputs=ds.fixture()
            old=json.loads(db.objects[model.CURRENT]);run=json.loads(db.objects[old['replay']['manifest_key']])
            hi=json.loads(db.objects[run['input']['key']]);hi['ownership_summary_policy']=model.OWNERSHIP_POLICY
            reader=store.reader(db,'fixture');ho=store.compile_output(hi,reader,lambda k,v:store.immutable(db,'fixture',k,v))
            href=store.retain(db,'fixture',hi,ho,reader);db.objects[model.CURRENT]=model.encoded({**ho,'replay':href})
            inputs['canonical_holdings']=desk.snapshot(db,'fixture',model.CURRENT,desk.reader(db,'fixture'))
            out=ds.RetainedDesk().native(db,inputs)
            self.assertEqual(desk.replay(out['replay'],desk.reader(db,'fixture')),{k:v for k,v in out.items() if k!='replay'})
            self.assertFalse(out['calls_eligible'])

    def test_all_importer_bundles_include_and_import_new_contract_without_clients(self):
        import os,subprocess,tempfile,zipfile
        sys.path.insert(0,str(ROOT/'aws/ops/checks'))
        from release_package_evidence import shared_imports
        for name in ('justhodl-etf-constituents','justhodl-flow-lookthrough','justhodl-etf-global-desk'):
            source=ROOT/'aws/lambdas'/name/'source';sources=list(source.glob('*.py'))
            closure=shared_imports(ROOT,sources)
            self.assertIn('etf_holdings_model',{p.stem for p in closure})
            with tempfile.TemporaryDirectory() as folder:
                archive=Path(folder)/'candidate.zip'
                with zipfile.ZipFile(archive,'w') as z:
                    for p in sources:z.write(p,p.name)
                    for p in closure:
                        if not (source/p.name).exists():z.write(p,p.name)
                package=Path(folder)/'package';package.mkdir()
                with zipfile.ZipFile(archive) as z:z.extractall(package)
                env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1','PYTHONPATH':str(package)+os.pathsep+os.environ.get('PYTHONPATH','')}
                check="import boto3; boto3.client=lambda *a,**k: (_ for _ in ()).throw(AssertionError('No AWS clients')); import lambda_function; import etf_holdings_model as m; assert m.OWNERSHIP_POLICY=='qualified-membership.v1'"
                subprocess.run([sys.executable,'-c',check],cwd=package,env=env,check=True,capture_output=True)

    def test_desk_predecessor_importer_replays_with_pinned_old_holdings(self):
        f=json.loads(gzip.decompress((ROOT/'tests/fixtures/holdings-summary-desk-predecessor-synthetic.json.gz').read_bytes()))
        db=Storage();db.objects={k:v.encode() for k,v in f['objects'].items()}
        with mock.patch.object(desk.catalog,'DESK',('SPY','VOO','BND')),mock.patch.dict(model.catalog.ETF_UNIVERSE,{'SPY':{'category':'broad'},'VOO':{'category':'broad'}},clear=True):
            actual=desk.replay(f['packet']['replay'],desk.reader(db,'fixture'))
        self.assertEqual(actual,{k:v for k,v in f['packet'].items() if k!='replay'})


if __name__=='__main__':unittest.main()
