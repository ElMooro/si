from copy import deepcopy
from datetime import date,timedelta
import contextlib,sys,unittest
import portwatch_acquisition as acquisition


class Tests(unittest.TestCase):
    def query_fixture(self,n):
        calls=[]
        def query(url,params):
            calls.append(params)
            if params.get('returnCountOnly')=='true':return {'count':n}
            if params.get('returnIdsOnly')=='true':return {'objectIdFieldName':'ObjectId','objectIds':list(range(n))}
            return {'features':[{'attributes':{'ObjectId':i,'zero':0}} for i in reversed(list(map(int,params['objectIds'].split(','))))]}
        return query,calls

    def test_all_120501_ids_survive_former_daily_and_reference_caps(self):
        query,calls=self.query_fixture(120501);reviews=[]
        rows=acquisition.all_features(query,'reviewed','1=1',lambda:140-len(calls),reviews)
        self.assertEqual([r['ObjectId'] for r in rows],list(range(120501)))
        self.assertEqual(len(calls),123);self.assertEqual(reviews[0]['returned_rows'],120501)
        self.assertFalse(reviews[0]['provider_snapshot_atomic']);self.assertTrue(all(row['zero']==0 for row in rows))

    def test_count_ids_transfer_and_feature_identity_must_all_agree(self):
        for case in ('count_bool','count_over_limit','list_short','list_duplicate','list_float','field',
                     'row_short','row_duplicate','row_foreign','row_no_identity','transfer','transfer_number','error'):
            query,calls=self.query_fixture(3)
            def bad(url,params):
                result=query(url,params)
                if params.get('returnCountOnly')=='true':
                    if case=='count_bool':result['count']=True
                    if case=='count_over_limit':result['count']=1000001
                elif params.get('returnIdsOnly')=='true':
                    if case=='list_short':result['objectIds'].pop()
                    if case=='list_duplicate':result['objectIds']=[0,0,2]
                    if case=='list_float':result['objectIds'][0]=0.0
                    if case=='field':result['objectIdFieldName']='guessed'
                else:
                    if case=='row_short':result['features'].pop()
                    if case=='row_duplicate':result['features'][0]=deepcopy(result['features'][1])
                    if case=='row_foreign':result['features'][0]['attributes']['ObjectId']=99
                    if case=='row_no_identity':del result['features'][0]['attributes']['ObjectId']
                    if case=='transfer':result['exceededTransferLimit']=True
                    if case=='transfer_number':result['exceededTransferLimit']=0
                    if case=='error':result={'_err':'unavailable'}
                return result
            with self.subTest(case=case),self.assertRaises(acquisition.AcquisitionError):
                acquisition.all_features(bad,'reviewed','1=1',lambda:140-len(calls),[])

    def test_budget_failure_does_not_acquire_partial_pages(self):
        query,calls=self.query_fixture(4000)
        with self.assertRaises(acquisition.AcquisitionError):acquisition.all_features(query,'reviewed','1=1',lambda:5-len(calls),[])
        self.assertEqual(len(calls),1)

    def test_each_selected_entity_controls_its_own_calendar_start(self):
        first=date(2025,8,23);today=first+timedelta(days=400);rows={}
        for ident in ('fast','slow'):
            for n in range(400):
                day=first+timedelta(days=n)
                if ident=='slow' and n==111:continue
                rows[ident+'|'+day.isoformat()]={'portid':ident,'date':day.isoformat(),'portcalls':None if n==122 else 0}
        self.assertEqual(acquisition.cohort_start(rows,['fast'],first,today),today-timedelta(days=4))
        self.assertEqual(acquisition.cohort_start(rows,['slow'],first,today),first+timedelta(days=111))
        self.assertEqual(acquisition.cohort_start(rows,['fast','slow'],first,today),first+timedelta(days=111))
        self.assertEqual(acquisition.cohort_start(rows,['new'],first,today),first)
        rows['new|2099-01-01']={'portid':'new','date':'2099-01-01','portcalls':1}
        self.assertEqual(acquisition.cohort_start(rows,['new'],first,today),first)

    def test_merge_rejects_ambiguous_rows_before_mutating_any_history(self):
        original={'old|2020-01-01':{'date':'2020-01-01','portid':'old','portcalls':0}}
        good={'portid':'new','date':'2026-09-26','portcalls':0}
        for bad in (good,{**good,'portid':None},{**good,'date':'2026-09-26T12:00:00Z'},{**good,'portid':'bad|identity'}):
            rows=deepcopy(original)
            with self.assertRaises(acquisition.AcquisitionError):acquisition.merge_rows(rows,[good,bad])
            self.assertEqual(rows,original)
        rows=deepcopy(original);self.assertEqual(acquisition.merge_rows(rows,[good]),1)
        self.assertEqual(rows['new|2026-09-26']['portcalls'],0);self.assertIn('old|2020-01-01',rows)
        self.assertEqual(acquisition.quoted_ids(["p'1"]),"'p''1'")

    def native_module(self):
        # The engine runner is __main__; importing it again would create a second compiler.
        return sys.modules['__main__']

    def test_native_queries_every_matching_port_beyond_120_and_replays_all(self):
        t=self.native_module();m,old,calls,opener=t.fixture()
        # 131 extra matching ports, including those formerly silently excluded.
        for i in range(3,134):
            opener.datasets['PortWatch_ports_database'].append({'ObjectId':i,'portid':'port'+str(i),'portname':'Shanghai '+str(i),'country':'China'})
            for n in range(30):
                opener.datasets['Daily_Ports_Data'].append({'ObjectId':len(opener.datasets['Daily_Ports_Data'])+1,
                    'portid':'port'+str(i),'date':(t.NOW-timedelta(days=30-n)).date().isoformat(),'portcalls':n})
        t.native.S3=m
        with contextlib.redirect_stdout(t.BytesIOText()):t.store.run(t.native,opener=opener,at=t.NOW.isoformat())
        packet=t.store.strict(m.data[t.store.HEAD]);hist=t.store.decode(m.data[t.store.HISTORY],t.store.HISTORY)
        self.assertEqual(packet['ports_ref_matched_total'],133);self.assertEqual(len(packet['ports']),133)
        self.assertEqual(len(hist['ports']),1200+131*30)
        self.assertEqual(len(packet['port_reference_review']),133)
        self.assertEqual(packet['measurement_review']['entity_count'],139)
        self.assertTrue(all(q['membership_reconciled'] for q in packet['acquisition_review']['queries']))
        with contextlib.redirect_stdout(t.BytesIOText()):result=t.store.replay(t.native,m,t.native.BUCKET,packet)
        self.assertEqual(result['history_rows']['ports'],5130);self.assertEqual(result['provider_requests'],0)

    def test_failed_membership_never_replaces_complete_public_or_history(self):
        t=self.native_module();m,old,calls,opener=t.fixture();before=deepcopy(m.data)
        def bad(req,timeout=None):
            response=opener(req,timeout);packet=t.store.strict(response.read())
            if 'objectIds' in packet:packet['objectIds']=[]
            raw=t.body(packet);return t.store.Response(raw,headers={'Content-Length':str(len(raw))})
        t.native.S3=m
        with contextlib.redirect_stdout(t.BytesIOText()),self.assertRaises(acquisition.AcquisitionError):
            t.store.run(t.native,opener=bad,at=t.NOW.isoformat())
        self.assertEqual({k:m.data[k] for k in before},before)

    def test_full_133_port_backfill_fits_original_budget_without_dropped_ids_or_days(self):
        t=self.native_module();m,old,calls,opener=t.fixture()
        ids=['port'+str(i) for i in range(1,134)]
        opener.datasets['PortWatch_ports_database']=[{'ObjectId':i,'portid':pid,'portname':'Shanghai '+pid,'country':'China'} for i,pid in enumerate(ids,1)]
        rows=[{'ObjectId':i*401+n+1,'portid':pid,'date':(t.NOW-timedelta(days=400-n)).date().isoformat(),'portcalls':0}
              for i,pid in enumerate(ids) for n in range(401)]
        opener.datasets['Daily_Ports_Data']=rows
        old['ports']={};m.seed(t.store.HISTORY,t.compress(old));t.native.S3=m
        with contextlib.redirect_stdout(t.BytesIOText()):t.store.run(t.native,opener=opener,at=t.NOW.isoformat())
        p=t.store.strict(m.data[t.store.HEAD]);hist=t.store.decode(m.data[t.store.HISTORY],t.store.HISTORY)
        self.assertEqual(len(hist['ports']),133*401)
        self.assertEqual(set(hist['ports']),{r['portid']+'|'+r['date'] for r in rows})
        reviews=[q for q in p['acquisition_review']['queries'] if '/Daily_Ports_Data/' in q['url']]
        self.assertEqual(len(reviews),14);self.assertEqual(sum(q['returned_rows'] for q in reviews),133*401)
        selected=[]
        for q in reviews:
            batch=t.re.findall(r"'([^']+)'",q['where'].split('portid IN (',1)[1])
            self.assertLessEqual(len(batch),10);selected.extend(batch)
            self.assertIn("2025-08-23",q['where']);self.assertIn("2026-09-27",q['where'])
        self.assertEqual(sorted(selected),sorted(ids));self.assertEqual(len(calls),106)
        self.assertEqual(t.native.REQ_BUDGET,140);self.assertEqual(p['acquisition_review']['port_cohort_max_ids'],10)
        with contextlib.redirect_stdout(t.BytesIOText()):result=t.store.replay(t.native,m,t.native.BUCKET,p)
        self.assertEqual(result['history_rows']['ports'],53333);self.assertEqual(result['provider_requests'],0)

    def test_port_count_transport_failure_has_one_attempt_and_preserves_both_heads(self):
        t=self.native_module();m,old,calls,opener=t.fixture();before=deepcopy(m.data);failed=[]
        def transport(req,timeout=None):
            if '/Daily_Ports_Data/' in req.full_url and 'returnCountOnly=true' in req.full_url:
                failed.append(req.full_url);raise TimeoutError('fixture transport timeout')
            return opener(req,timeout)
        t.native.S3=m
        with contextlib.redirect_stdout(t.BytesIOText()),self.assertRaises(t.store.CaptureError):
            t.store.run(t.native,opener=transport,at=t.NOW.isoformat())
        self.assertEqual(len(failed),1);self.assertEqual({k:m.data[k] for k in before},before)
