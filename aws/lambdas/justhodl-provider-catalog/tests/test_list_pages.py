import gzip
import json
import math
import unittest
from catalog_fixture import ListingFailure, Store, full_store, module_for, object_at, run

PREFIX = 'data/warm/ofr/'


class Tests(unittest.TestCase):
    def equivalent(self, before, after):
        for key in ('result','writes','rows','head_calls','get_calls'):
            self.assertEqual(before[key],after[key],key)
        walked = {p for r in module_for(1000).REG.values() for p in r.get('prefixes',[])}
        unchanged = lambda calls:[x for x in calls if x.get('Prefix') not in walked]
        self.assertEqual(unchanged(before['list_calls']),unchanged(after['list_calls']))
        for result, size in ((before,400),(after,1000)):
            self.assertTrue(all(x['MaxKeys']==size for x in result['list_calls'] if x.get('Prefix') in walked))

    def test_complete_handler_boundary_inventories(self):
        for n in (0,1,399,400,401,999,1000,1001,5001):
            with self.subTest(objects=n):
                before,after=run(400,full_store(n)),run(1000,full_store(n))
                self.equivalent(before,after)
                count=lambda result:sum(x['Prefix']==PREFIX for x in result['list_calls'])
                self.assertEqual(count(before),max(1,math.ceil(n/400)))
                self.assertEqual(count(after),max(1,math.ceil(n/1000)))
                self.assertEqual(len([r for r in after['rows'] if r[1]=='ofr' and r[5]=='asset']),n+1)
                self.assertEqual(json.loads(after['writes']['data/providers/ofr.json']['body'])['n_keys'],n+1)

    def test_short_and_empty_truncated_pages_follow_tokens(self):
        for plan in ([7,0,31,2],[399,1,0,37],[100,100,1]):
            a,b=full_store(1307),full_store(1307)
            a.page_plans[PREFIX]=plan;b.page_plans[PREFIX]=plan
            self.equivalent(run(400,a),run(1000,b))

    def test_empty_final_page_is_drained(self):
        a,b=full_store(37),full_store(37)
        a.empty_final={PREFIX};b.empty_final={PREFIX}
        before,after=run(400,a),run(1000,b)
        self.equivalent(before,after)
        self.assertEqual(sum(x['Prefix']==PREFIX for x in after['list_calls']),2)

    def test_independent_chains_overlaps_hot_duplicates_order_and_markers(self):
        registry={'fixture':{'name':'Invented provider','api':'fixture','engines':['invented-engine'],
            'prefixes':['data/warm/fixture/','data/warm/fixture/sub/','data/warm/other/'],
            'hot':['data/warm/fixture/key-000000.json','data/missing-hot.json'],
            'count_from':('data/fixture-counts.json','pages','pages_bytes')}}
        objects={f'data/warm/fixture/key-{i:06d}.json':object_at(f'data/warm/fixture/key-{i:06d}.json',100, i%25) for i in range(1301)}
        objects.update({f'data/warm/fixture/sub/key-{i:06d}.json':object_at(f'data/warm/fixture/sub/key-{i:06d}.json',100,2) for i in range(1001)})
        objects.update({f'data/warm/other/key-{i:06d}.json':object_at(f'data/warm/other/key-{i:06d}.json',100,3) for i in range(1001)})
        doc={'data/fixture-counts.json':{'pages':2500000,'pages_bytes':500000000000,'series_extracted':9000000,'updated_at':'2000-01-02T12:00:00+00:00'}}
        before=run(400,Store(objects,doc),registry);after=run(1000,Store(objects,doc),registry)
        for key in ('result','writes','rows','head_calls','get_calls'):self.assertEqual(before[key],after[key],key)
        allkeys=json.loads(after['writes']['data/providers/fixture.json']['body'])['keys']
        for key,item in sorted(after['writes'].items()):
            if key.startswith('data/providers/fixture/page-'):allkeys.extend(json.loads(item['body'])['keys'])
        # Registry prefix ordering survives equal-size stable sorting; hot duplicate follows.
        expected=sorted(k for k in objects if k.startswith('data/warm/fixture/'))+sorted(k for k in objects if k.startswith('data/warm/fixture/sub/'))+sorted(k for k in objects if k.startswith('data/warm/other/'))+['data/warm/fixture/key-000000.json','data/missing-hot.json']
        self.assertEqual([k['key'] for k in allkeys],expected)
        self.assertEqual(allkeys[-1],{'key':'data/missing-hot.json','missing':True,'hot':True,'status':'missing','engines':[]})
        manifest=json.loads(after['writes']['data/providers/fixture.json']['body'])
        self.assertEqual(manifest['derived']['objects'],2500000)
        self.assertEqual(manifest['derived']['bytes'],500000000000)
        self.assertEqual(manifest['total_bytes'],500000000000+(len(expected)-1)*100)
        self.assertEqual(manifest['freshest_h'],0)
        self.assertEqual(manifest['n_keys'],2500000+len(expected)-1)
        shard=json.loads(gzip.decompress(after['writes']['data/search/providers/fixture.json.gz']['body']))
        self.assertEqual(len(shard['rows']),len(expected))  # provider row offsets missing-hot row
        self.assertEqual(len(after['rows']),len(expected))
        for pref in registry['fixture']['prefixes']:
            calls=[x for x in after['list_calls'] if x['Prefix']==pref]
            self.assertNotIn('ContinuationToken',calls[0])
            self.assertGreater(len(calls),1)

    def test_second_page_failure_propagates_without_complete_publication(self):
        for size in (400,1000):
            store=full_store(1501);store.fail_page=(PREFIX,1)
            with self.assertRaisesRegex(ListingFailure,'Invented second-page failure'):
                run(size,store)
            self.assertNotIn('data/provider-catalog.json',store.writes)
            self.assertNotIn('data/search/provider-shards.json',store.writes)
            self.assertEqual(len([x for x in store.list_calls if x['Prefix']==PREFIX]),2)

    def test_manifest_counted_stores_are_not_enumerated(self):
        result=run(1000,full_store(1001))
        hub=json.loads(result['writes']['data/provider-catalog.json']['body'])
        providers={p['slug']:p for p in hub['providers']}
        self.assertEqual(providers['eurostat']['n_keys'],2500000)
        self.assertEqual(providers['eurostat']['total_bytes'],500000000000)
        self.assertEqual(providers['eurostat']['series_count'],9000000)
        self.assertNotIn('data/warm/eurostat-series/',[x['Prefix'] for x in result['list_calls']])
        self.assertNotIn('data/warm/ecb-series/',[x['Prefix'] for x in result['list_calls']])


if __name__=='__main__':unittest.main()
