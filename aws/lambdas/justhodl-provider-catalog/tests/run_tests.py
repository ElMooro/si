"""Dependency-free actual-handler regressions; optional invented benchmark."""
import argparse
import json
from pathlib import Path
import sys
import unittest

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--benchmark-output')
    args=parser.parse_args()
    result=unittest.TextTestRunner(verbosity=1).run(unittest.defaultTestLoader.discover(str(HERE),pattern='test_*.py'))
    if not result.wasSuccessful():return 1
    if args.benchmark_output:
        from catalog_fixture import full_store,run
        records=[]
        for count in (5001,25001):
            baseline=run(400,full_store(count),measure=True)
            candidate=run(1000,full_store(count),measure=True)
            assert baseline['writes']==candidate['writes'] and baseline['rows']==candidate['rows']
            row={'invented':True,'inventory_objects':count,'whole_outputs_equal':True}
            for label,value in (('before',baseline),('after',candidate)):
                row[label]={k:value[k] for k in ('elapsed_seconds','peak_python_bytes')}
                row[label]['prefix_list_calls']=sum(x['Prefix']=='data/warm/ofr/' for x in value['list_calls'])
                row[label]['total_fixture_list_calls']=len(value['list_calls'])
                row[label]['output_bytes']=sum(len(x['body']) for x in value['writes'].values())
            records.append(row)
        Path(args.benchmark_output).write_text(json.dumps({'schema_version':'provider-list-fixture-benchmark.v1',
            'scope':'Fixed invented inventories, clock and gzip mtime; full handler/SQLite/shard outputs; Python allocations only. No S3 network latency or Lambda runtime/memory proof.',
            'runs':records},indent=2)+'\n',encoding='utf-8')
    return 0


if __name__=='__main__':sys.exit(main())
