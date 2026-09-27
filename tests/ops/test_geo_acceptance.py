"""Exercise the read-only acceptance entry with whole public and history bodies."""
from pathlib import Path
from contextlib import contextmanager
from io import BytesIO
from unittest.mock import patch
import hashlib, importlib.util, json, unittest

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('geo_acceptance',ROOT/'aws/ops/staged/ops_6199_geopolitical_research_acceptance.py')
subject=importlib.util.module_from_spec(spec);spec.loader.exec_module(subject)


class Tests(unittest.TestCase):
    def test_complete_native_acceptance_reports_exact_unmodified_bytes(self):
        commit='a'*40;history=b'{"all_history": [1, 2, 3]}\n'
        packet={'contract':subject.store.model.CONTRACT,'generated_at':'2026-09-27T11:30:30+00:00','version':'2.0.0',
                'publication_context':{'planned_complete_history':{'key':'planned'}},
                'sources':{'feeds_in_corpus':164,'feeds_attempted':164,'feeds_responding':161},'quality':{'status':'partial'}}
        raw=(json.dumps(packet,indent=2)+'\n').encode();output={}
        runtime={'receipt':{'status':'matched','commit':commit},'runtime':'python3.12','handler':'lambda_function.lambda_handler',
                 'memory_mb':1024,'timeout':600,'architectures':['x86_64'],'role':'fixture-role','ephemeral_storage_mb':512,'schedules':['original']}
        baseline={'runtime':{'Runtime':runtime['runtime'],'Handler':runtime['handler'],'MemorySize':1024,'Timeout':600,
                            'Architectures':['x86_64'],'Role':'fixture-role','EphemeralStorage':{'Size':512}},'schedules':['original']}
        class S3:
            def get_object(self,**kw):
                value={subject.store.HEAD:raw,subject.store.HISTORY:history}[kw['Key']]
                return {'Body':BytesIO(value),'ContentLength':len(value)}
        class Report:
            def kv(self,**kw):output.update(kw)
        @contextmanager
        def report(name):yield Report()
        def retained(s3,bucket,ref):return history if ref['key']=='planned' else json.dumps(baseline).encode()
        with patch.object(subject.boto3,'client',return_value=S3()),patch.object(subject,'report',report), \
             patch.object(subject,'runtime',return_value=runtime),patch.object(subject.subprocess,'check_output',return_value=commit+'\n'), \
             patch.object(subject.subprocess,'run'),patch.object(subject.store,'retained',side_effect=retained), \
             patch.object(subject.store,'replay',return_value={'provider_requests':0}) as replay:
            subject.main()
        replay.assert_called_once()
        self.assertEqual(output['native_publication']['sha256'],hashlib.sha256(raw).hexdigest())
        self.assertEqual(output['native_publication']['bytes'],len(raw))
        self.assertEqual(output['native_publication']['status'],'complete_native_original_response_and_history_replayed')
        for name in ('native_invocations','provider_requests','account_reads','public_writes','history_writes','schedules_changed'):
            self.assertEqual(output[name],0)


if __name__=='__main__':unittest.main()
