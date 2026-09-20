import hashlib
import io
import json


class Conditional(Exception):
    response = {'Error': {'Code': 'PreconditionFailed'}}


class S3:
    def __init__(self):
        self.docs = {'data/position-sizing.json': json.dumps({'engine':'position-sizer','version':'1.1',
            'generated_at':'2026-09-19T23:32:24Z','sized_positions':[{'ticker':'SYNTHETIC','suggested_size_pct':6}]}).encode()}
        self.reads, self.writes = [], []
        self.race = False

    def get_object(self, **kw):
        key = kw['Key']; self.reads.append(key)
        if not key.startswith(('data/position-sizing.json','data/scenario-model/','audit-private/20260909-originals/legacy-public-sizer/')):
            raise AssertionError('Unexpected source read: '+key)
        raw = self.docs[key]
        return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest()}

    def put_object(self, **kw):
        key, raw = kw['Key'], kw['Body']
        if kw.get('IfNoneMatch') == '*' and key in self.docs: raise Conditional()
        if kw.get('IfMatch') and (self.race or hashlib.sha256(self.docs[key]).hexdigest() != kw['IfMatch']): raise Conditional()
        self.docs[key] = raw; self.writes.append(key)
        return {}
