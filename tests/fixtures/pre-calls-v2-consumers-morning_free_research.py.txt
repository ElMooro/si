"""Reuse the public deterministic Calls brief; never route to a model API."""
from datetime import datetime, timezone
import re
import json
import math
PUBLIC_KEY='data/ai-brief-public.json'
MAX_AGE_H=8
MAX_MESSAGE_UNITS=4096
PUBLICATION_STAMP=re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})',re.ASCII)
UNAVAILABLE=('JustHodl research update\n\nThe public source-backed brief is unavailable, malformed or more than eight hours old. '
             'No fresh measurements or investment conclusion are substituted.\n\n**DECISIVE CALL: WAIT**\n'
             'Abstain from new allocation guidance. WAIT does not mean sell existing positions. '
             'Review dated research at https://justhodl.ai/calls.html . No model API was called.')


def build(load,at=None):
    """One fixed public packet; preserve its own observation/publication dates."""
    try:
        p=load(PUBLIC_KEY)
        if not isinstance(p,dict):return UNAVAILABLE
        generated=p.get('generated_at')
        if not isinstance(generated,str) or not 20<=len(generated)<=32 or not PUBLICATION_STAMP.fullmatch(generated):return UNAVAILABLE
        stamp=datetime.fromisoformat(generated.replace('Z','+00:00'))
        now=at or datetime.now(timezone.utc)
        if stamp.tzinfo is None or now.tzinfo is None or not 0 <= (now-stamp).total_seconds() <= MAX_AGE_H*3600:return UNAVAILABLE
        if (p.get('generation_method')!='warehouse_deterministic_v1' or p.get('visibility')!='public_projection'
                or type(p.get('paid_api_calls')) is not int or p['paid_api_calls']!=0 or p.get('model') is not None
                or p.get('decision_eligible') is not False or p.get('sizing_eligible') is not False
                or p.get('call_verb')!='WAIT' or type(p.get('coverage',{}).get('eligible_votes')) is not int
                or p['coverage']['eligible_votes']!=0):return UNAVAILABLE
        text=p.get('brief_md')
        if (not isinstance(text,str) or not 120<=len(text)<=16000 or '\x00' in text
                or not text.startswith('# Source-backed market brief\n')
                or not re.search(r'\*\*DECISIVE CALL: WAIT\*\*\s+Abstain from new allocation guidance\.',text)
                or re.search(r'\*\*DECISIVE CALL: (?!WAIT\*\*)',text)):
            return UNAVAILABLE
        message=text.rstrip()+'\n\nResearch source: https://justhodl.ai/calls.html\nPublication: '+generated+'\nNo model API called by Morning Intelligence.'
        # Budget the complete delivered text, including provenance and astral
        # characters. Never let the transport truncate the abstention or dates.
        if len(message.encode('utf-16-le'))//2>MAX_MESSAGE_UNITS:
            return ('JustHodl source-backed research update\n\nPublication: '+generated+
                    '\nThe complete brief exceeds this message size. Read every dated measurement and limitation at '+
                    'https://justhodl.ai/calls.html . No excerpt is substituted for the complete evidence.\n\n'+
                    '**DECISIVE CALL: WAIT**\nAbstain from new allocation guidance. WAIT does not mean sell existing positions. '+
                    'No model API called by Morning Intelligence.')
        return message
    except Exception:
        return UNAVAILABLE


def accuracy_status():
    return ('Performance qualification: unavailable. Legacy hit rates, rankings and self-grading do not establish investable edge. '
            'No forecast or sizing permission is granted. Inspect prospective evidence and unresolved windows at '
            'https://justhodl.ai/signal-scorecard.html .')



def read_public(s3,bucket,key=PUBLIC_KEY):
    """One allowlisted, complete, versioned JSON body; no account/private reads."""
    if key!=PUBLIC_KEY:raise ValueError('Only the public brief may be read')
    obj=s3.get_object(Bucket=bucket,Key=key)
    body=obj['Body'];chunks=[];size=0
    try:
        while True:
            chunk=body.read(min(65536,2*1024*1024+1-size))
            if not chunk:break
            if not isinstance(chunk,bytes):raise ValueError('Byte stream required')
            chunks.append(chunk);size+=len(chunk)
            if size>2*1024*1024:raise ValueError('Public brief exceeds complete-body limit')
    finally:
        body.close()
    raw=b''.join(chunks)
    if type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(raw) or not obj.get('ETag'):
        raise ValueError('Complete versioned public body required')
    def pairs(items):
        result={}
        for k,v in items:
            if k in result:raise ValueError('Duplicate JSON field')
            result[k]=v
        return result
    def invalid(value):raise ValueError('Nonfinite JSON number')
    def number(value):
        out=float(value)
        if not math.isfinite(out):raise ValueError('Nonfinite JSON number')
        return out
    return json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_constant=invalid,parse_float=number)
