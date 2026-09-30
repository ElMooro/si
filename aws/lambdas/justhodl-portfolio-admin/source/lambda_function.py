"""
justhodl-portfolio-admin — Roadmap #9 portfolio CRUD

═══════════════════════════════════════════════════════════════════════
COMMAND-DRIVEN POSITION MANAGEMENT
─────────────────────────────────────
Invoked manually (no schedule) to add/remove/update positions. Event
payload specifies action + parameters. Returns JSON result.

Actions:
  add_position       symbol, qty, cost_basis_per_share, [stop_loss, target_weight_pct, sector, notes]
  remove_position    symbol
  update_position    symbol, [qty, cost_basis_per_share, stop_loss, target_weight_pct, notes]
                     Existing new_* aliases remain accepted; conflicting aliases fail.
                     expected_record_etag protects edits against a stale manager form.
  set_stop_loss      symbol, stop_price
  list               filter: "POSITION" | "WATCHLIST" | "STOPLOSS" | "ALL"
  add_watchlist      symbol, source ("MANUAL" default)
  remove_watchlist   symbol
  clear_auto_watchlist (removes all AUTO_TIER_S/A entries — for clean re-sync)

Example invoke payload:
  {"action": "add_position", "symbol": "LLY", "qty": 50,
   "cost_basis_per_share": 925.00, "stop_loss": 890,
   "target_weight_pct": 7, "sector": "Healthcare",
   "notes": "Daily brief 2026-05-12 LONG recommendation"}

═══════════════════════════════════════════════════════════════════════
"""
import json
from decimal import Decimal
from datetime import datetime, timezone

import boto3

TABLE_NAME = "justhodl-portfolio"
ddb = boto3.resource("dynamodb", region_name="us-east-1")
table = ddb.Table(TABLE_NAME)

# ── auth + pipeline plumbing (Function URL path) ──
import base64
TOKEN_SSM_NAME = "/justhodl/portfolio-admin/token"
ALLOWED_ORIGINS = {"https://justhodl.ai", "https://www.justhodl.ai"}
SNAPSHOT_FN = "justhodl-portfolio-snapshot"
# mutations to the book that should refresh the downstream snapshot
_MUTATING = {"add_position", "remove_position", "update_position",
             "set_stop_loss"}
_ssm = boto3.client("ssm", region_name="us-east-1")
_lam = boto3.client("lambda", region_name="us-east-1")
_token_cache = {"v": None}


import hashlib
import math
import re
import uuid
from functools import wraps
from decimal import localcontext
from boto3.dynamodb.types import TypeSerializer, DYNAMODB_CONTEXT

MAX_REQUEST_BYTES = 65536
MAX_LIST_ITEMS = 10000
MAX_LIST_BYTES = 4 * 1024 * 1024
POSITION_FIELDS = {'symbol','qty','cost_basis_per_share','cost_basis_total','position_type',
                   'stop_loss','target_weight_pct','sector','notes','entry_thesis','added_at',
                   'updated_at','mutation_id'}
EDIT_ALIASES = {'new_qty':'qty','new_cost_basis_per_share':'cost_basis_per_share',
                'new_stop_loss':'stop_loss','new_target_weight':'target_weight_pct','new_notes':'notes'}


class InputError(ValueError):
    def __init__(self, message, status=400, code='INVALID_INPUT'):
        super().__init__(message);self.status=status;self.code=code


def checked_action(function):
    @wraps(function)
    def wrapped(event):
        try:return function(event)
        except InputError as error:
            return {'ok':False,'err':str(error),'error_code':error.code,'_http_status':error.status}
    return wrapped


def _symbol(value):
    if not isinstance(value,str):raise InputError('symbol must be a ticker string')
    symbol=value.strip().upper()
    if not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,14}',symbol):raise InputError('symbol has an unsupported format')
    return symbol


def _number(value,name,minimum=None,positive=False):
    if type(value) not in (int,float,Decimal):raise InputError(name+' must be a finite number')
    try:
        number=Decimal(str(value))
        if not number.is_finite() or not math.isfinite(float(number)):raise ValueError()
        TypeSerializer().serialize(number)
    except Exception:raise InputError(name+' must be a finite DynamoDB-representable number') from None
    if minimum is not None and number<minimum:raise InputError(name+' must be at least '+str(minimum))
    if positive and number<=0:raise InputError(name+' must be positive')
    return number


def _text(value,name,limit=20000):
    if not isinstance(value,str) or len(value)>limit:raise InputError(name+' must be text within '+str(limit)+' characters')
    try:value.encode('utf-8')
    except UnicodeError:raise InputError(name+' contains invalid Unicode') from None
    return value


def _cost_total(qty,cost):
    try:
        with localcontext(DYNAMODB_CONTEXT):value=qty*cost
        return _number(value,'cost_basis_total')
    except Exception:raise InputError('quantity times unit cost exceeds exact supported numeric precision') from None


def _record_tag(item):
    raw=json.dumps(TypeSerializer().serialize(item),sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()


def _item_view(item):
    if not item:return None
    view={**_scrub(item),'record_etag':_record_tag(item),'computed_cost_basis_total':None,'basis_reconciliation_status':'UNAVAILABLE'}
    try:
        computed=_cost_total(_number(item.get('qty'),'stored qty'),_number(item.get('cost_basis_per_share'),'stored cost',minimum=0))
        view['computed_cost_basis_total']=float(computed)
        view['basis_reconciliation_status']='MATCHED' if _number(item.get('cost_basis_total'),'stored total')==computed else 'MISMATCH'
    except InputError:
        if view['computed_cost_basis_total'] is not None:view['basis_reconciliation_status']='STORED_TOTAL_UNAVAILABLE'
    return view


def _observed_condition(item):
    # Compare every observed value and absence of fields any position writer owns.
    # Unknown fields newly added by external writers are preserved by updates;
    # this is not a general table-wide transaction or cross-item revision claim.
    names,values,terms={}, {}, []
    for index,key in enumerate(sorted(set(item)|POSITION_FIELDS)):
        alias='#c'+str(index);names[alias]=key
        if key in item:
            token=':c'+str(index);values[token]=item[key];terms.append(alias+' = '+token)
        else:terms.append('attribute_not_exists('+alias+')')
    condition=' AND '.join(terms)
    if len(condition.encode())>3500:raise InputError('record exceeds conditional edit limit; no write attempted',409,'EDIT_CONDITION_TOO_LARGE')
    return condition,names,values


def _current_position(event):
    symbol=_symbol(event.get('symbol'))
    response=table.get_item(Key={'pk':'POSITION','sk':symbol},ConsistentRead=True)
    current=response.get('Item')
    if not isinstance(current,dict) or not current:
        raise InputError('position '+symbol+' does not exist -- use add_position',404,'POSITION_NOT_FOUND')
    expected=event.get('expected_record_etag')
    if 'expected_record_etag' in event:
        if not isinstance(expected,str) or not re.fullmatch('[0-9a-f]{64}',expected):raise InputError('expected_record_etag must be a complete edit token')
        if expected!=_record_tag(current):raise InputError('position changed since it was displayed; refresh before editing',409,'STALE_EDIT')
    return symbol,current


def _write(method,**request):
    try:response=getattr(table,method)(**request)
    except Exception as error:
        code=getattr(error,'response',{}).get('Error',{}).get('Code')
        if code=='ConditionalCheckFailedException' or 'ConditionalCheckFailed' in str(error):
            raise InputError('position already exists or a concurrent edit changed it; refresh before retrying',409,'CONCURRENT_EDIT') from None
        raise InputError('write outcome unconfirmed; refresh the book before retrying',503,'WRITE_UNCONFIRMED') from None
    if not isinstance(response,dict) or response.get('ResponseMetadata',{}).get('HTTPStatusCode')!=200:
        raise InputError('write outcome unconfirmed; refresh the book before retrying',503,'WRITE_UNCONFIRMED')
    return response


def _edit_values(event):
    allowed={'action','symbol','expected_record_etag','qty','cost_basis_per_share','stop_loss','target_weight_pct','sector','notes'}|set(EDIT_ALIASES)
    if set(event)-allowed:raise InputError('unsupported update fields: '+', '.join(sorted(set(event)-allowed)))
    result={}
    for key,value in event.items():
        field=EDIT_ALIASES.get(key,key)
        if field in {'action','symbol','expected_record_etag'}:continue
        if field in result and (type(value) is not type(result[field]) or value!=result[field]):raise InputError('conflicting aliases for '+field)
        result[field]=value
    if not result:raise InputError('No update fields provided')
    for field,value in list(result.items()):
        if field in {'qty','cost_basis_per_share'}:result[field]=_number(value,field,minimum=0 if field=='cost_basis_per_share' else None)
        elif field in {'stop_loss','target_weight_pct'}:
            result[field]=None if value is None else _number(value,field,positive=field=='stop_loss')
        else:result[field]=_text(value,field,256 if field=='sector' else 20000)
    return result


def _complete_partition(partition):
    rows,seen,cursor,seen_cursors=[],set(),None,set()
    size=0;pages=0
    while True:
        pages+=1
        if pages>100:raise InputError('complete list exceeds page bound; no partial book returned',413,'LIST_TOO_LARGE')
        request={'KeyConditionExpression':'pk = :pk','ExpressionAttributeValues':{':pk':partition},'ConsistentRead':True}
        if cursor is not None:request['ExclusiveStartKey']=cursor
        response=table.query(**request)
        if not isinstance(response,dict) or not isinstance(response.get('Items'),list):raise InputError('complete list unavailable',503,'LIST_UNAVAILABLE')
        for item in response['Items']:
            if not isinstance(item,dict) or item.get('pk')!=partition or not isinstance(item.get('sk'),str):raise InputError('list contains an invalid record',503,'LIST_UNAVAILABLE')
            identity=(item['pk'],item['sk'])
            if identity in seen:raise InputError('list changed during pagination; refresh',409,'LIST_CONFLICT')
            seen.add(identity);rows.append(item)
            size+=len(json.dumps(TypeSerializer().serialize(item),ensure_ascii=True).encode())
        if len(rows)>MAX_LIST_ITEMS or size>MAX_LIST_BYTES:raise InputError('complete list exceeds the response limit; no partial book returned',413,'LIST_TOO_LARGE')
        next_key=response.get('LastEvaluatedKey')
        if not next_key:break
        if not isinstance(next_key,dict) or next_key.get('pk')!=partition or not isinstance(next_key.get('sk'),str):raise InputError('invalid list continuation',503,'LIST_UNAVAILABLE')
        marker=json.dumps(TypeSerializer().serialize(next_key),sort_keys=True)
        # Cursor identity is tracked separately from returned item identity.
        if marker in seen_cursors:raise InputError('repeated list continuation',503,'LIST_UNAVAILABLE')
        seen_cursors.add(marker)
        cursor=next_key
    return rows
def _admin_token():
    """SSM SecureString token, cached for the warm container lifetime."""
    if _token_cache["v"] is None:
        _token_cache["v"] = _ssm.get_parameter(
            Name=TOKEN_SSM_NAME, WithDecryption=True)["Parameter"]["Value"]
    return _token_cache["v"]


def _trigger_snapshot():
    """A successful book write and an acknowledged refresh are different facts."""
    try:
        response=_lam.invoke(FunctionName=SNAPSHOT_FN,Qualifier='live',InvocationType='Event',Payload=b'{}')
        if isinstance(response,dict) and type(response.get('StatusCode')) is int and response['StatusCode']==202 and not response.get('FunctionError'):
            return 'queued'
    except Exception:
        pass
    print('[portfolio-admin] snapshot refresh acknowledgement unavailable')
    return 'unconfirmed'



def _dec(v):
    """Safely convert any numeric to Decimal (DDB requirement)."""
    if v is None: return None
    return Decimal(str(v))


def _scrub(item):
    """Convert Decimal back to float for JSON output."""
    if isinstance(item, list):
        return [_scrub(i) for i in item]
    if isinstance(item, dict):
        return {k: _scrub(v) for k, v in item.items()}
    if isinstance(item, Decimal):
        return float(item)
    return item


@checked_action
def add_position(event):
    allowed={'action','symbol','qty','cost_basis_per_share','stop_loss','target_weight_pct','sector','notes','entry_thesis'}
    if set(event)-allowed:raise InputError('unsupported add-position fields')
    symbol=_symbol(event.get('symbol'));qty=_number(event.get('qty'),'qty');cost=_number(event.get('cost_basis_per_share'),'cost_basis_per_share',minimum=0)
    now=datetime.now(timezone.utc).isoformat()
    item={'pk':'POSITION','sk':symbol,'symbol':symbol,'qty':qty,'cost_basis_per_share':cost,
          'cost_basis_total':_cost_total(qty,cost),'position_type':'LONG' if qty>=0 else 'SHORT',
          'added_at':now,'updated_at':now,'mutation_id':str(uuid.uuid4())}
    for field in ('stop_loss','target_weight_pct','sector','notes','entry_thesis'):
        if field not in event or event[field] is None:continue
        item[field]=_number(event[field],field,positive=field=='stop_loss') if field in ('stop_loss','target_weight_pct') else _text(event[field],field,256 if field=='sector' else 20000)
    _write('put_item',Item=item,ConditionExpression='attribute_not_exists(pk)')
    return {'ok':True,'action':'add_position','item':_item_view(item),'changed':True}



@checked_action
def remove_position(event):
    symbol=_symbol(event.get('symbol'))
    try:symbol,current=_current_position(event)
    except InputError as error:
        if error.code=='POSITION_NOT_FOUND':return {'ok':True,'action':'remove_position','symbol':symbol,'existed':False,'removed_item':None,'changed':False}
        raise
    condition,names,values=_observed_condition(current)
    response=_write('delete_item',Key={'pk':'POSITION','sk':symbol},ConditionExpression=condition,
                    ExpressionAttributeNames=names,ExpressionAttributeValues=values,ReturnValues='ALL_OLD')
    removed=response.get('Attributes')
    if not isinstance(removed,dict):raise InputError('delete acknowledgement lacks the removed record; refresh before retrying',503,'WRITE_UNCONFIRMED')
    return {'ok':True,'action':'remove_position','symbol':symbol,'existed':True,'removed_item':_item_view(removed),'changed':True}



def _finite(value,name):
    return float(_number(value,name))



@checked_action
def update_position(event):
    edits=_edit_values(event)
    symbol,current=_current_position(event)
    if 'qty' in edits or 'cost_basis_per_share' in edits:
        qty=edits['qty'] if 'qty' in edits else _number(current.get('qty'),'stored qty')
        cost=edits['cost_basis_per_share'] if 'cost_basis_per_share' in edits else _number(current.get('cost_basis_per_share'),'stored cost_basis_per_share',minimum=0)
        edits.update(cost_basis_total=_cost_total(qty,cost),position_type='LONG' if qty>=0 else 'SHORT')
    edits.update(updated_at=datetime.now(timezone.utc).isoformat(),mutation_id=str(uuid.uuid4()))
    condition,names,values=_observed_condition(current)
    sets,removes=[],[]
    for index,(field,value) in enumerate(edits.items()):
        alias='#u'+str(index);names[alias]=field
        if value is None:removes.append(alias)
        else:
            token=':u'+str(index);values[token]=value;sets.append(alias+' = '+token)
    expression='SET '+', '.join(sets)
    if removes:expression+=' REMOVE '+', '.join(removes)
    response=_write('update_item',Key={'pk':'POSITION','sk':symbol},UpdateExpression=expression,
                    ConditionExpression=condition,ExpressionAttributeNames=names,ExpressionAttributeValues=values,ReturnValues='ALL_NEW')
    updated=response.get('Attributes')
    if not isinstance(updated,dict) or updated.get('mutation_id')!=edits['mutation_id']:
        raise InputError('update acknowledgement lacks the intended revision; refresh before retrying',503,'WRITE_UNCONFIRMED')
    return {'ok':True,'action':'update_position','updated':_item_view(updated),'changed':True}



@checked_action
def set_stop_loss(event):
    if 'stop_price' not in event:raise InputError('stop_price is required')
    request={'symbol':event.get('symbol'),'stop_loss':event['stop_price']}
    if 'expected_record_etag' in event:request['expected_record_etag']=event['expected_record_etag']
    result=update_position(request)
    if result.get('ok'):
        result.update(action='set_stop_loss',symbol=result['updated']['symbol'],stop_price=result['updated'].get('stop_loss'))
    return result



def add_watchlist(event):
    sym = event["symbol"].upper().strip()
    source = event.get("source", "MANUAL")
    item = {
        "pk": "WATCHLIST", "sk": sym, "symbol": sym,
        "source": source,
        "added_at": datetime.now(timezone.utc).isoformat(),
    }
    if event.get("notes"): item["notes"] = str(event["notes"])
    table.put_item(Item=item)
    return {"ok": True, "action": "add_watchlist", "item": _scrub(item)}


def remove_watchlist(event):
    sym = event["symbol"].upper().strip()
    resp = table.delete_item(
        Key={"pk": "WATCHLIST", "sk": sym},
        ReturnValues="ALL_OLD",
    )
    return {"ok": True, "action": "remove_watchlist", "symbol": sym,
            "existed": bool(resp.get("Attributes"))}


def clear_auto_watchlist(event):
    """Delete all AUTO_TIER_S and AUTO_TIER_A watchlist entries.
    Used before snapshot Lambda re-syncs from current alpha-score."""
    resp = table.query(
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": "WATCHLIST"},
    )
    deleted = []
    with table.batch_writer() as batch:
        for item in resp.get("Items", []):
            if item.get("source", "MANUAL").startswith("AUTO_"):
                batch.delete_item(Key={"pk": item["pk"], "sk": item["sk"]})
                deleted.append(item["symbol"])
    return {"ok": True, "action": "clear_auto_watchlist",
            "deleted_count": len(deleted), "deleted_symbols": deleted}


@checked_action
def list_items(event):
    value=event.get('filter','ALL')
    if not isinstance(value,str) or value.upper() not in {'POSITION','WATCHLIST','STOPLOSS','ALL'}:raise InputError('unsupported list filter')
    selected=value.upper();out={'positions':[],'watchlist':[],'stoploss':[],'meta':[]}
    for partition,key in [('POSITION','positions'),('WATCHLIST','watchlist'),('STOPLOSS','stoploss'),('META','meta')]:
        if selected=='ALL' or selected==partition:
            out[key]=[_item_view(item) if partition=='POSITION' else _scrub(item) for item in _complete_partition(partition)]
    out['counts']={key:len(value) for key,value in out.items()}
    out.update(ok=True,list_complete=True,list_consistency='STRONGLY_CONSISTENT_PAGES_NOT_ATOMIC_SNAPSHOT')
    if len(json.dumps(out,allow_nan=False).encode('utf-8'))>MAX_LIST_BYTES:raise InputError('complete list exceeds response limit; no partial book returned',413,'LIST_TOO_LARGE')
    return out



ACTIONS = {
    "add_position":         add_position,
    "remove_position":      remove_position,
    "update_position":      update_position,
    "set_stop_loss":        set_stop_loss,
    "add_watchlist":        add_watchlist,
    "remove_watchlist":     remove_watchlist,
    "clear_auto_watchlist": clear_auto_watchlist,
    "list":                 list_items,
}


def _dispatch(payload):
    if not isinstance(payload,dict):return 400,{'ok':False,'err':'action body must be an object','error_code':'INVALID_INPUT'}
    action=payload.get('action')
    if not isinstance(action,str) or action not in ACTIONS:return 400,{'ok':False,'err':'unknown or missing action','available_actions':list(ACTIONS)}
    try:
        # Reject values that could not have appeared in a complete finite JSON request.
        if len(json.dumps(payload,allow_nan=False,ensure_ascii=False).encode('utf-8'))>MAX_REQUEST_BYTES:
            return 413,{'ok':False,'err':'request exceeds byte limit','error_code':'REQUEST_TOO_LARGE'}
        result=ACTIONS[action](payload)
    except (KeyError,ValueError,TypeError,OverflowError,UnicodeError):
        return 400,{'ok':False,'err':'invalid or missing action parameters','error_code':'INVALID_INPUT'}
    except Exception:
        return 503,{'ok':False,'err':'operation outcome unconfirmed; refresh before retrying','error_code':'OPERATION_UNCONFIRMED'}
    status=result.pop('_http_status',200)
    if action in _MUTATING and result.get('ok'):
        result['snapshot_refresh']=_trigger_snapshot() if result.get('changed',True) else 'not_requested'
    return status,result



def lambda_handler(event,context):
    if not isinstance(event,dict):return {'statusCode':400,'body':json.dumps({'ok':False,'err':'request must be an object'})}
    context_value=event.get('requestContext');http=context_value.get('http') if isinstance(context_value,dict) else None
    if 'requestContext' not in event:
        status,result=_dispatch(event)
        return {'statusCode':status,'body':json.dumps(result,allow_nan=False)}
    if not isinstance(http,dict):return {'statusCode':400,'body':'{"ok":false,"err":"invalid HTTP context"}'}
    method=http.get('method')
    if method=='OPTIONS':return {'statusCode':200,'body':''}
    if method!='POST':return {'statusCode':405,'body':'{"ok":false,"err":"POST required"}'}
    raw_headers=event.get('headers')
    headers={key.lower():value for key,value in raw_headers.items() if isinstance(key,str)} if isinstance(raw_headers,dict) else {}
    try:
        expected=_admin_token()
        if not isinstance(expected,str) or not expected:raise ValueError('unavailable token')
    except Exception:return {'statusCode':503,'body':'{"ok":false,"err":"authentication unavailable"}'}
    if headers.get('x-justhodl-token')!=expected:return {'statusCode':403,'body':'{"ok":false,"err":"forbidden"}'}
    origin=headers.get('origin')
    if origin and origin not in ALLOWED_ORIGINS:return {'statusCode':403,'body':'{"ok":false,"err":"origin not allowed"}'}
    raw=event.get('body','{}')
    try:
        if not isinstance(raw,str) or len(raw)>MAX_REQUEST_BYTES*2:raise ValueError('body shape or size')
        if event.get('isBase64Encoded') is True:raw=base64.b64decode(raw,validate=True).decode('utf-8','strict')
        if len(raw.encode('utf-8'))>MAX_REQUEST_BYTES:raise ValueError('body size')
        def pairs(items):
            out={}
            for key,value in items:
                if key in out:raise ValueError('duplicate JSON key')
                out[key]=value
            return out
        def constant(value):raise ValueError('nonfinite JSON')
        payload=json.loads(raw,object_pairs_hook=pairs,parse_constant=constant)
        json.dumps(payload,allow_nan=False,ensure_ascii=False).encode('utf-8')
    except Exception:return {'statusCode':400,'body':'{"ok":false,"err":"complete finite JSON object required"}'}
    status,result=_dispatch(payload)
    return {'statusCode':status,'body':json.dumps(result,allow_nan=False),'headers':{'Cache-Control':'private, no-store','Content-Type':'application/json'}}
