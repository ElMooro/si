"""
justhodl-portfolio-snapshot — Roadmap #9 portfolio enrichment

═══════════════════════════════════════════════════════════════════════
SNAPSHOTS YOUR ENTIRE PORTFOLIO + WATCHLIST EVERY HOUR
─────────────────────────────────────────────────────
Reads positions/watchlist from DDB, joins with the entire JustHodl
intelligence stack (alpha-score, confluence, regime-picks, sentiment),
fetches split-adjusted previous-day Polygon closes, computes descriptive
holdings P&L and writes a unified sidecar. Marks are not execution quotes.

Pipeline:
  1. Scan DDB for POSITION + WATCHLIST items
  2. Auto-sync watchlist: pull current TIER S/A stocks from alpha-score
     - Replace AUTO_TIER_S / AUTO_TIER_A entries with current top picks
     - Leave MANUAL watchlist entries untouched
  3. For each symbol (deduplicated):
       - Fetch identified, complete previous-day bars (per-symbol parallel)
       - Join with alpha-score row (alpha, tier, components, signals, flags)
       - Join with confluence row (confluence_tier, components_firing)
       - Join with regime row (regime_adj, regime_adj_score)
  4. For POSITIONS: compute eligible descriptive P&L from previous closes
  5. Write portfolio/snapshot.json

Schedule: existing hourly :40 EventBridge rule and Scheduler, preserved.
Uses the existing configured Polygon key; no new paid AI dependency.
"""
import json
from private_artifact import publish_private, private_http_denied
import math
import os
import time
import urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

import boto3

S3_BUCKET = "justhodl-dashboard-live"
SNAPSHOT_KEY = "portfolio/snapshot.json"
ALPHA_KEY = "screener/alpha-score.json"
CONFLUENCE_KEY = "signals/confluence.json"
REGIME_KEY = "signals/regime-picks.json"
SENTIMENT_KEY = "sentiment/data.json"

TABLE_NAME = "justhodl-portfolio"
POLY_KEY = os.environ.get("POLY_KEY", "")

# How many TIER S + TIER A stocks to auto-sync into watchlist
AUTO_WATCH_TIER_S_LIMIT = 10
AUTO_WATCH_TIER_A_LIMIT = 15

s3 = boto3.client("s3", region_name="us-east-1")
ddb_client = boto3.client("dynamodb", region_name="us-east-1")
ddb_res = boto3.resource("dynamodb", region_name="us-east-1")
table = ddb_res.Table(TABLE_NAME)


import hashlib
import json
import math
import re
import uuid
from datetime import datetime, timezone

SOURCE_KEY = "screener/alpha-score.json"
META_KEY = {"pk": "SYNC_META", "sk": "AUTO_WATCHLIST_V1"}
MAX_ALPHA_AGE_H = 3
MAX_SCREEN_AGE_H = 120
LIMITS = {"S": AUTO_WATCH_TIER_S_LIMIT, "A": AUTO_WATCH_TIER_A_LIMIT}
AUTO = {"AUTO_TIER_S", "AUTO_TIER_A"}


def stamp(value):
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError("SOURCE_CLOCK_INVALID")
    try:
        out = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if out.tzinfo is None or out.utcoffset() is None:
            raise ValueError()
        return out.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        raise ValueError("SOURCE_CLOCK_INVALID") from None


def source_frame(data, now):
    if not isinstance(data, dict):
        raise ValueError("SOURCE_NOT_OBJECT")
    rows = data.get("stocks")
    if not isinstance(rows, list) or not rows or len(rows) > 20000:
        raise ValueError("SOURCE_UNIVERSE_UNAVAILABLE")
    if type(data.get("count")) is not int or data["count"] != len(rows):
        raise ValueError("SOURCE_COUNT_MISMATCH")
    if not isinstance(data.get("model_version"), str) or not data["model_version"].strip():
        raise ValueError("SOURCE_VERSION_UNAVAILABLE")
    generated = stamp(data.get("generated_at"))
    inputs = data.get("inputs")
    screen = stamp(inputs.get("screener_generated_at") if isinstance(inputs, dict) else None)
    for clock, maximum in ((generated, MAX_ALPHA_AGE_H), (screen, MAX_SCREEN_AGE_H)):
        if not -300 <= (now - clock).total_seconds() <= maximum * 3600:
            raise ValueError("SOURCE_CLOCK_OUTSIDE_POLICY")
    if (screen - generated).total_seconds() > 300:
        raise ValueError("SOURCE_CLOCK_ORDER_INVALID")
    if "quality" in data:
        quality = data["quality"]
        if not isinstance(quality, dict) or quality.get("status") != "fresh":
            raise ValueError("SOURCE_EXPLICIT_QUALITY_INELIGIBLE")
    desired, seen, counts, scored = {}, set(), {t: 0 for t in "SABCD"}, 0
    previous_score = math.inf
    unscored_started = False
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("SOURCE_ROW_INVALID")
        symbol, score, tier = row.get("symbol"), row.get("alpha_score"), row.get("tier")
        if not isinstance(symbol, str) or not re.fullmatch(r"[A-Z0-9][A-Z0-9.\-]{0,31}", symbol) or symbol in seen:
            raise ValueError("SOURCE_IDENTITY_INVALID_OR_DUPLICATE")
        seen.add(symbol)
        if score is None:
            if tier != "—" or row.get("rank") is not None:
                raise ValueError("SOURCE_UNSCORED_ROW_INVALID")
            unscored_started = True
            continue
        if type(score) not in (int, float) or not 0 <= score <= 100 or not math.isfinite(score):
            raise ValueError("SOURCE_SCORE_INVALID")
        expected = "S" if score >= 90 else "A" if score >= 80 else "B" if score >= 70 else "C" if score >= 50 else "D"
        scored += 1
        if tier != expected or type(row.get("rank")) is not int or row["rank"] != scored or score > previous_score or unscored_started:
            raise ValueError("SOURCE_RANK_OR_TIER_INCONSISTENT")
        previous_score = score
        counts[tier] += 1
        if tier in LIMITS and counts[tier] <= LIMITS[tier]:
            desired[symbol] = "AUTO_TIER_" + tier
    if type(data.get("scored_count")) is not int or data["scored_count"] != scored or not scored:
        raise ValueError("SOURCE_SCORED_COUNT_INVALID")
    distribution = data.get("tier_distribution")
    if not isinstance(distribution, dict) or set(distribution) != set(counts) or any(type(distribution[t]) is not int or distribution[t] != counts[t] for t in counts):
        raise ValueError("SOURCE_TIER_COUNTS_MISMATCH")
    try:
        raw = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        raise ValueError("SOURCE_NOT_FINITE_JSON") from None
    return desired, {"source_key": SOURCE_KEY, "source_generated_at": generated.isoformat(),
                     "screener_generated_at": screen.isoformat(), "source_sha256": hashlib.sha256(raw).hexdigest(),
                     "source_count": len(rows), "source_model_version": data["model_version"],
                     "age_policy_hours": {"alpha": MAX_ALPHA_AGE_H, "screener": MAX_SCREEN_AGE_H},
                     "investment_qualified": False}


def observed_condition(item):
    """Compare all observed fields and absence of standard owner-edit fields."""
    names, values, terms = {}, {}, []
    fields = set(item) | {"source", "symbol", "added_at", "notes", "source_generated_at", "sync_version"}
    for index, field in enumerate(sorted(fields)):
        alias, placeholder = "#f" + str(index), ":v" + str(index)
        names[alias] = field
        if field in item:
            values[placeholder] = item[field]
            terms.append(alias + " = " + placeholder)
        else:
            terms.append("attribute_not_exists(" + alias + ")")
    return {"ConditionExpression": " AND ".join(terms), "ExpressionAttributeNames": names, "ExpressionAttributeValues": values}


def plan(existing, meta, desired, provenance, now, table_name, version):
    if meta is not None:
        if not isinstance(meta, dict) or meta.get("pk") != META_KEY["pk"] or meta.get("sk") != META_KEY["sk"] or not isinstance(meta.get("version"), str) or not re.fullmatch(r"[0-9a-f-]{36}", meta["version"]) or not isinstance(meta.get("source_sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", meta["source_sha256"]):
            raise ValueError("SYNC_CHECKPOINT_INVALID")
        prior, incoming = stamp(meta.get("source_generated_at")), stamp(provenance["source_generated_at"])
        if incoming < prior:
            raise ValueError("SOURCE_REVISION_OLDER_THAN_CHECKPOINT")
        if incoming == prior:
            if meta["source_sha256"] != provenance["source_sha256"]:
                raise ValueError("SOURCE_REVISION_CONTENT_CONFLICT")
            return [], {"status": "UNCHANGED", "reason_codes": [], "added_S": [], "added_A": [], "removed_S": [], "removed_A": [], "provenance": provenance}
    indexed = {}
    for row in existing:
        if not isinstance(row, dict) or row.get("pk") != "WATCHLIST" or not isinstance(row.get("symbol"), str) or row.get("sk") != row["symbol"] or row["symbol"] in indexed:
            raise ValueError("EXISTING_WATCHLIST_IDENTITY_INVALID")
        indexed[row["symbol"]] = row
    changes = {"added_S": [], "added_A": [], "removed_S": [], "removed_A": []}
    operations = []
    for symbol in sorted(set(indexed) | set(desired)):
        old, new_source = indexed.get(symbol), desired.get(symbol)
        if old is not None and old.get("source") not in AUTO:
            continue  # Manual and unrecognized owners are both protected.
        key = {"pk": "WATCHLIST", "sk": symbol}
        if old is not None and new_source is None:
            operations.append({"Delete": {"TableName": table_name, "Key": key, **observed_condition(old)}})
            changes["removed_" + old["source"][-1]].append(symbol)
        elif old is None and new_source is not None:
            operations.append({"Put": {"TableName": table_name, "Item": {**key, "symbol": symbol, "source": new_source, "added_at": now.isoformat(), "source_generated_at": provenance["source_generated_at"], "sync_version": version}, "ConditionExpression": "attribute_not_exists(pk)"}})
            changes["added_" + new_source[-1]].append(symbol)
        elif old is not None:
            condition = observed_condition(old)
            condition["ExpressionAttributeNames"].update({"#new_source": "source", "#new_clock": "source_generated_at", "#new_version": "sync_version"})
            condition["ExpressionAttributeValues"].update({":new_source": new_source, ":new_clock": provenance["source_generated_at"], ":new_version": version})
            operations.append({"Update": {"TableName": table_name, "Key": key, "UpdateExpression": "SET #new_source = :new_source, #new_clock = :new_clock, #new_version = :new_version", **condition}})
            if old["source"] != new_source:
                changes["removed_" + old["source"][-1]].append(symbol)
                changes["added_" + new_source[-1]].append(symbol)
    checkpoint = {**(meta or {}), **META_KEY, "version": version, "source_generated_at": provenance["source_generated_at"], "source_sha256": provenance["source_sha256"], "updated_at": now.isoformat()}
    guard = {"ConditionExpression": "attribute_not_exists(pk)"} if meta is None else {"ConditionExpression": "#v = :v AND #c = :c AND #h = :h", "ExpressionAttributeNames": {"#v": "version", "#c": "source_generated_at", "#h": "source_sha256"}, "ExpressionAttributeValues": {":v": meta["version"], ":c": meta["source_generated_at"], ":h": meta["source_sha256"]}}
    operations.insert(0, {"Put": {"TableName": table_name, "Item": checkpoint, **guard}})
    if len(operations) > 100:
        raise ValueError("SYNC_TRANSACTION_TOO_LARGE")
    return operations, {**changes, "status": "APPLIED", "reason_codes": [], "provenance": provenance, "sync_version": version, "transaction_operations": len(operations)}


def sync_watchlist(table, client, data, now=None):
    now = now or datetime.now(timezone.utc)
    empty = {"added_S": [], "added_A": [], "removed_S": [], "removed_A": []}
    try:
        desired, provenance = source_frame(data, now)
    except ValueError as error:
        return {**empty, "status": "SKIPPED_INVALID_SOURCE", "reason_codes": [str(error)], "write_attempted": False}
    try:
        response = table.get_item(Key=META_KEY, ConsistentRead=True)
        if not isinstance(response, dict):
            raise ValueError("SYNC_CHECKPOINT_READ_INVALID")
        meta = response.get("Item")
        existing, last, seen_pages = [], None, set()
        while True:
            args = {"KeyConditionExpression": "pk = :pk", "ExpressionAttributeValues": {":pk": "WATCHLIST"}, "ConsistentRead": True}
            if last is not None:
                args["ExclusiveStartKey"] = last
            response = table.query(**args)
            if not isinstance(response, dict) or not isinstance(response.get("Items"), list):
                raise ValueError("WATCHLIST_READ_INCOMPLETE")
            existing.extend(response["Items"])
            if len(existing) > 20000:
                raise ValueError("WATCHLIST_READ_BOUND_EXCEEDED")
            last = response.get("LastEvaluatedKey")
            if not last:
                break
            marker = json.dumps(last, sort_keys=True, default=str)
            if marker in seen_pages:
                raise ValueError("WATCHLIST_PAGINATION_REPEATED")
            seen_pages.add(marker)
        version = str(uuid.uuid4())
        operations, result = plan(existing, meta, desired, provenance, now, table.name, version)
        if not operations:
            return {**result, "write_attempted": False}
        from boto3.dynamodb.types import TypeSerializer
        serializer = TypeSerializer()
        wire = []
        for operation in operations:
            kind, body = next(iter(operation.items()))
            out = dict(body)
            for field in ("Item", "Key", "ExpressionAttributeValues"):
                if field in out:
                    out[field] = {k: serializer.serialize(v) for k, v in out[field].items()}
            wire.append({kind: out})
        if len(json.dumps(wire, default=str).encode("utf-8")) > 3_500_000:
            raise ValueError("SYNC_TRANSACTION_BYTE_BOUND")
    except ValueError as error:
        return {**empty, "status": "SKIPPED_INVALID_STATE", "reason_codes": [str(error)], "provenance": provenance, "write_attempted": False}
    except Exception:
        return {**empty, "status": "SKIPPED_READ_OR_PREPARE_ERROR", "reason_codes": ["SYNC_READ_OR_PREPARE_FAILED"], "provenance": provenance, "write_attempted": False}
    try:
        acknowledgement = client.transact_write_items(TransactItems=wire, ClientRequestToken=version)
        metadata = acknowledgement.get("ResponseMetadata") if isinstance(acknowledgement, dict) else None
        if not isinstance(metadata, dict) or type(metadata.get("HTTPStatusCode")) is not int or metadata["HTTPStatusCode"] != 200:
            raise ValueError("Transaction acknowledgement unavailable")
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        conflict = code in {"TransactionCanceledException", "ConditionalCheckFailedException"}
        return {**empty, "status": "CONFLICT" if conflict else "WRITE_UNCONFIRMED", "reason_codes": ["SYNC_TRANSACTION_REJECTED" if conflict else "SYNC_ACKNOWLEDGEMENT_UNAVAILABLE"], "provenance": provenance, "write_attempted": True}
    return {**result, "write_attempted": True}


# ═══════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════

def _scrub_decimals(obj):
    """Recursively convert Decimal → float for JSON output."""
    if isinstance(obj, Decimal): return float(obj)
    if isinstance(obj, dict): return {k: _scrub_decimals(v) for k, v in obj.items()}
    if isinstance(obj, list): return [_scrub_decimals(x) for x in obj]
    return obj


def load_s3_json(key, default=None):
    body = None
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = response["Body"]
        expected = response.get("ContentLength")
        if type(expected) is not int or not 0 <= expected <= 32 * 1024 * 1024:
            raise ValueError("invalid source byte length")
        chunks, size, deadline = [], 0, time.monotonic() + 20
        while True:
            if time.monotonic() > deadline:
                raise ValueError("source acceptance deadline exceeded")
            chunk = body.read(min(65536, 32 * 1024 * 1024 + 1 - size))
            if not isinstance(chunk, bytes):
                raise ValueError("source stream type invalid")
            if time.monotonic() > deadline:
                raise ValueError("source acceptance deadline exceeded")
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
            if size > 32 * 1024 * 1024:
                raise ValueError("source byte bound exceeded")
        if size != expected:
            raise ValueError("incomplete source body")
        def pairs(rows):
            out = {}
            for name, value in rows:
                if name in out:
                    raise ValueError("duplicate source member")
                out[name] = value
            return out
        def invalid_constant(value):
            raise ValueError("nonfinite source number")
        out = json.loads(b"".join(chunks).decode("utf-8"), object_pairs_hook=pairs, parse_constant=invalid_constant)
        # Reject numeric overflow, lone surrogates and non-object sidecars.
        json.dumps(out, allow_nan=False, ensure_ascii=False).encode("utf-8")
        if not isinstance(out, dict):
            raise ValueError("source sidecar is not an object")
        return out
    except Exception:
        print(f"  [s3:{key}] source unavailable or invalid")
        return default
    finally:
        if body is not None:
            body.close()


BOOK_MAX_ROWS=10000
BOOK_MAX_PAGES=100
BOOK_MAX_BYTES=8*1024*1024
BOOK_READ_SECONDS=20


class BookReadUnavailable(RuntimeError):
    """Fixed diagnostics never include private record values."""


def query_pk(pk):
    """Complete, bounded, individually consistent pages; retain exact stored values."""
    if pk not in {'POSITION','WATCHLIST'}:raise BookReadUnavailable('Unsupported book partition')
    items=[];last_key=None;seen_keys=set();seen_cursors=set();size=2;pages=0
    deadline=time.monotonic()+BOOK_READ_SECONDS
    while True:
        if pages>=BOOK_MAX_PAGES or time.monotonic()>deadline:raise BookReadUnavailable('Complete book read bound exceeded')
        kwargs={'KeyConditionExpression':'pk = :pk','ExpressionAttributeValues':{':pk':pk},'ConsistentRead':True}
        if last_key is not None:kwargs['ExclusiveStartKey']=last_key
        try:response=table.query(**kwargs)
        except Exception:raise BookReadUnavailable('Complete book page unavailable') from None
        pages+=1
        if time.monotonic()>deadline:raise BookReadUnavailable('Complete book read deadline exceeded')
        if not isinstance(response,dict) or not isinstance(response.get('Items'),list):raise BookReadUnavailable('Complete book page malformed')
        for item in response['Items']:
            if time.monotonic()>deadline:raise BookReadUnavailable('Complete book read deadline exceeded')
            if not isinstance(item,dict) or item.get('pk')!=pk or not isinstance(item.get('sk'),str) or not item['sk']:
                raise BookReadUnavailable('Book record identity malformed')
            identity=(item['pk'],item['sk'])
            if identity in seen_keys:raise BookReadUnavailable('Repeated book record identity')
            seen_keys.add(identity)
            try:
                source=accounting_source(item)
                json.dumps(source,ensure_ascii=False,allow_nan=False).encode('utf-8')
                size+=len(json.dumps(source,ensure_ascii=True,allow_nan=False).encode('utf-8'))+(1 if items else 0)
            except (TypeError,ValueError,UnicodeError,RecursionError):raise BookReadUnavailable('Unsupported complete book record encoding') from None
            if size>BOOK_MAX_BYTES or len(items)>=BOOK_MAX_ROWS:raise BookReadUnavailable('Complete book read bound exceeded')
            items.append(item)
        cursor=response.get('LastEvaluatedKey')
        if cursor is None or cursor=={}:break
        if not isinstance(cursor,dict) or set(cursor)!={'pk','sk'} or cursor.get('pk')!=pk or not isinstance(cursor.get('sk'),str) or not cursor['sk']:
            raise BookReadUnavailable('Book continuation malformed')
        marker=(cursor['pk'],cursor['sk'])
        if marker in seen_cursors:raise BookReadUnavailable('Repeated book continuation')
        seen_cursors.add(marker);last_key=cursor
    return items

PREVIOUS_CLOSE_MAX_BYTES=128*1024
PREVIOUS_CLOSE_READ_SECONDS=20


class PreviousCloseUnavailable(ValueError):
    def __init__(self,code):super().__init__(code);self.code=code


class NoQuoteRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,request,fp,code,message,headers,newurl):
        try:fp.close()
        finally:raise PreviousCloseUnavailable('REDIRECT_REFUSED')


def parse_previous_close(raw,symbol):
    """One identified split-adjusted previous-day bar, never a current trade quote."""
    if accounting_symbol({'symbol':symbol}) is None:raise PreviousCloseUnavailable('UNSUPPORTED_REQUEST_IDENTITY')
    def pairs(rows):
        value={}
        for key,item in rows:
            if key in value:raise PreviousCloseUnavailable('DUPLICATE_JSON_FIELD')
            value[key]=item
        return value
    def constant(_):raise PreviousCloseUnavailable('NONFINITE_JSON')
    try:
        data=json.loads(raw.decode('utf-8','strict'),object_pairs_hook=pairs,parse_constant=constant)
        json.dumps(data,allow_nan=False,ensure_ascii=False).encode('utf-8')
    except PreviousCloseUnavailable:raise
    except (ValueError,UnicodeError,TypeError,OverflowError,RecursionError):raise PreviousCloseUnavailable('INVALID_COMPLETE_JSON') from None
    if not isinstance(data,dict):raise PreviousCloseUnavailable('RESPONSE_OBJECT_REQUIRED')
    if data.get('status')!='OK':raise PreviousCloseUnavailable('PROVIDER_STATUS_NOT_OK')
    if data.get('ticker')!=symbol:raise PreviousCloseUnavailable('RESPONSE_TICKER_MISMATCH')
    if data.get('adjusted') is not True:raise PreviousCloseUnavailable('ADJUSTMENT_BASIS_MISMATCH')
    rows=data.get('results')
    if not isinstance(rows,list) or len(rows)!=1 or type(data.get('resultsCount')) is not int or data['resultsCount']!=1 or type(data.get('queryCount')) is not int or data['queryCount']!=1:
        raise PreviousCloseUnavailable('SINGLE_COMPLETE_BAR_REQUIRED')
    row=rows[0]
    if not isinstance(row,dict) or ('T' in row and row['T']!=symbol):raise PreviousCloseUnavailable('BAR_TICKER_MISMATCH')
    values={}
    for field in ('o','h','l','c','v'):
        value=row.get(field)
        try:finite=type(value) in (int,float) and math.isfinite(value)
        except (ValueError,OverflowError):finite=False
        if not finite or (value<0 if field=='v' else value<=0):raise PreviousCloseUnavailable('INVALID_BAR_NUMBER')
        values[field]=value
    if values['l']>min(values['o'],values['c']) or values['h']<max(values['o'],values['c']) or values['l']>values['h']:
        raise PreviousCloseUnavailable('INCOHERENT_OHLC_RANGE')
    stamp=row.get('t')
    if type(stamp) is not int or not 0<=stamp<=2**53-1:raise PreviousCloseUnavailable('INVALID_AGGREGATE_WINDOW_TIME')
    return {'price':values['c'],'open':values['o'],'high':values['h'],'low':values['l'],'volume':values['v'],
            'as_of_unix_ms':stamp,'price_basis':'SPLIT_ADJUSTED_PREVIOUS_DAY_CLOSE',
            'price_timestamp_basis':'AGGREGATE_WINDOW_START_UTC_MS','currency':None}


def read_previous_close_body(response,deadline):
    if type(getattr(response,'status',None)) is not int or response.status!=200:raise PreviousCloseUnavailable('HTTP_STATUS_NOT_OK')
    headers=getattr(response,'headers',None)
    if headers is None:raise PreviousCloseUnavailable('RESPONSE_HEADERS_UNAVAILABLE')
    content_type=headers.get('Content-Type','').split(';',1)[0].strip().lower()
    if content_type!='application/json':raise PreviousCloseUnavailable('CONTENT_TYPE_NOT_JSON')
    encoding=headers.get('Content-Encoding','identity').strip().lower()
    if encoding not in ('','identity'):raise PreviousCloseUnavailable('CONTENT_ENCODING_UNSUPPORTED')
    declared=headers.get('Content-Length')
    if declared is not None:
        if not isinstance(declared,str) or not re.fullmatch(r'[0-9]+',declared):raise PreviousCloseUnavailable('INVALID_CONTENT_LENGTH')
        declared=int(declared)
        if declared>PREVIOUS_CLOSE_MAX_BYTES:raise PreviousCloseUnavailable('RESPONSE_BYTE_BOUND')
    chunks=[];size=0
    while True:
        if time.monotonic()>deadline:raise PreviousCloseUnavailable('READ_DEADLINE')
        chunk=response.read(min(16384,PREVIOUS_CLOSE_MAX_BYTES+1-size))
        if time.monotonic()>deadline:raise PreviousCloseUnavailable('READ_DEADLINE')
        if not isinstance(chunk,bytes):raise PreviousCloseUnavailable('INVALID_BODY_CHUNK')
        if not chunk:break
        size+=len(chunk)
        if size>PREVIOUS_CLOSE_MAX_BYTES:raise PreviousCloseUnavailable('RESPONSE_BYTE_BOUND')
        chunks.append(chunk)
    if declared is not None and size!=declared:raise PreviousCloseUnavailable('INCOMPLETE_DECLARED_BODY')
    return b''.join(chunks)


def fetch_polygon_latest(symbol):
    """Keep complete accepted bytes and fixed failure reasons, never provider error text."""
    import base64
    import urllib.parse
    evidence={'schema_version':'previous-close-source.v1','requested_symbol':symbol,'requested_adjusted':True,
              'endpoint_origin':'https://api.polygon.io','endpoint_path':None,
              'started_at':datetime.now(timezone.utc).isoformat(),'completed_at':None,
              'body_complete':False,'status':'UNAVAILABLE','reason_code':None,
              'currency_verified':False,'instrument_binding_verified':False,'execution_quote':False}
    output={'price':None,'open':None,'high':None,'low':None,'volume':None,'as_of_unix_ms':None,
            'price_basis':'SPLIT_ADJUSTED_PREVIOUS_DAY_CLOSE','price_timestamp_basis':'AGGREGATE_WINDOW_START_UTC_MS',
            'currency':None,'source_evidence':evidence}
    try:
        if accounting_symbol({'symbol':symbol}) is None:raise PreviousCloseUnavailable('UNSUPPORTED_REQUEST_IDENTITY')
        path='/v2/aggs/ticker/'+symbol+'/prev';evidence['endpoint_path']=path
        if not isinstance(POLY_KEY,str) or not POLY_KEY:raise PreviousCloseUnavailable('PROVIDER_UNCONFIGURED')
        url=evidence['endpoint_origin']+path+'?adjusted=true&apiKey='+urllib.parse.quote(POLY_KEY,safe='')
        request=urllib.request.Request(url,headers={'User-Agent':'JustHodl-PS/1.0','Accept':'application/json','Accept-Encoding':'identity'})
        opener=urllib.request.build_opener(NoQuoteRedirect())
        deadline=time.monotonic()+PREVIOUS_CLOSE_READ_SECONDS
        with opener.open(request,timeout=8) as response:
            final=urllib.parse.urlsplit(response.geturl())
            if final.scheme!='https' or final.netloc!='api.polygon.io' or final.path!=path or final.query!=urllib.parse.urlsplit(url).query or final.fragment:
                raise PreviousCloseUnavailable('RESPONSE_URL_MISMATCH')
            raw=read_previous_close_body(response,deadline)
            evidence.update(body_complete=True,body_bytes=len(raw),body_sha256=hashlib.sha256(raw).hexdigest(),
                            body_encoding='base64',body=base64.b64encode(raw).decode('ascii'),http_status=200)
        output.update(parse_previous_close(raw,symbol))
        evidence.update(status='MEASURED_PREVIOUS_CLOSE',reason_code=None)
    except PreviousCloseUnavailable as failure:evidence['reason_code']=failure.code
    except urllib.error.HTTPError as failure:
        evidence['reason_code']='HTTP_ERROR'
        if type(failure.code) is int:evidence['http_status']=failure.code
        try:failure.close()
        except Exception:pass
    except Exception:evidence['reason_code']='TRANSPORT_OR_SOURCE_UNAVAILABLE'
    evidence['completed_at']=datetime.now(timezone.utc).isoformat()
    return output
def batch_fetch_prices(symbols, max_workers=10):
    """Parallel price fetch."""
    if not symbols: return {}
    out = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_polygon_latest, s): s for s in symbols}
        for f in as_completed(futures):
            sym = futures[f]
            try: out[sym] = f.result()
            except Exception: out[sym] = None
    return out


# ═══════════════════════════════════════════════════════════════════════
# AUTO-WATCHLIST SYNC
# ═══════════════════════════════════════════════════════════════════════

def sync_auto_watchlist(alpha_data):
    """Preserve existing rows on invalid input; atomically protect owner edits."""
    return {"schema_version": "watchlist-sync.v1", **sync_watchlist(table, ddb_client, alpha_data)}


# ═══════════════════════════════════════════════════════════════════════
# ENRICHMENT
# ═══════════════════════════════════════════════════════════════════════

def index_by_symbol(rows, sym_key="symbol"):
    return {r[sym_key]: r for r in rows if isinstance(r, dict) and isinstance(r.get(sym_key), str)} if isinstance(rows, list) else {}


def enrich_symbol(sym, price_data, alpha_idx, confluence_s_idx, confluence_a_idx,
                    confluence_b_idx, regime_picks_idx, sentiment_idx):
    """Build a unified enriched record for one symbol."""
    rec = {"symbol": sym}
    p = price_data.get(sym)
    p = p if isinstance(p, dict) else {}
    rec["current_price"] = p.get("price")
    # audit 2026-09-08 INST-08: carry the mark's provenance with the price
    rec["price_asof_unix_ms"] = p.get("as_of_unix_ms")
    quote_source = p.get("source_evidence") if isinstance(p.get("source_evidence"), dict) else {}
    rec["price_basis"] = p.get("price_basis")
    rec["price_timestamp_basis"] = p.get("price_timestamp_basis")
    rec["price_source"] = {key: quote_source.get(key) for key in
        ("schema_version", "requested_symbol", "requested_adjusted", "status", "reason_code",
         "body_sha256", "started_at", "completed_at", "currency_verified", "instrument_binding_verified")}

    rec["price_provider"] = "polygon:prev_close" if p.get("price") is not None else None
    rec["price_open"] = p.get("open")
    rec["price_high"] = p.get("high")
    rec["price_low"] = p.get("low")
    rec["volume"] = p.get("volume")

    # Alpha-score
    alpha_row = alpha_idx.get(sym)
    if alpha_row:
        rec["alpha_score"] = alpha_row.get("alpha_score")
        rec["tier"] = alpha_row.get("tier")
        rec["rank"] = alpha_row.get("rank")
        rec["name"] = alpha_row.get("name")
        rec["sector"] = alpha_row.get("sector")
        rec["components"] = alpha_row.get("components")
        rec["top_signals"] = (alpha_row.get("top_signals") or [])[:3]
        rec["risk_flags"] = (alpha_row.get("risk_flags") or [])[:3]

    # Confluence (which tier of confluence?)
    if sym in confluence_s_idx:
        rec["confluence_tier"] = "S"
        rec["confluence_count"] = confluence_s_idx[sym].get("confluence_count")
        rec["components_firing"] = confluence_s_idx[sym].get("components_firing")
    elif sym in confluence_a_idx:
        rec["confluence_tier"] = "A"
        rec["confluence_count"] = confluence_a_idx[sym].get("confluence_count")
    elif sym in confluence_b_idx:
        rec["confluence_tier"] = "B"
        rec["confluence_count"] = confluence_b_idx[sym].get("confluence_count")
    else:
        rec["confluence_tier"] = None

    # Regime fit
    regime_row = regime_picks_idx.get(sym)
    if regime_row:
        rec["regime_adj"] = regime_row.get("regime_adj")
        rec["regime_adj_score"] = regime_row.get("regime_adj_score")

    # News sentiment
    sent_row = sentiment_idx.get(sym)
    if sent_row:
        rec["sentiment_signal"] = sent_row.get("sentimentSignal")
        rec["sentiment_score"] = sent_row.get("sentimentScore")
        rec["sentiment_reason"] = (sent_row.get("sentimentReason") or "")[:140]

    return rec


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def accounting_number(value):
    """Only measured finite numbers; never coerce booleans, text or absence."""
    if type(value) not in (int, float, Decimal):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, OverflowError):
        return None


def accounting_round(value, digits=2):
    value = accounting_number(value)
    return round(value, digits) if value is not None else None


def accounting_sum(values, empty_book=False):
    if not values:
        return 0.0 if empty_book else None
    try:
        return accounting_number(math.fsum(values))
    except (ValueError, OverflowError):
        return None


def accounting_source(value):
    """Retain rejected source values without producing illegal JSON numbers."""
    if isinstance(value, dict):
        return {k: accounting_source(v) for k, v in value.items()}
    if isinstance(value, list):
        return [accounting_source(v) for v in value]
    if type(value) in (float, Decimal) and accounting_number(value) is None:
        return {"rejected_number_type": type(value).__name__, "representation": str(value)}
    if type(value) is Decimal:
        return {"source_number_type": "Decimal", "representation": str(value)}
    return value


def accounting_symbol(row):
    value = row.get("symbol") if isinstance(row, dict) else None
    return value if isinstance(value, str) and re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,14}", value) else None


def build_holdings_accounting(positions, enriched, now):
    """Descriptive legacy cash-equity arithmetic, not NAV or account returns.

    Calculate each row once, aggregate unrounded eligible legs, round for display.
    A missing or invalid input is retained and excluded, never replaced by zero.
    """
    if not isinstance(positions, list):
        raise ValueError("A complete positions list is required")
    records, values, costs, pnls, paired_costs, missing_costs = [], [], [], [], [], []
    sectors, unpriced, unpaired, raw_values = {}, [], [], []
    identities = [accounting_symbol(p) for p in positions]
    counts = {}
    for sym in identities:
        if sym is not None: counts[sym] = counts.get(sym, 0) + 1
    duplicates = {sym for sym, count in counts.items() if count > 1}
    mark_now = now.timestamp()
    for index, original in enumerate(positions):
        p = original if isinstance(original, dict) else {}
        sym = identities[index]
        e = dict(enriched.get(sym, {"symbol": sym}))
        reasons = []
        identity_ok = sym is not None and sym not in duplicates
        if not identity_ok: reasons.append("INVALID_OR_DUPLICATE_SYMBOL")
        qty = accounting_number(p.get("qty"))
        cost = accounting_number(p.get("cost_basis_per_share"))
        if qty is None: reasons.append("QUANTITY_UNAVAILABLE")
        if cost is None or cost < 0:
            cost = None
            reasons.append("COST_BASIS_UNAVAILABLE")
        cost_total = accounting_number(qty * cost) if identity_ok and qty is not None and cost is not None else None
        if identity_ok and qty is not None and cost is not None and cost_total is None:
            reasons.append("COST_BASIS_OVERFLOW")
        price = accounting_number(e.get("current_price"))
        if price is not None and price <= 0: price = None
        stamp_ms = accounting_number(e.get("price_asof_unix_ms"))
        age_s = accounting_number(mark_now - stamp_ms / 1000) if stamp_ms is not None else None
        mark_ok = price is not None and age_s is not None and -300 <= age_s <= 120 * 3600
        status = ("PRICED" if mark_ok else "UNPRICED" if price is None else
                  "STALE_MARK" if age_s is not None and age_s > 120 * 3600 else "INVALID_MARK")
        market = accounting_number(qty * price) if identity_ok and qty is not None and mark_ok else None
        if not identity_ok or qty is None:
            status = "INVALID_POSITION"
        elif mark_ok and market is None:
            status = "INVALID_POSITION"
            reasons.append("MARKET_VALUE_OVERFLOW")
        if not mark_ok: reasons.append(status)
        pnl = accounting_number(market - cost_total) if market is not None and cost_total is not None else None
        if market is not None and cost_total is not None and pnl is None: reasons.append("PNL_OVERFLOW")
        ratio = accounting_number(pnl / abs(cost_total) * 100) if pnl is not None and cost_total else None
        stop = accounting_number(p.get("stop_loss"))
        if stop is not None and stop <= 0: stop = None
        if p.get("stop_loss") is not None and stop is None: reasons.append("STOP_PRICE_INVALID")
        side = ("LONG" if qty >= 0 else "SHORT") if qty is not None else None
        stop_hit = ((price <= stop) if side == "LONG" else (price >= stop)) if market is not None and qty != 0 and stop is not None else None
        distance = accounting_number((price - stop) / stop * 100) if stop_hit is not None else None
        target = accounting_number(p.get("target_weight_pct"))
        if p.get("target_weight_pct") is not None and target is None: reasons.append("TARGET_WEIGHT_INVALID")
        row = {
            **e, "symbol": sym, "source_record_index": index, "qty": qty,
            "current_price": price, "price_asof_unix_ms": stamp_ms,
            "cost_basis_per_share": cost, "cost_basis_total": accounting_round(cost_total),
            "market_value": accounting_round(market), "pnl_dollars": accounting_round(pnl),
            "pnl_pct": accounting_round(ratio), "position_type": side,
            "valuation_status": status, "mark_age_h": accounting_round(age_s / 3600, 1) if age_s is not None else None,
            "pnl_eligible": pnl is not None, "accounting_reason_codes": reasons,
            "stop_loss": stop, "stop_hit": stop_hit, "stop_distance_pct": accounting_round(distance),
            "stop_comparison_scope": "PREVIOUS_CLOSE_NOT_EXECUTION",
            "target_weight_pct": target, "added_at": p.get("added_at"), "notes": p.get("notes"),
            "weight_drift_pct": None, "weight_drift_reason": "TARGET_DENOMINATOR_UNVERIFIED",
        }
        records.append(row); raw_values.append(market)
        identity = sym if sym is not None else "record:" + str(index + 1)
        if market is None:
            unpriced.append(identity)
            if cost_total is not None: missing_costs.append(abs(cost_total))
        else:
            values.append(market)
            sector = e.get("sector") or p.get("sector")
            sector = sector if isinstance(sector, str) and sector.strip() else "Unknown"
            sectors.setdefault(sector, []).append(market)
        if cost_total is not None: costs.append(cost_total)
        if pnl is None: unpaired.append(identity)
        else:
            pnls.append(pnl); paired_costs.append(abs(cost_total))
    count = len(records)
    total = accounting_sum(values, not count)
    gross = accounting_sum([abs(v) for v in values], not count)
    total_cost = accounting_sum(costs, not count)
    total_pnl = accounting_sum(pnls, not count)
    denominator = accounting_sum(paired_costs, not count)
    total_ratio = accounting_number(total_pnl / denominator * 100) if total_pnl is not None and denominator else None
    aggregate_issues = [name for name, rows, value in (
        ("MARKET_VALUE_AGGREGATE_OVERFLOW", values, total), ("GROSS_VALUE_AGGREGATE_OVERFLOW", values, gross),
        ("COST_BASIS_AGGREGATE_OVERFLOW", costs, total_cost), ("PNL_AGGREGATE_OVERFLOW", pnls, total_pnl),
        ("PNL_DENOMINATOR_OVERFLOW", paired_costs, denominator)) if rows and value is None]
    for row, market in zip(records, raw_values):
        row["current_weight_pct"] = accounting_round(market / total * 100) if market is not None and total and not unpriced else None
        row["current_weight_scope"] = "SIGNED_NET_HOLDINGS_NOT_NAV"
        row["gross_holdings_weight_pct"] = accounting_round(abs(market) / gross * 100) if market is not None and gross else None
        row["gross_holdings_weight_scope"] = "PRICED_HOLDINGS_ONLY" if unpriced else "ALL_HOLDINGS"
    concentration = []
    for sector, legs in sectors.items():
        value = accounting_sum(legs)
        concentration.append({"sector": sector, "value": accounting_round(value),
            "weight_pct": accounting_round(value / total * 100) if value is not None and total and not unpriced else None,
            "weight_scope": "SIGNED_NET_HOLDINGS_NOT_NAV"})
    concentration.sort(key=lambda row: (row["value"] is None, -(row["value"] or 0), row["sector"]))
    stops = [{"symbol": row["symbol"], "stop_loss": row["stop_loss"], "current_price": row["current_price"]} for row in records if row["stop_hit"] is True]
    summary = {
        "n_positions": count, "priced_positions_count": len(values), "basis_positions_count": len(costs),
        "pnl_eligible_positions_count": len(pnls),
        "total_market_value": accounting_round(total),
        "total_market_value_scope": "PRICED positions only" if unpriced else "all positions priced",
        "total_cost_basis": accounting_round(total_cost), "total_cost_basis_scope": "Known signed quantity-times-unit-cost only",
        "total_pnl_dollars": accounting_round(total_pnl), "total_pnl_pct": accounting_round(total_ratio),
        "pnl_scope": "Rows with eligible quantity, mark and cost basis only; unrealized price P&L before fees, income and FX",
        "pnl_pct_basis": "SUM_ABSOLUTE_ELIGIBLE_COST_NOT_NAV_RETURN", "pnl_pct_denominator": accounting_round(denominator),
        "unpriced_positions": unpriced, "pnl_excluded_positions": unpaired,
        "unpriced_cost_basis": accounting_round(accounting_sum(missing_costs, not unpriced)),
        "unpriced_cost_basis_known_count": len(missing_costs),
        "stops_hit_count": len(stops), "stops_hit": stops,
        "stops_not_evaluable": [row["symbol"] for row, original in zip(records, positions) if isinstance(original, dict) and original.get("stop_loss") is not None and row["stop_hit"] is None],
        "accounting_coverage_status": "EMPTY" if not count else "PARTIAL" if unpaired or unpriced or aggregate_issues else "COMPLETE",
        "accounting_reason_codes": aggregate_issues,
        "accounting_assumptions": "Legacy single-currency cash-equity arithmetic; instrument, currency and account reconciliation unverified",
    }
    return records, summary, concentration, accounting_round(gross)


def lambda_handler(event, context):
    denied = private_http_denied(event)
    if denied:
        return denied
    started = time.time()
    print(f"=== PORTFOLIO SNAPSHOT · {datetime.now(timezone.utc).isoformat()} ===")

    # 1. Load all sidecars
    alpha = load_s3_json(ALPHA_KEY, {})
    confluence = load_s3_json(CONFLUENCE_KEY, {})
    regime = load_s3_json(REGIME_KEY, {})
    sentiment_data = load_s3_json(SENTIMENT_KEY, {})

    alpha_idx = index_by_symbol(alpha.get("stocks") or [])
    confluence_s_idx = index_by_symbol(confluence.get("tier_s_confluence") or [])
    confluence_a_idx = index_by_symbol(confluence.get("tier_a_confluence") or [])
    confluence_b_idx = index_by_symbol(confluence.get("tier_b_confluence") or [])
    regime_picks_idx = index_by_symbol(regime.get("regime_picks") or [])
    sentiment_idx = index_by_symbol(sentiment_data.get("sentiment") or [])

    print(f"  loaded: alpha={len(alpha_idx)} conf_S={len(confluence_s_idx)} "
          f"conf_A={len(confluence_a_idx)} regime={len(regime_picks_idx)} "
          f"sentiment={len(sentiment_idx)}")

    # 2. Auto-sync watchlist
    validation_only = isinstance(event, dict) and event.get("mode") == "validate_only"
    sync_changes = ({"added_S":[],"added_A":[],"removed_S":[],"removed_A":[],"validation_skipped_sync":True}
                    if validation_only else sync_auto_watchlist(alpha))
    print(f"  watchlist sync: +{len(sync_changes['added_S'])} S "
          f"+{len(sync_changes['added_A'])} A "
          f"-{len(sync_changes['removed_S'])+len(sync_changes['removed_A'])} stale")

    # 3. Re-query after sync
    book_read_started_at = datetime.now(timezone.utc).isoformat()
    positions = query_pk("POSITION")
    watchlist = query_pk("WATCHLIST")
    book_read_completed_at = datetime.now(timezone.utc).isoformat()
    print(f"  positions={len(positions)} watchlist={len(watchlist)}")

    # 4. Build unique symbol set + fetch prices in parallel
    all_symbols = set()
    for row in positions + watchlist:
        sym = accounting_symbol(row)
        if sym is not None: all_symbols.add(sym)
    price_data = batch_fetch_prices(list(all_symbols))
    n_priced = sum(1 for v in price_data.values() if isinstance(v, dict) and accounting_number(v.get("price")) is not None and accounting_number(v.get("price")) > 0)
    print(f"  prices fetched: {n_priced}/{len(all_symbols)}")

    # 5. Enrich each symbol
    enriched_by_sym = {}
    for sym in all_symbols:
        enriched_by_sym[sym] = enrich_symbol(
            sym, price_data, alpha_idx,
            confluence_s_idx, confluence_a_idx, confluence_b_idx,
            regime_picks_idx, sentiment_idx)

    # Freeze one valuation clock for every row; eligibility uses unrounded seconds.
    accounting_now = datetime.now(timezone.utc)
    position_records, portfolio_summary, sector_concentration, gross_value = build_holdings_accounting(positions, enriched_by_sym, accounting_now)
    total_value = portfolio_summary["total_market_value"]
    total_pnl = portfolio_summary["total_pnl_dollars"]
    unpriced = portfolio_summary["unpriced_positions"]
    stops_hit = portfolio_summary["stops_hit"]

    # 7. Build WATCHLIST list
    watchlist_records = []
    for w in watchlist:
        sym = accounting_symbol(w)
        w = w if isinstance(w, dict) else {}
        e = enriched_by_sym.get(sym, {"symbol": sym})
        watchlist_records.append({
            **e,
            "source": w.get("source"),
            "auto_source_generated_at": w.get("source_generated_at"),
            "auto_sync_version": w.get("sync_version"),
            "added_at": w.get("added_at"),
            "notes": w.get("notes"),
        })
    # Sort watchlist: S confluence first, then by alpha
    watchlist_records.sort(key=lambda r: (
        0 if r.get("confluence_tier") == "S" else 1 if r.get("confluence_tier") == "A" else 2,
        -(r.get("alpha_score") or 0),
    ))

    elapsed = time.time() - started

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "audit_version": "2026-09-30.4",
        "generated_at_unix": int(time.time()),
        "elapsed_seconds": round(elapsed, 2),

        "portfolio_summary": portfolio_summary,
        "accounting": {
            "schema_version": "holdings-accounting.v1", "valuation_at": accounting_now.isoformat(),
            "source_positions": accounting_source(positions), "source_prices": accounting_source(price_data),
            "source_watchlist": accounting_source(watchlist),
            "book_read": {
                "schema_version": "portfolio-book-read.v1", "status": "COMPLETE",
                "consistency": "STRONGLY_CONSISTENT_PAGES_NOT_ATOMIC_SNAPSHOT",
                "started_at": book_read_started_at, "completed_at": book_read_completed_at,
                "position_count": len(positions), "watchlist_count": len(watchlist),
                "exact_stored_decimal_evidence": True, "account_reconciled": False,
                "per_partition_bounds": {"rows": BOOK_MAX_ROWS, "pages": BOOK_MAX_PAGES,
                    "encoded_bytes": BOOK_MAX_BYTES, "acceptance_seconds": BOOK_READ_SECONDS},
            },
            "nonfinite_source_encoding": "rejected_number_type plus exact source representation",
            "assumptions_verified": False, "allows_sizing": False,
        },

        # A marked holdings sum is not account equity: no cash/liability/order ledger exists here.
        # Consumers must not size from this incomplete book.
        "capital_book": {
            "schema_version": "1.0", "status": "BLOCKED", "allows_new_entries": False,
            "reason_codes": ["MISSING_RECONCILED_CAPITAL_LEDGER"],
            "as_of": datetime.now(timezone.utc).isoformat(), "book_id": None, "account_id": None,
            "currency": None, "equity_nav": None, "cash": None, "liabilities": None,
            "reconciled_at": None, "reserved_order_exposure": None, "nav_history": [], "open_orders": [],
            "gross_exposure": gross_value,
            "net_exposure": total_value,
            "exposure_scope": "PRICED_POSITIONS_ONLY" if unpriced else "ALL_POSITIONS",
            "positions": position_records, "unpriced_positions": unpriced,
        },
        # Positions + watchlist
        "positions": position_records,
        "watchlist": watchlist_records,
        "sector_concentration": sector_concentration,

        # Sync info
        "watchlist_sync": sync_changes,

        # Counts
        "counts": {
            "positions": len(position_records),
            "watchlist": len(watchlist_records),
            "auto_watch_S": sum(1 for w in watchlist if isinstance(w, dict) and w.get("source") == "AUTO_TIER_S"),
            "auto_watch_A": sum(1 for w in watchlist if isinstance(w, dict) and w.get("source") == "AUTO_TIER_A"),
            "manual_watch": sum(1 for w in watchlist if isinstance(w, dict) and w.get("source") == "MANUAL"),
        },
    }

    encoded = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode("utf-8")
    if validation_only:
        return {"ok": True, "validation_only": True, "schema_version": "audit-accounting-1.0",
                "status": payload["capital_book"]["status"], "artifact_size_bytes": len(encoded)}
    publish_private("portfolio-snapshot", payload)
    s3.put_object(Bucket=S3_BUCKET, Key=SNAPSHOT_KEY,
        Body=encoded,
        ContentType="application/json",
        CacheControl="private, no-store")

    print(f"  ✓ snapshot written · {elapsed:.2f}s")

    return {"statusCode": 200, "body": json.dumps({
        "success": True,
        "n_positions": len(position_records),
        "n_watchlist": len(watchlist_records),
        "total_market_value": total_value,
        "total_pnl_dollars": total_pnl,
        "stops_hit_count": len(stops_hit),
        "elapsed_seconds": round(elapsed, 2),
    })}
