/* Ordered snapshot mirror. Revisions precede acquisition; no portfolio authority.
 * One dedicated Durable Object serializes this store. S3 is a separate commit.
 */
import { riskBody, riskDigest } from './portfolio-publication.js';
import { snapshotFrame, SNAPSHOT_PROTOCOL } from './portfolio-snapshot.js';

export const SNAPSHOT_PUBLICATION_PROTOCOL = 'portfolio-snapshot-publication.v1';
export const SNAPSHOT_OBJECT = 'private-artifact:portfolio-snapshot:publication-v1';
const MAX_BYTES = 20000000, CHUNK_BYTES = 256 * 1024, RESERVATIONS = 128;
const KEY = 'private-artifact:portfolio-snapshot';
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const revision = value => Number.isSafeInteger(value) && value >= 1;
const digestPattern = /^[a-f0-9]{64}$/;
const tokenPattern = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
const headers = { 'Content-Type': 'application/json', 'Cache-Control': 'private, no-store', Vary: 'Authorization' };
const json = (value, status = 200) => new Response(JSON.stringify(value), { status, headers });
class SnapshotError extends Error {
  constructor(code, status = 503) { super(code); this.status = status; }
}
const fail = (code, status) => { throw new SnapshotError(code, status); };
function frame(raw) {
  try { return snapshotFrame(raw); }
  catch { fail('complete_compatible_snapshot_required', 400); }
}
function utc(value) {
  if (typeof value !== 'string') return false;
  const m = /^(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)(?:\.(\d{1,6}))?(?:Z|\+00:00)$/.exec(value);
  if (!m) return false;
  const y=Number(m[1]), month=Number(m[2]), day=Number(m[3]);
  const leap=y%4===0 && (y%100!==0 || y%400===0);
  const days=[31,leap?29:28,31,30,31,30,31,31,30,31,30,31];
  return y>=1 && month>=1 && month<=12 && day>=1 && day<=days[month-1] && Number(m[4])<24 && Number(m[5])<60 && Number(m[6])<60;
}
function publication(value) {
  return object(value) && value.schema_version===SNAPSHOT_PUBLICATION_PROTOCOL && revision(value.revision) && utc(value.started_at);
}
function manifestValid(m) {
  return object(m) && Number.isSafeInteger(m.bytes) && m.bytes>0 && m.bytes<=MAX_BYTES && typeof m.sha256==='string' && digestPattern.test(m.sha256) &&
    m.chunks===Math.ceil(m.bytes/CHUNK_BYTES) && typeof m.prefix==='string' && /^(?:snapshot:body:[1-9][0-9]*|snapshot:legacy):$/.test(m.prefix);
}
async function putBody(txn,prefix,raw,sha256) {
  const chunks=Math.ceil(raw.byteLength/CHUNK_BYTES);
  for(let i=0;i<chunks;i++) await txn.put(prefix+i,raw.slice(i*CHUNK_BYTES,(i+1)*CHUNK_BYTES));
  return {prefix,bytes:raw.byteLength,sha256,chunks};
}
async function readBody(storage,manifest) {
  if(!manifestValid(manifest)) fail('stored_manifest_invalid');
  const raw=new Uint8Array(manifest.bytes);
  for(let i=0;i<manifest.chunks;i++) {
    const part=await storage.get(manifest.prefix+i);
    if(!(part instanceof Uint8Array) || part.byteLength!==Math.min(CHUNK_BYTES,raw.byteLength-i*CHUNK_BYTES)) fail('stored_body_incomplete');
    raw.set(part,i*CHUNK_BYTES);
  }
  if(await riskDigest(raw)!==manifest.sha256) fail('stored_body_mismatch');
  return raw;
}
async function legacyBody(env) {
  const value=await env.USER_DATA.get(KEY,{type:'arrayBuffer'});
  if(value===null) return null;
  if(!(value instanceof ArrayBuffer)) fail('legacy_body_unavailable');
  const raw=new Uint8Array(value);
  try { snapshotFrame(raw); } catch { fail('legacy_body_invalid'); }
  return raw;
}
async function currentState(storage) {
  const current=await storage.get('snapshot:current');
  if(current!==undefined && (!object(current) || !revision(current.revision) || typeof current.token!=='string' || !tokenPattern.test(current.token) || !manifestValid(current.body) ||
      current.body.prefix!=='snapshot:body:'+current.revision+':' || !Number.isSafeInteger(current.identity_bytes) || current.identity_bytes<1 || current.identity_bytes>32*1024*1024)) fail('stored_publication_invalid');
  const governed=await storage.get('snapshot:governed');
  if((governed!==undefined && governed!==true) || (governed===true)!==(current!==undefined)) fail('stored_publication_incomplete');
  return current;
}
async function currentBody(storage,current) {
  const raw=await readBody(storage,current.body);
  let checked;
  try { checked=snapshotFrame(raw); } catch { fail('stored_snapshot_invalid'); }
  if(!publication(checked.document.publication) || checked.document.publication.revision!==current.revision || checked.identityBytes!==current.identity_bytes) fail('stored_snapshot_identity_mismatch');
  return raw;
}
function acknowledged(current,status) {
  return json({ok:true,protocol:SNAPSHOT_PUBLICATION_PROTOCOL,revision:current.revision,body_sha256:current.body.sha256,
    body_bytes:current.body.bytes,identity_bytes:current.identity_bytes,status});
}

// Called only through the dedicated serialized object after service/owner auth.
export async function handleSnapshotPublication(storage,env,request) {
  try {
    const url=new URL(request.url);
    if(request.method==='POST' && url.searchParams.get('action')==='reserve') {
      const {document:doc}=frame(await riskBody(request,4096));
      const minimum=Object.hasOwn(doc,'minimum_revision')?doc.minimum_revision:0;
      if(!Number.isSafeInteger(minimum) || minimum<0 || minimum>=Number.MAX_SAFE_INTEGER) fail('invalid_minimum_revision',400);
      const token=crypto.randomUUID();
      return await storage.transaction(async txn=>{
        const current=await currentState(txn),previous=await txn.get('snapshot:counter');
        if((current && previous===undefined) || (previous!==undefined && (!Number.isSafeInteger(previous) || previous<0 || previous<(current?.revision??0)))) fail('stored_counter_invalid');
        const issued=Math.max(previous??0,current?.revision??0,minimum)+1;
        if(!revision(issued)) fail('revision_exhausted');
        await txn.put('snapshot:counter',issued);await txn.put('snapshot:reservation:'+issued,token);
        const old=await txn.list({prefix:'snapshot:reservation:',limit:RESERVATIONS+1});
        for(const key of old.keys()) if(Number(key.slice('snapshot:reservation:'.length))<=issued-RESERVATIONS) await txn.delete(key);
        return json({ok:true,protocol:SNAPSHOT_PUBLICATION_PROTOCOL,revision:issued,token});
      });
    }
    const current=await currentState(storage);
    if(request.method==='GET' || request.method==='HEAD') {
      const raw=current?await currentBody(storage,current):await legacyBody(env);
      if(raw===null) return json({error:'private artifact awaiting sync'},503);
      return new Response(request.method==='HEAD'?null:raw,{headers:{...headers,'Content-Length':String(raw.byteLength),
        ...(current?{'X-JH-Publication-Revision':String(current.revision),'X-JH-Body-SHA256':current.body.sha256}:{})}});
    }
    if(request.method!=='PUT') return json({error:'method not allowed'},405);
    const raw=await riskBody(request),checked=frame(raw),pub=checked.document.publication;
    const sha256=await riskDigest(raw),declared=request.headers.get('X-JH-Body-SHA256');
    if(declared!==null && (!digestPattern.test(declared) || declared!==sha256)) fail('body_digest_mismatch',400);
    if(pub===undefined) {
      if(current) fail('ordered_publication_required',409);
      // Existing native writer remains compatible until ordered cutover. Its
      // acknowledgement and complete bytes are unchanged; KV is not ordered.
      await env.USER_DATA.put(KEY,new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(raw));
      return json({ok:true,protocol:SNAPSHOT_PROTOCOL,body_bytes:raw.byteLength,body_sha256:sha256,identity_bytes:checked.identityBytes});
    }
    if(!publication(pub)) fail('invalid_publication',400);
    const token=request.headers.get('X-JH-Publication-Token');
    if(!tokenPattern.test(token??'') || declared===null) fail('publication_proof_required',400);
    if(current && pub.revision<current.revision) fail('superseded_publication',409);
    if(current && pub.revision===current.revision) {
      if(token!==current.token || sha256!==current.body.sha256) fail('revision_body_conflict',409);
      await currentBody(storage,current);
      return acknowledged(current,'unchanged');
    }
    if(await storage.get('snapshot:reservation:'+pub.revision)!==token) fail('unknown_publication_reservation',409);
    const legacy=current?null:await legacyBody(env),legacySha=legacy?await riskDigest(legacy):null;
    if(current) await currentBody(storage,current);
    return await storage.transaction(async txn=>{
      const actual=await currentState(txn);
      if((actual?.revision??0)!==(current?.revision??0)) fail('publication_changed',409);
      if(await txn.get('snapshot:reservation:'+pub.revision)!==token) fail('unknown_publication_reservation',409);
      if(!actual && legacy) {
        if(await txn.get('snapshot:legacy')) fail('legacy_retention_conflict');
        await txn.put('snapshot:legacy',await putBody(txn,'snapshot:legacy:',legacy,legacySha));
      }
      const body=await putBody(txn,'snapshot:body:'+pub.revision+':',raw,sha256);
      const accepted={revision:pub.revision,token,body,identity_bytes:checked.identityBytes};
      await txn.put('snapshot:current',accepted);await txn.put('snapshot:governed',true);
      // Whole prior S3 packets belong to the native publisher's immutable
      // archive. This store keeps current plus its whole migration predecessor.
      if(actual) for(let i=0;i<actual.body.chunks;i++) await txn.delete(actual.body.prefix+i);
      return acknowledged(accepted,'published');
    });
  } catch(error) {
    if(error instanceof SnapshotError) return json({ok:false,error:error.message},error.status);
    if([400,408,413].includes(error?.status)) return json({ok:false,error:'complete_compatible_snapshot_required'},error.status);
    return json({ok:false,error:'private_snapshot_publication_unavailable'},503);
  }
}

export async function routeSnapshotPublication(request,env,cors) {
  const unavailable=()=>new Response(JSON.stringify({error:'durable private storage unavailable'}),{status:503,headers:{...cors,...headers}});
  if(!env.WORKSPACE_COORDINATOR) return unavailable();
  try {
    const stub=env.WORKSPACE_COORDINATOR.get(env.WORKSPACE_COORDINATOR.idFromName(SNAPSHOT_OBJECT));
    const response=await stub.fetch(new Request('https://portfolio-snapshot.internal/snapshot-publication'+new URL(request.url).search,request));
    const combined=new Headers(cors);
    for(const [key,value] of response.headers) combined.set(key,value);
    for(const [key,value] of Object.entries(headers)) combined.set(key,value);
    return new Response(response.body,{status:response.status,headers:combined});
  } catch { return unavailable(); }
}
