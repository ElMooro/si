// Synthetic fixtures only; never used by production pages.
const fs=require('node:fs'),zlib=require('node:zlib'),crypto=require('node:crypto');
function fixture(){
 const base=JSON.parse(fs.readFileSync(__dirname+'/fixtures/etf-holdings-native.json'));
 const overlay=JSON.parse(zlib.gunzipSync(fs.readFileSync(__dirname+'/fixtures/ownership-summary-ui-synthetic.json.gz')));
 const artifacts={...base.artifacts,...overlay.artifacts},p=overlay.packets.holdings;
 const fetcher=async key=>({ok:typeof artifacts[key.slice(1)]==='string',arrayBuffer:async()=>new TextEncoder().encode(artifacts[key.slice(1)]).buffer});
 const put=(doc,group='directories',prefix='data/etf-holdings-research/')=>{const raw=JSON.stringify(doc),sha256=crypto.createHash('sha256').update(raw).digest('hex'),key=prefix+group+'/'+sha256+'.json';artifacts[key]=raw;return {key,sha256,bytes:Buffer.byteLength(raw)};};
 const manifest=()=>JSON.parse(artifacts[p.ownership_summary.manifest.key]);
 const resign=m=>{p.ownership_summary.manifest=put(m);const {replay,...body}=p,output=put(body,'outputs'),run=JSON.parse(artifacts[replay.manifest_key]);run.output=output;run.output_sha256=output.sha256;p.replay={manifest_key:put(run,'runs').key,output_sha256:output.sha256};};
 return {p,packets:overlay.packets,artifacts,fetcher,put,manifest,resign};
}
module.exports=fixture;
