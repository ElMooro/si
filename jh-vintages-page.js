(function(){
'use strict';
const api=JHVintages,$=id=>document.getElementById(id);
const field=$('archive-series'),day=$('archive-day'),status=$('archive-status'),result=$('archive-result');
let index=null,collectionSha='',revision=0;
const initial=new URLSearchParams(location.search);let selectedSeries=initial.get('series')||'WTREGEN',initialError=null;
if(initial.has('day')){try{day.value=api.date(initial.get('day'));}catch(error){initialError=error.message;}}
function message(text,error=false){status.textContent=text;status.classList.toggle('error',error);}
function clear(){result.replaceChildren();$('archive-links').replaceChildren();$('archive-identity').textContent='';$('archive-coverage').textContent='';}
function anchor(label,href){const a=document.createElement('a');a.textContent=label;a.href=href;return a;}
async function inspect(){
 if(!index)return;const token=++revision;clear();result.setAttribute('aria-busy','true');message('Verifying the selected immutable archive…');
 try{
  const sid=field.value,doc=await api.loadPacket(index,sid);if(token!==revision)return;
  if(!day.value)day.value=doc.coverage.archive_end;api.date(day.value);
  const archiveDay=day.value;
  $('archive-coverage').textContent=`${sid} coverage: ${doc.coverage.archive_start} through ${doc.coverage.archive_end}. ${doc.n_vintages.toLocaleString()} retained value intervals. Collection built ${index.generated_at}.`;
  const packet=await api.resolve(doc,archiveDay);if(token!==revision)return;
  result.innerHTML=api.render(packet,archiveDay);
  const pinned=new URL(location.href);pinned.search=new URLSearchParams({series:sid,day:archiveDay,run:collectionSha}).toString();history.replaceState(null,'',pinned);
  $('archive-links').append(anchor('Permanent collection / date link',pinned.href),anchor('Complete series JSON','/'+index.detail[sid].key),anchor('Collection manifest','/data/vintage-research/collections/'+collectionSha+'.json'));
  if(doc.contract==='fred-vintage-segments.v1')$('archive-links').append(anchor('Selected segment JSON','/'+doc.segments.find(s=>s.coverage.archive_start<=archiveDay&&s.coverage.archive_end>=archiveDay).key));
  $('archive-identity').textContent=`Collection ${collectionSha} · Series output ${index.detail[sid].sha256}`;
  const missing=Object.entries(index.detail).filter(([,v])=>v.status!=='source_replayed').map(([k])=>k);
  message(`Output hash verified. ${index.n_series}/${index.requested_series} series retained in this collection.${missing.length?' Unavailable: '+missing.join(', ')+'.':''}`);
  const button=$('vintage-verify');if(button)button.addEventListener('click',async()=>{
   button.disabled=true;const note=$('vintage-verified');note.textContent='Verifying both original responses…';
   try{const text=await api.verify(packet,archiveDay);if(token===revision)note.textContent=text;}
   catch(error){if(token===revision){note.textContent='Verification failed: '+error.message;note.classList.add('error');}}
   finally{if(token===revision)button.disabled=false;}
  });
 }catch(error){if(token===revision){clear();message('Archive unavailable: '+error.message+'. No replacement value is used.',true);}}
 finally{if(token===revision)result.setAttribute('aria-busy','false');}
}
async function load(pinned){
 const token=++revision;clear();field.disabled=true;$('archive-inspect').disabled=true;message('Loading retained collection…');
 try{
  if(initialError)throw Error(initialError);
  if(pinned&&!/^[a-f0-9]{64}$/.test(pinned))throw Error('Invalid collection identity');
  const url=pinned?'/data/vintage-research/collections/'+pinned+'.json':'/data/vintage/_index.json';
  const response=await fetch(url,{cache:'no-store'});if(!response.ok)throw Error('Source catalog HTTP '+response.status);
  const raw=await api.bounded(response.body),hash=await api.sha(raw);if(pinned&&hash!==pinned)throw Error('Collection hash differs');
  const parsed=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw));if(token!==revision)return;
  if(parsed.contract!=='fred-vintage-index.v1'||!Array.isArray(parsed.series)||!parsed.series.length)throw Error('Original source catalog unavailable');
  index=parsed;collectionSha=hash;field.replaceChildren();
  for(const sid of index.series){const option=document.createElement('option');option.value=sid;option.textContent=sid+(index.detail?.[sid]?.status==='source_replayed'?'':' — unavailable');field.append(option);}
  if(!index.series.includes(selectedSeries))throw Error('Requested series is not present in this collection');
  field.value=selectedSeries;
  field.disabled=false;$('archive-inspect').disabled=false;await inspect();
 }catch(error){if(token===revision){index=null;clear();result.setAttribute('aria-busy','false');message('Archive unavailable: '+error.message,true);}}
}
$('archive-form').addEventListener('submit',event=>{event.preventDefault();selectedSeries=field.value;inspect();});
field.addEventListener('change',()=>{selectedSeries=field.value;inspect();});
$('archive-refresh').addEventListener('click',()=>{day.value='';initialError=null;selectedSeries=field.value&&index?.series.includes(field.value)?field.value:'WTREGEN';load(null);});
load(initial.get('run'));
})();
