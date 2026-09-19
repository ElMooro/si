(async function(){
 'use strict';const $=id=>document.getElementById(id),api=window.JHInflectionResearch;
 try{
  const response=await fetch('/data/liquidity-inflection.json',{cache:'no-store'});
  if(!response.ok)throw Error('Liquidity packet unavailable (HTTP '+response.status+').');
  const packet=await response.json(),view=api.render(packet);
  for(const key of ['summary','legs','series','chart','context','dates','evidence'])$(key).innerHTML=view[key];
  for(const key of ['stamp','archive','methodology','reason'])$(key).textContent=view[key];
  const select=$('dates'),show=()=>{$('sample').innerHTML=api.inspect(packet,select.value);};
  select.addEventListener('change',show);show();$('status').textContent='Research only · WAIT means abstain';
 }catch(error){$('status').textContent=error.message;$('status').classList.add('error');$('stamp').textContent='No current research values are displayed.';}
})();
