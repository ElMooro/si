/* Descriptive calculations on the complete identified chart frame. No forecasts. */
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.JHStockDeskResearch=api;})(typeof window==='object'?window:null,function(){
 'use strict';
 const finite=x=>typeof x==='number'&&Number.isFinite(x);
 const volume=x=>finite(x)&&x>=0;
 function time(value){
  if(Number.isInteger(value)&&Math.abs(value*1000)<=8640000000000000)return value*1000;
  if(value&&typeof value==='object'&&Number.isInteger(value.year)&&Number.isInteger(value.month)&&Number.isInteger(value.day))
   value=String(value.year).padStart(4,'0')+'-'+String(value.month).padStart(2,'0')+'-'+String(value.day).padStart(2,'0');
  if(typeof value==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(value)){
   const ms=Date.parse(value+'T00:00:00Z');return Number.isFinite(ms)&&new Date(ms).toISOString().slice(0,10)===value?ms:NaN;
  }
  return NaN;
 }
 const label=value=>Number.isFinite(time(value))?new Date(time(value)).toISOString():null;
 function width(rows,end,n=20){
  if(end<n-1)return null;
  const sample=rows.slice(end-n+1,end+1),mean=sample.reduce((s,b)=>s+b.close,0)/n;
  const variance=sample.reduce((s,b)=>s+(b.close-mean)**2,0)/n;
  const ratio=4*Math.sqrt(variance)/mean;if(!finite(ratio)||!finite(mean))return null;
  return {ratio,mean,population_sd:Math.sqrt(variance),observations:n,
   start:label(sample[0].time),end:label(sample.at(-1).time),basis:'four population standard deviations / mean of 20 closes'};
 }
 function volumeRatio(rows,excludeLatest){
  const last=rows.at(-1),end=rows.length-(excludeLatest?1:0),sample=rows.slice(Math.max(0,end-20),end);
  const available=sample.length===20&&sample.every(b=>volume(b.volume))&&volume(last?.volume);
  const computed=available?sample.reduce((s,b)=>s+b.volume,0)/20:null,mean=finite(computed)?computed:null;
  const ratio=available&&mean>0?last.volume/mean:null;
  return {ratio:finite(ratio)?ratio:null,latest:volume(last?.volume)?last.volume:null,
   mean,observations:sample.length,available_volume_rows:sample.filter(b=>volume(b.volume)).length,
   start:sample.length?label(sample[0].time):null,end:sample.length?label(sample.at(-1).time):null,
   includes_latest:!excludeLatest,unit:'reported chart volume units; economic unit unverified'};
 }
 function extrema(rows,kind){
  const points=[];
  for(let i=5;i<rows.length-5;i++){
   const px=rows[i][kind];let valid=true;
   for(let j=i-5;j<=i+5;j++)if(j!==i){
    const other=rows[j][kind];
    if((kind==='high'?other>px:other<px)||(other===px&&j>i)){valid=false;break;}
   }
   if(valid)points.push({index:i,price:px,volume:volume(rows[i].volume)?rows[i].volume:null,
    at:label(rows[i].time),identifiable_after:label(rows[i+5].time)});
  }
  return points;
 }
 function candidates(rows){
  const out=[],last=rows.at(-1);if(rows.length<40)return out;
  for(const kind of ['high','low']){
   const points=extrema(rows,kind);
   for(let a=0;a<points.length;a++)for(let b=a+1;b<points.length;b++){
    const left=points[a],right=points[b],distance=right.index-left.index;
    if(distance>90)break;
    if(distance<8||Math.abs(right.price-left.price)/left.price>0.03)continue;
    let neck=kind==='high'?Infinity:-Infinity;
    for(let k=left.index;k<=right.index;k++)neck=kind==='high'?Math.min(neck,rows[k].low):Math.max(neck,rows[k].high);
    out.push({id:kind+':'+left.at+':'+right.at,kind:kind==='high'?'paired_highs':'paired_lows',left,right,
     distance_observations:distance,neckline:neck,comparison_at:label(last.time),comparison_close:last.close,
     latest_close_relation:last.close>neck?'above':last.close<neck?'below':'equal',
     second_extremum_quieter:volume(left.volume)&&volume(right.volume)?right.volume<left.volume:null,
     observations_since_second:rows.length-1-right.index,forecast_qualified:false});
   }
  }
  return out.sort((a,b)=>b.right.index-a.right.index||b.left.index-a.left.index||a.kind.localeCompare(b.kind));
 }
 function calculate(frame,now=Date.now()){
  const rows=frame?.bars,base={contract:'chart-stock-desk-research.v1',symbol:frame?.symbol??null,interval:frame?.interval??null,
   source:frame?.source??null,frame_published_at:frame?.published_at??null,input_rows:rows,
   bar_count:Array.isArray(rows)?rows.length:0,valid:false,errors:[],patterns:[],
   calls_eligible:false,sizing_eligible:false,forecast_qualified:false,source_replayed:false,
   point_in_time_qualified:false,volume_unit_verified:false,corporate_action_basis_verified:false};
  if(!Array.isArray(rows)||!rows.length){base.errors.push('No complete chart frame');return base;}
  if(!finite(now))base.errors.push('Invalid evaluation clock');
  if(typeof frame.symbol!=='string'||!frame.symbol||typeof frame.interval!=='string'||!frame.interval||typeof frame.source!=='string'||!frame.source||['unavailable','—'].includes(frame.source))
   base.errors.push('Chart symbol, interval or source is unavailable');
  let previous=-Infinity;
  for(let index=0;index<rows.length;index++){
   const bar=rows[index];
   const at=time(bar?.time);
   if(!Number.isFinite(at)||at<=previous||at>now){base.errors.push('Invalid, duplicate, unordered or future clock at row '+index);}
   previous=at;
   if(!bar||!['open','high','low','close'].every(k=>finite(bar[k])&&bar[k]>0)||bar.high<Math.max(bar.open,bar.close,bar.low)||bar.low>Math.min(bar.open,bar.close,bar.high))
    base.errors.push('Invalid OHLC prices at row '+index);
  }
  if(base.errors.length)return base;
  base.valid=true;base.first_observation=label(rows[0].time);base.last_observation=label(rows.at(-1).time);
  base.missing_volume_rows=rows.filter(b=>!volume(b.volume)).length;
  base.relative_volume=volumeRatio(rows,true);base.legacy_inclusive_relative_volume=volumeRatio(rows,false);
  const current=width(rows,rows.length-1),history=[];
  for(let i=19;i<rows.length;i++){
   const measurement=width(rows,i);if(!measurement){base.valid=false;base.errors.push('Price arithmetic overflow');return base;}
   history.push(measurement.ratio);
  }
  base.bollinger=current?{...current,history_windows:history.length,
   percentile:(history.filter(x=>x<current.ratio).length+0.5*history.filter(x=>x===current.ratio).length)/history.length,
   percentile_basis:'empirical midrank among all complete 20-observation windows, including the latest'}:null;
  base.patterns=candidates(rows);
  base.methodology={extremum_left:5,extremum_right:5,min_separation:8,max_separation:90,price_tolerance:0.03,
   comparison:'latest chart close versus each historical pair neckline; not a breakout-time test',
   sample:'complete retained chart frame; overlapping pairs are not independent evidence',
   hindsight:'each extremum requires five later observed bars; no first-publication or forecast claim'};
  return base;
 }
 function bind(frame,bars,selected){
  return !!frame&&frame.bars===bars&&typeof selected==='string'&&selected.trim().toUpperCase()===String(frame.symbol||'').trim().toUpperCase();
 }
 return {time,label,width,volumeRatio,extrema,candidates,calculate,bind};
});
