(function(root){
  'use strict';
  const CONTRACT='domain-rule-monitor.v1';
  const finite=v=>typeof v==='number'&&Number.isFinite(v);
  function describe(packet,now=Date.now()){
    const stamp=packet?.generated_at,at=typeof stamp==='string'&&/(Z|[+-]\d{2}:\d{2})$/.test(stamp)?Date.parse(stamp):NaN;
    const age=(now-at)/3600000,current=Number.isFinite(age)&&age>=0&&age<=36;
    const reviewed=packet?.contract===CONTRACT;
    return {reviewed,collectionCurrent:current,renderRules:reviewed&&current,generatedAt:stamp||'Unavailable',
      title:!reviewed?'Historical calculation — unqualified':!current?'Publication late or clock unavailable':'Descriptive rule monitor — unvalidated',
      note:'Publication time is not observation time. Mixed periods, note-derived polarities and source dependence do not establish expected returns, independent votes or position sizes.'};
  }
  function comparison(row){
    if(row?.contract_version==='fred-native-level.v1'){
      return finite(row.value)&&finite(row.prev)&&finite(row.change)?{value:row.change,unit:row.change_unit==='percentage_points'?'percentage points':row.change_unit}:{value:null,unit:'Comparison unavailable'};
    }
    if(finite(row?.value)&&finite(row?.prev)){
      const value=row.value-row.prev;if(!finite(value))return {value:null,unit:'Unavailable'};
      return {value,unit:row.change_unit==='percentage_points'||row.unit==='% YoY'?'percentage points':typeof row.unit==='string'&&row.unit?row.unit:'source units unverified'};
    }
    return finite(row?.value)&&finite(row?.chg_pct)?{value:row.chg_pct,unit:'% (provider reported)'}:{value:null,unit:'Comparison unavailable'};
  }
  function paginate(rows,index,size=200){
    const pages=Math.max(1,Math.ceil(rows.length/size)),page=Math.max(0,Math.min(pages-1,Number.isInteger(index)?index:0));
    return {rows:rows.slice(page*size,(page+1)*size),page,pages,total:rows.length,first:rows.length?page*size+1:0,last:Math.min(rows.length,(page+1)*size)};
  }
  function evidence(row){
    const key=row?.replay?.key;
    return row?.contract_version==='fred-native-level.v1'&&typeof key==='string'&&/^data\/fred-levels\/runs\/[a-f0-9]{64}\.json$/.test(key)?'/'+key:null;
  }
  const api={CONTRACT,describe,comparison,paginate,evidence};
  if(typeof module==='object'&&module.exports)module.exports=api;else root.JHDomainMonitor=api;
})(typeof window==='object'?window:globalThis);
