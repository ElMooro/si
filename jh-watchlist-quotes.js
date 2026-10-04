/* jh-reskin-skip */
/* Qualified watchlist aggregate helpers; no renderer or storage writer. */
(function(root){
"use strict";
  function resolve(s, rules) {
    if(typeof s!=="string" || !s.trim())return {reason:"invalid instrument identity"};
    var t=s.trim().toUpperCase();
    if(!rules)return {reason:"instrument resolver unavailable"};
    if((rules.licensed_econ_skip||[]).indexOf(t)>=0)return {reason:"licensed economic series"};
    var exact=rules.exact&&rules.exact[t], prefix=t.split(":")[0];
    if(window.JHChartCatalog && window.JHChartCatalog.lookupSym && /^FRED:/.test(window.JHChartCatalog.lookupSym(t)||""))return {reason:"economic series identity requires the series endpoint"};
    if(exact)return {reason:"resolver maps to "+exact.id+"; daily equity endpoint cannot verify that instrument"};
    if(t.indexOf(":")>=0){
      var rule=rules.prefix&&rules.prefix[prefix];
      return {reason:rule?"qualified "+rule.engine+" instrument; endpoint cannot verify venue or series identity":"unresolved namespace "+prefix};
    }
    if(!window.jhWatchlistResolve)return {reason:"native instrument resolver unavailable"};
    if(window.jhWatchlistResolve){
      var native=window.jhWatchlistResolve(t);
      if(!native||native.engine!=="equity"||native.ticker!==t||native.yahoo!==t)return {reason:"existing resolver identifies an alias or non-equity instrument; endpoint identity unverified"};
    }
    if(/^[A-Z]{6}$/.test(t))return {reason:"ambiguous currency, metal or equity identity"};
    if(/^[A-Z]{1,3}[FGHJKMNQUVXZ][0-9]{1,2}$/.test(t))return {reason:"possible futures contract identity unverified"};
    var currencies=["USD","EUR","GBP","JPY","CHF","CAD","AUD","NZD","CNY","CNH","HKD","SEK","NOK","DKK","MXN","ZAR","SGD"];
    if(t.length===6&&currencies.indexOf(t.slice(0,3))>=0&&currencies.indexOf(t.slice(3))>=0)return {reason:"currency pair identity unverified"};
    if(!/^[A-Z][A-Z0-9.\-]{0,11}$/.test(t))return {reason:"unsupported instrument identity"};
    if(/(?:USDT|USDC|BUSD|-USD)$/.test(t)||/^(BTCUSD|ETHUSD)$/.test(t))return {reason:"crypto currency/provider identity unverified"};
    return {ticker:t};
  }
  function packet(j,ticker,now) {
    if(!j||typeof j!=="object"||j.ticker!==ticker)return {reason:"response instrument identity missing or conflicting"};
    if(j.span!=="day"||j.mult!==1)return {reason:"daily interval identity missing or conflicting"};
    if(typeof j.source!=="string"||!j.source.trim())return {reason:"response source unavailable"};
    if(!Array.isArray(j.bars)||!j.bars.length)return {reason:"no daily aggregates"};
    var prevTime=-Infinity;
    for(var i=0;i<j.bars.length;i++){
      var b=j.bars[i];
      if(!b||!Number.isSafeInteger(b.time)||b.time<=0||b.time<=prevTime||b.time*1000>now || b.time*1000>8640000000000000)return {reason:"invalid or unordered bar timestamps"};
      if(typeof b.close!=="number"||!Number.isFinite(b.close))return {reason:"invalid aggregate close"};
      prevTime=b.time;
    }
    var last=j.bars[j.bars.length-1], prev=j.bars[j.bars.length-2];
    var delta=prev?last.close-prev.close:null, change=prev&&prev.close!==0?delta/prev.close:null;
    if(delta!==null&&!Number.isFinite(delta)||change!==null&&(!Number.isFinite(change)||!Number.isFinite(change*100)))return {reason:"aggregate change overflow"};
    return {last:last.close,chgv:change===null?null:delta,chg:change,time:last.time,
      previousTime:prev?prev.time:null,date:new Date(last.time*1000).toISOString().slice(0,10),
      source:j.source,fetched:now,completion:"unverified",reason:prev?null:"previous aggregate unavailable"};
  }
  function request(url,fetcher,timeout){
    var ctrl=new AbortController(),timer;
    var deadline=new Promise(function(_,reject){timer=setTimeout(function(){ctrl.abort();reject(Error("request timeout"));},timeout||10000);});
    return Promise.race([Promise.resolve().then(function(){return fetcher(url,{signal:ctrl.signal});}).then(function(r){if(!r.ok)throw Error("HTTP "+r.status);return r.json();}),deadline]).finally(function(){clearTimeout(timer);});
  }
  root.JHWatchlistQuotes={resolve:resolve,packet:packet,request:request};
})(typeof window!=="undefined"?window:globalThis);
