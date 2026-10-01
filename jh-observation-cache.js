/* Bounded transport cache for existing observation packets. A recent download
 * does not establish source freshness, provenance, equivalence or trade authority. */
(function(root,factory){
  if(typeof module==='object'&&module.exports)module.exports=factory(globalThis);
  else root.JHObservationCache=factory(root);
})(typeof window==='object'?window:globalThis,function(root){
  'use strict';
  var CONTRACT='observation-cache.v1',OK_MS=300000,RETRY_MS=30000,DEADLINE_MS=10000,sharedInstance=null;
  function object(v){return v!==null&&typeof v==='object'&&!Array.isArray(v);}
  var BUILTIN_SOURCES={
    series:{paths:['/data/cryptoquant-series.json'],valid:function(v){return object(v)&&object(v.series);}},
    onchain:{paths:['/data/cryptoquant-onchain.json'],valid:function(v){return object(v)&&object(v.metrics);}},
    feed:{paths:['/data/cq-feed.json'],valid:function(v){return object(v)&&object(v.metrics);}},
    catalog:{paths:['/data/cq-catalog.json'],valid:function(v){return object(v)&&object(v.catalog);}},
    spec:{paths:['/data/config/cryptoquant-spec.json'],valid:function(v){return object(v)&&Array.isArray(v.metrics);}},
    universe:{paths:['/cq-universe.json','/assets/cq-universe.json'],valid:function(v){return object(v)&&Array.isArray(v.rows);}},
    ciss:{paths:['/data/ciss-stress.json'],valid:function(v){return object(v)&&Array.isArray(v.series);}}
  };
  function create(options){
    options=options||{};
    var definitions=BUILTIN_SOURCES;
    if(options.sources!==undefined){
      if(!object(options.sources)||!Object.keys(options.sources).length)throw new Error('Invalid observation source definitions');
      definitions=Object.create(null);
      Object.keys(options.sources).forEach(function(id){
        var def=options.sources[id];
        if(!object(def)||!Array.isArray(def.paths)||!def.paths.length||Array.from(def.paths).some(function(p){return typeof p!=='string'||!p.length;})||typeof def.valid!=='function')throw new Error('Invalid observation source definition');
        definitions[id]={paths:def.paths.slice(),valid:def.valid};
      });
    }
    var states=Object.create(null),now=options.now||function(){return Date.now();};
    var mono=options.monotonic||function(){return root.performance&&typeof root.performance.now==='function'?root.performance.now():now();};
    var later=options.setTimeout||function(fn,ms){return root.setTimeout(fn,ms);};
    var cancel=options.clearTimeout||function(id){root.clearTimeout(id);};
    function state(id){
      if(!Object.prototype.hasOwnProperty.call(definitions,id))throw new Error('Unknown observation source');
      return states[id]||(states[id]={generation:0,packet:null,received:null,acceptedPath:null,attempt:null,finished:null,error:null,pending:null,attempts:[]});
    }
    function clock(){return {utc:now(),mono:mono()};}
    function age(stamp){if(!stamp)return Infinity;var a=now()-stamp.utc,b=mono()-stamp.mono;return Number.isFinite(a)&&Number.isFinite(b)&&a>=0&&b>=0?Math.max(a,b):Infinity;}
    function iso(stamp){return stamp&&Number.isFinite(stamp.utc)?new Date(stamp.utc).toISOString():null;}
    function status(id){
      var s=state(id),elapsed=age(s.finished),ttl=s.error?RETRY_MS:OK_MS;
      return {contract:CONTRACT,source_id:id,packet_path:definitions[id].paths[0],accepted_path:s.acceptedPath,
        state:s.pending?'refreshing':s.error?(s.packet?'cached_after_failure':'unavailable'):s.packet?(elapsed<OK_MS?'checked_within_interval':'overdue'):'unavailable',
        has_packet:s.packet!==null,received_at:iso(s.received),last_successful_check_at:iso(s.received),attempted_at:iso(s.attempt),checked_at:!s.error?iso(s.finished):null,
        retry_after_s:Number.isFinite(elapsed)?Math.max(0,Math.ceil((ttl-elapsed)/1000)):0,
        last_error:s.error?Object.assign({},s.error):null,
        attempts:s.attempts.map(function(a){return Object.assign({},a);}),
        source_freshness_verified:false,source_equivalence_verified:false,calls_eligible:false,sizing_eligible:false};
    }
    function result(id){return {packet:state(id).packet,cache:status(id)};}
    function read(id,fetchFn){
      var s=state(id),def=definitions[id];
      if(s.pending)return s.pending.promise;
      if(age(s.finished)<(s.error?RETRY_MS:OK_MS))return Promise.resolve(result(id));
      var generation=++s.generation,controller=typeof root.AbortController==='function'?new root.AbortController():null;
      var settle,timer=null,done=false,attempts=[];
      var promise=new Promise(function(resolve){settle=resolve;});
      s.attempt=clock();s.pending={promise:promise,stop:function(){finish({kind:'superseded'},null,null,true);}};
      function finish(error,packet,path,superseded){
        if(done)return;
        // Synchronous JSON work can delay the timer's callback. Acceptance itself
        // must enforce the elapsed deadline, independently of callback ordering.
        if(!superseded&&age(s.attempt)>=DEADLINE_MS)error={kind:'timeout'};
        done=true;if(timer!==null)cancel(timer);
        if(error&&controller)controller.abort();
        if(superseded||s.generation!==generation){settle({packet:null,cache:{contract:CONTRACT,source_id:id,state:'superseded',source_freshness_verified:false,calls_eligible:false,sizing_eligible:false}});return;}
        if(error&&attempts.length&&['pending','accepted'].indexOf(attempts[attempts.length-1].status)>=0){
          attempts[attempts.length-1].status='rejected';attempts[attempts.length-1].error=Object.assign({},error);
          if(packet!==null)attempts[attempts.length-1].rejected_packet=packet;
        }
        s.pending=null;s.finished=clock();s.error=error;s.attempts=attempts;
        if(!error){s.packet=packet;s.received=s.finished;s.acceptedPath=path;}
        settle(result(id));
      }
      timer=later(function(){finish({kind:'timeout'},null,null,false);},DEADLINE_MS);
      fetchFn=fetchFn||options.fetch||root.fetch;
      function attempt(index){
        if(done)return;
        if(age(s.attempt)>=DEADLINE_MS){finish({kind:'timeout'},null,null,false);return;}
        var path=def.paths[index],entry={path:path,status:'pending'};attempts.push(entry);
        Promise.resolve().then(function(){
          if(done)return null;
          return fetchFn(path,{cache:'no-store',signal:controller?controller.signal:undefined});
        }).then(function(response){
          if(done)return null;
          if(age(s.attempt)>=DEADLINE_MS){finish({kind:'timeout'},null,null,false);return null;}
          if(!response||!response.ok){var error={kind:'http_error',http_status:response&&typeof response.status==='number'?response.status:null};throw error;}
          return Promise.resolve().then(function(){return response.json();}).catch(function(){throw {kind:'decode_error'};});
        }).then(function(packet){
          if(done)return;
          if(!def.valid(packet)){entry.rejected_packet=packet;throw {kind:'schema_error'};}
          entry.status='accepted';finish(null,packet,path,false);
        }).catch(function(error){
          if(done)return;
          var known=error&&['http_error','decode_error','schema_error'].indexOf(error.kind)>=0;
          var failure=known?error:{kind:'request_error'};
          entry.status='rejected';entry.error=Object.assign({},failure);
          if(index+1<def.paths.length)attempt(index+1);else finish(failure,null,null,false);
        });
      }
      attempt(0);return promise;
    }
    function reset(ids){
      (ids||Object.keys(states)).forEach(function(id){var s=state(id);s.generation++;if(s.pending)s.pending.stop();delete states[id];});
    }
    return {read:read,status:status,reset:reset};
  }
  function label(state){var text={checked_within_interval:'Download checked within five minutes',cached_after_failure:'Using previous packet after download failure',unavailable:'Unavailable',refreshing:'Checking download',overdue:'Download check overdue',superseded:'Request replaced'}[state];return typeof text==='string'?text:'Unavailable';}
  return {contract:CONTRACT,create:create,label:label,shared:function(){return sharedInstance||(sharedInstance=create());},
    policy:{success_recheck_ms:OK_MS,failure_retry_ms:RETRY_MS,request_deadline_ms:DEADLINE_MS}};
});
