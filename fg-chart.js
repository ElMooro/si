/* FG_CHART_OPS3477 + OPS3478 + OPS3481 + OPS3482 + OPS3486 (FGChart.ratio pairs) (px own-scale, no axis-steal; click-to-place vertical markers) — shared Fundamental Graphs chart core.
   Single source for the flagship (/fundamental-graphs.html) and the why.html
   embedded module. Extracted from flagship v1.3 draw(); behavior-identical.

   API:
     FGChart.fmt(v, u, mode)          -> display string ('$','%','x','d','c','s','n')
     FGChart.grp(u)                   -> axis unit-group id
     FGChart.render(svgEl, tipEl, drawn, opts) -> {autoPct, hiddenGroups}
       drawn: [{sym,label,u,color,dash,pts:[[date,val]..],grp,isPx,isEst}]
       opts:  {mode:'val'|'pct'|'yoy', log:bool, mixOk:bool}
     Caller policy: if r.autoPct -> switch own mode to 'pct', set mixOk, redraw.
*/
(function(){
'use strict';
var UGROUP={'$':'$','s':'$','c':'$/sh','%':'%','x':'x','d':'days','n':'x','px':'px'};
function fmt(v,u,mode){
  if(v==null||!isFinite(v))return '\u2014';
  if(mode&&mode!=='val')return (v>=0?'+':'')+v.toFixed(1)+'%';
  var a=Math.abs(v);
  if(u==='$'||u==='s'){
    if(a>=1e12)return (v/1e12).toFixed(2)+'T';
    if(a>=1e9)return (v/1e9).toFixed(2)+'B';
    if(a>=1e6)return (v/1e6).toFixed(1)+'M';
    if(a>=1e3)return (v/1e3).toFixed(1)+'K';
    return v.toFixed(2);
  }
  if(u==='%')return v.toFixed(2)+'%';
  if(u==='d')return v.toFixed(0)+'d';
  if(u==='c')return '$'+v.toFixed(2);
  if(u==='n')return v.toFixed(2);
  return v.toFixed(2)+'x';
}
function niceTicks(lo,hi,n){
  if(!(hi>lo)){hi=lo+1;lo=lo-1;}
  var span=hi-lo,step0=span/Math.max(2,n),mag=Math.pow(10,Math.floor(Math.log10(step0)));
  var norm=step0/mag;var step=(norm<1.5?1:norm<3.5?2:norm<7.5?5:10)*mag;
  var t=[];for(var v=Math.ceil(lo/step)*step;v<=hi+1e-9;v+=step)t.push(v);
  return t;
}
var esc=function(s){return String(s==null?'':s).replace(/[<>&]/g,function(c){return {'<':'&lt;','>':'&gt;','&':'&amp;'}[c];});};
function render(svg,tip,list,opts){
  opts=opts||{};var mode=opts.mode||'val',LOG=!!opts.log&&mode==='val';
  svg.innerHTML='';
  if(!list.length)return {autoPct:false,hiddenGroups:0,empty:true};

  var groups=[];list.forEach(function(s){if(!s.isPx&&groups.indexOf(s.grp)<0)groups.push(s.grp);});
  var hidden=0;
  if(mode!=='val'){groups=['%chg'];}
  if(mode==='val'&&groups.length>2){
    if(!opts.mixOk)return {autoPct:true,hiddenGroups:groups.length-2};
    hidden=groups.length-2;groups=groups.slice(0,2);
  }
  var axisOf=function(s){return s.isPx?2:(mode!=='val'?0:groups.indexOf(s.grp));};
  var drawn=list.filter(function(s){var a=axisOf(s);return a===0||a===1||a===2;});

  var W=svg.clientWidth||(svg.parentNode&&svg.parentNode.clientWidth)||1000,
      H=+svg.getAttribute('height')||470;
  var showPx=drawn.some(function(s){return s.isPx;});
  var padL=64,padR=(groups.length>1||showPx)?110:96,padT=16,padB=30;
  var x0=padL,x1=W-padR,y0=padT,y1=H-padB;

  var dmin='9999',dmax='0000';
  drawn.forEach(function(s){s.pts.forEach(function(p){if(p[0]<dmin)dmin=p[0];if(p[0]>dmax)dmax=p[0];});});
  var tmin=+new Date(dmin),tmax=(+new Date(dmax))||tmin+1;
  var X=function(d){return x0+(x1-x0)*((+new Date(d))-tmin)/Math.max(1,(tmax-tmin));};

  var T=function(v){return LOG?(v>0?Math.log10(v):null):v;};
  function dom(filter){
    var lo=Infinity,hi=-Infinity;
    drawn.filter(filter).forEach(function(s){s.pts.forEach(function(p){var t=T(p[1]);
      if(t==null)return;if(t<lo)lo=t;if(t>hi)hi=t;});});
    if(!isFinite(lo)){lo=0;hi=1;}
    if(lo===hi){lo-=Math.abs(lo)*.1||1;hi+=Math.abs(hi)*.1||1;}
    var pad=(hi-lo)*.07;return [lo-pad,hi+pad];
  }
  var d0=dom(function(s){return axisOf(s)===0;}),l0=d0[0],h0=d0[1];
  var need2=groups.length>1;
  var d1=need2?dom(function(s){return axisOf(s)===1;}):[0,1],l1=d1[0],h1=d1[1];
  var dp=showPx?dom(function(s){return s.isPx;}):[0,1],lp=dp[0],hp=dp[1];
  var Y0=function(v){var t=T(v);return t==null?null:y1-(y1-y0)*(t-l0)/(h0-l0);};
  var Y1=function(v){var t=T(v);return t==null?null:y1-(y1-y0)*(t-l1)/(h1-l1);};
  var Ypx=function(v){var t=T(v);return t==null?null:y1-(y1-y0)*(t-lp)/(hp-lp);};

  var NS='http://www.w3.org/2000/svg';
  var el=function(t,a){var e=svg.ownerDocument.createElementNS(NS,t);for(var k in a)e.setAttribute(k,a[k]);return e;};

  var fmtAxis=function(v,grp){
    if(mode!=='val')return (v>=0?'+':'')+v.toFixed(0)+'%';
    if(grp==='$'){var a=Math.abs(v);
      if(a>=1e12)return (v/1e12).toFixed(1)+'T';if(a>=1e9)return (v/1e9).toFixed(1)+'B';
      if(a>=1e6)return (v/1e6).toFixed(0)+'M';return v.toFixed(a<10?2:0);}
    if(grp==='%')return v.toFixed(0)+'%';
    if(grp==='days')return v.toFixed(0)+'d';
    if(grp==='px'||grp==='$/sh')return '$'+v.toFixed(v<10?2:0);
    return v.toFixed(1);
  };
  var untf=function(v){return LOG?Math.pow(10,v):v;};
  niceTicks(l0,h0,6).forEach(function(v){
    var y=y1-(y1-y0)*(v-l0)/(h0-l0);
    svg.appendChild(el('line',{x1:x0,x2:x1,y1:y,y2:y,stroke:'#111b2e','stroke-width':1}));
    var t=el('text',{x:x0-8,y:y+3,'text-anchor':'end',fill:'#64748b','font-size':'10.5'});
    t.textContent=fmtAxis(untf(v),mode!=='val'?'%chg':groups[0]);svg.appendChild(t);
  });
  if(mode==='val'&&need2){
    niceTicks(l1,h1,6).forEach(function(v){
      var t=el('text',{x:x1+8,y:(y1-(y1-y0)*(v-l1)/(h1-l1))+3,'text-anchor':'start',fill:'#64748b','font-size':'10.5'});
      t.textContent=fmtAxis(untf(v),groups[1]);svg.appendChild(t);
    });
  }else if(showPx){ /* price own-scale ticks only when the right rail is free */
    niceTicks(lp,hp,6).forEach(function(v){
      var t=el('text',{x:x1+8,y:(y1-(y1-y0)*(v-lp)/(hp-lp))+3,'text-anchor':'start',fill:'#8b98ad','font-size':'10',opacity:.85});
      t.textContent=fmtAxis(untf(v),mode!=='val'?'%chg':'px');svg.appendChild(t);
    });
  }
  if(mode!=='val'&&l0<0&&h0>0){
    var yz=y1-(y1-y0)*(0-l0)/(h0-l0);
    svg.appendChild(el('line',{x1:x0,x2:x1,y1:yz,y2:yz,stroke:'#334155','stroke-width':1.2}));
  }
  /* NBER recession backdrop (append future ranges here) */
  var NBER=[['2007-12-01','2009-06-30'],['2020-02-01','2020-04-30']];
  NBER.forEach(function(bR){
    var a=Math.max(tmin,+new Date(bR[0])),b2=Math.min(tmax,+new Date(bR[1]));
    if(b2<=a)return;
    var xa=x0+(x1-x0)*(a-tmin)/Math.max(1,tmax-tmin),xb=x0+(x1-x0)*(b2-tmin)/Math.max(1,tmax-tmin);
    svg.appendChild(el('rect',{x:xa,y:y0,width:Math.max(1,xb-xa),height:y1-y0,fill:'#94a3b8',opacity:.05}));
    var nt=el('text',{x:xa+3,y:y0+11,fill:'#94a3b8','font-size':'8.5',opacity:.55});nt.textContent='NBER';svg.appendChild(nt);
  });
  var yStart=+dmin.slice(0,4),yEnd=+dmax.slice(0,4);
  for(var yy=yStart;yy<=yEnd;yy++){
    var dd=yy+'-01-01';if(+new Date(dd)<tmin||+new Date(dd)>tmax)continue;
    var xx=X(dd);
    svg.appendChild(el('line',{x1:xx,x2:xx,y1:y0,y2:y1,stroke:'#111b2e','stroke-width':1}));
    var t2=el('text',{x:xx,y:y1+16,'text-anchor':'middle',fill:'#64748b','font-size':'10.5'});t2.textContent=yy;svg.appendChild(t2);
  }
  /* own-history percentile band (opts.band = {axis:0|1, lo, hi, mid?, color?}) — Values mode */
  if(opts.band&&mode==='val'){
    var B=opts.band,YB=B.axis===1?Y1:Y0,byl=YB(B.lo),byh=YB(B.hi);
    if(byl!=null&&byh!=null){
      svg.appendChild(el('rect',{x:x0,y:Math.min(byl,byh),width:x1-x0,height:Math.abs(byl-byh),
        fill:B.color||'#22d3ee',opacity:.07}));
      if(B.mid!=null){var bym=YB(B.mid);if(bym!=null)
        svg.appendChild(el('line',{x1:x0,x2:x1,y1:bym,y2:bym,stroke:B.color||'#22d3ee','stroke-dasharray':'5 5',opacity:.35}));}
      var bt=el('text',{x:x1-4,y:Math.min(byl,byh)+11,'text-anchor':'end',fill:B.color||'#22d3ee','font-size':'9.5',opacity:.7});
      bt.textContent='10y p10\u2013p90';svg.appendChild(bt);
    }
  }
  /* earnings layer (opts.earnings = [[date, epsActual, epsEstimated],...]) */
  if(opts.earnings&&opts.earnings.length){
    opts.earnings.forEach(function(e2){
      var t2=+new Date(e2[0]);if(t2<tmin||t2>tmax)return;
      var xe=X(e2[0]),act=e2[1],est2=e2[2];
      var cc=(act==null||est2==null)?'#64748b':(act>est2?'#34d399':(act<est2?'#f87171':'#fbbf24'));
      svg.appendChild(el('line',{x1:xe,x2:xe,y1:y1-7,y2:y1,stroke:cc,'stroke-width':1.4,opacity:.9}));
      var dot=el('circle',{cx:xe,cy:y1-11,r:3,fill:cc,opacity:.95});
      var tt=svg.ownerDocument.createElementNS(NS,'title');
      tt.textContent=e2[0]+'  EPS '+(act==null?'\u2014':act)+' vs est '+(est2==null?'\u2014':est2)+((act!=null&&est2!=null)?(act>=est2?'  BEAT':'  MISS'):'');
      dot.appendChild(tt);svg.appendChild(dot);
    });
  }
  var todayISO=new Date().toISOString().slice(0,10);
  if(+new Date(todayISO)>tmin&&+new Date(todayISO)<tmax){
    var xt=X(todayISO);
    svg.appendChild(el('line',{x1:xt,x2:xt,y1:y0,y2:y1,stroke:'#fbbf24','stroke-dasharray':'2 4','stroke-width':1.1,opacity:.8}));
    var tl=el('text',{x:xt+4,y:y0+10,fill:'#fbbf24','font-size':'10'});tl.textContent='today';svg.appendChild(tl);
  }

  var badges=[];
  drawn.forEach(function(s){
    var Y=s.isPx?Ypx:(axisOf(s)===0?Y0:Y1);
    var dstr='',pen=false;
    s.pts.forEach(function(p){var y=Y(p[1]);
      if(y==null){pen=false;return;}
      dstr+=(pen?'L':'M')+X(p[0]).toFixed(1)+' '+y.toFixed(1);pen=true;});
    if(!dstr)return;
    svg.appendChild(el('path',{d:dstr,fill:'none',stroke:s.color,'stroke-width':s.isPx?1.4:2,
      'stroke-dasharray':s.dash||'',opacity:s.isPx?.85:(s.isEst?.9:1),'stroke-linejoin':'round'}));
    if(s.pts.length<=60&&!s.isPx)s.pts.forEach(function(p){var y=Y(p[1]);
      if(y!=null)svg.appendChild(el('circle',{cx:X(p[0]),cy:y,r:2.1,fill:s.color}));});
    var lp=null;for(var i=s.pts.length-1;i>=0;i--){if(Y(s.pts[i][1])!=null){lp=s.pts[i];break;}}
    if(lp)badges.push({y:Y(lp[1]),txt:fmt(lp[1],s.u,mode),color:s.color,isPx:s.isPx});
  });
  badges.sort(function(a,b){return a.y-b.y;});
  var lastY=-99;badges.forEach(function(b){
    var y=Math.max(y0+8,Math.min(y1-8,b.y));if(y-lastY<15)y=lastY+15;lastY=y;
    var w2=Math.max(40,b.txt.length*7+10);
    svg.appendChild(el('rect',{x:x1+4,y:y-9,width:w2,height:16,rx:4,fill:b.color,opacity:b.isPx?.75:1}));
    var t3=el('text',{x:x1+4+w2/2,y:y+3.5,'text-anchor':'middle',fill:'#04121a','font-size':'10.5','font-weight':'800'});
    t3.textContent=b.txt;svg.appendChild(t3);
  });

  /* placed vertical markers (opts.marks: ISO dates; onUnmark(date) on handle) */
  var unionDates=[];drawn.forEach(function(s){s.pts.forEach(function(p){unionDates.push(p[0]);});});
  unionDates=Array.from(new Set(unionDates)).sort();
  (opts.marks||[]).forEach(function(md){
    var t2=+new Date(md);if(t2<tmin||t2>tmax)return;
    var xm=X(md);
    svg.appendChild(el('line',{x1:xm,x2:xm,y1:y0,y2:y1,stroke:'#a78bfa','stroke-width':1.4,opacity:.85}));
    var lb2=el('text',{x:xm+4,y:y1-6,fill:'#a78bfa','font-size':'9.5',opacity:.9});lb2.textContent=md;svg.appendChild(lb2);
    var hg=el('g',{style:'cursor:pointer'});
    hg.appendChild(el('circle',{cx:xm,cy:y0+7,r:7,fill:'#0a1120',stroke:'#a78bfa','stroke-width':1.2}));
    var xt=el('text',{x:xm,y:y0+10.5,'text-anchor':'middle',fill:'#a78bfa','font-size':'9','font-weight':'800'});xt.textContent='\u2715';hg.appendChild(xt);
    var tt2=svg.ownerDocument.createElementNS(NS,'title');tt2.textContent='remove marker '+md;hg.appendChild(tt2);
    hg.addEventListener('click',function(ev){ev.stopPropagation();if(opts.onUnmark)opts.onUnmark(md);});
    svg.appendChild(hg);
  });

  var cross=el('line',{x1:0,x2:0,y1:y0,y2:y1,stroke:'#475569','stroke-dasharray':'3 3',visibility:'hidden'});
  svg.appendChild(cross);
  var hit=el('rect',{x:x0,y:y0,width:Math.max(1,x1-x0),height:Math.max(1,y1-y0),fill:'transparent'});
  svg.appendChild(hit);
  function onMove(ev){
    if(!tip)return;
    var r=svg.getBoundingClientRect();
    var cx=(ev.touches?ev.touches[0].clientX:ev.clientX)-r.left;
    if(cx<x0||cx>x1){tip.style.display='none';cross.setAttribute('visibility','hidden');return;}
    cross.setAttribute('x1',cx);cross.setAttribute('x2',cx);cross.setAttribute('visibility','visible');
    var tcur=tmin+(tmax-tmin)*(cx-x0)/(x1-x0);
    var rows='',dshow='';
    drawn.forEach(function(s){
      var best=null,bd=Infinity,i;
      for(i=0;i<s.pts.length;i++){var ddf=Math.abs(+new Date(s.pts[i][0])-tcur);if(ddf<bd){bd=ddf;best=s.pts[i];}}
      if(!best)return;dshow=best[0];
      rows+='<div style="display:flex;justify-content:space-between;gap:14px;padding:1px 0"><span><span style="width:9px;height:9px;border-radius:3px;display:inline-block;background:'+s.color+'"></span> '+esc((s.sym?s.sym+'\u00b7':'')+(s.isPx?'Price':s.label))+(s.isEst?' (est)':'')+'</span><b style="font-family:monospace">'+fmt(best[1],s.u,mode)+'</b></div>';
    });
    tip.innerHTML='<div style="font-family:monospace;color:#64748b;margin-bottom:3px">'+dshow+'</div>'+rows;
    tip.style.display='block';
    var tw=tip.offsetWidth;
    tip.style.left=Math.min(cx+14,((svg.clientWidth||W)-tw-8))+'px';
    tip.style.top=(y0+8)+'px';
  }
  if(opts.onMark){
    var tt3=svg.ownerDocument.createElementNS(NS,'title');
    tt3.textContent='click: drop a vertical marker at this date';hit.appendChild(tt3);
    hit.addEventListener('click',function(ev){
      var r2=svg.getBoundingClientRect();
      var cx2=(ev.touches?ev.touches[0].clientX:ev.clientX)-r2.left;
      if(cx2<x0||cx2>x1||!unionDates.length)return;
      var tcur2=tmin+(tmax-tmin)*(cx2-x0)/(x1-x0);
      var best=unionDates[0],bd=Infinity;
      unionDates.forEach(function(d2){var df=Math.abs(+new Date(d2)-tcur2);if(df<bd){bd=df;best=d2;}});
      opts.onMark(best);
    });
  }
  hit.addEventListener('mousemove',onMove);
  hit.addEventListener('touchmove',function(e){onMove(e);e.preventDefault();},{passive:false});
  hit.addEventListener('mouseleave',function(){if(tip)tip.style.display='none';cross.setAttribute('visibility','hidden');});
  return {autoPct:false,hiddenGroups:hidden};
}
function ratio(a,b,maxGapDays){
  maxGapDays=maxGapDays||140;
  if(!a||!b||!a.length||!b.length)return [];
  var out=[],j=0;
  for(var i=0;i<a.length;i++){
    var d=a[i][0],va=a[i][1];
    if(va==null)continue;
    while(j+1<b.length&&b[j+1][0]<=d)j++;
    var db=b[j][0],vb=b[j][1];
    if(db>d||vb==null)continue;
    if((+new Date(d)-(+new Date(db)))/86400000>maxGapDays)continue;
    if(Math.abs(vb)<1e-12)continue;
    out.push([d,va/vb]);
  }
  return out;
}
window.FGChart={render:render,fmt:fmt,grp:function(u){return UGROUP[u]||'x';},niceTicks:niceTicks,ratio:ratio};
})();
