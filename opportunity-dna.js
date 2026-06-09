/* Opportunity DNA — a visual "fingerprint" of WHAT KIND of setup a name is.
   Maps the signal_keys / lenses each engine produces onto 6 archetype dimensions,
   so you can read a setup's character at a glance (not just its conviction).
   Pure function of existing data — no new backend. Used by board + dossier. */
(function(global){
  // signal_key -> {dimension: weight}. Dimensions:
  //   VALUE (cheap), QUALITY (durable compounder), DEMAND (backlog/orders inflecting),
  //   FLOW (institutional/options/squeeze pressure), INSIDER (insider/political conviction),
  //   CATALYST (event-driven / near-term trigger)
  const MAP = {
    DEEP_VALUE_OVERLAP:{VALUE:1.0,QUALITY:0.3},
    DISLOCATION:{VALUE:0.9},
    COMPOUNDER:{QUALITY:1.0},
    BUYBACK:{QUALITY:0.5,FLOW:0.4},
    CAPEX_ACCEL:{DEMAND:0.8,QUALITY:0.3},
    REVISION_UP:{DEMAND:0.7,CATALYST:0.4},
    EARNINGS_FRESH:{DEMAND:0.5,CATALYST:0.6},
    CAPITAL_FLOW:{FLOW:1.0},
    OPTIONS_EXTREME:{FLOW:0.9},
    OPTIONS_BULLISH:{FLOW:0.6},
    SHORT_SQUEEZE:{FLOW:0.8,CATALYST:0.3},
    INSIDER_CLUSTER:{INSIDER:1.0},
    EXECUTIVE_BUY:{INSIDER:0.8},
    POLITICIAN_COMMITTEE:{INSIDER:0.9,CATALYST:0.3},
    POLITICIAN_BUY:{INSIDER:0.6},
    FDA_CATALYST:{CATALYST:1.0},
    GOV_CONTRACT:{CATALYST:0.7,DEMAND:0.4},
    CASCADE_ALERT:{CATALYST:0.5,FLOW:0.3},
    CASCADE_LAGGARD:{VALUE:0.4,FLOW:0.3},
    CONVERGENCE:{QUALITY:0.2,VALUE:0.2,FLOW:0.2},
    EARLY_MOVER:{CATALYST:0.5,FLOW:0.3},
    OPTIONS:{FLOW:0.6},
    RETAIL_HOT:{FLOW:0.3,CATALYST:0.2},
    RETAIL_VELOCITY:{FLOW:0.2},
  };
  const DIMS=["VALUE","QUALITY","DEMAND","FLOW","INSIDER","CATALYST"];
  const DIM_LABEL={VALUE:"Cheap",QUALITY:"Quality",DEMAND:"Demand",FLOW:"Flow",INSIDER:"Insider",CATALYST:"Catalyst"};
  const DIM_COLOR={VALUE:"#26ffaf",QUALITY:"#a78bfa",DEMAND:"#22d3ee",FLOW:"#fb923c",INSIDER:"#fbbf24",CATALYST:"#ff5577"};

  // Build dimension scores 0..1 from a setup's signals (with strengths if available)
  function score(setup){
    const dim={VALUE:0,QUALITY:0,DEMAND:0,FLOW:0,INSIDER:0,CATALYST:0};
    const sigs=(setup&&setup.signals)||[];
    const keys=(setup&&setup.signal_keys)||sigs.map(s=>s.key)||[];
    // prefer per-signal strength when present
    if(sigs.length){
      sigs.forEach(s=>{const m=MAP[s.key];if(m){const str=s.strength||0.6;for(const d in m)dim[d]+=m[d]*str;}});
    } else {
      keys.forEach(k=>{const m=MAP[k];if(m){for(const d in m)dim[d]+=m[d]*0.7;}});
    }
    // also fold in lenses if present
    (setup&&setup.value_lenses||[]).forEach(()=>dim.VALUE+=0.15);
    (setup&&setup.flow_lenses||[]).forEach(()=>dim.FLOW+=0.15);
    // normalize each dim to 0..1 (cap)
    DIMS.forEach(d=>{dim[d]=Math.max(0,Math.min(1,dim[d]));});
    return dim;
  }

  // Name the archetype from the top dimensions
  function archetype(dim){
    const sorted=DIMS.slice().sort((a,b)=>dim[b]-dim[a]);
    const a=sorted[0],b=sorted[1];
    if(dim[a]<0.15)return"Emerging / mixed";
    const combos={
      "VALUE+QUALITY":"Cheap Compounder","QUALITY+VALUE":"Cheap Compounder",
      "VALUE+FLOW":"Forced-Flow Value","FLOW+VALUE":"Forced-Flow Value",
      "DEMAND+QUALITY":"Backlog Inflection","QUALITY+DEMAND":"Quality + Demand",
      "DEMAND+VALUE":"Backlog Inflection","VALUE+DEMAND":"Backlog Inflection",
      "FLOW+CATALYST":"Squeeze / Event","CATALYST+FLOW":"Event-Driven Flow",
      "INSIDER+VALUE":"Insider Conviction","INSIDER+QUALITY":"Insider Conviction",
      "FLOW+QUALITY":"Institutional Accumulation","QUALITY+FLOW":"Institutional Accumulation",
      "CATALYST+DEMAND":"Catalyst + Demand","DEMAND+CATALYST":"Catalyst + Demand",
    };
    return combos[a+"+"+b] || (DIM_LABEL[a]+" + "+DIM_LABEL[b]);
  }

  // Render a compact inline SVG fingerprint (radar). size px.
  function svg(dim,size){
    size=size||96; const cx=size/2,cy=size/2,r=size/2-10;
    const n=DIMS.length; let pts=[],axes=[];
    DIMS.forEach((d,i)=>{
      const ang=(Math.PI*2*i/n)-Math.PI/2;
      const val=Math.max(0.04,dim[d]);
      pts.push((cx+Math.cos(ang)*r*val).toFixed(1)+","+(cy+Math.sin(ang)*r*val).toFixed(1));
      axes.push(`<line x1="${cx}" y1="${cy}" x2="${(cx+Math.cos(ang)*r).toFixed(1)}" y2="${(cy+Math.sin(ang)*r).toFixed(1)}" stroke="#1c2433" stroke-width="1"/>`);
    });
    // dominant color = top dim
    const top=DIMS.slice().sort((a,b)=>dim[b]-dim[a])[0];
    const col=DIM_COLOR[top]||"#22d3ee";
    return `<svg viewBox="0 0 ${size} ${size}" width="${size}" height="${size}" xmlns="http://www.w3.org/2000/svg">
      <circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="#1c2433" stroke-width="1"/>
      <circle cx="${cx}" cy="${cy}" r="${r*0.66}" fill="none" stroke="#141a26" stroke-width="1"/>
      <circle cx="${cx}" cy="${cy}" r="${r*0.33}" fill="none" stroke="#141a26" stroke-width="1"/>
      ${axes.join('')}
      <polygon points="${pts.join(' ')}" fill="${col}33" stroke="${col}" stroke-width="1.5"/>
    </svg>`;
  }

  // Full labeled legend bars (for dossier)
  function bars(dim){
    return '<div style="display:flex;flex-direction:column;gap:5px">'+DIMS.map(d=>{
      const v=Math.round(dim[d]*100);
      return `<div style="display:flex;align-items:center;gap:8px;font-family:ui-monospace,monospace;font-size:10px">
        <span style="width:58px;color:#a8b3c7">${DIM_LABEL[d]}</span>
        <span style="flex:1;height:6px;background:#0f1420;border-radius:3px;overflow:hidden"><span style="display:block;height:100%;width:${v}%;background:${DIM_COLOR[d]}"></span></span>
        <span style="width:28px;text-align:right;color:#6f7b91">${v}</span></div>`;
    }).join('')+'</div>';
  }

  global.OpportunityDNA={score,archetype,svg,bars,DIMS,DIM_LABEL,DIM_COLOR};
})(window);
