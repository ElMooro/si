#!/usr/bin/env python3
"""Add the CDS workspace tab beside Bonds, and spell out why each active name is unpriced.

Does not touch the CDS packet, the engine, or any other desk.
"""
import hashlib
from pathlib import Path

ENGINE = Path("jh-chart-engine.js")
RAIL = Path("jh-chart-tvrail.js")
HTML = Path("chart.html")
DESK = Path("jh-cds-desk.js")
PEEL = Path("tests/helpers/chart-macro-line-preservation.cjs")

BEFORE = {
    "jh-chart-engine.js": "245e9b8d541a5c7de5d96bd3990d07ecfc6735c6f41f2604b455a0ff3585c993",
    "jh-chart-tvrail.js": "33c541f77bf942b178814ea115bb51aa65407e89dfe4d5daa3dd3cee5aabd1c3",
    "chart.html": "edc3fb88fa09756baeccf417bb3afbad86dbd83f8aa1a38beac3c5c6cb7caa7a",
}

BTN_OLD = '''"<button class='wsico wsdesk' id=btn-bnd title='Bonds & Yields'><span class=g>🏛</span><span class=l>Bonds</span></button>"+
'''
BTN_NEW = '''"<button class='wsico wsdesk' id=btn-bnd title='Bonds & Yields'><span class=g>🏛</span><span class=l>Bonds</span></button>"+
      "<button class='wsico wsdesk' id=btn-cds title='Credit default swaps'><span class=g>CDS</span><span class=l>CDS</span></button>"+
'''
CLICK_OLD = '''var bbnd=document.getElementById("btn-bnd"); if(bbnd) bbnd.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace("bonds"); };
'''
CLICK_NEW = '''var bbnd=document.getElementById("btn-bnd"); if(bbnd) bbnd.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace("bonds"); };
    var bcds=document.getElementById("btn-cds"); if(bcds) bcds.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace("cds"); };
'''
RAIL_OLD = '      bonds: ["Bonds & Yields", "/bonds.html"],\n'
RAIL_NEW = '      bonds: ["Bonds & Yields", "/bonds.html"],\n      cds: ["CDS · credit default swaps", "/cds.html"],\n'
HTML_RAIL = '&heat=20261014b"'
HTML_RAIL_NEW = '&heat=20261014b&cds=20261015"'
HTML_ENG = '&candles=20261013&auc=20261014"'
HTML_ENG_NEW = '&candles=20261013&auc=20261014&cds=20261015"'

BRIEF = r"""const unpricedBrief=u=>{
 const trades=Number(u.trades_30d)||0, amb=Number(u.ambiguous_30d)||0, days=Number(u.days_active_30d)||0;
 const cap=Number(u.capped_share_30d), capPct=Number.isFinite(cap)?Math.round(cap*100):null;
 const cs=u.candidates||[], parts=[];
 if(!cs.length&&capPct===100) parts.push('Every print in the last 30 days is a capped block. Notional is top-coded, so the upfront cannot be solved into a spread.');
 else if(!cs.length&&capPct>0) parts.push(capPct+'% of the prints are capped blocks, and the uncapped ones still do not solve to a spread.');
 else if(!cs.length) parts.push('Prints are on the public tape, but none of them solve to a spread.');
 else if(cs.length===1){
  const c=cs[0], gap=Math.abs(Number(c.above_coupon_bp)-Number(c.below_coupon_bp));
  parts.push('One '+c.coupon_bp+'bp coupon, on '+c.days+' day'+(Number(c.days)===1?'':'s')+'. The unsigned upfront fits both '+bp(c.below_coupon_bp)+' and '+bp(c.above_coupon_bp)+' ('+bp(gap,0)+' apart). Nothing on the tape picks a side: no quoted spread, no second coupon, no single feasible branch, no own fixed history, no index co-movement.');
 } else parts.push(cs.length+' coupons, each with two feasible spreads, and nothing on the tape picks a side.');
 if(trades) parts.push(amb+' of '+trades+' prints across '+days+' days are unsigned.');
 if(capPct!==null&&capPct>0&&capPct<100&&cs.length) parts.push(capPct+'% of those prints are capped blocks and were dropped.');
 if(fin(u.last_spread_bp)) parts.push('Last sign-fixed level was '+bp(u.last_spread_bp,1)+'bp on '+(u.last_priced_date||'an earlier day')+'. Every print after that is unsigned, so that old level is not reused.');
 else parts.push('This name has never had a sign-fixed level on the public tape.');
 if(fin(u.last_points_upfront)) parts.push('Last disclosed upfront was '+Number(u.last_points_upfront).toFixed(1)+' points.');
 return parts.join(' ');
};
"""

UNP_NEW = r""" byId('cdsdesk-unpriced-tag').textContent=`${unp.length} names · sorted by prints · the sentence is that name's reason`;
 const nTwo=unp.filter(u=>(u.candidates||[]).length===1).length;
 const nMany=unp.filter(u=>(u.candidates||[]).length>1).length;
 const nCap=unp.filter(u=>!(u.candidates||[]).length&&Number(u.capped_share_30d)>=0.999).length;
 const nNone=unp.length-nTwo-nMany-nCap;
 const whyPills=[[nTwo,'warning','two spreads, no side chosen'],[nMany,'warning','several coupons, still unsigned'],[nCap,'mute','capped block only — no spread'],[nNone,'mute','on the tape, spread does not solve']].map(p=>`<span class="pill ${p[1]}">${p[0]} · ${esc(p[2])}</span>`).join('');
 byId('cdsdesk-unpriced').innerHTML=unp.length?`<div style="display:flex;flex-wrap:wrap;gap:6px;margin:0 0 10px">${whyPills}</div><div style="overflow-x:auto;max-height:680px;overflow-y:auto"><table><thead><tr><th>Name</th><th>Group</th><th>Sector / region</th><th>Prints 30d</th><th>Days active</th><th>Ambiguous</th><th title="share of 30-day prints whose notional is capped, so a spread cannot be solved">Capped</th><th>Last print</th><th title="the two spreads the unsigned upfront could mean: below the coupon or above it">Either … or (bp)</th><th>Last fixed level</th><th>Fixed on</th><th>Why unpriced</th></tr></thead><tbody>${unp.map(u=>{const cap=Number(u.capped_share_30d);const capPct=Number.isFinite(cap)?Math.round(cap*100):null;return `<tr>${nameCell(u)}<td class="t-mute">${u.grp}</td><td class="t-mute">${esc(u.sector||u.region||'')}</td><td${sd(u.trades_30d)}>${u.trades_30d??'—'}</td><td${sd(u.days_active_30d)}>${u.days_active_30d??'—'}</td><td${sd(u.ambiguous_30d)}>${u.ambiguous_30d??'—'}</td><td${sd(capPct)}>${capPct==null?'—':capPct+'%'}</td><td class="t-mute" data-sort="${esc(u.last_date||'')}">${esc(u.last_date||'')}</td><td${sd((u.candidates||[]).length?u.candidates[0].above_coupon_bp:null)}>${cands(u)}</td><td${sd(u.last_spread_bp)}>${fin(u.last_spread_bp)?`<b>${bp(u.last_spread_bp,1)}</b>`:'<span class="t-mute">never</span>'}</td><td class="t-mute" data-sort="${esc(u.last_priced_date||'')}">${esc(u.last_priced_date||'—')}</td><td class="t-mute" style="font-size:11px;line-height:1.35;max-width:460px" title="${esc(u.why||'')}">${esc(unpricedBrief(u))}</td></tr>`;}).join('')}</tbody></table></div>
  <div class="t-mute" style="font-family:var(--font-mono);font-size:10.5px;margin-top:8px"><b>Why a name stays unpriced:</b> the public DTCC file carries each print's coupon, notional, maturity and upfront amount, but not which side paid it. A quoted spread is filled on only a minority of single-name prints, so the engine solves the ISDA standard model for the two spreads consistent with the unsigned upfront — one below the coupon, one above — and keeps the name unpriced unless a quoted print, a second coupon, a single feasible branch, the name's own fixed level, or co-movement with its index picks one. A capped block has a top-coded notional, so no spread can be solved from it. The four counts above are those cases. The sentence on each row uses only that name's prints, candidates, capped share and last fixed level. Listed rather than guessed.</div>`:'<span class="t-mute">every active name is priced</span>';
"""


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def once(text, old, new, label):
    if new in text:
        print("already", label)
        return text
    n = text.count(old)
    if n != 1:
        raise SystemExit(label + " count " + str(n))
    print("patched", label)
    return text.replace(old, new, 1)


def peel_source(after):
    return """
function peelCdsHtml(raw,file){
 const AFTER=%(html)s;
 const BEFORE=%(html_before)s;
 if(file!=='chart.html'||hash(raw)!==AFTER)return raw;
 const from='&cds=20261015';
 assert.equal(raw.split(from).length,3,'chart.html cds cache');
 raw=raw.split(from).join('');
 assert.equal(hash(raw),BEFORE,'chart.html');
 return raw;
}
function peelCdsEngine(raw,file){
 const AFTER=%(engine)s;
 const BEFORE=%(engine_before)s;
 const BTN=%(btn)s;
 const CLICK=%(click)s;
 if(file!=='jh-chart-engine.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(BTN).length,2,'jh-chart-engine.js cds button');
 assert.equal(raw.split(CLICK).length,2,'jh-chart-engine.js cds click');
 raw=raw.replace(BTN,'').replace(CLICK,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-engine.js');
 return raw;
}
function peelCdsRail(raw,file){
 const AFTER=%(rail)s;
 const BEFORE=%(rail_before)s;
 const LINE=%(line)s;
 if(file!=='jh-chart-tvrail.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(LINE).length,2,'jh-chart-tvrail.js cds meta');
 raw=raw.replace(LINE,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-tvrail.js');
 return raw;
}
""" % {
        "html": repr(after["chart.html"]),
        "html_before": repr(BEFORE["chart.html"]),
        "engine": repr(after["jh-chart-engine.js"]),
        "engine_before": repr(BEFORE["jh-chart-engine.js"]),
        "rail": repr(after["jh-chart-tvrail.js"]),
        "rail_before": repr(BEFORE["jh-chart-tvrail.js"]),
        "btn": repr('      "<button class=\'wsico wsdesk\' id=btn-cds title=\'Credit default swaps\'><span class=g>CDS</span><span class=l>CDS</span></button>"+\n'),
        "click": repr('    var bcds=document.getElementById("btn-cds"); if(bcds) bcds.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace("cds"); };\n'),
        "line": repr('      cds: ["CDS · credit default swaps", "/cds.html"],\n'),
    }


def main():
    if "id=btn-cds" not in ENGINE.read_text(encoding="utf-8"):
        for path in (ENGINE, RAIL, HTML):
            digest = sha(path)
            if digest != BEFORE[path.name]:
                raise SystemExit(path.name + " hash " + digest)
        ENGINE.write_text(once(once(ENGINE.read_text(encoding="utf-8"), BTN_OLD, BTN_NEW, "button"), CLICK_OLD, CLICK_NEW, "click"), encoding="utf-8")
        RAIL.write_text(once(RAIL.read_text(encoding="utf-8"), RAIL_OLD, RAIL_NEW, "rail"), encoding="utf-8")
        html = HTML.read_text(encoding="utf-8")
        html = once(html, HTML_RAIL, HTML_RAIL_NEW, "html rail")
        html = once(html, HTML_ENG, HTML_ENG_NEW, "html engine")
        HTML.write_text(html, encoding="utf-8")
    else:
        print("already chart tab")

    desk = DESK.read_text(encoding="utf-8")
    if "const unpricedBrief=" not in desk:
        desk = once(desk, "const evidence=r=>", BRIEF + "const evidence=r=>", "brief")
        start = desk.find(" byId('cdsdesk-unpriced-tag')")
        end = desk.find(" const basisBox=byId('cdsdesk-basis')", start)
        if start < 0 or end < 0:
            raise SystemExit("unpriced bounds")
        desk = desk[:start] + UNP_NEW + desk[end:]
        if desk.count("const unpricedBrief=") != 1 or desk.count("const whyPills=") != 1:
            raise SystemExit("desk markers")
        DESK.write_text(desk, encoding="utf-8")
        print("patched desk")
    else:
        print("already desk")

    peel = PEEL.read_text(encoding="utf-8")
    if "function peelCdsHtml" not in peel:
        after = {name: sha(Path(name)) for name in BEFORE}
        block = peel_source(after)
        needle = "function normalize(raw,file){"
        if peel.count(needle) != 1:
            raise SystemExit("normalize missing")
        peel = peel.replace(needle, block + "function normalize(raw,file){", 1)
        peel = peel.replace(
            "function normalize(raw,file){raw=peelHeatHtml(raw,file);",
            "function normalize(raw,file){raw=peelCdsHtml(raw,file);raw=peelCdsEngine(raw,file);raw=peelCdsRail(raw,file);raw=peelHeatHtml(raw,file);",
            1,
        )
        PEEL.write_text(peel, encoding="utf-8")
        print("patched peel", after["chart.html"][:12], after["jh-chart-engine.js"][:12], after["jh-chart-tvrail.js"][:12])
    else:
        print("already peel")


if __name__ == "__main__":
    main()
