/* Public whitelist projection only. The full account brief remains private. */
(function () {
  'use strict';
  const main = document.querySelector('main');
  if (!main) return;
  const box = document.createElement('section');
  box.id = 'public-market-brief';
  box.style.cssText = 'margin:24px 0;padding:20px;border:1px solid #596475;border-radius:8px;line-height:1.65';
  const heading = document.createElement('h2');
  heading.textContent = 'Source-backed market brief';
  const status = document.createElement('p');
  status.textContent = 'Loading the public market projection…';
  box.append(heading, status);
  main.append(box);
  fetch('/data/ai-brief-public.json', {cache:'no-store', signal:AbortSignal.timeout(15000)})
    .then(r => { if (!r.ok) throw new Error('HTTP '+r.status); return r.json(); })
    .then(doc => {
      if (doc.generation_method !== 'warehouse_deterministic_v1' || typeof doc.brief_md !== 'string') throw new Error('Invalid public brief contract');
      const age = Date.now()-Date.parse(doc.generated_at);
      const overdue = !Number.isFinite(age) || age < -300000 || age > 4.5*3600000;
      status.textContent = (overdue ? 'Overdue · ' : '')+'WAIT — observation only · '+doc.generated_at+' · deterministic synthesis, no paid model calls.';
      const details = document.createElement('details');
      const summary = document.createElement('summary');
      summary.textContent = 'Read evidence, dates, limitations and watch conditions';
      details.append(summary);
      for (const line of doc.brief_md.split('\n')) {
        if (!line.trim() || line.startsWith('# ')) continue;
        const el = document.createElement(line.startsWith('## ') ? 'h3' : 'p');
        el.textContent = line.replace(/^## /,'').replace(/^\*\*|\*\*$/g,'');
        el.style.overflowWrap = 'anywhere';
        details.append(el);
      }
      box.append(details);
    }).catch(() => { status.textContent = 'Public brief unavailable. WAIT — no new allocation guidance; inspect the dated ledger above.'; });
})();
