/**
 * JustHodl shared sidebar
 * 
 * Usage: include in <head>:
 *   <link rel="stylesheet" href="/assets/sidebar.css">
 *   <div id="sidebar"></div>
 *   <script src="/assets/sidebar.js"></script>
 * 
 * Wrap the rest of your page content in:
 *   <div class="jh-page">...</div>
 * 
 * The script:
 *   1. Fetches /_partials/sidebar.html and injects into #sidebar
 *   2. Marks the current page link as .jh-active
 *   3. Wires up the filter input (instant filter, cmd+k focus)
 *   4. Starts the UTC clock
 */

(function(){
  'use strict';

  const SIDEBAR_PATH = '/_partials/sidebar.html';

  function init(){
    const mountPoint = document.getElementById('sidebar');
    if (!mountPoint){
      console.warn('[jh-sidebar] no #sidebar element found');
      return;
    }

    fetch(SIDEBAR_PATH)
      .then(r => r.ok ? r.text() : Promise.reject(r.status))
      .then(html => {
        mountPoint.innerHTML = html;
        markActive();
        wireFilter();
        startClock();
        wireCmdK();
      })
      .catch(err => {
        console.error('[jh-sidebar] failed to load partial:', err);
        mountPoint.innerHTML = '<aside class="jh-sidebar"><div style="padding:20px;color:#f55;font-family:monospace;font-size:11px">sidebar load failed</div></aside>';
      });
  }

  function markActive(){
    // Match current path against link hrefs.
    // Index page (/) matches /, /index.html, /desk-v2.html (treat as home for now).
    const path = window.location.pathname;
    const links = document.querySelectorAll('.jh-nav a');
    let bestMatch = null;
    let bestMatchLen = -1;

    for (const link of links){
      const href = link.getAttribute('href');
      if (!href) continue;

      // Exact match
      if (href === path){
        bestMatch = link;
        bestMatchLen = href.length;
        break;
      }
      // Trailing-slash equivalence
      if (href === path + '/' || href + '/' === path){
        if (href.length > bestMatchLen){
          bestMatch = link;
          bestMatchLen = href.length;
        }
      }
      // Subdir prefix (e.g. /screener/ matches /screener/index.html)
      if (path.startsWith(href) && href !== '/' && href.endsWith('/')){
        if (href.length > bestMatchLen){
          bestMatch = link;
          bestMatchLen = href.length;
        }
      }
    }

    if (bestMatch){
      bestMatch.classList.add('jh-active');
    }
  }

  function wireFilter(){
    const input = document.getElementById('jhFilter');
    const nav = document.getElementById('jhNav');
    if (!input || !nav) return;

    function applyFilter(q){
      q = (q || '').toLowerCase().trim();
      const links = nav.querySelectorAll('a');
      const clusters = nav.querySelectorAll('.jh-cluster');

      if (!q){
        for (const link of links) link.classList.remove('jh-hidden');
        for (const cluster of clusters) cluster.classList.remove('jh-empty');
        return;
      }

      // Filter individual links
      for (const link of links){
        const name = (link.dataset.name || '').toLowerCase();
        const desc = (link.dataset.desc || '').toLowerCase();
        const text = link.textContent.toLowerCase();
        const matches = name.includes(q) || desc.includes(q) || text.includes(q);
        link.classList.toggle('jh-hidden', !matches);
      }

      // Hide empty clusters
      for (const cluster of clusters){
        const visible = cluster.querySelector('a:not(.jh-hidden)');
        cluster.classList.toggle('jh-empty', !visible);
      }
    }

    input.addEventListener('input', e => applyFilter(e.target.value));
    input.addEventListener('keydown', e => {
      if (e.key === 'Escape'){
        input.value = '';
        applyFilter('');
        input.blur();
      }
    });
  }

  function wireCmdK(){
    document.addEventListener('keydown', e => {
      // Cmd+K (Mac) or Ctrl+K (Win/Linux)
      if ((e.metaKey || e.ctrlKey) && e.key === 'k'){
        e.preventDefault();
        const input = document.getElementById('jhFilter');
        if (input){
          input.focus();
          input.select();
        }
      }
    });
  }

  function startClock(){
    const el = document.getElementById('jhClock');
    if (!el) return;
    function tick(){
      const d = new Date();
      const h = String(d.getUTCHours()).padStart(2, '0');
      const m = String(d.getUTCMinutes()).padStart(2, '0');
      const s = String(d.getUTCSeconds()).padStart(2, '0');
      el.textContent = `${h}:${m}:${s} UTC`;
    }
    tick();
    setInterval(tick, 1000);
  }

  if (document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
