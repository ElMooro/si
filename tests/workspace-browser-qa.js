const { chromium } = require('playwright');
const fs = require('fs');
const assert = require('node:assert/strict');

(async () => {
  const browser = await chromium.launch({ headless: true });
  const results = {};
  async function open(width, height) {
    const context = await browser.newContext({ viewport: { width, height }, hasTouch: width <= 390, isMobile: width <= 390 });
    const page = await context.newPage();
    const errors = [];
    page.on('pageerror', e => errors.push(String(e)));
    await page.goto('http://127.0.0.1:4173/', { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => document.querySelectorAll('.engine-card').length > 0, null, { timeout: 30000 });
    return { context, page, errors };
  }

  const desk = await open(1440, 900);
  const page = desk.page;
  await page.waitForFunction(() => /851 engines/.test(document.querySelector('#catalogSummary').textContent), null, { timeout: 30000 });
  results.catalog = await page.locator('#catalogSummary').textContent();
  assert.match(results.catalog, /851 engines/);

  await page.click('#openLibrary');
  await page.waitForSelector('.library-item');
  assert.match(await page.locator('#libraryCount').textContent(), /851 matches · showing 100/);
  let loads = 0;
  while (await page.locator('.library-more').count()) { await page.locator('.library-more').click(); loads++; }
  const libraryItems = await page.locator('.library-item').count();
  assert.equal(libraryItems, 851);
  results.browseAll = { loads, libraryItems, count: await page.locator('#libraryCount').textContent() };
  await page.fill('#engineSearch', 'zzzz-no-match');
  assert.match(await page.locator('#libraryCount').textContent(), /^0 matches/);
  await page.fill('#engineSearch', '');
  assert.match(await page.locator('#libraryCount').textContent(), /showing 100$/);
  await page.keyboard.press('Escape');
  assert.equal(await page.evaluate(() => document.activeElement.id), 'openLibrary');

  const zoneAdd = page.locator('.zone-add').first();
  await zoneAdd.click();
  await page.keyboard.press('Escape');
  assert.equal(await page.evaluate(() => document.activeElement === document.querySelector('.zone-add')), true);
  results.libraryFocus = true;

  const firstCard = page.locator('.engine-card').first();
  await firstCard.locator('.configure-card').click();
  await firstCard.locator('select[aria-label="Size"]').focus();
  await firstCard.locator('select[aria-label="Size"]').selectOption('wide');
  assert.equal(await page.evaluate(() => document.activeElement && document.activeElement.getAttribute('aria-label')), 'Size');
  assert.equal(await page.locator('.engine-card').first().locator('.card-settings').isVisible(), true);
  results.configFocus = true;

  const before = await page.locator('.zone-cards[data-zone="conviction"] .engine-card').evaluateAll(ns => ns.map(n => n.dataset.cardId));
  await page.locator(`[data-card-id="${before[0]}"] .drag-handle`).focus();
  await page.keyboard.press('ArrowDown');
  const after = await page.locator('.zone-cards[data-zone="conviction"] .engine-card').evaluateAll(ns => ns.map(n => n.dataset.cardId));
  assert.equal(after[1], before[0]);
  assert.equal(await page.evaluate(() => document.activeElement.classList.contains('drag-handle')), true);
  await page.keyboard.press('ArrowRight');
  assert.equal(await page.locator(`[data-card-id="${before[0]}"]`).evaluate(n => n.parentElement.dataset.zone), 'synthesis');
  results.keyboardReorder = true;

  results.selfDropNoop = "covered by reorderCard guard test/source assertion";

  results.touchReorder = "pointer handlers covered by automated source test; synthetic CDP touch unavailable in desktop context";

  const allocation = page.getByRole('heading', { name: 'Allocation Detail' }).locator('xpath=ancestor::article');
  await allocation.scrollIntoViewIfNeeded();
  await page.waitForFunction(() => { const h=[...document.querySelectorAll('.card-title')].find(x=>x.textContent==='Allocation Detail'); return h && h.closest('.engine-card').querySelector('.card-body').textContent.trim().length; });
  results.allocationText = (await allocation.locator('.card-body').textContent()).trim().slice(0, 120);
  assert.ok(results.allocationText.length > 0);

  const desktopFit = await page.evaluate(() => ({w:innerWidth, sw:document.documentElement.scrollWidth, cards:document.querySelectorAll('.engine-card').length}));
  assert.ok(desktopFit.sw <= desktopFit.w);
  results.desktopFit = desktopFit;
  await page.screenshot({path:'qa-engine-workspace-desktop-fixed.jpg', type:'jpeg', quality:85});
  assert.equal(desk.errors.length, 0, desk.errors.join('\n'));
  await desk.context.close();

  for (const width of [390, 320]) {
    const mobile = await open(width, 844);
    const p = mobile.page;
    await p.waitForFunction(() => /851 engines/.test(document.querySelector('#catalogSummary').textContent), null, { timeout: 30000 });
    const initial = await p.evaluate(() => ({w:innerWidth, sw:document.documentElement.scrollWidth}));
    assert.ok(initial.sw <= initial.w);
    await p.click('#openLibrary');
    const bounds = await p.evaluate(() => {
      const rect = s => { const e=document.querySelector(s); if(!e) return null; const r=e.getBoundingClientRect(); return {l:r.left,r:r.right,t:r.top,b:r.bottom,w:r.width,h:r.height}; };
      const overlap=(a,b)=>a&&b&&Math.max(0,Math.min(a.r,b.r)-Math.max(a.l,b.l))*Math.max(0,Math.min(a.b,b.b)-Math.max(a.t,b.t));
      const auth=rect('[data-auth-slot]'); const close=rect('#closeLibrary'); const theme=rect('#themeToggle');
      return {auth,close,theme,authClose:overlap(auth,close),authTheme:overlap(auth,theme),lib:rect('#libraryPanel'),vw:innerWidth,sw:document.documentElement.scrollWidth};
    });
    assert.equal(bounds.authClose, 0);
    assert.equal(bounds.authTheme, 0);
    assert.ok(bounds.lib.l >= 0 && bounds.lib.r <= width);
    assert.ok(bounds.sw <= bounds.vw);
    await p.click('#closeLibrary');
    await p.click('#themeToggle');
    assert.equal(await p.evaluate(() => document.documentElement.getAttribute('data-theme')), 'light');
    results[`mobile${width}`] = bounds;
    await p.screenshot({path:`qa-engine-workspace-${width}-fixed.jpg`, type:'jpeg', quality:85});
    assert.equal(mobile.errors.length, 0, mobile.errors.join('\n'));
    await mobile.context.close();
  }

  fs.writeFileSync('qa-engine-workspace-results.json', JSON.stringify(results, null, 2));
  console.log(JSON.stringify(results, null, 2));
  await browser.close();
})().catch(async e => { console.error(e); try { await browser.close(); } catch {} process.exit(1); });
