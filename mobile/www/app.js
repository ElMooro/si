// JustHodl AI — mobile shell logic
// Loads the live justhodl.ai platform in a webview with a native bottom tab
// bar, push notifications, network detection, and Android back-button handling.

import { App } from '@capacitor/app';
import { Network } from '@capacitor/network';
import { SplashScreen } from '@capacitor/splash-screen';
import { StatusBar, Style } from '@capacitor/status-bar';
import { PushNotifications } from '@capacitor/push-notifications';

const SITE = 'https://justhodl.ai';

// Bottom-tab tools (mobile-friendly key views)
const TABS = [
  { label: 'Chart',    icon: '📈', path: '/chart-pro.html' },
  { label: 'Setups',   icon: '🎯', path: '/signal-board.html' },
  { label: 'Flow',     icon: '💰', path: '/capital-flow.html' },
  { label: 'Ideas',    icon: '💡', path: '/opportunities.html' },
  { label: 'More',     icon: '⋯',  path: '/' },
];

let current = TABS[0].path;
const frame = () => document.getElementById('frame');

function buildTabs() {
  const el = document.getElementById('tabs');
  el.innerHTML = TABS.map((t, i) =>
    `<div class="tab${i === 0 ? ' active' : ''}" data-path="${t.path}" data-i="${i}">
       <span class="ic">${t.icon}</span><span class="lb">${t.label}</span>
     </div>`).join('');
  el.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
      el.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      navigate(tab.dataset.path);
    });
  });
}

function navigate(path) {
  current = path;
  frame().src = SITE + path;
}

function reload() { frame().src = SITE + current; checkNetwork(); }

async function checkNetwork() {
  try {
    const s = await Network.getStatus();
    document.getElementById('offline').classList.toggle('show', !s.connected);
    return s.connected;
  } catch { return true; }
}

// ── Push notifications (Triple/Quad-Threat & high-conviction alerts) ──
async function initPush() {
  try {
    let perm = await PushNotifications.checkPermissions();
    if (perm.receive === 'prompt') perm = await PushNotifications.requestPermissions();
    if (perm.receive !== 'granted') return;
    await PushNotifications.register();
    PushNotifications.addListener('registration', token => {
      // POST the device token to the platform so the alert pusher can target it.
      fetch(SITE.replace('justhodl.ai', 'justhodl-data-proxy.raafouis.workers.dev') + '/register-push', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: token.value, platform: 'mobile' }),
      }).catch(() => {});
    });
    PushNotifications.addListener('pushNotificationActionPerformed', n => {
      // Deep-link: a tapped alert can carry a path (e.g. a ticker on chart-pro)
      const p = n.notification?.data?.path;
      if (p) {
        const idx = TABS.findIndex(t => p.startsWith(t.path));
        document.querySelectorAll('.tab').forEach((t, i) => t.classList.toggle('active', i === (idx >= 0 ? idx : 0)));
        navigate(p);
      }
    });
  } catch (e) { /* push optional */ }
}

// ── Android hardware back button ──
function initBackButton() {
  App.addListener('backButton', ({ canGoBack }) => {
    try {
      const w = frame().contentWindow;
      // Try to go back within the site; if at a tab root, minimize instead.
      if (current !== TABS[0].path) {
        document.querySelector('.tab[data-i="0"]').click();
      } else {
        App.minimizeApp();
      }
    } catch { App.minimizeApp(); }
  });
}

async function boot() {
  try { await StatusBar.setStyle({ style: Style.Dark }); } catch {}
  buildTabs();
  initBackButton();
  Network.addListener('networkStatusChange', s => {
    document.getElementById('offline').classList.toggle('show', !s.connected);
  });

  const online = await checkNetwork();
  navigate(current);

  frame().addEventListener('load', () => {
    document.getElementById('splash').classList.add('hide');
    try { SplashScreen.hide(); } catch {}
  });
  // Safety: hide splash after 3.5s regardless
  setTimeout(() => { document.getElementById('splash').classList.add('hide'); try { SplashScreen.hide(); } catch {} }, 3500);

  initPush();
}

window.JH = { navigate, reload };
boot();
