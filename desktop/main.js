// JustHodl AI — Electron main process
// Loads the live justhodl.ai site inside a native desktop shell with a
// tool sidebar, desktop notifications for alerts, deep-linking, and an
// offline fallback. The site is loaded live so the app always has the
// latest tools without needing an app update.

const { app, BrowserWindow, BrowserView, ipcMain, Menu, shell, Notification, nativeTheme } = require('electron');
const path = require('path');

const SITE = 'https://justhodl.ai';
const SIDEBAR_WIDTH = 168;
let mainWindow = null;
let view = null;          // BrowserView that renders the live site
let currentUrl = `${SITE}/chart-pro.html`;

// The tools shown in the native sidebar (label, path, emoji)
const TOOLS = [
  { label: 'Chart Pro', path: '/chart-pro.html', icon: '📈' },
  { label: "Today's Setups", path: '/signal-board.html', icon: '🎯' },
  { label: 'Opportunities', path: '/opportunities.html', icon: '💡' },
  { label: 'Buy the Laggard', path: '/dislocations.html', icon: '⚖️' },
  { label: 'Compounders', path: '/compounders.html', icon: '🌱' },
  { label: 'Capital Flow', path: '/capital-flow.html', icon: '💰' },
  { label: 'Intelligence', path: '/intelligence.html', icon: '🧠' },
  { label: 'Retail Edges', path: '/retail-edges.html', icon: '🔥' },
  { label: 'Risk Desk', path: '/risk-desk.html', icon: '🛡️' },
  { label: 'Proof', path: '/proof.html', icon: '✅' },
];

function viewBounds() {
  const [w, h] = mainWindow.getContentSize();
  return { x: SIDEBAR_WIDTH, y: 0, width: w - SIDEBAR_WIDTH, height: h };
}

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1440,
    height: 900,
    minWidth: 1024,
    minHeight: 680,
    backgroundColor: '#0a0e14',
    titleBarStyle: process.platform === 'darwin' ? 'hiddenInset' : 'default',
    title: 'JustHodl AI',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  // The chrome (sidebar) is our own renderer page
  mainWindow.loadFile(path.join(__dirname, 'renderer', 'shell.html'));

  // The site itself renders in a BrowserView pinned next to the sidebar
  view = new BrowserView({
    webPreferences: { contextIsolation: true, nodeIntegration: false },
  });
  mainWindow.setBrowserView(view);
  view.setBounds(viewBounds());
  view.setAutoResize({ width: true, height: true });
  loadTool(currentUrl);

  mainWindow.on('resize', () => view && view.setBounds(viewBounds()));

  // External links open in the user's browser, not the app
  view.webContents.setWindowOpenHandler(({ url }) => {
    if (!url.startsWith(SITE)) { shell.openExternal(url); return { action: 'deny' }; }
    return { action: 'allow' };
  });

  // Offline fallback
  view.webContents.on('did-fail-load', (e, code, desc, url) => {
    if (code === -106 || code === -105 || code === -2) { // no internet / dns
      view.webContents.loadFile(path.join(__dirname, 'renderer', 'offline.html'));
    }
  });

  // keep the sidebar's active state in sync with navigation
  view.webContents.on('did-navigate', (e, url) => {
    currentUrl = url;
    mainWindow.webContents.send('nav-changed', url);
  });
  view.webContents.on('did-navigate-in-page', (e, url) => {
    currentUrl = url;
    mainWindow.webContents.send('nav-changed', url);
  });
}

function loadTool(url) {
  currentUrl = url;
  view.webContents.loadURL(url);
}

// ── IPC from the sidebar ──
ipcMain.handle('get-tools', () => TOOLS);
ipcMain.handle('get-site', () => SITE);
ipcMain.on('navigate', (e, pathOrUrl) => {
  const url = pathOrUrl.startsWith('http') ? pathOrUrl : `${SITE}${pathOrUrl}`;
  loadTool(url);
});
ipcMain.on('go-back', () => { if (view.webContents.canGoBack()) view.webContents.goBack(); });
ipcMain.on('go-forward', () => { if (view.webContents.canGoForward()) view.webContents.goForward(); });
ipcMain.on('reload', () => view.webContents.reload());
ipcMain.on('open-external', (e, url) => shell.openExternal(url));

// Desktop notification (the site can call window.jhNotify via preload)
ipcMain.on('notify', (e, { title, body }) => {
  if (Notification.isSupported()) {
    const n = new Notification({ title: title || 'JustHodl AI', body: body || '' });
    n.on('click', () => { mainWindow && mainWindow.focus(); });
    n.show();
  }
});

// ── App menu ──
function buildMenu() {
  const isMac = process.platform === 'darwin';
  const template = [
    ...(isMac ? [{ role: 'appMenu' }] : []),
    {
      label: 'View',
      submenu: [
        { label: 'Back', accelerator: 'CmdOrCtrl+[', click: () => view && view.webContents.canGoBack() && view.webContents.goBack() },
        { label: 'Forward', accelerator: 'CmdOrCtrl+]', click: () => view && view.webContents.canGoForward() && view.webContents.goForward() },
        { label: 'Reload', accelerator: 'CmdOrCtrl+R', click: () => view && view.webContents.reload() },
        { type: 'separator' },
        { role: 'resetZoom' }, { role: 'zoomIn' }, { role: 'zoomOut' },
        { type: 'separator' },
        { role: 'togglefullscreen' },
      ],
    },
    {
      label: 'Tools',
      submenu: TOOLS.map(t => ({
        label: `${t.icon}  ${t.label}`,
        click: () => loadTool(`${SITE}${t.path}`),
      })),
    },
    {
      label: 'Window',
      submenu: [{ role: 'minimize' }, { role: 'zoom' }, ...(isMac ? [{ role: 'front' }] : [{ role: 'close' }])],
    },
    {
      role: 'help',
      submenu: [
        { label: 'JustHodl.AI Website', click: () => shell.openExternal(SITE) },
        { label: 'Open in Browser', click: () => shell.openExternal(currentUrl) },
      ],
    },
  ];
  Menu.setApplicationMenu(Menu.buildFromTemplate(template));
}

app.whenReady().then(() => {
  nativeTheme.themeSource = 'dark';
  createWindow();
  buildMenu();
  app.on('activate', () => { if (BrowserWindow.getAllWindows().length === 0) createWindow(); });
});

app.on('window-all-closed', () => { if (process.platform !== 'darwin') app.quit(); });
