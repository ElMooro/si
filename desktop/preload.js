// Secure bridge — exposes a minimal, safe API to the sidebar shell and the site.
const { contextBridge, ipcRenderer } = require('electron');

// API for the native sidebar (shell.html)
contextBridge.exposeInMainWorld('jh', {
  getTools: () => ipcRenderer.invoke('get-tools'),
  getSite: () => ipcRenderer.invoke('get-site'),
  navigate: (p) => ipcRenderer.send('navigate', p),
  back: () => ipcRenderer.send('go-back'),
  forward: () => ipcRenderer.send('go-forward'),
  reload: () => ipcRenderer.send('reload'),
  openExternal: (u) => ipcRenderer.send('open-external', u),
  onNavChanged: (cb) => ipcRenderer.on('nav-changed', (e, url) => cb(url)),
});

// API for the live site — lets pages fire native desktop notifications
// (e.g. a new Triple/Quad-Threat alert). Pages call window.jhNotify(...).
contextBridge.exposeInMainWorld('jhNotify', (title, body) => {
  ipcRenderer.send('notify', { title, body });
});
contextBridge.exposeInMainWorld('jhDesktop', { isDesktop: true, version: '1.0.0' });
