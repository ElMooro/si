# JustHodl AI — Desktop App

Native Electron shell for the JustHodl.AI platform. Loads the live site
(so it's always current) with a native tool sidebar, desktop notifications
for high-conviction alerts, and an offline fallback.

## Develop
```bash
cd desktop
npm install
npm start
```

## Build installers
```bash
npm run dist:mac    # .dmg (arm64 + x64)
npm run dist:win    # .exe (NSIS installer)
npm run dist:linux  # .AppImage + .deb
```

CI builds all three automatically on a `desktop-v*` tag and attaches the
installers to the GitHub Release (see `.github/workflows/build-desktop.yml`).
The download page at `/download.html` auto-links to the latest release assets.
