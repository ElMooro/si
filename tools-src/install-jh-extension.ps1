# JustHodl TV Extension - installer logic (fetched by the .bat stub).
# No cmd escaping anywhere in here - this file is why the caret-pipe bug can't recur.
#
# v4 CHANGE: the old build ran Remove-Item -Recurse -Force on the install
# folder before re-expanding. That is the exact folder Chrome loaded the
# unpacked extension from, so every UPDATE destroyed Chrome's registration
# and forced a fresh "Load unpacked". v4 stages the download to a temp
# folder and copies files in place, so the folder path never disappears
# and an update needs nothing more than the reload arrow.
$ErrorActionPreference = 'Stop'
Write-Host ''
Write-Host '  ====================================================='
Write-Host '   JustHodl TV Notes + Source Harvester - Installer v4'
Write-Host '  ====================================================='
Write-Host ''
try {
  $dir     = Join-Path $env:LOCALAPPDATA 'JustHodl\jh-tv-extension'
  $zip     = Join-Path $env:TEMP 'jh-tv-extension.zip'
  $stage   = Join-Path $env:TEMP 'jh-tv-stage'
  $manPath = Join-Path $dir 'manifest.json'

  $oldVer = $null
  $isUpdate = Test-Path $manPath
  if ($isUpdate) {
    try { $oldVer = (Get-Content $manPath -Raw | ConvertFrom-Json).version } catch {}
  }

  Write-Host '  [1/4] Downloading latest build...'
  Invoke-WebRequest -UseBasicParsing 'https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/tools/jh-tv-extension.zip' -OutFile $zip

  Write-Host "  [2/4] Installing to $dir"
  if (Test-Path $stage) { Remove-Item -Recurse -Force $stage }
  New-Item -ItemType Directory -Force -Path $stage | Out-Null
  Expand-Archive -Force $zip $stage

  if (-not (Test-Path (Join-Path $stage 'manifest.json'))) {
    $inner = Get-ChildItem $stage -Directory | Select-Object -First 1
    if ($inner) { $stage = $inner.FullName }
  }
  if (-not (Test-Path (Join-Path $stage 'manifest.json'))) {
    throw 'Downloaded zip has no manifest.json - build is bad, tell Claude.'
  }

  New-Item -ItemType Directory -Force -Path $dir | Out-Null
  Get-ChildItem -Force $dir | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
  Copy-Item (Join-Path $stage '*') $dir -Recurse -Force

  $manPath = Join-Path $dir 'manifest.json'
  $man = Get-Content $manPath -Raw | ConvertFrom-Json
  if ($oldVer) {
    Write-Host ('  [3/4] Updated  ' + $oldVer + '  ->  ' + $man.version) -ForegroundColor Green
  } else {
    Write-Host ('  [3/4] Installed version ' + $man.version) -ForegroundColor Green
  }

  $chrome = $null
  $cands = @()
  try { $cands += (Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe' -ErrorAction SilentlyContinue).'(default)' } catch {}
  $cands += "$env:ProgramFiles\Google\Chrome\Application\chrome.exe"
  $cands += "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe"
  $cands += "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
  foreach ($p in $cands) { if ($p -and (Test-Path $p)) { $chrome = $p; break } }
  if (-not $chrome) { throw 'Chrome not found - install Google Chrome first.' }

  Write-Host '  [4/4] Opening the Chrome extensions page'
  Write-Host ''
  if ($isUpdate) {
    Write-Host '  THIS WAS AN UPDATE - one click, nothing else:' -ForegroundColor Yellow
    Write-Host ''
    Write-Host '     1. On the chrome://extensions tab that just opened,'
    Write-Host '        find the JustHodl card and click the RELOAD arrow.'
    Write-Host '     2. Open tradingview.com. The walk auto-starts on the'
    Write-Host '        new version - no button needed.'
    Write-Host ''
    Write-Host '     (Folder was updated in place, so Chrome keeps its'
    Write-Host '      registration. If the card somehow shows an error,'
    Write-Host '      click Load unpacked and pick the same folder.)'
  } else {
    Write-Host '  ONE-TIME step in Chrome (Google removed silent loading in 2025,' -ForegroundColor Yellow
    Write-Host '  so this is the only legitimate way - and it PERSISTS forever):' -ForegroundColor Yellow
    Write-Host ''
    Write-Host '     1. On the chrome://extensions tab that just opened:'
    Write-Host '        toggle  Developer mode  ON (top-right)'
    Write-Host '     2. Click  Load unpacked  and pick the folder that just'
    Write-Host ('        opened in Explorer:  ' + $dir)
    Write-Host '     3. Open tradingview.com - the amber JH panel appears.'
    Write-Host '        The harvest auto-starts; just walk away.'
  }
  Write-Host ''

  if (-not $isUpdate) { Start-Process explorer.exe $dir }
  Start-Process $chrome 'chrome://extensions/'

  $ws = New-Object -ComObject WScript.Shell
  $desks = @([Environment]::GetFolderPath('Desktop'),
             (Join-Path $env:USERPROFILE 'Desktop'))
  if ($env:OneDrive) { $desks += (Join-Path $env:OneDrive 'Desktop') }
  $desks = $desks | Where-Object { $_ -and (Test-Path $_) } | Select-Object -Unique
  foreach ($d in $desks) {
    try {
      $lnk = $ws.CreateShortcut((Join-Path $d 'TradingView (JH Harvester).lnk'))
      $lnk.TargetPath = $chrome
      $lnk.Arguments = 'https://www.tradingview.com/chart/'
      $lnk.IconLocation = "$chrome,0"
      $lnk.Description = 'TradingView - JH harvester persists once loaded'
      $lnk.Save()
    } catch {}
  }
  if (Test-Path $stage) { Remove-Item -Recurse -Force $stage -ErrorAction SilentlyContinue }
  Write-Host '  Desktop shortcut written (plain TradingView launcher - the'
  Write-Host '  extension persists on its own after the one-time load).'
} catch {
  Write-Host ''
  Write-Host ('  ERROR: ' + $_.Exception.Message) -ForegroundColor Red
  Write-Host '  Screenshot this window and send it to Claude.'
  exit 1
}
