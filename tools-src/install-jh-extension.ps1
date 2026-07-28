# JustHodl TV Extension - installer logic (fetched by the .bat stub).
# No cmd escaping anywhere in here - this file is why the caret-pipe bug can't recur.
$ErrorActionPreference = 'Stop'
Write-Host ''
Write-Host '  =====================================================' 
Write-Host '   JustHodl TV Notes + Source Harvester - Installer'
Write-Host '  ====================================================='
Write-Host ''
try {
  $dir = Join-Path $env:LOCALAPPDATA 'JustHodl\jh-tv-extension'
  $zip = Join-Path $env:TEMP 'jh-tv-extension.zip'
  Write-Host '  [1/4] Downloading latest build...'
  Invoke-WebRequest -UseBasicParsing 'https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/tools/jh-tv-extension.zip' -OutFile $zip
  Write-Host "  [2/4] Installing to $dir"
  if (Test-Path $dir) { Remove-Item -Recurse -Force $dir }
  New-Item -ItemType Directory -Force -Path $dir | Out-Null
  Expand-Archive -Force $zip $dir
  $manPath = Join-Path $dir 'manifest.json'
  if (-not (Test-Path $manPath)) {
    $inner = Get-ChildItem $dir -Directory | Select-Object -First 1
    if ($inner) { $dir = $inner.FullName; $manPath = Join-Path $dir 'manifest.json' }
  }
  $man = Get-Content $manPath -Raw | ConvertFrom-Json
  Write-Host ("  [3/4] Installed version " + $man.version)
  $chrome = $null
  $cands = @()
  try { $cands += (Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe' -ErrorAction SilentlyContinue).'(default)' } catch {}
  $cands += "$env:ProgramFiles\Google\Chrome\Application\chrome.exe"
  $cands += "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe"
  $cands += "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
  foreach ($p in $cands) { if ($p -and (Test-Path $p)) { $chrome = $p; break } }
  if (-not $chrome) { throw 'Chrome not found - install Google Chrome first.' }
  Write-Host '  [4/4] Opening Chrome extensions page + the install folder side-by-side'
  Write-Host ''
  Write-Host '  ONE-TIME step in Chrome (Google removed silent loading in 2025,' -ForegroundColor Yellow
  Write-Host '  so this is the only legitimate way - and it PERSISTS forever):' -ForegroundColor Yellow
  Write-Host ''
  Write-Host '     1. On the chrome://extensions tab that just opened:'
  Write-Host '        toggle  Developer mode  ON (top-right)'
  Write-Host '     2. Click  Load unpacked  and pick the folder that just'
  Write-Host ('        opened in Explorer:  ' + $dir)
  Write-Host '     3. Open tradingview.com - the amber JH panel appears.'
  Write-Host '        Click the JH icon -> "Harvest sources" -> walk away.'
  Write-Host ''
  Start-Process explorer.exe $dir
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
  Write-Host '  Desktop shortcut written (plain TradingView launcher - the'
  Write-Host '  extension persists on its own after the one-time load).'
} catch {
  Write-Host ''
  Write-Host ('  ERROR: ' + $_.Exception.Message) -ForegroundColor Red
  Write-Host '  Screenshot this window and send it to Claude.'
  exit 1
}
