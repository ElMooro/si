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
  Write-Host '  [4/4] Creating Desktop shortcut: TradingView (JH Harvester)'
  $ws = New-Object -ComObject WScript.Shell
  $lnkPath = Join-Path ([Environment]::GetFolderPath('Desktop')) 'TradingView (JH Harvester).lnk'
  $lnk = $ws.CreateShortcut($lnkPath)
  $lnk.TargetPath = $chrome
  $lnk.Arguments = ('--load-extension="' + $dir + '" https://www.tradingview.com/chart/')
  $lnk.IconLocation = "$chrome,0"
  $lnk.Description = 'TradingView with the JustHodl harvester pre-loaded'
  $lnk.Save()
  Write-Host ''
  Write-Host ("  DONE. Version " + $man.version + " installed.") -ForegroundColor Green
  Write-Host ''
  Write-Host '  ====================================================='
  Write-Host '   From now on:'
  Write-Host '   1. CLOSE all Chrome windows once (so the flag takes)'
  Write-Host '   2. Double-click "TradingView (JH Harvester)" on your'
  Write-Host '      Desktop - Chrome opens on TradingView with the'
  Write-Host '      extension already loaded.'
  Write-Host '   3. Click the JH icon -> "Harvest sources" -> walk away.'
  Write-Host '      It auto-uploads when finished.'
  Write-Host '   To UPDATE later: run the same installer again.'
  Write-Host '  ====================================================='
} catch {
  Write-Host ''
  Write-Host ('  ERROR: ' + $_.Exception.Message) -ForegroundColor Red
  Write-Host '  Screenshot this window and send it to Claude.'
  exit 1
}
