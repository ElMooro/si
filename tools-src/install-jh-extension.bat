@echo off
title JustHodl TV Extension - One-Click Installer
echo.
echo  =====================================================
echo   JustHodl TV Notes + Source Harvester - Installer
echo  =====================================================
echo.
echo  Downloading the latest build and setting everything up...
echo.
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='Stop';" ^
  "$dir=Join-Path $env:LOCALAPPDATA 'JustHodl\jh-tv-extension';" ^
  "$zip=Join-Path $env:TEMP 'jh-tv-extension.zip';" ^
  "Write-Host '  [1/4] Downloading latest build...';" ^
  "Invoke-WebRequest -UseBasicParsing 'https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/tools/jh-tv-extension.zip' -OutFile $zip;" ^
  "Write-Host '  [2/4] Installing to' $dir;" ^
  "if(Test-Path $dir){Remove-Item -Recurse -Force $dir};" ^
  "New-Item -ItemType Directory -Force -Path $dir ^| Out-Null;" ^
  "Expand-Archive -Force $zip $dir;" ^
  "$man=Get-Content (Join-Path $dir 'manifest.json') -Raw ^| ConvertFrom-Json;" ^
  "Write-Host ('  [3/4] Installed version ' + $man.version);" ^
  "$chrome=$null; foreach($p in @((Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe' -ErrorAction SilentlyContinue).'(default)', \"$env:ProgramFiles\Google\Chrome\Application\chrome.exe\", \"${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe\", \"$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe\")){ if($p -and (Test-Path $p)){ $chrome=$p; break } };" ^
  "if(-not $chrome){ throw 'Chrome not found - install Google Chrome first.' };" ^
  "Write-Host '  [4/4] Creating Desktop shortcut: TradingView (JH Harvester)';" ^
  "$ws=New-Object -ComObject WScript.Shell;" ^
  "$lnk=$ws.CreateShortcut((Join-Path $ws.SpecialFolders('Desktop') 'TradingView (JH Harvester).lnk'));" ^
  "$lnk.TargetPath=$chrome;" ^
  "$lnk.Arguments=('--load-extension=\"'+$dir+'\" https://www.tradingview.com/chart/');" ^
  "$lnk.IconLocation=$chrome+',0';" ^
  "$lnk.Description='TradingView with the JustHodl harvester pre-loaded';" ^
  "$lnk.Save();" ^
  "Write-Host ''; Write-Host '  DONE. Version' $man.version 'installed.' -ForegroundColor Green;"
if errorlevel 1 (
  echo.
  echo  Something went wrong - screenshot this window and send it to Claude.
  pause
  exit /b 1
)
echo.
echo  =====================================================
echo   All set. From now on:
echo.
echo   1. CLOSE all Chrome windows once (so the flag takes)
echo   2. Double-click "TradingView (JH Harvester)" on your
echo      Desktop - Chrome opens on TradingView with the
echo      extension already loaded. No chrome://extensions.
echo   3. Click the JH icon - "Harvest sources" - walk away.
echo      It auto-uploads when finished.
echo.
echo   To UPDATE later: just run this installer again.
echo  =====================================================
echo.
pause
