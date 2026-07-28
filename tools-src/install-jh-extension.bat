@echo off
title JustHodl TV Extension - One-Click Installer
echo.
echo  Fetching the installer (always the latest version)...
powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop'; $p=Join-Path $env:TEMP 'jh-install.ps1'; Invoke-WebRequest -UseBasicParsing 'https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/tools/install-jh-extension.ps1' -OutFile $p; & $p"
echo.
pause
