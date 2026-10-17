@echo off
set TASKNAME=MES_Fob_Bridge
set BRIDGE=C:\OPC Logger\run_fob_bridge.bat

echo ============================================
echo  MES Smart Card Bridge - Task Installer
echo ============================================
echo.

if not exist "%BRIDGE%" (
    echo [ERROR] Not found: %BRIDGE%
    echo Copy run_fob_bridge.bat and fob_bridge.py to C:\OPC Logger\
    pause & exit /b 1
)

schtasks /create ^
  /tn "%TASKNAME%" ^
  /tr "\"%BRIDGE%\"" ^
  /sc ONLOGON ^
  /rl HIGHEST ^
  /f

if %errorlevel% neq 0 (
    echo [ERROR] Failed to create task. Run as Administrator.
    pause & exit /b 1
)

powershell -NoProfile -Command ^
  "$t = Get-ScheduledTask -TaskName '%TASKNAME%';" ^
  "$t.Settings.ExecutionTimeLimit = 'PT0S';" ^
  "$t.Settings.RestartCount = 999;" ^
  "$t.Settings.RestartInterval = 'PT1M';" ^
  "$t.Settings.MultipleInstances = 'IgnoreNew';" ^
  "$t.Settings.StartWhenAvailable = $true;" ^
  "$t.Settings.DisallowStartIfOnBatteries = $false;" ^
  "$t.Settings.StopIfGoingOnBatteries = $false;" ^
  "Set-ScheduledTask -InputObject $t | Out-Null"

if %errorlevel% neq 0 (
    echo [WARN] Task created, but hardening settings were not applied.
    pause
)

echo.
echo [OK] Task "%TASKNAME%" installed.
echo      Runs at user logon, restarts automatically.
echo.
schtasks /query /tn "%TASKNAME%" /fo LIST /v | findstr /i "Status Stop Task If Runs Task To Run"
echo.
pause
