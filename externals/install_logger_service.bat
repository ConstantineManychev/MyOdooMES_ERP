@echo off
setlocal

set SCRIPT=C:\OPC Logger\logger_automate.py
set SUPERVISOR=C:\OPC Logger\run_logger_supervised.bat
set WORKDIR=C:\OPC Logger
set TASKNAME=MES_Logger_Automate
set LOGDIR=C:\OPC Logger\logs
set TASKXML=%TEMP%\mes_logger_task.xml

set PYTHON=
for %%P in ("C:\Program Files\Python314\python.exe" "C:\Program Files\Python313\python.exe" "C:\Program Files\Python312\python.exe" "C:\Program Files\Python311\python.exe") do (
    if not defined PYTHON if exist %%P set PYTHON=%%~P
)
if not defined PYTHON (
    echo [ERROR] No supported Python found in C:\Program Files\Python311 to Python314
    echo Run: where python    to find the installed version
    pause & exit /b 1
)
echo [OK] Python: %PYTHON%

if not exist "%SCRIPT%" (
    echo [ERROR] Script not found: %SCRIPT%
    echo Copy logger_automate.py to %WORKDIR% first
    pause & exit /b 1
)

if not exist "%SUPERVISOR%" (
    echo [ERROR] Supervisor not found: %SUPERVISOR%
    echo Copy run_logger_supervised.bat next to logger_automate.py
    echo The existing task was left untouched.
    pause & exit /b 1
)

if not exist "%LOGDIR%" mkdir "%LOGDIR%"

schtasks /end /tn "%TASKNAME%" >nul 2>&1
timeout /t 3 /nobreak >nul
schtasks /delete /tn "%TASKNAME%" /f >nul 2>&1

schtasks /create ^
  /tn "%TASKNAME%" ^
  /tr "\"%SUPERVISOR%\"" ^
  /sc ONSTART ^
  /delay 0000:30 ^
  /ru SYSTEM ^
  /rl HIGHEST ^
  /f

if %errorlevel% neq 0 (
    echo [ERROR] Failed to create task. Run as Administrator.
    pause & exit /b 1
)

powershell -NoProfile -Command ^
  "$t = Get-ScheduledTask -TaskName '%TASKNAME%';" ^
  "$t.Settings.RestartCount = 999;" ^
  "$t.Settings.RestartInterval = 'PT1M';" ^
  "$t.Settings.ExecutionTimeLimit = 'PT0S';" ^
  "$t.Settings.MultipleInstances = 'IgnoreNew';" ^
  "$t.Settings.StartWhenAvailable = $true;" ^
  "$t.Settings.DisallowStartIfOnBatteries = $false;" ^
  "$t.Settings.StopIfGoingOnBatteries = $false;" ^
  "Set-ScheduledTask -InputObject $t | Out-Null" 

if %errorlevel% neq 0 (
    echo [INFO] PowerShell ScheduledTasks module unavailable, restart settings skipped.
    echo        The time limit is removed below through the task XML instead.
)

schtasks /query /tn "%TASKNAME%" /xml > "%TASKXML%" 2>nul
powershell -NoProfile -Command "$p='%TASKXML%'; $t=[IO.File]::ReadAllText($p); $t=$t -replace '<ExecutionTimeLimit>[^<]*</ExecutionTimeLimit>','<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>'; if ($t -notmatch '<ExecutionTimeLimit>') { $t=$t -replace '</Settings>','<ExecutionTimeLimit>PT0S</ExecutionTimeLimit></Settings>' }; $t=$t -replace '<DisallowStartIfOnBatteries>true','<DisallowStartIfOnBatteries>false' -replace '<StopIfGoingOnBatteries>true','<StopIfGoingOnBatteries>false'; [IO.File]::WriteAllText($p,$t,[Text.Encoding]::Unicode)"
schtasks /create /tn "%TASKNAME%" /xml "%TASKXML%" /ru SYSTEM /f >nul

schtasks /query /tn "%TASKNAME%" /xml > "%TASKXML%" 2>nul
powershell -NoProfile -Command "if ([IO.File]::ReadAllText('%TASKXML%') -match '<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>') { exit 0 } else { exit 1 }"
if errorlevel 1 (
    echo [ERROR] The 72 hour time limit is still active on the task.
    echo         Open Task Scheduler, task %TASKNAME%, Settings tab, and untick
    echo         Stop the task if it runs longer than.
    pause
) else (
    echo [OK] No time limit: the task is never stopped by Task Scheduler.
)
del /q "%TASKXML%" >nul 2>&1

schtasks /run /tn "%TASKNAME%"

timeout /t 3 /nobreak >nul

schtasks /query /tn "%TASKNAME%" /fo LIST /v | findstr /i /c:"Status" /c:"Stop Task If Runs"

echo.
echo [DONE] Task "%TASKNAME%" created.
echo        Starts automatically 30 sec after Windows boot.
echo        Supervisor restarts the logger after any exit or crash.
echo        Runs as SYSTEM (full rights including shutdown).
echo.
echo To stop:   schtasks /end /tn "%TASKNAME%"
echo To start:  schtasks /run /tn "%TASKNAME%"
echo To delete: schtasks /delete /tn "%TASKNAME%" /f
echo To check:  schtasks /query /tn "%TASKNAME%" /fo LIST
pause
