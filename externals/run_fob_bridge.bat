@echo off
setlocal enabledelayedexpansion

set SCRIPT=%~dp0fob_bridge.py
set RESTART_LOG=%~dp0fob_bridge_supervisor.log
set RESTART_DELAY=10

if defined PYTHON_EXE goto have_python
for %%P in ("C:\Program Files\Python314\python.exe" "C:\Program Files\Python313\python.exe" "C:\Program Files\Python312\python.exe" "C:\Program Files\Python311\python.exe") do (
    if exist %%P set PYTHON_EXE=%%~P
)
:have_python

if not defined PYTHON_EXE (
    echo [%date% %time%] PYTHON_NOT_FOUND >> "%RESTART_LOG%"
    exit /b 1
)

echo [%date% %time%] BRIDGE_SUPERVISOR_START python="%PYTHON_EXE%" >> "%RESTART_LOG%"

:loop
"%PYTHON_EXE%" "%SCRIPT%"
set EXITCODE=!errorlevel!
echo [%date% %time%] BRIDGE_EXITED code=!EXITCODE! restarting in %RESTART_DELAY%s >> "%RESTART_LOG%"
timeout /t %RESTART_DELAY% /nobreak >nul
goto loop
