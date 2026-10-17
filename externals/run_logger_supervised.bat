@echo off
setlocal enabledelayedexpansion

set SCRIPT=%~dp0logger_automate.py
set RESTART_LOG=%~dp0supervisor.log
set RESTART_DELAY=10
set MARKER=%~dp0update_pending.json
set FAILED=%~dp0update_failed.json
set BACKUP=%SCRIPT%.bak
set CONFIG=%~dp0config.json
set CONFIG_BACKUP=%CONFIG%.bak
set CONFIG_MARKER=%~dp0config_pending.json
set CONFIG_FAILED=%~dp0config_failed.json
set UPDATE_EXIT=75
set ROLLBACK_AFTER=3
set /a POST_UPDATE_FAILS=0

if defined PYTHON_EXE goto have_python
for %%P in ("C:\Program Files\Python314\python.exe" "C:\Program Files\Python313\python.exe" "C:\Program Files\Python312\python.exe" "C:\Program Files\Python311\python.exe") do (
    if exist %%P set PYTHON_EXE=%%~P
)
:have_python

if not defined PYTHON_EXE (
    echo [%date% %time%] PYTHON_NOT_FOUND >> "%RESTART_LOG%"
    exit /b 1
)

echo [%date% %time%] SUPERVISOR_START python="%PYTHON_EXE%" >> "%RESTART_LOG%"

:loop
"%PYTHON_EXE%" "%SCRIPT%"
set EXITCODE=!errorlevel!

if "!EXITCODE!"=="%UPDATE_EXIT%" (
    set /a POST_UPDATE_FAILS=0
    echo [!date! !time!] SELF_UPDATE_RESTART >> "%RESTART_LOG%"
    timeout /t 2 /nobreak >nul
    goto loop
)

echo [!date! !time!] LOGGER_EXITED code=!EXITCODE! restarting in %RESTART_DELAY%s >> "%RESTART_LOG%"

set PENDING=0
if exist "%MARKER%" set PENDING=1
if exist "%CONFIG_MARKER%" set PENDING=1

if "!PENDING!"=="1" (
    set /a POST_UPDATE_FAILS+=1
    echo [!date! !time!] POST_UPDATE_FAILURE !POST_UPDATE_FAILS! of %ROLLBACK_AFTER% >> "%RESTART_LOG%"
    if !POST_UPDATE_FAILS! GEQ %ROLLBACK_AFTER% (
        if exist "%MARKER%" (
            if exist "%BACKUP%" (
                copy /y "%BACKUP%" "%SCRIPT%" >nul
                echo [!date! !time!] SELF_UPDATE_ROLLBACK restored previous version >> "%RESTART_LOG%"
            ) else (
                echo [!date! !time!] SELF_UPDATE_ROLLBACK_IMPOSSIBLE backup missing >> "%RESTART_LOG%"
            )
            move /y "%MARKER%" "%FAILED%" >nul
        )
        if exist "%CONFIG_MARKER%" (
            if exist "%CONFIG_BACKUP%" (
                copy /y "%CONFIG_BACKUP%" "%CONFIG%" >nul
                echo [!date! !time!] CONFIG_ROLLBACK restored previous config >> "%RESTART_LOG%"
            ) else (
                echo [!date! !time!] CONFIG_ROLLBACK_IMPOSSIBLE backup missing >> "%RESTART_LOG%"
            )
            move /y "%CONFIG_MARKER%" "%CONFIG_FAILED%" >nul
        )
        set /a POST_UPDATE_FAILS=0
    )
) else (
    set /a POST_UPDATE_FAILS=0
)

timeout /t %RESTART_DELAY% /nobreak >nul
goto loop
