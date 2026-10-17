@echo off
set TASKNAME=MES_Logger_Automate

echo 1. Start
echo 2. Stop
echo 3. Status
echo 4. Delete
echo.
set /p choice=Choose (1-4): 

if "%choice%"=="1" schtasks /run /tn "%TASKNAME%"
if "%choice%"=="2" schtasks /end /tn "%TASKNAME%"
if "%choice%"=="3" schtasks /query /tn "%TASKNAME%" /fo LIST
if "%choice%"=="4" schtasks /delete /tn "%TASKNAME%" /f
pause
