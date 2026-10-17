@echo off
echo Installing Python packages required by the smart card bridge...

set PYTHON_EXE=
for %%P in ("C:\Program Files\Python314\python.exe" "C:\Program Files\Python313\python.exe" "C:\Program Files\Python312\python.exe" "C:\Program Files\Python311\python.exe") do (
    if exist %%P set PYTHON_EXE=%%~P
)

if not defined PYTHON_EXE (
    echo [ERROR] Python not found.
    pause & exit /b 1
)

"%PYTHON_EXE%" -m pip install --upgrade pip
"%PYTHON_EXE%" -m pip install pyscard requests

echo.
echo [OK] Done. Verify the reader is visible:
"%PYTHON_EXE%" -c "from smartcard.System import readers; print(readers())"
pause
