@echo off
setlocal
set PYEXE=.venv\bench\python.exe

if not exist "%PYEXE%" (
  echo [setup] creating venv at .venv
  python -m venv .venv
  if errorlevel 1 (
    echo [error] failed to create venv
    exit /b 1
  )
  ".venv\bench\python.exe" -m pip install --upgrade pip
  ".venv\bench\python.exe" -m pip install -r requirements.txt
)

echo [python] using: %PYEXE%
"%PYEXE%" bench\report.py --rows 500000 --parts 16 --update-ratio 0.5 --workers 4 --chunk 200000 --windows 1,4,16,64 --out report_windows.md --no-exact

echo ===== Report ready: report_windows.md =====
type report_windows.md
endlocal
