@echo off
setlocal
set PYEXE=.venv\Scripts\python.exe
if not exist "%PYEXE%" (
  echo [error] %PYEXE% not found.
  echo Create it with: python -m venv .venv
  exit /b 1
)
"%PYEXE%" scripts\cli.py setup
"%PYEXE%" scripts\report_windows.py --rows 500000 --parts 16 --update-ratio 0.5 --workers 4 --chunk 200000 --windows 1,4,16,64 --out report_windows.md --no-exact
echo ===== Report ready: report_windows.md =====
type report_windows.md
endlocal
