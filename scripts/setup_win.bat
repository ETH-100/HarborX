@echo off
REM Windows quick setup
py -3 -m venv .venv
call .venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
echo.
echo Done. To activate later, run:  call .venv\Scripts\activate
