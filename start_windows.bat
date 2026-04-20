@echo off
REM ============================================================
REM  Start the Data Cycle pipeline on Windows (no Docker)
REM
REM  Prerequisites:
REM    pip install -r requirements.txt
REM    prefect server start   (in a separate terminal first)
REM
REM  Edit .env.windows to match your SQL Server instance name.
REM ============================================================

REM Load variables from .env.windows
for /f "usebackq tokens=1,* delims==" %%A in (".env.windows") do (
    if not "%%A"=="" if not "%%A:~0,1%"=="#" set "%%A=%%B"
)

echo.
echo  Data Cycle Pipeline — Windows startup
echo  Prefect API : %PREFECT_API_URL%
echo  Data dir    : %DATA_DIR%
echo  DB Server   : %DB_SERVER%
echo  DB Name     : %DB_NAME%
echo  Ollama      : %OPENAI_BASE_URL%
echo.

python main_flow.py
