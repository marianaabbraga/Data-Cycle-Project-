@echo off
setlocal enabledelayedexpansion

REM ============================================================
REM  Start the Data Cycle pipeline on Windows (no Docker)
REM
REM  Prerequisites:
REM    pip install -r requirements.txt
REM    prefect server start   (in a separate terminal first)
REM
REM  Edit .env.windows to match your SQL Server instance name.
REM ============================================================

cd /d "%~dp0"

REM ------------------------------------------------------------
REM  Prepare log file
REM ------------------------------------------------------------
if not exist "logs" mkdir "logs"

for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set "LDT=%%I"
set "STAMP=!LDT:~0,8!_!LDT:~8,6!"
set "LOGFILE=logs\pipeline_!STAMP!.log"

call :log "============================================================"
call :log " Data Cycle Pipeline  -  Windows startup"
call :log " Started at !DATE! !TIME!"
call :log " Log file: !LOGFILE!"
call :log "============================================================"

REM ------------------------------------------------------------
REM  Load .env.windows
REM ------------------------------------------------------------
if not exist ".env.windows" (
    call :log "[ERROR] .env.windows not found in %CD%"
    goto :fail
)

call :log "[INFO] Loading environment from .env.windows"
for /f "usebackq tokens=1,* delims==" %%A in (".env.windows") do (
    set "KEY=%%A"
    set "VAL=%%B"
    if not "!KEY!"=="" (
        set "FIRST=!KEY:~0,1!"
        if not "!FIRST!"=="#" (
            set "!KEY!=!VAL!"
        )
    )
)

REM Mirror to .env so scripts that call load_dotenv() (e.g. goldToSap.py) find it.
copy /Y ".env.windows" ".env" >nul
if errorlevel 1 (
    call :log "[WARN] Could not copy .env.windows to .env"
) else (
    call :log "[INFO] .env.windows mirrored to .env"
)

call :log ""
call :log "  Prefect API : %PREFECT_API_URL%"
call :log "  Data dir    : %DATA_DIR%"
call :log "  DB Server   : %DB_SERVER%"
call :log "  DB Name     : %DB_NAME%"
call :log "  Ollama      : %OPENAI_BASE_URL%"
call :log ""

REM ------------------------------------------------------------
REM  Activate virtualenv if present (venv / .venv / env)
REM ------------------------------------------------------------
set "VENV_ACTIVATE="
if exist "venv\Scripts\activate.bat"  set "VENV_ACTIVATE=venv\Scripts\activate.bat"
if exist ".venv\Scripts\activate.bat" set "VENV_ACTIVATE=.venv\Scripts\activate.bat"
if exist "env\Scripts\activate.bat"   set "VENV_ACTIVATE=env\Scripts\activate.bat"

if defined VENV_ACTIVATE (
    call :log "[INFO] Activating virtualenv: !VENV_ACTIVATE!"
    call "!VENV_ACTIVATE!"
    if errorlevel 1 (
        call :log "[ERROR] Failed to activate venv at !VENV_ACTIVATE!"
        goto :fail
    )
) else (
    call :log "[WARN] No venv found (looked for venv\, .venv\, env\). Using system Python."
)

REM ------------------------------------------------------------
REM  Preflight checks
REM ------------------------------------------------------------
call :log "[CHECK] Python available?"
python --version >>"!LOGFILE!" 2>&1
if errorlevel 1 (
    call :log "[ERROR] Python is not on PATH. Install it or activate your venv."
    goto :fail
)
for /f "delims=" %%V in ('python --version 2^>^&1') do call :log "[INFO] %%V"
for /f "delims=" %%W in ('python -c "import sys;print(sys.executable)"') do call :log "[INFO] Using interpreter: %%W"

call :log "[CHECK] main_flow.py present?"
if not exist "main_flow.py" (
    call :log "[ERROR] main_flow.py not found in %CD%"
    goto :fail
)

call :log "[CHECK] Prefect server reachable at %PREFECT_API_URL% ?"
python -c "import os,sys,httpx; r=httpx.get(os.environ['PREFECT_API_URL'].rstrip('/')+'/health', timeout=3); sys.exit(0 if r.status_code==200 else 1)" >>"!LOGFILE!" 2>&1
if errorlevel 1 (
    call :log "[WARN] Prefect API not reachable."
    call :log "       Start it first with:  prefect server start"
    echo.
    choice /M "Continue anyway"
    if errorlevel 2 goto :fail
) else (
    call :log "[OK]   Prefect API is up."
)

REM ------------------------------------------------------------
REM  Run the pipeline
REM ------------------------------------------------------------
call :log ""
call :log "[RUN]  python main_flow.py"
call :log "------------------------------------------------------------"

python -u main_flow.py 2>&1
set "RC=!ERRORLEVEL!"

call :log "------------------------------------------------------------"
if !RC! neq 0 (
    call :log "[ERROR] Pipeline exited with code !RC!"
    call :log "        See !LOGFILE! for details."
    goto :fail
)

call :log "[OK]   Pipeline finished successfully."
echo.
echo Press any key to close this window...
pause >nul
endlocal
exit /b 0

REM ------------------------------------------------------------
REM  Helpers
REM ------------------------------------------------------------
:log
echo %~1
>>"!LOGFILE!" echo %~1
exit /b 0

:fail
echo.
echo =========================  FAILED  =========================
echo  Log file: !LOGFILE!
echo  Press any key to close this window...
pause >nul
endlocal
exit /b 1
