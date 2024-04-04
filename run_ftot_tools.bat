
@ECHO OFF
cls
set PYTHONDONTWRITEBYTECODE=1
REM   default is @ECHO OFF, cls (clear screen), and disable .pyc files
REM   for debugging REM @ECHO OFF line above to see commands
REM -------------------------------------------------

echo Launching FTOT Tools...
echo This may take a few seconds.

REM ==============================================
REM ======== ENVIRONMENT VARIABLES ===============
REM ==============================================
set PYTHON="C:\FTOT\python3_env\python.exe"
set TOOLS="C:\FTOT\program\tools\ftot_tools.py"

REM  RUN FTOT TOOLS SUITE
%PYTHON% %TOOLS% 
