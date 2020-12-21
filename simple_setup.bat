@ECHO OFF
set PYTHONDONTWRITEBYTECODE=1

REM ===============================
REM ========  SETUP ===============
REM ===============================

echo Starting installation...

REM  REQUEST USER PYTHON PATH
:PATH
echo Enter the full path to your python.exe file including the file name.
set /P PYTHON=File path:

REM Check that file exists and is a python executable
if not exist %PYTHON% (

    echo Error: file not found.
    goto :PATH)

REM Check that the file is a Python executable
if x%PYTHON:python.exe=%==x%PYTHON% (
    echo Error: file is not a Python executable.
    goto :PATH)

REM Check if 64 bit
if not x%PYTHON:x64=%==x%PYTHON% (goto 64BIT) else (goto 32BIT)

REM  SET GDAL AND PYTHON VERSION
:64BIT
echo Installing 64bit version
set GDAL=%~dp0\dependencies\GDAL-2.2.4-cp27-cp27m-win_amd64.whl
goto INSTALL

:32BIT
echo Installing 32bit version
set GDAL=%~dp0\dependencies\GDAL-2.2.4-cp27-cp27m-win32.whl
goto INSTALL


REM ===========================================
REM ======== INSTALL DEPENDENCIES =============
REM ===========================================

:INSTALL
echo Installing FTOT dependencies...
%PYTHON% -m pip install pint
%PYTHON% -m pip install pulp==1.6.10
%PYTHON% -m pip install lxml==3.6
%PYTHON% -m pip install networkx
%PYTHON% -m pip install imageio==2.6
%PYTHON% -m pip install %GDAL%

echo Complete.
pause