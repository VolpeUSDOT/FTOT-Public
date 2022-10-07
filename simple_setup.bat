@ECHO OFF
set PYTHONDONTWRITEBYTECODE=1

set CONDA="%PROGRAMFILES%\ArcGIS\Pro\bin\Python\Scripts\conda.exe"
IF NOT EXIST %CONDA% (
    SET CONDA="%LOCALAPPDATA%\Programs\ArcGIS\Pro\bin\Python\Scripts\conda.exe"
)
set NEWENV="C:\FTOT\python3_env"
set NEWPYTHON="C:\FTOT\python3_env\python.exe"

echo Starting FTOT installation

echo Checking if directory %NEWENV% already exists
IF EXIST %NEWENV% (
    echo Warning: directory %NEWENV% already exists. If you have previously installed FTOT, this is expected.
    echo Continuing will delete the existing FTOT Python environment and ensure that the new environment
    echo is based on the latest FTOT requirements and your current version of ArcGIS Pro.
    echo If you do not want to proceed, close the window to exit.
    pause
    rmdir /q /s %NEWENV%
    echo Deleting existing directory
)

echo Cloning ArcGIS Pro Python environment. This may take a few minutes...
%CONDA% create --clone arcgispro-py3 --prefix %NEWENV%
echo New Python executable at: %NEWPYTHON%

echo Installing dependencies
%NEWPYTHON% -m pip install --no-warn-script-location pint
%NEWPYTHON% -m pip install --no-warn-script-location pulp
%NEWPYTHON% -m pip install --no-warn-script-location lxml
%NEWPYTHON% -m pip install --no-warn-script-location imageio==2.9.0

echo Complete.
pause
