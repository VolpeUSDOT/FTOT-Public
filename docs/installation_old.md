# FTOT Installation Guide for FTOT 2020.4 and earlier. These instructions are no longer maintained and may be out of date. Subsequent versions of FTOT have migrated to a dependency on ArcGIS Pro/Python 3.

The following guide walks the user through the steps needed to setup and install FTOT and its dependencies including the following:

* ESRI ArcGIS (versions 10.6.1-10.8.1 supported). Note: _FTOT is not currently compatible with any version of ArcGIS Pro_
* ESRI ArcGIS Desktop Background Geoprocessing (separate installation). 
* Additional required Python modules
* FTOT GitHub repository and quick start scenario/common data
* Tableau Reader for viewing Tableau dashboard graphical outputs.  

## ESRI ArcGIS (versions 10.6.1 - 10.8.1 supported)

ESRI ArcGIS Version 10.6.1 or higher (Geographic Information System (GIS) Program) performs the geospatial analysis elements of the tool. FTOT may run on earlier versions of ArcGIS dating back to v 10.1, but for full support and functionality v 10.6.1 or higher is recommended.

While FTOT will function with any license-level of ArcGIS (Basic, Standard, or Advanced) **the Advanced license level of ArcGIS is highly recommended. **Unexpected behavior and/or performance issues may emerge with a Basic or Standard license, and support is more limited.

Because ArcGIS is commercial, licensed software, the following installation instructions assume that A) You already have access to the software and B) You have a license to use the software.

NOTE: Some of the pictures in this document are from installing version 10.4.1 but the process is essentially the same for 10.6.1.

1. If this is the first time you are installing ArcGIS, or you are currently running version 10.1 or above, proceed to step 2. If you have ArcGIS version 10.0 or earlier uninstall it first.

2. Right click on the ArcGIS Desktop installation exe provided and select "run elevated" or "run as administrator".

![](https://user-images.githubusercontent.com/18051014/58042435-b64d8800-7b08-11e9-8a4b-3172d76735b7.png)

3. Click Next to unpack the installation files.
4. Make sure "Launch the setup program" is checked and click "Close".

![](https://user-images.githubusercontent.com/18051014/58042440-b9487880-7b08-11e9-954c-3d111b28984e.png)

5. Click next on the welcome screen that appears.

![](https://user-images.githubusercontent.com/18051014/58042446-bcdbff80-7b08-11e9-8bd8-a4e74cc14fe3.png)

6. Accept the license agreement and click "Next".

![](https://user-images.githubusercontent.com/18051014/58042448-bfd6f000-7b08-11e9-9cfd-e7cc7397ee08.png)

7. You will be presented with the options of doing either a Complete or Custom install. Select Complete.

![](https://user-images.githubusercontent.com/18051014/58042454-c2394a00-7b08-11e9-93c8-bdf63391e581.png)

8. For the "Destination Folder" and "Python Destination Folder" steps, accept the defaults and click "Next". Click "Next" again to proceed with the install. The installation may take some time, depending on your computer.
9. If a message box appears that says you need to update your cache, click ok.
10. When installation is complete, click finish.
11. If you are upgrading from ArcGIS 10.1 or later, no further action is required. Otherwise, if this is a fresh installation of ArcGIS, proceed to Step 11.
12. Up until this point the installation is very much like any other installation.  However, activating the license is the most important part!  You will be presented with a window to authorize your copy of the software (ArcGIS Administrator Wizard). Non-Volpe users should contact their system administrator for detailed instructions on activating the ArcGIS license. Note that FTOT is compatible with any ArcGIS license level (Basic, Standard, Advanced) However, it is generally advised that users select the highest level license available and full support for FTOT is only available with the Advanced license. Volpe users should select, on the left side:  "Advanced (ArcInfo) Concurrent Use". Make sure you choose Concurrent, NOT Single Use. Under Step 2 (Define a License Manger for Concurrent Use products) select "Define a License Manager now" and enter the license server provided by Gary Baker.
14. **Note** : if you closed the ArcGIS Administrator Wizard before setting up the license server or you need to change the license server you can configure this through the ArcGIS Administrator.  Navigate to the Start menu, right click on "ArcGISArcGIS Administrator", select "**run elevated**" or "**run as administrator**"  to launch the ArcGIS Administrator.  Then click the Desktop tab and set the license manager as defined by your system administrator. For full support and functionality in FTOT, the Advanced (ArcInfo) license is highly recommended.
15. You should now be able to run ArcGIS. However, see below for installing the 64-bit background geoprocessing module of ArcGIS, which will allow you to run FTOT in 64-bit.

## ESRI ArcGIS Desktop Background Geoprocessing
The user can also optionally install ArcGIS 64-bit background geoprocessing (including a 64-bit installation of Python) to allow FTOT to be run in 64-bit. Installing this will improve performance on FTOT scenarios and allow you to run larger scenarios that fail when run using the 32-bit installation of Python. This should be available from the same source where you retrieved the base installation of ArcGIS. Right click on the exe installation file and follow the prompts (similar to what you saw for the base installation) to install.

![](https://user-images.githubusercontent.com/18051014/58042465-c5ccd100-7b08-11e9-8342-d1f14007eb08.png)

## FTOT

#### Download FTOT repository

1. To install FTOT, navigate to the FTOT repository on GitHub: [https://github.com/VolpeUSDOT/FTOT-Public](https://github.com/VolpeUSDOT/FTOT-Public)

2. Navigate to the Releases section [https://github.com/VolpeUSDOT/FTOT-Public/releases](https://github.com/VolpeUSDOT/FTOT-Public/releases), click on the FTOT version you would like to download (2020.4.1 is the latest version compatible with ArcGIS/Python 2-- subsequent versions require ArcGIS Pro) and select Source Code to download a zipped copy of the release.

3. Unzip the contents into the following directory: C:\FTOT 
* Note: _on some systems the FTOT directory may need to be renamed from FTOT-Public-Master_.
* Ensure that the first layer inside C:\FTOT is a subfolder called "program", a file named 'simple_setup.bat', and two additional files named '.gitignore' and '.gitattribute' among other individual files. The contents may need to be moved into this configuration if they did not unzip this way automatically.

![ftot_dir](https://user-images.githubusercontent.com/45362680/92970347-21c91080-f444-11ea-8032-cc0d75b74265.png)

#### Install FTOT's Python dependencies

4. Open a command line window by clicking on the Windows Start Menu, searching for "Command Prompt" and clicking on it.
5. Click and drag the file 'simple_setup.bat' from the C:\FTOT folder to the command line window, and press enter.
6. When prompted in the command line window, copy and paste the full path to your Python installation into the command line, and press enter. The path you enter should have a form like C:\Python27\ArcGIS10.6\python.exe for the 32-bit version or C:\Python27\ArcGISx6410.6\python.exe for the 64-bit version. 
* Note: your version of ArcGIS may vary,for version 10.8.1, for example the path would include e.g. ArcGISx6410.8 
* Note:_ warnings about Python 2.7 being deprecated do not impact the installation of the dependencies. _ See Alternate Installation Instructions bellow if the simple_setup.bat scripts fails. 
![simple_setup.bat_snip](https://user-images.githubusercontent.com/45362680/92970083-b54e1180-f443-11ea-9976-d27babc8b196.png) 

#### Download FTOT datasets

7. There are additional scenario and transportation network data that is required in order to run FTOT. This dataset can be downloaded directly from [here](https://effective-guide-11263414.pages.github.io/data_download.html). Download the version associated with the version of FTOT you have downloaded from the GitHub repository.
8. Extract the contents to within the C:\FTOT directory on your machine. After you download and save this data, your C:\FTOT directory should contain a scenarios folder and documentation folder in addition to the existing folder and files. 

![](https://user-images.githubusercontent.com/18051014/60043529-49d92200-968e-11e9-87fd-0ecf95341035.png)

Inside the scenarios folder should be a common_data folder along with a directory containing quick start scenarios.

At this point, you can verify that FTOT is fully functional by running one of the pre-built quick start scenarios (for detailed instructions, consult the quick start documentation within the C:/FTOT/Documentation folder). However, Tableau dashboard outputs will not be fully functional until you have installed Tableau or Tableau Reader (see below for installation instructions).

## Tableau Reader 

  To view the Tableau dashboard, install the free software, Tableau Reader, downloaded from here: [https://www.tableau.com/products/reader](https://www.tableau.com/products/reader).  To install the software, follow the prompted instructions. (NOTE: If you already have Tableau Desktop v2019.2 or higher installed then you can skip this step.)


## Alternate Installation Method for Python Dependencies

If the simple_setup.bat script does not successfully run on your machine, follow these steps to manually install FTOT's dependencies:

1. Six additional Python modules which do not come with the base installation of Python accompanying ArcGIS 10.6.1 are required for running FTOT. These include Pint, PULP, LXML, NetworkX, ImageIO, and GDAL. 
2. Open a command line window by clicking on the Windows Start Menu, searching for "Command Prompt" and clicking on it.
3. For running FTOT in 32-bit, paste the following lines in the command line window and hit enter. This will install the six modules using the Python module Pip Installs Package (pip). 

C:\Python27\ArcGIS10.6\python.exe -m pip install pint

C:\Python27\ArcGIS10.6\python.exe -m pip install pulp==1.6.10

C:\Python27\ArcGIS10.6\python.exe -m pip install lxml==3.6

C:\Python27\ArcGIS10.6\python.exe -m pip install networkx

C:\Python27\ArcGIS10.6\python.exe -m pip install imageio==2.6

C:\Python27\ArcGIS10.6\python.exe -m pip install C:\FTOT\dependencies\GDAL-2.2.4-cp27-cp27m-win32.whl

4. For running FTOT in 64-bit, paste the following text in the command line window, one line at a time—press enter after pasting each line to install the module. Note that for GDAL (the last line), you will need to specify the exact path where you saved the whl file downloaded above.

C:\Python27\ArcGISx6410.6\python.exe -m pip install pint

C:\Python27\ArcGISx6410.6\python.exe -m pip install pulp==1.6.10

C:\Python27\ArcGISx6410.6\python.exe -m pip install lxml==3.6

C:\Python27\ArcGISx6410.6\python.exe -m pip install networkx

C:\Python27\ArcGISx6410.6\python.exe -m pip install imageio==2.6

C:\Python27\ArcGISx6410.6\python.exe -m pip install C:\FTOT\dependencies\GDAL-2.2.4-cp27-cp27m-win_amd64.whl
