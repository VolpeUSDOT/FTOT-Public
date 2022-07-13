# FTOT Installation Guide

The following guide walks the user through the steps needed to setup and install FTOT and its dependencies including the following:

* ESRI ArcGIS Pro (versions 2.6 or higher supported).
* FTOT, including instructions on downloading the latest version from the FTOT GitHub repository, installing additional required Python modules not included with ArcGIS Pro, and downloading quick start scenario/common data
* Tableau Reader for viewing Tableau dashboard graphical outputs.  

## ESRI ArcGIS Pro (versions 2.6 or higher supported)

ESRI ArcGIS Pro Version 2.6 or higher (Geographic Information System (GIS) Program) performs the geospatial analysis elements of the tool.

While FTOT will function with any license-level of ArcGIS Pro (Basic, Standard, or Advanced) **the Advanced license level of ArcGIS Pro is highly recommended.** Unexpected behavior and/or performance issues may emerge with a Basic or Standard license, and support is more limited.

* Contact your system administrator for help in installing and licensing the software (Volpe users can contact 5-Help)
* All defaults can be accepted during installation. The process will involve installing two files—one exe file for the major version (ArcGIS Pro 2.6) and a patch/msp file representing the minor version (e.g. ArcGIS Pro 2.6.3)
* The install may take a while, so don’t do this at the end of the day.
* If you are upgrading from an earlier version of ArcGIS Pro, the upgrade will remove your old version automatically.
* After installation is complete, make sure you properly set up the license authorizing the software. Users should contact their system administrator for detailed instructions on activating the ArcGIS Pro license. Note that FTOT is compatible with any ArcGIS license level (Basic, Standard, Advanced). However, it is generally advised that users select the highest level license available, and full support and functionality for FTOT is only available with the Advanced license.


## FTOT

#### Download FTOT repository

1. To install FTOT, navigate to the FTOT repository on GitHub: [https://github.com/VolpeUSDOT/FTOT-Public](https://github.com/VolpeUSDOT/FTOT-Public)

2. Click the Code dropdown, and select "Download Zip";
![clone_snip](https://user-images.githubusercontent.com/45362680/92968787-5be4e300-f441-11ea-9c6e-2e3e37e0dbcb.png)

3. Unzip the contents into the following directory: C:\FTOT 
* Note: _on some systems the FTOT directory may need to be renamed from FTOT-Public-Master_.
* Ensure that the first layer inside C:\FTOT includes a subfolder called "program", a file named "simple_setup.bat", and two additional files named ".gitignore" and ".gitattribute" among other individual files. The contents may need to be moved into this configuration if they did not unzip this way automatically.

![ftot_dir](https://user-images.githubusercontent.com/65978091/108519340-87dd7d80-7297-11eb-8887-71676707e87b.jpg)

#### Install FTOT's Python dependencies

4. Open a command line window by clicking on the Windows Start Menu, searching for "Command Prompt" and clicking on it.
5. Click and drag the file "simple_setup.bat" from the C:\FTOT folder to the command line window, and press enter.
6. The script will create a new FTOT-specific Python 3 environment derived from ArcGIS Pro's installation of Python, and install it in C:\FTOT\python3_env. Cloning the Python 3 environment (see screenshot below) may take a few minutes.

![](https://user-images.githubusercontent.com/65978091/108519333-8744e700-7297-11eb-93bc-5e26d06ee3bb.jpg)

7. When all dependencies are installed, the command line window will print "Complete" and prompt you to press any key to exit the installation. The executable file for the newly installed environment is located at C:\FTOT\python3_env\python.exe.

Note: Steps 4-7 above need to be re-run each time you install a new version of ArcGIS Pro. **If the FTOT-specific Python 3 environment already exists**, the setup script will ask to delete the existing C:\FTOT\python3_env directory before creating a new installation.

![](https://user-images.githubusercontent.com/65978091/108556275-c50c3480-72c4-11eb-95a9-b652d31c00ad.jpg)

#### Download FTOT datasets

8. Additional scenario and transportation network data is required to run FTOT. This dataset can be downloaded directly from [here](https://effective-guide-11263414.pages.github.io/data_download.html). Only the most recent version of the zip needs to be downloaded.
9. Extract the contents to within the C:\FTOT directory on your machine. After you download and save this data, your C:\FTOT directory should contain a scenarios folder and documentation folder in addition to the existing folder and files. 

![](https://user-images.githubusercontent.com/18051014/60043529-49d92200-968e-11e9-87fd-0ecf95341035.png)

Inside the scenarios folder should be a common_data folder along with a directory containing quick start scenarios.


## Tableau Reader 

Tableau dashboard outputs will not be fully functional until you have installed Tableau or Tableau Reader. If Tableau is not available at your organization, install the free software, Tableau Reader, downloaded from here: [https://www.tableau.com/products/reader](https://www.tableau.com/products/reader).  To install the software, follow the prompted instructions. (NOTE: If you already have Tableau Desktop v2019.2 or higher installed then you can skip this step.)

#### Resolving dashboard display issues

Some users may notice display issues in FTOT's Tableau outputs where dashboard text is too big or disappears altogether. Try the following steps to resolve display issues:
- Go to the folder that contains the tableau.exe file. This file is located in a folder like C:\Program Files\Tableau\Tableau 2020.3\bin.
- Right-click on tableau.exe, open "Properties", and go to the "Compatibility" tab.
- Click the "Change high DPI settings" button
- Check the box to override high DPI scaling behavior. Set the "Scaling performed by" drop-down box to "System".
- Restart the computer to ensure settings update.

## Getting Going with FTOT

At this point, you can verify that FTOT is fully functional by running one of the pre-built quick start scenarios (for detailed instructions, consult the quick start documentation within the C:/FTOT/Documentation folder). Alternatively, consult the [Creating New Scenarios](https://effective-guide-11263414.pages.github.io/userguide/create_scenario.html) guidance or the full FTOT documentation (also within C:/FTOT/Documentation) to dive directly into FTOT and building your own scenarios. Specific workflows and strategies to help build your own GIS-based FTOT facility location data are available here: https://effective-guide-11263414.pages.github.io/userguide/add_facility.html
