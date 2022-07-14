<p align="center">
  <img src="https://user-images.githubusercontent.com/68650998/178897476-092cb9dc-0162-4bcb-9e31-7f5f252ec42b.png?raw=true"/>
</p>

FTOT is a flexible scenario-testing tool that optimizes the transportation of materials for future energy and freight scenarios. FTOT models and tracks commodity-specific information and can take into account conversion of raw materials to products (e.g., crude oil to jet fuel and diesel) and the fulfillment of downstream demand. FTOT was developed at the US Dept. of Transportation's Volpe National Transportation Systems Center.

## Getting Started:
The following steps walk the user through how to download the FTOT code and supporting materials from GitHub.

_Please note that FTOT has a required dependency of Esri ArcGIS Pro Desktop software, and Tableau Reader (or another version of Tableau) is required for accessing dashboard functionality. For further details on installing ArcGIS Pro and Tableau, consult the full installation instructions inside the FTOT User Guide._

#### Download FTOT repository

1. To install FTOT, navigate to the FTOT repository on GitHub: [https://github.com/VolpeUSDOT/FTOT-Public](https://github.com/VolpeUSDOT/FTOT-Public)

2. Click the Code dropdown, and select "Download Zip";
![clone_snip](https://user-images.githubusercontent.com/45362680/92968787-5be4e300-f441-11ea-9c6e-2e3e37e0dbcb.png)

3. Unzip the contents into the following directory: C:\FTOT 
* Note: _on some systems the FTOT directory may need to be renamed from FTOT-Public-Master_.
* Ensure that the first layer inside C:\FTOT includes a subfolder called "program", a file named "simple_setup.bat", and two additional files named ".gitignore" and ".gitattribute" among other individual files. The contents may need to be moved into this configuration if they did not unzip this way automatically.

![ftot_dir](https://user-images.githubusercontent.com/65978091/108519340-87dd7d80-7297-11eb-8887-71676707e87b.jpg)

#### Download FTOT datasets

4. Additional scenario and transportation network data is required to run FTOT. This dataset can be downloaded directly from [here](https://volpeusdot.github.io/FTOT-Public/data_download.html). Only the most recent version of the zip needs to be downloaded.

5. Extract the contents to within the C:\FTOT directory on your machine. After you download and save this data, your C:\FTOT directory should contain a scenarios folder and a documentation folder in addition to the existing folder and files. 

![](https://user-images.githubusercontent.com/18051014/60043529-49d92200-968e-11e9-87fd-0ecf95341035.png)
 
Inside the scenarios folder should be a common_data folder along with directories containing quick start scenarios and reference scenarios.

#### Install Required Programs

6. See Section 2 of the FTOT User Guide documentation for the latest detailed installation instructions. If you need to run a legacy copy of FTOT compatible with ArcGIS rather than ArcGIS Pro, consult these [separate legacy instructions](https://volpeusdot.github.io/FTOT-Public/installation_old.html).

## Next Steps:
At this point, you can verify that FTOT is fully functional by running one of the pre-built quick start scenarios (for detailed instructions, consult the quick start documentation within the C:/FTOT/documentation folder). Alternatively, consult the [Creating New Scenarios](https://volpeusdot.github.io/FTOT-Public/create_scenario.html) guidance or the FTOT User Guide (also within C:/FTOT/documentation) to dive directly into FTOT and building your own scenarios.

## Additional Information:

#### Usage:
* Usage is explained in the Quick Start and Reference Scenarios documentation found here: [Documentation and Scenario Dataset Wiki](https://volpeusdot.github.io/FTOT-Public/data_download.html)

#### Contributing: 
* Add bugs and feature requests to the Issues tab in the [FTOT-Public GitHub repository](https://github.com/VolpeUSDOT/FTOT-Public/) for the Volpe Development Team to triage.

#### Validation:
* Volpe has validated FTOT using existing datasets commonly used by public sector freight professionals. More information on the validation effort is available here. (Check back soon for link.)

## Credits: 
* Dr. Kristin Lewis (Volpe) <Kristin.Lewis@dot.gov>
* Sean Chew (Volpe)
* Olivia Gillham (Volpe)  
* Kirby Ledvina (Volpe)
* Mark Mockett (Volpe)
* Alexander Oberg (Volpe) 
* Matthew Pearlson (Volpe)
* Samuel Rosofsky (Volpe)
* Kevin Zhang (Volpe)

## Project Sponsors:
The development of FTOT that contributed to this public version was funded by the U.S. Federal Aviation Administration (FAA) Office of Environment and Energy and the Department of Defense (DOD) Office of Naval Research through Interagency Agreements (IAA) FA4SCJ and FB48CS under the supervision of FAA’s Nathan Brown and by the U.S. Department of Energy (DOE) Office of Policy under IAA VXS3A2 under the supervision of Zachary Clement. Any opinions, findings, conclusions or recommendations expressed in this material are those of the authors and do not necessarily reflect the views of the FAA nor of DOE.

## Acknowledgements:
The FTOT team thanks our beta testers and collaborators for valuable input during the FTOT Public Release beta testing, including Dane Camenzind, Kristin Brandt, and Mike Wolcott (Washington State University), Mik Dale (Clemson University), Emily Newes and Ling Tao (National Renewable Energy Laboratory), Seckin Ozkul, Robert Hooker, and George Philippides (Univ. of South Florida), and Chris Ringo (Oregon State University).

## License: 
This project is licensed under the terms of the FTOT End User License Agreement. Please read it carefully.
