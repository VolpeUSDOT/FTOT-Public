<p align="center">
  <img src="https://user-images.githubusercontent.com/68650998/178897476-092cb9dc-0162-4bcb-9e31-7f5f252ec42b.png?raw=true"/>
</p>

FTOT is a flexible scenario-testing tool that optimizes the transportation of materials for future energy and freight scenarios. FTOT models and tracks commodity-specific information and can take into account conversion of raw materials to products (e.g., crude oil to jet fuel and diesel) and the fulfillment of downstream demand. [A video providing an overview of FTOT can be found below.](#ftot-overview) FTOT was developed at the US Dept. of Transportation's Volpe National Transportation Systems Center.

## Getting Started:
The following steps walk the user through how to download the FTOT code and supporting materials from GitHub. You can also follow along with a [video tutorial](#installing-ftot) on how to install FTOT.

_Please note that FTOT has a required dependency of Esri ArcGIS Pro Desktop software, and Tableau Reader (or another version of Tableau) is required for accessing dashboard functionality. For further details on installing ArcGIS Pro and Tableau, consult the full installation instructions inside the FTOT User Guide._

#### Download FTOT repository

1. To install FTOT, navigate to the FTOT repository on GitHub: [https://github.com/VolpeUSDOT/FTOT-Public](https://github.com/VolpeUSDOT/FTOT-Public)

2. Click the Code dropdown, and select "Download Zip":
![image](https://user-images.githubusercontent.com/68650998/179521528-b99ad2aa-39f0-472a-855d-2d9e5b8d2759.png)

3. Unzip the contents into the following directory on your local machine: C:\FTOT
* Note: _On some systems the FTOT directory may need to be renamed from FTOT-Public-Master_.
* Ensure that the first layer inside C:\FTOT includes a subfolder called "program", a file named "simple_setup.bat", and two additional files named ".gitignore" and ".gitattributes" among other individual files. The contents may need to be moved into this configuration if they did not unzip this way automatically.

![ftot_dir](https://user-images.githubusercontent.com/65978091/108519340-87dd7d80-7297-11eb-8887-71676707e87b.jpg)

#### Download FTOT documentation and datasets

4. Additional transportation network data is required to run FTOT. In addition, documentation and supplementary scenario data are provided to acclimate you to running FTOT. This documentation and scenario dataset can be downloaded directly from [here](https://volpeusdot.github.io/FTOT-Public/data_download.html). Only the most recent version of the zip needs to be downloaded.

5. Extract the contents to within the C:\FTOT directory on your machine. After you download and save this data, your C:\FTOT directory should contain a scenarios folder and a documentation folder in addition to the existing folder and files.

![](https://user-images.githubusercontent.com/18051014/60043529-49d92200-968e-11e9-87fd-0ecf95341035.png)

Inside the scenarios folder should be a common_data folder along with directories containing quick start scenarios and reference scenarios.

#### Install required programs

6. The FTOT Python environment and required dependencies are installed using the "simple_setup.bat" file. See Section 2 of the FTOT User Guide documentation for the latest detailed installation instructions. If you need to run a legacy copy of FTOT compatible with ArcGIS rather than ArcGIS Pro, consult these [separate legacy instructions](https://volpeusdot.github.io/FTOT-Public/installation_old.html).

## Next Steps:
Usage of FTOT is explained in the documentation. See above for instructions for how to download the documentation. Documentation consists of four files:
* Technical documentation - description of how FTOT works, underlying data and assumptions, FTOT structures and functions
* User guide - installation instructions and details on how to customize and develop scenarios, how to create input files, and how to interpret results
* Quick start tutorial - how-to guide for running pre-built quick start scenarios
* Reference scenarios documentation - how-to guide for running reference scenarios to demonstrate FTOT functionalities

At this point, you can verify that FTOT is fully functional by running one of the pre-built quick start scenarios. For detailed instructions, consult the Quick Start Tutorial within the C:/FTOT/documentation folder. Alternatively, consult the FTOT User Guide (also within C:/FTOT/documentation) or the [Creating New Scenarios](https://volpeusdot.github.io/FTOT-Public/create_scenario.html) guidance to dive directly into FTOT and building your own scenarios. In addition, the FTOT Team has developed a [Minimum Data Requirements](https://github.com/VolpeUSDOT/FTOT-Public/blob/github_pages/docs/FTOT%20International%20Data%20Requirements_July2023.pdf) document that provides a checklist of inputs that a user should collate in preparation for running an FTOT scenario. You can also follow along with the [video tutorial](#running-your-first-ftot-scenario) for running the first Quick Start scenario. 

## Users Group:
Every quarter, the FTOT users group is notified when the most recent FTOT public release is available for download.
* If you would like to be added to the email distribution list for FTOT users, please send an email to <Kristin.Lewis@dot.gov>.
* The users group is invited to attend a presentation on the newest changes included in the most recent public release. Slides from the latest presentation (FTOT 2023.3 release) are available [here](https://github.com/VolpeUSDOT/FTOT-Public/blob/github_pages/docs/FTOT-2023-3-User-Group.pdf).

## Additional Information:

#### Contributing:
* Add bugs and feature requests to the Issues tab in the [FTOT-Public GitHub repository](https://github.com/VolpeUSDOT/FTOT-Public/issues) for the Volpe Development Team to triage.

#### Validation:
* Volpe has validated FTOT using existing datasets commonly used by public sector freight professionals. More information on the informal validation effort is available [here](https://github.com/VolpeUSDOT/FTOT-Public/blob/github_pages/docs/FTOT_Validation_2%20Pager_2022_2_final.pdf).

## Resilience Tools:

#### FTOT-Resilience-Link_Removal:
[FTOT-Resilience-Link_Removal](https://github.com/VolpeUSDOT/FTOT-Resilience-Link_Removal) is a modification of the base FTOT program to assess the resilience of an FTOT optimal solution to disruption. The link removal resiliency testing process works as follows: (1) a baseline FTOT run is completed using a modified version of FTOT which retains additional network information, (2) network edges are ranked by importance (the default importance metric is betweenness-centrality, but the user can also specify an importance metric), (3) disruptions are applied by removing edges from the optimal solution, (4) new optimal solutions are calculated for the disrupted network, (5) total scenario costs are calculated and compared. The importance calculations are for the road network only.

To install the FTOT-Resilience-Link_Removal code, follow the instructions available [here](https://github.com/VolpeUSDOT/FTOT-Resilience-Link_Removal#install-conda) on the GitHub repository for the modified FTOT code and the rank and removal code. The rank and removal code are run through a Jupyter notebook, and generate an interactive report of the resilience results.

#### FTOT-Resilience-Supply_Chain:
[FTOT-Resilience-Supply_Chain](https://github.com/VolpeUSDOT/FTOT-Resilience-Supply_Chain) is a modification of the base FTOT program to support analysis of supply chain resilience. The supply chain resilience assessment includes two parts: integrated risk assessment to capture the combined effects of multiple risk factors on supply chain performance, and resilience assessment to calculate the long-term supply chain resilience in planning horizon. The supply chain methodology and modifications to the FTOT code were developed at Washington State University (WSU).

To install the FTOT-Resilience-Supply_Chain code, follow the instructions available [here](https://github.com/VolpeUSDOT/FTOT-Resilience-Supply_Chain#installation) on the GitHub repository for the modified tool. After installing, follow the instructions in the “Running the Scenario” section to run the two batch files to generate input data and analyze the resilience of the supply chain. The repository also contains two additional [documents](https://github.com/VolpeUSDOT/FTOT-Resilience-Supply_Chain/tree/main/docs) developed by the WSU team about the methodology used to create input data for the tool as well as the analysis performed in the tool.

## Video Series:

#### FTOT Overview:
{% include youtube.html id="wm0f4zPe0XE" %}
<br>
<br>

#### Installing FTOT:
{% include youtube.html id="VXay2v5KguA" %}
<br>
<br>

#### Running Your First FTOT Scenario:
{% include youtube.html id="P__8oRHxuSc" %}
<br>
<br>

#### Customizing Your Own FTOT Scenario:
{% include youtube.html id="JmDtU_cKI1g" %}
<br>
<br>

#### Preparing and Using Your Custom FTOT Network:
{% include youtube.html id="48eWacfT8al" %}
<br>
<br>

## Credits:
* Dr. Kristin Lewis (Volpe) <Kristin.Lewis@dot.gov>
* Sean Chew (Volpe)
* Olivia Gillham (Volpe)
* Kirby Ledvina (Volpe)
* Mark Mockett (Volpe)
* Alexander Oberg (Volpe)
* Matthew Pearlson (Volpe)
* Tess Perrone (Volpe)
* Samuel Rosofsky (Volpe)
* Kevin Zhang (Volpe)

## Project Sponsors:
The development of FTOT that contributed to this public version was funded by the U.S. Federal Aviation Administration (FAA) Office of Environment and Energy and the Department of Defense (DOD) Office of Naval Research through Interagency Agreements (IAA) FA4SCJ and FB48CS-FB48CY under the supervision of FAA’s Nathan Brown and by the U.S. Department of Energy (DOE) Office of Policy under IAA VXS3A2 under the supervision of Zachary Clement. Any opinions, findings, conclusions or recommendations expressed in this material are those of the authors and do not necessarily reflect the views of the FAA nor of DOE.

## Acknowledgements:
The FTOT team thanks our beta testers and collaborators for valuable input during the FTOT Public Release beta testing, including Dane Camenzind, Kristin Brandt, and Mike Wolcott (Washington State University), Mik Dale (Clemson University), Emily Newes and Ling Tao (National Renewable Energy Laboratory), Seckin Ozkul, Robert Hooker, and George Philippides (Univ. of South Florida), and Chris Ringo (Oregon State University).

## License:
This project is licensed under the terms of the FTOT End User License Agreement. Please read it carefully.
