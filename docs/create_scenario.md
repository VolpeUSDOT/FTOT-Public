_Note: FTOT’s Quick Start and Reference Scenario datasets include several ready-to-use example scenarios and input files. See [Documentation and Scenario Datasets](https://volpeusdot.github.io/FTOT-Public/data_download.html) for the latest scenario and documentation files._

The [FTOT User Guide documentation](https://volpeusdot.github.io/FTOT-Public/data_download.html) contains details on how to set up an FTOT scenario. The reference scenarios include examples of how to configure different FTOT features and can serve as templates for creating required and optional input files and scenario configuration. Sections of this documentation have been excerpted below for user convenience; see full documentation for further details.

# Getting Started
Each unique FTOT scenario requires a batch (.bat) file, a scenario XML file, and an `input_data` directory with the relevant facility-commodity input CSV files. To ensure you have all necessary files to run a new FTOT scenario, the user can
1. Copy over a Quick Start or Reference Scenario folder based on the intended supply chain structure of the user's FTOT scenario, and then
2. Customize the starter files according to the user's specific scenario needs.

For example, use files from Quick Start 2 for a scenario where freight flows from a raw material producer (rmp) to a processor (proc) to a destination (dest); use Reference Scenario 2 as a template for a candidate processor generation scenario. To then adapt the starter scenario, the user will need to update the scenario XML at a minimum. See the **Checklist of Critical Items to Update** section below for the minimum set of items to edit. Most custom scenarios will also require creating new facility-level GIS data and supply chain data (e.g., facility-commodity CSV files). Finally, customization of the scenario's batch file will also be necessary. See the FTOT User Guide for more details.

# Checklist of Critical Items to Update
**Batch file**
* `set XMLSCENARIO={}` - Replace brackets with full file path to scenario XML file.

**Scenario XML file**
* `Scenario_Name`
* `Scenario_Description`
* `RMP_Commodity_Data` - Confirm file path.
* `Destinations_Commodity_Data` - Confirm file path.
* `Rail_Density_Code_{NUM}_Weight` - Please be aware that some Quick Start and Reference Scenarios do NOT use the default rail weights detailed in the documentation, in order to encourage flows on road. Consider changing these weights back to their defaults (1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 10.0) if using a Quick Start or Reference Scenario folder as a template.

Other optional items to update:
* `Disruption_Data` - Change file path.
* `Base_RMP_Layer` - Change GDB layer if using your own GDB.
* `Base_Destination_Layer` - Change GDB layer if using your own GDB.
* `Base_Processors_Layer` - Change GDB layer if using your own GDB.
* `Processors_Commodity_Data` - Change file path.
* `Processors_Candidate_Commodity_Data` - Change file path.
* `Schedule_Data` - Change file path.
* `Commodity_Mode_Data` - Change file path.

# Updating FTOT Scenarios from Older Release Versions
The FTOT team often introduces optional XML elements as new features are added to FTOT, so older versions of Quick Start and Reference Scenario XML files may not have all available scenario elements. The quickest way to update an existing, older scenario is to manually add any additional elements needed. For reference, the master XML template saved at `C:/FTOT/program/lib/v6_temp_Scenario.xml` contains all required and optional XML elements. 

When updating the XML, the user does NOT need to include any comments from the original XML or in the template XML, but the elements do need to be in the same order as seen in the template.

# Tools for Updating XML Files
Several tools can help the user efficiently and thoroughly update their new scenario XML (and to a lesser extent, the batch files). Text file comparisons can help the user recognize items that still need updating. Consider comparing new scenario files to the following:

* **FTOT’s template XML**, located in the `C:/FTOT/program/lib` sub-directory. This file (named `v6_temp_Scenario.xml`) can help the user identify missing scenario elements or differences from the Quick Start or Reference Scenario defaults. The file is also used as the template for an FTOT Tool that creates new scenario files from scratch.
* **Previously-created versions** of the scenario being run, including any template Quick Start or Reference Scenario files. This comparison is especially helpful to verify scenario variations and new file paths.

Listed below are just a few tools to update scenario files:

* **Text editor.** Any text editor, such as Windows Notepad, can be used to directly edit XML element values (e.g., file paths).
* **BeyondCompare.** Using the [BeyondCompare](https://www.scootersoftware.com/) application, the user can "copy section to right/left" using arrows in the side bar and highlight differences in text (red lines) and in spacing (purple lines). Make sure to separate out distinct sections before copying over to avoid unintentional overwriting. Note: Requires a paid license after a trial period.
* **FTOT Tools.** The XML tool in ftot_tools.py can create a new, blank scenario XML or add missing elements to an existing XML. Make sure to manually check all element values if using this tool. See the FTOT User Guide for guidance on running FTOT Tools.
* **Visual Studio.** In Visual Studio, go to Tools > Command Line > Developer Command Prompt, and in the window that appears enter `devenv.exe /diff list1.txt list2.txt` replacing list1.txt and list2.txt with the XML or other files the user wishes to compare. The user can drag and drop files to paste in the full file path. A comparison window will open in Visual Studio. See a related Stack Exchange post [here](https://stackoverflow.com/questions/13752998/compare-two-files-in-visual-studio).

# Facility Location GIS Data
FTOT requires GIS-based input datasets containing the facility names and locations of raw material producers (rmp), processors (proc), and destinations (dest). There are many sources of GIS data an FTOT user can leverage to represent the locations of a scenario's facilities. FTOT comes with one preexisting facility location geodatabase included in FTOT's common data subdirectory--point-based representations of every county in the United States. If FTOT was installed in the default location, the county-based facility location geodatabase is stored in the following location: C:\FTOT\scenarios\common_data\facilities\counties.gdb. Users basing facility locations on the default data that come packaged with FTOT do not need to move or copy over those data into a new scenario directory; they can continue to be read from their default location (in the `C:/FTOT/scenarios/common_data/facilities` sub-directory).

If the user wants to create their own customized facility location GIS data, refer to the FTOT User Guide.
