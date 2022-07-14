_Note: FTOT’s Quick Start datasets include several ready-to-use example scenarios and input files. See [Documentation and Scenario Datasets](https://volpeusdot.github.io/FTOT-Public/data_download.html) for the latest scenario and documentation files._

# Getting Started
Each unique FTOT scenario requires a scenario XML file, a batch (.bat) file, and an `input_data` directory with the relevant facility-commodity input CSV files. To ensure you have all necessary files to run a new FTOT scenario, you can
1. Copy over a Quick Start folder (either a `Default` or `Exercise` folder) based on the intended supply chain structure of your FTOT scenario, and then
2. Customize the starter files according to your specific scenario needs.

For example, use files from Quick Start #2 for a scenario where freight flows from a raw material producer (rmp), to a processor (proc), to a destination (dest); or use Reference Scenario #2 as a template for a candidate processor generation scenario. To then adapt the starter scenario, you will need to update the scenario XML and batch files at a minimum. See the **Checklist of Items to Update** section at the bottom of this page for the minimum set of items to edit. Note that facility-commodity CSV files located in a scenario directory's `input_data` folder should be updated as described in the technical documentation.

The Quick Start datasets, Quick Start documentation, and overall FTOT technical documentation can all be downloaded [here](https://volpeusdot.github.io/FTOT-Public/data_download.html).

# GIS-Based Facility Location Data
GIS-based facility location data are also required for every scenario. However, if you are basing your facility locations on the default data that come packaged with FTOT (county-based populated place centroids), you do not need to move or copy over those data into your new scenario directory as they can continue to be read from their default location (in the `C:/FTOT/scenarios/common_data/facilities` sub-directory).

If you want to create your own customized facility location GIS data, refer to the FTOT User Guide.

# Tools for Updating Files
Several tools can help you efficiently and thoroughly update your new scenario XML and batch files. Text file comparisons can help you recognize items that still need updating. Consider comparing your new scenario files to the following:

* **FTOT’s template XML**, located in the `C:/FTOT/program/lib` sub-directory. This file (named `v6_temp_Scenario.xml`) can help you identify missing scenario elements or differences from the Quick Start defaults. The file is used as the template for an FTOT Tool that creates new scenario files from scratch.
* **Previously-created versions** of the scenario being run, including any template Quick Start files. This comparison is especially helpful to verify scenario variations and new file paths.

Listed below are just a few tools to update scenario files: 

* **Text editor.** You can use any text editor, such as Windows Notepad, to directly edit XML element values (e.g., file paths).
* **BeyondCompare.** Using the [BeyondCompare](https://www.scootersoftware.com/) application, you can "copy section to right/left" using arrows in the side bar and highlight differences in text (red lines) and in spacing (purple lines). Make sure to separate out distinct sections before copying over to avoid unintentional overwriting. Note: Requires a paid license after a trial period.
* **FTOT Tools.** The XML tool in ftot_tools.py can create a new, blank scenario XML or add missing elements to an existing XML. Make sure to manually check all element values if using this tool. See the FTOT documentation for guidance on running FTOT Tools.
* **Visual Studio.** In Visual Studio, go to Tools > Command Line > Developer Command Prompt, and in the window that appears enter `devenv.exe /diff list1.txt list2.txt` replacing list1.txt and list2.txt with the XML or other files you wish to compare. You can drag and drop files to paste in the full file path. A comparison window will open in Visual Studio. See a related Stack Exchange post [here](https://stackoverflow.com/questions/13752998/compare-two-files-in-visual-studio).

# Checklist of Items to Update
**Batch file**
* `set XMLSCENARIO={}` - Replace brackets with full file path to scenario XML file.

**Scenario XML file**
* `Scenario_Name`
* `Scenario_Description`
* `RMP_Commodity_Data` - Confirm file path.
* `Destinations_Commodity_Data` - Confirm file path.
* `Rail_Density_Code_{NUM}_Weight` - Please be aware that some Quick Start scenarios do NOT use the default rail impedances detailed in the documentation. Consider changing these impedances back to their defaults (1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 10.0) if using a Quick Start folder as a template.

_Other optional items to update:_
* `Disruption_Data` - Change file path.
* `Base_RMP_Layer` - Change GDB layer if using your own GDB.
* `Base_Destination_Layer` - Change GDB layer if using your own GDB.
* `Base_Processors_Layer` - Change GDB layer if using your own GDB.
* `Processors_Commodity_Data` - Change file path.
* `Processors_Candidate_Commodity_Data` - Change file path.
* `Schedule_Data` - Change file path.
* `Commodity_Mode_Data` - Change file path.

# Updating FTOT Scenarios from Older Release Versions
The FTOT team often introduces optional XML elements as new features are added to FTOT, so older versions of Quick Start XML files may not have all available scenario elements. The quickest way to update an existing, older scenario is to manually add any additional elements needed. For reference, the master XML template saved at `C:/FTOT/program/lib/v6_temp_Scenario.xml` contains all required and optional XML elements. 

When updating the XML, you do NOT need to include any comments from the original XML or in the template XML, but the elements do need to be in the same order as seen in the template.
