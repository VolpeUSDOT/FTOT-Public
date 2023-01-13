# FTOT Change Log

## v2022_4
The FTOT 2022.4 public release includes updates related to candidate generation using network density reduction (NDR), Tableau and reporting enhancements, detailed reporting on shortest paths between origin and destination facilities, and other minor bug fixes. The following changes have been made:
- Implements a new method for candidate generation scenarios through network density reduction. The new method incorporates NetworkX shortest path algorithms into (1) selection of candidate processor locations and (2) calculation of the optimal routing solution. Running candidate generation using NDR significantly decreases runtime by pre-solving the network for shortest paths; see documentation for details on methodology and known issues.
- Adds metrics for processor capacity and utilization to the text report and CSV report.
- Updates the Tableau workbook with new information on facility sizes, processor efficiencies and utilizations, and other minor aesthetic improvements.
- Expands reporting on shortest path routes between facilities in the "all_routes" CSV report (formerly "optimal_routes") and a new Tableau routes dashboard when NDR is enabled. Reporting allows users to analyze routes across different scenarios, modes, facility pairs, and optimal vs non-optimal routes. The scenario comparison dashboard has also been updated to reflect these enhancements.
- Corrects a minor bug on converting candidate processor build costs to default units.
- Replaces "kgal" with "thousand_gallon" as default liquid units in the template and example scenario XML files.

See documentation files for additional details.

## v2022_3
The FTOT 2022.3 public release includes updates related to the FTOT multimodal GIS network, Tableau and reporting enhancements, user experience (including video tutorials and supplemental tools), and resilience analysis tools. The following changes have been made:
- Refreshes the default continental United States multimodal network accompanying FTOT for 2022. Key highlights include a more detailed road network, updated pipeline tariff data, and rail and waterway networks have also been updated. For capacity-constrained scenarios, continue to use the older (2019) version of the network.
- Updates the Tableau workbook. The new workbook replaces the “Material Moved” graphs on the “By Commodity & Mode” dashboard with “Miles” graphs to show the miles of network used in the optimal solution. The workbook includes additional minor bug fixes and design enhancements.
- Fixes processor available capacity and utilization reporting within text report. Processor conversion ratios are scaled correctly for capacity; scenarios with unconstrained capacity are now noted in output tables.
- Clarifies reporting on total flow and vehicles. For the scenarios that use intermodal facilities, FTOT now (1) adds a “includes multimodal flows” note to the “total_flow” measure in the CSV report and (2) suppresses modal reporting on the “total_flow” metric to prevent misattributing flows to a particular mode.
- Releases the “Installing FTOT” and “Running Your First Scenario” instruction videos as part of a new FTOT video tutorial series. Tutorial videos are posted to the [FTOT landing page](https://volpeusdot.github.io/FTOT-Public/#video-series).
- Updates the suite of supplementary FTOT tools run through ftot_tools.py. Users can run these tools to help set up their own FTOT scenario by (1) creating new scenario XML files and updating old ones, (2) creating batch files, (3) creating facility-commodity input CSV files, (4) replacing XML elements across all XML files in a directory.
- Allows for either absolute file paths or relative file paths when specifying input CSV locations in the scenario XML. Relative paths should be expressed relative to the scenario XML file location.
- Resolves bug associated with running FTOT on a Basic/Standard license of ArcGIS Pro.
- Updates resilience tool repositories for [link removal resilience testing](https://github.com/VolpeUSDOT/FTOT-Resilience-Link_Removal) and [supply chain resilience](https://github.com/VolpeUSDOT/FTOT-Resilience-Supply_Chain).

See documentation files for additional details.

## v2022_2
The FTOT 2022.2 public release includes significant restructuring of user documentation and adds a landing page for the GitHub repository. There are four new documentation files:  
- Technical documentation - description of how FTOT works, underlying data and assumptions, and FTOT structures and functions.  
- User guide - instructions and details on how to customize and develop scenarios, how to create input files, and how to interpret results.  
- Quick start tutorial - how-to guide for running pre-built quick start scenarios; also helps ensure or check that FTOT has been installed correctly.  
- Reference scenarios documentation - how-to guide for running advanced reference scenarios to demonstrate specific FTOT functions.  

The 2022.2 release also updates FTOT reporting, adds user-specified minimum processor capacity, and corrects facility summary post-processing to account for multi-commodity inputs. The following changes have been made:
- Reorganized text report and added a scenario summary section, clarified names of summary metrics, and added processor build costs.
- Added option for user-specified minimum processor capacity in processor facility-commodity input file. Minimum capacity can be specified in an optional column; if not specified, minimum capacity defaults to 1/2 of maximum processor capacity.
- Updated Tableau workbook with new reporting elements, bug fixes, and other enhancements. 
- Fixed bug related to incorporating OC step logging into reports.
- Corrected calculation of commodity flow and facility utilization for facilities with multi-commodity inputs.

Finally, the 2022.2 release includes publication of a new repository, FTOT-SCR, to support analysis of supply chain resilience. The supply chain resilience assessment includes two parts: integrated risk assessment to capture the combined effects of multiple risk factors on supply chain performance, and resilience assessment to calculate the long-term supply chain resilience in planning horizon. The supply chain methodology and modifications to the FTOT code were developed at Washington State University (WSU).  The FTOT-SCR fork is available at [https://github.com/mark-mockett/FTOT-SCR/tree/FTOT-SCR](https://github.com/mark-mockett/FTOT-SCR/tree/FTOT-SCR).

See documentation files for additional details.

## v2022_1
The 2022.1 release provides updates related to emissions reporting, build costs for processors, and network density reduction (NDR). Supplementary FTOT-related resources and tools have also been expanded or updated. The following changes have been made:
- Expanded emissions report: Default emission factors for non-CO2 pollutants (CO, CO2e, CH4, N2O, PM10, PM2.5, and VOC) have been added for rail and water modes. Previously default emission factors were provided for just road. See Section 5.7 for more information.
- Commodity density input file: Users can include a new input file with a list of commodity densities to be used in emissions calculation. The pre-existing density conversion factor in the XML will be used as the default value in the absence of commodity-specific density information. See Section 5.2 for more information.
- Processor build costs: The user can now specify pre-defined candidate processors with unique build costs, in addition to or instead of FTOT-generated candidates. These pre-defined candidates are input as if they were existing processor facilities, but for the inclusion of a build cost that is added to the total scenario if they are used at all.
- Network Density Reduction (NDR) bug fixes: FTOT now calls the update_ndr_parameter() method in FTOT steps O, P, and D in addition to the original call in the G step to avoid a case where NDR is programmatically turned off in one step but not the others. Also, a SQL filter was added to prevent FTOT from trying to calculate the shortest path for an origin-destination pair that does not exist in the subgraph of accepted modes for a commodity.
- Changes to the scenario XML schema and templates: The scenario XML schema, template file, and Quick Start XMLs have been modified with (1) the addition of an optional element for a commodity densities input file and (2) updated default CO2 emission factors for rail and water modes.
- FTOT setup script: To ensure successful installation of FTOT’s Python dependencies, the file simple_setup.bat now specifies version 2.9.0 for the imageio package.
- New Resources on FTOT-Public Wiki: The “Guidance on Creating New Scenarios” Wiki page summarizes important steps and helpful tools for creating and updating FTOT scenarios. The “Adding Segments to the FTOT Network” Wiki page walks FTOT users through the process of adding segments to customize the existing FTOT Multimodal network. As part of this update, Section 3.2 of the main documentation was also updated to provide further information on the default FTOT network.
- Link Removal Tool: The Network Resiliency and Link Removal Tool is a separate tool that assesses network resilience by ranking and sequentially removing links in the optimal solution and rerunning scenarios in FTOT to evaluate the resilience of optimal solution costs in the face of disruption. The Link Removal tool has been updated to Python 3 and is now based on FTOT 2021.4. The original version of the tool was based on FTOT 2020.3.

## v2021_4
The 2021.4 release provides updates related to emissions reporting, transport costs, as well as enhancements to output files and runtime. The following changes have been made:
- Expanded emissions report: Users can now generate a separate emissions report with total emissions by commodity and mode for seven non-CO2 pollutants (CO, CO2e, CH4, N2O, PM10, PM2.5, and VOC). For this feature, FTOT includes a set of default non-CO2 emission factors for road which users can update. The CO2 emissions factor is still reported in the main FTOT outputs and the emissions factor can be adjusted in the XML.
- User-adjustable density conversion factor: An optional density conversion factor has been added to the scenario XML for calculating emissions for liquid commodities on rail, water, and pipeline modes.
- Additions to the scenario XML schema: (a) includes optional elements related to emissions reporting and a density conversion factor, (b) updates base transport costs for road, rail, and water modes based on most recent data available from BTS.
- Reporting outputs: The reports folder generated by FTOT has been streamlined. The primary FTOT outputs (excluding maps) have been consolidated in a timestamped reports folder for each scenario run. This includes the Tableau dashboard, the CSV file report, the main text report, and any supplementary text reports (e.g., artificial links summary, optimal routes, expanded emissions reporting).
- Runtime improvements: Runtime of the post-processing step in FTOT has been significantly reduced for scenarios with a large number of raw material producer facilities.

## v2021_3
The 2021.3 release adds functionality enhancements related to the scenario XML file and artificial links, along with more detailed documentation and user exercises for importing facility location data, capacity scenarios, and artificial links. The following changes have been made:
- A major update to the scenario XML schema: (a) includes optional elements related to disruption, schedules, commodity mode, and network density reduction by default, (b) allows users to customize units for more elements, (c) removes unused elements, and (d) adds elements for rail and water short haul penalties (previously hardcoded and not customizable). This is a breaking change—previous versions of the XML schema will no longer work with FTOT version 2021.3.
- Additional reporting of artificial links for each facility and mode has been added. A new artificial_links.csv file stored in the debug folder of the scenario reports the length in miles of each artificial link created, or reports “NA” if no artificial link was able to connect the facility to the modal network. Stranded facilities (e.g., those not connected to the multimodal network at all) are not reported in this file.
- New Quick Start examples have been added to walk the user through FTOT scenarios involving capacity availability and artificial link distances.
- The default truck CO2 emission factors and fuel efficiency have been updated based on data from EPA’s MOVES3 model.
- The Tableau dashboard has been updated. The “Supply Chain Summary” page now displays a more complete picture of the scenario’s input data with the capability to scale and filter facilities by commodity. Facility tooltips also list the user-specified commodities and quantities for a facility. In addition, minor enhancements to the dashboard include (a) corrections to other legend scaling and tooltip displays, (b) relabeling of Scenario Cost to Dollar cost on the “By Commodity & Mode” page to match FTOT reporting, and (c) a reordering of the FTOT steps presented on the “Runtimes” page.
- Fixed a bug in which the value for max_processor_input in the processors csv file was assigned the default scenario units instead of the processor input file’s units.
- Updated the bat tool: (a) added FTOT’s pre-set file locations to reduce the user inputs requested and (b) updated the v5 .bat file naming convention to v6.

## v2021_2
The 2021.2 release adds multiple functionality enhancements related to disruption, vehicle types, and routing. The following changes have been made:
- New functionality allowing the user to fully disrupt FTOT network segments (make segments unavailable) by leveraging a configurable disruption csv.
- An FTOT supporting tool automating the process of generating a disruption csv based on gridded exposure data (e.g., flooding data, HAZUS scenario outputs) has also been added.
- Ability to create custom vehicle types and assign them to individual commodities to enhance reporting. This feature introduces a new vehicle_types.csv file within FTOT and updates the functionality of the optional commodity_mode.csv input file.
- The network density reduction (NDR) functionality for identifying shortest paths (introduced in the 2020.4 release) has been refined to consider both phase of matter and commodity. With this additional development, NDR provides the optimal solution for all cases for which the functionality is allowed. The network presolve step for NDR remains disabled by default and is not compatible with candidate generation, the inclusion of modal capacity, and maximum allowable transport distance scenarios.
- Additional reporting of routes (shortest paths) included in the optimal solution has been added for scenarios where NDR has been enabled. A new optimal_routes.csv file stored in the debug folder of the scenario reports the starting and ending facility of each route, along with its routing cost and length in miles.
- The default background map for the Tableau dashboard has been changed from Dark to Streets to make it easier for the user to identify the local geography and path of the optimal solution.

## v2021_1
The 2021.1 release finalizes the transition to Python 3, migrating from a dependency on ArcGIS 10.x to a dependency on ArcGIS Pro. The following changes have been made:
-	Simplified and streamlined setup/installation instructions.
-	Creation of a dedicated FTOT Python 3 environment within the FTOT installation directory.
-	Due to the migration from ArcGIS to ArcGIS Pro, there is an updated backend process to outputting FTOT scenario maps. The ftot_maps.mxd mapping template provided with FTOT has been replaced with an ftot_maps.aprx project template compatible with ArcGIS Pro.

## v2020_4_1
The 2020.4.1 service pack release adds the ability to turn the network presolve step included in the
2020.4 release on or off through an optional network density reduction (NDR) function. The functionality 
is controlled by the NDR_On parameter in the scenario configuration file; the default is for the step 
to be disabled. Including the parameter in the scenario configuration file and setting it to True enables 
the network presolve step described below in v2020_4. An exercise has been added to Quick Start 6 to 
demonstrate the functionality and use cases of the NDR function. Additionally, the service pack release 
includes improvements to the shortest path algorithm used by the network presolve step. Finally, small 
fixes to handle unit conversion were made.

## v2020_4
The 2020.4 release includes a network presolve step that identifies links used in the shortest path
between sources and targets (e.g. RMP‐>Dest, RMP‐>Proc, Proc‐>Dest). It is enabled by default for
all scenarios except candidate generation and capacity. The presolve reduces the complexity of the
optimization and enhances performance. Mutli‐commodity input and Processor facilities were
enhanced to constrain the maximum capacity of the facility. Additionally, the release also includes
changes throughout for forward compatibility with Python 3.x in the future. Note however, that
2020.4 still requires Python 2.7 because of the ArcPy module dependency. Finally, several
miscellaneous fixes to improve the quality of the code base.

GENERAL
- Removed remaining references to xtot_objects module.
- Added Python3 forward compatibility and code cleanup
- Added compatibility for ArcGIS basic license for certain operations in ftot.py,
ftot_postprocess.py, and ftot_routing.py
- The network is presolved for each appropriate source and target (e.g. Origin‐Destination pair)
using the shortest path algorithm in the NetworkX module. The links used in the shortest path
are stored in the shortest_path table in the main.db and used to speed up edge creation in the
O1 step and solving the optimization problem in the O2 step.

PULP MODULE
- The optimization problem only includes edges created for network links identified in the
shortest path network presolve, except for scenarios that include candidate generation or
capacity.
- Multi‐commodity inputs for processors are properly constrained using a max_processor_input
field in the proc.csv input files.

TABLEAU DASHBOARDS
- Commas are removed from the scenario name to prevent errors when parsing the Tableau
report CSV file.

MAPS
- No changes to report in 2020.4

## v2020_3
The 2020.3 release includes a new setup script for installing FTOT and additional software dependencies, performance enhancements to reduce the memory footprint of scenarios at run time, and several miscellaneous fixes to improve the quality of the code base. 

GENERAL
- Added ArcGIS 10.8 and 10.8.1 to the list of compatible versions in ftot.py
- Created setup script for automating the installation of FTOT 2020.3 and Python dependencies
- Included the GDAL .whl files for installation in a new Dependencies folder
- Deprecated the use of Pickle in the post-optimization step
- Reduced the memory consumption in the G step by ignoring the geometry attributes of the temporary shape files.
- Moved the shape file export from the end of the C step to the beginning of the G step.
- Deletes the temporary networkx shape file directory from at the end of the G step.
- Renamed the post_optimization_64_bit() method  

PULP MODULE 
- Added schedule functionality at the facility level, and Quick Start 2 Exercise 4 for as example for how to use the new schedule feature. 

TABLEAU DASHBOARDS
- Added units to tooltips throughout
- Changed color scheme for commodity and scenario name symbols
- Changed the alias of _MAP SYBOLOGY CALC to “Symbol By”
- Fixed bug where the story would open up empty because the scenario name filters were not selected. 
- Fixed bug where the summary results by scenario name did not inherit the scenario name color scheme. 
- Updated dashboard sizes to support non-1080p resolutions (e.g. 4k)
- Added absolute, percent difference, and difference views for the Summary graphics.
- Facilities map now includes the optimal and non-optimal facility filters. 

MAPS
- No changes to report in 2020.3

## v2020_2
The 2020.2 release includes a refactoring of the optimization module for candidate processor scenarios, enhancements to the Tableau dashboard functionality, and several general fixes for stability and user experience improvements.

PULP MODULE REFACTORING
- Code is simplified to reduce redundancy between OC step and O1/O2 steps.
  - Removed the following methods from ftot_pulp_candidate_generation.py:
    -	generate_all_vertices_candidate_gen
    - generate_first_edges_from_source_facilities
    - generate_connector_and_storage_edges
    - add_storage_routes
    - generate_first_edges_from_source_facilities
    - set_edges_volume_capacity
    - create_constraint_max_route_capacity
- Removed calls to serialize the optimization problem with pickle before the call to prob.solve().
- Removed extra logger.debug messages from O1 and O2 steps.
- Removed redundant calls in pre_setup_pulp() to generate_first_edges_from_source_facilities and generate_all_edges_from_source_facilities in ftot_pulp.py 

TABLEAU DASHBOARDS
-	Created Tableau Story to step through scenario dashboards. 
- Added legend options to color routes by scenario name or commodity in comparison dashboards.
- Added fuel burn to scenario results.
- Added high-level scenario comparison summary.

MAPS
- No changes to report in 2020.2 

GENERAL
- Removed check for ArcGIS Network_analyst extension in ftot.py 
- Updated installation instructions to specify version number for dependencies: pulp v.1.6.10, and imageio v.2.6
- Fixed input validation bug for candidate facilities that crashed at runtime.
- Fixed Divide By Zero bug in Minimum Bounding Geometry step when road network is empty. 
- Added FTOT Tool to batch update configuration parameters recursively in scenario.xml files

## v2020_1
TABLEAU DASHBOARDS
- New Scenario Compare Workbook for comparing multiple FTOT scenarios in one Tableau workbook.
- Scenario Compare utility updated in FTOT Tools.py - now concatenates multiple scenarios and creates a new packaged Tableau workbook. 
- Cost and volume numbers buffered to prevent overlap with mode graphics
- Facilities map tool tips no longer display metadata. 
- Changes the config output from the S step (first one) to O2 step.
- Pipeline symbology fixes
- Runtimes now reported in the tableau report

MAPS
- Basemap capability added 
- Custom user created map capability added 
- Extent functionality for maps changed
- Maps that show aggregate flows have been deprecated and removed
- Some map names have changed. In particular, the basemap name has been added to each of the maps. The numbering of a few maps was also changed and two had their titles shortened (e.g. raw_material_producer became rmp) for consistency with other maps.

GENERAL 
- Documentation updated to reflect the rail and water short-haul penalty.
- Documentation updated and Quick Start Scenarios created to demonstrate pipeline movements with the commodity_mode.csv
- Fixed hundredweight units typo in the python module used to track and convert units, Pint.
- General FTOT input validation, especially against whitespace which could cause optimization to fail.

## v2019_4
- arcgis 10.7.1 compatibility fixed.
- reports fail with logged exceptions fixed.
- updated minimum bounding geometry algorithm in the C step.
- total scenario cost is reported twice fixed.
- change log file added to repository.

## v2019_3
-	processor facilities can now handle two input commodities.
-	commodity-mode specific restrictions are enabled.
-	tableau dashboard: display all facility types at once by default.
-	emission factors are updated to reflect 2019 values.
-	getter and setter definitions in the ftot_scenario.py module.
-	commodity names are now case insensitive for input CSV files.
-	optimizer passes back nearly zero values and causes bad maps.
-	D Step throws an exception when no flow optimal solution.
-	F step throws an exception if there is extra blank space in an input CSV file.
-	tableau dashboard: units label for material moved by commodity and mode.
