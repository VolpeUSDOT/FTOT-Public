# FTOT Change Log

## v2024_2

The FTOT 2024.2 public release includes updates related to cost reporting outputs and visualizations, modeling of intermodal movement costs, scenario input validation, and back-end improvements to how the transportation network is processed and translated into NetworkX. The following changes have been made:
* Developed additional FTOT outputs to explain and visualize costs associated with the optimal routing solution of a scenario. A new CSV report summarizes both scaled and unscaled costs by commodity and by mode. The scaled costs account for user-specified transport and CO2 cost scalars and are used in the optimization. Costs are categorized as movement costs (broken out into costs from transport, transloading, first mile/last mile, mode short haul penalties, and impedances), emissions costs (broken out into CO2 and CO2 first mile/last mile), processor build costs, and unmet demand penalties. A new Cost Breakdown dashboard in the Tableau workbook visualizes the scaled and unscaled cost components.
* Updated the transport cost and routing cost methodology for intermodal movements. In addition to the transloading cost applied per unit moved between modes, transportation costs along the edges connecting the transloading facility to the rest of the FTOT network are now applied using the default per ton-mile (or thousand gallon-mile) costs of the mode that the transloading facility is connected to. The routing cost component from transport for intermodal movements is equivalent to the (unimpeded) transport cost.
* Added new input validation checks to confirm alignment between facility geodatabase and facility-commodity input files. The user is provided log messages when all facilities in the facility-commodity input files fail to match with a facility location in the corresponding feature class of the geodatabase.
* Updated method for hooking user-specified facilities into the network to ensure that all network segment attributes are passed down to split links (previously, only attributes that were part of the FTOT network specification were retained).
* Other updates:
  * Generalized FTOT code used to translate the network geodatabase into a NetworkX object to allow network segment attributes to be passed through the G step to support future extensions cost varying link cost based on other network attributes.
  * Fixed a logging bug that was consistently printing out a warning to users that certain log files were not successfully added to the FTOT text report.
See documentation files for additional details.


## v2024_1

The FTOT 2024.1 public release includes updates related to the pipeline network, waterway background volume and capacity handling, FTOT Tools and the Scenario Setup Template, the Tableau routes dashboard, and network resilience analysis. The following changes have been made:
* The default FTOT multimodal network has been refreshed to incorporate updated pipeline tariff data for crude and petroleum products effective as of January 2024. Aside from updated tariff costs, some pipeline tariffs that are no longer effective have been removed, and others that are newly effective or have been newly geolocated have been added.
* Updated the optimization problem to enable use of lock volumes, capacity, and volume/capacity ratio attributes. Lock data are applied to adjacent waterway links when available and when capacity and/or background flows are enabled for the water network. Added a “locks” feature class schema to the FTOT network specification and improved documentation to clarify how waterway capacity and background volumes can be applied to the network.
* Renamed the XLSX-based Scenario Setup Template and complementary Scenario Setup Conversion Tool, which provide a user-friendly process for entering scenario data into corresponding FTOT input files. Additional input data validation has been added to confirm required data have been entered. The tool currently does not create required GIS inputs or optional CSV files. Example XLSX workbooks included with the FTOT codebase have been updated. A tutorial video explaining how to fill out the template and run the conversion tool has been published on the FTOT landing page. See the User Guide for more information.
* Updated default values for modal base transport costs in the scenario XML to align with 2020 data published by BTS.
* Redesigned and added CO2 emissions to the Tableau routes dashboard, which is generated when scenarios are run with network density reduction (NDR) turned on.
* Aligned the FTOT rank and removal tool with FTOT version 2024.1. Refactored for usability and added new visualizations to the output HTML report.
* Other updates:
  * Added a BAT file to launch FTOT Tools.
  * Fixed a bug in generating the Tableau routes dashboard in scenario comparison workbooks.
  * Removed the “network used” metric from the Tableau dashboard to ensure consistency across reports.
  * Removed ArcGIS dependency from scenarios that leverage multiprocessing (a potential cause of license-related crashes of FTOT).
  * Removed use of intermediate shapefiles in converting the GIS-based FTOT network to a NetworkX graph.
See documentation files for additional details.


## v2023_4

The FTOT 2023.4 public release includes updates related to scenario input validation, FTOT tools for data preparation and validation, reporting metrics, and network resilience testing. The following changes have been made:
* Input file validation around the facility-commodity CSV files (e.g., rmp.csv, dest.csv) and the schedules CSV file has been streamlined and updated. Validation checks and error messaging warn the user to issues with their scenario input files related to required columns, improper data formats, and mismatches between files. This development continues input validation work done in the 2023.2 public release; it will continue to be expanded in future releases.
* Enhancements to the network validation helper tool, including network connectivity checks and additional summary statistics. For more details, see the User Guide.
* Updated the XLSX-based input data template and complementary FTOT Tool to convert user scenario inputs into corresponding FTOT input files. Additional input data validation has been added to confirm required data have been entered. The tool currently does not create required GIS inputs or optional CSV files. Several example XLSX workbooks are included with the FTOT codebase. See the User Guide for more information.
* Added CO2 emissions to the all_routes CSV file, which is a supplementary report generated for scenarios run with Network Density Reduction (NDR) on. Also added a new database table with the same information provided in this report.
* Corrected reporting metrics for (1) amount of network used to avoid double-counting links used by multiple commodities and (2) processor utilization of commodity flow through a processor facility as a fraction of processor capacity when specified.
* Aligned the FTOT rank and removal tool with FTOT version 2023.1 and updated example scenario in the tool instructions to Reference Scenario 7.
* Minor updates:
  * Corrected a bug in certain versions of ArcGIS Pro preventing the completion of the M2 step (time and commodity mapping).
  * Corrected a bug in assigning CO2 emission factors to artificial links for non-road modes, specifically for use in CO2 optimization scenarios.
  * Moved Tableau template file from the supplementary materials to the “lib” folder in the FTOT code base.
  * Updated the optimization problem formulation in the technical documentation to align with the codebase.
  * Added high-level guidance for how to conduct end-to-end source tracking using database tables.
See documentation files for additional details.


## v2023_3

The FTOT 2023.3 public release includes major updates related to emissions-based optimization, a new North American multimodal network, user preparation of FTOT input files, and supply chain resilience. The following changes have been made:
* Added new functionality to incorporate carbon dioxide (CO2) emissions-related costs into the FTOT route optimization problem. Users can now set what share of (i) impeded transport cost and (ii) CO2 cost to combine in a composite routing cost for route optimization using optional Transport_Cost_Scalar, CO2_Cost_Scalar, and CO2_Unit_Cost elements in the scenario XML. Users can specify the cost per unit of CO2 emissions and separate scaling factors (in a range from 0.0 to 1.0) for the impeded transport cost component and CO2 cost component. The default is to use the full value of impeded transport cost (scaling factor of 1.0) and to zero out CO2 emissions cost (scaling factor of 0.0), which leads to a transportation cost-only optimization approach.
* Created a North American multimodal network by adding available Canadian and Mexican modal data to FTOT’s default contiguous U.S. multimodal network. The draft network includes road, rail, waterway and intermodal facility data for Canada, along with rail and road data for Mexico. The network is designed to facilitate North American scenarios with a scope beyond the continental United States and is available upon request from the FTOT Team. More details are available in Appendix B of the Technical Documentation.
* Built an XLSX-based input data template and complementary FTOT Tool to convert user scenario inputs into corresponding FTOT input files. The FTOT Tool takes an XLSX workbook filled out by the user as input and creates a batch file, a scenario XML, and all facility-commodity CSV files. The tool currently does not create required GIS inputs or optional CSV files. Several example XLSX workbooks have been included with the FTOT codebase. See the User Guide for more information.
* Aligned FTOT-SCR with FTOT version 2023.2, bringing in new FTOT base functionality from the last four releases. 
* Updated methodology for calculating CO2 emissions for movements on road to use partial truckloads. All other emissions for movements on road use nearest full truckload. For more details, see the Technical Documentation.
* Minor updates:
  * Corrected a bug related to use of network density reduction (NDR) for scenarios involving multiple processes, a subset of which are candidate generation processes.
  * Corrected a bug in how detailed emissions factors were assigned to road types missing values for either the limited_access or urban attributes.
  * Made the impedance weights CSV file optional. Weights will default to 1.0 if the impedance CSV file is not found, meaning FTOT will not distinguish transport cost on different link types (e.g., local roads versus highways) in the optimization.
  * Corrected a bug in how impedances were assigned to unrecognized link types. If an impedance weights CSV file is provided but a link type in the modal feature class is not recognized, FTOT now correctly applies the maximum weight listed for that mode.
  * Corrected a bug in the Tableau workbook summary graphs for transport cost, network used, vehicle-distance traveled, fuel burn, and CO2 and made other minor fixes and improvements.
See documentation files for additional details.


## v2023_2

The FTOT 2023.2 public release includes updates related to scenario input file validation, candidate generation methodology, first-mile/last-mile reporting, and processor capacities. The following changes have been made:
* Added input validation functionality to check that required scenario files exist, required fields are found in each file, and field values are in the correct format. Expanded documentation in the FTOT User Guide and Reference Scenarios Document in coordination with input validation. This functionality will continue to be expanded in future releases.
* Created a network validation tool within the FTOT Tools suite to help users confirm that custom networks follow the network schema and meet other requirements.
* Expanded candidate generation methodology for both NDR on and off cases to support candidate processes with multiple output commodities and difference in allowed modes between candidate process input and output commodities.
* Enhanced reporting for artificial links. Expanded the artificial links report with additional summary metrics for links used in the optimal solution. Added a toggle to the scenario XML so users optionally can include artificial links in summary calculations for the main CSV and text reports. Emissions on all artificial links are calculated using truck specifications to represent typical first/last-mile transport by road.
* In the Tableau workbook, added a “time period” filter to the “By Commodity & Mode” dashboard to better visualize optimal flows in scenarios using schedules. Made other minor changes in Tableau.
* Corrected a few bugs:
  * Liquid capacities and commodity quantities are now handled correctly in the processor facility-commodity CSV file.
  * Capacity rows in the processor facility-commodity CSV file are now able to handle blank values in the commodity quantity field, as expected.
  * Processor candidate slate CSV files generated during candidate generation scenarios correctly implement facility capacity values.

See documentation files for additional details.


## v2023_1

The FTOT 2023.1 public release includes a major update with the creation of a general network specification that enables users to run FTOT with a custom multimodal network. This is a breaking change; users will need to update their supplementary data and scenario files (e.g., scenario XML file) in order to use FTOT 2023.1. This release also includes updates related to candidate generation using network density reduction (NDR), commodity-specific minimum and maximum processing capacities, first mile/last mile costs, and minor Tableau enhancements and bug fixes. The following changes have been made:
* Creates an FTOT general network specification schema for each component of the FTOT multimodal network (road, rail, water, locks, pipeline, multimodal facilities). Generalizes FTOT codebase to align with any transportation network that follows the schema. 
* Updates FTOT’s default multimodal network to the general network specification. The new default network is named “FTOT_Public_US_Contiguous_Network_v2023.gdb” and is located with the supplementary data and scenarios download. For users bringing custom networks to FTOT, see Section 2.2 in the FTOT User Guide for network schema requirements and Appendix B in FTOT’s technical documentation for the full network specification schema.
* Updates the XML schema to version 7 to align with the new network specification. To run FTOT 2023.1, users must update any pre-existing scenario XML files to this new version. Changes include:
  * New elements for default distance and currency units
  *	Relocation of network impedance weights to a new impedance weights CSV file
  * Relocation of CO2 emission factors by specific road type to the detailed emission factors CSV
  *	Switch from currency-based short haul penalties to distance-based penalties
  *	Updated XML template and corresponding updates to quick start and reference scenario XMLs
*	Enables commodity-specific processor capacities through optional “min_capacity” and “max_capacity” columns in the processor CSV file. Users can still apply facility_level capacity by including a row in the CSV file with “total” as the commodity; input or output must be specified in the “io” column for the “total” row.  “Max_processor_input” and “min_processor_input” columns are still supported and interpreted as a “total” input row. This change resolves a bug where  commodities were being erroneously combined across different units when checking processor capacity.
*	Resolves issues with network density reduction (NDR) for candidate processor generation in scenarios with large max transport distances or with a commodity mode CSV file.
*	Resolves a bug in the creation of processor flow constraints where an incorrect input/output ratio was being calculated for processors with multiple input commodities and different required quantities.
*	Updates transport cost and routing cost on artificial links for all modes to reflect the first mile/last mile costs of traveling by local roads to/from facilities.
*	Generalizes metric names in the reports and Tableau workbook to accommodate user-specified distance and currency units. Renamed metrics include transport cost (formerly dollar cost), network used (formerly miles), and vehicle-distance traveled (former vehicle-miles traveled).
*	Corrects minor bugs in (1) assigning urban/rural code and limited/nonlimited access information to road edges split by artificial links, (2) removing links in the optimization problem incorrectly coded as bidirectional that should be one-direction only, (3) minimizing offset of facility locations (raw material producers, processors, destinations) on the network, and (4) running NDR scenarios with the pipeline network.
*	Releases the “Customizing Scenarios” instruction video. All video tutorials are posted to the FTOT landing page. 

See documentation files for additional details.


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
