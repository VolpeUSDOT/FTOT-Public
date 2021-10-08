# FTOT Change Log

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
