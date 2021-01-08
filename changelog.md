# FTOT Change Log

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
