# FTOT Change Log

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
