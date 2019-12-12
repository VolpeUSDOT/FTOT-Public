# FTOT Change Log

## v2019_4
- FIXED - issue #175 - arcgis 10.7.1 compatibility.
- FIXED - issue #182 - reports fail with logged exceptions.
- FIXED - issue #142 - updated minimum bounding geometry algorithm in the C step.
- FIXED - issue #106 - total scenario cost is reported twice.
- ADDED - change log file added to repository.

## v2019_3
-	ADDED – issue #148 – processor facilities can now handle two input commodities.
-	ADDED – issue #137 – commodity-mode specific restrictions are enabled.
-	UPDATED – issue #108 – tableau dashboard: display all facility types at once by default.
-	UPDATED – issue #124 and #46 - emission factors are updated to reflect 2019 values.
-	DEPRECATED – issue #113 - getter and setter definitions in the ftot_scenario.py module.
-	FIXED – bug #131 – commodity names are now case insensitive for input CSV files.
-	FIXED – bug #147 – optimizer passes back nearly zero values and causes bad maps.
-	FIXED – bug #152 – D Step throws an exception when no flow optimal solution.
-	FIXED – bug #149 – F step throws an exception if there is extra blank space in an input CSV file.
-	FIXED – bug #108 - tableau dashboard: units label for material moved by commodity and mode.
