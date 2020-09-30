# -----------------------------------------------------------------------------
# Name: ftot_suppporting_gis
#
# Purpose:
#
# -----------------------------------------------------------------------------

import os
import math
import itertools
import arcpy
LCC_PROJ = arcpy.SpatialReference('USA Contiguous Lambert Conformal Conic')

THOUSAND_GALLONS_PER_THOUSAND_BARRELS = 42

# ======================================================================================================================


def get_loads_per_vehicle_dict(the_scenario):
    loads_per_vehicle_dict = {}
    # rename load and loads to payload
    loads_per_vehicle_dict['liquid'] = { 'road': the_scenario.truck_load_liquid, 'rail': the_scenario.railcar_load_liquid, 'water': the_scenario.barge_load_liquid, 'pipeline_crude': the_scenario.pipeline_crude_load_liquid, 'pipeline_prod': the_scenario.pipeline_prod_load_liquid}
    loads_per_vehicle_dict['solid'] = { 'road': the_scenario.truck_load_solid, 'rail': the_scenario.railcar_load_solid, 'water': the_scenario.barge_load_solid}

    return loads_per_vehicle_dict
# =============================================================================

STATES_DATA = [
['01', 'AL', 'ALABAMA'],
['02', 'AK', 'ALASKA'],
['04', 'AZ', 'ARIZONA'],
['05', 'AR', 'ARKANSAS'],
['06', 'CA', 'CALIFORNIA'],
['08', 'CO', 'COLORADO'],
['09', 'CT', 'CONNECTICUT'],
['10', 'DE', 'DELAWARE'],
['11', 'DC', 'DISTRICT_OF_COLUMBIA'],
['12', 'FL', 'FLORIDA'],
['13', 'GA', 'GEORGIA'],
['15', 'HI', 'HAWAII'],
['16', 'ID', 'IDAHO'],
['17', 'IL', 'ILLINOIS'],
['18', 'IN', 'INDIANA'],
['19', 'IA', 'IOWA'],
['20', 'KS', 'KANSAS'],
['21', 'KY', 'KENTUCKY'],
['22', 'LA', 'LOUISIANA'],
['23', 'ME', 'MAINE'],
['24', 'MD', 'MARYLAND'],
['25', 'MA', 'MASSACHUSETTS'],
['26', 'MI', 'MICHIGAN'],
['27', 'MN', 'MINNESOTA'],
['28', 'MS', 'MISSISSIPPI'],
['29', 'MO', 'MISSOURI'],
['30', 'MT', 'MONTANA'],
['31', 'NE', 'NEBRASKA'],
['32', 'NV', 'NEVADA'],
['33', 'NH', 'NEW_HAMPSHIRE'],
['34', 'NJ', 'NEW_JERSEY'],
['35', 'NM', 'NEW_MEXICO'],
['36', 'NY', 'NEW_YORK'],
['37', 'NC', 'NORTH_CAROLINA'],
['38', 'ND', 'NORTH_DAKOTA'],
['39', 'OH', 'OHIO'],
['40', 'OK', 'OKLAHOMA'],
['41', 'OR', 'OREGON'],
['42', 'PA', 'PENNSYLVANIA'],
['72', 'PR', 'PUERTO_RICO'],
['44', 'RI', 'RHODE_ISLAND'],
['45', 'SC', 'SOUTH_CAROLINA'],
['46', 'SD', 'SOUTH_DAKOTA'],
['47', 'TN', 'TENNESSEE'],
['48', 'TX', 'TEXAS'],
['49', 'UT', 'UTAH'],
['50', 'VT', 'VERMONT'],
['51', 'VA', 'VIRGINIA'],
['53', 'WA', 'WASHINGTON'],
['54', 'WV', 'WEST_VIRGINIA'],
['55', 'WI', 'WISCONSIN'],
['56', 'WY', 'WYOMING']
]

# =======================================================================================================================


def get_state_abb_from_state_fips(input_fips):

    return_value = 'XX'

    for state_data in STATES_DATA:
        if state_data[0] == input_fips:
           return_value = state_data[1]

    return return_value

# =====================================================================================================================


def assign_pipeline_costs(the_scenario, logger, include_pipeline):
    scenario_gdb = os.path.join(the_scenario.scenario_run_directory, "main.gdb")
    # calc the cost for pipeline
    # ---------------------

    if include_pipeline:
        logger.info("starting calculate cost for PIPELINE")
        # base rate is cents per barrel

        # calculate for all
        arcpy.MakeFeatureLayer_management (os.path.join(scenario_gdb, "pipeline"), "pipeline_lyr")
        arcpy.SelectLayerByAttribute_management(in_layer_or_view="pipeline_lyr", selection_type="NEW_SELECTION", where_clause="Artificial = 0")
        arcpy.CalculateField_management("pipeline_lyr", field="FROM_TO_ROUTING_COST",  expression="(((!base_rate! / 100) / 42.0) * 1000.0)", expression_type="PYTHON_9.3")
        arcpy.CalculateField_management("pipeline_lyr", "TO_FROM_ROUTING_COST", -1, "PYTHON_9.3")
        arcpy.CalculateField_management("pipeline_lyr", field="FROM_TO_DOLLAR_COST",  expression="(((!base_rate! / 100) / 42.0) * 1000.0)", expression_type="PYTHON_9.3")
        arcpy.CalculateField_management("pipeline_lyr", "TO_FROM_DOLLAR_COST", -1, "PYTHON_9.3")

        # if base_rate is null, select those links that are not artifiical and set to 0 manually.
        arcpy.SelectLayerByAttribute_management(in_layer_or_view="pipeline_lyr", selection_type="NEW_SELECTION", where_clause="Artificial = 0 and base_rate is null")
        arcpy.CalculateField_management("pipeline_lyr", "FROM_TO_ROUTING_COST", 0, "PYTHON_9.3")
        arcpy.CalculateField_management("pipeline_lyr", "TO_FROM_ROUTING_COST", -1, "PYTHON_9.3")
        arcpy.CalculateField_management("pipeline_lyr", "FROM_TO_DOLLAR_COST", 0, "PYTHON_9.3")
        arcpy.CalculateField_management("pipeline_lyr", "TO_FROM_DOLLAR_COST", -1, "PYTHON_9.3")
        no_baserate_count = int(arcpy.GetCount_management("pipeline_lyr").getOutput(0))
        if  no_baserate_count > 0:
            logger.warn("pipeline network contains {} routes with no base_rate.  Flow cost will be set to 0".format(no_baserate_count))

    else:
        logger.info("starting calculate cost for PIPELINE = -1")
        arcpy.MakeFeatureLayer_management (os.path.join(scenario_gdb, "pipeline"), "pipeline_lyr")
        arcpy.CalculateField_management("pipeline_lyr", "FROM_TO_ROUTING_COST", -1, "PYTHON_9.3")
        arcpy.CalculateField_management("pipeline_lyr", "TO_FROM_ROUTING_COST", -1, "PYTHON_9.3")
        arcpy.CalculateField_management("pipeline_lyr", "FROM_TO_DOLLAR_COST", -1, "PYTHON_9.3")
        arcpy.CalculateField_management("pipeline_lyr", "TO_FROM_DOLLAR_COST", -1, "PYTHON_9.3")

# ======================================================================================================================


def set_intermodal_links(the_scenario, logger):
    scenario_gdb = the_scenario.main_gdb
    # set the artificial link field to 2 so that it
    # does not have flow restrictions applied to it.

    # assumes that the only artificial links in the network
    # are connecting intermodal facilities.

    logger.info("start: set_intermodal_links")

    for mode in ["road", "rail", "pipeline_prod_trf_rts", "pipeline_crude_trf_rts", "water"]:
        mode_layer = "{}_lyr".format(mode)
        arcpy.MakeFeatureLayer_management(os.path.join(scenario_gdb, mode), mode_layer)
        arcpy.SelectLayerByAttribute_management(in_layer_or_view=mode_layer,
                                                selection_type="NEW_SELECTION",
                                                where_clause="Artificial = 1")
        arcpy.CalculateField_management(mode_layer, "ARTIFICIAL", 2, "PYTHON_9.3")

# ======================================================================================================================


class LicenseError(Exception):
    pass

# ======================================================================================================================


def load_afpat_data_to_memory(fullPathToTable, logger):

    """load afpat data to memory"""

    logger.debug("START: load_afpat_data_to_memory")

    crop_yield_dict = {}  # keyed off crop name --> crop yield (e.g. kg/ha/yr)
    fuel_yield_dict = {}  # keyed off crop name + processing type --> fuel yields (e.g. jet, diesel, total biomass)

    afpatAsList = []

    # iterate through the afpat table and save as a list
    # --------------------------------------------
    with arcpy.da.SearchCursor(fullPathToTable, "*") as cursor:

        for row in cursor:

            afpatAsList.append(row)

    # now identify the relevant indexs for the values we want to extract

    # land specific information
    # -----------------------------------------
    countryIndex = afpatAsList[34].index('Country')
    landAreaIndex = afpatAsList[34].index('Total Land Area Used')


    # feedstock names
    # -----------------------------------
    Feedstock_Source_Index = afpatAsList[34].index('Feedstock Source')
    feedstock_type = afpatAsList[34].index('Feedstock Type')
    feedstock_source_category = afpatAsList[34].index('Source Category')

    # crop yield index
    #----------------------------
    oilIndex = afpatAsList[34].index('Oil Yield')
    noncellulosicIndex = afpatAsList[34].index('Non-Cellulosic Sugar Yield')
    lignoIndex = afpatAsList[34].index('Lignocellulosic Yield')

    # process index
    # -------------------------------------------
    primaryProcTypeIndex = afpatAsList[34].index('Primary Processing Type')
    secondaryProcTypeIndex = afpatAsList[34].index('Secondary Processing Type')
    tertiaryProcTypeIndex = afpatAsList[34].index('Tertiary Processing Type')
    capital_costs = afpatAsList[34].index('Capital Costs')

    # fuel yeilds index
    # -------------------------------------------
    jetFuelIndex = afpatAsList[34].index('Jet Fuel ')
    dieselFuelIndex = afpatAsList[34].index('Diesel fuel')
    naphthaIndex = afpatAsList[34].index('Naphtha')
    aromaticsIndex = afpatAsList[34].index('Aromatics')
    total_fuel = afpatAsList[34].index('Total fuel (Excluding propane, LPG and heavy oil)')
    conversion_efficiency = afpatAsList[34].index('Conversion Eff')
    total_daily_biomass = afpatAsList[34].index('Total Daily Biomass')


    table2cIndex = 0
    table2dIndex = 0

    from ftot_supporting import get_cleaned_process_name
    from ftot_supporting import create_full_crop_name

    for r in afpatAsList:

        # move biowaste to another table
        if r[2] == "Table 2c:":

            table2cIndex = afpatAsList.index(r)

        # move fossil resources to another table
        elif r[2] == "Table 2d:":

            table2dIndex = afpatAsList.index(r)

        # add commodity and process information to the crop_yield_dict and fuel_yield_dict
        if r[countryIndex] == "U.S." and r[Feedstock_Source_Index] != "":

            # concatinate crop name
            feedstock_name = create_full_crop_name(r[feedstock_type], r[feedstock_source_category],r[Feedstock_Source_Index])



            # determine which crop yield field we want depending on the feedstock_type
            if r[feedstock_type] == "Oils":

                    preprocYield = float(r[oilIndex])

            elif r[feedstock_type] == "Non-Cellulosic Sugars":

                preprocYield = float(r[noncellulosicIndex])

            elif r[feedstock_type] == "Lignocellulosic Biomass and Cellulosic Sugars":

                preprocYield = float(r[lignoIndex])

            else:
                logger.error("Error in feedstock type specification from AFPAT")
                raise Exception("Error in feedstock type specification from AFPAT")


            # preprocYield is independent of processor type, so its okay
            # if this gets written over by the next instance of the commodity
            crop_yield_dict[feedstock_name] = preprocYield

            # Fuel Yield Dictionary
            # ---------------------------------
            # each commodity gets its own dictionary, keyed on the 3 process types; value is a list of the form
            # [land area, jet, diesel, naphtha, aromatics, yield]; I'm adding total fuel and
            # feedstock type to the end of this list; maybe units also?

            process_name = get_cleaned_process_name(r[primaryProcTypeIndex], r[secondaryProcTypeIndex],r[tertiaryProcTypeIndex])

            if feedstock_name not in fuel_yield_dict:

                fuel_yield_dict[feedstock_name] = {process_name: [float(r[landAreaIndex]),
                                        float(r[jetFuelIndex]), float(r[dieselFuelIndex]),
                                        float(r[naphthaIndex]), float(r[aromaticsIndex]),
                                        preprocYield, float(r[total_fuel]),
                                        float(r[conversion_efficiency]), float(r[total_daily_biomass])]}
            else:

                fuel_yield_dict[feedstock_name][process_name] = [float(r[landAreaIndex]),
                                        float(r[jetFuelIndex]), float(r[dieselFuelIndex]),
                                        float(r[naphthaIndex]), float(r[aromaticsIndex]),
                                        preprocYield, float(r[total_fuel]),
                                        float(r[conversion_efficiency]), float(r[total_daily_biomass])]

    table2c = list(itertools.islice(afpatAsList, table2cIndex + 2, table2dIndex))
    table2d = list(itertools.islice(afpatAsList, table2dIndex + 2, len(afpatAsList)))

    bioWasteDict = {}
    fossilResources = {}

    resourceIndex = table2c[0].index("Resource")
    procTypeIndex = table2c[0].index("Processing Type")
    percentIndex = table2c[0].index("Percent of Resource")
    resourceKgIndex = table2c[1].index("kg/yr")
    facilitiesIndex = table2c[0].index("# FTx or HEFA Facilities")
    capCostsIndex = table2c[0].index("Capital Costs")
    totFuelIndex = table2c[0].index("Total fuel")
    jetFuelIndex = table2c[0].index("Jet fuel")
    dieselFuelIndex = table2c[0].index("Diesel fuel")
    naphthaIndex = table2c[0].index("naphtha")

    numFtxIndex = table2d[0].index("# FTx Facilities")
    ccsIndex = table2d[0].index("CCS Required")
    eorIndex = table2d[0].index("Percent of EOR Capacity")
    tabDcapCost = table2d[0].index("Capital Costs")

    secondaryProcType = "N/A"
    tertiaryProcType = "N/A"


    del table2c[0]
    del table2d[0]

    for t in table2c:

        if t[2] != "":

            resourceName = t[resourceIndex].replace("-", "_").replace(" ", "_")
            processType = (t[procTypeIndex].replace("-", "_").replace(" ", "_"), secondaryProcType, tertiaryProcType)

            if resourceName not in bioWasteDict:

                bioWasteDict[resourceName] = {processType: [float(t[percentIndex]),
                                                                float(t[resourceKgIndex]),
                                                                int(math.ceil(float( t[facilitiesIndex]))),
                                                                float(t[capCostsIndex]),
                                                                int(math.ceil(float( t[totFuelIndex]))),
                                                                int(math.ceil(float( t[jetFuelIndex]))),
                                                                int(math.ceil(float( t[dieselFuelIndex]))),
                                                                int(math.ceil(float( t[naphthaIndex])))]}
            else:

                bioWasteDict[resourceName][processType] = [float(t[percentIndex]),
                                                                float(t[resourceKgIndex]),
                                                                int(math.ceil(float( t[facilitiesIndex]))),
                                                                float(t[capCostsIndex]),
                                                                int(math.ceil(float( t[totFuelIndex]))),
                                                                int(math.ceil(float( t[jetFuelIndex]))),
                                                                int(math.ceil(float( t[dieselFuelIndex]))),
                                                                int(math.ceil(float( t[naphthaIndex])))]
    for t in table2d:
        if t[2] != "" and t[3] != "":

            resourceName = t[resourceIndex].replace("-", "_").replace(" ", "_")
            processType =  (t[procTypeIndex].replace("-", "_").replace(" ", "_"), secondaryProcType, tertiaryProcType)

            if resourceName not in fossilResources:

                 fossilResources[resourceName] = {processType: [int(t[numFtxIndex]),
                                                                   float(t[tabDcapCost]),
                                                                   float(t[ccsIndex]),
                                                                   float(t[eorIndex]),
                                                                   int(math.ceil(float( t[totFuelIndex]))),
                                                                   int(math.ceil(float( t[jetFuelIndex]))),
                                                                   int(math.ceil(float( t[dieselFuelIndex]))),
                                                                   int(math.ceil(float( t[naphthaIndex])))]}


            else:

                 fossilResources[resourceName][processType] = [int(t[numFtxIndex]),
                                                                   float(t[tabDcapCost]),
                                                                   float(t[ccsIndex]),
                                                                   float(t[eorIndex]),
                                                                   int(math.ceil(float( t[totFuelIndex]))),
                                                                   int(math.ceil(float( t[jetFuelIndex]))),
                                                                   int(math.ceil(float( t[dieselFuelIndex]))),
                                                                   int(math.ceil(float( t[naphthaIndex])))]

    logger.debug("FINISH: load_afpat_data_to_memory")

    return fuel_yield_dict, crop_yield_dict, bioWasteDict, fossilResources
# ==============================================================================


def persist_AFPAT_tables(the_scenario,  logger):
    # pickle the AFPAT tables so it can be read in later without ArcPy

    scenario_gdb = the_scenario.main_gdb

    afpat_raw_table = os.path.join(scenario_gdb, "afpat_raw")

    fuel_yield_dict, crop_yield_dict, bioWasteDict, fossilResources = load_afpat_data_to_memory(afpat_raw_table, logger)

    afpat_tables = [fuel_yield_dict, crop_yield_dict, bioWasteDict, fossilResources]

# ==============================================================================

# **********************************************************************
# File name: Zip.py
# Description:
#    Zips the contents of a folder, file geodatabase or ArcInfo workspace
#    containing coverages into a zip file.
# Arguments:
#  0 - Input workspace
#  1 - Output zip file. A .zip extension should be added to the file name
#
# Created by: ESRI
# **********************************************************************

# Import modules and create the geoprocessor
import os


# Function for zipping files
def zipgdb(path, zip_file):
    isdir = os.path.isdir

    # Check the contents of the workspace, if it the current
    # item is a directory, gets its contents and write them to
    # the zip file, otherwise write the current file item to the
    # zip file
    for each in os.listdir(path):
        fullname = path + "/" + each
        if not isdir(fullname):
            # If the workspace is a file geodatabase, avoid writing out lock
            # files as they are unnecessary
            if not each.endswith('.lock'):
                # Write out the file and give it a relative archive path
                try: zip_file.write(fullname, each)
                except IOError: None # Ignore any errors in writing file
        else:
            # Branch for sub-directories
            for eachfile in os.listdir(fullname):
                if not isdir(eachfile):
                    if not each.endswith('.lock'):
                        gp.AddMessage("Adding " + eachfile + " ...")
                        # Write out the file and give it a relative archive path
                        try: zip_file.write(fullname + "/" + eachfile, os.path.basename(fullname) + "/" + eachfile)
                        except IOError: None # Ignore any errors in writing file


