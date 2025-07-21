# -----------------------------------------------------------------------------
# Name: ftot_suppporting_gis.py
#
# Purpose: This module provides helper functions for loading and processing gis data.
#
# -----------------------------------------------------------------------------

import os
import math
import itertools
import arcpy
import sqlite3
import numpy as np
from ftot import Q_

THOUSAND_GALLONS_PER_THOUSAND_BARRELS = 42


# ===================================================================================================


def make_commodity_density_dict(the_scenario, logger):
    """
    Creates a dictionary of commodity densities, using the default density from XML and/or commodity
    density CSV file. Units are default solid commodity unit per default liquid commodity unit.

    Args:
        the_scenario: Scenario XML parameters.
        logger: Logger object.
    
    Returns:
        density_dict: Dictionary with all commodity names as keys, densities (as pint quantities) as values.
    """
    logger.debug("start: make_commodity_density_dict")

    # Query commodities
    commodity_names = [] 
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        commodities = main_db_con.execute("select commodity_name from commodities where commodity_name <> 'multicommodity';")
        commodities = commodities.fetchall()
        for name in commodities:
            commodity_names.append(name[0])
    
    # Initialize density dict with default density factor
    density_dict = dict([(comm, the_scenario.densityFactor) for comm in commodity_names])

    # Use default if input file set to None
    if the_scenario.commodity_density_data == "None":
        logger.info('Commodity density file not specified. Defaulting to density {} for all commodities.'.format(the_scenario.densityFactor))
        return density_dict

    # Read through densities csv
    with open(the_scenario.commodity_density_data, 'r') as cd:
        line_num = 1
        for line in cd:
            flds = line.rstrip('\n').split(',')
            if line_num == 1:
                if flds[0] != 'commodity' or flds[1] != 'density':
                    error = "Error: commodity_density_data file {} does not match the appropriate "\
                            "schema. Please check that the first two columns are 'commodity' "\
                            "and 'density'.".format(the_scenario.commodity_density_data)
                    logger.error(error)
                    raise Exception(error)
            else:
                commodity = flds[0].lower().strip()
                density = flds[1]

                # Check commodity
                if commodity not in commodity_names:
                    logger.warning("Commodity: {} in commodity_density_data is not recognized.".format(commodity))
                    continue # skip this commodity
                
                # Assign default density if commodity has blank density
                # Otherwise do unit conversion
                if density == "":
                    density = the_scenario.densityFactor
                else:
                    try:
                        density = Q_(density).to('{}/{}'.format(the_scenario.default_units_solid_phase, the_scenario.default_units_liquid_phase))
                    except Exception as e:
                        logger.error("FAIL: {} ".format(e))
                        raise Exception("FAIL: {}".format(e))

                # Populate dictionary
                density_dict[commodity] = density

            line_num += 1

    for commodity in density_dict:
        logger.debug("Commodity: {}, Density: {}".format(commodity, density_dict[commodity]))

    return density_dict


# ===================================================================================================


def make_emission_factors_dict(the_scenario, logger):
    """
    Returns emissions factors dictionary keyed on mode, vehicle label, pollutant, and link type.
    Values are pint quantities (e.g., magnitude and units). Units are in g / default_units_distance
    for road and g / default_units_solid_phase / default_units_distance for rail, water, and pipeline.

    Args:
        the_scenario: Scenario XML parameters.
        logger: Logger object.
    
    Returns:
        factors_dict: Dictionary with emissions factors for all modes, vehicle types, pollutants, and link types.
    """
    # Initiate emissions factors dictionary using general CO2 factors from XML
    # For artificial links, use road values for all modes
    factors_dict = {'road': {'Default': {'co2': {'general': the_scenario.roadCO2Emissions,
                                                 'artificial': the_scenario.roadCO2Emissions}}},
                    'rail': {'Default': {'co2': {'general': the_scenario.railroadCO2Emissions,
                                                 'artificial': the_scenario.roadCO2Emissions}}},
                    'water': {'Default': {'co2': {'general': the_scenario.bargeCO2Emissions,
                                                  'artificial': the_scenario.roadCO2Emissions}}},
                    'pipeline': {'Default': {'co2': {'general': the_scenario.pipelineCO2Emissions,
                                                     'artificial': the_scenario.roadCO2Emissions}}}}

    # query CUSTOM vehicle labels for validation
    available_vehicles = []
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        all_vehicles = main_db_con.execute("select * from vehicle_types;")
        all_vehicles = all_vehicles.fetchall()
        for row in all_vehicles:
            mode = row[0]
            vehicle_label = row[1]
            property_name = row[2]
            value = row[3]

            available_vehicles.append(vehicle_label)

            # For custom vehicles, initiate emissions dictionary by adding artificial link emission factor
            # Uses general road CO2 emissions factor for artificial links regardless of vehicle type
            factors_dict[mode][vehicle_label] = {'co2': {'artificial': the_scenario.roadCO2Emissions}}

            # For custom vehicles, add general CO2 emissions to emissions dictionary
            # Note that pipeline is not supported for vehicle types
            if property_name in ['Truck_CO2_Emissions', 'Railcar_CO2_Emissions', 'Barge_CO2_Emissions']:

                # convert units
                if mode == 'road':
                    value = Q_(value).to('g/{}'.format(the_scenario.default_units_distance))
                else:
                    value = Q_(value).to('g/{}/{}'.format(the_scenario.default_units_solid_phase, the_scenario.default_units_distance))

                # save value
                factors_dict[mode][vehicle_label]['co2']['general'] = value

    # All vehicle labels including Default
    available_vehicles.append("Default")
    available_vehicles = set(available_vehicles)

    # Prepopulate detailed road types with general CO2 factor for the specific vehicle type
    # Will overwrite later if info available in detailed_emissions
    for truck in factors_dict['road']:
        for item in ['urban', 'rural', 'limited', 'nonlimited', 'urban_limited', 'rural_limited', 'urban_nonlimited', 'rural_nonlimited']:
            factors_dict['road'][truck]['co2'][item] = factors_dict['road'][truck]['co2']['general']

    # Check for detailed emissions factors file
    if not os.path.exists(the_scenario.detailed_emissions_data):
        
        # copy generic 'pipeline' entries to specific pipeline types
        factors_dict['pipeline_crude_trf_rts'] = factors_dict['pipeline']
        factors_dict['pipeline_prod_trf_rts'] = factors_dict['pipeline']

        return factors_dict # Return defaults

    # Otherwise read and parse detailed_emissions_factors CSV
    with open(the_scenario.detailed_emissions_data, 'r') as ef:

        line_num = 1
        for line in ef:
            flds = line.rstrip('\n').split(',')
            if line_num == 1:
                if flds[0] != 'vehicle_label' or flds[1] != 'mode' or flds[2] != 'urban' or flds[3] != 'limited_access' or flds[4] != 'pollutant' or flds[5] != 'value':
                    error = "Error: detailed_emissions_factors file {} does not match the appropriate "\
                            "schema. Please check that the first six columns are 'vehicle_label', 'mode', "\
                            "'urban', 'limited_access', 'pollutant', and 'value'.".format(the_scenario.detailed_emissions_data)
                    logger.error(error)
                    raise Exception(error)
            else:
                vehicle_label = flds[0]
                mode = flds[1].lower().strip()
                urban = flds[2].strip()
                limited_access = flds[3].strip()
                pollutant = flds[4].lower().strip()
                factor = flds[5]

                # Check vehicle label
                # Note: We're strict with capitalization here since vehicle_types processing is also strict 
                if vehicle_label not in available_vehicles:
                    logger.warning("Vehicle: {} on line: {} in detailed emissions file is not recognized.".format(vehicle_label, line_num))
                    line_num += 1
                    continue # move to next line in CSV

                # Check mode
                assert mode in ['road', 'water', 'rail', 'pipeline'], "Mode: {} is not supported. Please specify road, water, rail, or pipeline".format(mode)

                # Check urban and limited_access entries
                if mode == 'road':
                    allowed_entries = ['0', '1', 'NA']
                    assert urban in ['0', '1', 'NA'], "Urban entry: {} is not recognized. Must be one of {}.".format(urban, allowed_entries)
                    assert limited_access in ['0', '1', 'NA'], "Limited_access entry: {} is not recognized. Must be one of {}.".format(limited_access, allowed_entries)
                else:
                    assert urban == 'NA', "Urban entry must be 'NA' for non-road modes."
                    assert limited_access == 'NA', "Limited_access entry must be 'NA' for non-road modes."

                assert pollutant in ['co2', 'co', 'co2e', 'ch4', 'n2o', 'nox', 'pm10', 'pm2.5', 'voc'], \
                    "Pollutant: {} is not recognized. Refer to the documentation for allowed pollutants.".format(pollutant)
                
                # convert units
                # Pint throws an exception if units are invalid
                if mode == 'road':
                    factor = Q_(factor).to('g/{}'.format(the_scenario.default_units_distance))
                else:
                    factor = Q_(factor).to('g/{}/{}'.format(the_scenario.default_units_solid_phase, the_scenario.default_units_distance))
 
                # if pollutant is CO2, this will override what's in XML
                # add pollutant to dictionary if not already there
                if pollutant not in factors_dict[mode][vehicle_label]:
                    factors_dict[mode][vehicle_label][pollutant] = {}

                if mode == 'road':
                    # store detailed emissions factors for road
                    if urban == 'NA' and limited_access == 'NA':
                        # if pollutant is CO2, this will override what's in XML
                        factors_dict[mode][vehicle_label][pollutant]["general"] = factor 
                    elif urban == '1' and limited_access == 'NA':
                        factors_dict[mode][vehicle_label][pollutant]["urban"] = factor
                    elif urban == '0' and limited_access == 'NA':
                        factors_dict[mode][vehicle_label][pollutant]["rural"] = factor
                    elif urban == 'NA' and limited_access == '1':
                        factors_dict[mode][vehicle_label][pollutant]["limited"] = factor
                    elif urban == 'NA' and limited_access == '0':
                        factors_dict[mode][vehicle_label][pollutant]["nonlimited"] = factor
                    elif urban == '1' and limited_access == '1':
                        factors_dict[mode][vehicle_label][pollutant]["urban_limited"] = factor                              
                    elif urban == '0' and limited_access == '1':
                        factors_dict[mode][vehicle_label][pollutant]["rural_limited"] = factor 
                    elif urban == '1' and limited_access == '0':
                        factors_dict[mode][vehicle_label][pollutant]["urban_nonlimited"] = factor                              
                    elif urban == '0' and limited_access == '0':
                        factors_dict[mode][vehicle_label][pollutant]["rural_nonlimited"] = factor
                    
                else:
                    # store detailed emissions factors for non-road
                    # if pollutant is CO2, this will override what's in XML
                    factors_dict[mode][vehicle_label][pollutant]['general'] = factor
            line_num += 1

    # For non-CO2 road types, use provided data to fill in missing values
    for truck in factors_dict['road']:
        for pollutant in factors_dict['road'][truck]:
            if pollutant != 'co2':

                # get the avg of provided factors for this pollutant
                ef_values = [v for i, v in factors_dict['road'][truck][pollutant].items()]
                ef_unit = ef_values[0].units # get unit from the first emission factor
                ef_avg = np.mean([v.magnitude for v in ef_values]) # magnitude, not a Pint object

                for item in ['general', 'urban', 'rural', 'limited', 'nonlimited', 'urban_limited', 
                             'rural_limited', 'urban_nonlimited', 'rural_nonlimited']:
                    # assign value (as Pint object) to any link type without a value
                    # whether it's used or not depends on link types in provided network
                    if item not in factors_dict['road'][truck][pollutant]:
                        factors_dict['road'][truck][pollutant][item] = Q_(ef_avg, ef_unit)

    # For all vehicles, assign the Default road vehicle's general non-CO2 to artificial links
    for mode in factors_dict:
        for vehicle in factors_dict[mode]:
            for pollutant in factors_dict['road']['Default']: # loop through available pollutants
                if pollutant != 'co2':
                    if pollutant not in factors_dict[mode][vehicle]:
                        factors_dict[mode][vehicle][pollutant] = {'artificial': factors_dict['road']['Default'][pollutant]['general']}
                    else:
                        factors_dict[mode][vehicle][pollutant]['artificial'] = factors_dict['road']['Default'][pollutant]['general']

    # copy generic 'pipeline' entries to specific pipeline types
    factors_dict['pipeline_crude_trf_rts'] = factors_dict['pipeline']
    factors_dict['pipeline_prod_trf_rts'] = factors_dict['pipeline']

    return factors_dict # mode, vehicle label, pollutant, link type


# ===================================================================================================


def get_commodity_vehicle_attributes_dict(the_scenario, logger):
    """
    Creates a dictionary of commodity and mode specifications with vehicle attributes. Dictionary is
    keyed off of commodity name, mode, vehicle attribute (pollutant name or 'Load' or 'Fuel_Efficiency'),
    and link type (except for 'Fuel_Efficiency'). Values are pint quantities (e.g., magnitude and units).
    Note that units for rail/water/pipeline 'general' emissions for 'liquid' commodities are grams per
    default LIQUID commodity unit per default distance unit. Note that units for road/rail/water
    'Fuel_Efficiency' for 'liquid' commodities are default LIQUID commodity unit times default distance
    unit per gallon.

    Args:
        the_scenario: Scenario XML parameters.
        logger: Logger object.
    
    Returns:
        attribute_dict: Dictionary keyed off commodity name then mode then vehicle attribute then link type.
    """

    with sqlite3.connect(the_scenario.main_db) as main_db_con:

        # query commodities table
        commodities_dict = {}  # key off ID
        commodities = main_db_con.execute("select * from commodities where commodity_name <> 'multicommodity';")
        commodities = commodities.fetchall()
        for row in commodities:
            commodity_id = str(row[0])
            commodity_name = row[1]
            commodities_dict[commodity_id] = commodity_name

        # query commodity modes table
        commodity_mode_dict = {}  # key off phase, then commodity name (not ID!)
        commodity_mode = main_db_con.execute("select * from commodity_mode;")
        commodity_mode = commodity_mode.fetchall()
        for row in commodity_mode:
            mode = row[0]
            commodity_id = row[1]
            phase = row[2]
            vehicle_label = row[3]
            allowed_yn = row[4]
            if allowed_yn == 'N':
                continue  # skip row if not permitted

            # use commodities dictionary to determine commodity name
            commodity_name = commodities_dict[commodity_id]

            # populate commodity modes dictionary
            if phase not in commodity_mode_dict:
                # add new phase to dictionary and start commodity and mode dictionary
                commodity_mode_dict[phase] = {commodity_name: {mode: vehicle_label}}
            elif commodity_name not in commodity_mode_dict[phase]:
                # add new commodity to dictionary and start mode dictionary
                commodity_mode_dict[phase][commodity_name] = {mode: vehicle_label}
            else:
                # add new mode to dictionary
                commodity_mode_dict[phase][commodity_name][mode] = vehicle_label

        # query vehicle types table
        vehicle_types_dict = {}  # key off mode
        vehs = main_db_con.execute("select * from vehicle_types;")
        vehs = vehs.fetchall()
        for row in vehs:
            mode = row[0]
            vehicle_label = row[1]
            property_name = row[2]
            property_value = Q_(row[3])
            if mode not in vehicle_types_dict:
                # add new mode to dictionary and start vehicle and property dictionary
                vehicle_types_dict[mode] = {vehicle_label: {property_name: property_value}}
            elif vehicle_label not in vehicle_types_dict[mode]:
                # add new vehicle to dictionary and start property dictionary
                vehicle_types_dict[mode][vehicle_label] = {property_name: property_value}
            else:
                # add to existing dictionary entry
                vehicle_types_dict[mode][vehicle_label][property_name] = property_value

    # load emissions data
    factors_dict = make_emission_factors_dict(the_scenario, logger)

    # load density data
    density_dict = make_commodity_density_dict(the_scenario, logger)

    logger.debug("----- commodity/vehicle attribute table -----")

    # create commodity/vehicle attribute dictionary
    attribute_dict = {}  # key off commodity name
    for phase in commodity_mode_dict:
        for commodity_name in commodity_mode_dict[phase]:
            for mode in commodity_mode_dict[phase][commodity_name]:

                # Get vehicle assigned to commodity on this mode
                vehicle_label = commodity_mode_dict[phase][commodity_name][mode]

                if commodity_name not in attribute_dict:
                    # Create dictionary entry for commodity
                    attribute_dict[commodity_name] = {mode: {}}
                else:
                    # Create dictionary entry for commodity's mode
                    attribute_dict[commodity_name][mode] = {}

                # --- EMISSIONS ---
                # Copy dictionary for pollutant with key = link type and value = emissions factor
                for pollutant in factors_dict[mode][vehicle_label]:
                    attribute_dict[commodity_name][mode][pollutant] = dict(factors_dict[mode][vehicle_label][pollutant])

                    # Use commodity density to convert emissions factors to g/default liquid unit/default distance unit for non-road links
                    if phase == 'liquid' and mode != 'road':
                        for link_type in attribute_dict[commodity_name][mode][pollutant]:
                            if link_type != 'artificial':
                                attribute_dict[commodity_name][mode][pollutant][link_type] = density_dict[commodity_name] * attribute_dict[commodity_name][mode][pollutant][link_type]

                # --- ARTIFICIAL LINKS ---
                if phase == 'solid':
                    attribute_dict[commodity_name][mode]['Load'] = {'artificial' : the_scenario.truck_load_solid}
                else:
                    attribute_dict[commodity_name][mode]['Load'] = {'artificial' : the_scenario.truck_load_liquid}

                # --- VEHICLE ATTRIBUTES ---
                # Set remaining attributes based on mode, vehicle label, and commodity phase

                # ROAD
                if mode == 'road':
                    
                    if vehicle_label == 'Default':
                        # use default attributes
                        attribute_dict[commodity_name][mode]['Fuel_Efficiency'] = the_scenario.truckFuelEfficiency
                        if phase == 'liquid':
                            attribute_dict[commodity_name][mode]['Load']['general'] = the_scenario.truck_load_liquid
                        else:
                            attribute_dict[commodity_name][mode]['Load']['general'] = the_scenario.truck_load_solid

                    elif vehicle_label != 'NA':
                        # use custom vehicle attributes
                        attribute_dict[commodity_name][mode]['Fuel_Efficiency'] = vehicle_types_dict[mode][vehicle_label]['Truck_Fuel_Efficiency']
                        if phase == 'liquid':
                            attribute_dict[commodity_name][mode]['Load']['general'] = vehicle_types_dict[mode][vehicle_label]['Truck_Load_Liquid']
                        else:
                            attribute_dict[commodity_name][mode]['Load']['general'] = vehicle_types_dict[mode][vehicle_label]['Truck_Load_Solid']

                # RAIL
                elif mode == 'rail':

                    if vehicle_label == 'Default':
                        # use default attributes
                        attribute_dict[commodity_name][mode]['Fuel_Efficiency'] = the_scenario.railFuelEfficiency
                        if phase == 'liquid':
                            attribute_dict[commodity_name][mode]['Load']['general'] = the_scenario.railcar_load_liquid
                        else:
                            attribute_dict[commodity_name][mode]['Load']['general'] = the_scenario.railcar_load_solid

                    elif vehicle_label != 'NA':
                        # use custom vehicle attributes
                        attribute_dict[commodity_name][mode]['Fuel_Efficiency'] = vehicle_types_dict[mode][vehicle_label]['Railcar_Fuel_Efficiency']
                        if phase == 'liquid':
                            attribute_dict[commodity_name][mode]['Load']['general'] = vehicle_types_dict[mode][vehicle_label]['Railcar_Load_Liquid']
                        else:
                            attribute_dict[commodity_name][mode]['Load']['general'] = vehicle_types_dict[mode][vehicle_label]['Railcar_Load_Solid']

                # WATER
                elif mode == 'water':

                    if vehicle_label == 'Default':
                        # use default attributes
                        attribute_dict[commodity_name][mode]['Fuel_Efficiency'] = the_scenario.bargeFuelEfficiency
                        if phase == 'liquid':
                            attribute_dict[commodity_name][mode]['Load']['general'] = the_scenario.barge_load_liquid
                        else:
                            attribute_dict[commodity_name][mode]['Load']['general'] = the_scenario.barge_load_solid

                    elif vehicle_label != 'NA':
                        # use custom vehicle attributes
                        attribute_dict[commodity_name][mode]['Fuel_Efficiency'] = vehicle_types_dict[mode][vehicle_label]['Barge_Fuel_Efficiency']
                        if phase == 'liquid':
                            attribute_dict[commodity_name][mode]['Load']['general'] = vehicle_types_dict[mode][vehicle_label]['Barge_Load_Liquid']
                        else:
                            attribute_dict[commodity_name][mode]['Load']['general'] = vehicle_types_dict[mode][vehicle_label]['Barge_Load_Solid']
                
                # PIPELINE
                # Note: liquids only, no custom vehicles
                elif mode == 'pipeline_crude_trf_rts':
                    attribute_dict[commodity_name][mode]['Load']['general'] = the_scenario.pipeline_crude_load_liquid
                elif mode == 'pipeline_prod_trf_rts':
                    attribute_dict[commodity_name][mode]['Load']['general'] = the_scenario.pipeline_prod_load_liquid
                
                # --- FUEL EFFICIENCY ---
                # Use commodity density to convert fuel efficiencies to default liquid unit * default distance unit / gal for non-pipeline liquid commodities
                if phase == 'liquid' and 'pipeline' not in mode:
                    attribute_dict[commodity_name][mode]['Fuel_Efficiency'] = attribute_dict[commodity_name][mode]['Fuel_Efficiency'] / density_dict[commodity_name]

                # Check if user assigns custom vehicle without detailed emissions factors
                if mode in vehicle_types_dict and vehicle_label in vehicle_types_dict[mode]:
                    # user used a custom vehicle. warn if don't have matching emissions factors
                    if mode not in factors_dict or vehicle_label not in factors_dict[mode]:
                        logger.warning("Detailed emissions factors are not specified for vehicle: {} for mode: {}. Excluding this vehicle from the emissions report.".format(vehicle_label, mode))
        
    with sqlite3.connect(the_scenario.main_db) as db_con:
        
        db_con.executescript("""
        drop table if exists commodity_vehicle_attrs;
        create table commodity_vehicle_attrs(
        commodity text,
        mode text,
        attribute text,
        link_type text,
        value text, -- incl. units, no need to separate out in this table
        constraint unique_elements unique(commodity, mode, attribute, link_type))
        ;""")

        for commodity in attribute_dict:
            for mode in attribute_dict[commodity]:
                for attribute in attribute_dict[commodity][mode]:
                    if attribute in ['Fuel_Efficiency']:
                        value = attribute_dict[commodity][mode][attribute]

                        sql_nonemissions = """insert into commodity_vehicle_attrs
                                              (commodity, mode, attribute, link_type, value)
                                              values ('{}', '{}', '{}', NULL, '{}')
                                              ;""".format(commodity, mode, attribute, value)
                        db_con.execute(sql_nonemissions)
                        
                    else:
                        # emissions, load
                        for link_type in attribute_dict[commodity][mode][attribute]:
                            value = attribute_dict[commodity][mode][attribute][link_type]

                            sql_emissions = """insert into commodity_vehicle_attrs
                                               (commodity, mode, attribute, link_type, value)
                                               values ('{}', '{}', '{}', '{}', '{}')
                                               ;""".format(commodity, mode, attribute, link_type, value)
                            db_con.execute(sql_emissions)

    return attribute_dict  # Keyed off of commodity name, then mode, then vehicle attribute, then link type (unless Fuel_Efficiency)


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
    # does not have flow restrictions applied to it

    # assumes that the only artificial links in the network
    # are connecting intermodal facilities

    logger.info("start: set_intermodal_links")

    for mode in the_scenario.permittedModes:
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

    crop_yield_dict = {}  # keyed off crop name --> crop yield (e.g., kg/ha/yr)
    fuel_yield_dict = {}  # keyed off crop name + processing type --> fuel yields (e.g., jet, diesel, total biomass)

    afpatAsList = []

    # iterate through the afpat table and save as a list
    # --------------------------------------------
    with arcpy.da.SearchCursor(fullPathToTable, "*") as cursor:

        for row in cursor:

            afpatAsList.append(row)

    # now identify the relevant indexes for the values we want to extract

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

    # fuel yields index
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

            # concatenate crop name
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

            # preprocYield is independent of processor type, so it's okay
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


def get_coordinate_system(the_scenario):

    feature_dataset = os.path.join(the_scenario.main_gdb, "network")
    scenario_proj = arcpy.Describe(feature_dataset).spatialReference

    return scenario_proj


# **********************************************************************
# File name: Zip.py
# Description:
#    Zips the contents of a folder, file geodatabase or ArcInfo workspace
#    containing coverages into a zip file.
# Arguments:
#    0 - Input workspace
#    1 - Output zip file. A .zip extension should be added to the file name
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

