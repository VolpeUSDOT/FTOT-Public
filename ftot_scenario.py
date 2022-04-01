
#---------------------------------------------------------------------------------------------------
# Name: ftot_scenario
#
# Purpose: declare all of the attributes of the_scenario object.
# create getter and setter methods for each attribute.
#
#---------------------------------------------------------------------------------------------------



import os
import sys
from xml.dom import minidom
from ftot import SCHEMA_VERSION
from ftot import Q_, ureg
import sqlite3
        
try:
    from lxml import etree
except ImportError:
    print ("This script requires the lxml Python library to validate the XML scenario file.")
    print("Download the library here: https://pypi.python.org/pypi/lxml/2.3")
    print("Exiting...")
    sys.exit()

#===================================================================================================

def getElementFromXmlFile(xmlFile, elementName):
    return xmlFile.getElementsByTagName(elementName)[0].firstChild.data

#===================================================================================================

def format_number(numString):
    """Removes any number formatting, i.e., thousand's separator or dollar sign"""

    if numString.rfind(",") > -1:
        numString = numString.replace(",", "")
    if numString.rfind("$") > -1:
        numString = numString.replace("$", "")
    return float(numString)

#===================================================================================================

class Scenario:
    def __init__(self):
        pass

def load_scenario_config_file(fullPathToXmlConfigFile, fullPathToXmlSchemaFile, logger):

    if not os.path.exists(fullPathToXmlConfigFile):
        raise IOError("XML Scenario File {} not found at specified location.".format(fullPathToXmlConfigFile))

    if fullPathToXmlConfigFile.rfind(".xml") < 0:
        raise IOError("XML Scenario File {} is not an XML file type.".format(fullPathToXmlConfigFile))

    if not os.path.exists(fullPathToXmlSchemaFile):
        raise IOError("XML Schema File not found at {}".format(fullPathToXmlSchemaFile))

    xmlScenarioFile = minidom.parse(fullPathToXmlConfigFile)

    # Validate XML scenario against XML schema
    schemaObj = etree.XMLSchema(etree.parse(fullPathToXmlSchemaFile))
    xmlFileObj = etree.parse(fullPathToXmlConfigFile)
    validationResult = schemaObj.validate(xmlFileObj)

    logger.debug("validate XML scenario against XML schema")
    if validationResult == False:
        logger.warning("XML scenario validation failed. Error messages to follow.")
        for error in schemaObj.error_log:
            logger.warning("ERROR ON LINE: {} - ERROR MESSAGE: {}".format(error.line, error.message))

        raise Exception("XML Scenario File does not meet the requirements in the XML schema file.")

    # initialize scenario ojbect
    logger.debug("initialize scenario object")
    scenario = Scenario()

    # Check the scenario schema version (current version is specified in FTOT.py under VERSION_NUMBER global var)
    logger.debug("validate schema version is correct")
    scenario.scenario_schema_version = xmlScenarioFile.getElementsByTagName('Scenario_Schema_Version')[0].firstChild.data

    #if not str(VERSION_NUMBER) == str(scenario.scenario_schema_version):
    if not str(SCHEMA_VERSION).split(".")[0:2] == str(scenario.scenario_schema_version).split(".")[0:2]:
        error = "XML Schema File Version is {}. Expected version {}. " \
                "Use the XML flag to run the XML upgrade tool. " \
                .format(str(scenario.scenario_schema_version), str(SCHEMA_VERSION))
        logger.error(error)
        raise Exception(error)

    scenario.scenario_name = xmlScenarioFile.getElementsByTagName('Scenario_Name')[0].firstChild.data
    # Convert any commas in the scenario name to dashes
    warning = "Replace any commas in the scenario name with dashes to accomodate csv files."
    logger.debug(warning)
    scenario.scenario_name = scenario.scenario_name.replace(",", "-")
    scenario.scenario_description = xmlScenarioFile.getElementsByTagName('Scenario_Description')[0].firstChild.data

    # SCENARIO INPUTS SECTION
    # ----------------------------------------------------------------------------------------
    scenario.common_data_folder = xmlScenarioFile.getElementsByTagName('Common_Data_Folder')[0].firstChild.data
    scenario.base_network_gdb = xmlScenarioFile.getElementsByTagName('Base_Network_Gdb')[0].firstChild.data
    scenario.disruption_data = xmlScenarioFile.getElementsByTagName('Disruption_Data')[0].firstChild.data
    scenario.base_rmp_layer = xmlScenarioFile.getElementsByTagName('Base_RMP_Layer')[0].firstChild.data
    scenario.base_destination_layer = xmlScenarioFile.getElementsByTagName('Base_Destination_Layer')[0].firstChild.data
    scenario.base_processors_layer = xmlScenarioFile.getElementsByTagName('Base_Processors_Layer')[0].firstChild.data

    scenario.rmp_commodity_data = xmlScenarioFile.getElementsByTagName('RMP_Commodity_Data')[0].firstChild.data
    scenario.destinations_commodity_data = xmlScenarioFile.getElementsByTagName('Destinations_Commodity_Data')[0].firstChild.data
    scenario.processors_commodity_data = xmlScenarioFile.getElementsByTagName('Processors_Commodity_Data')[0].firstChild.data
    scenario.processors_candidate_slate_data = xmlScenarioFile.getElementsByTagName('Processors_Candidate_Commodity_Data')[0].firstChild.data
    # note: the processor_candidates_data is defined under other since it is not a user specified file.
    scenario.schedule = xmlScenarioFile.getElementsByTagName('Schedule_Data')[0].firstChild.data
    scenario.commodity_mode_data = xmlScenarioFile.getElementsByTagName('Commodity_Mode_Data')[0].firstChild.data

    if len(xmlScenarioFile.getElementsByTagName('Commodity_Density_Data')):
        scenario.commodity_density_data = xmlScenarioFile.getElementsByTagName('Commodity_Density_Data')[0].firstChild.data
        if scenario.commodity_density_data != "None":
            assert os.path.exists(scenario.commodity_density_data), 'Cannot find commodity_density_data file: {}'.format(scenario.commodity_density_data)
    else:
        scenario.commodity_density_data = "None"

    # use pint to set the default units
    logger.debug("test: setting the default units with pint")
    try:
        scenario.default_units_solid_phase = Q_(xmlScenarioFile.getElementsByTagName('Default_Units_Solid_Phase')[0].firstChild.data).units
        scenario.default_units_liquid_phase = Q_(xmlScenarioFile.getElementsByTagName('Default_Units_Liquid_Phase')[0].firstChild.data).units
        logger.debug("PASS: setting the default units with pint")
    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))

    # ASSUMPTIONS SECTION
    # ----------------------------------------------------------------------------------------

    # solid and liquid vehicle loads
    try:

        logger.debug("test: setting the vehicle loads for solid phase of matter with pint")
        scenario.truck_load_solid = Q_(xmlScenarioFile.getElementsByTagName('Truck_Load_Solid')[0].firstChild.data).to(scenario.default_units_solid_phase)
        scenario.railcar_load_solid = Q_(xmlScenarioFile.getElementsByTagName('Railcar_Load_Solid')[0].firstChild.data).to(scenario.default_units_solid_phase)
        scenario.barge_load_solid = Q_(xmlScenarioFile.getElementsByTagName('Barge_Load_Solid')[0].firstChild.data).to(scenario.default_units_solid_phase)

        logger.debug("test: setting the vehicle loads for liquid phase of matter with pint")
        scenario.truck_load_liquid = Q_(xmlScenarioFile.getElementsByTagName('Truck_Load_Liquid')[0].firstChild.data).to(scenario.default_units_liquid_phase)
        scenario.railcar_load_liquid = Q_(xmlScenarioFile.getElementsByTagName('Railcar_Load_Liquid')[0].firstChild.data).to(scenario.default_units_liquid_phase)
        scenario.barge_load_liquid = Q_(xmlScenarioFile.getElementsByTagName('Barge_Load_Liquid')[0].firstChild.data).to(scenario.default_units_liquid_phase)
        scenario.pipeline_crude_load_liquid = Q_(xmlScenarioFile.getElementsByTagName('Pipeline_Crude_Load_Liquid')[0].firstChild.data).to(scenario.default_units_liquid_phase)
        scenario.pipeline_prod_load_liquid = Q_(xmlScenarioFile.getElementsByTagName('Pipeline_Prod_Load_Liquid')[0].firstChild.data).to(scenario.default_units_liquid_phase)
        logger.debug("PASS: setting the vehicle loads with pint passed")

        logger.debug("test: setting the vehicle fuel efficiencies with pint")
        scenario.truckFuelEfficiency = Q_(xmlScenarioFile.getElementsByTagName('Truck_Fuel_Efficiency')[0].firstChild.data).to('mi/gal')
        scenario.railFuelEfficiency = Q_(xmlScenarioFile.getElementsByTagName('Rail_Fuel_Efficiency')[0].firstChild.data).to('mi/gal')
        scenario.bargeFuelEfficiency = Q_(xmlScenarioFile.getElementsByTagName('Barge_Fuel_Efficiency')[0].firstChild.data).to('mi/gal')
        logger.debug("PASS: setting the vehicle fuel efficiencies with pint passed")

        logger.debug("test: setting the vehicle emission factors with pint")
        scenario.CO2urbanUnrestricted = Q_(xmlScenarioFile.getElementsByTagName('Atmos_CO2_Urban_Unrestricted')[0].firstChild.data).to('g/mi')
        scenario.CO2urbanRestricted = Q_(xmlScenarioFile.getElementsByTagName('Atmos_CO2_Urban_Restricted')[0].firstChild.data).to('g/mi')
        scenario.CO2ruralUnrestricted = Q_(xmlScenarioFile.getElementsByTagName('Atmos_CO2_Rural_Unrestricted')[0].firstChild.data).to('g/mi')
        scenario.CO2ruralRestricted = Q_(xmlScenarioFile.getElementsByTagName('Atmos_CO2_Rural_Restricted')[0].firstChild.data).to('g/mi')        
        scenario.railroadCO2Emissions = Q_(xmlScenarioFile.getElementsByTagName('Railroad_CO2_Emissions')[0].firstChild.data).to('g/{}/mi'.format(scenario.default_units_solid_phase))
        scenario.bargeCO2Emissions = Q_(xmlScenarioFile.getElementsByTagName('Barge_CO2_Emissions')[0].firstChild.data).to('g/{}/mi'.format(scenario.default_units_solid_phase))
        scenario.pipelineCO2Emissions = Q_(xmlScenarioFile.getElementsByTagName('Pipeline_CO2_Emissions')[0].firstChild.data).to('g/{}/mi'.format(scenario.default_units_solid_phase))
        # setting density conversion based on 'Density_Conversion_Factor' field if it exists, or otherwise default to 3.33 ton/kgal
        if len(xmlScenarioFile.getElementsByTagName('Density_Conversion_Factor')):
            scenario.densityFactor = Q_(xmlScenarioFile.getElementsByTagName('Density_Conversion_Factor')[0].firstChild.data).to('{}/{}'.format(scenario.default_units_solid_phase, scenario.default_units_liquid_phase))
        else:
            if scenario.commodity_density_data == "None":
                # User didn't specify a density file OR factor
                logger.warning("FTOT is assuming a density of 3.33 ton/kgal for emissions reporting for liquids. Use scenario XML parameter 'Density_Conversion_Factor' to adjust this value.")
            scenario.densityFactor = Q_('3.33 ton/kgal').to('{}/{}'.format(scenario.default_units_solid_phase, scenario.default_units_liquid_phase))
        logger.debug("PASS: setting the vehicle emission factors with pint passed")
    
    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))
    
    # Setting flag for detailed emissions reporting if it exists, or otherwise default to 'False'
    if len(xmlScenarioFile.getElementsByTagName('Detailed_Emissions_Reporting')):
        if xmlScenarioFile.getElementsByTagName('Detailed_Emissions_Reporting')[0].firstChild.data == "True":
            scenario.detailed_emissions = True
        else:
            scenario.detailed_emissions = False
    else:
        logger.debug("Detailed_Emissions_Reporting field not specified. Defaulting to False.")
        scenario.detailed_emissions = False

        # SCRIPT PARAMETERS SECTION FOR NETWORK
    # ----------------------------------------------------------------------------------------

    # rail costs
    try:
        logger.debug("test: setting the base costs for rail with pint")
        scenario.solid_railroad_class_1_cost = Q_(xmlScenarioFile.getElementsByTagName('solid_Railroad_Class_I_Cost')[0].firstChild.data).to("usd/{}/mile".format(scenario.default_units_solid_phase))
        scenario.liquid_railroad_class_1_cost = Q_(xmlScenarioFile.getElementsByTagName('liquid_Railroad_Class_I_Cost')[0].firstChild.data).to("usd/{}/mile".format(scenario.default_units_liquid_phase))
        logger.debug("PASS: setting the base costs for rail with pint passed")

    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))

    # rail penalties
    scenario.rail_dc_7 = format_number(xmlScenarioFile.getElementsByTagName('Rail_Density_Code_7_Weight')[0].firstChild.data)
    scenario.rail_dc_6 = format_number(xmlScenarioFile.getElementsByTagName('Rail_Density_Code_6_Weight')[0].firstChild.data)
    scenario.rail_dc_5 = format_number(xmlScenarioFile.getElementsByTagName('Rail_Density_Code_5_Weight')[0].firstChild.data)
    scenario.rail_dc_4 = format_number(xmlScenarioFile.getElementsByTagName('Rail_Density_Code_4_Weight')[0].firstChild.data)
    scenario.rail_dc_3 = format_number(xmlScenarioFile.getElementsByTagName('Rail_Density_Code_3_Weight')[0].firstChild.data)
    scenario.rail_dc_2 = format_number(xmlScenarioFile.getElementsByTagName('Rail_Density_Code_2_Weight')[0].firstChild.data)
    scenario.rail_dc_1 = format_number(xmlScenarioFile.getElementsByTagName('Rail_Density_Code_1_Weight')[0].firstChild.data)
    scenario.rail_dc_0 = format_number(xmlScenarioFile.getElementsByTagName('Rail_Density_Code_0_Weight')[0].firstChild.data)

    # truck costs
    try:
        logger.debug("test: setting the base costs for truck with pint")
        scenario.solid_truck_base_cost = Q_(xmlScenarioFile.getElementsByTagName('solid_Truck_Base_Cost')[0].firstChild.data).to("usd/{}/mile".format(scenario.default_units_solid_phase))
        scenario.liquid_truck_base_cost = Q_(xmlScenarioFile.getElementsByTagName('liquid_Truck_Base_Cost')[0].firstChild.data).to("usd/{}/mile".format(scenario.default_units_liquid_phase))
        logger.debug("PASS: setting the base costs for truck with pint passed")

    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))

    # road penalties
    scenario.truck_interstate = format_number(xmlScenarioFile.getElementsByTagName('Truck_Interstate_Weight')[0].firstChild.data)
    scenario.truck_pr_art = format_number(xmlScenarioFile.getElementsByTagName('Truck_Principal_Arterial_Weight')[0].firstChild.data)
    scenario.truck_m_art = format_number(xmlScenarioFile.getElementsByTagName('Truck_Minor_Arterial_Weight')[0].firstChild.data)
    scenario.truck_local = format_number(xmlScenarioFile.getElementsByTagName('Truck_Local_Weight')[0].firstChild.data)

    # barge costs
    try:
        logger.debug("test: setting the base costs for barge with pint")
        scenario.solid_barge_cost = Q_(xmlScenarioFile.getElementsByTagName('solid_Barge_cost')[0].firstChild.data).to("usd/{}/mile".format(scenario.default_units_solid_phase))
        scenario.liquid_barge_cost = Q_(xmlScenarioFile.getElementsByTagName('liquid_Barge_cost')[0].firstChild.data).to("usd/{}/mile".format(scenario.default_units_liquid_phase))
        logger.debug("PASS: setting the base costs for barge with pint passed")

    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))

    # water penalties
    scenario.water_high_vol = format_number(xmlScenarioFile.getElementsByTagName('Water_High_Volume_Weight')[0].firstChild.data)
    scenario.water_med_vol = format_number(xmlScenarioFile.getElementsByTagName('Water_Medium_Volume_Weight')[0].firstChild.data)
    scenario.water_low_vol = format_number(xmlScenarioFile.getElementsByTagName('Water_Low_Volume_Weight')[0].firstChild.data)
    scenario.water_no_vol = format_number(xmlScenarioFile.getElementsByTagName('Water_No_Volume_Weight')[0].firstChild.data)

    # transloading costs
    try:
        logger.debug("test: setting the transloading costs with pint")
        scenario.solid_transloading_cost = Q_(xmlScenarioFile.getElementsByTagName('solid_Transloading_Cost')[0].firstChild.data).to("usd/{}".format(scenario.default_units_solid_phase))
        scenario.liquid_transloading_cost = Q_(xmlScenarioFile.getElementsByTagName('liquid_Transloading_Cost')[0].firstChild.data).to("usd/{}".format(scenario.default_units_liquid_phase))
        logger.debug("PASS: setting the transloading costs with pint passed")

    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))

    # artificial link distances
    try:
        logger.debug("test: setting the artificial link distances with pint")
        scenario.road_max_artificial_link_dist = Q_(xmlScenarioFile.getElementsByTagName('Road_Max_Artificial_Link_Distance')[0].firstChild.data).to('mi')
        scenario.rail_max_artificial_link_dist = Q_(xmlScenarioFile.getElementsByTagName('Rail_Max_Artificial_Link_Distance')[0].firstChild.data).to('mi')
        scenario.water_max_artificial_link_dist = Q_(xmlScenarioFile.getElementsByTagName('Water_Max_Artificial_Link_Distance')[0].firstChild.data).to('mi')
        scenario.pipeline_crude_max_artificial_link_dist = Q_(xmlScenarioFile.getElementsByTagName('Pipeline_Crude_Max_Artificial_Link_Distance')[0].firstChild.data).to('mi')
        scenario.pipeline_prod_max_artificial_link_dist = Q_(xmlScenarioFile.getElementsByTagName('Pipeline_Products_Max_Artificial_Link_Distance')[0].firstChild.data).to('mi')
        logger.debug("PASS: setting the artificial link distances with pint passed")

    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))

    # short haul penalties
    try:
        logger.debug("test: setting the short haul penalties with pint")
        scenario.liquid_rail_short_haul_penalty = Q_(xmlScenarioFile.getElementsByTagName('liquid_Rail_Short_Haul_Penalty')[0].firstChild.data).to("usd/{}".format(scenario.default_units_liquid_phase))
        scenario.solid_rail_short_haul_penalty = Q_(xmlScenarioFile.getElementsByTagName('solid_Rail_Short_Haul_Penalty')[0].firstChild.data).to("usd/{}".format(scenario.default_units_solid_phase))
        scenario.liquid_water_short_haul_penalty = Q_(xmlScenarioFile.getElementsByTagName('liquid_Water_Short_Haul_Penalty')[0].firstChild.data).to("usd/{}".format(scenario.default_units_liquid_phase))
        scenario.solid_water_short_haul_penalty = Q_(xmlScenarioFile.getElementsByTagName('solid_Water_Short_Haul_Penalty')[0].firstChild.data).to("usd/{}".format(scenario.default_units_solid_phase))
        logger.debug("PASS: setting the short haul penalties with pint passed")

    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))

    # RUN ROUTE OPTIMIZATION SCRIPT SECTION
    # ----------------------------------------------------------------------------------------

    # Setting flag for network density reduction based on 'NDR_On' field
    if xmlScenarioFile.getElementsByTagName('NDR_On')[0].firstChild.data == "True":
        scenario.ndrOn = True
    else:
        scenario.ndrOn = False

    scenario.permittedModes = []
    if xmlScenarioFile.getElementsByTagName('Permitted_Modes')[0].getElementsByTagName('Road')[0].firstChild.data == "True":
        scenario.permittedModes.append("road")
    if xmlScenarioFile.getElementsByTagName('Permitted_Modes')[0].getElementsByTagName('Rail')[0].firstChild.data == "True":
        scenario.permittedModes.append("rail")
    if xmlScenarioFile.getElementsByTagName('Permitted_Modes')[0].getElementsByTagName('Water')[0].firstChild.data == "True":
        scenario.permittedModes.append("water")

    # TODO ALO-- 10/17/2018-- make below compatible with distinct crude/product pipeline approach-- ftot_pulp.py changes will be needed.
    if xmlScenarioFile.getElementsByTagName('Permitted_Modes')[0].getElementsByTagName('Pipeline_Crude')[0].firstChild.data == "True":
        scenario.permittedModes.append("pipeline_crude_trf_rts")
    if xmlScenarioFile.getElementsByTagName('Permitted_Modes')[0].getElementsByTagName('Pipeline_Prod')[0].firstChild.data == "True":
        scenario.permittedModes.append("pipeline_prod_trf_rts")

    if xmlScenarioFile.getElementsByTagName('Capacity_On')[0].firstChild.data == "True":
        scenario.capacityOn = True
    else:
        scenario.capacityOn = False

    scenario.backgroundFlowModes = []
    if xmlScenarioFile.getElementsByTagName('Background_Flows')[0].getElementsByTagName('Road')[0].firstChild.data == "True":
        scenario.backgroundFlowModes.append("road")
    if xmlScenarioFile.getElementsByTagName('Background_Flows')[0].getElementsByTagName('Rail')[0].firstChild.data == "True":
        scenario.backgroundFlowModes.append("rail")
    if xmlScenarioFile.getElementsByTagName('Background_Flows')[0].getElementsByTagName('Water')[0].firstChild.data == "True":
        scenario.backgroundFlowModes.append("water")

    if xmlScenarioFile.getElementsByTagName('Background_Flows')[0].getElementsByTagName('Pipeline_Crude')[0].firstChild.data == "True":
        scenario.backgroundFlowModes.append("pipeline")
    if xmlScenarioFile.getElementsByTagName('Background_Flows')[0].getElementsByTagName('Pipeline_Prod')[0].firstChild.data == "True":
        scenario.backgroundFlowModes.append("pipeline")

    scenario.minCapacityLevel = float(xmlScenarioFile.getElementsByTagName('Minimum_Capacity_Level')[0].firstChild.data)

    scenario.unMetDemandPenalty = float(xmlScenarioFile.getElementsByTagName('Unmet_Demand_Penalty')[0].firstChild.data)
     
    # OTHER
    # ----------------------------------------------------------------------------------------

    scenario.scenario_run_directory = os.path.dirname(fullPathToXmlConfigFile)

    scenario.main_db = os.path.join(scenario.scenario_run_directory, "main.db")
    scenario.main_gdb = os.path.join(scenario.scenario_run_directory, "main.gdb")

    scenario.rmp_fc          = os.path.join(scenario.main_gdb, "raw_material_producers")
    scenario.destinations_fc = os.path.join(scenario.main_gdb, "ultimate_destinations")
    scenario.processors_fc   = os.path.join(scenario.main_gdb, "processors")
    scenario.processor_candidates_fc = os.path.join(scenario.main_gdb, "all_candidate_processors")
    scenario.locations_fc    = os.path.join(scenario.main_gdb, "locations")

    # this file is generated by the processor_candidates() method
    scenario.processor_candidates_commodity_data = os.path.join(scenario.scenario_run_directory, "debug", "ftot_generated_processor_candidates.csv")

    # this is the directory to store the shp files that a programtically generated for the networkx read_shp method
    scenario.networkx_files_dir = os.path.join(scenario.scenario_run_directory, "temp_networkx_shp_files")

    return scenario

#===================================================================================================


def dump_scenario_info_to_report(the_scenario, logger):
    logger.config("xml_scenario_name: \t{}".format(the_scenario.scenario_name))
    logger.config("xml_scenario_description: \t{}".format(the_scenario.scenario_description))
    logger.config("xml_scenario_run_directory: \t{}".format(the_scenario.scenario_run_directory))
    logger.config("xml_scenario_schema_version: \t{}".format(the_scenario.scenario_schema_version))

    logger.config("xml_common_data_folder: \t{}".format(the_scenario.common_data_folder))

    logger.config("xml_base_network_gdb: \t{}".format(the_scenario.base_network_gdb))
    logger.config("xml_disruption_data: \t{}".format(the_scenario.disruption_data))

    logger.config("xml_base_rmp_layer: \t{}".format(the_scenario.base_rmp_layer))
    logger.config("xml_base_destination_layer: \t{}".format(the_scenario.base_destination_layer))
    logger.config("xml_base_processors_layer: \t{}".format(the_scenario.base_processors_layer))

    logger.config("xml_rmp_commodity_data: \t{}".format(the_scenario.rmp_commodity_data))
    logger.config("xml_destinations_commodity_data: \t{}".format(the_scenario.destinations_commodity_data))
    logger.config("xml_processors_commodity_data: \t{}".format(the_scenario.processors_commodity_data))
    logger.config("xml_processors_candidate_slate_data: \t{}".format(the_scenario.processors_candidate_slate_data))

    logger.config("xml_schedule_data: \t{}".format(the_scenario.schedule))
    logger.config("xml_commodity_mode_data: \t{}".format(the_scenario.commodity_mode_data))
    logger.config("xml_commodity_density_data: \t{}".format(the_scenario.commodity_density_data))

    logger.config("xml_default_units_solid_phase: \t{}".format(the_scenario.default_units_solid_phase))
    logger.config("xml_default_units_liquid_phase: \t{}".format(the_scenario.default_units_liquid_phase))

    logger.config("xml_truck_load_solid: \t{}".format(the_scenario.truck_load_solid))
    logger.config("xml_railcar_load_solid: \t{}".format(the_scenario.railcar_load_solid))
    logger.config("xml_barge_load_solid: \t{}".format(the_scenario.barge_load_solid))

    logger.config("xml_truck_load_liquid: \t{}".format(the_scenario.truck_load_liquid))
    logger.config("xml_railcar_load_liquid: \t{}".format(the_scenario.railcar_load_liquid))
    logger.config("xml_barge_load_liquid: \t{}".format(the_scenario.barge_load_liquid))
    logger.config("xml_pipeline_crude_load_liquid: \t{}".format(the_scenario.pipeline_crude_load_liquid))
    logger.config("xml_pipeline_prod_load_liquid: \t{}".format(the_scenario.pipeline_prod_load_liquid))

    logger.config("xml_solid_railroad_class_1_cost: \t{}".format(the_scenario.solid_railroad_class_1_cost))
    logger.config("xml_liquid_railroad_class_1_cost: \t{}".format(the_scenario.liquid_railroad_class_1_cost))
    logger.config("xml_rail_dc_7: \t{}".format(the_scenario.rail_dc_7))
    logger.config("xml_rail_dc_6: \t{}".format(the_scenario.rail_dc_6))
    logger.config("xml_rail_dc_5: \t{}".format(the_scenario.rail_dc_5))
    logger.config("xml_rail_dc_4: \t{}".format(the_scenario.rail_dc_4))
    logger.config("xml_rail_dc_3: \t{}".format(the_scenario.rail_dc_3))
    logger.config("xml_rail_dc_2: \t{}".format(the_scenario.rail_dc_2))
    logger.config("xml_rail_dc_1: \t{}".format(the_scenario.rail_dc_1))
    logger.config("xml_rail_dc_0: \t{}".format(the_scenario.rail_dc_0))

    logger.config("xml_liquid_truck_base_cost: \t{}".format(the_scenario.liquid_truck_base_cost))
    logger.config("xml_solid_truck_base_cost: \t{}".format(the_scenario.solid_truck_base_cost))
    logger.config("xml_truck_interstate: \t{}".format(the_scenario.truck_interstate))
    logger.config("xml_truck_pr_art: \t{}".format(the_scenario.truck_pr_art))
    logger.config("xml_truck_m_art: \t{}".format(the_scenario.truck_m_art))
    logger.config("xml_truck_local: \t{}".format(the_scenario.truck_local))

    logger.config("xml_liquid_barge_cost: \t{}".format(the_scenario.liquid_barge_cost))
    logger.config("xml_solid_barge_cost: \t{}".format(the_scenario.solid_barge_cost))
    logger.config("xml_water_high_vol: \t{}".format(the_scenario.water_high_vol))
    logger.config("xml_water_med_vol: \t{}".format(the_scenario.water_med_vol))
    logger.config("xml_water_low_vol: \t{}".format(the_scenario.water_low_vol))
    logger.config("xml_water_no_vol: \t{}".format(the_scenario.water_no_vol))

    logger.config("xml_solid_transloading_cost: \t{}".format(the_scenario.solid_transloading_cost))
    logger.config("xml_liquid_transloading_cost: \t{}".format(the_scenario.liquid_transloading_cost))

    logger.config("xml_road_max_artificial_link_dist: \t{}".format(the_scenario.road_max_artificial_link_dist))
    logger.config("xml_rail_max_artificial_link_dist: \t{}".format(the_scenario.rail_max_artificial_link_dist))
    logger.config("xml_water_max_artificial_link_dist: \t{}".format(the_scenario.water_max_artificial_link_dist))
    logger.config("xml_pipeline_crude_max_artificial_link_dist: \t{}".format(the_scenario.pipeline_crude_max_artificial_link_dist))
    logger.config("xml_pipeline_prod_max_artificial_link_dist: \t{}".format(the_scenario.pipeline_prod_max_artificial_link_dist))

    logger.config("xml_liquid_rail_short_haul_penalty: \t{}".format(the_scenario.liquid_rail_short_haul_penalty))
    logger.config("xml_solid_rail_short_haul_penalty: \t{}".format(the_scenario.solid_rail_short_haul_penalty))
    logger.config("xml_liquid_water_short_haul_penalty: \t{}".format(the_scenario.liquid_water_short_haul_penalty))
    logger.config("xml_solid_water_short_haul_penalty: \t{}".format(the_scenario.solid_water_short_haul_penalty))

    logger.config("xml_truckFuelEfficiency: \t{}".format(the_scenario.truckFuelEfficiency))
    logger.config("xml_bargeFuelEfficiency: \t{}".format(the_scenario.bargeFuelEfficiency))
    logger.config("xml_railFuelEfficiency: \t{}".format(the_scenario.railFuelEfficiency))
    logger.config("xml_CO2urbanUnrestricted: \t{}".format(the_scenario.CO2urbanUnrestricted))
    logger.config("xml_CO2urbanRestricted: \t{}".format(the_scenario.CO2urbanRestricted))
    logger.config("xml_CO2ruralUnrestricted: \t{}".format(the_scenario.CO2ruralUnrestricted))
    logger.config("xml_CO2ruralRestricted: \t{}".format(the_scenario.CO2ruralRestricted))
    logger.config("xml_railroadCO2Emissions: \t{}".format(round(the_scenario.railroadCO2Emissions,2)))
    logger.config("xml_bargeCO2Emissions: \t{}".format(round(the_scenario.bargeCO2Emissions,2)))
    logger.config("xml_pipelineCO2Emissions: \t{}".format(the_scenario.pipelineCO2Emissions))
    logger.config("xml_densityFactor: \t{}".format(the_scenario.densityFactor))

    logger.config("xml_detailed_emissions: \t{}".format(the_scenario.detailed_emissions))
    logger.config("xml_ndrOn: \t{}".format(the_scenario.ndrOn))
    logger.config("xml_permittedModes: \t{}".format(the_scenario.permittedModes))
    logger.config("xml_capacityOn: \t{}".format(the_scenario.capacityOn))
    logger.config("xml_backgroundFlowModes: \t{}".format(the_scenario.backgroundFlowModes))
    logger.config("xml_minCapacityLevel: \t{}".format(the_scenario.minCapacityLevel))
    logger.config("xml_unMetDemandPenalty: \t{}".format(the_scenario.unMetDemandPenalty))

#=======================================================================================================================


def create_scenario_config_db(the_scenario, logger):
    logger.debug("starting make_scenario_config_db")

    # dump the scenario into a db so that FTOT can warn user about any config changes within a scenario run
    with sqlite3.connect(the_scenario.main_db) as db_con:

        # drop the table
        sql = "drop table if exists scenario_config"
        db_con.execute(sql)

        # create the table
        sql = """create table scenario_config(
            permitted_modes text,
            capacity_on text,
            background_flow_modes text
            );"""

        db_con.execute(sql)

        config_list = []
        # format for the optimal route segments table
        config_list.append([str(the_scenario.permittedModes),
                the_scenario.capacityOn,
                str(the_scenario.backgroundFlowModes)])

        logger.debug("done making the config_list")
        with sqlite3.connect(the_scenario.main_db) as db_con:
            insert_sql = """
                INSERT into scenario_config
                values (?,?,?)
                ;"""

            db_con.executemany(insert_sql, config_list)
            logger.debug("finish scenario_config db_con.executemany()")
            db_con.commit()
            logger.debug("finish scenario_config db_con.commit")

def check_scenario_config_db(the_scenario, logger):
    logger.debug("checking consistency of scenario config file with previous step")


def create_network_config_id_table(the_scenario, logger):
    logger.info("start: create_network_config_id_table")

    # connect to the database and set the values
    # ------------------------------------------
    with sqlite3.connect(the_scenario.routes_cache) as db_con:
        # populate the network configuration table with the network config from the scenario object
        # network_config_id  INTEGER PRIMARY KEY,

        sql = """
                insert into network_config (network_config_id,
                                            network_template,
                                            intermodal_network,
                                            road_artificial_link_dist,
                                            rail_artificial_link_dist,
                                            water_artificial_link_dist,
                                            pipeline_crude_artificial_link_dist,
                                            pipeline_prod_artificial_link_dist,
                                            liquid_railroad_class_I_cost,
                                            solid_railroad_class_I_cost,
                                            railroad_density_code_7,
                                            railroad_density_code_6,
                                            railroad_density_code_5,
                                            railroad_density_code_4,
                                            railroad_density_code_3,
                                            railroad_density_code_2,
                                            railroad_density_code_1,
                                            railroad_density_code_0,
                                            liquid_truck_base_cost,
                                            solid_truck_base_cost,
                                            truck_interstate_weight,
                                            truck_principal_arterial_weight,
                                            truck_minor_arterial_weight,
                                            truck_local_weight,
                                            liquid_barge_cost,
                                            solid_barge_cost,
                                            water_high_vol,
                                            water_med_vol,
                                            water_low_vol,
                                            water_no_vol,
                                            solid_transloading_cost,
                                            liquid_transloading_cost,
                                            liquid_rail_short_haul_penalty,
                                            solid_rail_short_haul_penalty,
                                            liquid_water_short_haul_penalty,
                                            solid_water_short_haul_penalty
                                        )
                values (
                        NULL, '{}', '{}', {}, {}, {}, {}, {},  {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});""".format(
            the_scenario.template_network_gdb,
            the_scenario.base_network_gdb,
            the_scenario.road_max_artificial_link_dist,
            the_scenario.rail_max_artificial_link_dist,
            the_scenario.water_max_artificial_link_dist,
            the_scenario.pipeline_crude_max_artificial_link_dist,
            the_scenario.pipeline_prod_max_artificial_link_dist,
            the_scenario.liquid_railroad_class_1_cost.magnitude,
            the_scenario.solid_railroad_class_1_cost.magnitude,
            the_scenario.rail_dc_7,
            the_scenario.rail_dc_6,
            the_scenario.rail_dc_5,
            the_scenario.rail_dc_4,
            the_scenario.rail_dc_3,
            the_scenario.rail_dc_2,
            the_scenario.rail_dc_1,
            the_scenario.rail_dc_0,

            the_scenario.liquid_truck_base_cost.magnitude,
            the_scenario.solid_truck_base_cost.magnitude,
            the_scenario.truck_interstate,
            the_scenario.truck_pr_art,
            the_scenario.truck_m_art,
            the_scenario.truck_local,

            the_scenario.liquid_barge_cost.magnitude,
            the_scenario.solid_barge_cost.magnitude,
            the_scenario.water_high_vol,
            the_scenario.water_med_vol,
            the_scenario.water_low_vol,
            the_scenario.water_no_vol,

            the_scenario.solid_transloading_cost.magnitude,
            the_scenario.liquid_transloading_cost.magnitude,
            
            the_scenario.liquid_rail_short_haul_penalty,
            the_scenario.solid_rail_short_haul_penalty,
            the_scenario.liquid_water_short_haul_penalty,
            the_scenario.solid_water_short_haul_penalty)

        db_con.execute(sql)

        logger.debug("finish: create_network_config_id_table")


# ==============================================================================

def get_network_config_id(the_scenario, logger):
    logger.info("start: get_network_config_id")
    network_config_id = 0

    # check if the database exists
    try:
        # connect to the database and get the network_config_id that matches the scenario
        # -------------------------------------------------------------------------------
        with sqlite3.connect(the_scenario.routes_cache) as db_con:

            sql = """select network_config_id
                     from network_config
                     where
                         network_template = '{}' and
                         intermodal_network = '{}' and
                         road_artificial_link_dist = {} and
                         rail_artificial_link_dist = {} and
                         water_artificial_link_dist = {} and
                         pipeline_crude_artificial_link_dist = {} and
                         pipeline_prod_artificial_link_dist = {} and
                         liquid_railroad_class_I_cost = {} and
                         solid_railroad_class_I_cost = {} and
                         railroad_density_code_7 = {} and
                         railroad_density_code_6 = {} and
                         railroad_density_code_5 = {} and
                         railroad_density_code_4 = {} and
                         railroad_density_code_3 = {} and
                         railroad_density_code_2 = {} and
                         railroad_density_code_1 = {} and
                         railroad_density_code_0 = {} and
                         liquid_truck_base_cost = {} and
                         solid_truck_base_cost = {} and
                         truck_interstate_weight = {} and
                         truck_principal_arterial_weight = {} and
                         truck_minor_arterial_weight = {} and
                         truck_local_weight = {} and
                         liquid_barge_cost = {} and
                         solid_barge_cost = {} and
                         water_high_vol = {} and
                         water_med_vol = {} and
                         water_low_vol = {} and
                         water_no_vol = {} and
                         solid_transloading_cost = {} and
                         liquid_transloading_cost = {} and
                         liquid_rail_short_haul_penalty = {} and
                         solid_rail_short_haul_penalty = {} and
                         liquid_water_short_haul_penalty = {} and
                         solid_water_short_haul_penalty = {}
                         ; """.format(
                the_scenario.template_network_gdb,
                the_scenario.base_network_gdb,
                the_scenario.road_max_artificial_link_dist,
                the_scenario.rail_max_artificial_link_dist,
                the_scenario.water_max_artificial_link_dist,
                the_scenario.pipeline_crude_max_artificial_link_dist,
                the_scenario.pipeline_prod_max_artificial_link_dist,

                the_scenario.liquid_railroad_class_1_cost.magnitude,
                the_scenario.solid_railroad_class_1_cost.magnitude,
                the_scenario.rail_dc_7,
                the_scenario.rail_dc_6,
                the_scenario.rail_dc_5,
                the_scenario.rail_dc_4,
                the_scenario.rail_dc_3,
                the_scenario.rail_dc_2,
                the_scenario.rail_dc_1,
                the_scenario.rail_dc_0,

                the_scenario.liquid_truck_base_cost.magnitude,
                the_scenario.solid_truck_base_cost.magnitude,
                the_scenario.truck_interstate,
                the_scenario.truck_pr_art,
                the_scenario.truck_m_art,
                the_scenario.truck_local,

                the_scenario.liquid_barge_cost.magnitude,
                the_scenario.solid_barge_cost.magnitude,
                the_scenario.water_high_vol,
                the_scenario.water_med_vol,
                the_scenario.water_low_vol,
                the_scenario.water_no_vol,

                the_scenario.solid_transloading_cost.magnitude,
                the_scenario.liquid_transloading_cost.magnitude,
                
                the_scenario.liquid_rail_short_haul_penalty,
                the_scenario.solid_rail_short_haul_penalty,
                the_scenario.liquid_water_short_haul_penalty,
                the_scenario.solid_water_short_haul_penalty)

        db_cur = db_con.execute(sql)
        network_config_id = db_cur.fetchone()[0]
    except:
        warning = "could not retrieve network configuration id from the routes_cache. likely, it doesn't exist yet"
        logger.debug(warning)

    # if the id is 0 it couldnt find it in the entry. now try it  to the DB
    if network_config_id == 0:
        create_network_config_id_table(the_scenario, logger)

    return network_config_id
