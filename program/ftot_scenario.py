#---------------------------------------------------------------------------------------------------
# Name: ftot_scenario.py
#
# Purpose: This module declares all of the attributes of the_scenario object and
# creates getter and setter methods for each attribute.
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


#===================================================================================================


def load_scenario_config_file(fullPathToXmlConfigFile, fullPathToXmlSchemaFile, logger):

    if not os.path.exists(fullPathToXmlConfigFile):
        raise IOError("XML Scenario File {} not found at specified location.".format(fullPathToXmlConfigFile))

    if fullPathToXmlConfigFile.rfind(".xml") < 0:
        raise IOError("XML Scenario File {} is not an XML file type.".format(fullPathToXmlConfigFile))

    if not os.path.exists(fullPathToXmlSchemaFile):
        raise IOError("XML Schema File not found at {}.".format(fullPathToXmlSchemaFile))

    xmlScenarioFile = minidom.parse(fullPathToXmlConfigFile)

    # Validate XML scenario against XML schema
    schemaObj = etree.XMLSchema(etree.parse(fullPathToXmlSchemaFile))
    xmlFileObj = etree.parse(fullPathToXmlConfigFile)
    validationResult = schemaObj.validate(xmlFileObj)

    logger.debug("validate XML scenario against XML schema")
    if validationResult == False:
        logger.error("XML scenario validation failed. Error messages to follow.")
        for error in schemaObj.error_log:
            logger.error("ERROR ON LINE: {} - ERROR MESSAGE: {}".format(error.line, error.message))

        raise Exception("XML Scenario File does not meet the requirements in the XML schema file.")

    # initialize scenario object
    logger.debug("initialize scenario object")
    scenario = Scenario()

    # Check the scenario schema version (current version is specified in FTOT.py under SCHEMA_VERSION global var)
    logger.debug("validate schema version is correct")
    scenario.scenario_schema_version = xmlScenarioFile.getElementsByTagName('Scenario_Schema_Version')[0].firstChild.data

    if not str(SCHEMA_VERSION).split(".")[0:2] == str(scenario.scenario_schema_version).split(".")[0:2]:
        error = "XML Schema File Version is {}. Expected version {}. " \
                "Use the XML flag to run the XML upgrade tool. " \
                .format(str(scenario.scenario_schema_version), str(SCHEMA_VERSION))
        logger.error(error)
        raise Exception(error)

    scenario.scenario_name = 'Scenario Name'
    if len(xmlScenarioFile.getElementsByTagName('Scenario_Name')[0].childNodes) > 0:
        scenario.scenario_name = xmlScenarioFile.getElementsByTagName('Scenario_Name')[0].firstChild.data
    # Convert any commas in the scenario name to dashes
    warning = "Replace any commas in the scenario name with dashes to accomodate CSV files."
    logger.debug(warning)
    scenario.scenario_name = scenario.scenario_name.replace(",", "-")
    scenario.scenario_description = 'Scenario Description'
    if len(xmlScenarioFile.getElementsByTagName('Scenario_Description')[0].childNodes) > 0:
        scenario.scenario_description = xmlScenarioFile.getElementsByTagName('Scenario_Description')[0].firstChild.data

    # SCENARIO INPUTS SECTION
    # ----------------------------------------------------------------------------------------
    scenario.common_data_folder = xmlScenarioFile.getElementsByTagName('Common_Data_Folder')[0].firstChild.data
    scenario.base_network_gdb = xmlScenarioFile.getElementsByTagName('Base_Network_Gdb')[0].firstChild.data

    # Change to FAF4-based network if using the default network
    if scenario.base_network_gdb.endswith('FTOT_Public_US_Contiguous_Network_v2023.gdb') and xmlScenarioFile.getElementsByTagName('Capacity_On')[0].firstChild.data == "True":
        scenario.base_network_gdb = scenario.base_network_gdb.replace('FTOT_Public_US_Contiguous_Network_v2023.gdb', 'FTOT_Public_US_Contiguous_Network_v2023_FAF4_Capacity.gdb')
        logger.warning("Capacity based scenarios are not available with the FTOT_Public_US_Contiguous_Network_v2023.gdb. Automatically changing to FTOT_Public_US_Contiguous_Network_v2023_FAF4_Capacity.gdb")

    if scenario.base_network_gdb.endswith('FTOT_Public_US_Contiguous_Network_v2024.gdb') and xmlScenarioFile.getElementsByTagName('Capacity_On')[0].firstChild.data == "True":
        scenario.base_network_gdb = scenario.base_network_gdb.replace('FTOT_Public_US_Contiguous_Network_v2024.gdb', 'FTOT_Public_US_Contiguous_Network_v2024_FAF4_Capacity.gdb')
        logger.warning("Capacity based scenarios are not available with the FTOT_Public_US_Contiguous_Network_v2024.gdb. Automatically changing to FTOT_Public_US_Contiguous_Network_v2024_FAF4_Capacity.gdb")

    if scenario.base_network_gdb.endswith('FTOT_Public_US_Contiguous_Network_v2025.gdb') and xmlScenarioFile.getElementsByTagName('Capacity_On')[0].firstChild.data == "True":
        scenario.base_network_gdb = scenario.base_network_gdb.replace('FTOT_Public_US_Contiguous_Network_v2025.gdb', 'FTOT_Public_US_Contiguous_Network_v2025_FAF4_Capacity.gdb')
        logger.warning("Capacity based scenarios are not available with the FTOT_Public_US_Contiguous_Network_v2025.gdb. Automatically changing to FTOT_Public_US_Contiguous_Network_v2025_FAF4_Capacity.gdb")

    scenario.disruption_data = xmlScenarioFile.getElementsByTagName('Disruption_Data')[0].firstChild.data
    scenario.base_rmp_layer = xmlScenarioFile.getElementsByTagName('Base_RMP_Layer')[0].firstChild.data
    scenario.base_destination_layer = xmlScenarioFile.getElementsByTagName('Base_Destination_Layer')[0].firstChild.data
    scenario.base_processors_layer = xmlScenarioFile.getElementsByTagName('Base_Processors_Layer')[0].firstChild.data

    # Function to check for relative vs. absolute vs. other paths:
    def check_relative_paths(mypath):
        if mypath != "None":
                if os.path.exists(os.path.realpath(os.path.join(fullPathToXmlConfigFile,'..',mypath))):
                    return(os.path.realpath(os.path.join(fullPathToXmlConfigFile,'..',mypath)))
                elif os.path.exists(mypath):
                    return(mypath)
                else:
                    assert os.path.exists(mypath), 'Cannot find file: {}'.format(mypath.rsplit('\\', 1)[-1])
        else:
            return("None")

    # save all paths
    scenario.rmp_commodity_data = check_relative_paths(xmlScenarioFile.getElementsByTagName('RMP_Commodity_Data')[0].firstChild.data)
    scenario.destinations_commodity_data = check_relative_paths(xmlScenarioFile.getElementsByTagName('Destinations_Commodity_Data')[0].firstChild.data)
    scenario.processors_commodity_data = check_relative_paths(xmlScenarioFile.getElementsByTagName('Processors_Commodity_Data')[0].firstChild.data)
    scenario.schedule = check_relative_paths(xmlScenarioFile.getElementsByTagName('Schedule_Data')[0].firstChild.data)
    scenario.processors_candidate_slate_data = check_relative_paths(xmlScenarioFile.getElementsByTagName('Processors_Candidate_Commodity_Data')[0].firstChild.data)
    scenario.commodity_mode_data = check_relative_paths(xmlScenarioFile.getElementsByTagName('Commodity_Mode_Data')[0].firstChild.data)
    
    if len(xmlScenarioFile.getElementsByTagName('Commodity_Density_Data')):
        scenario.commodity_density_data = check_relative_paths(xmlScenarioFile.getElementsByTagName('Commodity_Density_Data')[0].firstChild.data)
    else:
        scenario.commodity_density_data = "None"
    
    scenario.disruption_data = check_relative_paths(xmlScenarioFile.getElementsByTagName('Disruption_Data')[0].firstChild.data)

    # note: the processor_candidates_data is defined under other since it is not a user specified file

    # use pint to set the default units
    logger.debug("test: setting the default units with pint")
    try:
        scenario.default_units_solid_phase = Q_(xmlScenarioFile.getElementsByTagName('Default_Units_Solid_Phase')[0].firstChild.data.lower()).units
        scenario.default_units_liquid_phase = Q_(xmlScenarioFile.getElementsByTagName('Default_Units_Liquid_Phase')[0].firstChild.data.lower()).units
        scenario.default_units_distance = Q_(xmlScenarioFile.getElementsByTagName('Default_Units_Distance')[0].firstChild.data.lower()).units
        scenario.default_units_currency = xmlScenarioFile.getElementsByTagName('Default_Units_Currency')[0].firstChild.data.lower()
        ureg.define('{} = [currency]'.format(scenario.default_units_currency))  # add custom currency unit to the unit registry
        logger.debug("PASS: setting the default units with pint")

    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))

    # check default units are in expected dimensionality
    if not (1 * scenario.default_units_solid_phase).check('[mass]'):
        error = ("The default units for solid phase specified in the scenario XML is {}, ".format(scenario.default_units_solid_phase) +
                 "which is not a unit of mass. Please specify a unit of mass.")
        logger.error(error)
        raise Exception(error)

    if not (1 * scenario.default_units_liquid_phase).check('[volume]'):
        error = ("The default units for liquid phase specified in the scenario XML is {}, ".format(scenario.default_units_liquid_phase) +
                 "which is not a unit of volume. Please specify a unit of volume.")
        logger.error(error)
        raise Exception(error)

    if not (1 * scenario.default_units_distance).check('[length]'):
        error = ("The default units for distance specified in the scenario XML is {}, ".format(scenario.default_units_distance) +
                 "which is not a unit of distance. Please specify a unit of distance.")
        logger.error(error)
        raise Exception(error)

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
        scenario.truckFuelEfficiency = Q_(xmlScenarioFile.getElementsByTagName('Truck_Fuel_Efficiency')[0].firstChild.data).to('{}*{}/gal'.format(scenario.default_units_solid_phase, scenario.default_units_distance))
        scenario.railFuelEfficiency = Q_(xmlScenarioFile.getElementsByTagName('Railcar_Fuel_Efficiency')[0].firstChild.data).to('{}*{}/gal'.format(scenario.default_units_solid_phase, scenario.default_units_distance))
        scenario.bargeFuelEfficiency = Q_(xmlScenarioFile.getElementsByTagName('Barge_Fuel_Efficiency')[0].firstChild.data).to('{}*{}/gal'.format(scenario.default_units_solid_phase, scenario.default_units_distance))
        logger.debug("PASS: setting the vehicle fuel efficiencies with pint passed")

        logger.debug("test: setting the vehicle emission factors with pint")
        scenario.roadCO2Emissions = Q_(xmlScenarioFile.getElementsByTagName('Truck_CO2_Emissions')[0].firstChild.data).to('g/{}'.format(scenario.default_units_distance))
        scenario.railroadCO2Emissions = Q_(xmlScenarioFile.getElementsByTagName('Railcar_CO2_Emissions')[0].firstChild.data).to('g/{}/{}'.format(scenario.default_units_solid_phase, scenario.default_units_distance))
        scenario.bargeCO2Emissions = Q_(xmlScenarioFile.getElementsByTagName('Barge_CO2_Emissions')[0].firstChild.data).to('g/{}/{}'.format(scenario.default_units_solid_phase, scenario.default_units_distance))
        scenario.pipelineCO2Emissions = Q_(xmlScenarioFile.getElementsByTagName('Pipeline_CO2_Emissions')[0].firstChild.data).to('g/{}/{}'.format(scenario.default_units_solid_phase, scenario.default_units_distance))
        
        logger.debug("reading in detailed emissions data")
        scenario.detailed_emissions_data = check_relative_paths(xmlScenarioFile.getElementsByTagName('Detailed_Emissions_Data')[0].firstChild.data)

        # setting density conversion based on 'Density_Conversion_Factor' field if it exists, or otherwise default to 3.33 ton/thousand_gallon
        if len(xmlScenarioFile.getElementsByTagName('Density_Conversion_Factor')):
            scenario.densityFactor = Q_(xmlScenarioFile.getElementsByTagName('Density_Conversion_Factor')[0].firstChild.data).to('{}/{}'.format(scenario.default_units_solid_phase, scenario.default_units_liquid_phase))
        else:
            if scenario.commodity_density_data == "None":
                # User didn't specify a density file OR factor
                logger.warning("FTOT is assuming a density of 3.33 ton/thousand_gallon for emissions reporting for liquids. Use scenario XML parameter 'Density_Conversion_Factor' to adjust this value.")
            scenario.densityFactor = Q_('3.33 ton/thousand_gallon').to('{}/{}'.format(scenario.default_units_solid_phase, scenario.default_units_liquid_phase))
        logger.debug("PASS: setting the vehicle emission factors with pint passed")

        scenario.speed_time_data = check_relative_paths(xmlScenarioFile.getElementsByTagName('Speed_Time_Data')[0].firstChild.data)
    
    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))

    # SCRIPT PARAMETERS SECTION FOR NETWORK
    # ----------------------------------------------------------------------------------------

    # truck costs
    try:
        logger.debug("test: setting the base costs for truck with pint")

        scenario.solid_truck_base_cost = xmlScenarioFile.getElementsByTagName('Truck_Base_Cost')[0].firstChild.data.lower()
        assert scenario.solid_truck_base_cost.split(" ")[1].split("/")[0] == scenario.default_units_currency, "XML element Truck_Base_Cost must use the default currency units."
        scenario.solid_truck_base_cost = Q_(scenario.solid_truck_base_cost).to("{}/{}/{}".format(scenario.default_units_currency, scenario.default_units_solid_phase, scenario.default_units_distance))

        logger.debug("PASS: setting the base costs for truck with pint passed")

    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))

    # rail costs
    try:
        logger.debug("test: setting the base costs for rail with pint")

        scenario.solid_railroad_class_1_cost = xmlScenarioFile.getElementsByTagName('Railroad_Class_I_Cost')[0].firstChild.data.lower()
        assert scenario.solid_railroad_class_1_cost.split(" ")[1].split("/")[0] == scenario.default_units_currency, "XML element Railroad_Class_I_Cost must use the default currency units."
        scenario.solid_railroad_class_1_cost = Q_(scenario.solid_railroad_class_1_cost).to("{}/{}/{}".format(scenario.default_units_currency, scenario.default_units_solid_phase, scenario.default_units_distance))

        logger.debug("PASS: setting the base costs for rail with pint passed")

    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))

    # barge costs
    try:
        logger.debug("test: setting the base costs for barge with pint")

        scenario.solid_barge_cost = xmlScenarioFile.getElementsByTagName('Barge_Base_Cost')[0].firstChild.data.lower()
        assert scenario.solid_barge_cost.split(" ")[1].split("/")[0] == scenario.default_units_currency, "XML element Barge_Base_Cost must use the default currency units."
        scenario.solid_barge_cost = Q_(scenario.solid_barge_cost).to("{}/{}/{}".format(scenario.default_units_currency, scenario.default_units_solid_phase, scenario.default_units_distance))

        logger.debug("PASS: setting the base costs for barge with pint passed")

    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))

    # impedance data
    try:
         logger.debug("test: reading in impedance weights data")
         scenario.impedance_weights_data = check_relative_paths(xmlScenarioFile.getElementsByTagName('Impedance_Weights_Data')[0].firstChild.data)
         logger.debug("PASS: reading in impedance weights data")

    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))

    # transloading costs
    try:
        logger.debug("test: setting the transloading costs with pint")

        scenario.solid_transloading_cost = xmlScenarioFile.getElementsByTagName('Transloading_Cost')[0].firstChild.data.lower()
        assert scenario.solid_transloading_cost.split(" ")[1].split("/")[0] == scenario.default_units_currency, "XML element Transloading_Cost must use the default currency units."
        scenario.solid_transloading_cost = Q_(scenario.solid_transloading_cost).to("{}/{}".format(scenario.default_units_currency, scenario.default_units_solid_phase))

        logger.debug("PASS: setting the transloading costs with pint passed")

    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))

    # artificial link costs
    try:
        logger.debug("test: setting the base costs for artificial links with pint")

        # if these xml elements don't exist, just default to road cost
        if len(xmlScenarioFile.getElementsByTagName('Artificial_Link_Cost')):
            scenario.solid_artificial_cost = xmlScenarioFile.getElementsByTagName('Artificial_Link_Cost')[0].firstChild.data.lower()
            assert scenario.solid_artificial_cost.split(" ")[1].split("/")[0] == scenario.default_units_currency, "XML element Artificial_Link_Cost must use the default currency units."
            scenario.solid_artificial_cost = Q_(scenario.solid_artificial_cost).to("{}/{}/{}".format(scenario.default_units_currency, scenario.default_units_solid_phase, scenario.default_units_distance))
        else: 
            scenario.solid_artificial_cost = scenario.solid_truck_base_cost

        logger.debug("PASS: setting the artificial link costs for all modes with pint passed")

    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))

    # artificial links
    try:
        logger.debug("test: setting the artificial link distances with pint")
        scenario.road_max_artificial_link_dist = Q_(xmlScenarioFile.getElementsByTagName('Road_Max_Artificial_Link_Distance')[0].firstChild.data).to('{}'.format(scenario.default_units_distance))
        scenario.rail_max_artificial_link_dist = Q_(xmlScenarioFile.getElementsByTagName('Rail_Max_Artificial_Link_Distance')[0].firstChild.data).to('{}'.format(scenario.default_units_distance))
        scenario.water_max_artificial_link_dist = Q_(xmlScenarioFile.getElementsByTagName('Water_Max_Artificial_Link_Distance')[0].firstChild.data).to('{}'.format(scenario.default_units_distance))
        scenario.pipeline_crude_max_artificial_link_dist = Q_(xmlScenarioFile.getElementsByTagName('Pipeline_Crude_Max_Artificial_Link_Distance')[0].firstChild.data).to('{}'.format(scenario.default_units_distance))
        scenario.pipeline_prod_max_artificial_link_dist = Q_(xmlScenarioFile.getElementsByTagName('Pipeline_Products_Max_Artificial_Link_Distance')[0].firstChild.data).to('{}'.format(scenario.default_units_distance))
        logger.debug("PASS: setting the artificial link distances with pint passed")

        # Setting flag for artificial links in reporting
        scenario.report_with_artificial = False
        if len(xmlScenarioFile.getElementsByTagName('Report_With_Artificial_Links')):
            if xmlScenarioFile.getElementsByTagName('Report_With_Artificial_Links')[0].firstChild.data == "True":
                scenario.report_with_artificial = True
                
    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))

    # short haul penalties
    # these penalties are read in as distances but will be converted to cost penalties in set_network_costs_in_db() in ftot_networkx.py 
    try:
        logger.debug("test: setting the short haul penalties with pint")
        scenario.rail_short_haul_penalty = Q_(xmlScenarioFile.getElementsByTagName('Rail_Short_Haul_Penalty')[0].firstChild.data).to("{}".format(scenario.default_units_distance))
        scenario.water_short_haul_penalty = Q_(xmlScenarioFile.getElementsByTagName('Water_Short_Haul_Penalty')[0].firstChild.data).to("{}".format(scenario.default_units_distance))
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

    if xmlScenarioFile.getElementsByTagName('Permitted_Modes')[0].getElementsByTagName('Pipeline_Crude')[0].firstChild.data == "True":
        scenario.permittedModes.append("pipeline_crude_trf_rts")
    if xmlScenarioFile.getElementsByTagName('Permitted_Modes')[0].getElementsByTagName('Pipeline_Prod')[0].firstChild.data == "True":
        scenario.permittedModes.append("pipeline_prod_trf_rts")

    if xmlScenarioFile.getElementsByTagName('Capacity_On')[0].firstChild.data == "True":
        scenario.capacityOn = True
        # capacity & NDR are incompatible - check if NDR was set to true
        if scenario.ndrOn:
            logger.debug("NDR de-activated due to capacity enforcement")
        # update parameter to false regardless of previous value
        scenario.ndrOn = False
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

    # CO2 optimization options
    # FTOT defaults to routing cost only and will overwrite the hardcoded variables below if elements are included in the XML
    scenario.transport_cost_scalar = 1.0
    if len(xmlScenarioFile.getElementsByTagName('Transport_Cost_Scalar')):
        scenario.transport_cost_scalar = float(xmlScenarioFile.getElementsByTagName('Transport_Cost_Scalar')[0].firstChild.data)
    
    scenario.co2_cost_scalar = 0.0  
    if len(xmlScenarioFile.getElementsByTagName('CO2_Cost_Scalar')):
        scenario.co2_cost_scalar = float(xmlScenarioFile.getElementsByTagName('CO2_Cost_Scalar')[0].firstChild.data)
    
    # FTOT default value for CO2 unit cost is 191 usd/ton, which does not account for conversion to default currency units
    scenario.co2_unit_cost = Q_("0.0002105414603865581 usd/gram")
    if len(xmlScenarioFile.getElementsByTagName('CO2_Unit_Cost')):
        scenario.co2_unit_cost = xmlScenarioFile.getElementsByTagName('CO2_Unit_Cost')[0].firstChild.data.lower()
        assert scenario.co2_unit_cost.split(" ")[1].split("/")[0] == scenario.default_units_currency, "XML Element CO2_Unit_Cost must use the default currency units."
        scenario.co2_unit_cost = Q_(scenario.co2_unit_cost).to("{}/gram".format(scenario.default_units_currency))
    elif scenario.co2_cost_scalar != 0.0:
        # Raise error if missing CO2 cost but CO2 weight is greater than 0
        # This is important considering default currency might not be USD
        error = "Scenario XML is missing the CO2_Unit_Cost element. This element is required if CO2_Cost_Scalar is greater than 0."
        logger.error(error)
        raise Exception(error)

    # Solver options
    try:
        logger.debug("test: setting the solver configuration")

        # if Solver element doesn't exist, default to CBC
        if len(xmlScenarioFile.getElementsByTagName('Solver')):
            solver_input = xmlScenarioFile.getElementsByTagName('Solver')[0].firstChild.data.lower()
            if solver_input in ("cbc", "default"):
                scenario.solver = 'cbc'
            elif solver_input == "highs":
                scenario.solver = 'highs'
            else:
                logger.warning("Solver name not recognized. Defaulting to CBC solver.")
                scenario.solver = 'cbc'
        else:
            scenario.solver = 'cbc'

        if len(xmlScenarioFile.getElementsByTagName('Solver_Time_Limit')):
            time_limit = xmlScenarioFile.getElementsByTagName('Solver_Time_Limit')[0].firstChild.data.lower()
            if time_limit == "none":
                scenario.time_limit = "none"
            else:
                scenario.time_limit = Q_(time_limit).to(ureg.seconds)
        else: # set to none to represent no time limit
            scenario.time_limit = "none"

        logger.debug("PASS: setting the solver configuration passed")

    except Exception as e:
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))

    scenario.unMetDemandPenalty = float(xmlScenarioFile.getElementsByTagName('Unmet_Demand_Penalty')[0].firstChild.data)

    # OTHER
    # ----------------------------------------------------------------------------------------

    scenario.scenario_run_directory = os.path.dirname(fullPathToXmlConfigFile)

    scenario.main_db = os.path.join(scenario.scenario_run_directory, "main.db")
    scenario.main_gdb = os.path.join(scenario.scenario_run_directory, "main.gdb")

    scenario.rmp_fc = os.path.join(scenario.main_gdb, "raw_material_producers")
    scenario.destinations_fc = os.path.join(scenario.main_gdb, "ultimate_destinations")
    scenario.processors_fc = os.path.join(scenario.main_gdb, "processors")
    scenario.processor_candidates_fc = os.path.join(scenario.main_gdb, "all_candidate_processors")
    scenario.locations_fc = os.path.join(scenario.main_gdb, "locations")

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
    logger.config("xml_default_units_distance: \t{}".format(the_scenario.default_units_distance))
    logger.config("xml_default_units_currency: \t{}".format(the_scenario.default_units_currency))

    logger.config("xml_truck_load_solid: \t{}".format(the_scenario.truck_load_solid))
    logger.config("xml_railcar_load_solid: \t{}".format(the_scenario.railcar_load_solid))
    logger.config("xml_barge_load_solid: \t{}".format(the_scenario.barge_load_solid))

    logger.config("xml_truck_load_liquid: \t{}".format(the_scenario.truck_load_liquid))
    logger.config("xml_railcar_load_liquid: \t{}".format(the_scenario.railcar_load_liquid))
    logger.config("xml_barge_load_liquid: \t{}".format(the_scenario.barge_load_liquid))
    logger.config("xml_pipeline_crude_load_liquid: \t{}".format(the_scenario.pipeline_crude_load_liquid))
    logger.config("xml_pipeline_prod_load_liquid: \t{}".format(the_scenario.pipeline_prod_load_liquid))

    logger.config("xml_truck_base_cost: \t{}".format(the_scenario.solid_truck_base_cost))
    logger.config("xml_railroad_class_1_cost: \t{}".format(the_scenario.solid_railroad_class_1_cost))
    logger.config("xml_barge_base_cost: \t{}".format(the_scenario.solid_barge_cost))

    logger.config("xml_impedance_weights_data: \t{}".format(the_scenario.impedance_weights_data))
    logger.config("xml_speed_time_data: \t{}".format(the_scenario.speed_time_data))

    logger.config("xml_transloading_cost: \t{}".format(the_scenario.solid_transloading_cost))
    logger.config("xml_artificial_link_cost: \t{}".format(the_scenario.solid_artificial_cost))

    logger.config("xml_road_max_artificial_link_dist: \t{}".format(the_scenario.road_max_artificial_link_dist))
    logger.config("xml_rail_max_artificial_link_dist: \t{}".format(the_scenario.rail_max_artificial_link_dist))
    logger.config("xml_water_max_artificial_link_dist: \t{}".format(the_scenario.water_max_artificial_link_dist))
    logger.config("xml_pipeline_crude_max_artificial_link_dist: \t{}".format(the_scenario.pipeline_crude_max_artificial_link_dist))
    logger.config("xml_pipeline_prod_max_artificial_link_dist: \t{}".format(the_scenario.pipeline_prod_max_artificial_link_dist))
    logger.config("xml_report_with_artificial_links: \t{}".format(the_scenario.report_with_artificial))

    logger.config("xml_rail_short_haul_penalty: \t{}".format(the_scenario.rail_short_haul_penalty))
    logger.config("xml_water_short_haul_penalty: \t{}".format(the_scenario.water_short_haul_penalty))

    logger.config("xml_truckFuelEfficiency: \t{}".format(the_scenario.truckFuelEfficiency))
    logger.config("xml_bargeFuelEfficiency: \t{}".format(the_scenario.bargeFuelEfficiency))
    logger.config("xml_railcarFuelEfficiency: \t{}".format(the_scenario.railFuelEfficiency))

    logger.config("xml_truckCO2Emissions: \t{}".format(the_scenario.roadCO2Emissions))
    logger.config("xml_railcarCO2Emissions: \t{}".format(round(the_scenario.railroadCO2Emissions,2)))
    logger.config("xml_bargeCO2Emissions: \t{}".format(round(the_scenario.bargeCO2Emissions,2)))
    logger.config("xml_pipelineCO2Emissions: \t{}".format(the_scenario.pipelineCO2Emissions))
    logger.config("xml_detailed_emissions_data: \t{}".format(the_scenario.detailed_emissions_data))
    logger.config("xml_densityFactor: \t{}".format(the_scenario.densityFactor))

    logger.config("xml_ndrOn: \t{}".format(the_scenario.ndrOn))
    logger.config("xml_permittedModes: \t{}".format(the_scenario.permittedModes))
    logger.config("xml_capacityOn: \t{}".format(the_scenario.capacityOn))
    logger.config("xml_backgroundFlowModes: \t{}".format(the_scenario.backgroundFlowModes))
    logger.config("xml_minCapacityLevel: \t{}".format(the_scenario.minCapacityLevel))
    logger.config("xml_transport_cost_scalar: \t{}".format(the_scenario.transport_cost_scalar))
    logger.config("xml_co2_cost_scalar: \t{}".format(the_scenario.co2_cost_scalar))
    logger.config("xml_co2_unit_cost: \t{}".format(the_scenario.co2_unit_cost))
    logger.config("xml_solver: \t{}".format(the_scenario.solver))
    logger.config("xml_solver_time_limit: \t{}".format(the_scenario.time_limit))
    logger.config("xml_unMetDemandPenalty (default): \t{}".format(the_scenario.unMetDemandPenalty))


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

        logger.debug("done making the scenario_config")
        with sqlite3.connect(the_scenario.main_db) as db_con:
            insert_sql = """
                INSERT into scenario_config
                values (?,?,?)
                ;"""

            db_con.executemany(insert_sql, config_list)
            logger.debug("finish scenario_config db_con.executemany()")
            db_con.commit()
            logger.debug("finish scenario_config db_con.commit")


#=======================================================================================================================


def check_scenario_config_db(the_scenario, logger):
    logger.debug("checking consistency of scenario config file with previous step")


#=======================================================================================================================


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
                                            liquid_truck_base_cost,
                                            solid_truck_base_cost,
                                            liquid_barge_cost,
                                            solid_barge_cost,
                                            solid_transloading_cost,
                                            liquid_transloading_cost,
                                            rail_short_haul_penalty,
                                            water_short_haul_penalty
                                        )
                values (
                        NULL, '{}', '{}', {}, {}, {}, {}, {},  {}, {}, {}, {}, {}, {}, {}, {}, {}, {});""".format(
            the_scenario.template_network_gdb,
            the_scenario.base_network_gdb,
            the_scenario.road_max_artificial_link_dist,
            the_scenario.rail_max_artificial_link_dist,
            the_scenario.water_max_artificial_link_dist,
            the_scenario.pipeline_crude_max_artificial_link_dist,
            the_scenario.pipeline_prod_max_artificial_link_dist,
            the_scenario.liquid_railroad_class_1_cost.magnitude,
            the_scenario.solid_railroad_class_1_cost.magnitude,
            the_scenario.liquid_truck_base_cost.magnitude,
            the_scenario.solid_truck_base_cost.magnitude,
            the_scenario.liquid_barge_cost.magnitude,
            the_scenario.solid_barge_cost.magnitude,
            the_scenario.solid_transloading_cost.magnitude,
            the_scenario.liquid_transloading_cost.magnitude,
            the_scenario.rail_short_haul_penalty,
            the_scenario.water_short_haul_penalty)

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
                         liquid_truck_base_cost = {} and
                         solid_truck_base_cost = {} and
                         liquid_barge_cost = {} and
                         solid_barge_cost = {} and
                         solid_transloading_cost = {} and
                         liquid_transloading_cost = {} and
                         rail_short_haul_penalty = {} and
                         water_short_haul_penalty = {} and
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
                the_scenario.liquid_truck_base_cost.magnitude,
                the_scenario.solid_truck_base_cost.magnitude,
                the_scenario.liquid_barge_cost.magnitude,
                the_scenario.solid_barge_cost.magnitude,
                the_scenario.solid_transloading_cost.magnitude,
                the_scenario.liquid_transloading_cost.magnitude,
                the_scenario.rail_short_haul_penalty,
                the_scenario.water_short_haul_penalty)

        db_cur = db_con.execute(sql)
        network_config_id = db_cur.fetchone()[0]
    except:
        warning = "could not retrieve network configuration id from the routes_cache. likely, it doesn't exist yet"
        logger.debug(warning)

    # if the id is 0 it couldn't find it in the entry. now try adding it to the DB
    if network_config_id == 0:
        create_network_config_id_table(the_scenario, logger)

    return network_config_id

