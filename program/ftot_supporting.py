# ---------------------------------------------------------------------------------------------------
# Name: ftot_suppporting
#
# Purpose:
#
# ---------------------------------------------------------------------------------------------------
import os
import math

import logging
import datetime
import sqlite3
from ftot import ureg, Q_
from six import iteritems

# <!--Create the logger -->
def create_loggers(dirLocation, task):
    """Create the logger"""

    loggingLocation = os.path.join(dirLocation, "logs")

    if not os.path.exists(loggingLocation):
        os.makedirs(loggingLocation)

    # BELOW ARE THE LOGGING LEVELS. WHATEVER YOU CHOOSE IN SETLEVEL WILL BE SHOWN ALONG WITH HIGHER LEVELS.
    # YOU CAN SET THIS FOR BOTH THE FILE LOG AND THE DOS WINDOW LOG
    # -----------------------------------------------------------------------------------------------------
    # CRITICAL       50
    # ERROR          40
    # WARNING        30
    # RESULT         25
    # CONFIG         24
    # INFO           20
    # RUNTIME        11
    # DEBUG          10
    # DETAILED_DEBUG  5

    logging.RESULT = 25
    logging.addLevelName(logging.RESULT, 'RESULT')

    logging.CONFIG = 19
    logging.addLevelName(logging.CONFIG, 'CONFIG')

    logging.RUNTIME = 11
    logging.addLevelName(logging.RUNTIME, 'RUNTIME')

    logging.DETAILED_DEBUG = 5
    logging.addLevelName(logging.DETAILED_DEBUG, 'DETAILED_DEBUG')

    logger = logging.getLogger('log')
    logger.setLevel(logging.DEBUG)

    logger.runtime = lambda msg, *args: logger._log(logging.RUNTIME, msg, args)
    logger.result = lambda msg, *args: logger._log(logging.RESULT, msg, args)
    logger.config = lambda msg, *args: logger._log(logging.CONFIG, msg, args)
    logger.detailed_debug = lambda msg, *args: logger._log(logging.DETAILED_DEBUG, msg, args)

    # FILE LOG
    # ------------------------------------------------------------------------------
    logFileName = task + "_" + "log_" + datetime.datetime.now().strftime("%Y_%m_%d_%H-%M-%S") + ".log"
    file_log = logging.FileHandler(os.path.join(loggingLocation, logFileName), mode='a')

    file_log.setLevel(logging.DEBUG)


    file_log_format = logging.Formatter('%(asctime)s.%(msecs).03d %(levelname)-8s %(message)s',
                                        datefmt='%m-%d %H:%M:%S')
    file_log.setFormatter(file_log_format)

    # DOS WINDOW LOG
    # ------------------------------------------------------------------------------
    console = logging.StreamHandler()

    console.setLevel(logging.INFO)

    console_log_format = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M:%S')
    console.setFormatter(console_log_format)

    # ADD THE HANDLERS
    # ----------------
    logger.addHandler(file_log)
    logger.addHandler(console)

    # NOTE: with these custom levels you can now do the following
    # test this out once the handlers have been added
    # ------------------------------------------------------------

    return logger


# ==============================================================================


def clean_file_name(value):
    deletechars = '\/:*?"<>|'
    for c in deletechars:
        value = value.replace(c, '')
    return value;


# ==============================================================================


def get_total_runtime_string(start_time):
    end_time = datetime.datetime.now()

    duration = end_time - start_time

    seconds = duration.total_seconds()

    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)

    hms = str("{:02}:{:02}:{:02}").format(hours, minutes, seconds)

    return hms


# ==============================================================================

def euclidean_distance(xCoord, yCoord, xCoord2, yCoord2):
    """Calculates the Euclidean distance between two sets of coordinates"""

    return math.sqrt(math.pow((xCoord - xCoord2), 2) + math.pow((yCoord - yCoord2), 2))


# =============================================================================

class CropData:
    """Class object containing crop information"""

    def __init__(self, prod, crop):
        self.production = prod
        self.crop = crop


# ==============================================================================
def check_OD_commodities_for_routes(origin_commodity_slate, destination_commodity_slate, logger):
    # check commodities and see if they match

    OD_commodity_match_dict = {}

    all_origin_commodities = origin_commodity_slate.commodities

    for origin_commodity in all_origin_commodities:

        all_destination_commodities = destination_commodity_slate.commodities

        for destination_commodity in all_destination_commodities:

            if origin_commodity.find(destination_commodity) > -1:

                OD_commodity_match_dict[origin_commodity] = all_origin_commodities[origin_commodity]

    return OD_commodity_match_dict


# ==============================================================================

def split_feedstock_commidity_name_into_parts(feedstock, logger):
    # Source Categories
    afpat_source_category_list = ["Agricultural Residues", "Woody Crops and Residues", "Herbaceous Energy Crops",
                                  "Oil Crops", "Waste Oils and Animal Fats", "Sugary and Starchy Biomass", "Bakken",
                                  "None"]
    feedstock_in_parts = ()
    feedstock_type = ""
    the_source_category = ""
    feedstock_source = ""

    for source_category in afpat_source_category_list:

        # add "_" to the front and back of the string so the type and source will be cleanly split
        source_category_as_separator = str("_" + source_category.replace(" ", "_") + "_")

        # Split the string at the first occurrence of sep, and return a 3-tuple
        # containing the part before the separator, the separator itself, and
        # the part after the separator.
        feedstock_in_parts = feedstock.partition(source_category_as_separator)

        # If the separator is not found, return a 3-tuple containing the string itself,
        # followed by two empty strings.
        if not feedstock_in_parts[2] == "":
            feedstock_type = feedstock_in_parts[0]

            the_source_category = source_category.replace(" ", "_").replace("-", "_")

            feedstock_source = feedstock_in_parts[2]
    if feedstock_type == "":
        logger.warning("the feedstock {} is not in the list {}".format(feedstock, afpat_source_category_list))
        logger.error("did not parse the feedstock commodity name into parts")
        raise Exception("did not parse the feedstock commodity name into parts")

    return [feedstock_type, the_source_category, feedstock_source]


# ==============================================================================

def create_full_crop_name(Feedstock_Type, Source_Category, Feedstock_Source):
    crop = str(Feedstock_Type + "_" + Source_Category + "_" + Feedstock_Source).replace(" ", "_").replace("-", "_")

    return crop


# ==============================================================================

def get_cleaned_process_name(_Primary_Processing_Type, _Secondary_Processing_Type, _Tertiary_Processing_Type):
    Primary_Processing_Type = str(_Primary_Processing_Type).replace(" ", "_").replace("-", "_")
    Secondary_Processing_Type = str(_Secondary_Processing_Type).replace(" ", "_").replace("-", "_")
    Tertiary_Processing_Type = str(_Tertiary_Processing_Type).replace(" ", "_").replace("-", "_")

    return (Primary_Processing_Type, Secondary_Processing_Type, Tertiary_Processing_Type)


# ==============================================================================
def make_rmp_as_proc_slate(the_scenario, commodity_name, commodity_quantity_with_units, logger):
    # we're going to query the database for facilities named candidate* (wildcard)
    # and use their product slate ratio to return a fuel_dictrionary.
    sql = """ select f.facility_name, c.commodity_name, fc.quantity, fc.units, c.phase_of_matter, fc.io 
                from facility_commodities fc
                join facilities f on f.facility_id = fc.facility_id
                join commodities c on c.commodity_id = fc.commodity_id
                where facility_name like 'candidate%' 
          """

    input_commodities = {}
    output_commodities = {}
    scaled_output_dict = {}
    with sqlite3.connect(the_scenario.main_db) as db_con:

        db_cur = db_con.cursor()
        db_cur.execute(sql)

        for row in db_cur:
            facility_name = row[0]
            a_commodity_name = row[1]
            quantity = row[2]
            units = row[3]
            phase_of_matter = row[4]
            io = row[5]

            if io == 'i':
                if not facility_name in list(input_commodities.keys()):
                    input_commodities[facility_name] = []

                input_commodities[facility_name].append([a_commodity_name, quantity, units, phase_of_matter, io])

            elif io == 'o':
                if not facility_name in list(output_commodities.keys()):
                    output_commodities[facility_name] = []
                output_commodities[facility_name].append([a_commodity_name, quantity, units, phase_of_matter, io])
            elif io == 'maxsize' or io == 'minsize':
                logger.detailed_debug("io flag == maxsize or min size")
            elif io == 'cost_formula':
                logger.detailed_debug("io flag == cost_formula")
            else:
                logger.warning(
                    "the io flag: {} is not recognized for commodity: {} - at facility: {}".format(io, a_commodity_name,
                                                                                                   facility_name))

    # check if there is more than one input commodity
    for facility in input_commodities:
        if not len(input_commodities[facility]) == 1:
            logger.warning(
                "there are: {} input commodities in the product slate for facility {}".format(len(input_commodities),
                                                                                              facility_name))
            for an_input_commodity in input_commodities[facility]:
                logger.warning("commodity_name: {}, quantity: {}, units: {}, io: {}".format(an_input_commodity[0],
                                                                                            an_input_commodity[1],
                                                                                            an_input_commodity[2],
                                                                                            an_input_commodity[3]))
        else:  # there is only one input commodity to use in the ratio.

            # check if this is the ratio we want to save
            if input_commodities[facility][0][0].lower() == commodity_name.lower():

                a_commodity_name = input_commodities[facility][0][0]
                quantity = input_commodities[facility][0][1]
                units = input_commodities[facility][0][2]
                input_commodity_quantity_with_units = Q_(quantity, units)

                # store all the output commodities
                for an_output_commodity in output_commodities[facility_name]:
                    a_commodity_name = an_output_commodity[0]
                    quantity = an_output_commodity[1]
                    units = an_output_commodity[2]
                    phase_of_matter = an_output_commodity[3]
                    output_commodity_quantity_with_units = Q_(quantity, units)

                    # the output commodity quantity is divided by the input
                    # commodity quantity specified in the candidate slate csv
                    # and then multiplied by the commodity_quantity_with_units
                    # factor from the RMP passed into this module
                    oc = output_commodity_quantity_with_units
                    ic = input_commodity_quantity_with_units
                    cs = commodity_quantity_with_units

                    # finally add it to the scaled output dictionary
                    scaled_output_dict[a_commodity_name] = [(oc / ic * cs), phase_of_matter]

    return scaled_output_dict


# ==============================================================================
def get_max_fuel_conversion_process_for_commodity(commodity, the_scenario, logger):

    max_conversion_process = ""  # max conversion process
    processes_for_commodity = []  # list of all processess for feedstock
    conversion_efficiency_dict = {}  # a temporary dictionary to store the conversion efficiencies for each process

    ag_fuel_yield_dict, cropYield, bioWasteDict, fossilResources = load_afpat_tables(the_scenario, logger)

    if commodity in ag_fuel_yield_dict:
        processes_for_commodity.extend(list(ag_fuel_yield_dict[commodity].keys()))

    # DO THE BIOWASTE RESOURCES
    if commodity in bioWasteDict:
        processes_for_commodity.extend(list(bioWasteDict[commodity].keys()))

    # DO THE FOSSIL RESOURCES
    fossil_keys = list(fossilResources.keys())

    for key in fossil_keys:

        if commodity.find(key) > -1:
            processes_for_commodity.extend(list(fossilResources[key].keys()))

    if processes_for_commodity == []:
        logger.warning("processes_for_commodity is empty: {}".format(processes_for_commodity))

    for conversion_process in processes_for_commodity:
        input_commodity, output_commodity = get_input_and_output_commodity_quantities_from_afpat(commodity,
                                                                                                 conversion_process,
                                                                                                 the_scenario, logger)

        conversion_efficiency = input_commodity / output_commodity["total_fuel"]

        conversion_efficiency_dict[conversion_process] = conversion_efficiency

    # get the most efficient conversion process from the sorted conversion_efficiency_dict
    # this method is some black magic using the lambda keyword to sort the dictionary by
    # values and gets the lowest kg/bbl of total fuel conversion rate (== max biomass -> fuel efficiency)
    if conversion_efficiency_dict == {}:
        logger.warning("conversion_efficiency_dict is empty: {}".format(conversion_efficiency_dict))

    else:
        max_conversion_process = sorted(iteritems(conversion_efficiency_dict), key=lambda k_v: (k_v[1], k_v[0]))[0]

    return max_conversion_process  # just return the name of the max conversion process, not the conversion efficiency


# ==============================================================================

def create_list_of_sub_commodities_from_afpat(commodity, process, the_scenario, logger):
    ag_fuel_yield_dict, cropYield, bioWasteDict, fossilResources = load_afpat_tables(the_scenario, logger)

    list_of_sub_commodities = []

    # first check the agricultural feedstocks in the ag_fuel_dict

    for ag_key in ag_fuel_yield_dict:

        if ag_key.lower().find(commodity) > -1:
            logger.debug("adding commodity {} list_of_sub_commodities for {}".format(ag_key, commodity))
            list_of_sub_commodities.append(ag_key)

    # second check the biowaste resources in the biowaste dict

    for biowaste_key in bioWasteDict:

        if biowaste_key.lower().find(commodity) > -1:
            logger.debug("adding commodity {} list_of_sub_commodities for {}".format(biowaste_key, commodity))
            list_of_sub_commodities.append(biowaste_key)

    # last, check the fossil resources feedstocks in the fossil resource dict

    for fossil_key in fossilResources:

        if fossil_key.lower().find(commodity) > -1:
            logger.debug("adding commodity {} list_of_sub_commodities for {}".format(fossil_key, commodity))
            list_of_sub_commodities.append(fossil_key)

    if list_of_sub_commodities == []:
        list_of_sub_commodities = [
            "the commodity {} has no sub_commodities in AFPAT agricultural, biowaste, or fossil fuel yield dictionaries".format(
                commodity)]
        logger.warning(
            "the commodity {} has no sub_commodities in AFPAT agricultural, biowaste, or fossil fuel yield dictionaries".format(
                commodity))

    return list_of_sub_commodities


# ==============================================================================
def get_demand_met_multiplier(simple_fuel_name, primary_process_type, logger):
    # switch the demand_met_multiplier based on fuel name and process type
    if simple_fuel_name == "jet":
        if primary_process_type == "HEFA":
            demand_met_multiplier = 2
        elif primary_process_type == "FTx":
            demand_met_multiplier = 2
        elif primary_process_type == "AFx":
            # 30% alcohol/fuel to 70% petroleum
            demand_met_multiplier = float((3 + 1 / 3))
        elif primary_process_type == "Petroleum_Refinery":
            demand_met_multiplier = 1
        elif primary_process_type == "NA":
            demand_met_multiplier = 1
        else:
            logger.error("the demand met multiplier was not set")
            raise Exception("the demand met multiplier was not set")

    elif simple_fuel_name == "diesel":

        demand_met_multiplier = 1

    else:
        demand_met_multiplier = 1
        logger.error("the fuel type is not jet or diesel; demand met multiplier was set to 1")

    return demand_met_multiplier


# ==============================================================================

# ==============================================================================
def get_processor_capacity(primary_processing, logger):
    capacity = 0

    if primary_processing == "FTx":
        capacity = Q_(200000, "kgal")
    if primary_processing == "Petroleum_Refinery":
        capacity = Q_(7665000, "kgal")
    else:
        capacity = Q_(200000, "kgal")

    return capacity


# ==============================================================================

def load_afpat_tables(the_scenario, logger):
    import pickle
    pickle_file = os.path.join(the_scenario.scenario_run_directory, "debug", "AFPAT_tables.p")

    afpat_tables = pickle.load(open(pickle_file, "rb"))
    ag_fuel_yield_dict = afpat_tables[0]
    cropYield = afpat_tables[1]
    bioWasteDict = afpat_tables[2]
    fossilResources = afpat_tables[3]

    return ag_fuel_yield_dict, cropYield, bioWasteDict, fossilResources


# ==============================================================================

def get_input_and_output_commodity_quantities_from_afpat(commodity, process, the_scenario, logger):

    input_commodity_quantities = 0  # a quantity of input resource required to produce fuels
    output_commodity_quantities = {}  # a dictionary containing the fuel outputs

    ag_fuel_yield_dict, cropYield, bioWasteDict, fossilResources = load_afpat_tables(the_scenario, logger)

    if commodity.lower().find("test_liquid_none_none") > -1:
        #        print "in the right place"
        input_commodity_quantities = Q_(1, "kgal")
        output_commodity_quantities['test_product_liquid_None_None'] = Q_(1, "kgal")
        output_commodity_quantities['jet'] = Q_(0, "kgal")
        output_commodity_quantities['diesel'] = Q_(0, "kgal")
        output_commodity_quantities['naphtha'] = Q_(0, "kgal")
        output_commodity_quantities['aromatics'] = Q_(0, "kgal")
        output_commodity_quantities['total_fuel'] = Q_(1, "kgal")
        # hack to skip the petroleum refinery
        commodity = "hack-hack-hack"
        process = "hack-hack-hack"

    elif commodity in ag_fuel_yield_dict:
        #        print "in the wrong right place"
        if process in ag_fuel_yield_dict[commodity]:

            input_commodity_quantities = Q_(ag_fuel_yield_dict[commodity][process][8], "kg/day")

            output_commodity_quantities['jet'] = Q_(ag_fuel_yield_dict[commodity][process][1], "oil_bbl / day")
            output_commodity_quantities['diesel'] = Q_(ag_fuel_yield_dict[commodity][process][2], "oil_bbl / day")
            output_commodity_quantities['naphtha'] = Q_(ag_fuel_yield_dict[commodity][process][3], "oil_bbl / day")
            output_commodity_quantities['aromatics'] = Q_(ag_fuel_yield_dict[commodity][process][4], "oil_bbl / day")
            output_commodity_quantities['total_fuel'] = Q_(ag_fuel_yield_dict[commodity][process][6], "oil_bbl / day")

        else:

            logger.error(
                "the commodity {} has no process {} in the AFPAT agricultural yield dictionary".format(commodity,
                                                                                                       process))
            raise Exception(
                "the commodity {} has no process {} in the AFPAT agricultural yield dictionary".format(commodity,
                                                                                                       process))

    elif commodity in bioWasteDict:

        if process in bioWasteDict[commodity]:

            input_commodity_quantities = Q_(bioWasteDict[commodity][process][1], "kg / year")

            output_commodity_quantities['total_fuel'] = Q_(bioWasteDict[commodity][process][4], "oil_bbl / day")
            output_commodity_quantities['jet'] = Q_(bioWasteDict[commodity][process][5], "oil_bbl / day")
            output_commodity_quantities['diesel'] = Q_(bioWasteDict[commodity][process][6], "oil_bbl / day")
            output_commodity_quantities['naphtha'] = Q_(bioWasteDict[commodity][process][7], "oil_bbl / day")
            output_commodity_quantities['aromatics'] = Q_(0.000, "oil_bbl / day")

        else:

            logger.debug(
                "the process {} for commodity {} is not in the biowaste yield dictionary {}".format(process, commodity,
                                                                                                    bioWasteDict[
                                                                                                        commodity]))
            logger.error(
                "the commodity {} has no process {} in the AFPAT biowaste yield dictionary".format(commodity, process))
            raise Exception(
                "the commodity {} has no process {} in the AFPAT fossilResources yield dictionary".format(commodity,
                                                                                                          process))

    # DO THE FOSSIL RESOURCES
    fossil_keys = list(fossilResources.keys())

    for key in fossil_keys:

        if commodity.find(key) > -1:

            if process in fossilResources[key]:

                input_commodity_quantities = Q_(500e3, "oil_bbl / day")

                output_commodity_quantities['total_fuel'] = Q_(fossilResources[key][process][4], "oil_bbl / day")
                output_commodity_quantities['jet'] = Q_(fossilResources[key][process][5], "oil_bbl / day")
                output_commodity_quantities['diesel'] = Q_(fossilResources[key][process][6], "oil_bbl / day")
                output_commodity_quantities['naphtha'] = Q_(fossilResources[key][process][7], "oil_bbl / day")
                output_commodity_quantities['aromatics'] = Q_(0.000, "oil_bbl / day")

            else:

                logger.error(
                    "the commodity {} has no process {} in the AFPAT fossilResources yield dictionary".format(commodity,
                                                                                                              process))
                raise Exception(
                    "the commodity {} has no process {} in the AFPAT fossilResources yield dictionary".format(commodity,
                                                                                                              process))

    if input_commodity_quantities == 0 or output_commodity_quantities == {}:
        logger.error(
            "the commodity {} and process {} is not in the AFPAT agricultural, biowaste, or fossil fuel yield dictionaries".format(
                commodity, process))
        raise Exception(
            "the commodity {} and process {} is not in the AFPAT agricultural, biowaste, or fossil fuel yield dictionaries".format(
                commodity, process))

    return input_commodity_quantities, output_commodity_quantities


# =============================================================================
def get_RMP_commodity_list(the_scenario, logger):
    import sqlite3

    main_db = the_scenario.main_db
    RMP_commodity_list = []

    with sqlite3.connect(main_db) as db_con:
        db_cur = db_con.cursor()

        sql = "select distinct commodity from raw_material_producers;"
        db_cur.execute(sql)

        for record in db_cur:
            RMP_commodity_list.append(record[0])

    return RMP_commodity_list


# ==============================================================================
def get_route_type(commodity_name, rmp_commodity_list):
    route_type = ""

    if commodity_name in rmp_commodity_list:

        route_type = "RMP_Processor"

    else:

        route_type = "Processor_Destination"

    return route_type


# ==============================================================================

def get_commodity_simple_name(commodity_name):
    simple_commodity_name = ""

    if commodity_name.find("diesel") >= 0:

        simple_commodity_name = "diesel"

    elif commodity_name.find("jet") >= 0:

        simple_commodity_name = "jet"

    else:

        simple_commodity_name = commodity_name  # a raw material producer material that doesn't have a simple name

    return simple_commodity_name


# ==============================================================================
def post_optimization(the_scenario, task_id, logger):
    from ftot_pulp import parse_optimal_solution_db

    logger.info("START: post_optimization for {} task".format(task_id))
    start_time = datetime.datetime.now()

    # Parse the Problem for the Optimal Solution
    parsed_optimal_solution = parse_optimal_solution_db(the_scenario, logger)

    logger.info("FINISH: post_optimization: Runtime (HMS): \t{}".format(get_total_runtime_string(start_time)))

# ===================================================================================================


def debug_write_solution(the_scenario, prob, logfile, logger):
    logger.info("starting debug_write_solution")

    with open(os.path.join(the_scenario.scenario_run_directory, "logs", logfile), 'w') as solution_file:

        for v in prob.variables():

            if v.varValue > 0.0:
                solution_file.write("{} -> {:,.2f}\n".format(v.name, v.varValue))
    logger.info("finished: debug_write_solution")
