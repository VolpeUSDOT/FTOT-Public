
# ---------------------------------------------------------------------------------------------------
# Name: ftot_setup
#
# Purpose: The ftot_setup module creates the main.db, and main.gdb for the scenario. The Alternative
# Fuel Production Assessment Tool (AFPAT) is also copied locally and stored in the main.gdb.
#
# ---------------------------------------------------------------------------------------------------

import os
import datetime
import sqlite3
from shutil import rmtree

import ftot_supporting
import ftot_supporting_gis

# ===================================================================================================


def import_afpat_data(afpat_spreadsheet, output_gdb, logger):

    import arcpy

    """
    import afpat from spreadsheet to arcgis table
    """

    logger.debug("start: import_afpat_data")

    if not os.path.exists(afpat_spreadsheet):
        error = "can't find afpat spreadsheet {}".format(afpat_spreadsheet)
        logger.error(error)
        raise IOError(error)


    if not os.path.exists(output_gdb):

        error = "can't find scratch gdb {}".format(output_gdb)
        logger.error(error)
        raise IOError(error)

    sheet = "2. User input & Results"

    out_table = os.path.join(output_gdb, "afpat_raw")

    if arcpy.Exists(out_table):
        arcpy.Delete_management(out_table)

    arcpy.ExcelToTable_conversion(afpat_spreadsheet, out_table, sheet)

    logger.debug("finish: import_afpat_data")


# ===================================================================================================

def cleanup(the_scenario, logger):
    logger.info("start: cleanup")

    all_files = os.listdir(the_scenario.scenario_run_directory)
    if all_files:
        logger.info("deleting everything but the scenario .xml file and the .bat file.")
        for file_or_dir in all_files:

            if file_or_dir.find("input_data") == -1:
                if file_or_dir.find(".bat") == -1:
                    if file_or_dir.find(".xml") == -1:
                        try:
                            rmtree(os.path.join(the_scenario.scenario_run_directory, file_or_dir))
                        except:
                            try:
                                os.remove(os.path.join(the_scenario.scenario_run_directory, file_or_dir))
                            except:
                                pass
            continue
    all_files = os.listdir(the_scenario.scenario_run_directory)
    if len(all_files) > 2:
        logger.warning("something went wrong in checks_and_cleanup. There are {} extra folders and files".format(len(all_files)-2))
    else:
        logger.debug("only two files left in the scenario run directory... continue")

# ===================================================================================================


def setup(the_scenario, logger):

    logger.debug("start: setup")
    start_time = datetime.datetime.now()
    logger.info("Scenario Name: \t{}".format(the_scenario.scenario_name))
    logger.debug("Scenario Description: \t{}".format(the_scenario.scenario_description))
    logger.info("Scenario Start Date/Time: \t{}".format(start_time))

    # create a folder for debug and intermediate files
    # delete everything in there if it exists
    # ------------------------------------------------
    debug_directory = os.path.join(the_scenario.scenario_run_directory, "debug")

    if os.path.exists(debug_directory):
       logger.debug("deleting debug_directory and contents.")
       rmtree(debug_directory)

    if not os.path.exists(debug_directory):
        os.makedirs(debug_directory)
        logger.debug("creating debug_directory.")

    # create the scenario database main.db
    create_main_db(logger, the_scenario)

    # create the scenario geodatabase; main.gdb
    create_main_gdb(logger, the_scenario)

    logger.debug("finish: SETUP:  Runtime (HMS): \t{}".format(ftot_supporting.get_total_runtime_string(start_time)))


# ===================================================================================================


def create_main_gdb(logger, the_scenario):
    # create the GIS geodatabase main.gdb
    # -----------------------------------
    logger.info("start: create_main_gdb")
    scenario_gdb = the_scenario.main_gdb

    import arcpy

    if arcpy.Exists(scenario_gdb):

        logger.debug("start: delete existing main.gdb")
        logger.debug("main.gdb file location: " + scenario_gdb)
        try:
            arcpy.Delete_management(scenario_gdb)

        except:
            logger.error("Couldn't delete " + scenario_gdb)
            raise Exception("Couldn't delete " + scenario_gdb)

    # copy in the base network from the baseline data
    # -----------------------------------------------

    if not arcpy.Exists(the_scenario.base_network_gdb):
        error = "can't find base network gdb {}".format(the_scenario.base_network_gdb)
        raise IOError(error)

    logger.info("start: copy base network to main.gdb")
    logger.config("Copy Scenario Base Network: \t{}".format(the_scenario.base_network_gdb))
    arcpy.Copy_management(the_scenario.base_network_gdb, scenario_gdb)

    # Check for disruption csv-- if one exists, this is where we remove links in the network that are fully disrupted.
    if the_scenario.disruption_data == "None":
        logger.info('disruption file not specified. No disruption to the network will be applied.')
    else:
        if not os.path.exists(the_scenario.disruption_data):
            logger.warning("warning: cannot find disruption_data file: {}. No disruption to the network will be applied"
                           .format(the_scenario.disruption_data))
        else:
            # check that is actually a csv
            if not the_scenario.disruption_data.endswith("csv"):
                error = "error: disruption_data file: {} is not a csv file. Please use valid disruption_data csv"\
                    .format(the_scenario.disruption_data)
                logger.error(error)
                raise Exception(error)
            else:
                logger.info("Disruption csv to be applied to the network: {}".format(the_scenario.disruption_data))
                with open(the_scenario.disruption_data, 'r') as rf:
                    line_num = 1
                    for line in rf:
                        csv_row = line.rstrip('\n').split(',')
                        if line_num == 1:
                            if csv_row[0] != 'mode' or csv_row[1]!= 'unique_link_id' or csv_row[2] != 'link_availability':
                                error = "Error: disruption_data file: {} does not match the appropriate disruption "\
                                        "data schema. Please check that the first three columns are 'mode', "\
                                        "'unique_link_id' and 'link_availability'".format(the_scenario.disruption_data)
                                logger.error(error)
                                raise Exception(error)
                        if line_num > 1:
                            mode = csv_row[0]
                            link = csv_row[1]
                            link_availability = float(csv_row[2])
                            if link_availability == 0:  # 0 = 100% disruption
                                with arcpy.da.UpdateCursor(os.path.join(scenario_gdb, "network/" + mode), ['OBJECTID'], "OBJECTID = {}".format(link)) as ucursor:
                                    for gis_row in ucursor:
                                        ucursor.deleteRow()
                                        logger.info("Disruption scenario removed OID {} from {} network".format(link, mode))
                                del ucursor
                        line_num += 1

    # double check the artificial links for intermodal facilities are set to 2
    # -------------------------------------------------------------------------
    ftot_supporting_gis.set_intermodal_links(the_scenario, logger)


# ==============================================================================


def import_afpat(logger, the_scenario):
    # import the aftpat excel data to a table in the gdb
    # --------------------------------------------------

    ftot_program_directory = os.path.dirname(os.path.realpath(__file__))

    afpat_spreadsheet = os.path.join(ftot_program_directory, "lib", "AFPAT.xlsx")

    if not os.path.exists(afpat_spreadsheet):
        error = "can't find afpat excel spreadsheet {}".format(afpat_spreadsheet)
        raise IOError(error)

    import_afpat_data(afpat_spreadsheet, the_scenario.main_gdb, logger)

    ftot_supporting_gis.persist_AFPAT_tables(the_scenario,  logger)

# ==============================================================================


def create_main_db(logger, the_scenario):
    # create a configuration table and a
    # facilities table
    #---------------------------------------------------

    scenario_db = the_scenario.main_db
    logger.info("start: create_main_db")
    if os.path.exists(scenario_db):
        logger.debug("start: deleting main.db")
        logger.debug("sqlite file database {} exists".format(scenario_db))
        os.remove(scenario_db)
        logger.debug("finished: deleted main.db")

    if not os.path.exists(scenario_db):
        logger.debug("sqlite database file doesn't exist and will be created: {}".format(scenario_db))

        with sqlite3.connect(scenario_db) as db_con:

            db_cur = db_con.cursor()

            db_cur.executescript(
                "create table config(param text, value text, primary key(param));")

    logger.debug("finished: created main.db")

# ==============================================================================
