# ---------------------------------------------------------------------------------------------------
# Name: ftot.py
#
# Purpose: This module initiates an FTOT run, parses the scenario xml into a scenario object
# and calls the appropriate tasks based on user supplied arguments.
#
# ---------------------------------------------------------------------------------------------------

import sys
import os
import argparse
import ftot_supporting
import traceback
import datetime

from pint import UnitRegistry

ureg = UnitRegistry()
Q_ = ureg.Quantity
ureg.define('usd = [currency]')  # add the US Dollar, "USD" to the unit registry

VERSION_NUMBER = "5.0.4"
VERSION_DATE = "12/12/2019"

# ===================================================================================================

if __name__ == '__main__':

    start_time = datetime.datetime.now()

    # PARSE ARGS
    # ----------------------------------------------------------------------------------------------

    program_description = 'Freight/Fuels Transportation Optimization Tool (FTOT). Version Number: ' \
                          + VERSION_NUMBER + ", (" + VERSION_DATE + ")"

    help_text = """
    The command-line input expected for this script is as follows:

    TheFilePathOfThisScript FullPathToXmlScenarioConfigFile TaskToRun

    Valid values of Task to run include:
        
            # pre-optimization options
            # -----------------------
            s  = setup; deletes existing database and geodatabase, creates new database, and copies base network to 
            scenario geodatabase
            f  = facilities; process GIS feature classes and commodity input CSV files
            c  = connectivity; connects facilities to the network using artificial links and exports geodatabase FCs as 
            shapefiles
            g  = graph; reads in shapefiles using networkX and prepares, cleans, and stores the network digraph in the database
            
            # optimization options
            # --------------------
            o1 = optimization setup; structures tables necessary for optimization run
            TODO pickle "prob" and move at least variable creation to o1, constraints if possible
            
            o2 = optimization calculation; Calculates the optimal flow and unmet demand for each OD pair with a route
            
            o2b = optional step to solve and save pulp problem from pickled, constrained, problem
            
            oc1-3 = optimization candidate generation; optional step to create candidate processors in the GIS and DB 
            based off the FTOT v5 candidate generation algorithm optimization results 
            
            oc2b = optional step to solve and save pulp problem from pickled, constrained, problem. Run oc3 after.

            os =  optimization sourcing; optional step to calculate source facilities for optimal flows, 
            uses existing solution from o and must be run after o
            
            # post-optimization options
            # -------------------------
            
            f2 = facilities; same as f, used for candidate generation and labeled differently for reporting purposes
            c2 = connectivity; same as c, used for candidate generation and labeled differently for reporting purposes 
            g2 = graph; same as g, used for candidate generation and labeled differently for reporting purposes
            
            # reporting and mapping 
            # ---------------------
            p  = post-processing of optimizal solution and reporting preparation
            d  = create data reports
            m  = create map documents
            m2 = time and commodity mapping
            
            # utilities, tools, and advanced options
            # ---------------------------------------
            test = a test method that can be used for debugging purposes
            --skip_arcpy_check = a command line option to skip the arcpy dependency check  
    """

    parser = argparse.ArgumentParser(description=program_description, usage=help_text)

    parser.add_argument("config_file", help="The full path to the XML Scenario", type=str)

    parser.add_argument("task", choices=("s", "f", "f2", "c", "c2", "g", "g2",
                                         "o", "oc",
                                         "o1", "o2", "o2b", "oc1", "oc2", "oc2b", "oc3", "os", "p",
                                         "d", "m", "m2",
                                         "test"
                                         ), type=str)
    parser.add_argument('-skip_arcpy_check', action='store_true',
                        default=False,
                        dest='skip_arcpy_check',
                        help='Use argument to skip the arcpy dependency check')

    args = parser.parse_args()

    if len(sys.argv) >= 3:
        args = parser.parse_args()
    else:
        parser.print_help()
        sys.exit()

    if os.path.exists(args.config_file):
        ftot_program_directory = os.path.dirname(os.path.realpath(__file__))
    else:
        print "{} doesn't exist".format(args.config_file)
        sys.exit()

    # set up logging and report run start time
    # ----------------------------------------------------------------------------------------------
    xml_file_location = os.path.abspath(args.config_file)
    from ftot_supporting import create_loggers

    logger = create_loggers(os.path.dirname(xml_file_location), args.task)

    logger.info("=================================================================================")
    logger.info("============= FTOT RUN STARTING.  Run Option = {:2} ===============================".format(str(args.task).upper()))
    logger.info("=================================================================================")

    #  load the ftot scenario
    # ----------------------------------------------------------------------------------------------

    from ftot_scenario import *

    xml_schema_file_location = os.path.join(ftot_program_directory, "lib", "Master_FTOT_Schema.xsd")

    if not os.path.exists(xml_schema_file_location):
        logger.error("can't find xml schema at {}".format(xml_schema_file_location))
        sys.exit()

    logger.debug("start: load_scenario_config_file")
    the_scenario = load_scenario_config_file(xml_file_location, xml_schema_file_location, logger)

    # write scenario configuration to the log for incorporation in the report
    # -----------------------------------------------------------------------
    dump_scenario_info_to_report(the_scenario, logger)

    if args.task in ['s', 'sc']:
        create_scenario_config_db(the_scenario, logger)
    else:
        check_scenario_config_db(the_scenario, logger)

    # check that arcpy is available if the -skip_arcpy_check flag is not set
    # ----------------------------------------------------------------------
    if not args.skip_arcpy_check:
        try:
            import arcpy
            arcmap_version = arcpy.GetInstallInfo()['Version']
            if not arcmap_version in ['10.1', '10.2', '10.2.1', '10.2.2', '10.3.0', '10.3.1', '10.4.1', '10.5.1', '10.6.1', '10.7.1']:
                logger.error("Version {} of ArcGIS is not currently supported. Exiting.".format(arcmap_version))
                sys.exit()

        except RuntimeError:
            logger.error("You will need ArcGIS 10.1 or later to run this script. Exiting.")
            sys.exit()

        # Check for version of ArcGIS and Network Analyst
        try:
            arcpy.CheckOutExtension("Network")
        except:
            logger.error("This script requires the ArcGIS Network Analyst Toolbox. Exiting.")
            sys.exit()

    # check that pulp is available
    # ----------------------------------------------------------------------------------------------

    try:
        from pulp import *
    except ImportError:
        logger.error("This script requires the PuLP LP modeler to optimize the routing. "
                     "Download the modeler here: http://code.google.com/p/pulp-or/downloads/list.")
        sys.exit()

    # run the task
    # ----------------------------------------------------------------------------------------------

    try:

        # setup the scenario
        if args.task in ['s', 'sc']:
            from ftot_setup import setup
            setup(the_scenario, logger)

        # facilities
        elif args.task in ['f', 'f2']:
            from ftot_facilities import facilities
            facilities(the_scenario, logger)

        # connectivity; hook facilities into the network
        elif args.task in ['c', 'c2']:
            from ftot_routing import connectivity
            connectivity(the_scenario, logger)

        # graph, convert GIS network and facility shapefiles into to a networkx graph
        elif args.task in ['g', 'g2']:
            from ftot_networkx import graph
            graph(the_scenario, logger)

        # optimization
        elif args.task in ['o']:
            from ftot_pulp import o1, o2
            o1(the_scenario, logger)
            o2(the_scenario, logger)

        # candidate optimization
        elif args.task in ['oc']:
            from ftot_pulp_candidate_generation import oc1, oc2, oc3
            oc1(the_scenario, logger)
            oc2(the_scenario, logger)
            oc3(the_scenario, logger)

        # optimization setup
        elif args.task in ['o1']:
            from ftot_pulp import o1
            o1(the_scenario, logger)

        # optimization solve
        elif args.task in ['o2']:
            from ftot_pulp import o2
            o2(the_scenario, logger)

        # optional step to solve and save pulp problem from pickle
        elif args.task == 'o2b':
            from ftot_pulp import o2b
            o2b(the_scenario,logger)

        # optimization option - processor candidates generation
        elif args.task == 'oc1':
            from ftot_pulp_candidate_generation import oc1
            oc1(the_scenario, logger)

        # optimization option - processor candidates generation
        elif args.task == 'oc2':
            from ftot_pulp_candidate_generation import oc2
            oc2(the_scenario, logger)

        elif args.task == 'oc3':
            from ftot_pulp_candidate_generation import oc3
            oc3(the_scenario, logger)

        # optional step to solve and save pulp problem from pickle
        elif args.task == 'oc2b':
            from ftot_pulp import oc2b
            oc2b(the_scenario, logger)

        # post-optimization tracking for source based on existing optimal solution
        elif args.task in ['os', 'bscoe']:
            from ftot_pulp_sourcing import o_sourcing
            o_sourcing(the_scenario, logger)

        # post-process the optimal solution
        elif args.task in ["p", "p2"]:
            from ftot_postprocess import route_post_optimization_db
            route_post_optimization_db(the_scenario, logger)

        # data report
        elif args.task == "d":
            from ftot_report import generate_reports
            generate_reports(the_scenario, logger)

        # maps
        elif args.task == "m":
            from ftot_maps import new_map_creation
            new_map_creation(the_scenario, logger)

        # Time and Commodity Mapping
        elif args.task == "m2":
            from ftot_maps import prepare_time_commodity_subsets_for_mapping
            prepare_time_commodity_subsets_for_mapping(the_scenario, logger)

        elif args.task == "test":
            logger.info("in the test case")

    except:

        stack_trace = traceback.format_exc()
        split_stack_trace = stack_trace.split('\n')
        logger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!! EXCEPTION RAISED !!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        for i in range(0, len(split_stack_trace)):
            trace_line = split_stack_trace[i].rstrip()
            if trace_line != "":  # issue #182 - check if the line is blank. if it isn't, record it in the log.
                logger.error(trace_line)
        logger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!! EXCEPTION RAISED !!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

        sys.exit(1)

    logger.info("======================== FTOT RUN FINISHED: {:2} ==================================".format(
        str(args.task).upper()))
    logger.info("======================== Total Runtime (HMS): \t{} \t ".format(
        ftot_supporting.get_total_runtime_string(start_time)))
    logger.info("=================================================================================")
    logger.runtime(
        "{} Step - Total Runtime (HMS): \t{}".format(args.task, ftot_supporting.get_total_runtime_string(start_time)))
    logging.shutdown()
