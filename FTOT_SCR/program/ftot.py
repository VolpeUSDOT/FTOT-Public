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
import numpy as np

import pint
from pint import UnitRegistry

ureg = UnitRegistry()
Q_ = ureg.Quantity
ureg.define('usd = [currency]')  # add the US Dollar, "USD" to the unit registry
# solves issue in pint 0.9
if float(pint.__version__) < 1:
    ureg.define('short_hundredweight = short_hunderdweight')
    ureg.define('long_hundredweight = long_hunderdweight')

VERSION_NUMBER = "5.0.6"
VERSION_DATE = "06/26/2020"

#=============load parameters from newly developed Python files======================
# Load earthquake scenario matrix  
earthquake_scenario = np.load("earthquake_events.npy")
total_repair_time = np.load("total_repair_time.npy")

total_fuel_amount = 2941633.8
# planning horizon in this example: 20 years
plan_horizon = 20 # unit: year
# total scenario amount:  here we consider 50 scenarios
N = 30
# resilience array
Resilience = np.zeros((N))
R1 = np.zeros((N))
R2 = np.zeros((N))
R3 = np.zeros((N))
weight1 = np.zeros((N))
weight2 = np.zeros((N))
weight3 = np.zeros((N))

# simulation weeks in one year
WeekInYear = 52
DayInYear = 52*7
# Unmet demand threshold
UD_level = 4190600
# Total final demand
total_demand_fuel = 136497 # total demand for this supply chain layout
total_final_demand = 4073312.9 + 254401.1 # total demand before optimization
# Variables for output from optimization
#UnmetDemandAmount = np.zeros((int(WeekInYear*7),plan_horizon, N))
#DailyCost = np.zeros((int(WeekInYear*7),plan_horizon, N))
#np.save("UnmetDemandAmount.npy", UnmetDemandAmount)
#np.save("DailyCost.npy", DailyCost)
# ===================================================================================================

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
            p  = post-processing of optimal solution and reporting preparation
            d  = create data reports
            m  = create map documents with simple basemap
            mb = optionally create map documents with a light gray basemap
            mc = optionally create map documents with a topographic basemap
            m2 = time and commodity mapping with simple basemap
            m2b = optionally create time and commodity mapping with a light gray basemap
            m2c = optionally create time and commodity mapping with a topographic basemap
            
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
                                         "d", "m", "mb", "mc", "m2", "m2b", "m2c",
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
            if not arcmap_version in ['10.1', '10.2', '10.2.1', '10.2.2', '10.3', '10.3.1', '10.4', '10.4.1',
                                      '10.5', '10.5.1', '10.6', '10.6.1', '10.7', '10.7.1']:
                logger.error("Version {} of ArcGIS is not currently supported. Exiting.".format(arcmap_version))
                sys.exit()

        except RuntimeError:
            logger.error("You will need ArcGIS 10.1 or later to run this script. Exiting.")
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
        elif args.task in ['o', 'o1','o2']:
            #=================================================Modification================================================================
            # Purpose of following part: (a)iterate optimization per day/year; (b)divide yearly and weekly simulation, based on earthquake occurrence;
            # (c)incorporate resilience calculation for each scenario; (d)save related outputs.

            # for each scenario and time:
            for i in range(N):
                UnmetDemandAmount = np.load("UnmetDemandAmount.npy")
                DailyCost = np.load("DailyCost.npy")
                cost1 = 0
                cost2 = 0
                cost3 = 0
                w1 = np.zeros((plan_horizon))
                w2 = np.zeros((plan_horizon))
                w3 = np.zeros((plan_horizon))
                scenario_num = i
                np.save("scenario_num.npy", scenario_num)
                for t in range(plan_horizon):
                    time_horizon = t
                    np.save("time_horizon.npy", time_horizon)                        
                  # for non earthquake occurrence scenario: weekly basis interval   
                    if earthquake_scenario[i][t] != 0:
                       temp_repair_week = float(total_repair_time[i][t])/float(7) # one week = 7 days 
                       for j in range(WeekInYear):
                           earthquake_week = j
                           #temp_day = int(7*j)
                           np.save("earthquake_week.npy", earthquake_week)
                           
                           from ftot_pulp_weekly import o1, o2
                           o1(the_scenario, logger)
                           o2(the_scenario, logger)


                           for day_1 in range (int(j*7),int((j+1)*7)):
                               UnmetDemandAmount[day_1][t][i] = np.load("unmet_demand_daily.npy")
                               DailyCost[day_1][t][i]  = np.load("costs_daily.npy")  

                               np.save("UnmetDemandAmount.npy", UnmetDemandAmount)
                               np.save("DailyCost.npy", DailyCost)
                           
                           
                           # Calculate initial daily costs for each scenario
                           temp_day = int(7*j)
                           daily_cost = DailyCost[temp_day][t][i] - UnmetDemandAmount[0][0][0] * 5000
                           initial_cost = DailyCost[0][0][0] - UnmetDemandAmount[0][0][0] * 5000
                           UD = UnmetDemandAmount[temp_day][t][i] - float(UD_level)/float(DayInYear)
                           UDR = float(UD*DayInYear)/float(total_demand_fuel)
                           production = float(total_final_demand)/float(DayInYear) - UnmetDemandAmount[temp_day][t][i]

                           
                           if j <= temp_repair_week:
                               UDP = abs(UD * 5000)
                               R2[i] = R2[i] + (float(daily_cost-initial_cost + UDP)/float(production))*7
                               w2[t] = w2[t] + (float(daily_cost-initial_cost + UDP)/float(production))
                               
                           else:
                               if UDR  > 0:
                                   UDP = UD * 5000
                                   R1[i] = R1[i] + (float(daily_cost-initial_cost + UDP)/float(production))*7
                                   w1[t] = w1[t] + (float(daily_cost-initial_cost + UDP)/float(production))
                                   
                               else:
                                   R3[i] = R3[i] + (float(daily_cost-initial_cost)/float(production))*7
                                   w3[t] = w3[t] + (float(daily_cost-initial_cost)/float(production))
                                   
                       # weight factor for R2
                       weight2[i] = weight2[i] + float(sum(w2[:]))/float(temp_repair_week)      

                            
                  # for non earthquake occurrence scenario: yearly basis interval   
                    else:
                       from ftot_pulp_yearly import o1, o2
                       o1(the_scenario, logger)
                       o2(the_scenario, logger)
                       
                       temp_demand = np.load("unmet_demand_yearly.npy")
                       UnmetDemandAmount[:,t,i] = float(temp_demand)/float(DayInYear)
                       temp_dailycost= np.load("costs_yearly.npy")
                       DailyCost[:,t,i] = float(temp_dailycost)/float(DayInYear)
                       np.save("UnmetDemandAmount.npy", UnmetDemandAmount)
                       np.save("DailyCost.npy", DailyCost)

                       # Calculate initial daily costs for each scenario
                       temp_day = 2 # either day is okay, because daily cost is the same for every day
                       daily_cost = DailyCost[temp_day][t][i] - UnmetDemandAmount[0][0][0] * 5000
                       initial_cost = DailyCost[0][0][0]- UnmetDemandAmount[0][0][0] * 5000
                       UD = UnmetDemandAmount[temp_day][t][i] - float(UD_level)/float(DayInYear)
                       UDR = float(UD*DayInYear)/float(total_demand_fuel)
                       production = float(total_final_demand)/float(DayInYear) - UnmetDemandAmount[temp_day][t][i]
                       
                       if UDR  > 0:
                           UDP = UD * 5000
                           R1[i] = R1[i] + (float(daily_cost-initial_cost + UDP)/float(production))*DayInYear
                           w1[t] = w1[t] + (float(daily_cost-initial_cost + UDP)/float(production))
                       else:
                           R3[i] = R3[i] + (float(daily_cost-initial_cost)/float(production))*DayInYear
                           w3[t] = w3[t] + (float(daily_cost-initial_cost)/float(production))

                # weight factor for each resilience componenet equals to average daily cost                    
                weight1[i] = float(sum(w1[:]))/float(np.count_nonzero(w1) + 0.01)
                weight2[i] = float(weight2[i])/float(np.count_nonzero(earthquake_scenario[i,:])+0.01)
                weight3[i] = float(sum(w3[:]))/float(np.count_nonzero(w3)+0.01)
                # assume the same weight for resilience cost and initial cost
                weight_factor = 1
                
                Resilience[i] = weight_factor*(abs(weight1[i]*R1[i]) + abs(weight2[i]*R2[i]) + abs(weight3[i]*R3[i])) + initial_cost*DayInYear*plan_horizon

                #Resilience[i] = weight_factor*(weight1[i]*R1[i] + weight2[i]*R2[i] + weight3[i]*R3[i])+initial_cost*year*plan_horizon
               
                
                logger.info("Weight for hazard-induced CLF for scenario {}: {}".format(i,weight2[i]))
                logger.info("Weight for non-hazard-induced CLF for scenario {}: {}".format(i,weight1[i]))
                logger.info("Weight for opportunity-induced CGF for scenario {}: {}".format(i,weight3[i]))
                logger.info("Resilience for scenario {} : {}".format(i,Resilience[i]))


                np.save("R1.npy",R1)
                np.save("R2.npy",R2)
                np.save("R3.npy",R3)
                np.save("weight1.npy",weight1)
                np.save("weight2.npy",weight2)
                np.save("weight3.npy",weight3)
                np.save("Resilience.npy",Resilience)                   
                                
            

       
        # candidate optimization
        elif args.task in ['oc']:
            from ftot_pulp_candidate_generation import oc1, oc2, oc3
            oc1(the_scenario, logger)
            oc2(the_scenario, logger)
            oc3(the_scenario, logger)
        
           

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

        # currently m step has three basemap alternatives-- see key above
        elif args.task in ["m", "mb", "mc"]:
            from ftot_maps import new_map_creation
            new_map_creation(the_scenario, logger, args.task)

        # currently m2 step has three basemap alternatives-- see key above
        elif args.task in ["m2", "m2b", "m2c"]:
            from ftot_maps import prepare_time_commodity_subsets_for_mapping
            prepare_time_commodity_subsets_for_mapping(the_scenario, logger, args.task)

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
