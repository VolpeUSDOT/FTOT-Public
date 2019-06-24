#-------------------------------------------------------------------------------
# Name:        network testing
# Purpose:      create testable chunks for the phase 4 testing
#
# Author:      Matthew.Pearlson.CTR
#
# Created:     06/01/2017
# Copyright:   (c) Matthew.Pearlson.CTR 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import os
import arcpy
import sqlite3
import datetime


import ftot_routing
import ftot_facilities
import ftot_supporting_gis



def test_rmp_pipeline_artificial_links(the_scenario, logger):

    # checks to see if the raw material producers are hooked into the network correctly.
    logger.info("Starting: test_rmp_pipeline_artificial_links()")
    logger.info("checks to see if the raw material producers are hooked into the pipeline network.")
    scenario_gdb = the_scenario.main_gdb
    if not os.path.exists(scenario_gdb):
        error = "can't find scenario gdb {}".format(scenario_gdb)
        raise IOError(error)

    raw_mat_producers_fc = the_scenario.rmp_fc

    # FLAG raw material producerS THAT CONNECT INTO THE PIPELINE NETWORK
    # ---------------------------------------------------------------------------------
    query = "connects_pipeline = 1"

    list_of_pipeline_connected_facilities = []
    with arcpy.da.SearchCursor(raw_mat_producers_fc, ['facility_name'], where_clause=query) as scursor:
        for row in scursor:
            list_of_pipeline_connected_facilities.append(row[0])
            logger.warn('Raw Mat Supplier {} connects to the pipeline'.format(row[0]))

    logger.result("# of raw material producers  connected to the pipeline: \t{}".format(len(list_of_pipeline_connected_facilities)))

    if len(list_of_pipeline_connected_facilities) > 0:
        logger.result("TEST FAIL: {} raw material producers attached to the pipeline network".format(len(list_of_pipeline_connected_facilities)))
        error = "There are raw material facilities connected to the pipeline! See the log for detailed warnings"
        logger.error(error)
        raise IOError(error)
    else:
        logger.result("TEST PASS: No raw material producers attached to the pipeline network")

    return

# this is the network testing code
# the network and routing will be testing.
# tests include:
# -- test as is, baseline to get a list of routes as is.
# -- no modes, where we set the cost for each mode to -1 to restrict flow.
# -- unimodal test, where one mode is allowed at a time.
# -- multimodal test, where many modes are allowed
# -- intermodal test, where intermodal transport is enabled and verified.
# -- intermodal negative test, where intermodal transport is restricted and verfied.
#

# the general flow of these tests is as follows:
# (1) the network template is populated with test collateral.
# (2) the facilities hooked in with aritifical links
# (3) network costs and flow restrictions are set,
# (4) the network is built
# (5) the routes are solved
# (6) the routes are cached
# (7) the routes are processed and tested for validity.
# (8) next test.

def test_network_routing(the_scenario, logger):

    # already called aftot_setup.setup() to initialize and clean up :
    # - starting with populate network template with modes and intermodal.
    # no other facilities yet (e.g. raw material producers or destinations).



    # Setup Facilities for the Test
    # i.   - append facilities to the network dataset template
    # ii.  - hook the facilities with artificial links
    # --------------------------------------------------------

    ftot_facilities.raw_material_producers_setup(the_scenario, logger)
    ftot_facilities.ultimate_destinations_setup(the_scenario, logger)
    ftot_facilities.hook_facilities_into_network(the_scenario, logger, False) # false means pipeline doesn't get used

    #   assign baseline costs to the network
    ftot_supporting_gis.assign_network_costs(the_scenario, logger, False, None)  # false means pipeline doesn't get used

    # delete the route cache from the previous test
    delete_routes_cache_db(logger, the_scenario)


    # test 0 : baseline as is.
    # ---------------------------
    logger.info("START TEST 0: baseline as is")
    temp_start_time = datetime.datetime.now()

    # reset artificial link costs to what they should be
    logger.info("reset_flow_restrictions to what they should be")
    reset_artificial_link_flow_restrictions(logger, the_scenario)

    # - build the network template
    ftot_supporting_gis.build_the_network(the_scenario, logger)

    # - route as is
    ftot_routing.initial_routing(the_scenario, logger)

    # output route information and maps
    # -----------------------------------
    # some sort of test or diagnostics. what is the baseline?
    # report out the following from the main.db:
        # GENERAL - number of routes, avg. cost, total cost,
    get_main_db_route_info(the_scenario, logger)


    # report out from the routes_cahce.db
    get_route_cache_db_route_info(the_scenario, logger)

    logger.info("TODO: output route information and maps")

    temp_end_time = datetime.datetime.now()
    temp_total_time = temp_end_time - temp_start_time
    logger.result("END TEST 0: baseline as is. Total time: {:.10}".format(temp_total_time))

# todo - mnp - 06152015 -- test 1 runs fine, but test 2 fails at "start: delete_temp_gdbs".
# this test has 4 messages: No routing solution found -- the other methods have 2. ???
# WindowsError: [Error 32] The process cannot access the file because it is being used by another process: 'C:\\FTOT\\branches\\2017_06_15_V3_SP1\\Scenarios\\prototype_testing\\proto1\\test5_ASCENT_a2j\\temp_route_gdbs\\tmp_route_solve_batch_0.gdb\\a00000009.gdbtable'
#    # test 1 : no available modes - all artificial links cost -1
#    # ----------------------------------------------------------
#    logger.info("START TEST 1: no available modes - all artificial links cost -1")
#    temp_start_time = datetime.datetime.now()
#
#    # delete the route cache from the previous test
#    delete_routes_cache_db(logger, the_scenario)
#
#    # set artificial link costs to -1
#    logger.info("set_flow_restrictions for all aritificial links to -1")
#    ftot_supporting_gis.set_flow_restrictions(logger, the_scenario, "no_modes")
#
#    # build the network inline since the only thing that changed is network artificial link costs
#    logger.info("Building the network again with new impedences")
#    arcpy.BuildNetwork_na(os.path.join(the_scenario.main_gdb, "Network/Network_ND"))
#
#    # do the routing
#    logger.info("Do the routing again.")
#    try:
#        ftot_routing.initial_routing(the_scenario, logger)
#        logger.error("FAIL: the routing should have failed, but didn't. check your artificial links.")
#    except:
#        logger.result("PASS: the routing failed as expected because there are no valid routes.")
#
#    #TODO - need to deeper check to make sure that if the scenario solves a valid route
#    get_main_db_route_info(the_scenario, logger)
#    temp_end_time = datetime.datetime.now()
#    temp_total_time = temp_end_time - temp_start_time
#    logger.result("END TEST 1: no modes. Total time: {:.10}".format(temp_total_time))

#
    # test 2 : only available mode: road  - all other artificial links cost -1
    # ----------------------------------------------------------
    logger.info("START TEST 2 : only available mode: road  - all other artificial links cost -1")
    temp_start_time = datetime.datetime.now()

    # delete the route cache from the previous test
    delete_routes_cache_db(logger, the_scenario)

    # reset artificial link costs to what they should be
    logger.info("reset_flow_restrictions to what they should be")
    reset_artificial_link_flow_restrictions(logger, the_scenario)


    # set artificial link costs to -1 for everything but road
    logger.info("set flow restrictions on rail, water, pipeline")
    for mode in ['rail', 'water', 'pipeline']:
        ftot_supporting_gis.set_flow_restrictions(logger, the_scenario, "no_modes", mode)


    # build the network inline since the only thing that changed is network artificial link costs
    logger.info("Building the network again with new impedences")
    arcpy.BuildNetwork_na(os.path.join(the_scenario.main_gdb, "Network/Network_ND"))

    # do the routing
    logger.info("Do the routing again.")
    ftot_routing.initial_routing(the_scenario, logger)

    get_main_db_route_info(the_scenario, logger)
    get_route_cache_db_route_info(the_scenario, logger)

    temp_end_time = datetime.datetime.now()
    temp_total_time = temp_end_time - temp_start_time
    logger.result("END TEST 2: road only. Total time: {:.10}".format(temp_total_time))

    # test 3: only available mode: rail  - all other artificial links cost -1
    # ----------------------------------------------------------
    logger.info("START TEST 3 : only available mode: rail - all other artificial links cost -1")
    temp_start_time = datetime.datetime.now()

    # delete the route cache from the previous test
    delete_routes_cache_db(logger, the_scenario)

    # reset artificial link costs to what they should be
    logger.info("reset_flow_restrictions to what they should be")
    reset_artificial_link_flow_restrictions(logger, the_scenario)


    # set artificial link costs to -1 for everything but road
    logger.info("set flow restrictions on road, water, pipeline")
    for mode in ['road', 'water', 'pipeline']:
        ftot_supporting_gis.set_flow_restrictions(logger, the_scenario, "no_modes", mode)


    # build the network inline since the only thing that changed is network artificial link costs
    logger.info("Building the network again with new impedences")
    arcpy.BuildNetwork_na(os.path.join(the_scenario.main_gdb, "Network/Network_ND"))

    # do the routing
    logger.info("Do the routing again.")
    ftot_routing.initial_routing(the_scenario, logger)

    get_main_db_route_info(the_scenario, logger)
    get_route_cache_db_route_info(the_scenario, logger)

    temp_end_time = datetime.datetime.now()
    temp_total_time = temp_end_time - temp_start_time
    logger.result("END TEST 3: rail only. Total time: {:.10}".format(temp_total_time))


    # test 4: only available mode: water - all other artificial links cost -1
    # ----------------------------------------------------------
    logger.info("START TEST 4 : only available mode: water- all other artificial links cost -1")
    temp_start_time = datetime.datetime.now()

    # delete the route cache from the previous test
    delete_routes_cache_db(logger, the_scenario)

    # reset artificial link costs to what they should be
    logger.info("reset_flow_restrictions to what they should be")
    reset_artificial_link_flow_restrictions(logger, the_scenario)


    # set artificial link costs to -1 for everything but road
    logger.info("set flow restrictions on road, water, pipeline")
    for mode in ['road', 'rail', 'pipeline']:
        ftot_supporting_gis.set_flow_restrictions(logger, the_scenario, "no_modes", mode)


    # build the network inline since the only thing that changed is network artificial link costs
    logger.info("Building the network again with new impedences")
    arcpy.BuildNetwork_na(os.path.join(the_scenario.main_gdb, "Network/Network_ND"))

    # do the routing
    logger.info("Do the routing again.")
    try:
        ftot_routing.initial_routing(the_scenario, logger)
    except:
        logger.warning("WARNING: the routing has no valid routes on the water mode.")
        logger.result("WARNING: the routing has no valid routes on the water mode.")

    #TODO - need to deeper check to make sure that if the scenario solves a valid route
    get_main_db_route_info(the_scenario, logger)
    get_route_cache_db_route_info(the_scenario, logger)

    temp_end_time = datetime.datetime.now()
    temp_total_time = temp_end_time - temp_start_time
    logger.result("END TEST 4: water only. Total time: {:.10}".format(temp_total_time))


    # test 5: only available mode: pipeline - all other artificial links cost -1
    # --------------------------------------------------------------------------
    # todo - mnp - 06152017 - this will fail if pipeline is not connected in the setup phase.
    logger.info("START TEST 5 : only available mode: pipeline - all other artificial links cost -1")
    temp_start_time = datetime.datetime.now()

    # delete the route cache from the previous test
    delete_routes_cache_db(logger, the_scenario)

    # reset artificial link costs to what they should be
    logger.info("reset_flow_restrictions to what they should be")
    reset_artificial_link_flow_restrictions(logger, the_scenario)


    # set artificial link costs to -1 for everything but road
    logger.info("set flow restrictions on road, water, pipeline")
    for mode in ['road', 'rail', 'water']:
        ftot_supporting_gis.set_flow_restrictions(logger, the_scenario, "no_modes", mode)


    # build the network inline since the only thing that changed is network artificial link costs
    logger.info("Building the network again with new impedences")
    arcpy.BuildNetwork_na(os.path.join(the_scenario.main_gdb, "Network/Network_ND"))

    # do the routing
    logger.info("Do the routing again.")
    try:
        ftot_routing.initial_routing(the_scenario, logger)
    except:
        logger.warning("WARNING: the routing has no valid routes on the pipeline mode.")
        logger.result("WARNING: the routing has no valid routes on the pipeline mode.")

    get_main_db_route_info(the_scenario, logger)
    get_route_cache_db_route_info(the_scenario, logger)

    temp_end_time = datetime.datetime.now()
    temp_total_time = temp_end_time - temp_start_time
    logger.result("END TEST 5: pipeline only. Total time: {:.10}".format(temp_total_time))




def delete_routes_cache_db(logger, the_scenario):

    routes_cache_db = the_scenario.routes_cache

    if os.path.exists(routes_cache_db):
        #delete the routes cache and start over.
        logger.info("start: deleting routes_cache.db")
        logger.debug("sqlite file database {} exists".format(routes_cache_db))
        os.remove(routes_cache_db)
        logger.info("finished: deleted routes_cache.db")

#-----------------------------------------------------------------------------

def reset_artificial_link_flow_restrictions(logger, the_scenario, mode='All'):
    logger.info("reset_artificial_link_flow_restrictions on")
    for flow_restriction in ['raw_material_producers', 'ultimate_destinations', 'intermodal']:
        ftot_supporting_gis.set_flow_restrictions(logger, the_scenario, flow_restriction, mode)
    logger.info("end reset_artificial_link_flow_restrictions")


#-----------------------------------------------------------------------------
def  get_main_db_route_info(the_scenario, logger):

    logger.info("start: get_main_db_route_info")

    with sqlite3.connect(the_scenario.main_db) as main_db_con:

        # create a query for getting the routes and costs
        # -------------------------------------------------------------

        logger.debug("start: get routes.")

        sql = "select count(cost), min(cost), max(cost), avg(cost), sum(cost) from routes;"

        db_cur = main_db_con.execute(sql)

        count_cost = 0
        min_cost   = 0
        max_cost   = 0
        avg_cost   = 0
        sum_cost   = 0

        for row in db_cur:

            count_cost = row[0]
            min_cost = row[1]
            max_cost = row[2]
            avg_cost = row[3]
            sum_cost = row[4]

    if count_cost > 0:
        logger.result("count_cost: {:,.2f}".format(count_cost))
        logger.result("min_cost: {:,.2f}".format(min_cost))
        logger.result("max_cost: {:,.2f}".format(max_cost))
        logger.result("avg_cost: {:,.2f}".format(avg_cost))
        logger.result("sum_cost: {:,.2f}".format(sum_cost))
    else:
        logger.result("count_cost: {:,.2f}".format(0))
        logger.result("min_cost: {:,.2f}".format(0))
        logger.result("max_cost: {:,.2f}".format(0))
        logger.result("avg_cost: {:,.2f}".format(0))
        logger.result("sum_cost: {:,.2f}".format(0))
    logger.debug("finish: get_main_db_route_info")

    return

    #-----------------------------------------------------------------------------
def  get_route_cache_db_route_info(the_scenario, logger):

    logger.info("start: get_route_cache_db_route_info")

    with sqlite3.connect(the_scenario.routes_cache) as routes_cache_con:

        # create a query for getting the mode length, mode avg cost, mode total cost
        # how many links, etc.
        # -------------------------------------------------------------

        logger.debug("start: get route info by mode.")

        sql = "select source_mode_id, count(source_mode_id) from route_segments group by source_mode_id;"

        db_cur = routes_cache_con.execute(sql)

        source_mode_id = -1
        count_source_mode_id  = {}

        for row in db_cur:
            source_mode_id = row[0]

            if source_mode_id not in count_source_mode_id.keys():
                count_source_mode_id[source_mode_id] = 0

            count_source_mode_id[source_mode_id] = row[1]


    if len(count_source_mode_id.keys()) > 0:
        for source_mode_id in count_source_mode_id.keys():
            logger.result("Mode: {} count_links: {}".format(ftot_routing.get_source_mode_name(source_mode_id), count_source_mode_id[source_mode_id] ))

    else:
        logger.result("No route info to display.")


    logger.debug("finish: get_route_cache_db_route_info")

    return

