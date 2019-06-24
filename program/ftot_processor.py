# ------------------------------------------------------------------------------
# Name: ftot_processor.py
#
# Purpose: generates candidates from optimized flow of feedstock to the
#  max_raw_material_transport distance
#
# ------------------------------------------------------------------------------

import os
import random
import sqlite3
import datetime
import arcpy
import ftot_supporting
import ftot_supporting_gis
from ftot_facilities import get_commodity_id
from ftot_pulp import parse_optimal_solution_db
from ftot import Q_


# ==============================================================================


def clean_candidate_processor_tables(the_scenario, logger):
    logger.debug("start: clean_candidate_processor_tables")

    # clean up the tables, then create them.
    # ----------------------------------------
    with sqlite3.connect(the_scenario.main_db) as db_con:
        db_con.executescript("""
            drop table if exists candidate_process_list;
            drop table if exists candidate_process_commodities;
    
            create table candidate_process_list(
                process_id   INTEGER PRIMARY KEY,
                process_name text,
                minsize numeric,
                maxsize numeric,
                min_max_size_units text,
                cost_formula numeric,
                cost_formula_units text
            );
    
    
            create table candidate_process_commodities(
                process_id         integer,
                io                 text,
                commodity_name     text,
                commodity_id       integer,
                quantity           numeric,
                units              text,
                phase_of_matter    text
            );
        """)

    return


# ------------------------------------------------------------------------------


def generate_candidate_processor_tables(the_scenario, logger):
    logger.info("start: generate_candidate_processor_tables")
    # drops, and then creates 2 tables related to candidates:
    # (1) candidate_process_list, which creates a unique ID for each process name, 
    # and records the respective min and max size, units, and cost formula and units. 

    # clean up the tables, then create them.
    # ----------------------------------------
    clean_candidate_processor_tables(the_scenario, logger)

    # if the user specifies a processor_candidate_slate_data file
    # use that to populate the candidate_processor_list and
    # candidate_processor_commodities tables.
    #
    # note: the processors_candidate_slate_data attribute is user specified slates for generic conversion.
    # whereas the processors_commodity_data is FTOT generated for specific candidates
    if the_scenario.processors_candidate_slate_data != 'None':

        # get the slate from the facility_commodities table
        # for all facilities named "candidate%" 
        # (note the wild card, '%' in the search).
        candidate_process_list, candidate_process_commodities = get_candidate_process_data(the_scenario, logger)

        # process the candidate_process_list first so we can assign a process_id
        # the method returns a dictionary keyed off the process_name and has process_id as the value.
        # -----------------------------------------------------------------------
        process_id_name_dict = populate_candidate_process_list_table(the_scenario, candidate_process_list, logger)

        # process the candidate_process_commodities list.
        # ------------------------------------------------
        populate_candidate_process_commodities(the_scenario, candidate_process_commodities, process_id_name_dict,
                                               logger)


# ------------------------------------------------------------------------------


def populate_candidate_process_commodities(the_scenario, candidate_process_commodities, process_id_name_dict, logger):
    logger.info("start: populate_candidate_process_commodities")

    logger.debug("number of process IDs: {} ".format(len(process_id_name_dict.keys())))
    logger.debug("number of commodity records: {}".format(len(candidate_process_commodities)))

    with sqlite3.connect(the_scenario.main_db) as db_con:

        # iterate through the list and get the process_id and commodity_id
        for commodity in candidate_process_commodities:
            logger.debug("start: {} ".format(commodity[0]))

            # replace the process_name with process_id
            process_name = commodity[0]
            process_id = process_id_name_dict[process_name]
            commodity[0] = process_id

            # reusing the ftot_facilities.get_commodity_id module.
            # so we need to get the dataset into the proper format it is expecting
            # to add the commodity if it doesn't exist in the db yet.
            commodity_name = commodity[1]
            commodity_quantity = commodity[3]
            commodity_unit = commodity[4]
            commodity_phase = commodity[5]
            commodity_max_transport_dist = 'Null'
            io = commodity[6]
            commodity_data = ['', commodity_name, commodity_quantity, commodity_unit, commodity_phase, commodity_max_transport_dist, io]

            # get commodity_id. (adds commodity if it doesn't exist)
            commodity_id = get_commodity_id(the_scenario, db_con, commodity_data, logger)
            commodity[2] = commodity_id

        # now do an execute many on the lists for the segments and route_segments table
        sql = "insert into candidate_process_commodities (" \
              "process_id, commodity_name, commodity_id, quantity, units, phase_of_matter, io) " \
              "values (?, ?, ?, ?, ?, ?, ?);"
        db_con.executemany(sql, candidate_process_commodities)
        db_con.commit()


# ------------------------------------------------------------------------------


def create_commodity_id_name_dict(the_scenario, logger):
    logger.debug("start: create_commodity_id_name_dict")
    commodity_id_name_dict = {}
    with sqlite3.connect(the_scenario.main_db) as db_con:
        sql = "select commodity_name, commodity_id from commodities;"
        db_cur = db_con.execute(sql)
        data = db_cur.fetchall()
        for row in data:
            commodity_id_name_dict[row[0]] = row[1]
    logger.debug("returning {} commodity id/name records".format(len(commodity_id_name_dict)))
    return commodity_id_name_dict


# ------------------------------------------------------------------------------


def populate_candidate_process_list_table(the_scenario, candidate_process_list, logger):
    logger.debug("start: populate_candidate_process_list_table")

    candidate_process_list_data = []

    for process in candidate_process_list:
        min_size = ''
        min_size_units = ''
        max_size = ''
        max_size_units = ''
        cost_formula = ''
        cost_formula_units = ''
        process_name = process

        for process_property in candidate_process_list[process]:
            if 'minsize' == process_property[0]:
                min_size = process_property[1]
                min_size_units = str(process_property[2])
            if 'maxsize' == process_property[0]:
                max_size = process_property[1]
                max_size_units = str(process_property[2])
            if 'cost_formula' == process_property[0]:
                cost_formula = process_property[1]
                cost_formula_units = str(process_property[2])

        # do some checks to make sure things aren't weird.
        # -------------------------------------------------
        if max_size_units != min_size_units:
            logger.warning("the units for the max_size and min_size candidate process do not match!")
        if min_size == '':
            logger.warning("the min_size is set to Null")
        if max_size == '':
            logger.warning("the min_size is set to Null")
        if cost_formula == '':
            logger.warning("the cost_formula is set to Null")
        if cost_formula_units == '':
            logger.warning("the cost_formula_units is set to Null")

        # otherwise, build up a list to add to the sql database.
        candidate_process_list_data.append(
            [process_name, min_size, max_size, max_size_units, cost_formula, cost_formula_units])

    # now do an execute many on the lists for the segments and route_segments table
    with sqlite3.connect(the_scenario.main_db) as db_con:
        sql = "insert into candidate_process_list " \
              "(process_name, minsize, maxsize, min_max_size_units, cost_formula, cost_formula_units) " \
              "values (?, ?, ?, ?, ?, ?);"
        db_con.executemany(sql, candidate_process_list_data)
        db_con.commit()

        # now return a dictionary  of process_id and process_names
        process_id_name_dict = {}
        sql = "select process_name, process_id from candidate_process_list;"
        db_cur = db_con.execute(sql)
        db_data = db_cur.fetchall()
        for row in db_data:
            process_id_name_dict[row[0]] = row[1]
    logger.info("note: added {} candidate processes to the candidate_process_list table".format(
        len(process_id_name_dict.keys())))
    logger.debug("ID || Process Name:")
    logger.debug("-----------------------")
    for process_name in process_id_name_dict:
        logger.debug("{} || {}".format(process_id_name_dict[process_name], process_name))
    return process_id_name_dict


# =====================================================================================================================


def get_candidate_process_data(the_scenario, logger):
    logger.info("start: get_candidate_process_data")

    from ftot_facilities import load_facility_commodities_input_data

    candidate_process_data = load_facility_commodities_input_data(the_scenario,
                                                                  the_scenario.processors_candidate_slate_data, logger)
    candidate_process_commodities = []
    candidate_process_list = {}

    for facility_name, facility_commodity_list in candidate_process_data.iteritems():
        for row in facility_commodity_list:
            commodity_name = row[1]
            commodity_id = None
            quantity = row[2]
            units = str(row[3])
            phase_of_matter = row[4]
            io = row[6]

            # store the input and output commodities
            # in the candidate_process_commodities (CPC) list .
            if io == 'i' or io == 'o':
                candidate_process_commodities.append(
                    [facility_name, commodity_name, commodity_id, quantity, units, phase_of_matter, io])

            # store the facility size and cost information in the
            # candidate_process_list (CPL).
            else:
                if facility_name not in candidate_process_list.keys():
                    candidate_process_list[facility_name] = []

                if commodity_name == 'maxsize':
                    candidate_process_list[facility_name].append(["maxsize", quantity, units])
                elif commodity_name == 'minsize':
                    candidate_process_list[facility_name].append(["minsize", quantity, units])
                elif commodity_name == 'cost_formula':
                    candidate_process_list[facility_name].append(["cost_formula", quantity, units])

    # log a warning if nothing came back from the query
    if len(candidate_process_list) == 0:
        logger.warning("the len(candidate_process_list) == 0.")
        logger.info("TIP: Make sure the process names in the Processors_Candidate_Commodity_Data "
                    "are prefixed by the word 'candidate_'. e.g. 'candidate_HEFA'")

    # log a warning if nothing came back from the query
    if len(candidate_process_commodities) == 0:
        logger.warning("the len(candidate_process_commodities) == 0.")
        logger.info("TIP: Make sure the process names in the Processors_Candidate_Commodity_Data "
                    "are prefixed by the word 'candidate_'. e.g. 'candidate_HEFA'")

    return candidate_process_list, candidate_process_commodities


# ------------------------------------------------------------------------------


def get_candidate_processor_slate_output_ratios(the_scenario, logger):
    logger.info("start: get_candidate_processor_slate_output_ratios")
    output_dict = {}
    with sqlite3.connect(the_scenario.main_db) as db_con:
        # first get the input commodities and quantities
        sql = """ 
            select 
                cpl.process_id, 
                cpl.process_name, 
                cpc.io, 
                cpc.commodity_name, 
                cpc.commodity_id,
                quantity,
                units
            from candidate_process_commodities cpc
            join candidate_process_list cpl on cpl.process_id = cpc.process_id
            where cpc.io = 'i'
        ;"""

        db_cur = db_con.execute(sql)
        db_data = db_cur.fetchall()

        for row in db_data:
            process_name = row[1]
            io = row[2]
            commodity_name = row[3]
            commodity_id = row[4]
            quantity = float(row[5])
            units = row[6]

            if process_name not in output_dict.keys():
                output_dict[process_name] = {}
                output_dict[process_name]['i'] = []
                output_dict[process_name]['o'] = []  # initialize the output dict at the same time
            output_dict[process_name]['i'].append([commodity_name, Q_(quantity, units)])

            # next get the output commodities and quantities and scale them by the input quantities
        # e.g. output scaled = output / input
        sql = """ 
            select 
                cpl.process_id, 
                cpl.process_name, 
                cpc.io, 
                cpc.commodity_name, 
                cpc.commodity_id,
                cpc.quantity,
                cpc.units,
                c.phase_of_matter
            from candidate_process_commodities cpc
            join candidate_process_list cpl on cpl.process_id = cpc.process_id
            join commodities c on c.commodity_id = cpc.commodity_id
            where cpc.io = 'o'
        ;"""

        db_cur = db_con.execute(sql)
        db_data = db_cur.fetchall()

        for row in db_data:
            process_name = row[1]
            io = row[2]
            commodity_name = row[3]
            commodity_id = row[4]
            quantity = float(row[5])
            units = row[6]
            phase_of_matter = row[7]

            output_dict[process_name]['o'].append([commodity_name, Q_(quantity, units), phase_of_matter])

    for process in output_dict:
        if 1 != len(output_dict[process]['i']):
            logger.warning("there is more than one input commodity specified for this process!!")
        if 0 == len(output_dict[process]['i']):
            logger.warning("there are no input commodities specified for this process!!")
        if 0 == len(output_dict[process]['o']):
            logger.warning("there are no output commodities specified for this process!!")

    return output_dict


# =============================================================================


def processor_candidates(the_scenario, logger):
    # 12/22/18 - new processor candidates code based on the FTOTv5
    # candidate generation tables generated at the end of the first optimization step 
    # -----------------------------------------------------------------------------

    logger.info("start: generate_processor_candidates")

    # use candidate_nodes, candidate_process_list,
    # and candidate_process_commodities to create the output
    # product slate and candidate facility information including:
    # (min_size, max_size, cost_formula)

    with sqlite3.connect(the_scenario.main_db) as main_db_con:

        # clean-up candidate_processors table
        # ------------------------------------
        logger.debug("drop the candidate_processors table")
        main_db_con.execute("drop table if exists candidate_processors;")
        main_db_con.commit()

        # create the candidate_processors table 
        # with the appropriate XY shape information from the nodeID
        # ---------------------------------------------------------
        logger.debug("create the candidate_processors table")
        main_db_con.executescript(
            """create table candidate_processors as 
                select 
                    xy.shape_x shape_x, 
                    xy.shape_y shape_y, 
                    cpl.process_name || '_' || cn.node_id facility_name, 
                    cpl.process_id process_id,
                    cpc.commodity_name commodity_name, 
                    cpc.commodity_id commodity_id,
                    cn.'agg_value:1' as quantity, 
                    cpc.units units, 
                    cpc.io io,
                    c.phase_of_matter phase_of_matter
                    from candidate_nodes cn
                    join candidate_process_commodities cpc on cpc.commodity_id = cn.commodity_id and cpc.process_id = cn.process_id
                    join candidate_process_list cpl on cpl.process_id = cn.process_id
                    join networkx_nodes xy on cn.node_id = xy.node_id
                    join commodities c on c.commodity_id = cpc.commodity_id
                    group by xy.shape_x, 
                        xy.shape_y, 
                        facility_name, 
                        cpl.process_id,
                        cpc.commodity_name, 
                        cpc.commodity_id,
                        quantity, 
                        cpc.units, 
                        cpc.io,
                        c.phase_of_matter;
                ;""")
        main_db_con.commit()

    # generate the product slates for the candidate locations
    # first get a dictionary of output commodity scalars per unit of input
    # output_dict[process_name]['i'].append([commodity_name, Q_(quantity, units)]) 
    # output_dict[process_name]['o'].append([commodity_name, Q_(quantity, units), phase_of_matter]) 

    output_dict = get_candidate_processor_slate_output_ratios(the_scenario, logger)

    logger.info("opening a csv file")
    with open(the_scenario.processor_candidates_commodity_data, 'w') as wf:

        # write the header line
        header_line = "facility_name,facility_type,commodity,value,units,phase_of_matter,io"
        wf.write(str(header_line + "\n"))

        ## WRITE THE CSV FILE OF THE PROCESSOR CANDIDATES PRODUCT SLATE

        sql = """ 
            select 
                facility_name, 'processor', commodity_name, quantity, units, phase_of_matter, io, cpl.process_name
            from candidate_processors cp
            join candidate_process_list cpl on cpl.process_id = cp.process_id            
        ;"""
        db_cur = main_db_con.execute(sql)
        db_data = db_cur.fetchall()
        for row in db_data:

            facility_name = row[0]
            facility_type = row[1]
            commodity_name = row[2]
            input_quantity = float(row[3])
            input_units = row[4]
            phase_of_matter = row[5]
            io = row[6]
            process_name = row[7]

            # logger.info("writing input commodity: {} and demand: {} \t {}".format(row[2], row[3], row[4]))
            wf.write("{},{},{},{},{},{},{}\n".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6]))

            # write the scaled output commodities too
            # first get the input for the denomenator
            input_scaler_quantity = output_dict[process_name]['i'][0][1]
            # then get the output scaler for the numerator
            for output_scaler in output_dict[process_name]['o']:
                output_commodity_name = output_scaler[0]
                output_scaler_quantity = output_scaler[1]
                output_phase_of_matter = output_scaler[2]
                output_quantity = Q_(input_quantity, input_units) * output_scaler_quantity / input_scaler_quantity
                wf.write(
                    "{},{},{},{},{},{},{}\n".format(row[0], row[1], output_commodity_name, output_quantity.magnitude,
                                                    output_quantity.units, output_phase_of_matter, 'o'))

    # MAKE THE FIRST PROCESSOR POINT LAYER
    # this layer consists of candidate nodes where flow exceeds the min facility size at a RMP,
    # or flow aggregates on the network above the min_facility size (anywhere it gets bigger)
    # ---------------------------------------------------------------------------------------------
    logger.info("create a feature class with all the candidate processor locations: all_candidate_processors")
    scenario_gdb = the_scenario.main_gdb
    all_candidate_processors_fc = os.path.join(scenario_gdb, "all_candidate_processors")

    if arcpy.Exists(all_candidate_processors_fc):
        arcpy.Delete_management(all_candidate_processors_fc)
        logger.debug("deleted existing {} layer".format(all_candidate_processors_fc))

    arcpy.CreateFeatureclass_management(scenario_gdb, "all_candidate_processors", "POINT", "#", "DISABLED", "DISABLED",
                                        ftot_supporting_gis.LCC_PROJ)

    # add fields and set capacity and prefunded fields.
    # ---------------------------------------------------------------------
    arcpy.AddField_management(all_candidate_processors_fc, "facility_name", "TEXT")
    arcpy.AddField_management(all_candidate_processors_fc, "candidate", "SHORT")
    fields = ("SHAPE@X", "SHAPE@Y", "facility_name", "candidate")
    icursor = arcpy.da.InsertCursor(all_candidate_processors_fc, fields)

    main_scenario_gdb = the_scenario.main_gdb

    ### THIS IS WERE WE CHANGE THE CODE ###
    ### DO A NEW SQL QUERY TO GROUP BY FACILITY_NAME AND GET THE SHAPE_X, SHAPE_Y, PROCESS, ETC.
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        sql = """
                select shape_x, shape_y, facility_name
                from candidate_processors
                group by facility_name
            ;"""
        db_cur = main_db_con.execute(sql)
        db_data = db_cur.fetchall()

    for candidate_processor in db_data:
        shape_x = float(candidate_processor[0])
        shape_y = float(candidate_processor[1])
        facility_name = candidate_processor[2]
        # offset slightly from the network node
        offset_x = random.randrange(100, 250, 25)
        offset_y = random.randrange(100, 250, 25)
        shape_x += offset_x
        shape_y += offset_y

        icursor.insertRow([shape_x, shape_y, facility_name, 1])

    del icursor

    return


# ---------------------------------------------------------------------------
# deprecate the below if teh above works.
# ========================================================================

def old_processor_candidates(the_scenario, logger):
    logger.info("start: generate_processor_candidates")
    start_time = datetime.datetime.now()

    layers_to_merge = []
    candidates_fc = 0  # initialize

    # Processor Candidates generation and IDW ranking
    # ---------------------------------------------------------------

    # Generate bulk processor candidates.
    # In this step, prototype candidates are generated based on an accumulation of
    # flow over the optimal links. The output of this step are generic processor facility
    # locations that could theoretically source enough material to justify building a facility.
    # The locations are stored in all_candidate_processors fc in the main.gdb
    generate_bulk_processor_candidates(the_scenario, logger)

    # todo - 8/3/18 - mnp
    # generally there are two approaches here.
    # first, I could just generate the bulk processor_candidates 
    # on the post processed results from first optimization
    # and keep everything else the same.
    # altnernatively, I could query the optimal result and do it that way.
    # generate_bulk_processor_candidates_db(the_scenario, logger)

    # deprecated   
    # Evaluate Feedstock Availability and Assign Processes 
    # This takes the generic facility locations and differentiates them based on feedstock and process.
    # It evaluate availabile commodities and assign a process type to the bulk candidate processors.
    # This goes through each prototype processor and tries to source adequate material
    # from the surrounding counties based of the maximum raw material's maximum 
    # transport distance. The output of this step is the "candidates_for_merging" fc, 
    # and these candidates are added to the locations, facilities, and facilities_commodities tables 
    # in the main.db

    # Create a layer with the processor candidates that are not ignored


#    # ------------------------------------------------------------------
#    logger.info("Create feature class with processors where ignore field != 1")
#
#    candidate_processors_to_merge_fc = os.path.join(the_scenario.main_gdb, "candidate_processors_to_merge")
#
#    if arcpy.Exists(candidate_processors_to_merge_fc):
#        arcpy.Delete_management(candidate_processors_to_merge_fc)
#        logger.debug("deleted existing {} layer".format(candidate_processors_to_merge_fc))
#
#    # first create temporary feature layer from the feature class
#    all_candidate_processors_fc = os.path.join(the_scenario.main_gdb, "all_candidate_processors")
#    arcpy.MakeFeatureLayer_management (all_candidate_processors_fc, "all_candidate_processors_lyr", "ignore is null")
#
#    # select only the processors where ignore fields does not == 1.
#    #arcpy.SelectLayerByAttribute_management("all_candidate_processors_lyr", "NEW_SELECTION", "ignore is null")
#
#    # now copy the features
#    arcpy.CopyFeatures_management("all_candidate_processors_lyr", candidate_processors_to_merge_fc)
#
#    candidate_processors_to_merge_fc
#    
#    # get summary statistics on the candidate processorts
#    get_processor_fc_summary_statistics(the_scenario, candidate_processors_to_merge_fc, logger)  
#
#    logger.info("FINISH: generate_processor_candidates: Runtime (HMS): \t{}".format(
#    ftot_supporting.get_total_runtime_string(start_time)))

# =============================================================================

# def assign_feedstock_and_processes(the_scenario, logger):
#    # mnp 8-23-18 - deprecated the IDW
#    # Inverse Distance Weighting (IDW) Drop lowest scoring candidates
#    # ---------------------------------------------------------------
#    # idw_ranking_of_candidates(the_scenario, logger)
#
#    # Create a layer with the processor candidates that are not ignored
#    # ------------------------------------------------------------------
#    logger.info("Create feature class with processors where ignore field != 1")
#
#    candidate_processors_to_merge_fc = os.path.join(the_scenario.main_gdb, "candidate_processors_to_merge")
#
#    if arcpy.Exists(candidate_processors_to_merge_fc):
#        arcpy.Delete_management(candidate_processors_to_merge_fc)
#        logger.debug("deleted existing {} layer".format(candidate_processors_to_merge_fc))
#
#    # first create temporary feature layer from the feature class
#    all_candidate_processors_fc = os.path.join(the_scenario.main_gdb, "all_candidate_processors")
#    arcpy.MakeFeatureLayer_management (all_candidate_processors_fc, "all_candidate_processors_lyr", "ignore is null")
#
#    # select only the processors where ignore fields does not == 1.
#    #arcpy.SelectLayerByAttribute_management("all_candidate_processors_lyr", "NEW_SELECTION", "ignore is null")
#
#    # now copy the features
#    arcpy.CopyFeatures_management("all_candidate_processors_lyr", candidate_processors_to_merge_fc)
#
#    return candidate_processors_to_merge_fc

# ==============================================================================
# mnp - 8-3-18 --- new candidate generation work in the db.
def generate_bulk_processor_candidates(the_scenario, logger):
    logger.info("starting generate_bulk_processor_candidates")

    candidate_location_oids_dict = {}
    commodities_max_transport_dist_dict = {}

    # the first routing will flow finished fuels.
    # we can back track to the original commodity (or optionally assign a generic feedstock)
    # so that we can aggregate feedstocks for candiate facilities locations. 

    # 8/9/18 -- join the optimal route segments table to convert from feedstock-as-fuel
    # back to feedstock with the appropriate quantities and raw material distances.
    # extra credit -- use a generic feedstock type to aggregate common feedstocks. 

    logger.debug(
        "dropping then adding the optimal_feedstock_flows table with the feedstock_as_fuel flows on the route segments")
    logger.debug(
        "note: this table selects the flow from the first link in the route. this is because pulp sums all flows on "
        "the link")
    logger.warning(
        "this can take a few minutes on the capacitated seattle network... might want to look into optimizing the sql "
        "statement for larger runs")
    sql1 = "DROP TABLE if exists optimal_feedstock_flows;"

    sql2 = """CREATE INDEX  if not exists 'ors_index' ON 'optimal_route_segments' (
	'scenario_rt_id',
	'rt_variant_id',
	'from_position'
         );"""

    sql3 = """
               CREATE TABLE optimal_feedstock_flows as
               select 
                    ors.network_source_id as network_source_id, 
                    ors.network_source_oid as network_source_oid, 
                    odp.from_location_id as location_id, 
                    ors.scenario_rt_id as scenario_rt_Id, 
                    ors.rt_variant_id as rt_variant_id, 
                    ors.from_position as from_position, 
                   (select sum(cumm.miles) from optimal_route_segments cumm where (cumm.scenario_rt_id = 
                   ors.scenario_rt_id and cumm.rt_variant_id = ors.rt_variant_id and ors.from_position >= 
                   cumm.from_position )) as cumm_dist, 
            	   ors.commodity_name as feedstock_as_fuel_name, 
                    (select orig_flow.commodity_flow from optimal_route_segments orig_flow where (
                    orig_flow.scenario_rt_id = ors.scenario_rt_id)) as feedstock_as_fuel_flow , 
                    null as commodity_name, 
                    null as commodity_flow, 
                    cast(null as real) as max_transport_distance,
                    null as ignore_link
                from optimal_route_segments ors
                join optimal_route_segments cumm on ( cumm.scenario_rt_id = ors.scenario_rt_id and ors.from_position 
                >= cumm.from_position ) 
                
                join od_pairs odp on odp.scenario_rt_id = ors.scenario_rt_id
                group by ors.scenario_rt_id, ors.rt_variant_id, ors.from_position
                order by ors.scenario_rt_id, ors.rt_variant_id, ors.from_position
                ;"""

    with sqlite3.connect(the_scenario.main_db) as db_con:
        logger.debug("drop the optimal_feedstock_flows table")
        db_con.execute(sql1)  # drop the table
        logger.debug("create the index on optimal_route_segments")
        db_con.execute(sql2)  # create the index on optimal_route_segments
        logger.debug("create the optimal_feedstock_flows table and add the records")
        db_con.execute(sql3)  # create the table and add the records

    # now set the appropriate feedstock and quantities.
    # then sum the flows by commodity
    # then query the table for flows greater than the facility size but within
    # the raw material transport distance

    # feedstock ratio can be identified by joining the facility_commodities
    # table with the facilities table and the commodities table 
    # using the facility_name (rmp:xxx vs. rmp:xxx_as_proc) and commodity_id 
    # commodity table has raw material transport distance 
    # the facility_commodities table has the ratio of the output feedstock        
    # we can get that ratio and then multiply it against the quantity of 
    # optimal feedstock that flowed to the destination. 

    # note: the optimal route segments table has flows already summed up on the links.
    # that is, a route appears to gain flow as it approaches the destination because
    # other routes add flow to those links. we need to just use the original 
    # amount leaving the facility on that route.

    logger.debug("creating a list of fuel ratios by location_id to scale the feedstock_as_fuel flows")
    sql = """  
            -- this is the code for selecting the rmps and rmps_as_proc facilities 
            -- along with the commodity quanities to get the ratio of feedstock
            -- to feedstock as fuel
    
            select f.facility_name, fti.facility_type, c.commodity_name, c.max_transport_distance, fc.location_id, 
            fc.quantity, fc.units 
            from facility_commodities fc
            join facilities f on f.facility_id = fc.facility_id
            join commodities c on c.commodity_id = fc.commodity_id
            join facility_type_id fti on fti.facility_type_id = f.facility_type_id
            where fti.facility_type like 'raw_material_producer'
            order by fc.location_id
        ;"""

    with sqlite3.connect(the_scenario.main_db) as db_con:
        db_cur = db_con.cursor()
        db_cur.execute(sql)

        # iterate through the db and build the ratios
        fuel_ratios = {}
        for row in db_cur:
            facility_name = row[0]
            facility_type = row[1]
            commodity_name = row[2]
            max_transport_distance = row[3]
            location_id = row[4]
            quantity = row[5]
            units = row[6]
            if not location_id in fuel_ratios.keys():
                fuel_ratios[location_id] = []
                fuel_ratios[location_id].append(0)  # one feedstock
                fuel_ratios[location_id].append([])  # many feedstocks-as-fuel

            if not "_as_proc" in facility_name:
                fuel_ratios[location_id][0] = ([commodity_name, Q_(quantity, units), max_transport_distance])
            else:
                # this is the feedstock-as-fuel value
                fuel_ratios[location_id][1].append([commodity_name, Q_(quantity, units)])

    # I have the ratios and the optimal route segments. 
    # I need to scale the optimal feedstock-as-fuel flows by the ratio to get back to the 
    # feedstock flows. then i can use raw material transport distance to assign only as far as it will go.
    # finally, i can summ on segment link by commodity type?  (generic commodity???) 
    # notes:
    #     - how are we assigning processor information? (8/23/18 mp - in the user specified
    #     processor_candidate_slate.csv )
    #     - how are we assigning the generic feedstock type? (pending)
    #     - we have the product slate but not the processor information (HEFA, feedstock types, etc...)
    #     - how do we deal with units from pulp. i dont think its tracking units. (8/23/18 mnp - use the native units
    #     stored in the db for that commodity)

    # now iterate through the table we just created to make a list of 
    # updated fields for a bulk update execute many
    logger.debug("interating through the list of feedstock_as_fuel to generate the update_list for the execute many")
    update_list = []
    sql = """ 
             select scenario_rt_id, rt_variant_id, location_id, feedstock_as_fuel_name, feedstock_as_fuel_flow
             from optimal_feedstock_flows
          """
    with sqlite3.connect(the_scenario.main_db) as db_con:
        db_cur = db_con.cursor()
        db_cur.execute(sql)

        # iterate through the db and build the ratios
        for row in db_cur:
            scenario_rt_id = row[0]
            rt_variant_id = row[1]
            location_id = row[2]
            feedstock_as_fuel_name = row[3]
            feedstock_as_fuel_flow = row[4]

            # now look up what feedstock came from that rmp_as_proc
            feedstock_name = fuel_ratios[location_id][0][0]
            feedstock_quant_and_units = fuel_ratios[location_id][0][1]
            max_transport_distance = fuel_ratios[location_id][0][2]

            # iterate through the feedstock_as_fuels to scale the right records
            for feedstock_as_fuel in fuel_ratios[location_id][1]:
                ratio_feedstock_as_fuel_name = feedstock_as_fuel[0]
                feedstock_as_fuel_quant_and_units = feedstock_as_fuel[1]

                if feedstock_as_fuel_name == ratio_feedstock_as_fuel_name:
                    # this is the commodity we want to scale:
                    feedstock_fuel_quant = float(feedstock_as_fuel_flow) * (
                            feedstock_quant_and_units / feedstock_as_fuel_quant_and_units).magnitude
                    update_list.append(
                        [str(feedstock_name), str(feedstock_fuel_quant), float(max_transport_distance), scenario_rt_id,
                         rt_variant_id, location_id, float(max_transport_distance)])

    logger.debug("starting the execute many to update the list of feedstocks and scaled quantities from the RMPs")
    with sqlite3.connect(the_scenario.main_db) as db_con:
        update_sql = """ 
            UPDATE optimal_feedstock_flows 
            set commodity_name = ?, commodity_flow = ?, max_transport_distance = ?
            where scenario_rt_id = ? and rt_variant_id = ? and location_id = ? and cumm_dist <= ?
           ;"""
        db_con.executemany(update_sql, update_list)
        db_con.commit()

        # mnp - 8/14/18
        # intermediate check in on the candidate facilities sql code
        # now we have the original feedstock flows on each of the links
        # so, at this point we can sum up the flow on the link by commodity
        # then we can group the commodities by whatever group/sub-group we want
        # and generate a candidate there 
        # 
        sql = """drop table if exists candidates_aggregated_feedstock_flows;"""
        db_con.execute(sql)

        sql = """create table candidates_aggregated_feedstock_flows as  
                select 
                    ors.network_source_id as network_source_id, 
                    ors.network_source_oid as network_source_oid, 
                    commodity_name, 
                    sum(commodity_flow) as cumm_comm_flow
                from optimal_feedstock_flows ors
                where ors.commodity_flow > 0
                group by ors.network_source_id, ors.network_source_oid, commodity_name; """
        db_con.execute(sql)

    #
    # MNP - 8/14/18 
    # NOW WE PROCESS THE FEEDSTOCKS 
    # AND WRITE THE CSV FILE AT THE SAME TIME
    # - USE MIN AND MAX FACILITY SIZES (feedstock based, not product based)
    # -- get this from the candidate csv file stored in the commodities table. two commodities: minsize and maxsize
    # -- can define the processor type using the name (e.g. candidate_HEFA, candidate_FT_BTL, candidate_Soup_Factory,
    # etc)
    # -- mnp 8/23/18 - updated the facility_name query search on 'candidate%'

    # get the commodity name and quantity at each link that has  flow
    # >= minsize, and <= maxsize for that commodity
    # store them in a dictionary keyed off the source_id 
    # need to match that to a product slate and scale the facility size
    logger.info("opening a csv file")
    with open(the_scenario.processor_candidates_commodity_data, 'w') as wf:

        # write the header line
        header_line = "facility_name,facility_type,commodity,value,units,phase_of_matter,io"
        wf.write(str(header_line + "\n"))

        candidate_location_oids_dict = {}
        with sqlite3.connect(the_scenario.main_db) as db_con:
            sql = """ 
                -- this query pulls the aggregated flows by commodity within 
                -- it joins in candidate processor information for that commodity
                -- as specified in the facility_commodities table such as:
                -- min_size, max_size, and process_type
                -- note: that by convention the facility_name specified by the user 
                -- in the processor_candidate_slate.csv file is used to define the 
                -- process type. e.g. "candidate_hefa"
                
                select
                	caff.network_source_id, 
                	caff.network_source_oid, 
                	c.commodity_id, 
                	caff.commodity_name, 
                	caff.cumm_comm_flow, 
                	c.units,
                 c.phase_of_matter,
                	f.facility_name process_type,
                	maxsize.quantity as max_size,
                	minsize.quantity as min_size
                from candidates_aggregated_feedstock_flows caff
                join commodities c on c.commodity_name = caff.commodity_name
                join facility_commodities maxsize on maxsize.commodity_id =  (select c2.commodity_id from commodities 
                c2 where c2.commodity_name = "maxsize")
                join facility_commodities minsize on minsize.commodity_id = (select c3.commodity_id from commodities 
                c3 where c3.commodity_name = "minsize")
                join facilities f on f.facility_id = minsize.facility_id
                where caff.cumm_comm_flow < max_size and caff.cumm_comm_flow > min_size
                ;"""

            db_cur = db_con.execute(sql)

            # key the candidate_location_oids_dict off the source_name, and the source_oid
            # and append a list of feedstocks and flows for that link
            i = 0  # counter to give the facilities unique names in the csv file
            for row in db_cur:
                i += 1  # increment the candidate generation index
                network_source = row[0]
                net_source_oid = row[1]
                commodity_name = row[3]
                commodity_flow = row[4]
                commodity_units = row[5]
                phase_of_matter = row[6]
                processor_type = row[7]

                commodity_quantity_with_units = Q_(commodity_flow, commodity_units)

                ## WRITE THE CSV FILE OF THE PROCESSOR CANDIDATES PRODUCT SLATE

                # write the inputs from the crop object
                facility_name = "{}_{}_{}".format(processor_type, commodity_name, i)
                facility_type = "processor"
                # phase of matter
                io = "i"
                logger.info("writing input commodity: {} and demand: {} \t {}".format(facility_name, commodity_flow,
                                                                                      commodity_units))
                wf.write("{},{},{},{},{},{},{}\n".format(facility_name, facility_type, commodity_name, commodity_flow,
                                                         commodity_units, phase_of_matter, io))

                # write the outputs from the fuel_dict
                # first get a dictionary of output commodities based on the amount of feedstock available. 
                # if the user specifies a processor_candidate_slate_data file 
                # use that to convert feedstock flows to scaled processor outputs 
                # note: the processors_candidate_slate_data attribute is user specified slates for generic conversion.
                # whereas the processors_commodity_data is FTOT generated for specific candidates
                # the output dict returned from make_rmp_as_proc_slate is of the following form:
                #  output_dict[an_output_commodity] = Q_(quantity, units)
                if the_scenario.processors_candidate_slate_data != 'None':
                    output_dict = ftot_supporting.make_rmp_as_proc_slate(the_scenario, commodity_name,
                                                                         commodity_quantity_with_units, logger)
                # otherwise try AFPAT
                else:
                    # estimate max fuel from that commodity
                    max_conversion_process = ""
                    # values and gets the lowest kg/bbl of total fuel conversion rate (== max biomass -> fuel
                    # efficiency)
                    max_conversion_process = \
                        ftot_supporting.get_max_fuel_conversion_process_for_commodity(commodity_name, the_scenario,
                                                                                      logger)[
                            0]

                    # fuel_dict = get_max_fuel_quantity_from_feedstock(commodity_name, crop_quantity_with_units,
                    # max_conversion_process, the_scenario, logger)
                    output_dict = get_processor_slate(commodity_name, commodity_quantity_with_units,
                                                      max_conversion_process, the_scenario, logger)

                # write the outputs from the fuel_dict.
                for ouput_commodity, values in output_dict.iteritems():
                    logger.info("processing commodity: {}  quantity: {}".format(ouput_commodity, values[0]))
                    # facility_name = facility_name_with_crop # mnp 8-23-18 doesnt change from above
                    # facility_type = "processor" # mnp 8-23-18 doesn't change from above
                    commodity = ouput_commodity
                    quantity = values[0]
                    value = quantity.magnitude
                    units = quantity.units
                    phase_of_matter = values[1]
                    io = "o"

                    # csv writer.write(row)
                    logger.info("writing outputs for: {} and commodity: {} \t {}".format(facility_name, value, units))
                    wf.write("{},{},{},{},{},{},{}\n".format(facility_name, facility_type, commodity, value, units,
                                                             phase_of_matter, io))

                # add the mode to the dictionary and initialize as empty dict
                if network_source not in candidate_location_oids_dict.keys():
                    candidate_location_oids_dict[network_source] = {}

                # add the network link to the dict and initialize as empty list            
                if not net_source_oid in candidate_location_oids_dict[network_source].keys():
                    candidate_location_oids_dict[network_source][net_source_oid] = []

                # prepare the candidate_location_oids_dict with all the information
                # to "set" the candidate processors in the GIS. 
                # 8 /23/18 - mnp - note: its unclear, what if any, this labeling is handling downstream.
                # is it pulp?
                candidate_location_oids_dict[network_source][net_source_oid].append([ \
                    facility_name,  # need this to match the CSV we just wrote
                    0,  # todo mnp 8-23-18 candidates by convention are not prefunded.
                    commodity_flow,  # this is sort of a useless field because it has no units
                    facility_name.replace("candidate_", ""),  # remove the prefix
                    "",  # no secondary processing for now
                    "",  # no tertiary processing for now
                    "",  # feedstock_type
                    "",  # source_category
                    commodity_name  # feedstock_source; input commodity
                    ])

    # todo mnp 8/15/18 -- this code now creates the prototype processor.
    # we need to assign a feedstock and fuel ratio 
    # start digging into the IDW code and bring over whats necessary.
    # assume are going to use the feedstock flow as the commodity
    # then query he RMP_as_prod_slate method in ftot_facilities
    # to determine the output commodities. use this to create the candidate csv file. 
    #          
    # TODO MNP - 8/16/18
    # BRAIN STORM 4pm
    # - generate the specific candidates here. theres no real sense doing it later.
    # - get the processor output product slate from the rmp_as_proc_product_slate ethod in ftot_facilities.py
    # -- this takes a given input feedstock commodity and quantity and returns a prod_dict of outputs.
    # -- use this to write the csv file. 
    # 
    # 08/23/2018 -- MNP NOTES AFTER IDW REVIEW
    # - the IDW code is pretty duplicative of the work we've alreacy done
    #   to get the base feedstock, check raw material distance, and facility size,
    # - the only thing left to determine is the process and write the csv file.
    # - we have everything we need to determine the process, and everything we 
    #   need to write the CSV file. so I'm just going to do that here. 

    # MAKE THE FIRST PROCESSOR POINT LAYER
    # this layer consists of candidates located at segments that have enough cummmulative flow
    # within the raw material distance to meet the minimum processor facility size.
    # ---------------------------------------------------------------------------------------------

    logger.info(
        "create a feature class with all the candidate processor locations: all_candidate_processors_at_segments")
    scenario_gdb = the_scenario.main_gdb
    all_candidate_processors_fc = os.path.join(scenario_gdb, "all_candidate_processors")

    if arcpy.Exists(all_candidate_processors_fc):
        arcpy.Delete_management(all_candidate_processors_fc)
        logger.debug("deleted existing {} layer".format(all_candidate_processors_fc))

    arcpy.CreateFeatureclass_management(scenario_gdb, "all_candidate_processors", "POINT", "#", "DISABLED", "DISABLED",
                                        ftot_supporting_gis.LCC_PROJ)

    # add fields and set capacity and prefunded fields.
    # ---------------------------------------------------------------------
    arcpy.AddField_management(all_candidate_processors_fc, "facility_name", "TEXT")
    arcpy.AddField_management(all_candidate_processors_fc, "Prefunded", "SHORT")
    arcpy.AddField_management(all_candidate_processors_fc, "Capacity", "DOUBLE")
    arcpy.AddField_management(all_candidate_processors_fc, "Primary_Processing_Type", "TEXT")
    arcpy.AddField_management(all_candidate_processors_fc, "Secondary_Processing_Type", "TEXT")
    arcpy.AddField_management(all_candidate_processors_fc, "Tertiary_Processing_Type", "TEXT")
    arcpy.AddField_management(all_candidate_processors_fc, "Feedstock_Type", "TEXT")
    arcpy.AddField_management(all_candidate_processors_fc, "Source_Category", "TEXT")
    arcpy.AddField_management(all_candidate_processors_fc, "Feedstock_Source", "TEXT")
    fields = ("SHAPE@X", "SHAPE@Y", "facility_name", "Prefunded", "Capacity", "Primary_Processing_Type",
              "Secondary_Processing_Type", "Tertiary_Processing_Type", "Feedstock_Type", "Source_Category",
              "Feedstock_Source")
    icursor = arcpy.da.InsertCursor(all_candidate_processors_fc, fields)

    main_scenario_gdb = the_scenario.main_gdb

    for source_name in candidate_location_oids_dict:
        with arcpy.da.SearchCursor(os.path.join(main_scenario_gdb, source_name), ["OBJECTID", "SHAPE@"]) as cursor:

            for row in cursor:

                if row[0] in candidate_location_oids_dict[source_name]:
                    # might need to giggle the locations here? 
                    for candidate in candidate_location_oids_dict[source_name][row[0]]:
                        facility_name = candidate[0]
                        prefunded = candidate[1]
                        commodity_flow = candidate[2]
                        facility_type = candidate[3]
                        sec_proc_type = candidate[4]
                        tert_proc_type = candidate[5]
                        feedstock_type = candidate[6]
                        source_category = candidate[7]
                        commodity_name = candidate[8]
                        # off-set the candidates from each other
                        point = row[1].firstPoint
                        shape_x = point.X
                        shape_y = point.Y
                        offset_x = random.randrange(100, 250, 25)
                        offset_y = random.randrange(100, 250, 25)
                        shape_x += offset_x
                        shape_y += offset_y

                        icursor.insertRow([ \
                            shape_x,
                            shape_y,
                            facility_name,
                            prefunded,
                            commodity_flow,
                            facility_type,
                            sec_proc_type,
                            tert_proc_type,
                            feedstock_type,
                            source_category,
                            commodity_name \
                            ])

    del icursor

    #

    return


# -----------------------------------------------------------------------------
# todo mnp  - 8-3-18 -- deprecate if the above work using hte main.db and pulp optimization
# using networkx edges.
# def generate_bulk_processor_candidates(the_scenario, logger):
#
#    logger.info("starting generate_bulk_processor_candidates")
#
#    logger.info("Clean up the join_segments table in the route cache.")
##    logger.info("Get optimal routes and flows for all time-periods")
#    routes_cache_db = the_scenario.routes_cache
#    sql = "DROP TABLE IF EXISTS joined_segments;"
#    with sqlite3.connect(routes_cache_db) as db_con:
#        db_cur = db_con.cursor()
#        db_cur.execute(sql)
#
#    # create an intermediate table with the optimal route_segments
#    sql = "CREATE TABLE joined_segments( cache_route_id INT, source_name INT, source_oid INT, from_jct_id INT,
#    from_pos INT, shape_length REAL)"
#    # todo 7/4/2017 -- do we need to convert route_id to cache_rt_id?   
#    with sqlite3.connect(routes_cache_db) as db_con:
#        db_cur = db_con.cursor()
#        db_cur.execute(sql) 
#
##    logger.info("Flattening the optimal_route_flows results by commodity and time period.")
#
#    #  initialize the flattened route dictionary
#    #  and the optimal segments dictionary
#    flat_optimized_route_dict = {}
#    cummulative_flow_on_segment_dict = {}  # key = source_name, source_oid?
#    optimal_route_segments_data_dict = {}
#
#    # Parse the Problem for the Optimal Solution
#    # returns the following structure: 
#    # [optimal_processors, optimal_route_flows, optimal_unmet_demand, optimal_storage_flows, optimal_excess_material]
#    optimal_route_flows  = parse_optimal_solution_db(the_scenario, logger)[1]
#
#
#    # go through each optimal route and sum up the total amount of material
#    # flowing on that route for all commodities and time periods.
#    for route_id in optimal_route_flows:
#
#        # iterate through all the time period and commodities for each route_id
#        for i in range(len(optimal_route_flows[route_id])):
#
#            #[od_pair_name, time_period, commodity_flowed, v.varValue]
#
#            od_pair_name = optimal_route_flows[route_id][i][0]
#            o_name = od_pair_name[0]
#            d_name = od_pair_name[1]
#            od_pair_name_string = o_name + "_" + d_name
#
#            commodity_quantity = optimal_route_flows[route_id][i][3]
#
#            time_period = "all_time_periods"
#            commodity_name = "all_commodities"
#            route_type = "feedstock_as_fuel"
#
#            # the dictionary values are added for each time time period.
#            data = [route_id, od_pair_name_string, o_name, d_name, route_type, time_period,  commodity_name,
#            commodity_quantity]
#                    ## 0 ##       #####  1 ####     # 2 #   # 3 #   ## 4 ##     ### 5 ###       ###6###          ###
#                    7 ####
#
#            # if the flattened dictionary doesn't include this route_id, commodity combination
#            # add it.
#            if not route_id in flat_optimized_route_dict:
#
#                 flat_optimized_route_dict[route_id] = {}
#
#            if not commodity_name in flat_optimized_route_dict[route_id]:
#                 flat_optimized_route_dict[route_id] = data
#
#            # add additional commodity quantities for othe time periods.
#            flat_optimized_route_dict[route_id][7] += commodity_quantity
#
#        
#        # -------------------------------------------------------------
#        # query sql for all route segments with the optimal route id
#        # notes: 
#        # - from_jct_id is used to sort the list of segments in order.
#        # - segments table has shape_length, so join that to the route_segments
#
#        # gets the segments for the route we're working on
#        sql = """insert into joined_segments
#                    select 
#                        rs.cache_route_id, 
#                        rs.source_mode_id, 
#                        rs.source_oid, 
#                        rs.junction_id, 
#                        rs.from_position, 
#                        segments.shape_length 
#                            from (route_segments) rs
#                        left join segments on segments.source_mode_id = rs.source_mode_id and segments.source_oid =
#                        rs.source_oid
#                        where rs.cache_route_id =""" + str(route_id) + """ ;"""
#
#        routes_cache_db = the_scenario.routes_cache
#        with sqlite3.connect(routes_cache_db) as db_con:
#            db_cur = db_con.cursor()
#            db_cur.execute(sql)
#
#        sql = """
#        select cache_route_id, from_jct_id, shape_length, source_name, source_oid, from_pos, running_total
#        from (
#            select a.cache_route_id, a.from_jct_id, a.shape_length, a.source_name, a.source_oid, a.from_pos,
#            sum(b.shape_length) as running_total
#            from (
#                select cache_route_id, from_jct_id, shape_length, source_name, source_oid, from_pos
#                from joined_segments where cache_route_id = """ + str(route_id) + """
#            ) as a
#            LEFT JOIN (
#                select cache_route_id, from_jct_id, shape_length, source_name, source_oid, from_pos
#                from joined_segments where cache_route_id = """ + str(route_id) + """
#            ) as b
#                ON (a.cache_route_id = b.cache_route_id  AND a.from_jct_id >= b.from_jct_id)
#            group by a.cache_route_id, a.from_jct_id, a.shape_length, a.source_name, a.source_oid, a.from_pos
#            order by a.cache_route_id, a.from_jct_id, a.shape_length, a.source_name, a.source_oid, a.from_pos
#        ) x
#        where running_total < """ + str(Q_(250,'miles').to('meters').magnitude) + ";"
#        # todo mnp 1-12-18 - I think we should remove this restriction or set it by the user. or at the very leastr 
#        # todo - mnp - 09/05/2017 clean up  of global raw material distance
#        # where running_total < """ + str(int(the_scenario.maxPreprocBiorefDist) *
#        ftot_supporting_gis.METERS_IN_A_MILE) + ";"
#
#        routes_cache_db = the_scenario.routes_cache
#
#        with sqlite3.connect(routes_cache_db) as db_con:
#
#            # step 2- get the cursor
#            db_cur = db_con.cursor()
#
#            # step 3  - execute the SELECT sql statement
#            #logger.info("Get the segments for this Route from the database")
#            db_cur.execute(sql)
#
#            # step 4 - iterate through the rows that the query returned
#            # and add the segments to the optimal_segments dictionary
#            for row in db_cur:
#
#               # logger.info("save the route segment info, and add commodity and commodity flow into a dictionary")
#
#                cache_route_id, from_jct_id, shape_length, source_name, source_oid, from_pos, running_total = row
#
#                if not source_name in cummulative_flow_on_segment_dict:
#
#                    cummulative_flow_on_segment_dict[source_name] = {}
#
#                if not source_oid in cummulative_flow_on_segment_dict[source_name]:
#
#                    cummulative_flow_on_segment_dict[source_name][source_oid] = 0 # cum. flow
#
#                cummulative_flow_on_segment_dict[source_name][source_oid]+= flat_optimized_route_dict[route_id][7]
#
#                if not route_id in optimal_route_segments_data_dict:
#                    optimal_route_segments_data_dict[route_id] = []
#
#                optimal_route_segments_data_dict[route_id].append((from_jct_id,  source_name, source_oid,
#                from_pos)) # this list are the sorted segments for that route in order
#
#    
#
#    
#
#    # figure out which segments have enough flow to justify a candidate
#    # ------------------------------------------------------------------
#    logger.info("figure out which route segments to grab to locate candidate processor there. ")
#    candidate_location_oids_dict = {} # these are the segments that will have candidates located at them
#
#    for route_id in optimal_route_segments_data_dict:
#
#        last_total_flow_value = 0
#
#        for route_segment_tuple in optimal_route_segments_data_dict[route_id]:
#
#            from_jct_id,  source_name, source_oid, from_pos =  route_segment_tuple
#
#            cummulative_commodity_flow = cummulative_flow_on_segment_dict[source_name][source_oid]
#
#            if cummulative_commodity_flow >= float(the_scenario.min_bioref_capacity_kgal)/8:
#
#                if cummulative_commodity_flow > last_total_flow_value:
#                    #  if the segment has enough flow to generate a processor and more flow than the last segment,
#                    add a candidate here.
#                    last_total_flow_value = cummulative_commodity_flow
#
#                    # add this segment to the list
#                    if not source_name in candidate_location_oids_dict:
#                        candidate_location_oids_dict[source_name] = []
#
#                    if not source_oid in candidate_location_oids_dict[source_name]:
#                        candidate_location_oids_dict[source_name].append(source_oid)
#
#
#    # MAKE THE FIRST PROCESSOR POINT LAYER
#    # this layer consists of candidates located at segments that have enough cummmulative flow
#    # within the raw material distance to meet the minimum processor facility size.
#    # ---------------------------------------------------------------------------------------------
#
#    logger.info("create a feature class with all the candidate processor locations:
#    all_candidate_processors_at_segments")
#    scenario_gdb =  the_scenario.main_gdb
#    all_candidate_processors_fc = os.path.join(scenario_gdb, "all_candidate_processors")
#
#
#    if arcpy.Exists(all_candidate_processors_fc):
#        arcpy.Delete_management(all_candidate_processors_fc)
#        logger.debug("deleted existing {} layer".format(all_candidate_processors_fc))
#
#    arcpy.CreateFeatureclass_management(scenario_gdb, "all_candidate_processors", "POINT", "#", "DISABLED",
#    "DISABLED", ftot_supporting_gis.LCC_PROJ)
#
#    icursor = arcpy.da.InsertCursor(all_candidate_processors_fc, ("SHAPE@"))
#
#    main_scenario_gdb = the_scenario.main_gdb
#
#    for source_mode_id in candidate_location_oids_dict:
#        source_name = ftot_routing.get_source_mode_name(source_mode_id)
#        with arcpy.da.SearchCursor(os.path.join(main_scenario_gdb, source_name),  ["OBJECTID", "SHAPE@"]) as cursor:
#
#            for row in cursor:
#
#                if row[0] in candidate_location_oids_dict[source_mode_id]:
#
#                        icursor.insertRow([row[1].firstPoint])
#
#    del icursor
#
#    # add fields and set capacity and prefunded fields.
#    # ---------------------------------------------------------------------
#    arcpy.AddField_management(all_candidate_processors_fc, "facility_name", "TEXT")
#    arcpy.AddField_management(all_candidate_processors_fc, "Prefunded", "SHORT")
#    arcpy.AddField_management(all_candidate_processors_fc, "Capacity", "DOUBLE")
#    arcpy.AddField_management(all_candidate_processors_fc, "Primary_Processing_Type", "TEXT")
#    arcpy.AddField_management(all_candidate_processors_fc, "Secondary_Processing_Type", "TEXT")
#    arcpy.AddField_management(all_candidate_processors_fc, "Tertiary_Processing_Type", "TEXT")
#    arcpy.AddField_management(all_candidate_processors_fc, "Feedstock_Type", "TEXT")
#    arcpy.AddField_management(all_candidate_processors_fc, "Source_Category", "TEXT")
#    arcpy.AddField_management(all_candidate_processors_fc, "Feedstock_Source", "TEXT")
#
#    logger.info("set the capacity and prefunded fields for the all_candidate_processors_fc FC")
#    arcpy.CalculateField_management(all_candidate_processors_fc, "Capacity", "0", "PYTHON_9.3")
#    arcpy.CalculateField_management(all_candidate_processors_fc, "Prefunded", "0", "PYTHON_9.3")
#
#    # Assign state names to the candidates and offset from the network
#    # ---------------------------------------------------------------
#    assign_state_names_and_offset_processors(the_scenario, all_candidate_processors_fc, logger)
#
#    return 
#    
#    
# def assign_feedstock_and_processes(the_scenario, logger):
#    # Inverse Distance Weighting (IDW) Drop lowest scoring candidates
#    # ---------------------------------------------------------------
#    idw_ranking_of_candidates(the_scenario, logger)
#
#    # Create a layer with the processor candidates that are not ignored
#    # ------------------------------------------------------------------
#    logger.info("Create feature class with processors where ignore field != 1")
#
#    candidate_processors_to_merge_fc = os.path.join(the_scenario.main_gdb, "candidate_processors_to_merge")
#
#    if arcpy.Exists(candidate_processors_to_merge_fc):
#        arcpy.Delete_management(candidate_processors_to_merge_fc)
#        logger.debug("deleted existing {} layer".format(candidate_processors_to_merge_fc))
#
#    # first create temporary feature layer from the feature class
#    all_candidate_processors_fc = os.path.join(the_scenario.main_gdb, "all_candidate_processors")
#    arcpy.MakeFeatureLayer_management (all_candidate_processors_fc, "all_candidate_processors_lyr", "ignore is null")
#
#    # select only the processors where ignore fields does not == 1.
#    #arcpy.SelectLayerByAttribute_management("all_candidate_processors_lyr", "NEW_SELECTION", "ignore is null")
#
#    # now copy the features
#    arcpy.CopyFeatures_management("all_candidate_processors_lyr", candidate_processors_to_merge_fc)
#
#    return candidate_processors_to_merge_fc

# ==============================================================================    
def make_flat_locationxy_flow_dict(the_scenario, logger):
    # BIG PICTURE
    # gets the optimal flow for all time periods for each xy by commodity

    logger.info("starting make_processor_candidates_db")

    # Parse the Problem for the Optimal Solution
    # returns the following structure: 
    # [optimal_processors, optimal_route_flows, optimal_unmet_demand, optimal_storage_flows, optimal_excess_material]
    optimal_route_flows = parse_optimal_solution_db(the_scenario, logger)[1]
    optimal_location_id_flow = {}
    optimal_commodity_max_transport_dist_list = {}

    # get location_XYs from location_ids in main.db so we can save in a format thats useful to the route_cache
    # xy_location_id_dict[location_id] = (x,y)
    from ftot_routing import get_xy_location_id_dict
    xy_location_id_dict = get_xy_location_id_dict(the_scenario, logger)

    # flatten the optimal_route_flows by time period
    # -------------------------------------------------
    for route_id, optimal_data_list in optimal_route_flows.iteritems():
        for data in optimal_data_list:
            # break out the data:
            # e.g optimal_route_flows[5633] : ['275, 189', 1, u'2', 63.791322], ['275, 189', 2, u'2', 63.791322]]
            location_id_pair = data[0]
            commodity_id = data[2]
            optimal_flow = data[3]

            #       save in case the xy stuff doesn't work out.
            #        if not location_id_pair in optimal_location_id_flow:
            #            optimal_location_id_flow[location_id_pair] = {}
            #        if not commodity in optimal_location_id_flow[location_id_pair]:
            #            optimal_location_id_flow[location_id_pair][commodity] = optimal_flow
            #        optimal_location_id_flow[location_id_pair][commodity] += optimal_flow

            # keep track of which commodities are optimal
            if not commodity_id in optimal_commodity_max_transport_dist_list:
                optimal_commodity_max_transport_dist_list[commodity_id] = 0

            # convert the location_id to a location_xy
            from_location_xy = xy_location_id_dict[location_id_pair.split(',')[0]]
            to_location_xy = xy_location_id_dict[location_id_pair.split(', ')[1]]  # note the extraspace
            location_xy_pair = "[{},{}]".format(from_location_xy, to_location_xy)

            if not location_xy_pair in optimal_location_id_flow:
                optimal_location_id_flow[location_xy_pair] = {}
            if not commodity_id in location_xy_pair[location_id_pair]:
                optimal_location_id_flow[location_xy_pair][commodity_id] = optimal_flow

            optimal_location_id_flow[location_xy_pair][commodity_id] += optimal_flow


# ===============================================================================
def get_commodity_max_transport_dist_list(the_scenario, logger):
    # need commodities maximum transport distance from the main.db
    # -------------------------------------------------------------

    optimal_commodity_max_transport_dist_list = []
    scenario_db = the_scenario.main_db
    with sqlite3.connect(scenario_db) as db_con:
        logger.info("connected to the db")
        sql = "select commodity_id, commodity_name, max_transportation_distance from commodities"
        db_cur = db_con.execute(sql)

        for row in db_cur:
            commodity_id = row[0]
            commodity_name = row[1]
            max_trans_dist = row[2]

            print "found optimal commodity: {} (id: {}) has max raw trans distance: {}".format(commodity_name,
                                                                                               commodity_id,
                                                                                               max_trans_dist)
            optimal_commodity_max_transport_dist_list[commodity_id] = max_trans_dist

    return optimal_commodity_max_transport_dist_list


# ===============================================================================


# ==============================================================================
def assign_state_names_and_offset_processors(the_scenario, processor_canidates_fc, logger):
    # Assign state names to the processors, e.g., "ND1"
    # ---------------------------------------------------------------
    logger.info("Starting - Assign state names to the processors")

    scenario_gdb = the_scenario.main_gdb
    temp_join_output = os.path.join(scenario_gdb, "temp_state_name_and_candidates_join")

    stateFC = os.path.join(the_scenario.common_data_folder, "base_layers\state.shp")

    if arcpy.Exists(temp_join_output):
        logger.debug("Deleting existing temp_join_output {}".format(temp_join_output))
        arcpy.Delete_management(temp_join_output)

    arcpy.CalculateField_management(processor_canidates_fc, "facility_name", "!OBJECTID!", "PYTHON_9.3")
    arcpy.SpatialJoin_analysis(processor_canidates_fc, stateFC, temp_join_output, "JOIN_ONE_TO_ONE", "KEEP_ALL", "",
                               "WITHIN")
    logger.info("spatial join of all candidates with state FIPS.")

    initial_counter = 1
    processor_state_dict = {}
    state_processor_counter_dict = {}
    logger.info("iterating through the processors in each state and incrementing the name...e.g B:FL1, B:FL2, etc")

    with arcpy.da.SearchCursor(temp_join_output, ["facility_name", "STFIPS"]) as cursor:

        for row in cursor:

            state_fips = row[1]
            state_abb = ftot_supporting_gis.get_state_abb_from_state_fips(state_fips)

            if state_abb not in state_processor_counter_dict:
                state_processor_counter_dict[state_abb] = initial_counter
            else:
                state_processor_counter_dict[state_abb] += 1

            processor_state_dict[row[0]] = str(state_abb).replace(" ", "") + str(
                state_processor_counter_dict[state_abb])

    # the processor_state_dict is all we needed from that temp join FC
    # so now we can delete it.
    arcpy.Delete_management(temp_join_output)
    logger.debug("deleted temp_join_output {} layer".format(temp_join_output))

    # Offset processor candidate locations from the network
    # ---------------------------------------------------------------

    logger.info("Offset processor candidates from the network")

    logger.debug("Iterating through the processor_candidates_fc and updating the names and offsetting the locations.")
    with arcpy.da.UpdateCursor(processor_canidates_fc, ["facility_name", "SHAPE@X", "SHAPE@Y"]) as cursor:

        for row in cursor:

            if row[0] in processor_state_dict:
                row[0] = row[0] + str(processor_state_dict[row[0]])  # mnp - 8/23/18 -- change the b prefix to
                # something else.
                row[1] += 100  # TODO make this offset a constant
                row[2] += 100
                cursor.updateRow(row)

    return


# --------------------------------------------------------------------------
# todo  - mnp - 08/23/18 -- revisiting IDW
## NOTE MOVED TO THE END OF THE GENERATE BULK PROCESSORS METHOD
## PROBABLY DEPRECATED AS A STAND ALONE METHOD FOR THE TIME BEING.
# at this point we have the commodity flows that are 
# within the feedstock specific maximum transport distance
# and the minsize and maxsize of a facility. 
# the next step is to write the candidate CSV file. 
# taking the current IDW and reducing, essentially eliminating the IDW approach. 

# def write_candidate_csv_file(the_scenario, logger):
#
#    logger.info("start write_candidate_csv_file")
#
#    # prepare to write the processor_candidates csv file
#    # ---------------------------------------------------
#    
#    # if the csv file exists, delete it
#    if os.path.exists(the_scenario.processor_candidates_commodity_data):
#        # delete the file the_scenario.processor_candidates_commodity_data
#        logger.debug("deleting processor_candidates_commodity_data csv files")
#        os.remove(the_scenario.processor_candidates_commodity_data)
#        
#    # open csv file for writing 
#    logger.info("opening a csv file")
#    with open(the_scenario.processor_candidates_commodity_data, 'w') as wf:
#        
#        # write the header line
#        header_line = "facility_name,facility_type,commodity,value,units,phase_of_matter,io"
#        wf.write(str(header_line+"\n"))
#        
#        ## WRITE THE CSV FILE OF THE PROCESSOR CANDIDATES PRODUCT SLATE
#                    
#        # write the inputs from the crop object
#        logger.info("processing input commodity: {}  quantity: {}".format(crop, potential_feedstocks_for_processor[
#        crop]))
#        facility_name = facility_name_with_crop
#        facility_type = "processor"
#        commodity = crop
#        value = potential_feedstocks_for_processor[crop].magnitude
#        units = potential_feedstocks_for_processor[crop].units
#        # mnp todo 1-16-18 -- get the phase of matter
#        phase_of_matter = "solid"
#        io = "i"
#        logger.info("writing input commodity: {} and demand: {} \t {}".format(facility_name, value, units))
#        wf.write("{},{},{},{},{},{},{}\n".format(facility_name,facility_type,commodity,value,units,phase_of_matter,io))
#        
#        
#        # write the outputs from the fuel_dict.
#        for fuel, quantity in fuel_dict.iteritems():
#            logger.info("processing commodity: {}  quantity: {}".format(fuel, quantity))
#            facility_name = facility_name_with_crop
#            facility_type = "processor"
#            commodity = fuel
#            value = quantity.magnitude
#            units = quantity.units
#            phase_of_matter = "liquid"
#            io = "o"
#            
#            # csv writer.write(row)
#            logger.info("writing outputs for: {} and commodity: {} \t {}".format(facility_name, value, units))
#            wf.write("{},{},{},{},{},{},{}\n".format(facility_name,facility_type,commodity,value,units,
#            phase_of_matter,io))


## --------------------------------------------------------------------------
## todo  - mnp - 07/17/2017 -- revist this method and break it up. TOO MUCH GOING ON!
## - IDW ranking
## assigns processing type by max fuel production
## estiamtes max fuel production
# def idw_ranking_of_candidates(the_scenario, logger):
#
#    # IDW ranking of candidates; drops lowest scoring candidates
#    logger.info("start IDW ranking of candidates")
#
#    from ftot_facilities import get_max_fuel_quantity_from_feedstock
#
#    crop_county_dict = {}  # crop_county_dict[FIPS] = [Supply, SHAPE@X, SHAPE@Y]
#
#    scenario_gdb = the_scenario.main_gdb
#
#    # first get all the location and production information for each preprocessor
#    import ftot_facilities
#
#    rmp_dict = {}
#    sql = """select f2.facility_name, f.quantity, f.units, f.io, f.commodity_id, c.commodity_name,
#    c.max_transport_distance
#            from facility_commodities f
#            join commodities c on c.commodity_id = f.commodity_id
#            join facilities f2 on f.facility_id = f2.facility_id
#            join facility_type_id fti on fti.facility_type_id = f2.facility_type_id
#            where fti.facility_type = "raw_material_producer" and f2.facility_name not like '%_as_proc'; """
#    
#    with sqlite3.connect(the_scenario.main_db) as db_con:
#    
#        db_cur = db_con.execute(sql)
#        for row in db_cur:
#            if not rmp_dict.has_key(row[0]):
#                rmp_dict[row[0]] = []
#            rmp_dict[row[0]].append([ftot_supporting.CropData(Q_(row[1],row[2]),row[5]),Q_(float(row[6]),"miles")])
#        
#
#    rmp_fc = the_scenario.rmp_fc
#
#    with arcpy.da.SearchCursor(rmp_fc, ["facility_name", "SHAPE@X", "SHAPE@Y"]) as cursor:
#
#       for row in cursor:
#
#            facility_name = row[0]
#
#            if facility_name in rmp_dict:
#
#                list_of_crop_data_objects = rmp_dict[facility_name]
#
#                crop_county_dict[row[0]] = [list_of_crop_data_objects, row[1], row[2]]
#
#    # second, get the locations of all the processor candidates
#    processor_dict = {}
#
#    processor_candidates_fc = os.path.join(scenario_gdb, "all_candidate_processors")
#
#    with arcpy.da.SearchCursor(processor_candidates_fc, ["facility_name", "SHAPE@X", "SHAPE@Y"]) as cursor:
#
#        for row in cursor:
#
#                processor_dict[row[0]] = [row[1], row[2]]
#
#
#    if processor_dict == {}:
#        logger.warning("the scenario has no candidate processor facilities.")
#        logger.warning("TODO: hack for now. this method has two return loops.")
#        return
#
#    logger.info("there are {} processor facilities.".format(len(processor_dict.keys())))
#    
#
#
#    # third, for each processor, check if the raw material producer is within the maximum transport distance,
#    # if it is,  create a dictionary keyed off the crop with values of production divided by the
#    # square of the distance between the preproc and processor.
#
#    # its important to keep a dictionary of all the IDWs so we can reduce the number
#    # we want appropriately.
#
#    list_of_processor_candidate_IDWs = []
#
#    arcpy.AddField_management(processor_candidates_fc, "IDW_Weighting", "DOUBLE")
#
#    # prepare to write the processor_candidates csv file
#    # ---------------------------------------------------
#    
#    # if the csv file exists, delete it
#    if os.path.exists(the_scenario.processor_candidates_commodity_data):
#        # delete the file the_scenario.processor_candidates_commodity_data
#        logger.debug("deleting processor_candidates_commodity_data csv files")
#        os.remove(the_scenario.processor_candidates_commodity_data)
#        
#    # open csv file for writing 
#    logger.info("opening a csv file")
#    with open(the_scenario.processor_candidates_commodity_data, 'w') as wf:
#        
#        # write the header line
#        header_line = "facility_name,facility_type,commodity,value,units,phase_of_matter,io"
#        wf.write(str(header_line+"\n"))
#    
#        # set the processor with the IDW, and feedstock and processing type
#        with arcpy.da.InsertCursor(processor_candidates_fc, ["facility_name", "SHAPE@X", "SHAPE@Y",
#                                                         "CAPACITY", "PREFUNDED", "IDW_Weighting",
#                                                          "Feedstock_Type","Source_Category","Feedstock_Source",
#                                                          "Primary_Processing_Type", "Secondary_Processing_Type",
#                                                          "Tertiary_Processing_Type"]) as insert_cursor:
#             for processor in processor_dict:
#    
#                potential_feedstocks_for_processor = {}
#                potential_IDW_normalized_feedstocks_for_processor = {}
#    
#                for county_neighbor in crop_county_dict:
#    
#                    dist = ftot_supporting.euclidean_distance(processor_dict[processor][0], processor_dict[
#                    processor][1], crop_county_dict[county_neighbor][1], crop_county_dict[county_neighbor][2])
#     
#                    for crop_data_object in crop_county_dict[county_neighbor][0]:
#                        
#                        # note the maximum raw material transport distance
#                       
#
#                        max_dist_meters =  crop_data_object[1].to("meters").magnitude
#                        
#                        if dist < max_dist_meters:
#    
#                            if not crop_data_object[0].crop in potential_feedstocks_for_processor.keys():
#        
#                                potential_IDW_normalized_feedstocks_for_processor[crop_data_object[0].crop] = 0
#                                potential_feedstocks_for_processor[crop_data_object[0].crop] = 0
#        
#                            potential_IDW_normalized_feedstocks_for_processor[crop_data_object[0].crop] += (
#                            crop_data_object[0].production * (1 / (dist * dist)))
#                            potential_feedstocks_for_processor[crop_data_object[0].crop] += crop_data_object[
#                            0].production
#    
#                if not potential_feedstocks_for_processor == {}:
#    
#                    # create a processor candidate for each crop is the potential_feedstock_for_processor dictionary
#    
#                    for crop in potential_feedstocks_for_processor:
#                        logger.info("PROCESSOR: {} CROP: {}".format(processor, crop))
#                        facility_name_with_crop = processor + "_" + crop #"facility_name",  ## TODO MNP - 
#                        shape_x = processor_dict[processor][0] #"SHAPE@X",
#                        shape_y = processor_dict[processor][1] #"SHAPE@Y",
#    
#                        prefunded = 0 #"PREFUNDED",
#                        IDW_weighting = potential_IDW_normalized_feedstocks_for_processor[crop].magnitude
#                        #"IDW_Weighting",
#    
#                        #feedstock_name_in_parts = ftot_supporting.split_feedstock_commidity_name_into_parts(crop,
#                        logger)
#                        feedstock_type = "" #feedstock_name_in_parts[0] # "Feedstock_Type",
#                        source_category = "" #feedstock_name_in_parts[1] # "Source_Category"
#                        feedstock_source = "" #feedstock_name_in_parts[2] #"Feedstock_Source",
#    
#                        
#                        max_conversion_process = ftot_supporting.get_max_fuel_conversion_process_for_commodity(crop,
#                        the_scenario, logger)[0]
#                        primary_processing = max_conversion_process[0] #"Primary_Processing_Type",
#                        secondary_processing = max_conversion_process[1] # "Secondary_Processing_Type",
#                        tertiary_processing = max_conversion_process[2] #"Tertiary_Processing_Type"]
#    
#                        candidate_capacity = 0
#                        max_capacity = 0
#                        theoretical_capacity = 0
#                        
##                        logger.info("{}, {}, {}, {}, {}, {}, {}, {}, {}, {}".format(facility_name_with_crop,
# candidate_capacity, prefunded, IDW_weighting,
# #                                                feedstock_type, source_category, feedstock_source,
#  #                                               primary_processing, secondary_processing, tertiary_processing))
#    
#                        # get the max capacity for a given processing technology
#                        max_capacity = ftot_supporting.get_processor_capacity(primary_processing, logger)
#    
#                        # get the theoretical capacity based on total fuel produced from all the adjacent feedstock
#                        availability
#                        fuel_dict = get_max_fuel_quantity_from_feedstock(crop, potential_feedstocks_for_processor[
#                        crop], max_conversion_process, the_scenario, logger)
#                        
#                        theoretical_capacity = fuel_dict["total_fuel"]
#    
#                        if theoretical_capacity > max_capacity:
#    
#                            logger.warning("Facility: {} - Process: {} - theoretical capacity is {:,.0f}. The max
#                            capacity it is restricted to {}.".format(facility_name_with_crop, primary_processing,
#                            theoretical_capacity, max_capacity))
#                        
#                            candidate_capacity = max_capacity
#                        else:
#                            candidate_capacity = theoretical_capacity
#    
#                        if candidate_capacity >= max_capacity/7: # "big enough" go ahead and create the candidate
#                            #off-set the candidates from each other
#                            offset_x = random.randrange(1, 99)
#                            offset_y = random.randrange(1, 99)
#                            shape_x += offset_x
#                            shape_y += offset_y
#    
#                            insert_cursor.insertRow([facility_name_with_crop, shape_x, shape_y,
#                                                 candidate_capacity.to("kgal").magnitude, prefunded, IDW_weighting,
#                                                 feedstock_type, source_category, feedstock_source,
#                                                 primary_processing, secondary_processing, tertiary_processing])
#    
#                            list_of_processor_candidate_IDWs.append(IDW_weighting)
#                            
#                            
#                            ## WRITE THE CSV FILE OF THE PROCESSOR CANDIDATES PRODUCT SLATE
#                            
#                            # write the inputs from the crop object
#                            logger.info("processing input commodity: {}  quantity: {}".format(crop,
#                            potential_feedstocks_for_processor[crop]))
#                            facility_name = facility_name_with_crop
#                            facility_type = "processor"
#                            commodity = crop
#                            value = potential_feedstocks_for_processor[crop].magnitude
#                            units = potential_feedstocks_for_processor[crop].units
#                            # mnp todo 1-16-18 -- get the phase of matter
#                            phase_of_matter = "solid"
#                            io = "i"
#                            logger.info("writing input commodity: {} and demand: {} \t {}".format(facility_name,
#                            value, units))
#                            wf.write("{},{},{},{},{},{},{}\n".format(facility_name,facility_type,commodity,value,
#                            units,phase_of_matter,io))
#                            
#                            
#                            # write the outputs from the fuel_dict.
#                            for fuel, quantity in fuel_dict.iteritems():
#                                logger.info("processing commodity: {}  quantity: {}".format(fuel, quantity))
#                                facility_name = facility_name_with_crop
#                                facility_type = "processor"
#                                commodity = fuel
#                                value = quantity.magnitude
#                                units = quantity.units
#                                phase_of_matter = "liquid"
#                                io = "o"
#                                
#                                # csv writer.write(row)
#                                logger.info("writing outputs for: {} and commodity: {} \t {}".format(facility_name,
#                                value, units))
#                                wf.write("{},{},{},{},{},{},{}\n".format(facility_name,facility_type,commodity,
#                                value,units,phase_of_matter,io))
#                            
#    
#                        else:
#                            logger.warning("{} is ignored because capacity {:,.0f} is too small (max_capacity/7) {:,
#                            .0f}".format(facility_name_with_crop, candidate_capacity, max_capacity/7))
#
#    list_of_processor_candidate_IDWs.sort()
#    list_of_processor_candidate_IDWs.reverse()
#
#
#    # TODO : we have three variables called MIN IDW. This needs to be cleaned up in a code review.
#    # MIN_IDW_WEIGHTING_TO_BE_CANDIDATE specifies the percentile necessary to be considered.
#    # For example, 0.9 means that the top 90% are kept, and the bottom 10% are droped.
#    MIN_IDW_WEIGHTING_TO_BE_CANDIDATE = 1.0
#    logger.config("MIN_IDW_WEIGHTING_TO_BE_CANDIDATE: \t {}".format(MIN_IDW_WEIGHTING_TO_BE_CANDIDATE))
#
#    if MIN_IDW_WEIGHTING_TO_BE_CANDIDATE == 1:
#        logger.warning("TODO: IDW is shutoff by creating arbitrary small minimum IDW value")
#        min_idw_value = 0.001 # todo: arbitrary low value to skip IDW
#    else:
#        min_idw_value = list_of_processor_candidate_IDWs[int(len(list_of_processor_candidate_IDWs) *
#        MIN_IDW_WEIGHTING_TO_BE_CANDIDATE)]
#
#    logger.result("Minimum IDW Value: \t{0:,.0f}".format(min_idw_value))
#
#    proto_ignored_processors = 0
#    XX_name_ignored_processors = 0
#    idw_ignored_processors = 0
#    candidate_processors = 0
#    prefunded_processors = 0
#
#    IDW_min_count = 75 #  TODO mmp - 7/17/17 - move this to XML
#    logger.info("IDW_min_count set to : {}".format(IDW_min_count))
#
#    arcpy.AddField_management(processor_candidates_fc, "ignore", "SHORT")
#
#    with arcpy.da.UpdateCursor(processor_candidates_fc, ["facility_name", "IDW_Weighting", "Prefunded", "ignore"])
#    as cursor:
#
#        for row in cursor:
#
#            if row[1] == None and row[2] == 0:
#
#                row[3] = 1
#
#                cursor.updateRow(row)
#
#                logger.debug("ignoring candidate processor {} it was not assigned a feedstock or process".format(
#                row[0]))
#
#                proto_ignored_processors += 1
#
#            elif not row[0].find("XX") == -1:
#
#                row[3] = 1
#
#                cursor.updateRow(row)
#
#                logger.debug("ignored candidate processor {} because it was not assigned a state name (
#                B:XX)".format(row[0]))
#
#                XX_name_ignored_processors += 1
#
#            elif row[1] < min_idw_value and row[2] != 1:
#
#                result = arcpy.GetCount_management(processor_candidates_fc)
#
#                count = int(result.getOutput(0))
#
#                if count > IDW_min_count:
#
#                    row[3] = 1
#
#                    cursor.updateRow(row)
#
#                    logger.debug("ignoring processor {} because it was {} less than the min_idw_value".format(row[
#                    0], row[1]))
#
#                    idw_ignored_processors += 1
#
#                else:
#                    logger.debug("IDW_min_count of {} not met. Only {} candidates available. Keeping processor {}
#                    because  {} less than the min_idw_value".format(IDW_min_count, count, row[0], row[1]))
#
#                    candidate_processors += 1  # don't delete and keep track
#
#            elif row[2] == 1:
#                logger.warning("prefunded candidate in the idw_ranking_of_candidates method")
#                prefunded_processors += 1
#
#            else:
#                row[3] = 0
#                logger.debug("Keeping processor {} because it exceeds the min_idw_value".format(row[0]))
#                candidate_processors += 1
#
#    
#    logger.result("Number of prototype processors ignored because not assigned a feedstock or process: \t{}".format(
#    proto_ignored_processors))
#    logger.result("Number of processors ignored because of XX name: \t{}".format(XX_name_ignored_processors))
#    logger.result("Number of processors ignored because of IDW: \t{}".format(idw_ignored_processors))
#    logger.result("Number of candidate processors included: \t{}".format(candidate_processors))
#    #logger.result("Number of prefunded processors included: \t{}".format(prefunded_processors))
#
#    logger.info("end IDW weighting")
#
#

# ========================================================================

def get_processor_fc_summary_statistics(the_scenario, candidates_fc, logger):
    logger.info("starting: get_processor_fc_summary_statistics")
    # set gdb
    scenario_gdb = the_scenario.main_gdb

    # input table
    #    processor_fc = the_scenario.processors_fc
    processor_fc = candidates_fc

    # NEW WORK ON 3/16/18
    # Add the processor candidates types to the database.
    # ---------------------------------------------------------------------------------

    # clean up the table if it exists in the db
    logger.debug("clean up the processor_candidates table if it exists")
    main_db = the_scenario.main_db

    with sqlite3.connect(main_db) as db_con:
        sql = "DROP TABLE IF EXISTS processor_candidates;"
        db_con.execute(sql)

        # create an empty table with the processor candidate information
        sql = """CREATE TABLE processor_candidates(facility_name TEXT, SHAPE_X TEXT, SHAPE_Y TEXT,
              CAPACITY TEXT, PREFUNDED TEXT, IDW_Weighting TEXT,
              Feedstock_Type TEXT, Source_Category TEXT, Feedstock_Source TEXT,
              Primary_Processing_Type TEXT, Secondary_Processing_Type TEXT, Tertiary_Processing_Type TEXT);"""
        db_con.execute(sql)

    query = "ignore IS NULL"  # not interested in facilities that get ignored
    fields = ["facility_name", "SHAPE@X", "SHAPE@Y",
              "CAPACITY", "PREFUNDED", "IDW_Weighting",
              "Feedstock_Type", "Source_Category", "Feedstock_Source",
              "Primary_Processing_Type", "Secondary_Processing_Type", "Tertiary_Processing_Type"]
    with sqlite3.connect(main_db) as db_con:
        with arcpy.da.SearchCursor(processor_fc, fields, where_clause=query) as scursor:
            for row in scursor:
                sql = """insert into processor_candidates
                          (facility_name, SHAPE_X, SHAPE_Y,
                          CAPACITY, PREFUNDED, IDW_Weighting,
                          Feedstock_Type, Source_Category, Feedstock_Source,
                          Primary_Processing_Type, Secondary_Processing_Type, Tertiary_Processing_Type)
                        VALUES ('{}', '{}', '{}', 
                                '{}', '{}', '{}', 
                                '{}', '{}', '{}', 
                                '{}', '{}', '{}')
                                ;""".format(row[0], row[1], row[2],
                                            row[3], row[4], row[5],
                                            row[6], row[7], row[8],
                                            row[9], row[10], row[11])
                db_con.execute(sql)

    ###### OLD PROCESSOR FC SUMMARY STATISTICS ************ ###########

    # Create output table
    candidate_processor_summary_by_processing_type_table = os.path.join(scenario_gdb,
                                                                        "candidate_processor_summary_by_processing_type")
    candidate_processor_summary_by_feedstock_type_table = os.path.join(scenario_gdb,
                                                                       "candidate_processor_summary_by_feedstock_type")

    if arcpy.Exists(candidate_processor_summary_by_processing_type_table):
        arcpy.Delete_management(candidate_processor_summary_by_processing_type_table)
        logger.debug("deleted existing {} layer".format(candidate_processor_summary_by_processing_type_table))

    if arcpy.Exists(candidate_processor_summary_by_feedstock_type_table):
        arcpy.Delete_management(candidate_processor_summary_by_feedstock_type_table)
        logger.debug("deleted existing {} layer".format(candidate_processor_summary_by_feedstock_type_table))

    # Summary Statistics on those fields
    logger.info("starting: Statistics_analysis for candidate_processor_summary_by_processing_type_table")
    arcpy.Statistics_analysis(in_table=processor_fc, out_table=candidate_processor_summary_by_processing_type_table,
                              statistics_fields="ignore SUM; CAPACITY SUM; IDW_Weighting MEAN; Prefunded SUM",
                              case_field="Primary_Processing_Type")

    logger.info("starting: Statistics_analysis for candidate_processor_summary_by_feedstock_type_table")
    arcpy.Statistics_analysis(in_table=processor_fc, out_table=candidate_processor_summary_by_feedstock_type_table,
                              statistics_fields="ignore SUM; CAPACITY SUM; IDW_Weighting MEAN; Prefunded SUM",
                              case_field="Feedstock_Type")

    summary_dict = {}

    for table in ["candidate_processor_summary_by_processing_type", "candidate_processor_summary_by_feedstock_type"]:

        full_path_to_table = os.path.join(scenario_gdb, table)

        with arcpy.da.SearchCursor(full_path_to_table, "*") as search_cursor:  # * accesses all fields in searchCursor

            for row in search_cursor:

                if row[1] is not None:

                    table_short_name = table.replace("candidate_processor_summary_by_", "")

                    if table_short_name not in summary_dict.keys():
                        summary_dict[table_short_name] = {}

                    summary_field = row[1].upper()

                    summary_dict[table_short_name][summary_field] = {}
                    summary_dict[table_short_name][summary_field]["frequency"] = row[2]
                    summary_dict[table_short_name][summary_field]["ignore"] = row[3]

                    summary_dict[table_short_name][summary_field]["Sum_Capacity"] = row[4]
                    summary_dict[table_short_name][summary_field]["Avg_IDW_Weighting"] = row[5]

                    summary_dict[table_short_name][summary_field]["Prefunded"] = row[6]
                    # TODO - mnp - 01062017 - do some math to subtract
                    # summary_dict[table_short_name][summary_field]["Candidates_Kept"] = row[2] - row[3]

    for table_key in sorted(summary_dict.keys()):

        table_dict = summary_dict[table_key]

        for summary_field_key in sorted(table_dict.keys()):

            summary_field_dict = table_dict[summary_field_key]

            for metric_key in sorted(summary_field_dict.keys()):

                metric_sum = summary_field_dict[metric_key]

                if metric_sum is None:

                    metric_sum = 0  # sometimes summary statistics hands back null values which can't be cast as a
                    # float.

                logger.result('{}_{}_{}: \t{:,.1f}'.format(table_key.upper(), summary_field_key.upper(), metric_key,
                                                           float(metric_sum)))

    logger.info("finished: get_processor_fc_summary_statistics")

# =====================================================================================================
# deprecated 1/12/18 mnp
# def calc_optimal_segment_route_info(the_scenario, optimization_result_dict, scenario_gdb, logger):
#
#    logger.info("starting calc_optimal_segment_route_info")
#    optimized_route_segments_fc = os.path.join(scenario_gdb, "optimized_route_segments")
#    # calculates the fafcount, aggrflow, flowdirection, and mode type.
#    # these fields are used for the processors siting code
#    routeList = []
#    amountDict = {}
#
#    # move optimized amounts down to the route segment level.
#    for routeId in optimization_result_dict:
#        routeList.append(routeId)
#        for i in range(len(optimization_result_dict[routeId])):
#            if routeId not in amountDict:
#                amountDict[routeId] = optimization_result_dict[routeId][i][3]
#            else:
#                amountDict[routeId] += optimization_result_dict[routeId][i][3]
#
#    with arcpy.da.UpdateCursor(optimized_route_segments_fc, ["AFTOT_RT_ID"]) as cursor:
#        for row in cursor:
#            if row[0] not in routeList:
#                cursor.deleteRow()
#
#    arcpy.AddField_management(optimized_route_segments_fc, "corrMiles", "DOUBLE")
#    arcpy.AddField_management(optimized_route_segments_fc, "fafCount", "SHORT")
#    arcpy.AddField_management(optimized_route_segments_fc, "flowCount", "DOUBLE")
#    arcpy.AddField_management(optimized_route_segments_fc, "aggrFlow", "DOUBLE")
#    arcpy.AddField_management(optimized_route_segments_fc, "flowDirec", "TEXT")
#    arcpy.AddField_management(optimized_route_segments_fc, "modeType", "TEXT")
#
#    arcpy.CalculateField_management(optimized_route_segments_fc, "corrMiles", "!SHAPE.LENGTH@MILES!", "PYTHON_9.3")
#
#    fafDict = {}
#    flowDict = {}
#
#    flds = ["NET_SOURCE_OID", "fafCount", "flowCount", "AFTOT_RT_ID", "aggrFlow", "flowDirec", "modeType"]
#    with arcpy.da.UpdateCursor(optimized_route_segments_fc, flds) as cursor:
#
#        for row in cursor:
#
#            if row[0] in fafDict:
#                fafDict[row[0]] += 1
#                row[1] = fafDict[row[0]]
#            elif row[0] not in fafDict:
#                fafDict[row[0]] = 1
#                row[1] = 1
#
#            row[2] = amountDict[row[3]]
#
#            if row[0] not in flowDict:
#                flowDict[row[0]] = row[2]
#                row[4] = flowDict[row[0]]
#
#            elif row[0] in flowDict:
#                flowDict[row[0]] += row[2]
#                row[4] = flowDict[row[0]]
#
#            cursor.updateRow(row)


# =============================================================================
# deprecated 1/12/18 mnp
# def make_template_processor_layer(the_scenario, fc_name, logger):
#
#    scenario_gdb = the_scenario.main_gdb
#    template_processor_fc = os.path.join(scenario_gdb, fc_name)
#
#    if arcpy.Exists(template_processor_fc):
#        arcpy.Delete_management(template_processor_fc)
#        logger.debug("deleted existing {} layer".format(template_processor_fc))
#
#
#    # create the fc with the name passed into the method
#    logger.info("Creating featureclass {}".format(fc_name))
#    arcpy.CreateFeatureclass_management(scenario_gdb, fc_name, "POINT", "", "DISABLED", "DISABLED",
#    arcpy.SpatialReference(4326))
#
#    # add all the relevant processor fields
#    logger.info("Adding fields for processor feature class")
#
#    arcpy.AddField_management(template_processor_fc, "facility_name", "TEXT")
#    arcpy.AddField_management(template_processor_fc, "Capacity", "DOUBLE", 20, 3)
#    arcpy.AddField_management(template_processor_fc, "Prefunded", "SHORT")
#    arcpy.AddField_management(template_processor_fc, "Primary_Processing_Type", "TEXT")
#    arcpy.AddField_management(template_processor_fc, "Secondary_Processing_Type", "TEXT")
#    arcpy.AddField_management(template_processor_fc, "Tertiary_Processing_Type", "TEXT")
#    arcpy.AddField_management(template_processor_fc, "Feedstock_Type", "TEXT")
#    arcpy.AddField_management(template_processor_fc, "Source_Category", "TEXT")
#    arcpy.AddField_management(template_processor_fc, "Feedstock_Source", "TEXT")
#
#    return template_processor_fc
