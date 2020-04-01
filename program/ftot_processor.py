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
import pdb
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
                cost_formula_units text,
                min_aggregation numeric
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
            shared_max_transport_distance = 'N'
            commodity_data = ['', commodity_name, commodity_quantity, commodity_unit, commodity_phase, commodity_max_transport_dist, io, shared_max_transport_distance]

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
        min_aggregation = ''
        min_aggregation_units = ''
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
            if 'min_aggregation' == process_property[0]:
                min_aggregation = process_property[1]
                min_aggregation_units = str(process_property[2])

        # do some checks to make sure things aren't weird.
        # -------------------------------------------------
        if max_size_units != min_size_units:
            logger.warning("the units for the max_size and min_size candidate process do not match!")
        if max_size_units != min_aggregation_units:
            logger.warning("the units for the max_size and min_aggregation candidate process do not match!")
        if min_size == '':
            logger.warning("the min_size is set to Null")
        if max_size == '':
            logger.warning("the max_size is set to Null")
        if min_aggregation == '':
            logger.warning("the min_aggregation was not specified by csv and has been set to 1/4 min_size")
            min_aggregation = float(min_size)/4
        if cost_formula == '':
            logger.warning("the cost_formula is set to Null")
        if cost_formula_units == '':
            logger.warning("the cost_formula_units is set to Null")

        # otherwise, build up a list to add to the sql database.
        candidate_process_list_data.append(
            [process_name, min_size, max_size, max_size_units, cost_formula, cost_formula_units, min_aggregation])

    # now do an execute many on the lists for the segments and route_segments table
    with sqlite3.connect(the_scenario.main_db) as db_con:
        sql = "insert into candidate_process_list " \
              "(process_name, minsize, maxsize, min_max_size_units, cost_formula, cost_formula_units, " \
              "min_aggregation) values (?, ?, ?, ?, ?, ?, ?);"
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
                elif commodity_name == 'min_aggregation':
                    candidate_process_list[facility_name].append(["min_aggregation", quantity, units])

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

