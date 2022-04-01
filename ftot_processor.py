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
from ftot_facilities import get_schedule_id
from ftot_pulp import parse_optimal_solution_db
from ftot import Q_
from six import iteritems


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
                min_aggregation numeric,
                schedule_id integer
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
            processor_max_input = commodity[3]
            build_cost = 0
            candidate = 1
            # empty string for facility type and schedule id because fields are not used
            commodity_data = ['', commodity_name, commodity_quantity, commodity_unit, commodity_phase, commodity_max_transport_dist, io, shared_max_transport_distance, candidate, build_cost, processor_max_input, '']

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

    with sqlite3.connect(the_scenario.main_db) as db_con:
        for process in candidate_process_list:
            min_size = ''
            min_size_units = ''
            max_size = ''
            max_size_units = ''
            cost_formula = ''
            cost_formula_units = ''
            min_aggregation = ''
            min_aggregation_units = ''
            schedule_name = ''
            process_name = process

            for process_property in candidate_process_list[process]:
                if 'schedule_name' == process_property[0]:
                    schedule_name = process_property[1]
                    schedule_id = get_schedule_id(the_scenario, db_con, schedule_name, logger)
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

            #   otherwise, build up a list to add to the sql database.
            candidate_process_list_data.append(
                [process_name, min_size, max_size, max_size_units, cost_formula, cost_formula_units, min_aggregation, schedule_id])

        # now do an execute many on the lists for the segments and route_segments table

        sql = "insert into candidate_process_list " \
              "(process_name, minsize, maxsize, min_max_size_units, cost_formula, cost_formula_units, " \
              "min_aggregation, schedule_id) values (?, ?, ?, ?, ?, ?, ?, ?);"
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
        len(list(process_id_name_dict.keys()))))
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

    for facility_name, facility_commodity_list in iteritems(candidate_process_data):
        for row in facility_commodity_list:
            commodity_name = row[1]
            commodity_id = None
            quantity = row[2]
            units = str(row[3])
            phase_of_matter = row[4]
            io = row[6]
            schedule_name = row[-1]

            # store the input and output commodities
            # in the candidate_process_commodities (CPC) list .
            if io == 'i' or io == 'o':
                candidate_process_commodities.append(
                    [facility_name, commodity_name, commodity_id, quantity, units, phase_of_matter, io])

            # store the facility size and cost information in the
            # candidate_process_list (CPL).
            else:
                if facility_name not in list(candidate_process_list.keys()):
                    candidate_process_list[facility_name] = []
                    # add schedule name to the candidate_process_list array for the facility
                    candidate_process_list[facility_name].append(["schedule_name", schedule_name])

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

            if process_name not in list(output_dict.keys()):
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
                    cpl.schedule_id schedule_id,
                    sn.schedule_name schedule_name,
                    cpc.commodity_name commodity_name, 
                    cpc.commodity_id commodity_id,
                    cpl.maxsize quantity, 
                    cpl.min_max_size_units units, 
                    cpc.io io,
                    c.phase_of_matter phase_of_matter
                    from candidate_nodes cn
                    join candidate_process_commodities cpc on cpc.commodity_id = cn.commodity_id and cpc.process_id = cn.process_id
                    join candidate_process_list cpl on cpl.process_id = cn.process_id
                    join networkx_nodes xy on cn.node_id = xy.node_id
                    join commodities c on c.commodity_id = cpc.commodity_id
                    join schedule_names sn on sn.schedule_id = cpl.schedule_id
                    group by xy.shape_x, 
                        xy.shape_y, 
                        facility_name, 
                        cpl.process_id,
                        cpl.schedule_id,
                        sn.schedule_name,
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

    output_dict = get_candidate_processor_slate_output_ratios(the_scenario, logger)

    logger.info("opening a csv file")
    with open(the_scenario.processor_candidates_commodity_data, 'w') as wf:

        # write the header line
        header_line = "facility_name,facility_type,commodity,value,units,phase_of_matter,io,schedule," \
                      "max_processor_input"
        wf.write(str(header_line + "\n"))

        # WRITE THE CSV FILE OF THE PROCESSOR CANDIDATES PRODUCT SLATE

        sql = """ 
            select 
                facility_name, 'processor', commodity_name, quantity, units, phase_of_matter, io, schedule_name, 
                cpl.process_name
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
            schedule_name = row[7]
            process_name = row[8]
            max_processor_input = input_quantity

            wf.write("{},{},{},{},{},{},{},{},{}\n".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6],
                                                           row[7], max_processor_input))

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
                    "{},{},{},{},{},{},{},{},{}\n".format(row[0], row[1], output_commodity_name, output_quantity.magnitude,
                                                       output_quantity.units, output_phase_of_matter, 'o',
                                                       schedule_name, max_processor_input))

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


def generate_bulk_processor_candidates(the_scenario, logger):
    logger.info("starting generate_bulk_processor_candidates")

    candidate_location_oids_dict = {}
    commodities_max_transport_dist_dict = {}

    # the first routing will flow finished fuels.
    # we can back track to the original commodity (or optionally assign a generic feedstock)
    # so that we can aggregate feedstocks for candiate facilities locations.

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
            if not location_id in list(fuel_ratios.keys()):
                fuel_ratios[location_id] = []
                fuel_ratios[location_id].append(0)  # one feedstock
                fuel_ratios[location_id].append([])  # many feedstocks-as-fuel

            if not "_as_proc" in facility_name:
                fuel_ratios[location_id][0] = ([commodity_name, Q_(quantity, units), max_transport_distance])
            else:
                # this is the feedstock-as-fuel value
                fuel_ratios[location_id][1].append([commodity_name, Q_(quantity, units)])

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

                    output_dict = get_processor_slate(commodity_name, commodity_quantity_with_units,
                                                      max_conversion_process, the_scenario, logger)

                # write the outputs from the fuel_dict.
                for ouput_commodity, values in iteritems(output_dict):
                    logger.info("processing commodity: {}  quantity: {}".format(ouput_commodity, values[0]))
                    commodity = ouput_commodity
                    quantity = values[0]
                    value = quantity.magnitude
                    units = quantity.units
                    phase_of_matter = values[1]
                    io = "o"

                    logger.info("writing outputs for: {} and commodity: {} \t {}".format(facility_name, value, units))
                    wf.write("{},{},{},{},{},{},{}\n".format(facility_name, facility_type, commodity, value, units,
                                                             phase_of_matter, io))

                # add the mode to the dictionary and initialize as empty dict
                if network_source not in list(candidate_location_oids_dict.keys()):
                    candidate_location_oids_dict[network_source] = {}

                # add the network link to the dict and initialize as empty list            
                if not net_source_oid in list(candidate_location_oids_dict[network_source].keys()):
                    candidate_location_oids_dict[network_source][net_source_oid] = []

                # prepare the candidate_location_oids_dict with all the information
                # to "set" the candidate processors in the GIS.
                candidate_location_oids_dict[network_source][net_source_oid].append([ \
                    facility_name,  # need this to match the CSV we just wrote
                    0,
                    commodity_flow,
                    facility_name.replace("candidate_", ""),
                    "",  # no secondary processing for now
                    "",  # no tertiary processing for now
                    "",  # feedstock_type
                    "",  # source_category
                    commodity_name  # feedstock_source; input commodity
                    ])
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
# ==============================================================================    
def make_flat_locationxy_flow_dict(the_scenario, logger):
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
    for route_id, optimal_data_list in iteritems(optimal_route_flows):
        for data in optimal_data_list:
            # break out the data:
            # e.g optimal_route_flows[5633] : ['275, 189', 1, u'2', 63.791322], ['275, 189', 2, u'2', 63.791322]]
            location_id_pair = data[0]
            commodity_id = data[2]
            optimal_flow = data[3]

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

            print ("found optimal commodity: {} (id: {}) has max raw trans distance: {}".format(commodity_name,
                                                                                               commodity_id,
                                                                                               max_trans_dist))
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
                row[0] = row[0] + str(processor_state_dict[row[0]])
                row[1] += 100
                row[2] += 100
                cursor.updateRow(row)

    return

# ========================================================================

def get_processor_fc_summary_statistics(the_scenario, candidates_fc, logger):
    logger.info("starting: get_processor_fc_summary_statistics")
    # set gdb
    scenario_gdb = the_scenario.main_gdb

    processor_fc = candidates_fc
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

                    if table_short_name not in list(summary_dict.keys()):
                        summary_dict[table_short_name] = {}

                    summary_field = row[1].upper()

                    summary_dict[table_short_name][summary_field] = {}
                    summary_dict[table_short_name][summary_field]["frequency"] = row[2]
                    summary_dict[table_short_name][summary_field]["ignore"] = row[3]

                    summary_dict[table_short_name][summary_field]["Sum_Capacity"] = row[4]
                    summary_dict[table_short_name][summary_field]["Avg_IDW_Weighting"] = row[5]

                    summary_dict[table_short_name][summary_field]["Prefunded"] = row[6]

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