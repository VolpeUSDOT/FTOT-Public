# ---------------------------------------------------------------------------------------------------
# Name: ftot_postprocess.py
#
# Purpose: Post processes the optimal results, generates optimal segments FC and
# optimal_scenario_results table in the database
#
# ---------------------------------------------------------------------------------------------------

import ftot_supporting_gis
import arcpy
import sqlite3
import os
from collections import defaultdict
from ftot import Q_


# =========================================================================


def route_post_optimization_db(the_scenario, logger):

    logger.info("starting route_post_optimization_db")

    # Parse the Optimal Solution from the DB
    # ---------------------------------------
    from ftot_pulp import parse_optimal_solution_db
    parsed_optimal_solution = parse_optimal_solution_db(the_scenario, logger)
    optimal_processors, optimal_route_flows, optimal_unmet_demand, optimal_storage_flows, optimal_excess_material = parsed_optimal_solution

    from ftot_networkx import update_ndr_parameter

    # Check NDR conditions before post-processing results
    update_ndr_parameter(the_scenario, logger)

    if not the_scenario.ndrOn:
        # Make the optimal routes and route_segments FCs from the db
        # ------------------------------------------------------------
        make_optimal_route_segments_db(the_scenario, logger)
    elif the_scenario.ndrOn:
        # Make the optimal routes and route_segments FCs from the db
        # ------------------------------------------------------------
        make_optimal_route_segments_from_routes_db(the_scenario, logger)

    # Make the optimal routes and route segments FCs
    # -----------------------------------------------
    # One record for each link in the optimal flow by commodity
    make_optimal_route_segments_featureclass_from_db(the_scenario, logger)

    # Add functional class and urban code to the road segments
    add_link_type_and_urban_rural(the_scenario, logger)

    dissolve_optimal_route_segments_feature_class_for_mapping(the_scenario, logger)

    make_optimal_scenario_results_db(the_scenario, logger)

    # Report the utilization by commodity
    # ------------------------------------
    db_report_commodity_utilization(the_scenario, logger)

    # Generate the scenario summary
    # ------------------------------
    generate_scenario_summary(the_scenario, logger)

    # Calculate detailed emissions
    # ------------------------------
    if the_scenario.detailed_emissions_data != 'None':
        detailed_emissions_setup(the_scenario, logger)

    # Make the optimal intermodal facilities
    # ----------------------------
    if not the_scenario.ndrOn:
         make_optimal_intermodal_db(the_scenario, logger)
    elif the_scenario.ndrOn:
         make_optimal_intermodal_from_routes_db(the_scenario, logger)

    make_optimal_intermodal_featureclass(the_scenario, logger)

    # Make the optimal facilities
    # ----------------------------
    make_optimal_facilities_db(the_scenario, logger)

    # Now that we have a table with the optimal facilities, we can go through each FC and see if the name
    # matches an "optimal" facility_name, and set the flag.

    # -- RMP fc and reporting
    make_optimal_raw_material_producer_featureclass(the_scenario, logger)

    # -- Processors fc and reporting
    make_optimal_processors_featureclass(the_scenario, logger)

    # -- Ultimate Destinations fc and reporting
    make_optimal_destinations_featureclass(the_scenario, logger)


# ======================================================================================================================


def make_optimal_facilities_db(the_scenario, logger):
    logger.info("starting make_optimal_facilities_db")

    # use the optimal solution and edges tables in the db to reconstruct what facilities are used
    with sqlite3.connect(the_scenario.main_db) as db_con:

        # drop the table
        sql = "drop table if exists optimal_facilities"
        db_con.execute(sql)

        # create the table
        sql = """
            create table optimal_facilities(
            facility_id integer,
            facility_name text,
            facility_type text,
            facility_type_id integer,
            time_period text,
            commodity text
            );"""

        db_con.execute(sql)

        sql = """
            insert into optimal_facilities
            select f.facility_ID, f.facility_name, fti.facility_type, f.facility_type_id, ov.time_period, ov.commodity_name
            from facilities f
            join facility_type_id fti on f.facility_type_id = fti.facility_type_id
            join optimal_variables ov on  ov.o_facility = f.facility_name
            join optimal_variables ov2 on  ov2.d_facility = f.facility_name
            where ov.o_facility is not NULL and ov2.d_facility is not NULL
            ;"""

        db_con.execute(sql)


# ======================================================================================================================


def make_optimal_intermodal_db(the_scenario, logger):
    logger.info("starting make_optimal_intermodal_db")

    # use the optimal solution and edges tables in the db to reconstruct what facilities are used
    with sqlite3.connect(the_scenario.main_db) as db_con:

        # drop the table
        sql = "drop table if exists optimal_intermodal_facilities"
        db_con.execute(sql)

        # create the table
        sql = """
            create table optimal_intermodal_facilities(
            source text,
            source_OID integer
            );"""

        db_con.execute(sql)

        sql = """
            insert into optimal_intermodal_facilities
            select nx_n.source, nx_n.source_OID
            from networkx_nodes nx_n
            join optimal_variables ov on ov.from_node_id = nx_n.node_id
            join optimal_variables ov2 on ov2.to_node_id = nx_n.node_id
            where nx_n.source = 'intermodal'
            group by nx_n.source_OID
            ;"""

        db_con.execute(sql)


# ======================================================================================================================


def make_optimal_intermodal_from_routes_db(the_scenario, logger):
    logger.info("starting make_optimal_intermodal_from_routes_db")

    # use the optimal solution and edges tables in the db to reconstruct what facilities are used
    with sqlite3.connect(the_scenario.main_db) as db_con:

        # drop the table
        sql = "drop table if exists optimal_intermodal_facilities"
        db_con.execute(sql)

        # create the table
        sql = """
            create table optimal_intermodal_facilities(
            source text,
            source_OID integer
            );"""

        db_con.execute(sql)

        sql = """
            insert into optimal_intermodal_facilities
            select nx_n.source, nx_n.source_OID
            from networkx_nodes nx_n
            join optimal_route_segments ors1 on ors1.from_node_id = nx_n.node_id
            join optimal_route_segments ors2 on ors2.to_node_id = nx_n.node_id
            where nx_n.source = 'intermodal'
            group by nx_n.source_OID
            ;"""

        db_con.execute(sql)


# ======================================================================================================================


def make_optimal_intermodal_featureclass(the_scenario, logger):

    logger.info("starting make_optimal_intermodal_featureclass")

    intermodal_fc = os.path.join(the_scenario.main_gdb, "network", "intermodal")

    for field in arcpy.ListFields(intermodal_fc):

        if field.name.lower() =="optimal":
           arcpy.DeleteField_management(intermodal_fc , "optimal")

    arcpy.AddField_management(intermodal_fc, "optimal", "DOUBLE")

    edit = arcpy.da.Editor(the_scenario.main_gdb)
    edit.startEditing(False, False)
    edit.startOperation()

    # get a list of the optimal raw_material_producer facilities
    with sqlite3.connect(the_scenario.main_db) as db_con:
        sql = """select source_oid from optimal_intermodal_facilities;"""
        db_cur = db_con.execute(sql)
        intermodal_db_data = db_cur.fetchall()

    # loop through the GIS feature class and see if any of the facility_names match the list of optimal facilities.
    with arcpy.da.UpdateCursor(intermodal_fc, ["source_OID", "optimal"]) as cursor:
         for row in cursor:
             source_OID = row[0]
             row[1] = 0  #  assume to start, it is not an optimal facility
             for opt_fac in intermodal_db_data:
                 if source_OID in opt_fac: # if the OID matches and optimal facility
                     row[1] = 1 # give it a positive value since we're not keep track of flows.
             cursor.updateRow(row)

    edit.stopOperation()
    edit.stopEditing(True)


# ======================================================================================================================


def make_optimal_raw_material_producer_featureclass(the_scenario, logger):

    logger.info("starting make_optimal_raw_material_producer_featureclass")
    # add rmp flows to rmp fc
    # ----------------------------------------------------

    rmp_fc = the_scenario.rmp_fc

    for field in arcpy.ListFields(rmp_fc ):

        if field.name.lower() == "optimal":
           arcpy.DeleteField_management(rmp_fc, "optimal")

    arcpy.AddField_management(rmp_fc, "optimal", "DOUBLE")

    edit = arcpy.da.Editor(the_scenario.main_gdb)
    edit.startEditing(False, False)
    edit.startOperation()

    # get a list of the optimal raw_material_producer facilities
    with sqlite3.connect(the_scenario.main_db) as db_con:
        sql = """select facility_name from optimal_facilities where facility_type = "raw_material_producer";"""
        db_cur = db_con.execute(sql)
        rmp_db_data = db_cur.fetchall()

    # loop through the GIS feature class and see if any of the facility_names match the list of optimal facilities.
    with arcpy.da.UpdateCursor(rmp_fc, ["facility_name", "optimal"]) as cursor:
        for row in cursor:
            facility_name = row[0]
            row[1] = 0  # assume to start, it is not an optimal facility
            for opt_fac in rmp_db_data:

                if facility_name in opt_fac:  # if the name matches and optimal facility

                    row[1] = 1  # give it a positive value since we're not keep track of flows.
            cursor.updateRow(row)

    edit.stopOperation()
    edit.stopEditing(True)


# ======================================================================================================================


def make_optimal_processors_featureclass(the_scenario, logger):
    logger.info("starting make_optimal_processors_featureclass")

    # query the db and get a list of optimal processors from the optimal_facilities table in the db.
    # iterate through the processors_FC and see if the name of the facility match
    # set the optimal field in the GIS if there is a match

    processor_fc = the_scenario.processors_fc

    for field in arcpy.ListFields(processor_fc):

        if field.name.lower() == "optimal":
            arcpy.DeleteField_management(processor_fc, "optimal")

    arcpy.AddField_management(processor_fc, "optimal", "DOUBLE")

    edit = arcpy.da.Editor(the_scenario.main_gdb)
    edit.startEditing(False, False)
    edit.startOperation()

    # get a list of the optimal raw_material_producer facilities
    with sqlite3.connect(the_scenario.main_db) as db_con:
        sql = """select facility_name from optimal_facilities where facility_type = "processor";"""
        db_cur = db_con.execute(sql)
        opt_fac_db_data = db_cur.fetchall()

    # loop through the GIS feature class and see if any of the facility_names match the list of optimal facilities.
    with arcpy.da.UpdateCursor(processor_fc, ["facility_name", "optimal"]) as cursor:
        for row in cursor:
            facility_name = row[0]
            row[1] = 0  # assume to start, it is not an optimal facility
            for opt_fac in opt_fac_db_data:
                if facility_name in opt_fac: # if the name matches and optimal facility
                    row[1] = 1  # give it a positive value since we're not keep track of flows.
            cursor.updateRow(row)

    edit.stopOperation()
    edit.stopEditing(True)


# ===================================================================================================


def make_optimal_destinations_featureclass(the_scenario, logger):
    logger.info("starting make_optimal_destinations_featureclass")

    # query the db and get a list of optimal processors from the optimal_facilities table in the db.
    # iterate through the processors_FC and see if the name of the facility matches
    # set the optimal field in the GIS if there is a match

    destinations_fc = the_scenario.destinations_fc

    for field in arcpy.ListFields(destinations_fc):

        if field.name.lower() == "optimal":
           arcpy.DeleteField_management(destinations_fc , "optimal")

    arcpy.AddField_management(destinations_fc, "optimal", "DOUBLE")

    edit = arcpy.da.Editor(the_scenario.main_gdb)
    edit.startEditing(False, False)
    edit.startOperation()

    # get a list of the optimal raw_material_producer facilities
    with sqlite3.connect(the_scenario.main_db) as db_con:
        sql = """select facility_name from optimal_facilities where facility_type = "ultimate_destination";"""
        db_cur = db_con.execute(sql)
        opt_fac_db_data = db_cur.fetchall()

    # loop through the GIS feature class and see if any of the facility_names match the list of optimal facilities.
    with arcpy.da.UpdateCursor(destinations_fc, ["facility_name", "optimal"]) as cursor:
        for row in cursor:
            facility_name = row[0]
            row[1] = 0  # assume to start, it is not an optimal facility
            for opt_fac in opt_fac_db_data:
                if facility_name in opt_fac: # if the name matches and optimal facility
                    row[1] = 1 # give it a positive value since we're not keep track of flows.
            cursor.updateRow(row)

    edit.stopOperation()
    edit.stopEditing(True)

    scenario_gdb = the_scenario.main_gdb


# =====================================================================================================================


def make_optimal_route_segments_db(the_scenario, logger):
    # iterate through the db to create a dictionary of dictionaries (DOD)
    # then generate the graph using the method
    # >>> dod = {0: {1: {'weight': 1}}} # single edge (0,1)
    # >>> G = nx.from_dict_of_dicts(dod)
    # I believe this is read as the outer dict is the from_node, the inner dictionary
    # is the to_node, the inner-inner dictionary is a list of attributes.

    logger.info("START: make_optimal_route_segments_db")

    with sqlite3.connect(the_scenario.main_db) as db_con:

        # drop the table
        sql = "drop table if exists optimal_route_segments"
        db_con.execute(sql)

        # create the table
        sql = """
            create table optimal_route_segments(
                                                scenario_rt_id integer, rt_variant_id integer,
                                                network_source_id integer, network_source_oid integer,
                                                from_position integer, from_junction_id integer,
                                                time_period integer,
                                                commodity_name text, commodity_flow real,
                                                volume real, capacity real, capacity_minus_volume,
                                                units text, phase_of_matter text,
                                                length real, route_type text, link_transport_cost real, link_routing_cost real,
                                                artificial integer,
                                                link_type text,
                                                urban_rural integer,
                                                limited_access integer,
                                                from_node_id integer,
                                                to_node_id integer
                                                );"""
        db_con.execute(sql)

    # initialize dod
    optimal_segments_list = []
    with sqlite3.connect(the_scenario.main_db) as db_con:

        db_con.execute("""create index if not exists nx_edge_index_2 on networkx_edges(edge_id);""")
        db_con.execute("""create index if not exists nx_edge_cost_index on networkx_edge_costs(edge_id);""")
        db_con.execute("""create index if not exists ov_index on optimal_variables(nx_edge_id);""")
        db_con.execute("""create index if not exists ov_index_2 on optimal_variables(commodity_name);""")

        sql = """
            select
            ov.mode,
            ov.mode_oid,
            ov.commodity_name,
            ov.variable_value,
            ov.converted_volume,
            ov.converted_capacity,
            ov.converted_capac_minus_volume,
            ov.units,
            ov.time_period,
            ov.length,
            c.phase_of_matter,
            nx_e_cost.transport_cost,
            nx_e_cost.route_cost,
            nx_e.artificial,
            nx_e.from_node_id,
            nx_e.to_node_id
            from optimal_variables ov
            join commodities as c on c.commodity_name = ov.commodity_name
            join networkx_edges as nx_e on nx_e.edge_id = ov.nx_edge_id
            join networkx_edge_costs as nx_e_cost on nx_e_cost.edge_id = ov.nx_edge_id and nx_e_cost.phase_of_matter_id = c.phase_of_matter
            ;"""

        db_cur = db_con.execute(sql)
        logger.info("done with the execute...")

        logger.info("starting the fetch all... possible to increase speed with an index")
        rows = db_cur.fetchall()

        row_count = len(rows)
        logger.info("number of rows in the optimal segments select to process: {}".format(row_count))

        logger.info("starting to iterate through the results and build up the dod")
        for row in rows:
            mode                = row[0]
            mode_oid            = row[1]
            commodity           = row[2]
            opt_flow            = row[3]
            volume              = row[4]
            capacity            = row[5]
            capacity_minus_volume = row[6]
            units               = row[7]
            time_period         = row[8]
            length              = row[9]
            phase_of_matter     = row[10]
            link_transport_cost    = row[11]
            link_routing_cost   = row[12]
            artificial          = row[13]
            from_node_id        = row[14]
            to_node_id          = row[15]

            # format for the optimal route segments table
            optimal_segments_list.append([1,  # rt id
                                          None,  # rt variant id
                                          mode,
                                          mode_oid,
                                          None,  # segment order
                                          None,
                                          time_period,
                                          commodity,
                                          opt_flow,
                                          volume,
                                          capacity,
                                          capacity_minus_volume,
                                          units,
                                          phase_of_matter,
                                          length,
                                          None,  # route type
                                          link_transport_cost,
                                          link_routing_cost,
                                          artificial,
                                          None,  # brought in later
                                          None,  # brought in later
                                          None,  # brought in later
                                          from_node_id,
                                          to_node_id])

    logger.info("done making the optimal_segments_list")
    with sqlite3.connect(the_scenario.main_db) as db_con:
        insert_sql = """
            INSERT into optimal_route_segments
            values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ;"""

        db_con.executemany(insert_sql, optimal_segments_list)
        logger.info("finish optimal_segments_list db_con.executemany()")
        db_con.commit()
        logger.info("finish optimal_segments_list db_con.commit")

    return


# ===================================================================================================


def make_optimal_route_segments_from_routes_db(the_scenario, logger):
    # iterate through the db to create a dictionary of dictionaries (DOD)
    # then generate the graph using the method
    # >>> dod = {0: {1: {'weight': 1}}} # single edge (0,1)
    # >>> G = nx.from_dict_of_dicts(dod)
    # I believe this is read as the outer dict is the from_node, the inner dictionary
    # is the to_node, the inner-inner dictionary is a list of attributes.

    logger.info("START: make_optimal_route_segments_from_routes_db")

    with sqlite3.connect(the_scenario.main_db) as db_con:

        # drop the table
        sql = "drop table if exists optimal_route_segments"
        db_con.execute(sql)

        # create the table
        db_con.execute("""create table optimal_route_segments(
                                                     scenario_rt_id integer, rt_variant_id integer,
                                                     network_source_id integer, network_source_oid integer,
                                                     from_position integer, from_junction_id integer,
                                                     time_period integer,
                                                     commodity_name text, commodity_flow real,
                                                     volume real, capacity real, capacity_minus_volume,
                                                     units text, phase_of_matter text,
                                                     length real, route_type text, link_transport_cost real, link_routing_cost real,
                                                     artificial integer,
                                                     link_type text,
                                                     urban_rural integer, limited_access integer,
                                                     from_node_id integer, to_node_id integer
                                                     );""")

        db_con.execute("""create index if not exists nx_edge_index_2 on networkx_edges(edge_id);""")
        db_con.execute("""create index if not exists nx_edge_cost_index on networkx_edge_costs(edge_id);""")
        db_con.execute("""create index if not exists ov_index on optimal_variables(nx_edge_id);""")
        db_con.execute("""create index if not exists ov_index_2 on optimal_variables(commodity_name);""")

        sql = """
            select
            nx_e.mode_source,
            nx_e.mode_source_oid,
            ov.commodity_name,
            ov.variable_value,
            ov.converted_volume,
            ov.converted_capacity,
            ov.converted_capac_minus_volume,
            ov.units,
            ov.time_period,
            nx_e.length,
            c.phase_of_matter,
            nx_e_cost.transport_cost,
            nx_e_cost.route_cost ,
            nx_e.artificial,
			re.scenario_rt_id,
			nx_e.from_node_id,
			nx_e.to_node_id
            from optimal_variables ov
            join commodities as c on c.commodity_name = ov.commodity_name
            join edges as e on e.edge_id = ov.var_id
            join route_reference as rr on rr.route_id = e.route_id
            join route_edges as re on re.scenario_rt_id = rr.scenario_rt_id
            left join networkx_edges as nx_e on nx_e.edge_id = re.edge_id
            join networkx_edge_costs as nx_e_cost on nx_e_cost.edge_id = re.edge_id and nx_e_cost.phase_of_matter_id = c.phase_of_matter
            ;"""

        db_cur = db_con.execute(sql)
        logger.info("done with the execute...")

        logger.info("starting the fetch all... possible to increase speed with an index")
        rows = db_cur.fetchall()

        row_count = len(rows)
        logger.info("number of rows in the optimal segments select to process: {}".format(row_count))

        # intiialize dod
        optimal_segments_list = []
        logger.info("starting to iterate through the results and build up the dod")
        for row in rows:
            mode                = row[0]
            mode_oid            = row[1]
            commodity           = row[2]
            opt_flow            = row[3]
            volume              = row[4]
            capacity            = row[5]
            capacity_minus_volume = row[6]
            units               = row[7]
            time_period         = row[8]
            length               = row[9]
            phase_of_matter     = row[10]
            link_transport_cost    = row[11]
            link_routing_cost   = row[12]
            artificial          = row[13]
            scenario_rt_id      = row[14]
            from_node_id        = row[15]
            to_node_id          = row[16]

            # format for the optimal route segments table
            optimal_segments_list.append([scenario_rt_id,  # rt id
                                          None,  # rt variant id
                                          mode,
                                          mode_oid,
                                          None,  # segment order
                                          None,
                                          time_period,
                                          commodity,
                                          opt_flow,
                                          volume,
                                          capacity,
                                          capacity_minus_volume,
                                          units,
                                          phase_of_matter,
                                          length,
                                          None,  # route type
                                          link_transport_cost,
                                          link_routing_cost,
                                          artificial,
                                          None,
                                          None,
                                          None,
                                          from_node_id,
                                          to_node_id])

    logger.info("done making the optimal_segments_list")
    with sqlite3.connect(the_scenario.main_db) as db_con:
        insert_sql = """
            INSERT into optimal_route_segments
            values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ;"""

        db_con.executemany(insert_sql, optimal_segments_list)
        logger.info("finish optimal_segments_list db_con.executemany()")
        db_con.commit()
        logger.info("finish optimal_segments_list db_con.commit")

    return


# ===================================================================================================


def make_optimal_scenario_results_db(the_scenario, logger):
    logger.info("starting make_optimal_scenario_results_db")

    # Set notes and conditions to include/exclude artificial links
    if the_scenario.report_with_artificial:
        note = "Includes first-mile last-mile (artificial links)"
        artificial_cond = "(0,1,2)" 
    else:
        note = ""
        artificial_cond = "(0,2)" # excludes artificial links

    with sqlite3.connect(the_scenario.main_db) as db_con:

        # drop the table
        sql = "drop table if exists optimal_scenario_results"
        db_con.execute(sql)

        # create the table
        sql = """
            create table optimal_scenario_results(
                                                  table_name text,
                                                  commodity text,
                                                  facility_name text,
                                                  measure text,
                                                  mode text,
                                                  value real,
                                                  units text,
                                                  notes text
                                                  );"""
        db_con.execute(sql)

        # populate a temporary table for multimodal commodities
        sql_multimodal = "drop table if exists tmp_multimodal_commodities;"
        db_con.execute(sql_multimodal)

        sql_multimodal = """ create table tmp_multimodal_commodities as
                             select distinct
                             scenario_rt_id,
                             commodity_name,
                             'multimodal' as multimodal
                             from optimal_route_segments
                             where artificial = 2
                             ;"""
        db_con.execute(sql_multimodal)

        # sum all the flows on artificial = 1, and divide by 2 for each commodity.
        # this assumes we have flows leaving and entering a facility on 1 artificial link at the beginning and the end.
        # when route tracking is enabled (ndrOn = True), identify and separately report routes that are multimodal for each commodity.
        logger.debug("start: summarize total flow")
        sql_total_flow = """ -- total flow query
                        insert into optimal_scenario_results
                        select
                        'commodity_summary',
                        commodity_name,
                        NULL,
                        'total_flow',
                        network_source_id,
                        sum(commodity_flow)/2,
                        units,
                        ""
                        from optimal_route_segments
                        where artificial = 1
                        group by commodity_name, network_source_id
                        ;"""
        db_con.execute(sql_total_flow)

        # length by mode
        # total network length, not route distance traveled
        logger.debug("start: summarize total distance")
        sql_total_length = """ -- total scenario distance (not route distance)
                              insert into optimal_scenario_results
                              select
                              'commodity_summary',
                              commodity_name,
                              NULL,
                              'network_used',
                              network_source_id,
                              sum(length),
                              '{}',
                              '{}' --note
                              from 
                              (select commodity_name, network_source_id, min(length) as length,network_source_oid, artificial from
                              optimal_route_segments group by network_source_oid, commodity_name, network_source_id, artificial)
                              where artificial in {}
                              group by commodity_name, network_source_id
                              ;""".format(the_scenario.default_units_distance, note, artificial_cond)
        db_con.execute(sql_total_length)

        logger.debug("start: summarize unit distance")
        # liquid unit-distance
        sql_liquid_unit_distance = """ -- total liquid unit-distance by mode
                                    insert into optimal_scenario_results
                                    select
                                    'commodity_summary',
                                    commodity_name,
                                    NULL,
                                    "commodity-distance",
                                    network_source_id,
                                    sum(commodity_flow*length),
                                    '{}-{}',
                                    '{}' --note
                                    from optimal_route_segments
                                    where units = '{}' and artificial in {}
                                    group by commodity_name, network_source_id
                                    ;""".format(the_scenario.default_units_liquid_phase, the_scenario.default_units_distance,
                                                note, the_scenario.default_units_liquid_phase, artificial_cond)
        db_con.execute(sql_liquid_unit_distance)

        # solid unit-distance for completeness
        sql_solid_unit_distance = """ -- total solid unit-distance by mode
                                   insert into optimal_scenario_results
                                   select
                                   'commodity_summary',
                                   commodity_name,
                                   NULL,
                                   "commodity-distance",
                                   network_source_id,
                                   sum(commodity_flow*length),
                                   '{}-{}',
                                   '{}' --note
                                   from optimal_route_segments
                                   where units = '{}' and artificial in {}
                                   group by commodity_name, network_source_id
                                   ;""".format(the_scenario.default_units_solid_phase, the_scenario.default_units_distance,
                                               note, the_scenario.default_units_solid_phase, artificial_cond)
        db_con.execute(sql_solid_unit_distance)

        logger.debug("start: summarize transport and routing costs")
        # transport cost and routing cost by mode
        # multiply the distance by the flow
        sql_transport_costs = """ -- total transport_cost
                               insert into optimal_scenario_results
                               select
                               'commodity_summary',
                               commodity_name,
                               NULL,
                               "transport_cost",
                               network_source_id,
                               sum(commodity_flow*link_transport_cost),
                               '{}',
                               '{}' --note
                               from optimal_route_segments
                               where artificial in {}
                               group by commodity_name, network_source_id
                               ;""".format(the_scenario.default_units_currency, note, artificial_cond)
        db_con.execute(sql_transport_costs)

        logger.debug("start: summarize routing costs")
        # note: artificial links always included in routing cost regardless of toggle
        sql_routing_costs = """ -- total routing_cost
                                insert into optimal_scenario_results
                                select
                                'commodity_summary',
                                commodity_name,
                                NULL,
                                "routing_cost",
                                network_source_id,
                                sum(commodity_flow*link_routing_cost),
                                '{}',
                                '' --note
                                from optimal_route_segments
                                group by commodity_name, network_source_id
                                ;""".format(the_scenario.default_units_currency)
        db_con.execute(sql_routing_costs)

        # loads by mode and commodity
        # use artificial links = 1 and = 2 to calculate loads per loading event
        # (e.g., leaving a facility or switching modes at intermodal facility)
        sql_mode_commodity = "select network_source_id, commodity_name from optimal_route_segments group by network_source_id, commodity_name;"
        db_cur = db_con.execute(sql_mode_commodity)
        mode_and_commodity_list = db_cur.fetchall()

        attributes_dict = ftot_supporting_gis.get_commodity_vehicle_attributes_dict(the_scenario, logger)

        logger.debug("start: summarize vehicle loads")
        for row in mode_and_commodity_list:
            mode = row[0]
            if 'pipeline_crude' in mode:
                measure_name = "pipeline_crude_mvts"
            if 'pipeline_prod' in mode:
                measure_name = "pipeline_prod_mvts"
            if "road" in mode:
                measure_name = "truck_loads"
            if "rail" in mode:
                measure_name = "rail_cars"
            if "water" in mode:
                measure_name = "barge_loads"
            commodity_name = row[1]
            vehicle_payload = attributes_dict[commodity_name][mode]['Load']['general'].magnitude
            sql_vehicle_load = """ -- total loads by mode and commodity
                                   insert into optimal_scenario_results
                                   select
                                   'commodity_summary',
                                   commodity_name,
                                   NULL,
                                   'vehicles',
                                   network_source_id,
                                   round(sum(commodity_flow/{}/2) + 0.4999),
                                   '{}', -- units
                                   ''
                                   from optimal_route_segments
                                   where (artificial = 1 or artificial = 2) and network_source_id = '{}' and commodity_name = '{}'
                                   group by commodity_name, network_source_id
                                   ;""".format(vehicle_payload, measure_name, mode, commodity_name)
            db_con.execute(sql_vehicle_load)

        # VMT
        # Do not report for pipeline

        logger.debug("start: summarize vehicle-distance traveled")
        for row in mode_and_commodity_list:
            mode = row[0]
            commodity_name = row[1]

            if 'pipeline' in mode:
                pass
            else:
                vehicle_payload = attributes_dict[commodity_name][mode]['Load']['general'].magnitude
                # vmt is loads multiplied by distance
                # we know the flow on a link we can calculate the loads on that link
                # and multiply by the distance to get VMT
                sql_vmt = """ -- Vehicle-Distance Traveled by mode and commodity
                              insert into optimal_scenario_results
                              select
                              'commodity_summary',
                              commodity_name,
                              NULL,
                              'vehicle-distance_traveled',
                              '{}',
                              sum(round(commodity_flow/{} + 0.4999) * length),
                              'vehicle-{}',
                              '{}' --note
                              from optimal_route_segments
                              where network_source_id = '{}' and commodity_name = '{}' and artificial in {}
                              group by commodity_name, network_source_id
                              ;""".format(mode, vehicle_payload, the_scenario.default_units_distance, note,
                                          mode, commodity_name, artificial_cond)
                db_con.execute(sql_vmt)

        # CO2 -- convert commodity and distance to CO2
        # truck is a bit different than rail and water
        # limited_access = 1 -- otherwise 0
        # urban_rural: urban = 1, rural = 0
        # road CO2 emission factors in g/mi

        logger.debug("start: summarize emissions")

        # Where clauses for road types
        access_and_urban_code = {}
        access_and_urban_code["general"] = {'where': "urban_rural IS NULL and limited_access IS NULL"}
        access_and_urban_code["urban"] = {'where': "urban_rural == 1 and limited_access IS NULL"}
        access_and_urban_code["rural"] = {'where': "urban_rural == 0 and limited_access IS NULL"}
        access_and_urban_code["limited"] = {'where': "urban_rural IS NULL and limited_access == 1"}
        access_and_urban_code["nonlimited"] = {'where': "urban_rural == 0 and limited_access IS NULL"}
        access_and_urban_code["urban_limited"] = {'where': "urban_rural == 1 and limited_access == 1"}
        access_and_urban_code["urban_nonlimited"] = {'where': "urban_rural == 1 and limited_access == 0"}
        access_and_urban_code["rural_limited"] = {'where': "urban_rural == 0 and limited_access == 1"}
        access_and_urban_code["rural_nonlimited"] = {'where': "urban_rural == 0 and limited_access == 0"}

        # Loop through CO2
        # Separate SQL queries if road vs. non-road
        for row in mode_and_commodity_list:
            mode = row[0]
            commodity_name = row[1]

            # CO2 on main network links (excludes artificial links here)

            vehicle_payload = attributes_dict[commodity_name][mode]['Load']['general'].magnitude

            if 'road' == mode:

                for road_measure_name in access_and_urban_code:
                    co2_val = attributes_dict[commodity_name][mode]['co2'][road_measure_name].magnitude
                    where_clause = access_and_urban_code[road_measure_name]['where']

                    sql_road_co2 = """ -- co2 for road by limited_access and urban_rural designation
                                        insert into optimal_scenario_results
                                        select
                                        'commodity_summary',
                                        commodity_name,
                                        NULL,
                                        'co2_{}', -- measure_name
                                        '{}', -- mode
                                        sum({} * length * round(commodity_flow/{} + 0.4999)), -- value (emissions scalar * vmt)
                                        'grams', -- units
                                        ''
                                        from optimal_route_segments
                                        where network_source_id = '{}' and commodity_name = '{}' and {} -- additional where_clause
                                        and artificial != 1 -- exclude artificial links
                                        group by commodity_name, network_source_id
                                        ;""".format(road_measure_name, mode, co2_val, vehicle_payload, mode, commodity_name, where_clause)
                    db_con.execute(sql_road_co2)

            else: # rail, water, or pipeline

                co2_val = attributes_dict[commodity_name][mode]['co2']['general'].magnitude
                sql_nonroad_co2 = """ -- co2 by mode and commodity
                                insert into optimal_scenario_results
                                select
                                'commodity_summary',
                                commodity_name,
                                NULL,
                                'co2_network',
                                '{}',
                                sum(commodity_flow * length * {}),
                                'grams',
                                ''
                                from optimal_route_segments
                                where network_source_id = '{}' and commodity_name = '{}' and artificial != 1
                                group by commodity_name, network_source_id
                                ;""".format(mode, co2_val, mode, commodity_name)
                db_con.execute(sql_nonroad_co2)

            # CO2 on artificial links 
            # Needs special handling because uses road values for all modes
            # Only runs if user wants to include artificial links in metric totals

            if the_scenario.report_with_artificial:

                co2_val = attributes_dict[commodity_name][mode]['co2']['artificial'].magnitude
                vehicle_payload = attributes_dict[commodity_name][mode]['Load']['artificial'].magnitude
                sql_co2_art = """ -- co2 for artificial links
                                insert into optimal_scenario_results
                                select
                                'commodity_summary',
                                commodity_name,
                                NULL,
                                'co2_artificial_links',
                                '{}',
                                sum({} * length * round(commodity_flow/{} + 0.4999)), -- value (emissions scalar * vmt)
                                'grams',
                                ''
                                from optimal_route_segments
                                where network_source_id = '{}' and commodity_name = '{}' and artificial == 1
                                group by commodity_name, network_source_id
                                ;""".format(mode, co2_val, vehicle_payload, mode, commodity_name)
                db_con.execute(sql_co2_art)
          
            # now total the co2 values

            sql_co2_total = """insert into optimal_scenario_results
                                    select
                                    'commodity_summary',
                                    commodity,
                                    NULL,
                                    'co2',
                                    mode,
                                    sum(osr.value),
                                    units,
                                    '{}' --note
                                    from optimal_scenario_results osr
                                    where osr.measure like 'co2^_%' ESCAPE '^' --escape clause specifies the character used to denote a literal underscore
                                    and mode = '{}' and commodity = '{}'
                                    group by commodity, mode
                                    ;""".format(note, mode, commodity_name)
            db_con.execute(sql_co2_total)     
            
            # now delete the intermediate records
            sql_co2_delete = """delete
                                from optimal_scenario_results
                                where measure like 'co2^_%' ESCAPE '^' --escape clause specifies the character used to denote a literal underscore
                                ;"""
            db_con.execute(sql_co2_delete)

        # Fuel burn
        # convert VMT to fuel burn
        # truck, rail, barge, no pipeline
        # Same as VMT but divide result by fuel efficiency

        logger.debug("start: summarize vehicle loads")
        for row in mode_and_commodity_list:
            mode = row[0]
            commodity_name = row[1]
            if 'pipeline' in mode:
                pass
            else:
                vehicle_payload = attributes_dict[commodity_name][mode]['Load']['general'].magnitude
                fuel_efficiency = attributes_dict[commodity_name][mode]['Fuel_Efficiency'].magnitude #mi/gal

                # vmt is loads multiplied by length
                # we know the flow on a link we can calculate the loads on that link
                # and multiply by the length to get VMT. Then divide by fuel efficiency to get fuel burn
                sql_fuel_burn = """ -- Fuel burn by mode and commodity
                                    insert into optimal_scenario_results
                                    select
                                    'commodity_summary',
                                    commodity_name,
                                    NULL,
                                    'fuel_burn',
                                    '{}',
                                    sum(length * round(commodity_flow/{} + 0.4999) / {}),
                                    'Gallons',
                                    '{}' --note
                                    from optimal_route_segments
                                    where network_source_id = '{}' and commodity_name = '{}' and artificial in {}
                                    group by commodity_name, network_source_id
                                    ;""".format(mode, vehicle_payload, fuel_efficiency, note,
                                                mode, commodity_name, artificial_cond)
                db_con.execute(sql_fuel_burn)

        # Destination Deliveries
        # look at artificial = 1 and join on facility_name or d_location
        # compare optimal flow at the facility (flow on all links)
        # to the destination demand in the facility commodities table.
        # report % fulfillment by commodity
        # Destination report

        logger.debug("start: summarize supply and demand")
        sql_destination_demand = """insert into optimal_scenario_results
                                    select
                                    "facility_summary",
                                    c.commodity_name,
                                    f.facility_name,
                                    "destination_demand_potential",
                                    "total",
                                    fc.scaled_quantity,
                                    fc.units,
                                    ''
                                    from facility_commodities fc
                                    join facilities f on f.facility_id = fc.facility_id
                                    join commodities c on c.commodity_id = fc.commodity_id
                                    join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                    where fti.facility_type = 'ultimate_destination'
        							order by facility_name
							        ;"""

        # RMP supplier report
        sql_rmp_supply = """insert into optimal_scenario_results
                            select
                            "facility_summary",
                            c.commodity_name,
                            f.facility_name,
                            "rmp_supply_potential",
                            "total",
                            fc.scaled_quantity,
                            fc.units,
                            ''
                            from facility_commodities fc
                            join facilities f on f.facility_id = fc.facility_id
                            join commodities c on c.commodity_id = fc.commodity_id
                            join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                            where fti.facility_type = 'raw_material_producer'
                            order by facility_name
                            ;"""

        # Processor capacity report (input)
        sql_proc_input_capacity = """insert into optimal_scenario_results
                                     select
                                     "facility_summary",
                                     c.commodity_name,
                                     f.facility_name,
                                     "processor_input_capacity",
                                     "total",
                                     IFNULL(fc.scaled_quantity, "Unconstrained"),
                                     fc.units,
                                     ''
                                     from facility_commodities fc
                                     join facilities f on f.facility_id = fc.facility_id
                                     join commodities c on c.commodity_id = fc.commodity_id
                                     join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                     where fti.facility_type = 'processor' and fc.io = 'i'
                                     order by facility_name
                                     ;"""

        # Processor capacity report (output)
        sql_proc_output_capacity = """insert into optimal_scenario_results
                                      select
                                      "facility_summary",
                                      c.commodity_name,
                                      f.facility_name,
                                      "processor_output_capacity",
                                      "total",
                                      IFNULL(fc.scaled_quantity, "Unconstrained"),
                                      fc.units,
                                      ''
                                      from facility_commodities fc
                                      join facilities f on f.facility_id = fc.facility_id
                                      join commodities c on c.commodity_id = fc.commodity_id
                                      join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                      where fti.facility_type = 'processor' and fc.io = 'o'
                                      order by facility_name
                                      ;"""

        # initialize SQL queries that vary based on whether routes are used or not
        logger.debug("start: summarize optimal supply and demand")
        if the_scenario.ndrOn:
            sql_destination_demand_optimal = """insert into optimal_scenario_results
                                                select
                                                "facility_summary",
                                                ov.commodity_name,
                                                ov.d_facility,
                                                "destination_demand_optimal",
                                                ne.mode_source,
                                                sum(ov.variable_value),
                                                ov.units,
                                                ''
                                                from optimal_variables ov
                                                join edges e on e.edge_id = ov.var_id
                                                join route_reference rr on rr.route_id = e.route_id
                                                join networkx_edges ne on ne.edge_id = rr.last_nx_edge_id
                                                join facilities f on f.facility_name = ov.d_facility
                                                join facility_commodities fc on fc.facility_id = f.facility_id and fc.commodity_id = ov.commodity_id
                                                join commodities c on c.commodity_id = fc.commodity_id
                                                join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                                where ov.d_facility > 0 and ov.edge_type = 'transport' and fti.facility_type = 'ultimate_destination'
                                                group by ov.commodity_name, ov.d_facility, ne.mode_source, ov.units
                                                ;"""

            sql_destination_demand_optimal_frac = """insert into optimal_scenario_results
                                                     select
                                                     "facility_summary",
                                                     ov.commodity_name,
                                                     ov.d_facility,
                                                     "destination_demand_optimal_frac",
                                                     ne.mode_source,
                                                     (sum(ov.variable_value) / fc.scaled_quantity),
                                                     "fraction",
                                                     ''
                                                     from optimal_variables ov
                                                     join edges e on e.edge_id = ov.var_id
                                                     join route_reference rr on rr.route_id = e.route_id
                                                     join networkx_edges ne on ne.edge_id = rr.last_nx_edge_id
                                                     join facilities f on f.facility_name = ov.d_facility
                                                     join facility_commodities fc on fc.facility_id = f.facility_id and fc.commodity_id = ov.commodity_id
                                                     join commodities c on c.commodity_id = fc.commodity_id
                                                     join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                                     where ov.d_facility > 0 and ov.edge_type = 'transport' and fti.facility_type = 'ultimate_destination'
                                                     group by ov.commodity_name, ov.d_facility, ne.mode_source
                                                     ;"""

            # RMP supplier report
            sql_rmp_supply_optimal = """insert into optimal_scenario_results
                                        select
                                        "facility_summary",
                                        ov.commodity_name,
                                        ov.o_facility,
                                        "rmp_supply_optimal",
                                        ne.mode_source,
                                        sum(ov.variable_value),
                                        ov.units,
                                        ''
                                        from optimal_variables ov
                                        join edges e on e.edge_id = ov.var_id
                                        join route_reference rr on rr.route_id = e.route_id
                                        join networkx_edges ne on ne.edge_id = rr.first_nx_edge_id
                                        join facilities f on f.facility_name = ov.o_facility
                                        join facility_commodities fc on fc.facility_id = f.facility_id and fc.commodity_id = ov.commodity_id
                                        join commodities c on c.commodity_id = fc.commodity_id
                                        join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                        where ov.o_facility > 0 and ov.edge_type = 'transport' and fti.facility_type = 'raw_material_producer'
                                        group by ov.commodity_name, ov.o_facility, ne.mode_source, ov.units
                                        ;"""

            # RMP supplier report
            sql_rmp_supply_optimal_frac = """insert into optimal_scenario_results
                                             select
                                             "facility_summary",
                                             ov.commodity_name,
                                             ov.o_facility,
                                             "rmp_supply_optimal_frac",
                                             ne.mode_source,
                                             (sum(ov.variable_value) / fc.scaled_quantity),
                                             "fraction",
                                             ''
                                             from optimal_variables ov
                                             join edges e on e.edge_id = ov.var_id
                                             join route_reference rr on rr.route_id = e.route_id
                                             join networkx_edges ne on ne.edge_id = rr.first_nx_edge_id
                                             join facilities f on f.facility_name = ov.o_facility
                                             join facility_commodities fc on fc.facility_id = f.facility_id and fc.commodity_id = ov.commodity_id
                                             join commodities c on c.commodity_id = fc.commodity_id
                                             join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                             where ov.o_facility > 0 and ov.edge_type = 'transport' and fti.facility_type = 'raw_material_producer'
                                             group by ov.commodity_name, ov.o_facility, ne.mode_source
                                             ;"""

            # Processor report
            sql_processor_output = """insert into optimal_scenario_results
                                      select
                                      "facility_summary",
                                      ov.commodity_name,
                                      ov.o_facility,
                                      "processor_output_optimal",
                                      ne.mode_source,
                                      sum(ov.variable_value),
                                      ov.units,
                                      ''
                                      from optimal_variables ov
                                      join edges e on e.edge_id = ov.var_id
                                      join route_reference rr on rr.route_id = e.route_id
                                      join networkx_edges ne on ne.edge_id = rr.first_nx_edge_id
                                      join facilities f on f.facility_name = ov.o_facility
                                      join facility_commodities fc on fc.facility_id = f.facility_id and fc.commodity_id = ov.commodity_id
                                      join commodities c on c.commodity_id = fc.commodity_id
                                      join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                      where ov.o_facility > 0 and ov.edge_type = 'transport' and fti.facility_type = 'processor'
                                      group by ov.commodity_name, ov.o_facility, ne.mode_source, ov.units
                                      ;"""
            
            sql_processor_input = """insert into optimal_scenario_results
                                     select
                                     "facility_summary",
                                     ov.commodity_name,
                                     ov.d_facility,
                                     "processor_input_optimal",
                                     ne.mode_source,
                                     sum(ov.variable_value),
                                     ov.units,
                                     ''
                                     from optimal_variables ov
                                     join edges e on e.edge_id = ov.var_id
                                     join route_reference rr on rr.route_id = e.route_id
                                     join networkx_edges ne on ne.edge_id = rr.last_nx_edge_id
                                     join facilities f on f.facility_name = ov.d_facility
                                     join facility_commodities fc on fc.facility_id = f.facility_id and fc.commodity_id = ov.commodity_id
                                     join commodities c on c.commodity_id = fc.commodity_id
                                     join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                     where ov.d_facility > 0 and ov.edge_type = 'transport' and fti.facility_type = 'processor'
                                     group by ov.commodity_name, ov.d_facility, ne.mode_source, ov.units
                                     ;"""

            sql_processor_build_cost = """insert into optimal_scenario_results
                                       select
                                       "facility_summary",
                                       NULL,
                                       ov.o_facility,
                                       "processor_amortized_build_cost",
                                       "total",
                                       f.build_cost,
                                       "{}",
                                       ''
                                       from facilities f
                                       join optimal_variables ov on f.facility_name = ov.o_facility
                                       join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                       where ov.o_facility > 0 and ov.edge_type = 'transport' and fti.facility_type = 'processor' and f.build_cost > 0
                                       group by ov.o_facility
                                       ;""".format(the_scenario.default_units_currency)

        else :
            sql_destination_demand_optimal = """insert into optimal_scenario_results
                                                select
                                                "facility_summary",
                                                ov.commodity_name,
                                                ov.d_facility,
                                                "destination_demand_optimal",
                                                ov.mode,
                                                sum(ov.variable_value),
                                                ov.units,
                                                ''
                                                from optimal_variables ov
                                                join facilities f on f.facility_name = ov.d_facility
                                                join facility_commodities fc on fc.facility_id = f.facility_id and fc.commodity_id = ov.commodity_id
                                                join commodities c on c.commodity_id = fc.commodity_id
            							        join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                                where ov.d_facility > 0 and ov.edge_type = 'transport' and fti.facility_type = 'ultimate_destination'
                                                group by ov.commodity_name, ov.d_facility, ov.mode, ov.units
                                                ;"""

            sql_destination_demand_optimal_frac = """insert into optimal_scenario_results
                                                     select
                                                     "facility_summary",
                                                     ov.commodity_name,
                                                     ov.d_facility,
                                                     "destination_demand_optimal_frac",
                                                     ov.mode,
                                                     (sum(ov.variable_value) / fc.scaled_quantity),
                                                     "fraction",
                                                     ''
                                                     from optimal_variables ov
                                                     join facilities f on f.facility_name = ov.d_facility
                                                     join facility_commodities fc on fc.facility_id = f.facility_id and fc.commodity_id = ov.commodity_id
                                                     join commodities c on c.commodity_id = fc.commodity_id
                                                     join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                                     where ov.d_facility > 0 and ov.edge_type = 'transport' and fti.facility_type = 'ultimate_destination'
                                                     group by ov.commodity_name, ov.d_facility, ov.mode
                                                     ;"""

            # RMP optimal supply
            sql_rmp_supply_optimal = """insert into optimal_scenario_results
                                        select
                                        "facility_summary",
                                        ov.commodity_name,
                                        ov.o_facility,
                                        "rmp_supply_optimal",
                                        ov.mode,
                                        sum(ov.variable_value),
                                        ov.units,
                                        ''
                                        from optimal_variables ov
                                        join facilities f on f.facility_name = ov.o_facility
                                        join facility_commodities fc on fc.facility_id = f.facility_id and fc.commodity_id = ov.commodity_id
                                        join commodities c on c.commodity_id = fc.commodity_id
                                        join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                        where ov.o_facility > 0 and ov.edge_type = 'transport' and fti.facility_type = 'raw_material_producer'
                                        group by ov.commodity_name, ov.o_facility, ov.mode, ov.units
                                        ;"""

            # RMP supplier report
            sql_rmp_supply_optimal_frac = """insert into optimal_scenario_results
                                             select
                                             "facility_summary",
                                             ov.commodity_name,
                                             ov.o_facility,
                                             "rmp_supply_optimal_frac",
                                             ov.mode,
                                             (sum(ov.variable_value) / fc.scaled_quantity),
                                             "fraction",
                                             ''
                                             from optimal_variables ov
                                             join facilities f on f.facility_name = ov.o_facility
                                             join facility_commodities fc on fc.facility_id = f.facility_id and fc.commodity_id = ov.commodity_id
                                             join commodities c on c.commodity_id = fc.commodity_id
                                             join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                             where ov.o_facility > 0 and ov.edge_type = 'transport' and fti.facility_type = 'raw_material_producer'
                                             group by ov.commodity_name, ov.o_facility, ov.mode
                                             ;"""

            # Processor report
            sql_processor_output = """insert into optimal_scenario_results
                                      select
                                      "facility_summary",
                                      ov.commodity_name,
                                      ov.o_facility,
                                      "processor_output_optimal",
                                      ov.mode,
                                      (sum(ov.variable_value)),
                                      ov.units,
                                      ''
                                      from optimal_variables ov
                                      join facilities f on f.facility_name = ov.o_facility
                                      join facility_commodities fc on fc.facility_id = f.facility_id and fc.commodity_id = ov.commodity_id
                                      join commodities c on c.commodity_id = fc.commodity_id
                                      join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                      where ov.o_facility > 0 and ov.edge_type = 'transport' and fti.facility_type = 'processor'
                                      group by ov.commodity_name, ov.o_facility, ov.mode, ov.units
                                      ;"""

            sql_processor_input = """insert into optimal_scenario_results
                                     select
                                     "facility_summary",
                                     ov.commodity_name,
                                     ov.d_facility,
                                     "processor_input_optimal",
                                     ov.mode,
                                     (sum(ov.variable_value)),
                                     ov.units,
                                     ''
                                     from optimal_variables ov
                                     join facilities f on f.facility_name = ov.d_facility
                                     join facility_commodities fc on fc.facility_id = f.facility_id and fc.commodity_id = ov.commodity_id
                                     join commodities c on c.commodity_id = fc.commodity_id
                                     join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                     where ov.d_facility > 0 and ov.edge_type = 'transport' and fti.facility_type = 'processor'
                                     group by ov.commodity_name, ov.d_facility, ov.mode, ov.units
                                     ;"""

            sql_processor_build_cost = """insert into optimal_scenario_results
                                       select
                                       "facility_summary",
                                       NULL,
                                       ov.o_facility,
                                       "processor_amortized_build_cost",
                                       "total",
                                       f.build_cost,
                                       "{}",
                                       ''
                                       from facilities f
                                       join optimal_variables ov on f.facility_name = ov.o_facility
                                       join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                       where o_facility > 0 and edge_type = 'transport' and fti.facility_type = 'processor' and f.build_cost > 0
                                       group by o_facility
                                       ;""".format(the_scenario.default_units_currency)

        db_con.execute(sql_destination_demand)
        db_con.execute(sql_destination_demand_optimal)
        db_con.execute(sql_destination_demand_optimal_frac)
        db_con.execute(sql_rmp_supply)
        db_con.execute(sql_proc_input_capacity)
        db_con.execute(sql_proc_output_capacity)

        db_con.execute(sql_rmp_supply_optimal)
        db_con.execute(sql_rmp_supply_optimal_frac)
        db_con.execute(sql_processor_output)
        db_con.execute(sql_processor_input)
        db_con.execute(sql_processor_build_cost)

        # totals across modes
        sql_allmodes = """insert into optimal_scenario_results
                       select
                       table_name,
                       commodity,
                       facility_name,
                       measure,
                       "allmodes" as mode,
                       sum(value),
                       case --units
                            when measure == "vehicles" then "vehicles"
                            else units
                            end units,
                       max(notes) -- use non-blank note if it exists (technically takes the last alphabetically)
                       from optimal_scenario_results
                       where mode != "total"
                       group by table_name, commodity, facility_name, measure
                       ;"""
        db_con.execute(sql_allmodes)

        # processor optimal fractions, as share of input and output capacity
        sql_proc_input_optimal_frac = """insert into optimal_scenario_results
                                             select
                                             "facility_summary",
                                             ov.commodity_name,
                                             ov.d_facility,
                                             "processor_input_optimal_frac",
                                             "allmodes",
                                             IFNULL((sum(ov.variable_value) / fc.scaled_quantity), "Unconstrained"),
                                             "fraction",
                                             ''
                                             from optimal_variables ov
                                             join facilities f on f.facility_name = ov.d_facility
                                             join facility_commodities fc on fc.facility_id = f.facility_id and fc.commodity_id = ov.commodity_id
                                             join commodities c on c.commodity_id = fc.commodity_id
                                             join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                             where ov.d_facility > 0 and ov.edge_type = 'transport' and fti.facility_type = 'processor' and fc.io = 'i'
                                             group by ov.commodity_name, ov.d_facility
                                             ;"""
        db_con.execute(sql_proc_input_optimal_frac)

        sql_proc_output_optimal_frac = """insert into optimal_scenario_results
                                             select
                                             "facility_summary",
                                             ov.commodity_name,
                                             ov.o_facility,
                                             "processor_output_optimal_frac",
                                             "allmodes",
                                             IFNULL((sum(ov.variable_value) / fc.scaled_quantity), "Unconstrained"),
                                             "fraction",
                                             ''
                                             from optimal_variables ov
                                             join facilities f on f.facility_name = ov.o_facility
                                             join facility_commodities fc on fc.facility_id = f.facility_id and fc.commodity_id = ov.commodity_id
                                             join commodities c on c.commodity_id = fc.commodity_id
                                             join facility_type_id fti on fti.facility_type_id  = f.facility_type_id
                                             where ov.o_facility > 0 and ov.edge_type = 'transport' and fti.facility_type = 'processor' and fc.io = 'o'
                                             group by ov.commodity_name, ov.o_facility
                                             ;"""
        
        db_con.execute(sql_proc_output_optimal_frac)

        # scenario totals
        sql_total = """insert into optimal_scenario_results
                       select "scenario_summary" as table_name, NULL as commodity, NULL as facility_name, measure, mode, sum(value), units, notes
                       from optimal_scenario_results
                       where measure in ("co2", "transport_cost", "fuel_burn", "network_used", "processor_amortized_build_cost", "vehicles", "vehicle-distance_traveled")
                       group by measure, mode, units
                       ;"""
        db_con.execute(sql_total)

        # get a list of the commodities that have multimodal movements
        sql = "select distinct commodity_name from tmp_multimodal_commodities;"
        db_cur = db_con.execute(sql)
        mm_commodities = db_cur.fetchall()

        # add a note to optimal_scenario_results for commodities with multimodal flows
        update_sql = """
                     UPDATE optimal_scenario_results
                     set notes = 'includes multimodal flows'
                     where measure = 'total_flow' and mode = 'allmodes'
                     and commodity = ?
                     ;"""
        db_con.executemany(update_sql, mm_commodities)

        # suppress modes for total_flow if commodity is multimodal
        delete_sql = """
                DELETE from optimal_scenario_results
                where measure = 'total_flow' and mode != 'allmodes'
                and commodity = ?
                ;"""
        
        db_con.executemany(delete_sql, mm_commodities)
        db_con.commit()

        # delete the temporary table for multimodal commodities
        logger.debug("drop the tmp_multimodal_commodities table")
        db_con.execute("drop table if exists tmp_multimodal_commodities;")

    logger.debug("finish: make_optimal_scenario_results_db()")


# ===================================================================================================


def generate_scenario_summary(the_scenario, logger):
    logger.info("starting generate_scenario_summary")

    # query the optimal scenario results table and report out the results
    with sqlite3.connect(the_scenario.main_db) as db_con:

        # Log scenario summary
        sql = "select * from optimal_scenario_results where table_name = 'scenario_summary' order by commodity, measure, mode;"
        db_cur = db_con.execute(sql)
        data = db_cur.fetchall()
        for row in data:
            logger.result('{}_{}_{}: \t {:,.2f} : \t {}'.format(row[0].upper(), row[3].upper(), row[4].upper(), row[5], row[6]))

        # Log commodity results
        sql = "select * from optimal_scenario_results where table_name = 'commodity_summary' order by commodity, measure, mode;"
        db_cur = db_con.execute(sql)
        data = db_cur.fetchall()
        for row in data:
            logger.result('{}_{}_{}_{}: \t {:,.2f} : \t {}'.format(row[0].upper(), row[1].upper(), row[3].upper(), row[4].upper(), row[5], row[6]))

        # Log facility results
        sql = "select * from optimal_scenario_results where table_name = 'facility_summary' order by facility_name, measure, commodity, mode;"
        db_cur = db_con.execute(sql)
        data = db_cur.fetchall()
        for row in data:
            if row[3] == 'processor_amortized_build_cost':
                logger.result('{}_{}_{}_{}: \t {:,.2f} : \t {}'.format(row[0].upper(), row[2].upper(), row[3].upper(), row[4].upper(), row[5], row[6]))
            elif type(row[5]) == str:
                logger.result('{}_{}_{}_{}_{}: \t {} : \t {}'.format(row[0].upper(), row[2].upper(), row[3].upper(), row[1].upper(), row[4].upper(), row[5], row[6]))
            else:
                logger.result('{}_{}_{}_{}_{}: \t {:,.2f} : \t {}'.format(row[0].upper(), row[2].upper(), row[3].upper(), row[1].upper(), row[4].upper(), row[5], row[6]))

    logger.debug("finish: generate_scenario_summary()")


# ===================================================================================================


def detailed_emissions_setup(the_scenario, logger):

    logger.info("START: detailed_emissions_setup")

    # Set notes to include/exclude artificial links
    if the_scenario.report_with_artificial:
        note = "Includes first-mile last-mile (artificial links)"
    else:
        note = ""
       
    with sqlite3.connect(the_scenario.main_db) as db_con:

        # initiate detailed_emissions DB table
        db_con.executescript("""
            drop table if exists detailed_emissions;
            create table detailed_emissions(
            commodity text,
            mode text,
            measure text,
            value real,
            units text,
            notes text
            --constraint unique_elements unique(commodity, mode, measure)
            )
            ;""")

        # query commodity_vehicle_attrs DB table
        # populate attributes dictionary
        attribute_dict = {}
        commodity_vehicle_attrs = db_con.execute("select * from commodity_vehicle_attrs;")
        commodity_vehicle_attrs = commodity_vehicle_attrs.fetchall()
        for row in commodity_vehicle_attrs:
            commodity = row[0]
            mode = row[1]
            attribute = row[2]
            link_type = row[3]
            value = Q_(row[4])

            if attribute in ['Fuel_Efficiency']:
                if commodity not in attribute_dict:
                    attribute_dict[commodity] = {mode: {attribute: value}}
                elif mode not in attribute_dict[commodity]:
                    attribute_dict[commodity][mode] = {attribute: value}
                elif attribute not in attribute_dict[commodity][mode]:
                    attribute_dict[commodity][mode][attribute] = value

            else: # emissions, load
                if commodity not in attribute_dict:
                    attribute_dict[commodity] = {mode: {attribute: {link_type: value}}}
                elif mode not in attribute_dict[commodity]:
                    attribute_dict[commodity][mode] = {attribute: {link_type: value}}
                elif attribute not in attribute_dict[commodity][mode]:
                    attribute_dict[commodity][mode][attribute] = {link_type: value}
                elif link_type not in attribute_dict[commodity][mode][attribute]:
                    attribute_dict[commodity][mode][attribute][link_type] = value
            
        # query optimal_route_segments table
        sql_mode_commodity = "select network_source_id, commodity_name from optimal_route_segments group by network_source_id, commodity_name;"
        db_cur = db_con.execute(sql_mode_commodity)
        mode_and_commodity_list = db_cur.fetchall()

        # Loop through all gases, not just CO2
        # Separate SQL queries if road vs. non-road

        # Where clauses for road types
        access_and_urban_code = {}
        access_and_urban_code["general"] = {'where': "urban_rural IS NULL and limited_access IS NULL"}
        access_and_urban_code["urban"] = {'where': "urban_rural == 1 and limited_access IS NULL"}
        access_and_urban_code["rural"] = {'where': "urban_rural == 0 and limited_access IS NULL"}
        access_and_urban_code["limited"] = {'where': "urban_rural IS NULL and limited_access == 1"}
        access_and_urban_code["nonlimited"] = {'where': "urban_rural == 0 and limited_access IS NULL"}
        access_and_urban_code["urban_limited"] = {'where': "urban_rural == 1 and limited_access == 1"}
        access_and_urban_code["urban_nonlimited"] = {'where': "urban_rural == 1 and limited_access == 0"}
        access_and_urban_code["rural_limited"] = {'where': "urban_rural == 0 and limited_access == 1"}
        access_and_urban_code["rural_nonlimited"] = {'where': "urban_rural == 0 and limited_access == 0"}

        for row in mode_and_commodity_list:
            mode = row[0]
            commodity_name = row[1]

            for pollutant in set(attribute_dict[commodity_name][mode]):
                if pollutant in ['Load','Fuel_Efficiency']:
                    continue # SKIP because not a pollutant

                # emissions on main network links
                # may not have detailed emissions for all modes so check if 'general' emission factor exists
                if 'general' in attribute_dict[commodity_name][mode][pollutant]: 

                    vehicle_payload = attribute_dict[commodity_name][mode]['Load']['general'].magnitude

                    if 'road' == mode:

                        for road_measure_name in access_and_urban_code:
                            emissions_val = attribute_dict[commodity_name][mode][pollutant][road_measure_name].magnitude 
                            where_clause = access_and_urban_code[road_measure_name]['where']
                            
                            sql_road_emissions = """ -- emissions for road by limited_access and urban_rural designation
                                        insert into detailed_emissions
                                        select
                                        commodity_name,
                                        '{}', -- mode
                                        '{}_{}', -- measure_name
                                        sum({} * length * round(commodity_flow/{} + 0.4999)), -- value (emissions scalar * vmt)
                                        'grams', -- units
                                        ''
                                        from optimal_route_segments
                                        where network_source_id = '{}' and commodity_name = '{}' and {} -- additional where_clause
                                        and artificial != 1 -- exclude artificial links
                                        group by commodity_name, network_source_id
                                        ;""".format(mode, pollutant, road_measure_name, emissions_val, vehicle_payload, mode, commodity_name, where_clause)
                            db_con.execute(sql_road_emissions)
                    
                    else: # rail, water, or pipeline

                        emissions_val = attribute_dict[commodity_name][mode][pollutant]['general'].magnitude
                        sql_nonroad_emissions = """ -- emissions by mode and commodity
                                    insert into detailed_emissions
                                    select
                                    commodity_name,
                                    '{}', --mode
                                    '{}_network', --pollutant
                                    sum(commodity_flow * length * {}),
                                    'grams',
                                    ''
                                    from optimal_route_segments
                                    where network_source_id = '{}' and commodity_name = '{}' and artificial != 1
                                    group by commodity_name, network_source_id
                                    ;""".format(mode, pollutant, emissions_val, mode, commodity_name)
                        db_con.execute(sql_nonroad_emissions)
                
                # emissions on artificial links
                # if reporting with artificial link set to true
                if the_scenario.report_with_artificial:
                    emissions_val = attribute_dict[commodity_name][mode][pollutant]['artificial'].magnitude
                    vehicle_payload = attribute_dict[commodity_name][mode]['Load']['artificial'].magnitude

                    sql_emissions_art = """ -- emissions for artificial links
                                        insert into detailed_emissions
                                        select
                                        commodity_name,
                                        '{}', --mode
                                        '{}_artificial_links', --pollutant
                                        sum({} * length * round(commodity_flow/{} + 0.4999)), -- value (emissions scalar * vmt)
                                        'grams',
                                        ''
                                        from optimal_route_segments
                                        where network_source_id = '{}' and commodity_name = '{}' and artificial == 1
                                        group by commodity_name, network_source_id
                                        ;""".format(mode, pollutant, emissions_val, vehicle_payload, mode, commodity_name)
                    db_con.execute(sql_emissions_art)

                # now total the pollutant values
                sql_emissions_total = """insert into detailed_emissions
                                    select
                                    commodity,
                                    mode,
                                    '{}', --pollutant
                                    sum(de.value),
                                    units,
                                    '{}' --note
                                    from detailed_emissions de
                                    where de.measure like '{}^_%' ESCAPE '^'
                                    and mode = '{}' and commodity = '{}'
                                    group by commodity, mode, units
                                    ;""".format(pollutant, note, pollutant, mode, commodity_name)
                db_con.execute(sql_emissions_total)

                # now delete the intermediate records
                # except keep the artificial link breakout
                sql_emissions_delete = """delete
                            from detailed_emissions
                            where measure like '{}^_%' ESCAPE '^'
                            ;""".format(pollutant)
                db_con.execute(sql_emissions_delete)


# ==============================================================================================


def db_report_commodity_utilization(the_scenario, logger):
    logger.info("start: db_report_commodity_utilization")

    # This query pulls the total quantity flowed of each commodity from the optimal scenario results (osr) table.
    # It groups by commodity_name, facility type, and units. The io field is included to help the user
    # determine the potential supply, demand, and processing utilization in the scenario.
    # -----------------------------------

    with sqlite3.connect(the_scenario.main_db) as db_con:
        sql = """select c.commodity_name,
                 fti.facility_type,
                 fc.io,
                 IFNULL(round((select osr.value
                               from optimal_scenario_results osr
                               where osr.commodity = c.commodity_name
                               and osr.measure = 'total_flow'
                               and osr.mode = 'allmodes'), 2), "No Flow") as optimal_flow,
                 fc.units
                 from facility_commodities fc
                 join commodities c on fc.commodity_id = c.commodity_id
                 join facilities f on f.facility_id = fc.facility_id
                 join facility_type_id fti on fti.facility_type_id = f.facility_type_id
                 where f.ignore_facility == 'false'
                 group by c.commodity_name, fti.facility_type, fc.io, fc.units
                 order by c.commodity_name, fc.io asc
                 ;"""
        db_cur = db_con.execute(sql)

        db_data = db_cur.fetchall()
        logger.result("-------------------------------------------------------------------")
        logger.result("Scenario Total Flow of Supply and Demand")
        logger.result("-------------------------------------------------------------------")
        logger.result("commodity_name | facility_type | io |  optimal_flow |     units     ")
        logger.result("---------------|---------------|----|---------------|---------------")
        for row in db_data:
            if (type(row[3]) == str):
                logger.result("{:15.15} {:15.15} {:4.1} {:15.15} {:15.15}".format(row[0], row[1], row[2], row[3], row[4]))
            else:
                logger.result("{:15.15} {:15.15} {:4.1} {:15,.2f} {:15.15}".format(row[0], row[1], row[2], row[3], row[4]))                
        logger.result("-------------------------------------------------------------------")

    # This query compares the optimal flow to the net available quantity of material in the scenario.
    # It groups by commodity_name, facility type, and units. The io field is included to help the user
    # determine the potential supply, demand, and processing utilization in the scenario.
    # -----------------------------------
    with sqlite3.connect(the_scenario.main_db) as db_con:
        sql = """select c.commodity_name, fti.facility_type, fc.io,
                 (CASE WHEN (select osr.value
                            from optimal_scenario_results osr
                            where osr.commodity = c.commodity_name and osr.measure = 'total_flow'
                            and osr.mode = 'allmodes') IS NULL THEN "No Flow"
					   WHEN sum(CASE WHEN fc.scaled_quantity IS NULL THEN 1 ELSE 0 END) <> 0 THEN "Unconstrained"
                       ELSE round((select osr.value
                            from optimal_scenario_results osr
                            where osr.commodity = c.commodity_name
                            and osr.measure = 'total_flow'
                            and osr.mode = 'allmodes') / sum(fc.scaled_quantity), 2) END) as Utilization,
                 'fraction'
                 from facility_commodities fc
                 join commodities c on fc.commodity_id = c.commodity_id
                 join facilities f on f.facility_id = fc.facility_id
                 join facility_type_id fti on fti.facility_type_id = f.facility_type_id
                 where f.ignore_facility == 'false'
                 group by c.commodity_name, fti.facility_type, fc.io, fc.units
                 order by c.commodity_name, fc.io asc;"""
        db_cur = db_con.execute(sql)

        db_data = db_cur.fetchall()
        logger.result("-------------------------------------------------------------------")
        logger.result("Scenario Total Utilization of Supply and Demand")
        logger.result("-------------------------------------------------------------------")
        logger.result("note: total utilization is defined as (total flow / net available)")
        logger.result("commodity_name | facility_type | io |  utilization  |     units     ")
        logger.result("---------------|---------------|----|---------------|---------------")
        for row in db_data:
            if (type(row[3]) == str):
                logger.result("{:15.15} {:15.15} {:4.1} {:15.15} {:15.15}".format(row[0], row[1], row[2], row[3], row[4]))
            else:
                logger.result("{:15.15} {:15.15} {:4.1} {:15,.2f} {:15.15}".format(row[0], row[1], row[2], row[3], row[4]))
        logger.result("-------------------------------------------------------------------")


# ===================================================================================================


def make_optimal_route_segments_featureclass_from_db(the_scenario, logger):

    logger.info("starting make_optimal_route_segments_featureclass_from_db")

    # create the segments layer
    # -------------------------
    optimized_route_segments_fc = os.path.join(the_scenario.main_gdb, "optimized_route_segments")

    if arcpy.Exists(optimized_route_segments_fc):
        arcpy.Delete_management(optimized_route_segments_fc)
        logger.debug("deleted existing {} layer".format(optimized_route_segments_fc))

    scenario_proj = ftot_supporting_gis.get_coordinate_system(the_scenario)
    arcpy.CreateFeatureclass_management(the_scenario.main_gdb, "optimized_route_segments", \
       "POLYLINE", "#", "DISABLED", "DISABLED", scenario_proj, "#", "0", "0", "0")

    arcpy.AddField_management(optimized_route_segments_fc, "ROUTE_TYPE", "TEXT", "#", "#", "25", "#", "NULLABLE", "NON_REQUIRED", "#")
    arcpy.AddField_management(optimized_route_segments_fc, "FTOT_RT_ID", "LONG")
    arcpy.AddField_management(optimized_route_segments_fc, "FTOT_RT_ID_VARIANT", "LONG")
    arcpy.AddField_management(optimized_route_segments_fc, "NET_SOURCE_NAME", "TEXT")
    arcpy.AddField_management(optimized_route_segments_fc, "NET_SOURCE_OID", "LONG")
    arcpy.AddField_management(optimized_route_segments_fc, "ARTIFICIAL", "SHORT")

    # if from pos = 0 then traveresed in direction of underlying arc, otherwise traversed against the flow of the arc.
    # used to determine if from_to_dollar cost should be used or to_from_dollar cost should be used.
    arcpy.AddField_management(optimized_route_segments_fc, "FromPosition", "DOUBLE")  # mnp 8/30/18 -- deprecated
    arcpy.AddField_management(optimized_route_segments_fc, "FromJunctionID", "DOUBLE")  # mnp - 060116 - added this for processor siting step
    arcpy.AddField_management(optimized_route_segments_fc, "TIME_PERIOD", "TEXT")
    arcpy.AddField_management(optimized_route_segments_fc, "COMMODITY", "TEXT")
    arcpy.AddField_management(optimized_route_segments_fc, "COMMODITY_FLOW", "FLOAT")
    arcpy.AddField_management(optimized_route_segments_fc, "VOLUME", "FLOAT")
    arcpy.AddField_management(optimized_route_segments_fc, "CAPACITY", "FLOAT")
    arcpy.AddField_management(optimized_route_segments_fc, "CAPACITY_MINUS_VOLUME", "FLOAT")
    arcpy.AddField_management(optimized_route_segments_fc, "UNITS", "TEXT")
    arcpy.AddField_management(optimized_route_segments_fc, "LENGTH", "FLOAT")
    arcpy.AddField_management(optimized_route_segments_fc, "PHASE_OF_MATTER", "TEXT")
    arcpy.AddField_management(optimized_route_segments_fc, "LINK_ROUTING_COST", "FLOAT")
    arcpy.AddField_management(optimized_route_segments_fc, "LINK_TRANSPORT_COST", "FLOAT")

    # get a list of the modes used in the optimal route_segments stored in the db.
    with sqlite3.connect(the_scenario.main_db) as db_con:

        db_cur = db_con.cursor()

        # get a list of the source_ids and convert to modes:
        sql = "select distinct network_source_id from optimal_route_segments;"
        db_cur = db_con.execute(sql)
        rows = db_cur.fetchall()

        mode_source_list = []
        for row in rows:
            mode_source_list.append(row[0])

        logger.debug("List of modes used in the optimal solution: {}".format(mode_source_list))

    # iterate through the segment_dict_by_source_name
    # and get the segments to insert into the optimal segments layer
    # ---------------------------------------------------------------------

    optimized_route_seg_flds = ("SHAPE@", "FTOT_RT_ID", "FTOT_RT_ID_VARIANT", "ROUTE_TYPE", "NET_SOURCE_NAME",
     "NET_SOURCE_OID", "FromPosition", "FromJunctionID", "LENGTH", "TIME_PERIOD", "COMMODITY", "COMMODITY_FLOW",
     "VOLUME", "CAPACITY", "CAPACITY_MINUS_VOLUME", "UNITS", "PHASE_OF_MATTER", "ARTIFICIAL", "LINK_ROUTING_COST", "LINK_TRANSPORT_COST")

    for network_source in mode_source_list:
        if network_source == 'intermodal':
            logger.debug("network_source is: {}. can't flatten this, so skipping.".format(network_source))
            continue

        network_link_counter = 0
        network_link_coverage_counter = 0
        network_source_fc = os.path.join(the_scenario.main_gdb, "network", network_source)

        sql = """select DISTINCT
                 scenario_rt_id,
                 NULL,
                 network_source_id,
                 network_source_oid,
                 NULL,
                 NULL,
                 time_period,
                 commodity_name,
                 commodity_flow,
                 volume,
                 capacity,
                 capacity_minus_volume,
                 units,
                 phase_of_matter,
                 length,
                 route_type,
                 artificial,
                 link_transport_cost,
                 link_routing_cost
                 from optimal_route_segments
                 where network_source_id = '{}'
                 ;""".format(network_source)

        logger.debug("starting the execute for the {} mode".format(network_source))
        db_cur = db_con.execute(sql)
        logger.debug("done with the execute")
        logger.debug("starting  fetch all for the {} mode".format(network_source))
        rows = db_cur.fetchall()
        logger.debug("done with the fetchall")

        optimal_route_segments_dict = {}

        logger.debug("starting to build the dict for {}".format(network_source))
        for row in rows:
            if row[3] not in optimal_route_segments_dict:
                optimal_route_segments_dict[row[3]] = []
                network_link_coverage_counter += 1

            route_id            = row[0]
            route_id_variant    = row[1]
            network_source_id   = row[2]
            network_object_id   = row[3]
            from_position       = row[4]
            from_junction_id    = row[5]
            time_period         = row[6]
            commodity           = row[7]
            commodity_flow      = row[8]
            volume              = row[9]
            capacity            = row[10]
            capacity_minus_volume= row[11]
            units               = row[12]
            phase_of_matter     = row[13]
            length              = row[14]
            route_type          = row[15]
            artificial          = row[16]
            link_transport_cost    = row[17]
            link_routing_cost   = row[18]

            optimal_route_segments_dict[row[3]].append([route_id, route_id_variant, network_source_id,
                                                        network_object_id, from_position, from_junction_id, time_period,
                                                        commodity, commodity_flow, volume, capacity,
                                                        capacity_minus_volume, units, phase_of_matter, length,
                                                        route_type, artificial, link_transport_cost, link_routing_cost])

        logger.debug("done building the dict for {}".format(network_source))

        logger.debug("starting the search cursor")
        with arcpy.da.SearchCursor(network_source_fc, ["OBJECTID", "SHAPE@"]) as search_cursor:
            logger.info("start: looping through the {} mode".format(network_source))
            for row in search_cursor:
                network_link_counter += 1
                object_id = row[0]
                geom = row[1]

                if object_id not in optimal_route_segments_dict:
                    continue
                else:
                    for segment_info in optimal_route_segments_dict[object_id]:
                        (route_id, route_id_variant, network_source_id, network_object_id, from_position,
                         from_junction_id, time_period, commodity, commodity_flow, volume, capacity, capacity_minus_volume,
                         units, phase_of_matter, length, route_type, artificial, link_transport_cost, link_routing_cost) = segment_info
                        with arcpy.da.InsertCursor(optimized_route_segments_fc, optimized_route_seg_flds) as insert_cursor:
                            insert_cursor.insertRow([geom, route_id, route_id_variant, route_type, network_source_id,
                                                     network_object_id, from_position, from_junction_id,
                                                     length, time_period, commodity, commodity_flow, volume, capacity,
                                                     capacity_minus_volume, units, phase_of_matter, artificial,
                                                     link_routing_cost, link_transport_cost])

        logger.debug("finish: looping through the {} mode".format(network_source))
        logger.info("mode: {} coverage: {:,.1f} - total links: {} , total links used: {}".format(network_source,
                                                                                              100.0*(int(network_link_coverage_counter)/float(network_link_counter)), network_link_counter, network_link_coverage_counter ))


# ======================================================================================================================


def add_link_type_and_urban_rural(the_scenario, logger):

    logger.info("starting add_link_type_and_urban_rural")
    scenario_gdb = the_scenario.main_gdb
    optimized_route_segments_fc = os.path.join(scenario_gdb, "optimized_route_segments")

    # build up dictionary network links we are interested in
    # ----------------------------------------------------

    network_source_oid_dict = {}
    network_source_oid_dict['road'] = {}

    with arcpy.da.SearchCursor(optimized_route_segments_fc, ("NET_SOURCE_NAME", "NET_SOURCE_OID")) as search_cursor:
        for row in search_cursor:
            if row[0] == 'road':
                network_source_oid_dict['road'][row[1]] = True

    # iterate different network layers (i.e. rail, road, water, pipeline)
    # and get needed info into the network_source_oid_dict dict
    # -----------------------------------------------------------

    road_fc = os.path.join(scenario_gdb, "network", "road")
    flds = ("OID@", "Link_Type", "Urban_Rural", "Limited_Access")
    with arcpy.da.SearchCursor(road_fc, flds) as search_cursor:
        for row in search_cursor:
            if row[0] in network_source_oid_dict['road']:
                network_source_oid_dict['road'][row[0]] = [row[1], row[2], row[3]]

    # add fields to hold data from network links
    # ------------------------------------------

    # FROM ROAD NET ONLY
    arcpy.AddField_management(optimized_route_segments_fc, "Link_Type", "Text")
    arcpy.AddField_management(optimized_route_segments_fc, "Urban_Rural", "Short")
    arcpy.AddField_management(optimized_route_segments_fc, "Limited_Access", "Short")

    # set network link related values in optimized_route_segments layer
    # -----------------------------------------------------------------

    flds = ["NET_SOURCE_NAME", "NET_SOURCE_OID", "Link_Type", "Urban_Rural", "Limited_Access"]

    # initialize a list for the update

    update_list = []
    net_oid_seen = {}

    with arcpy.da.UpdateCursor(optimized_route_segments_fc, flds) as update_cursor:

        for row in update_cursor:

            net_source = row[0]
            net_oid = row[1]

            if net_source == 'road':
                data = network_source_oid_dict[net_source][net_oid]
                row[2] = data[0]  # link_type
                row[3] = data[1]  # urban_rural
                row[4] = data[2]  # limited_access                

                update_cursor.updateRow(row)

                # Just adding the link_type urban_rural and limited_access to the list once per net_source_oid
                if net_oid not in net_oid_seen:
                    net_oid_seen[net_oid] = 0
                    update_list.append([row[2], row[3], row[4], net_source, net_oid])
                net_oid_seen[net_oid] += 1

    logger.info("starting the execute many to update the list of optimal_route_segments")
    with sqlite3.connect(the_scenario.main_db) as db_con:
        db_con.execute("CREATE INDEX if not exists network_index ON optimal_route_segments(network_source_id, network_source_oid, commodity_name, artificial)")
        update_sql = """
            UPDATE optimal_route_segments
            set link_type = ?, urban_rural = ?, limited_access = ?
            where network_source_id = ? and network_source_oid = ?
            ;"""

        db_con.executemany(update_sql, update_list)
        db_con.commit()


# ======================================================================================================================


def dissolve_optimal_route_segments_feature_class_for_mapping(the_scenario, logger):

    # Make a dissolved version of fc for mapping aggregate flows
    logger.info("starting dissolve_optimal_route_segments_feature_class_for_mapping")

    scenario_gdb = the_scenario.main_gdb
    optimized_route_segments_fc = os.path.join(scenario_gdb, "optimized_route_segments")

    arcpy.env.workspace = scenario_gdb

    if arcpy.Exists("optimized_route_segments_dissolved"):
        arcpy.Delete_management("optimized_route_segments_dissolved")

    arcpy.MakeFeatureLayer_management(optimized_route_segments_fc, "segments_lyr")
    result = arcpy.GetCount_management("segments_lyr")
    count = str(result.getOutput(0))

    if arcpy.Exists("optimized_route_segments_dissolved_tmp"):
        arcpy.Delete_management("optimized_route_segments_dissolved_tmp")

    if arcpy.Exists("optimized_route_segments_split_tmp"):
        arcpy.Delete_management("optimized_route_segments_split_tmp")

    if arcpy.Exists("optimized_route_segments_dissolved_tmp2"):
        arcpy.Delete_management("optimized_route_segments_dissolved_tmp2")

    if int(count) > 0:

        # Dissolve
        arcpy.Dissolve_management(optimized_route_segments_fc, "optimized_route_segments_dissolved_tmp",
                                  ["NET_SOURCE_NAME", "NET_SOURCE_OID", "ARTIFICIAL", "PHASE_OF_MATTER", "UNITS"],
                                  [['COMMODITY_FLOW', 'SUM']], "SINGLE_PART", "DISSOLVE_LINES")

        if arcpy.ProductInfo() == "ArcInfo":
            # Second dissolve needed to accurately show aggregate pipeline flows
            arcpy.FeatureToLine_management("optimized_route_segments_dissolved_tmp",
                                           "optimized_route_segments_split_tmp")

            arcpy.AddGeometryAttributes_management("optimized_route_segments_split_tmp", "LINE_START_MID_END")

            arcpy.Dissolve_management("optimized_route_segments_split_tmp", "optimized_route_segments_dissolved_tmp2",
                                      ["NET_SOURCE_NAME", "Shape_Length", "MID_X", "MID_Y", "ARTIFICIAL",
                                       "PHASE_OF_MATTER", "UNITS"],
                                      [["SUM_COMMODITY_FLOW", "SUM"]], "SINGLE_PART", "DISSOLVE_LINES")

            arcpy.AddField_management("optimized_route_segments_dissolved_tmp2", "SUM_COMMODITY_FLOW", "DOUBLE")
            arcpy.CalculateField_management("optimized_route_segments_dissolved_tmp2", "SUM_COMMODITY_FLOW",
                                            "!SUM_SUM_COMMODITY_FLOW!", "PYTHON_9.3")
            arcpy.DeleteField_management("optimized_route_segments_dissolved_tmp2", "SUM_SUM_COMMODITY_FLOW")
            arcpy.DeleteField_management("optimized_route_segments_dissolved_tmp2", "MID_X")
            arcpy.DeleteField_management("optimized_route_segments_dissolved_tmp2", "MID_Y")

        else:
            # Doing it differently because feature to line isn't available without an advanced license
            logger.warning("The Advanced/ArcInfo license level of ArcGIS Pro is not available. A modification to the "
                           "dissolve_optimal_route_segments_feature_class_for_mapping method is being automatically run.")

            scenario_proj = ftot_supporting_gis.get_coordinate_system(the_scenario)
            # Create the fc
            arcpy.CreateFeatureclass_management(the_scenario.main_gdb, "optimized_route_segments_split_tmp",
                                                "POLYLINE", "#", "DISABLED", "DISABLED", scenario_proj)

            arcpy.AddField_management("optimized_route_segments_split_tmp", "NET_SOURCE_NAME", "TEXT")
            arcpy.AddField_management("optimized_route_segments_split_tmp", "NET_SOURCE_OID", "LONG")
            arcpy.AddField_management("optimized_route_segments_split_tmp", "ARTIFICIAL", "SHORT")
            arcpy.AddField_management("optimized_route_segments_split_tmp", "UNITS", "TEXT")
            arcpy.AddField_management("optimized_route_segments_split_tmp", "PHASE_OF_MATTER", "TEXT")
            arcpy.AddField_management("optimized_route_segments_split_tmp", "SUM_COMMODITY_FLOW", "DOUBLE")

            # Go through the pipeline segments separately
            tariff_segment_dict = defaultdict(float)
            with arcpy.da.SearchCursor("optimized_route_segments_dissolved_tmp",
                                       ["NET_SOURCE_NAME", "NET_SOURCE_OID", "ARTIFICIAL", "PHASE_OF_MATTER", "UNITS",
                                        "SUM_COMMODITY_FLOW", "SHAPE@"]) as search_cursor:
                for row1 in search_cursor:
                    if 'pipeline' in row1[0]:
                        # Must not be artificial, otherwise pass the link through
                        if row1[2] == 0:
                            # Capture the tariff ID so that we can link to the segments
                            mode = row1[0]
                            with arcpy.da.SearchCursor(mode, ["OBJECTID", "Tariff_ID", "SHAPE@"]) \
                                    as search_cursor_2:
                                for row2 in search_cursor_2:
                                    if row1[1] == row2[0]:
                                        tariff_id = row2[1]
                            mode = row1[0].strip("rts")
                            with arcpy.da.SearchCursor(mode + "sgmts", ["MASTER_OID", "Tariff_ID", "SHAPE@"]) \
                                    as search_cursor_3:
                                for row3 in search_cursor_3:
                                    if tariff_id == row3[1]:
                                        # keying off master_oid, net_source_name, phase of matter, units + shape
                                        tariff_segment_dict[(row3[0], row1[0], row1[3], row1[4], row3[2])] += row1[5]
                        else:
                            with arcpy.da.InsertCursor("optimized_route_segments_split_tmp",
                                                       ["NET_SOURCE_NAME", "NET_SOURCE_OID", "ARTIFICIAL",
                                                        "PHASE_OF_MATTER", "UNITS", "SUM_COMMODITY_FLOW", "SHAPE@"]) \
                                    as insert_cursor:
                                insert_cursor.insertRow([row1[0], row1[1], row1[2], row1[3], row1[4], row1[5], row1[6]])
                    # If it isn't pipeline just pass the data through.
                    else:
                        with arcpy.da.InsertCursor("optimized_route_segments_split_tmp",
                                                   ["NET_SOURCE_NAME", "NET_SOURCE_OID", "ARTIFICIAL",
                                                    "PHASE_OF_MATTER", "UNITS", "SUM_COMMODITY_FLOW", "SHAPE@"])\
                                as insert_cursor:
                            insert_cursor.insertRow([row1[0], row1[1], row1[2], row1[3], row1[4], row1[5], row1[6]])

            # Now that pipeline segment dictionary is built, get the pipeline segments in there as well
            for master_oid, net_source_name, phase_of_matter, units, shape in tariff_segment_dict:
                commodity_flow = tariff_segment_dict[master_oid, net_source_name, phase_of_matter, units, shape]
                with arcpy.da.InsertCursor("optimized_route_segments_split_tmp",
                                           ["NET_SOURCE_NAME", "NET_SOURCE_OID", "ARTIFICIAL",
                                            "PHASE_OF_MATTER", "UNITS", "SUM_COMMODITY_FLOW", "SHAPE@"]) \
                        as insert_cursor:
                    insert_cursor.insertRow([net_source_name, master_oid, 0, phase_of_matter, units, commodity_flow,
                                             shape])
            # No need for dissolve because dictionaries have already summed flows
            arcpy.Copy_management("optimized_route_segments_split_tmp", "optimized_route_segments_dissolved_tmp2")

        # Sort for mapping order
        arcpy.AddField_management("optimized_route_segments_dissolved_tmp2", "SORT_FIELD", "SHORT")
        arcpy.MakeFeatureLayer_management("optimized_route_segments_dissolved_tmp2", "dissolved_segments_lyr")
        arcpy.SelectLayerByAttribute_management("dissolved_segments_lyr", "NEW_SELECTION", "NET_SOURCE_NAME = 'road'")
        arcpy.CalculateField_management("dissolved_segments_lyr", "SORT_FIELD", 1, "PYTHON_9.3")
        arcpy.SelectLayerByAttribute_management("dissolved_segments_lyr", "NEW_SELECTION", "NET_SOURCE_NAME = 'rail'")
        arcpy.CalculateField_management("dissolved_segments_lyr", "SORT_FIELD", 2, "PYTHON_9.3")
        arcpy.SelectLayerByAttribute_management("dissolved_segments_lyr", "NEW_SELECTION", "NET_SOURCE_NAME = 'water'")
        arcpy.CalculateField_management("dissolved_segments_lyr", "SORT_FIELD", 3, "PYTHON_9.3")
        arcpy.SelectLayerByAttribute_management("dissolved_segments_lyr", "NEW_SELECTION",
                                                "NET_SOURCE_NAME LIKE 'pipeline%'")
        arcpy.CalculateField_management("dissolved_segments_lyr", "SORT_FIELD", 4, "PYTHON_9.3")

        arcpy.Sort_management("optimized_route_segments_dissolved_tmp2", "optimized_route_segments_dissolved",
                              [["SORT_FIELD", "ASCENDING"]])

        # Delete temp fc's
        arcpy.Delete_management("optimized_route_segments_dissolved_tmp")
        arcpy.Delete_management("optimized_route_segments_split_tmp")
        arcpy.Delete_management("optimized_route_segments_dissolved_tmp2")

    else:
        scenario_proj = ftot_supporting_gis.get_coordinate_system(the_scenario)
        arcpy.CreateFeatureclass_management(the_scenario.main_gdb, "optimized_route_segments_dissolved",
                                            "POLYLINE", "#", "DISABLED", "DISABLED", scenario_proj, "#",
                                            "0", "0", "0")

        arcpy.AddField_management("optimized_route_segments_dissolved", "NET_SOURCE_NAME", "TEXT")
        arcpy.AddField_management("optimized_route_segments_dissolved", "ARTIFICIAL", "SHORT")
        arcpy.AddField_management("optimized_route_segments_dissolved", "UNITS", "TEXT")
        arcpy.AddField_management("optimized_route_segments_dissolved", "PHASE_OF_MATTER", "TEXT")
        arcpy.AddField_management("optimized_route_segments_dissolved", "SUM_COMMODITY_FLOW", "DOUBLE")

    arcpy.Delete_management("segments_lyr")

