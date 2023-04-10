
#---------------------------------------------------------------------------------------------------
# Name: aftot_pulp_sasc
#
# Purpose: PulP optimization  - source facility as subcommodity variant
#
#---------------------------------------------------------------------------------------------------

import os
import sys

import ftot_supporting
import ftot_pulp
import sqlite3
import re

import logging
import datetime

from ftot import ureg, Q_

from collections import defaultdict

from pulp import *
from ftot_supporting import get_total_runtime_string
#import ftot_supporting_gis
from six import iteritems

#=================== constants=============
storage = 1
primary = 0
fixed_route_max_daily_capacity = 10000000000
fixed_route_min_daily_capacity = 0
fixed_schedule_id = 2
fixed_route_duration = 0
THOUSAND_GALLONS_PER_THOUSAND_BARRELS = 42


candidate_processing_facilities = []

storage_cost_1 = 0.01
storage_cost_2 = 0.05
facility_onsite_storage_max = 10000000000
facility_onsite_storage_min = 0
storage_cost_1 = .01
storage_cost_2 = .05
fixed_route_max_daily_capacity = 10000000000
fixed_route_min_daily_capacity = 0
default_max_capacity = 10000000000
default_min_capacity = 0

#at this level, duplicate edges (i.e. same nx_edge_id, same subcommodity) are only created if they are lower cost.
duplicate_edge_cap = 1

#while loop cap - only used for dev to keep runtime reasonable in testing
while_loop_cap = 40
#debug only
max_transport_distance = 50


def o_sourcing(the_scenario, logger):
    import ftot_pulp
    pre_setup_pulp_from_optimal(logger, the_scenario)
    prob = setup_pulp_problem(the_scenario, logger)
    prob = solve_pulp_problem(prob, the_scenario, logger)
    save_pulp_solution(the_scenario, prob, logger)

    from ftot_supporting import post_optimization
    post_optimization(the_scenario, 'os', logger)


#===============================================================================


def delete_all_global_dictionaries():
    global processing_facilities
    global processor_storage_vertices
    global supply_storage_vertices
    global demand_storage_vertices
    global processor_vertices
    global supply_vertices
    global demand_vertices
    global storage_facility_vertices
    global connector_edges
    global storage_edges
    global transport_edges


    processing_facilities = []
    processor_storage_vertices = {}
    supply_storage_vertices = {}
    demand_storage_vertices = {}
    # storage_facility_storage_vertices = {}
    processor_vertices = {}
    supply_vertices = {}
    demand_vertices = {}
    storage_facility_vertices = {}
    connector_edges = {}
    storage_edges = {}
    transport_edges = {}
    fuel_subtypes = {}


def save_existing_solution(logger, the_scenario):
    logger.info("START: save_existing_solution")
    with sqlite3.connect(the_scenario.main_db) as main_db_con:

        logger.debug("save the current optimal_variables table as optimal_variables_prior")
        main_db_con.executescript("""
        drop table if exists optimal_variables_prior;

        create table if not exists optimal_variables_prior as select * from optimal_variables;

        drop table if exists optimal_variables;
        """)

        logger.debug("save the current vertices table as vertices_prior and create the new vertices table")

        main_db_con.executescript(
        """
        drop table if exists vertices_prior;

        create table if not exists vertices_prior as select * from vertices;
        """)


#===============================================================================
def source_as_subcommodity_setup(the_scenario, logger):
    logger.info("START: source_as_subcommodity_setup")
    multi_commodity_name = "multicommodity"
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        main_db_con.executescript("""
            drop table if exists subcommodity
            ;

            create table subcommodity(sub_id INTEGER PRIMARY KEY,
            source_facility_id integer,
            source_facility_name text,
            commodity_id integer,
            commodity_name text,
            units text,
            phase_of_matter text,
            max_transport_distance text,
            CONSTRAINT unique_source_and_name UNIQUE(commodity_name, source_facility_id))
            ;


            insert into subcommodity (
            source_facility_id,
            source_facility_name,
            commodity_id,
            commodity_name,
            units,
            phase_of_matter,
            max_transport_distance)

            select
            f.facility_id,
            f.facility_name,
            c.commodity_id,
            c.commodity_name,
            c.units,
            c.phase_of_matter,
            c.max_transport_distance

            from commodities c, facilities f, facility_type_id ft
            where f.facility_type_id = ft.facility_type_id
            and ft.facility_type = 'raw_material_producer'
            ;"""
            )
        main_db_con.execute("""insert or ignore into commodities(commodity_name) values ('{}');""".format(multi_commodity_name))
        main_db_con.execute("""insert or ignore into subcommodity(source_facility_id, source_facility_name, commodity_name, commodity_id)
                                      select sc.source_facility_id, sc.source_facility_name, c.commodity_name, c.commodity_id
                                      from subcommodity sc, commodities c
                                      where c.commodity_name = '{}'
                                      """.format(multi_commodity_name))
    return
#===============================================================================

def generate_all_vertices_from_optimal_for_sasc(the_scenario, schedule_dict, schedule_length, logger):
    logger.info("START: generate_all_vertices_from_optimal_for_sasc")
    #this should be run instead of generate_all_vertices_table, not in addition

    total_potential_production = {}
    multi_commodity_name = "multicommodity"
    #dictionary items should have the form (vertex, edge_names_in, edge_names_out)

    storage_availability = 1

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        main_db_con.executescript(
            """
        drop table if exists vertices;
        
        create table if not exists vertices (
        vertex_id INTEGER PRIMARY KEY, location_id,
        facility_id integer, facility_name text, facility_type_id integer, schedule_day integer,
        commodity_id integer, activity_level numeric, storage_vertex binary,
        udp numeric, supply numeric, demand numeric,
        subcommodity_id integer,
        source_facility_id integer,
        CONSTRAINT unique_vertex UNIQUE(facility_id, schedule_day, subcommodity_id, storage_vertex));

        insert or ignore into commodities(commodity_name) values ('{}');""".format(multi_commodity_name))

        main_db_con.execute("""insert or ignore into subcommodity(source_facility_id, source_facility_name, commodity_name, commodity_id)
                                      select sc.source_facility_id, sc.source_facility_name, c.commodity_name, c.commodity_id
                                      from subcommodity sc, commodities c
                                      where c.commodity_name = '{}'
                                      """.format(multi_commodity_name))

        #for all facilities that are used in the optimal solution, retrieve facility and facility_commodity information
        #each row is a facility_commodity entry then, will get at least 1 vertex; more for days and storage, less for processors?

        #for raw_material_suppliers
        #--------------------------------


        db_cur = main_db_con.cursor()
        db_cur4 = main_db_con.cursor()
        counter = 0
        for row in db_cur.execute("select count(distinct facility_id) from  facilities;"):
            total_facilities = row[0]

        #create vertices for each facility
        # facility_type can be "raw_material_producer", "ultimate_destination","processor"; get id from original vertices table for sasc
        # use UNION (? check for sqlite) to get a non-repeat list of facilities

        for row_a in db_cur.execute("""select f.facility_id, facility_type, facility_name, location_id, f.facility_type_id, schedule_id
        from facilities f, facility_type_id ft, optimal_variables_prior ov
        where ignore_facility = '{}'
        and f.facility_type_id = ft.facility_type_id
        and f.facility_name = ov.o_facility
        union
        select f.facility_id, facility_type, facility_name, location_id, f.facility_type_id
        from facilities f, facility_type_id ft, optimal_variables_prior ov
        where ignore_facility = '{}'
        and f.facility_type_id = ft.facility_type_id
        and f.facility_name = ov.d_facility
         ;""".format('false', 'false')):
            db_cur2 = main_db_con.cursor()
            facility_id = row_a[0]
            facility_type = row_a[1]
            facility_name = row_a[2]
            facility_location_id = row_a[3]
            facility_type_id = row_a[4]
            schedule_id = row_a[5]
            if counter % 10000 == 0:
               logger.info("vertices created for {} facilities of {}".format(counter, total_facilities))
               for row_d in db_cur4.execute("select count(distinct vertex_id) from vertices;"):
                   logger.info('{} vertices created'.format(row_d[0]))
            counter = counter + 1


            if facility_type == "processor":

               #each processor facility should have 1 input commodity with 1 storage vertex, 1 or more output commodities each with 1 storage vertex, and 1 primary processing vertex
               #explode by time, create primary vertex, explode by commodity to create storage vertices; can also create primary vertex for input commodity
               #do this for each subcommodity now instead of each commodity
               for row_b in db_cur2.execute("""select fc.commodity_id,
                   ifnull(fc.quantity, 0),
                   fc.units,
                   ifnull(c.supertype, c.commodity_name),
                   fc.io,
                   mc.commodity_id,
                   c.commodity_name,
                   s.sub_id,
                   s.source_facility_id
                   from facility_commodities fc, commodities c, commodities mc, subcommodity s
                   where fc.facility_id = {}
                   and fc.commodity_id = c.commodity_id
                   and mc.commodity_name = '{}'
                   and s.commodity_id = c.commodity_id;""".format(facility_id, multi_commodity_name)):

                   commodity_id = row_b[0]
                   quantity = row_b[1]
                   units = row_b[2]
                   commodity_supertype = row_b[3]
                   io = row_b[4]
                   id_for_mult_commodities = row_b[5]
                   commodity_name = row_b[6]
                   subcommodity_id = row_b[7]
                   source_facility_id = row_b[8]
                   #vertices for generic demand type, or any subtype specified by the destination
                   for day_before, availability in enumerate(schedule_dict[schedule_id]):
                         if io == 'i':
                            main_db_con.execute("""insert or ignore into vertices (
                            location_id, facility_id, facility_type_id, facility_name, schedule_day, commodity_id, activity_level, storage_vertex,
                            subcommodity_id, source_facility_id)
                            values ({}, {}, {}, '{}', {}, {}, {}, {},
                            {},{}
                            );""".format(facility_location_id, facility_id, facility_type_id, facility_name, day_before+1, id_for_mult_commodities, availability, primary,
                            subcommodity_id, source_facility_id))

                            main_db_con.execute("""insert or ignore into vertices (
                            location_id, facility_id, facility_type_id, facility_name, schedule_day, commodity_id, activity_level, storage_vertex, demand,
                            subcommodity_id, source_facility_id)
                            values ({}, {}, {}, '{}', {}, {}, {}, {}, {},
                            {}, {});""".format(facility_location_id, facility_id, facility_type_id, facility_name, day_before+1, commodity_id, storage_availability, storage, quantity,
                            subcommodity_id, source_facility_id))

                         else:
                              if commodity_name != 'total_fuel':
                                 main_db_con.execute("""insert or ignore into vertices (
                                 location_id, facility_id, facility_type_id, facility_name, schedule_day, commodity_id, activity_level, storage_vertex, supply, subcommodity_id, source_facility_id)
                                 values ({}, {}, {}, '{}', {}, {}, {}, {}, {}, {}, {});""".format(facility_location_id, facility_id, facility_type_id, facility_name, day_before+1, commodity_id, storage_availability, storage, quantity, subcommodity_id, source_facility_id))

            elif facility_type == "raw_material_producer":     #raw material producer

            # handle as such, exploding by time and commodity
                for row_b in db_cur2.execute("""select fc.commodity_id, fc.quantity, fc.units, s.sub_id, s.source_facility_id
                from facility_commodities fc, subcommodity s
                where fc.facility_id = {}
                and s.source_facility_id = {}
                and fc.commodity_id = s.commodity_id;""".format(facility_id, facility_id)):
                    commodity_id = row_b[0]
                    quantity = row_b[1]
                    units = row_b[2]
                    subcommodity_id = row_b[3]
                    source_facility_id = row_b[4]

                    if commodity_id in total_potential_production:
                        total_potential_production[commodity_id] =  total_potential_production[commodity_id] + quantity
                    else:
                        total_potential_production[commodity_id] =  quantity
                    for day_before, availability in enumerate(schedule_dict[schedule_id]):
                        main_db_con.execute("""insert or ignore into vertices (
                        location_id, facility_id, facility_type_id, facility_name, schedule_day, commodity_id, activity_level, storage_vertex, supply,
                            subcommodity_id, source_facility_id)
                        values ({}, {}, {}, '{}', {}, {}, {}, {}, {},
                        {},{});""".format(facility_location_id, facility_id, facility_type_id, facility_name, day_before+1, commodity_id, availability, primary, quantity,
                            subcommodity_id, source_facility_id))
                        main_db_con.execute("""insert or ignore into vertices (
                        location_id, facility_id, facility_type_id, facility_name, schedule_day, commodity_id, activity_level, storage_vertex, supply,
                            subcommodity_id, source_facility_id)
                        values ({}, {}, {}, '{}', {}, {}, {}, {}, {},
                        {},{});""".format(facility_location_id, facility_id, facility_type_id, facility_name, day_before+1, commodity_id, storage_availability, storage, quantity,
                            subcommodity_id, source_facility_id))

            elif facility_type == "storage":     #storage facility
                # handle as such, exploding by time and commodity
                for row_b in db_cur2.execute("""select commodity_id, quantity, units, s.sub_id, s.source_facility_id
                from facility_commodities fc, subcommodity s
                where fc.facility_id = {}
                and fc.commodity_id = s.commodity_id;""".format(facility_id)):
                    commodity_id = row_b[0]
                    quantity = row_b[1]
                    units = row_b[2]
                    subcommodity_id = row_b[3]
                    source_facility_id = row_b[4]
                    for day_before, availability in enumerate(schedule_dict[schedule_id]):
                       main_db_con.execute("""insert or ignore into vertices (
                       location_id, facility_id, facility_type_id, facility_name, schedule_day, commodity_id, storage_vertex,
                            subcommodity_id, source_facility_id)
                       values ({}, {}, {}, '{}', {}, {}, {},
                       {},{});""".format(facility_location_id, facility_id, facility_type_id, facility_name, day_before+1, commodity_id, storage,
                            subcommodity_id, source_facility_id))

            elif facility_type == "ultimate_destination":               #ultimate_destination
                # handle as such, exploding by time and commodity
                for row_b in db_cur2.execute("""select fc.commodity_id, ifnull(fc.quantity, 0), fc.units,
                fc.commodity_id, ifnull(c.supertype, c.commodity_name), s.sub_id, s.source_facility_id
                from facility_commodities fc, commodities c, subcommodity s
                where fc.facility_id = {}
                and fc.commodity_id = c.commodity_id
                and fc.commodity_id = s.commodity_id;""".format(facility_id)):
                    commodity_id = row_b[0]
                    quantity = row_b[1]
                    units = row_b[2]
                    commodity_id = row_b[3]
                    commodity_supertype = row_b[4]
                    subcommodity_id = row_b[5]
                    source_facility_id = row_b[6]

                    #vertices for generic demand type, or any subtype specified by the destination
                    for day_before, availability in enumerate(schedule_dict[schedule_id]):
                       main_db_con.execute("""insert or ignore into vertices (
                       location_id, facility_id, facility_type_id, facility_name, schedule_day, commodity_id, activity_level, storage_vertex, demand, udp,
                            subcommodity_id, source_facility_id)
                       values ({}, {}, {}, '{}', {},
                        {}, {}, {}, {}, {},
                        {},{});""".format(facility_location_id, facility_id, facility_type_id, facility_name, day_before+1,
                       commodity_id, availability, primary, quantity, the_scenario.unMetDemandPenalty,
                            subcommodity_id, source_facility_id))
                       main_db_con.execute("""insert or ignore into vertices (
                       location_id, facility_id, facility_type_id, facility_name, schedule_day, commodity_id, activity_level, storage_vertex, demand,
                            subcommodity_id, source_facility_id)
                       values ({}, {}, {}, '{}', {}, {}, {}, {}, {},
                       {},{});""".format(facility_location_id, facility_id, facility_type_id, facility_name, day_before+1, commodity_id, storage_availability, storage, quantity,
                            subcommodity_id, source_facility_id))

                    #vertices for other fuel subtypes that match the destination's supertype
                    #if the subtype is in the commodity table, it is produced by some facility (not ignored) in the scenario
                    db_cur3 = main_db_con.cursor()
                    for row_c in db_cur3.execute("""select commodity_id, units from commodities
                    where supertype = '{}';""".format(commodity_supertype)):
                        new_commodity_id = row_c[0]
                        new_units= row_c[1]
                        for day_before, availability in enumerate(schedule_dict[schedule_id]):
                              main_db_con.execute("""insert or ignore into vertices (
                              location_id, facility_id, facility_type_id, facility_name, schedule_day, commodity_id, activity_level, storage_vertex, demand, udp,
                            subcommodity_id, source_facility_id)
                              values ({}, {}, {}, '{}', {}, {}, {}, {}, {}, {},
                              {},{});""".format(facility_location_id, facility_id, facility_type_id, facility_name, day_before+1, new_commodity_id, availability, primary, quantity, the_scenario.unMetDemandPenalty,
                            subcommodity_id, source_facility_id))
                              main_db_con.execute("""insert or ignore into vertices (
                              location_id, facility_id, facility_type_id, facility_name, schedule_day, commodity_id, activity_level, storage_vertex, demand,
                            subcommodity_id, source_facility_id)
                              values ({}, {}, {}, '{}', {}, {}, {}, {}, {},
                              {},{});""".format(facility_location_id, facility_id, facility_type_id, facility_name, day_before+1, new_commodity_id, storage_availability, storage, quantity,
                            subcommodity_id, source_facility_id))


            else:
                logger.warning("error, unexpected facility_type: {}, facility_type_id: {}".format(facility_type, facility_type_id))

            # if it's an origin/supply facility, explode by commodity and time
            # if it's a destination/demand facility, explode by commodity and time
            # if it's an independent storage facility, explode by commodity and time
            # if it's a processing/refining facility, only explode by time - all commodities on the product slate must
            # enter and exit the vertex

    #add destination storage vertices for all demand subtypes after all facilities have been iterated, so that we know what fuel subtypes are in this scenario


    logger.debug("total possible production in scenario: {}".format(total_potential_production))

#===============================================================================

def add_storage_routes(the_scenario, logger):
    # these are loops to and from the same facility; when exploded to edges, they will connect primary to storage vertices, and storage vertices day to day
    # is one enough? how many edges will be created per route here?
    # will always create edge for this route from storage to storage vertex
    # will always create edges for extra routes connecting here
    # IF a primary vertex exists, will also create an edge connecting the storage vertex to the primary


    with sqlite3.connect(the_scenario.main_db) as main_db_con:

        logger.debug("create the storage_routes table")

        main_db_con.execute("drop table if exists storage_routes;")
        main_db_con.execute("""create table if not exists storage_routes as
        select facility_name || '_storage' as route_name,
        location_id,
        facility_id,
        facility_name as o_name,
        facility_name as d_name,
        {} as cost_1,
        {} as cost_2,
        1 as travel_time,
        {} as storage_max,
        0 as storage_min
        from facilities
        where ignore_facility = 'false'
        ;""".format(storage_cost_1, storage_cost_2, facility_onsite_storage_max))
        main_db_con.execute("""create table if not exists route_reference(
        route_id INTEGER PRIMARY KEY, route_type text, route_name text, scenario_rt_id integer,
        CONSTRAINT unique_routes UNIQUE(route_type, route_name, scenario_rt_id));""")
        #main_db_con.execute("insert or ignore into route_reference select scenario_rt_id, 'transport', 'see scenario_rt_id', scenario_rt_id from routes;")
        main_db_con.execute("insert or ignore into route_reference select null,'storage', route_name, 0 from storage_routes;")


    logger.debug("storage_routes table created")

    return

#===============================================================================
def generate_all_edges_from_optimal_for_sasc(the_scenario, schedule_length, logger):
    logger.info("START: generate_all_edges_from_optimal_for_sasc")
    #create edges table
    #plan to generate start and end days based on nx edge time to traverse and schedule
    #can still have route_id, but only for storage routes now; nullable

    multi_commodity_name = "multicommodity"

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        if ('pipeline_crude_trf_rts' in the_scenario.permittedModes) or ('pipeline_prod_trf_rts' in the_scenario.permittedModes) :
            logger.info("create indices for the capacity_nodes and pipeline_mapping tables")
            main_db_con.executescript(
            """
            CREATE INDEX IF NOT EXISTS pm_index ON pipeline_mapping (id, id_field_name, mapping_id_field_name, mapping_id);
            CREATE INDEX IF NOT EXISTS cn_index ON capacity_nodes (source, id_field_name, source_OID);
            """)
        main_db_con.executescript(
        """
        drop table if exists edges_prior;

        create table edges_prior as select * from edges;

        drop table if exists edges;

        create table edges (edge_id INTEGER PRIMARY KEY,
        route_id integer,
        from_node_id integer,
        to_node_id integer,
        start_day integer,
        end_day integer,
        commodity_id integer,
        o_vertex_id integer,
        d_vertex_id integer,
        max_edge_capacity numeric,
        volume numeric,
        capac_minus_volume_zero_floor numeric,
        min_edge_capacity numeric,
        capacity_units text,
        units_conversion_multiplier numeric,
        edge_flow_cost numeric,
        edge_flow_cost2 numeric,
        edge_type text,
        nx_edge_id integer,
        mode text,
        mode_oid integer,
        length numeric,
        simple_mode text,
        tariff_id numeric,
        phase_of_matter text,
        subcommodity_id integer,
        source_facility_id integer
       );""")



        db_cur = main_db_con.cursor()
        counter = 0
        for row in db_cur.execute("select commodity_id from commodities where commodity_name = '{}';""".format(multi_commodity_name)):
            id_for_mult_commodities = row[0]
        for row in db_cur.execute("select count(*) from networkx_edges;"):
            total_transport_routes = row[0]



        #create transport edges, only between storage vertices and nodes, based on networkx graph and optimal variables
        #never touch primary vertices; either or both vertices can be null (or node-type) if it's a mid-route link
        #iterate through nx edges: if neither node has a location, create 1 edge per viable commodity
        #should also be per day, subject to nx edge schedule
        #before creating an edge, check: commodity allowed by nx and max transport distance if not null
        #will need nodes per day and commodity? or can I just check that with constraints?
        #select data for transport edges
        for row_a in db_cur.execute("""select
        ne.edge_id,
        ifnull(fn.location_id, 'NULL'),
        ifnull(tn.location_id, 'NULL'),
        ne.mode_source,
        ifnull(nec.phase_of_matter_id, 'NULL'),
        nec.route_cost,
        ne.from_node_id,
        ne.to_node_id,
        nec.transport_cost,
        ne.length,
        ne.capacity,
        ne.artificial,
        ne.mode_source_oid
        from networkx_edges ne, networkx_nodes fn, networkx_nodes tn, networkx_edge_costs nec, optimal_variables_prior ov

        where ne.from_node_id = fn.node_id
        and ne.to_node_id = tn.node_id
        and ne.edge_id = nec.edge_id
        and ifnull(ne.capacity, 1) > 0
        and ne.edge_id = ov.nx_edge_id
        ;"""):

            nx_edge_id = row_a[0]
            from_location = row_a[1]
            to_location = row_a[2]
            mode = row_a[3]
            phase_of_matter = row_a[4]
            route_cost = row_a[5]
            from_node = row_a[6]
            to_node = row_a[7]
            transport_cost = row_a[8]
            length = row_a[9]
            max_daily_capacity = row_a[10]
            mode_oid = row_a[12]
            simple_mode = row_a[3].partition('_')[0]


            db_cur3 = main_db_con.cursor()

            if counter % 10000 == 0:
               logger.info("edges created for {} transport links of {}".format(counter, total_transport_routes))
               for row_d in db_cur3.execute("select count(distinct edge_id) from edges;"):
                   logger.info('{} edges created'.format(row_d[0]))
            counter = counter +1

            tariff_id = 0
            if simple_mode == 'pipeline':

               #find tariff_ids

               sql="select mapping_id from pipeline_mapping where id = {} and id_field_name = 'source_OID' and source = '{}' and mapping_id is not null;".format(mode_oid, mode)
               for tariff_row in db_cur3.execute(sql):
                   tariff_id = tariff_row[0]

            if mode in the_scenario.permittedModes:

                  # Edges are placeholders for flow variables
                  # 4-17: if both ends have no location, iterate through viable commodities and days, create edge
                  # for all days (restrict by link schedule if called for)
                  # for all allowed commodities, as currently defined by link phase of matter


                for day in range(1, schedule_length+1):
                    if (day+fixed_route_duration <= schedule_length): #if link is traversable in the timeframe
                        if (simple_mode != 'pipeline' or tariff_id >= 0):
                            #for allowed commodities
                            for row_c in db_cur3.execute("select commodity_id, sub_id, source_facility_id from subcommodity where phase_of_matter = '{}' group by commodity_id, sub_id, source_facility_id".format(phase_of_matter)):
                                db_cur4 = main_db_con.cursor()
                                commodity_id = row_c[0]
                                subcommodity_id = row_c[1]
                                source_facility_id = row_c[2]
                                #if from_location != 'NULL':
                                #   logger.info("non-null from location nxedge {}, {}, checking for commodity {}".format(from_location,nx_edge_id, commodity_id))
                                if(from_location == 'NULL' and to_location == 'NULL'):
                                    main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                        start_day, end_day, commodity_id,
                                        min_edge_capacity, edge_flow_cost, edge_flow_cost2,
                                        edge_type, nx_edge_id, mode, mode_oid, length, simple_mode, tariff_id, phase_of_matter, subcommodity_id, source_facility_id) VALUES ({}, {},
                                        {}, {}, {},
                                        {}, {}, {},
                                        '{}',{},'{}',{},{},'{}',{},'{}',{},{});
                                        """.format(from_node, to_node,
                                        day, day+fixed_route_duration, commodity_id,
                                        default_min_capacity, route_cost, transport_cost,
                                        'transport', nx_edge_id, mode, mode_oid, length, simple_mode, tariff_id,phase_of_matter, subcommodity_id, source_facility_id))

                                elif(from_location != 'NULL' and to_location == 'NULL'):
                                    #for each day and commodity, get the corresponding origin vertex id to include with the edge info
                                    #origin vertex must not be "ultimate_destination
                                    #transport link outgoing from facility - checking fc.io is more thorough than checking if facility type is 'ultimate destination'
                                    #new for bsc, only connect to vertices with matching subcommodity_id (only limited for RMP vertices)
                                    for row_d in db_cur4.execute("""select vertex_id
                                    from vertices v, facility_commodities fc
                                    where v.location_id = {} and v.schedule_day = {} and v.commodity_id = {} and v.subcommodity_id = {}
                                    and v.storage_vertex = 1
                                    and v.facility_id = fc.facility_id
                                    and v.commodity_id = fc.commodity_id
                                    and fc.io = 'o'""".format(from_location, day, commodity_id, subcommodity_id)):
                                        from_vertex_id = row_d[0]
                                        main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                            start_day, end_day, commodity_id,
                                            o_vertex_id,
                                            min_edge_capacity,edge_flow_cost, edge_flow_cost2,
                                            edge_type, nx_edge_id, mode, mode_oid, length, simple_mode, tariff_id,phase_of_matter, subcommodity_id, source_facility_id) VALUES ({}, {},
                                            {}, {}, {},
                                            {},
                                            {}, {}, {},
                                            '{}',{},'{}',{},{},'{}',{},'{}',{},{});
                                            """.format(from_node, to_node,
                                            day, day+fixed_route_duration, commodity_id,
                                            from_vertex_id,
                                            default_min_capacity, route_cost, transport_cost,
                                            'transport', nx_edge_id, mode, mode_oid, length, simple_mode, tariff_id, phase_of_matter, subcommodity_id, source_facility_id))
                                elif(from_location == 'NULL' and to_location != 'NULL'):
                                    #for each day and commodity, get the corresponding destination vertex id to include with the edge info
                                    for row_d in db_cur4.execute("""select vertex_id
                                    from vertices v, facility_commodities fc
                                    where v.location_id = {} and v.schedule_day = {} and v.commodity_id = {} and v.subcommodity_id = {}
                                    and v.storage_vertex = 1
                                    and v.facility_id = fc.facility_id
                                    and v.commodity_id = fc.commodity_id
                                    and fc.io = 'i'""".format(to_location, day, commodity_id, subcommodity_id)):
                                        to_vertex_id = row_d[0]
                                        main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                            start_day, end_day, commodity_id,
                                            d_vertex_id,
                                            min_edge_capacity, edge_flow_cost, edge_flow_cost2,
                                            edge_type, nx_edge_id, mode, mode_oid, length, simple_mode, tariff_id, phase_of_matter, subcommodity_id, source_facility_id) VALUES ({}, {},
                                            {}, {}, {},
                                            {},
                                            {}, {}, {},
                                            '{}',{},'{}',{},{},'{}',{},'{}',{},{});
                                            """.format(from_node, to_node,
                                            day, day+fixed_route_duration, commodity_id,
                                            to_vertex_id,
                                            default_min_capacity, route_cost, transport_cost,
                                            'transport', nx_edge_id, mode, mode_oid, length, simple_mode, tariff_id, phase_of_matter, subcommodity_id, source_facility_id))
                                elif(from_location != 'NULL' and to_location != 'NULL'):
                                    #for each day and commodity, get the corresponding origin and destination vertex ids to include with the edge info
                                    for row_d in db_cur4.execute("""select vertex_id
                                    from vertices v, facility_commodities fc
                                    where v.location_id = {} and v.schedule_day = {} and v.commodity_id = {} and v.subcommodity_id = {}
                                    and v.storage_vertex = 1
                                    and v.facility_id = fc.facility_id
                                    and v.commodity_id = fc.commodity_id
                                    and fc.io = 'o'""".format(from_location, day, commodity_id, subcommodity_id)):
                                        from_vertex_id = row_d[0]
                                        db_cur5 = main_db_con.cursor()
                                        for row_e in db_cur5.execute("""select vertex_id
                                    from vertices v, facility_commodities fc
                                    where v.location_id = {} and v.schedule_day = {} and v.commodity_id = {} and v.subcommodity_id = {}
                                    and v.storage_vertex = 1
                                    and v.facility_id = fc.facility_id
                                    and v.commodity_id = fc.commodity_id
                                    and fc.io = 'i'""".format(to_location, day, commodity_id, subcommodity_id)):
                                            to_vertex_id = row_d[0]
                                            main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                                start_day, end_day, commodity_id,
                                                o_vertex_id, d_vertex_id,
                                                min_edge_capacity, edge_flow_cost, edge_flow_cost2,
                                                edge_type, nx_edge_id, mode, mode_oid, length, simple_mode, tariff_id, phase_of_matter, subcommodity_id, source_facility_id) VALUES ({}, {},
                                                {}, {}, {},
                                                {}, {},
                                                {}, {}, {},
                                                '{}',{},'{}', {},{},'{}',{},'{}',{},{});
                                                """.format(from_node, to_node,
                                                day, day+fixed_route_duration, commodity_id,
                                                from_vertex_id, to_vertex_id,
                                                default_min_capacity, route_cost, transport_cost,
                                                'transport', nx_edge_id, mode, mode_oid, length, simple_mode, tariff_id, phase_of_matter, subcommodity_id, source_facility_id))




        for row_d in db_cur.execute("select count(distinct edge_id) from edges;"):
            logger.info('{} edges created'.format(row_d[0]))

        logger.debug("all transport edges created")

        #create storage & connector edges
        #for each storage route, get origin storage vertices; if storage vertex exists on day of origin + duration, create a storage edge
        for row_a in db_cur.execute("select sr.route_name, o_name, d_name, cost_1,cost_2, travel_time, storage_max, storage_min, rr.route_id, location_id, facility_id from storage_routes sr, route_reference rr where sr.route_name = rr.route_name;"):
            route_name = row_a[0]
            origin_facility_name = row_a[1]
            dest_facility_name = row_a[2]
            storage_route_cost_1 = row_a[3]
            storage_route_cost_2 = row_a[4]
            storage_route_travel_time = row_a[5]
            max_daily_storage_capacity = row_a[6]
            min_daily_storage_capacity = row_a[7]
            route_id = row_a[8]
            facility_location_id = row_a[9]
            facility_id = row_a[10]


            db_cur2 = main_db_con.cursor()
            #storage vertices as origin for this route
            #storage routes have no commodity phase restriction so we don't need to check for it here
            for row_b in db_cur2.execute("""select v.vertex_id, v.schedule_day,
            v.commodity_id, v.storage_vertex, v.subcommodity_id, v.source_facility_id
            from vertices v
            where v.facility_id = {}
            and v.storage_vertex = 1;""".format(facility_id)):
                o_vertex_id = row_b[0]
                o_schedule_day = row_b[1]
                o_commodity_id = row_b[2]
                o_storage_vertex = row_b[3]
                o_subcommodity_id = row_b[4]
                o_source_facility_id = row_b[5]


                #matching dest vertex for storage - same commodity and facility name, iterate day
                db_cur3 = main_db_con.cursor()
                for row_c in db_cur3.execute("""select v.vertex_id, v.schedule_day, v.storage_vertex
                from vertices v
                where v.facility_id = {}
                and v.schedule_day = {}
                and v.commodity_id = {}
                and v.storage_vertex = 1
                and v.vertex_id != {}
                and v.subcommodity_id = {};""".format(facility_id, o_schedule_day+storage_route_travel_time, o_commodity_id, o_vertex_id, o_subcommodity_id)):
                    d_vertex_id = row_c[0]
                    d_schedule_day = row_c[1]
                    o_storage_vertex = row_b[2]
                    main_db_con.execute("""insert or ignore into edges (route_id,
                    start_day, end_day, commodity_id, o_vertex_id, d_vertex_id,
                    max_edge_capacity, min_edge_capacity, edge_flow_cost, edge_type, subcommodity_id, source_facility_id)
                     VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, '{}',{},{});""".format(
                     route_id, o_schedule_day, d_schedule_day, o_commodity_id, o_vertex_id,
                     d_vertex_id, max_daily_storage_capacity, min_daily_storage_capacity, storage_route_cost_1, 'storage', o_subcommodity_id, o_source_facility_id))

                #connector edges from storage into primary vertex - processors and destinations
                #check commodity direction = i instead of and v.facility_type_id != 'raw_material_producer'
                #12-8 upates to account for multi-commodity processor vertices
                #subcommodity has to match even if multi commodity - keep track of different processor vertex copies
                db_cur3 = main_db_con.cursor()
                for row_c in db_cur3.execute("""select d.vertex_id, d.schedule_day
                from vertices d, facility_commodities fc
                where d.facility_id = {}
                and d.schedule_day = {}
                and (d.commodity_id = {} or d.commodity_id = {})
                and d.storage_vertex = 0
                and d.facility_id = fc.facility_id
                and fc.commodity_id = {}
                and fc.io = 'i'
                and d.source_facility_id = {}
                ;
                """.format(facility_id, o_schedule_day, o_commodity_id, id_for_mult_commodities, o_commodity_id,o_source_facility_id)):
                    d_vertex_id = row_c[0]
                    d_schedule_day = row_c[1]
                    connector_cost = 0
                    main_db_con.execute("""insert or ignore into edges (route_id, start_day, end_day, commodity_id, o_vertex_id, d_vertex_id,
                    max_edge_capacity, min_edge_capacity, edge_flow_cost, edge_type, subcommodity_id, source_facility_id)
                     VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, '{}',{},{});""".format(
                     route_id, o_schedule_day, d_schedule_day, o_commodity_id, o_vertex_id,
                     d_vertex_id, default_max_capacity, default_min_capacity, connector_cost, 'connector', o_subcommodity_id, o_source_facility_id))


        #create remaining connector edges
        #primary to storage vertex - supplier, destination, processor; not needed for storage facilities
        #same day, same commodity (unless processor), no cost; purpose is to separate control of flows
        #into and out of the system from flows within the system (transport & storage)
        #and v.facility_type_id != 'ultimate_destination'
        #create connectors ending at storage


                d_vertex_id = row_b[0]
                d_schedule_day = row_b[1]
                d_commodity_id = row_b[2]
                d_storage_vertex = row_b[3]
                d_subcommodity_id = row_b[4]
                d_source_facility_id = row_b[5]


                db_cur3 = main_db_con.cursor()
                for row_c in db_cur3.execute("""select vertex_id, schedule_day from vertices v, facility_commodities fc
                where v.facility_id = {}
                and v.schedule_day = {}
                and (v.commodity_id = {} or v.commodity_id = {})
                and v.storage_vertex = 0
                and v.facility_id = fc.facility_id
                and fc.commodity_id = {}
                and fc.io = 'o'
                and v.source_facility_id = {}
                ;
                """.format(facility_id, d_schedule_day, d_commodity_id, id_for_mult_commodities, d_commodity_id,d_source_facility_id)):
                    o_vertex_id = row_c[0]
                    o_schedule_day = row_c[1]
                    connector_cost = 0
                    main_db_con.execute("""insert or ignore into edges (route_id, start_day, end_day, commodity_id, o_vertex_id, d_vertex_id,
                    max_edge_capacity, min_edge_capacity, edge_flow_cost, edge_type, subcommodity_id, source_facility_id)
                    VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, '{}',{},{});""".format(
                    route_id, o_schedule_day, d_schedule_day, d_commodity_id,
                    o_vertex_id, d_vertex_id, default_max_capacity, default_min_capacity,
                    connector_cost, 'connector', d_subcommodity_id, d_source_facility_id))

        logger.info("all edges created")
        logger.info("create an index for the edges table by nodes")

        sql = ("""CREATE INDEX IF NOT EXISTS edge_index ON edges (
        edge_id, route_id, from_node_id, to_node_id, commodity_id,
        start_day, end_day, commodity_id, o_vertex_id, d_vertex_id,
        max_edge_capacity, min_edge_capacity, edge_flow_cost, edge_flow_cost2,
        edge_type, nx_edge_id, mode, mode_oid, length, subcommodity_id, source_facility_id);""")
        db_cur.execute(sql)

    return

#===============================================================================
def set_edge_capacity_and_volume(the_scenario, logger):

    logger.info("START: set_edge_capacity_and_volume")

    multi_commodity_name = "multicommodity"

    with sqlite3.connect(the_scenario.main_db) as main_db_con:


        logger.debug("starting to record volume and capacity for non-pipeline edges")

        main_db_con.execute("update edges set volume = (select ifnull(ne.volume,0) from networkx_edges ne where ne.edge_id = edges.nx_edge_id ) where simple_mode in ('rail','road','water');")
        main_db_con.execute("update edges set max_edge_capacity = (select ne.capacity from networkx_edges ne where ne.edge_id = edges.nx_edge_id) where simple_mode in ('rail','road','water');")
        logger.debug("volume and capacity recorded for non-pipeline edges")

        logger.debug("starting to record volume and capacity for pipeline edges")
##
        main_db_con.executescript("""update edges set volume =
        (select l.background_flow
         from pipeline_mapping pm,
         (select id_field_name, cn.source_OID as link_id, min(cn.capacity) capac,
        max(cn.volume) background_flow, source
        from capacity_nodes cn
        where cn.id_field_name = 'MASTER_OID'
        and ifnull(cn.capacity,0)>0
        group by link_id) l

        where edges.tariff_id = pm.id
        and pm.id_field_name = 'tariff_ID'
        and pm.mapping_id_field_name = 'MASTER_OID'
        and l.id_field_name = 'MASTER_OID'
        and pm.mapping_id = l.link_id
        and instr(edges.mode, l.source)>0)
         where simple_mode = 'pipeline'
        ;

        update edges set max_edge_capacity =
        (select l.capac
        from pipeline_mapping pm,
        (select id_field_name, cn.source_OID as link_id, min(cn.capacity) capac,
        max(cn.volume) background_flow, source
        from capacity_nodes cn
        where cn.id_field_name = 'MASTER_OID'
        and ifnull(cn.capacity,0)>0
        group by link_id) l

        where edges.tariff_id = pm.id
        and pm.id_field_name = 'tariff_ID'
        and pm.mapping_id_field_name = 'MASTER_OID'
        and l.id_field_name = 'MASTER_OID'
        and pm.mapping_id = l.link_id
        and instr(edges.mode, l.source)>0)
        where simple_mode = 'pipeline'
        ;""")

        logger.debug("volume and capacity recorded for pipeline edges")
        logger.debug("starting to record units and conversion multiplier")
        logger.debug("update edges set capacity_units")

        main_db_con.execute("""update edges
        set capacity_units =
        (case when simple_mode = 'pipeline' then 'kbarrels'
        when simple_mode = 'road' then 'truckload'
        when simple_mode = 'rail' then 'railcar'
        when simple_mode = 'water' then 'barge'
        else 'unexpected mode' end)
        ;""")

        logger.debug("update edges set units_conversion_multiplier;")

        main_db_con.execute("""update edges
        set units_conversion_multiplier =
        (case when simple_mode = 'pipeline' and phase_of_matter = 'liquid' then {}
        when simple_mode = 'road'  and phase_of_matter = 'liquid' then {}
        when simple_mode = 'road'  and phase_of_matter = 'solid' then {}
        when simple_mode = 'rail'  and phase_of_matter = 'liquid' then {}
        when simple_mode = 'rail'  and phase_of_matter = 'solid' then {}
        when simple_mode = 'water'  and phase_of_matter = 'liquid' then {}
        when simple_mode = 'water'  and phase_of_matter = 'solid' then {}
        else 1 end)
        ;""".format(THOUSAND_GALLONS_PER_THOUSAND_BARRELS,
        the_scenario.truck_load_liquid.magnitude,
        the_scenario.truck_load_solid.magnitude,
        the_scenario.railcar_load_liquid.magnitude,
        the_scenario.railcar_load_solid.magnitude,
        the_scenario.barge_load_liquid.magnitude,
        the_scenario.barge_load_solid.magnitude,
        ))

        logger.debug("units and conversion multiplier recorded for all edges; starting capacity minus volume")

        main_db_con.execute("""update edges
        set capac_minus_volume_zero_floor =
        max((select (max_edge_capacity - ifnull(volume,0)) where  max_edge_capacity is not null),0)
        where  max_edge_capacity is not null
        ;""")

        logger.debug("capacity minus volume (minimum set to zero) recorded for all edges")

    return


#===============================================================================
def pre_setup_pulp_from_optimal(logger, the_scenario):

    logger.info("START: pre_setup_pulp")

    source_as_subcommodity_setup(the_scenario, logger)

    schedule_dict, schedule_length = ftot_pulp.generate_schedules(the_scenario, logger)

    generate_all_vertices_from_optimal_for_sasc(the_scenario, schedule_dict, schedule_length, logger)

    add_storage_routes(the_scenario, logger)
    generate_all_edges_from_optimal_for_sasc(the_scenario, schedule_length, logger)
    set_edge_capacity_and_volume(the_scenario, logger)

    logger.info("Edges generated for modes: {}".format(the_scenario.permittedModes))


    return

#===============================================================================

def create_flow_vars(the_scenario, logger):
    # all_edges is a list of strings to be the keys for LPVariable.dict

    # we have a table called edges.
    # call helper method to get list of unique IDs from the Edges table.
    # use a the rowid as a simple unique integer index
    edge_list = []

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        edge_list_cur = db_cur.execute("""select edge_id--, commodity_id, start_day, subcommodity_id
        from edges;""")
        edge_list_data = edge_list_cur.fetchall()
        counter = 0
        for row in edge_list_data:
            if counter % 500000 == 0:
                logger.info("processed {:,.0f} records. size of edge_list {:,.0f}".format(counter, sys.getsizeof(edge_list)))
            counter += 1
             #create an edge for each commodity allowed on this link - this construction may change as specific commodity restrictions are added
             #TODO4-18 add days, but have no scheduel for links currently
             #running just with nodes for now, will add proper facility info and storage back soon
            edge_list.append((row[0]))

    logger.debug("MNP DEBUG: start assign flow_var with edge_list")

    flow_var = LpVariable.dicts("Edge",edge_list,0,None)
    logger.debug("MNP DEBUG: Size of flow_var: {:,.0f}".format(sys.getsizeof(flow_var)))
    #delete edge_list_data
    edge_list_data = []
    return flow_var
    # flow_var is the flow on each arc, being determined; this can be defined any time after all_arcs is defined

#===============================================================================
def create_unmet_demand_vars(the_scenario, logger):

    demand_var_list = []
    #may create vertices with zero demand, but only for commodities that the facility has demand for at some point



    with sqlite3.connect(the_scenario.main_db) as main_db_con:
         db_cur = main_db_con.cursor()
         for row in db_cur.execute("""select v.facility_id, v.schedule_day, ifnull(c.supertype, c.commodity_name) top_level_commodity_name, v.udp
         from vertices v, commodities c, facility_type_id ft, facilities f
         where v.commodity_id = c.commodity_id
         and ft.facility_type = "ultimate_destination"
         and v.storage_vertex = 0
         and v.facility_type_id = ft.facility_type_id
         and v.facility_id = f.facility_id
         and f.ignore_facility = 'false'
         group by v.facility_id, v.schedule_day, ifnull(c.supertype, c.commodity_name)
         ;""".format('')):
             #facility_id, day, and simplified commodity name
             demand_var_list.append((row[0], row[1], row[2], row[3]))

    unmet_demand_var = LpVariable.dicts("UnmetDemand", demand_var_list, 0, None)

    return unmet_demand_var

#===============================================================================

def create_candidate_processor_build_vars(the_scenario, logger):
    processors_build_list = []

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        for row in db_cur.execute("select f.facility_id from facilities f, facility_type_id ft where f.facility_type_id = ft.facility_type_id and facility_type = 'processor' and candidate = 1 and ignore_facility = 'false' group by facility_id;"):
             #grab all candidate processor facility IDs
            processors_build_list.append(row[0])

    processor_build_var = LpVariable.dicts("BuildProcessor", processors_build_list,0,None, 'Binary')



    return processor_build_var
#===============================================================================
def create_binary_processor_vertex_flow_vars(the_scenario):
    processors_flow_var_list = []

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        for row in db_cur.execute("""select v.facility_id, v.schedule_day
        from vertices v, facility_type_id ft
        where v.facility_type_id = ft.facility_type_id
        and facility_type = 'processor'
        and storage_vertex = 0
        group by v.facility_id, v.schedule_day;"""):
             #facility_id, day
            processors_flow_var_list.append((row[0], row[1]))

    processor_flow_var = LpVariable.dicts("ProcessorDailyFlow", processors_flow_var_list, 0,None, 'Binary')


    return processor_flow_var
#===============================================================================
def create_processor_excess_output_vars(the_scenario):
    excess_var_list = []
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        xs_cur =  db_cur.execute("""
        select vertex_id, commodity_id
        from vertices v, facility_type_id ft
        where v.facility_type_id = ft.facility_type_id
        and facility_type = 'processor'
        and storage_vertex = 1;""")
             #facility_id, day, and simplified commodity name
        xs_data = xs_cur.fetchall()
        for row in xs_data:
            excess_var_list.append(row[0])


    excess_var = LpVariable.dicts("XS", excess_var_list, 0,None)


    return excess_var
#===============================================================================

def create_constraint_unmet_demand(logger, the_scenario, prob, flow_var, unmet_demand_var):

    logger.debug("START: create_constraint_unmet_demand")



    #apply activity_level to get corresponding actual demand for var

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
    # var has form(facility_name, day, simple_fuel)
    #unmet demand commodity should be simple_fuel = supertype
        logger.debug("MNP: DEBUG: length of unmet_demand_vars: {}".format(len(unmet_demand_var)))

        demand_met_dict = defaultdict(list)
        actual_demand_dict = {}

        demand_met = []
        # want to specify that all edges leading into this vertex + unmet demand = total demand
        #demand primary (non-storage)  vertices

        db_cur = main_db_con.cursor()
        #each row_a is a primary vertex whose edges in contributes to the met demand of var
        #will have one row for each fuel subtype in the scenario
        unmet_data = db_cur.execute("""select v.vertex_id, v.commodity_id,
        v.demand, ifnull(c.proportion_of_supertype, 1), ifnull(v.activity_level, 1), v.subcommodity_id,
        v.facility_id, v.schedule_day, ifnull(c.supertype, c.commodity_name), v.udp, e.edge_id
        from vertices v, commodities c, subcommodity sc, facility_type_id ft, facilities f, edges e
        where v.facility_id = f.facility_id
        and ft.facility_type = 'ultimate_destination'
        and f.facility_type_id = ft.facility_type_id
        and f.ignore_facility = 'false'
        and v.facility_type_id = ft.facility_type_id
        and v.storage_vertex = 0
        and c.commodity_id = v.commodity_id
        and sc.sub_id = v.subcommodity_id
        and e.d_vertex_id = v.vertex_id
        group by v.vertex_id, v.commodity_id,
        v.demand, ifnull(c.proportion_of_supertype, 1), ifnull(v.activity_level, 1), v.subcommodity_id,
        v.facility_id, v.schedule_day, ifnull(c.supertype, c.commodity_name), v.udp, e.edge_id
        ;""")

        unmet_data = unmet_data.fetchall()
        for row_a in unmet_data:

            primary_vertex_id = row_a[0]
            commodity_id = row_a[1]
            var_full_demand = row_a[2]
            proportion_of_supertype = row_a[3]
            var_activity_level = row_a[4]
            subc_id = row_a[5]
            facility_id = row_a[6]
            day = row_a[7]
            top_level_commodity = row_a[8]
            udp = row_a[9]
            edge_id = row_a[10]
            var_actual_demand = var_full_demand*var_activity_level

            #next get inbound edges and apply appropriate modifier proportion to get how much of var's demand they satisfy
            demand_met_dict[(facility_id, day, top_level_commodity, udp)].append(flow_var[edge_id]*proportion_of_supertype)
            actual_demand_dict[(facility_id, day, top_level_commodity, udp)]=var_actual_demand

        for var in unmet_demand_var:
            if var in demand_met_dict:
               #then there are some edges in
               prob += lpSum(demand_met_dict[var]) == actual_demand_dict[var] - unmet_demand_var[var], "constraint set unmet demand variable for facility {}, day {}, commodity {}".format(var[0], var[1], var[2])
            else:
                 #no edges in, so unmet demand equals full demand
                prob += actual_demand_dict[var] == unmet_demand_var[var], "constraint set unmet demand variable for facility {}, day {}, commodity {} - no edges able to meet demand".format(var[0], var[1], var[2])



    logger.debug("FINISHED: create_constraint_unmet_demand and return the prob ")
    return prob

#===============================================================================
# create_constraint_max_flow_out_of_supply_vertex
# primary vertices only
# flow out of a vertex <= supply of the vertex, true for every day and commodity
def create_constraint_max_flow_out_of_supply_vertex(logger, the_scenario, prob, flow_var):
    logger.debug("STARTING:  create_constraint_max_flow_out_of_supply_vertex")
    logger.debug("Length of flow_var: {}".format(len(list(flow_var.items()))))

    #for each primary (non-storage) supply vertex
    #Assumption - each RMP produces a single commodity -
    #Assumption - only one vertex exists per day per RMP (no multi commodity or subcommodity)
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        for row_a in db_cur.execute("""select vertex_id, activity_level, supply
        from vertices v, facility_type_id ft
        where v.facility_type_id = ft.facility_type_id
        and ft.facility_type = 'raw_material_producer'
        and storage_vertex = 0;"""):
            supply_vertex_id = row_a[0]
            activity_level = row_a[1]
            max_daily_supply = row_a[2]
            actual_vertex_supply = activity_level*max_daily_supply

            flow_out = []
            db_cur2 = main_db_con.cursor()
            #select all edges leaving that vertex and sum their flows
            #should be a single connector edge
            for row_b in db_cur2.execute("select edge_id from edges where o_vertex_id = {};".format(supply_vertex_id)):
                edge_id = row_b[0]
                flow_out.append(flow_var[edge_id])

            prob += lpSum(flow_out) <= actual_vertex_supply, "constraint max flow of {} out of origin vertex {}".format(actual_vertex_supply, supply_vertex_id)
        #could easily add human-readable vertex info to this if desirable

    logger.debug("FINISHED:  create_constraint_max_flow_out_of_supply_vertex")
    return prob
    #force flow out of origins to be <= supply

#===============================================================================


# for all of these vertices, flow in always  == flow out
def create_processor_constraints(logger, the_scenario, prob, flow_var, processor_excess_vars, processor_build_vars, processor_daily_flow_vars):
    logger.debug("STARTING:  create_processor_constraints - capacity and conservation of flow")
    node_counter = 0
    node_constraint_counter = 0

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()


        logger.debug("conservation of flow, primary processor vertices:")
        sql = """select v.vertex_id,
        (case when e.o_vertex_id = v.vertex_id then 'out' when e.d_vertex_id = v.vertex_id then 'in' else 'error' end) in_or_out_edge,
        (case when e.o_vertex_id = v.vertex_id then start_day when e.d_vertex_id = v.vertex_id then end_day else 0 end) constraint_day,
        e.commodity_id,
        e.mode,
        e.edge_id,
        nx_edge_id, fc.quantity, v.facility_id, c.commodity_name,
        fc.io,
        v.activity_level,
        ifnull(f.candidate, 0) candidate_check,
        e.subcommodity_id,
        e.source_facility_id,
        v.subcommodity_id,
        v.source_facility_id
        from vertices v, facility_commodities fc, facility_type_id ft, commodities c, facilities f
        join edges e on (v.vertex_id = e.o_vertex_id or v.vertex_id = e.d_vertex_id)
        where ft.facility_type = 'processor'
        and v.facility_id = f.facility_id
        and ft.facility_type_id = v.facility_type_id
        and storage_vertex = 0
        and v.facility_id = fc.facility_id
        and fc.commodity_id = c.commodity_id
        and fc.commodity_id = e.commodity_id
        group by v.vertex_id,
        in_or_out_edge,
        constraint_day,
        e.commodity_id,
        e.mode,
        e.edge_id,
        nx_edge_id, fc.quantity, v.facility_id, c.commodity_name,
        fc.io,
        v.activity_level,
        candidate_check,
        e.subcommodity_id,
        e.source_facility_id,
        v.subcommodity_id,
        v.source_facility_id
        order by v.facility_id, e.source_facility_id, v.vertex_id, fc.io, e.edge_id
        ;"""

        logger.info("Starting the execute")
        execute_start_time = datetime.datetime.now()
        sql_data = db_cur.execute(sql)
        logger.info("Done with the execute fetch all for :")
        logger.info("execute for processor primary vertices, with their in and out edges - Total Runtime (HMS): \t{} \t ".format(get_total_runtime_string(execute_start_time)))

        logger.info("Starting the fetchall")
        fetchall_start_time = datetime.datetime.now()
        sql_data = sql_data.fetchall()
        logger.info("fetchall processor primary vertices, with their in and out edges - Total Runtime (HMS): \t{} \t ".format(get_total_runtime_string(fetchall_start_time)))


        #keys are processor vertices
        flow_in = {}
        flow_out_lists = {}


        for row_a in sql_data:

            vertex_id = row_a[0]
            in_or_out_edge = row_a[1]
            constraint_day = row_a[2]
            commodity_id = row_a[3]
            mode = row_a[4]
            edge_id = row_a[5]
            nx_edge_id = row_a[6]
            quantity = float(row_a[7])
            facility_id = row_a[8]
            commodity_name = row_a[9]
            fc_io_commodity = row_a[10]
            activity_level = row_a[11]
            is_candidate = row_a[12]
            edge_subcommodity_id = row_a[13]
            edge_source_facility_id = row_a[14]
            vertex_subcommodity_id = row_a[15]
            vertex_source_facility_id = row_a[16]

            #edges should only connect to vertices with matching source facility
            #if that's not true there could be issues here


            if in_or_out_edge == 'in':
                flow_in[vertex_id] = (flow_var[edge_id], quantity, commodity_name, activity_level, is_candidate, facility_id, vertex_source_facility_id)
            elif in_or_out_edge == 'out':
                flow_out_lists[(vertex_id, commodity_id)] = (flow_var[edge_id], quantity, commodity_name, facility_id, vertex_source_facility_id)


            #require ratio to match for each output commodityalso do processor capacity constraint here

        for key, value in iteritems(flow_out_lists):
            flow_out = value[0]
            out_quantity = value[1]
            vertex_id = key[0]
            out_commodity = value[2]
            flow_in_var = flow_in[vertex_id][0]
            flow_in_quant = flow_in[vertex_id][1]
            flow_in_commodity = flow_in[vertex_id][2]
            required_flow_out = flow_out/out_quantity
            required_flow_in = flow_in_var/flow_in_quant


        # Sort edges by commodity and whether they're going in or out of the node

            prob += required_flow_out == required_flow_in, "conservation of flow, processor vertex {}, processor facility {}, commodity {} to {}, source facility {}".format(vertex_id, facility_id, flow_in_commodity, out_commodity, vertex_source_facility_id)

        logger.debug("FINISHED:  processor conservation of flow and ratio constraint")


    logger.debug("FINISHED:  create_processor_constraints")
    return prob

def create_constraint_conservation_of_flow(logger, the_scenario, prob, flow_var, processor_excess_vars):
    logger.debug("STARTING:  create_constraint_conservation_of_flow")
    node_counter = 0
    node_constraint_counter = 0
    storage_vertex_constraint_counter = 0

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()


        logger.info("conservation of flow, storage vertices:")
        #storage vertices, any facility type
        #these have at most one direction of transport edges, so no need to track mode
        sql = """select v.vertex_id,
        (case when e.o_vertex_id = v.vertex_id then 'out' when e.d_vertex_id = v.vertex_id then 'in' else 'error' end) in_or_out_edge,
        (case when e.o_vertex_id = v.vertex_id then start_day when e.d_vertex_id = v.vertex_id then end_day else 0 end) constraint_day,
        v.commodity_id,
        e.edge_id,
        nx_edge_id, v.facility_id, c.commodity_name,
        v.activity_level,
        ft.facility_type

        from vertices v, facility_type_id ft, commodities c, facilities f
        join edges e on ((v.vertex_id = e.o_vertex_id or v.vertex_id = e.d_vertex_id) and (e.o_vertex_id = v.vertex_id or e.d_vertex_id = v.vertex_id) and v.commodity_id = e.commodity_id)

        where  v.facility_id = f.facility_id
        and ft.facility_type_id = v.facility_type_id
        and storage_vertex = 1
        and v.commodity_id = c.commodity_id

        group by v.vertex_id,
        in_or_out_edge,
        constraint_day,
        v.commodity_id,
        e.edge_id,
        nx_edge_id,v.facility_id, c.commodity_name,
        v.activity_level

        order by v.facility_id, v.vertex_id, e.edge_id
        ;"""

        # get the data from sql and see how long it takes.
        logger.info("Starting the long step:")

        logger.info("Starting the execute")
        execute_start_time = datetime.datetime.now()
        vertexid_data = db_cur.execute(sql)
        logger.info("Done with the execute fetch all for :")
        logger.info("execute for storage vertices, with their in and out edges - Total Runtime (HMS): \t{} \t ".format(get_total_runtime_string(execute_start_time)))

        logger.info("Starting the fetchall")
        fetchall_start_time = datetime.datetime.now()
        vertexid_data = vertexid_data.fetchall()
        logger.info("fetchall nodes with no location id, with their in and out edges - Total Runtime (HMS): \t{} \t ".format(get_total_runtime_string(fetchall_start_time)))


        flow_in_lists = {}
        flow_out_lists = {}
        for row_v in vertexid_data:
            vertex_id = row_v[0]
            in_or_out_edge = row_v[1]
            constraint_day = row_v[2]
            commodity_id = row_v[3]
            edge_id = row_v[4]
            nx_edge_id = row_v[5]
            facility_id = row_v[6]
            commodity_name = row_v[7]
            activity_level = row_v[8]
            facility_type = row_v[9]

            if in_or_out_edge == 'in':
                flow_in_lists.setdefault((vertex_id, commodity_name, constraint_day, facility_type), []).append(flow_var[edge_id])
            elif in_or_out_edge == 'out':
                flow_out_lists.setdefault((vertex_id, commodity_name, constraint_day, facility_type), []).append(flow_var[edge_id])

        logger.info("adding processor excess variabless to conservation of flow")
        for key, value in iteritems(flow_out_lists):
            vertex_id = key[0]
            commodity_name = key[1]
            day = key[2]
            facility_type = key[3]
            if facility_type == 'processor':
                flow_out_lists.setdefault(key, []).append(processor_excess_vars[vertex_id])


        for key, value in iteritems(flow_out_lists):

            if key in flow_in_lists:
                prob += lpSum(flow_out_lists[key]) == lpSum(flow_in_lists[key]), "conservation of flow, vertex {}, commodity {},  day {}".format(key[0], key[1], key[2])
                storage_vertex_constraint_counter = storage_vertex_constraint_counter + 1
            else:
                prob += lpSum(flow_out_lists[key]) == lpSum(0), "conservation of flow (zero out),  vertex {}, commodity {},  day {}".format(key[0], key[1], key[2])
                storage_vertex_constraint_counter = storage_vertex_constraint_counter + 1

        for key, value in iteritems(flow_in_lists):

            if key not in flow_out_lists:
                prob += lpSum(flow_in_lists[key]) == lpSum(0), "conservation of flow (zero in),  vertex {}, commodity {},  day {}".format(key[0], key[1], key[2])
                storage_vertex_constraint_counter = storage_vertex_constraint_counter + 1

        logger.info("total conservation of flow constraints created on nodes: {}".format(storage_vertex_constraint_counter))



        logger.info("conservation of flow, nx_nodes:")
        #non-vertex nodes, no facility type, connect 2 transport edges over matching days and commodities
        #for each day, get all edges in and out of the node. Sort them by commodity and whether they're going in or out of the node
        sql = """select nn.node_id,
            (case when e.from_node_id = nn.node_id then 'out' when e.to_node_id = nn.node_id then 'in' else 'error' end) in_or_out_edge,
            (case when e.from_node_id = nn.node_id then start_day when e.to_node_id = nn.node_id then end_day else 0 end) constraint_day,
            e.commodity_id,
            ifnull(mode, 'NULL'),
            e.edge_id, nx_edge_id,
            length,
            (case when ifnull(nn.source, 'N') == 'intermodal' then 'Y' else 'N' end) intermodal_flag,
            e.subcommodity_id
            from networkx_nodes nn
            join edges e on (nn.node_id = e.from_node_id or nn.node_id = e.to_node_id)
            where nn.location_id is null
            order by node_id, commodity_id,
            (case when e.from_node_id = nn.node_id then start_day when e.to_node_id = nn.node_id then end_day else 0 end),
            in_or_out_edge, e.subcommodity_id
            ;"""

        # get the data from sql and see how long it takes.
        logger.info("Starting the long step:")

        logger.info("Starting the execute")
        execute_start_time = datetime.datetime.now()
        nodeid_data = db_cur.execute(sql)
        logger.info("Done with the execute fetch all for :")
        logger.info("execute for  nodes with no location id, with their in and out edges - Total Runtime (HMS): \t{} \t ".format(get_total_runtime_string(execute_start_time)))

        logger.info("Starting the fetchall")
        fetchall_start_time = datetime.datetime.now()
        nodeid_data = nodeid_data.fetchall()
        logger.info("fetchall nodes with no location id, with their in and out edges - Total Runtime (HMS): \t{} \t ".format(get_total_runtime_string(fetchall_start_time)))


        flow_in_lists = {}
        flow_out_lists = {}

        for row_a in nodeid_data:
            node_id = row_a[0]
            in_or_out_edge = row_a[1]
            constraint_day = row_a[2]
            commodity_id = row_a[3]
            mode = row_a[4]
            edge_id = row_a[5]
            nx_edge_id = row_a[6]
            length = row_a[7]
            intermodal = row_a[8]
            subcommodity_id = row_a[9]

            #if node is not intermodal, conservation of flow holds per mode;
            #if intermodal, then across modes
            if intermodal == 'N':
                if in_or_out_edge == 'in':
                    flow_in_lists.setdefault((node_id, intermodal, subcommodity_id, constraint_day, mode), []).append(flow_var[edge_id])
                elif in_or_out_edge == 'out':
                    flow_out_lists.setdefault((node_id, intermodal, subcommodity_id, constraint_day, mode), []).append(flow_var[edge_id])
            else:
                 if in_or_out_edge == 'in':
                     flow_in_lists.setdefault((node_id, intermodal, subcommodity_id, constraint_day), []).append(flow_var[edge_id])
                 elif in_or_out_edge == 'out':
                     flow_out_lists.setdefault((node_id, intermodal, subcommodity_id, constraint_day), []).append(flow_var[edge_id])


        for key, value in iteritems(flow_out_lists):
            node_id = key[0]
            intermodal_flag = key[1]
            subcommodity_id = key[2]
            day = key[3]
            if len(key) == 5:
                node_mode = key[4]
            else: node_mode = 'intermodal'
            if key in flow_in_lists:
                prob += lpSum(flow_out_lists[key]) == lpSum(flow_in_lists[key]), "conservation of flow, nx node {}, subcommodity {},  day {}, mode {}".format(node_id, subcommodity_id, day, node_mode)
                node_constraint_counter = node_constraint_counter + 1
            else:
                prob += lpSum(flow_out_lists[key]) == lpSum(0), "conservation of flow (zero out), nx node {}, subcommodity {},  day {}, mode {}".format(node_id, subcommodity_id, day, node_mode)
                node_constraint_counter = node_constraint_counter + 1

        for key, value in iteritems(flow_in_lists):
            node_id = key[0]
            intermodal_flag = key[1]
            subcommodity_id = key[2]
            day = key[3]
            if len(key) == 5:
                node_mode = key[4]
            else: node_mode = 'intermodal'

            if key not in flow_out_lists:
                prob += lpSum(flow_in_lists[key]) == lpSum(0), "conservation of flow (zero in), nx node {}, subcommodity {},  day {}, mode {}".format(node_id, subcommodity_id, day, node_mode)
                node_constraint_counter = node_constraint_counter + 1

        logger.info("total conservation of flow constraints created on nodes: {}".format(node_constraint_counter))

        #Note: no consesrvation of flow for primary vertices for supply & demand - they have unique constraints

    logger.debug("FINISHED:  create_constraint_conservation_of_flow")

    return prob

# set route capacity by phase of matter - may connect multiple facilities, definitely multiple edges
#do storage capacity separately, even though it is also routes
def create_constraint_max_route_capacity(logger, the_scenario, prob, flow_var):
    logger.debug("STARTING:  create_constraint_max_route_capacity")
    logger.debug("Length of flow_var: {}".format(len(list(flow_var.items()))))
    logger.info("modes with background flow turned on: {}".format(the_scenario.backgroundFlowModes))
    #min_capacity_level must be a number from 0 to 1, inclusive
    #min_capacity_level is only relevant when background flows are turned on
    #it sets a floor to how much capacity can be reduced by volume.
    #min_capacity_level = .25 means route capacity will never be less than 25% of full capacity, even if "volume" would otherwise restrict it further
    #min_capacity_level = 0 allows a route to be made unavailable for FTOT flow if base volume is too high
    #this currently applies to all modes; could be made mode specific
    logger.info("minimum available capacity floor set at: {}".format(the_scenario.minCapacityLevel))

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        #capacity for storage routes
        sql = """select
                rr.route_id, sr.storage_max, sr.route_name, e.edge_id, e.start_day
                from route_reference rr
                join storage_routes sr on sr.route_name = rr.route_name
                join edges e on rr.route_id = e.route_id
                ;"""
        # get the data from sql and see how long it takes.

        logger.info("Starting the execute")
        execute_start_time = datetime.datetime.now()
        storage_edge_data = db_cur.execute(sql)
        logger.info("Done with the execute fetch all for storage edges:")
        logger.info("execute for edges for storage - Total Runtime (HMS): \t{} \t ".format(get_total_runtime_string(execute_start_time)))

        logger.info("Starting the fetchall")
        fetchall_start_time = datetime.datetime.now()
        storage_edge_data = storage_edge_data.fetchall()
        logger.info("fetchall edges for storage - Total Runtime (HMS): \t{} \t ".format(get_total_runtime_string(fetchall_start_time)))

        flow_lists = {}

        for row_a in storage_edge_data:
            route_id = row_a[0]
            aggregate_storage_capac = row_a[1]
            storage_route_name = row_a[2]
            edge_id = row_a[3]
            start_day = row_a[4]

            flow_lists.setdefault((route_id, aggregate_storage_capac, storage_route_name, start_day), []).append(flow_var[edge_id])



        for key, flow in iteritems(flow_lists):
            prob += lpSum(flow) <= key[1], "constraint max flow on storage route {} named {} for day {}".format(key[0], key[2], key[3])

        logger.debug("route_capacity constraints created for all storage routes")



        #capacity for transport routes
        #Assumption - all flowing material is in thousand_gallon, all flow is summed on a single non-pipeline nx edge
        sql = """select e.edge_id, e.nx_edge_id, e.max_edge_capacity, e.start_day, e.simple_mode, e.phase_of_matter, e.capac_minus_volume_zero_floor
        from edges e
        where e.max_edge_capacity is not null
        and e.simple_mode != 'pipeline'
        ;"""
        # get the data from sql and see how long it takes.

        logger.info("Starting the execute")
        execute_start_time = datetime.datetime.now()
        route_capac_data = db_cur.execute(sql)
        logger.info("Done with the execute fetch all for transport edges:")
        logger.info("execute for non-pipeline edges for transport edge capacity - Total Runtime (HMS): \t{} \t ".format(get_total_runtime_string(execute_start_time)))

        logger.info("Starting the fetchall")
        fetchall_start_time = datetime.datetime.now()
        route_capac_data = route_capac_data.fetchall()
        logger.info("fetchall non-pipeline  edges for transport edge capacity - Total Runtime (HMS): \t{} \t ".format(get_total_runtime_string(fetchall_start_time)))

        flow_lists = {}

        for row_a in route_capac_data:
            edge_id = row_a[0]
            nx_edge_id = row_a[1]
            nx_edge_capacity = row_a[2]
            start_day = row_a[3]
            simple_mode = row_a[4]
            phase_of_matter = row_a[5]
            capac_minus_background_flow = max(row_a[6], 0)
            min_restricted_capacity = max(capac_minus_background_flow, nx_edge_capacity*the_scenario.minCapacityLevel)

            if simple_mode in the_scenario.backgroundFlowModes:
               use_capacity = min_restricted_capacity
            else: use_capacity = nx_edge_capacity

            # flow is in thousand gallons, for liquid, or metric tons, for solid
            # capacity is in truckload, rail car, barge, or pipeline movement per day
            # if mode is road and phase is liquid, capacity is in truckloads per day, we want it in thousand_gallon
            # ftot_supporting_gis tells us that there are 8 thousand_gallon per truckload, so capacity * 8 gives us correct units or thousand_gallon per day
            # use capacity * ftot_supporting_gis multiplier to get capacity in correct flow units

            multiplier = 1 #unless otherwise specified
            if simple_mode == 'road':
                if phase_of_matter == 'liquid':
                    multiplier = the_scenario.truck_load_liquid.magnitude
                elif phase_of_matter == 'solid':
                    multiplier = the_scenario.truck_load_solid.magnitude
            elif simple_mode == 'water':
                if phase_of_matter == 'liquid':
                    multiplier =  the_scenario.barge_load_liquid.magnitude
                elif phase_of_matter == 'solid':
                    multiplier =  the_scenario.barge_load_solid.magnitude
            elif simple_mode == 'rail':
                if phase_of_matter == 'liquid':
                    multiplier = the_scenario.railcar_load_liquid.magnitude
                elif phase_of_matter == 'solid':
                    multiplier = the_scenario.railcar_load_solid.magnitude

            converted_capacity = use_capacity*multiplier

            flow_lists.setdefault((nx_edge_id, converted_capacity, start_day), []).append(flow_var[edge_id])

        for key, flow in iteritems(flow_lists):
            prob += lpSum(flow) <= key[1], "constraint max flow on nx edge {} for day {}".format(key[0], key[2])

        logger.debug("route_capacity constraints created for all non-pipeline  transport routes")


    logger.debug("FINISHED:  create_constraint_max_route_capacity")
    return prob

def create_constraint_pipeline_capacity(logger, the_scenario, prob, flow_var):
    logger.debug("STARTING:  create_constraint_pipeline_capacity")
    logger.debug("Length of flow_var: {}".format(len(list(flow_var.items()))))
    logger.info("modes with background flow turned on: {}".format(the_scenario.backgroundFlowModes))
    logger.info("minimum available capacity floor set at: {}".format(the_scenario.minCapacityLevel))


    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()

        #capacity for pipeline tariff routes
        #with sasc, may have multiple flows per segment, slightly diff commodities
        sql = """select e.edge_id, e.tariff_id, l.link_id, l.capac, e.start_day, l.capac-l.background_flow allowed_flow, l.source, e.mode, instr(e.mode, l.source)
        from edges e, pipeline_mapping pm,
        (select id_field_name, cn.source_OID as link_id, min(cn.capacity) capac,
        max(cn.volume) background_flow, source
        from capacity_nodes cn
        where cn.id_field_name = 'MASTER_OID'
        and ifnull(cn.capacity,0)>0
        group by link_id) l

        where e.tariff_id = pm.id
        and pm.id_field_name = 'tariff_ID'
        and pm.mapping_id_field_name = 'MASTER_OID'
        and l.id_field_name = 'MASTER_OID'
        and pm.mapping_id = l.link_id
        and instr(e.mode, l.source)>0
        group by e.edge_id, e.tariff_id, l.link_id, l.capac, e.start_day, allowed_flow, l.source
        ;"""
        # capacity needs to be shared over link_id for any edge_id associated with that link

        logger.info("Starting the execute")
        execute_start_time = datetime.datetime.now()
        pipeline_capac_data = db_cur.execute(sql)
        logger.info("Done with the execute fetch all for transport edges:")
        logger.info("execute for edges for transport edge capacity - Total Runtime (HMS): \t{} \t ".format(get_total_runtime_string(execute_start_time)))

        logger.info("Starting the fetchall")
        fetchall_start_time = datetime.datetime.now()
        pipeline_capac_data = pipeline_capac_data.fetchall()
        logger.info("fetchall edges for transport edge capacity - Total Runtime (HMS): \t{} \t ".format(get_total_runtime_string(fetchall_start_time)))

        flow_lists = {}

        for row_a in pipeline_capac_data:
            edge_id = row_a[0]
            tariff_id = row_a[1]
            link_id = row_a[2]
            # Link capacity is recorded in "thousand barrels per day"; 1 barrel = 42 gall
            # Link capacity * 42 is now in thousand_gallon per day, to match flow in thousand_gallon
            link_capacity_kgal_per_day = THOUSAND_GALLONS_PER_THOUSAND_BARRELS*row_a[3]
            start_day = row_a[4]
            capac_minus_background_flow_kgal = max(THOUSAND_GALLONS_PER_THOUSAND_BARRELS*row_a[5],0)
            min_restricted_capacity = max(capac_minus_background_flow_kgal, link_capacity_kgal_per_day*the_scenario.minCapacityLevel)

            capacity_nodes_mode_source = row_a[6]
            edge_mode = row_a[7]
            mode_match_check = row_a[8]
            if 'pipeline' in the_scenario.backgroundFlowModes:
               link_use_capacity = min_restricted_capacity
            else: link_use_capacity = link_capacity_kgal_per_day

            #add flow from all relevant edges, for one start; may be multiple tariffs
            flow_lists.setdefault((link_id, link_use_capacity, start_day, edge_mode), []).append(flow_var[edge_id])

        for key, flow in iteritems(flow_lists):
            prob += lpSum(flow) <= key[1], "constraint max flow on pipeline link {} for mode {} for day {}".format(key[0], key[3], key[2])

        logger.debug("pipeline capacity constraints created for all transport routes")


    logger.debug("FINISHED:  create_constraint_pipeline_capacity")
    return prob

#===============================================================================
##

def setup_pulp_problem(the_scenario, logger):


    logger.info("START: setup PuLP problem")

    # flow_var is the flow on each edge by commodity and day.
    # the optimal value of flow_var will be solved by PuLP
    logger.info("calling create_flow_vars")
    flow_vars = create_flow_vars(the_scenario, logger)

    logger.info("calling create_unmet_demand_vars")
    #unmet_demand_var is the unmet demand at each destination, being determined
    unmet_demand_vars = create_unmet_demand_vars(the_scenario, logger)

    logger.info("calling create_candidate_processor_build_vars")
    #processor_build_vars is the binary variable indicating whether a candidate processor is used and thus its build cost charged
    processor_build_vars = create_candidate_processor_build_vars(the_scenario, logger)

    logger.info("calling create_binary_processor_vertex_flow_vars")
    #binary tracker variables
    processor_vertex_flow_vars = create_binary_processor_vertex_flow_vars(the_scenario)

    logger.info("calling create_processor_excess_output_vars")
    #tracking unused production
    processor_excess_vars = create_processor_excess_output_vars(the_scenario)



    # THIS IS THE OBJECTIVE FUCTION FOR THE OPTIMIZATION
    # ==================================================
    logger.info("calling create_opt_problem")

    prob = ftot_pulp.create_opt_problem(logger,the_scenario, unmet_demand_vars, flow_vars, processor_build_vars)
    logger.info("MNP DEBUG: size of prob: {}".format(sys.getsizeof(prob)))

    logger.info("calling create_constraint_unmet_demand")
    prob = create_constraint_unmet_demand(logger,the_scenario, prob, flow_vars, unmet_demand_vars)
    logger.debug("MNP DEBUG: size of prob: {}".format(sys.getsizeof(prob)))

    logger.info("calling create_constraint_max_flow_out_of_supply_vertex")
    prob = create_constraint_max_flow_out_of_supply_vertex(logger, the_scenario, prob, flow_vars)


    logger.info("calling create_constraint_daily_processor_capacity from ftot_pulp")
    from ftot_pulp import create_constraint_daily_processor_capacity
    prob = create_constraint_daily_processor_capacity(logger, the_scenario, prob, flow_vars, processor_build_vars, processor_vertex_flow_vars)

    logger.info("calling create_processor_constraints")
    prob = create_processor_constraints(logger, the_scenario, prob, flow_vars, processor_excess_vars, processor_build_vars, processor_vertex_flow_vars)

    logger.info("calling create_constraint_conservation_of_flow")
    prob = create_constraint_conservation_of_flow(logger, the_scenario, prob, flow_vars, processor_excess_vars)

    if the_scenario.capacityOn:

        logger.info("calling create_constraint_max_route_capacity")
        prob = create_constraint_max_route_capacity(logger, the_scenario, prob, flow_vars)

        logger.info("calling create_constraint_pipeline_capacity")
        prob = create_constraint_pipeline_capacity(logger, the_scenario, prob, flow_vars)

    del(unmet_demand_vars)

    del(flow_vars)

    # SCENARIO SPECIFIC CONSTRAINTS

    logger.info("calling write LP file")
    prob.writeLP(os.path.join(the_scenario.scenario_run_directory, "debug", "LP_output_sasc.lp"))
    logger.info("FINISHED: setup PuLP problem")

    return prob

###===============================================================================
def solve_pulp_problem(prob_final, the_scenario, logger):

    import datetime

    logger.info("START: prob.solve()")
    #begin new prob.solve logging, 9-04-2018
    start_time = datetime.datetime.now()
    from os import dup, dup2, close
    f = open(os.path.join(the_scenario.scenario_run_directory, "debug", 'probsolve_capture.txt'), 'w')
    orig_std_out = dup(1)
    dup2(f.fileno(), 1)

    #status = prob_final.solve (PULP_CBC_CMD(maxSeconds = i_max_sec, fracGap = d_opt_gap, msg=1))  #  CBC time limit and relative optimality gap tolerance
    status = prob_final.solve (PULP_CBC_CMD(msg=1))  #  CBC time limit and relative optimality gap tolerance
    print('Completion code: %d; Solution status: %s; Best obj value found: %s' % (status, LpStatus[prob_final.status], value(prob_final.objective)))

    dup2(orig_std_out, 1)
    close(orig_std_out)
    f.close()
    #end new prob.solve logging, 9-04-2018

    logger.info("completed calling prob.solve()")
    logger.info("FINISH: prob.solve(): Runtime (HMS): \t{}".format(ftot_supporting.get_total_runtime_string(start_time)))

    # THIS IS THE SOLUTION
    # The status of the solution is printed to the screen
    ##LpStatus key    string value    numerical value
    ##LpStatusOptimal    ?Optimal?    1
    ##LpStatusNotSolved    ?Not Solved?    0
    ##LpStatusInfeasible    ?Infeasible?    -1
    ##LpStatusUnbounded    ?Unbounded?    -2
    ##LpStatusUndefined    ?Undefined?    -3
    logger.result("prob.Status: \t {}".format(LpStatus[prob_final.status]))

    logger.result("Total Scenario Cost = (transportation + unmet demand penalty + processor construction): \t ${0:,.0f}".format(float(value(prob_final.objective))))

    return prob_final
#------------------------------------------------------------------------------
def save_pulp_solution(the_scenario, prob, logger):
    import datetime

    logger.info("START: save_pulp_solution")
    non_zero_variable_count = 0


    with sqlite3.connect(the_scenario.main_db) as db_con:

        db_cur = db_con.cursor()
        # drop  the optimal_solution table
        #-----------------------------
        db_cur.executescript("drop table if exists optimal_solution;")

        # create the optimal_solution table
        #-----------------------------
        db_cur.executescript("""
                                create table optimal_solution
                                (
                                    variable_name string,
                                    variable_value real
                                );
                                """)


        # insert the optimal data into the DB
        #-------------------------------------
        for v in prob.variables():
             if v.varValue > 0.0:

                sql = """insert into optimal_solution  (variable_name, variable_value) values ("{}", {});""".format(v.name, float(v.varValue))
                db_con.execute(sql)
                non_zero_variable_count = non_zero_variable_count + 1

        logger.info("number of solution variables greater than zero: {}".format(non_zero_variable_count))

        sql = """
            create table optimal_variables as
            select
            'UnmetDemand' as variable_type,
            cast(substr(variable_name, 13) as int) var_id,
            variable_value,
            null as converted_capacity,
            null as converted_volume,
            null as converted_capac_minus_volume,
            null as  edge_type,
            null as  commodity_name,
            null as o_facility,
            'placeholder' as d_facility,
            null as  o_vertex_id,
            null as  d_vertex_id,
            null as  from_node_id,
            null as  to_node_id,
            null as  time_period,
            null as commodity_id,
            null as subcommodity_id,
            null as source_facility_id,
            null as source_facility_name,
            null as units,
            variable_name,
            null as  nx_edge_id,
            null as mode,
            null as mode_oid,
            null as length,
            null as original_facility,
            null as final_facility,
            null as prior_edge
            from optimal_solution
            where variable_name like 'UnmetDemand%'
            union
            select
            'Edge' as variable_type,
            cast(substr(variable_name, 6) as int) var_id,
            variable_value,
            edges.max_edge_capacity*edges.units_conversion_multiplier as converted_capacity,
            edges.volume*edges.units_conversion_multiplier as converted_volume,
            edges.capac_minus_volume_zero_floor*edges.units_conversion_multiplier as converted_capac_minus_volume,
            edges.edge_type,
            commodities.commodity_name,
            ov.facility_name as o_facility,
            dv.facility_name as d_facility,
            o_vertex_id,
            d_vertex_id,
            from_node_id,
            to_node_id,
            start_day time_period,
            edges.commodity_id,
            edges.subcommodity_id,
            edges.source_facility_id,
            s.source_facility_name,
            commodities.units,
            variable_name,
            edges.nx_edge_id,
            edges.mode,
            edges.mode_oid,
            edges.length,
            null as original_facility,
            null as final_facility,
            null as prior_edge
            from optimal_solution
            join edges on edges.edge_id = cast(substr(variable_name, 6) as int)
            join commodities on edges.commodity_id = commodities.commodity_ID
            left outer join vertices as ov on edges.o_vertex_id = ov.vertex_id
            left outer join vertices as dv on edges.d_vertex_id = dv.vertex_id
            left outer join subcommodity as s on edges.subcommodity_id = s.sub_id
            where variable_name like 'Edge%'
            union
            select
            'BuildProcessor' as variable_type,
            cast(substr(variable_name, 16) as int) var_id,
            variable_value,
            null as converted_capacity,
            null as converted_volume,
            null as converted_capac_minus_volume,
            null as  edge_type,
            null as  commodity_name,
            'placeholder' as o_facility,
            'placeholder' as d_facility,
            null as  o_vertex_id,
            null as  d_vertex_id,
            null as  from_node_id,
            null as  to_node_id,
            null as  time_period,
            null as commodity_id,
            null as subcommodity_id,
            null as source_facility_id,
            null as source_facility_name,
            null as units,
            variable_name,
            null as  nx_edge_id,
            null as mode,
            null as mode_oid,
            null as length,
            null as original_facility,
            null as final_facility,
            null as prior_edge
            from optimal_solution
            where variable_name like 'Build%';
            """
        db_con.execute("drop table if exists optimal_variables;")
        db_con.execute(sql)



        # query the optimal_solution table in the DB for each variable we care about
        #----------------------------------------------------------------------------
        sql = "select count(variable_name) from optimal_solution where variable_name like 'BuildProcessor%';"
        data = db_con.execute(sql)
        optimal_processors_count = data.fetchone()[0]
        logger.info("number of optimal_processors: {}".format(optimal_processors_count))

        sql = "select count(variable_name) from optimal_solution where variable_name like 'UnmetDemand%';"
        data = db_con.execute(sql)
        optimal_unmet_demand_count = data.fetchone()[0]
        logger.info("number facilities with optimal_unmet_demand : {}".format(optimal_unmet_demand_count))
        sql = "select ifnull(sum(variable_value),0) from optimal_solution where variable_name like 'UnmetDemand%';"
        data = db_con.execute(sql)
        optimal_unmet_demand_sum = data.fetchone()[0]
        logger.info("Total Unmet Demand : {}".format(optimal_unmet_demand_sum))
        logger.info("Penalty per unit of Unmet Demand : ${0:,.0f}".format(the_scenario.unMetDemandPenalty))
        logger.info("Total Cost of Unmet Demand : \t ${0:,.0f}".format(optimal_unmet_demand_sum*the_scenario.unMetDemandPenalty))
        logger.info("Total Cost of building and transporting : \t ${0:,.0f}".format(float(value(prob.objective)) - optimal_unmet_demand_sum*the_scenario.unMetDemandPenalty))
        logger.info("Total Scenario Cost = (transportation + unmet demand penalty + processor construction): \t ${0:,"
                    ".0f}".format(float(value(prob.objective))))

        sql = "select count(variable_name) from optimal_solution where variable_name like 'Edge%';"
        data = db_con.execute(sql)
        optimal_edges_count = data.fetchone()[0]
        logger.info("number of optimal edges: {}".format(optimal_edges_count))

    start_time = datetime.datetime.now()
    logger.info("FINISH: save_pulp_solution: Runtime (HMS): \t{}".format(ftot_supporting.get_total_runtime_string(start_time)))

#------------------------------------------------------------------------------


def parse_optimal_solution_db(the_scenario, logger):
    logger.info("starting parse_optimal_solution")


    optimal_processors = []
    optimal_processor_flows = []
    optimal_route_flows = {}
    optimal_unmet_demand = {}
    optimal_storage_flows = {}
    optimal_excess_material = {}
    vertex_id_to_facility_id_dict = {}

    with sqlite3.connect(the_scenario.main_db) as db_con:

        # do the Storage Edges
        sql = "select variable_name, variable_value from optimal_solution where variable_name like 'Edge%_storage';"
        data = db_con.execute(sql)
        optimal_storage_edges = data.fetchall()
        for edge in optimal_storage_edges:
            optimal_storage_flows[edge] = optimal_storage_edges[edge]

        # do the Route Edges
        sql = """select
            variable_name, variable_value,
            cast(substr(variable_name, 6) as int) edge_id,
            route_ID, start_day time_period, edges.commodity_id,
            o_vertex_id, d_vertex_id,
            v1.facility_id o_facility_id,
         v2.facility_id d_facility_id
            from optimal_solution
            join edges on edges.edge_id = cast(substr(variable_name, 6) as int)
            join vertices v1 on edges.o_vertex_id = v1.vertex_id
        join vertices v2 on edges.d_vertex_id = v2.vertex_id
            where variable_name like 'Edge%_' and variable_name not like 'Edge%_storage';
            """
        data = db_con.execute(sql)
        optimal_route_edges = data.fetchall()
        for edge in optimal_route_edges:

                   variable_name = edge[0]

                   variable_value = edge[1]

                   edge_id = edge[2]

                   route_ID = edge[3]

                   time_period = edge[4]

                   commodity_flowed = edge[5]

                   od_pair_name =  "{}, {}".format(edge[8], edge[9])


                   if route_ID not in optimal_route_flows: # first time route_id is used on a day or commodity
                        optimal_route_flows[route_ID] = [[od_pair_name, time_period, commodity_flowed, variable_value]]

                   else: # subsequent times route is used on different day or for other commodity
                        optimal_route_flows[route_ID].append([od_pair_name, time_period, commodity_flowed, variable_value])

        # do the processors
        sql = "select variable_name, variable_value from optimal_solution where variable_name like 'BuildProcessor%';"
        data = db_con.execute(sql)
        optimal_candidates_processors = data.fetchall()
        for proc in optimal_candidates_processors:
            optimal_processors.append(proc)


        # do the processor vertex flows
        sql = "select variable_name, variable_value from optimal_solution where variable_name like 'ProcessorVertexFlow%';"
        data = db_con.execute(sql)
        optimal_processor_flows_sql = data.fetchall()
        for proc in optimal_processor_flows_sql:
            optimal_processor_flows.append(proc)
            # optimal_biorefs.append(v.name[22:(v.name.find(",")-1)])# find the name from the

        # do the UnmetDemand
        sql = "select variable_name, variable_value from optimal_solution where variable_name like 'UnmetDemand%';"
        data = db_con.execute(sql)
        optimal_unmetdemand = data.fetchall()
        for ultimate_destination in optimal_unmetdemand:
            v_name = ultimate_destination[0]
            v_value = ultimate_destination[1]

            search = re.search('\(.*\)', v_name.replace("'", ""))

            if search:
               parts = search.group(0).replace("(", "").replace(")", "").split(",_")

               dest_name = parts[0]
               commodity_flowed = parts[2]
               if not dest_name in optimal_unmet_demand:
                   optimal_unmet_demand[dest_name] = {}

               if not commodity_flowed in optimal_unmet_demand[dest_name]:
                   optimal_unmet_demand[dest_name][commodity_flowed] = int(v_value)
               else:
                   optimal_unmet_demand[dest_name][commodity_flowed] += int(v_value)

    logger.info("length of optimal_processors list: {}".format(len(optimal_processors))) # a list of optimal processors
    logger.info("length of optimal_processor_flows list: {}".format(len(optimal_processor_flows))) # a list of optimal processor flows
    logger.info("length of optimal_route_flows dict: {}".format(len(optimal_route_flows))) # a dictionary of routes keys and commodity flow values
    logger.info("length of optimal_unmet_demand dict: {}".format(len(optimal_unmet_demand))) # a dictionary of route keys and unmet demand values

    return optimal_processors, optimal_route_flows, optimal_unmet_demand, optimal_storage_flows, optimal_excess_material
