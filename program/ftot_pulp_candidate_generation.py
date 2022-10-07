# ---------------------------------------------------------------------------------------------------
# Name: pulp c2-spiderweb variant
#
# Purpose: PulP optimization  - partial source facility as subcommodity variant
# only creates from source edges for commodities with max transport distance
# other commodities get regular edges to reduce problem size
# ---------------------------------------------------------------------------------------------------

import datetime
import pdb
import re
import sqlite3
from collections import defaultdict
from six import iteritems

from pulp import *

import ftot_supporting
from ftot_supporting import get_total_runtime_string
from ftot_pulp import zero_threshold

# =================== constants=============
storage = 1
primary = 0
fixed_schedule_id = 2
fixed_route_duration = 0
THOUSAND_GALLONS_PER_THOUSAND_BARRELS = 42

candidate_processing_facilities = []

storage_cost_1 = 0.01
storage_cost_2 = 0.05
facility_onsite_storage_max = 10000000000
facility_onsite_storage_min = 0
fixed_route_max_daily_capacity = 100000000
fixed_route_min_daily_capacity = 0
default_max_capacity = 10000000000
default_min_capacity = 0


# ===============================================================================
def oc1(the_scenario, logger):
    # create vertices, then edges for permitted modes, then set volume & capacity on edges
    pre_setup_pulp(logger, the_scenario)


def oc2(the_scenario, logger):
    import ftot_pulp
    # create variables, problem to optimize, and constraints
    prob = setup_pulp_problem_candidate_generation(the_scenario, logger)
    prob = ftot_pulp.solve_pulp_problem(prob, the_scenario, logger)  # imported from ftot_pulp as of 11/19/19
    ftot_pulp.save_pulp_solution(the_scenario, prob, logger, zero_threshold)  # imported from ftot pulp as of 12/03/19


def oc3(the_scenario, logger):
    record_pulp_candidate_gen_solution(the_scenario, logger, zero_threshold)
    from ftot_supporting import post_optimization
    post_optimization(the_scenario, 'oc3', logger)
    # finalize candidate creation and report out
    from ftot_processor import processor_candidates
    processor_candidates(the_scenario, logger)


def check_max_transport_distance_for_OC_step(the_scenario, logger):
    # --------------------------------------------------------------------------
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        sql = "SELECT COUNT(*) FROM commodities WHERE max_transport_distance IS NOT NULL;"
        db_cur = main_db_con.execute(sql)
        count_data = db_cur.fetchone()[0]
        print (count_data)
    if count_data == 0:
        logger.error("running the OC step requires that at least commodity from the RMPs have a max transport "
                     "distance specified in the input CSV file.")
        logger.warning("Please add a max transport distance (in miles) to the RMP csv file.")
        logger.warning("NOTE: run time for the optimization step increases with max transport distance.")
        sys.exit()


# ===============================================================================


def source_as_subcommodity_setup(the_scenario, logger):
    logger.info("START: source_as_subcommodity_setup")
    # create  table source_commodity_ref that only has commodities that can flow out of a facility
    # no multi commodity entry
    # does include entries even if there's no max transport distance, has a flag to indicate that
    multi_commodity_name = "multicommodity"
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        main_db_con.executescript("""

            insert or ignore into commodities(commodity_name) values ('{}');

            drop table if exists source_commodity_ref
            ;

            create table source_commodity_ref(id INTEGER PRIMARY KEY,
            source_facility_id integer,
            source_facility_name text,
            source_facility_type_id integer, --lets us differentiate spiderweb from processor
            commodity_id integer,
            commodity_name text,
            units text,
            phase_of_matter text,
            max_transport_distance numeric,
            max_transport_distance_flag text,
            CONSTRAINT unique_source_and_name UNIQUE(commodity_id, source_facility_id))
            ;

            insert or ignore into source_commodity_ref (
            source_facility_id,
            source_facility_name,
            source_facility_type_id,
            commodity_id,
            commodity_name,
            units,
            phase_of_matter,
            max_transport_distance,
            max_transport_distance_flag)
            select
            f.facility_id,
            f.facility_name,
            f.facility_type_id,
            c.commodity_id,
            c.commodity_name,
            c.units,
            c.phase_of_matter,
            (case when c.max_transport_distance is not null then
            c.max_transport_distance else Null end) max_transport_distance,
            (case when c.max_transport_distance is not null then 'Y' else 'N' end) max_transport_distance_flag

            from commodities c, facilities f, facility_commodities fc
            where f.facility_id = fc.facility_id
            and f.ignore_facility = 'false'
            and fc.commodity_id = c.commodity_id
            and fc.io = 'o'
            ;
            --this will populated processors as potential sources for facilities, but with 
            --max_transport_distance_flag set to 'N'
            
            --candidate_process_commodities data setup
            -- TODO as of 2-25 this will throw an error if run repeatedly, 
            -- adding columns to the candidate_process_commodities table
            

            drop table if exists candidate_process_commodities_temp;

            create table candidate_process_commodities_temp as
            select c.process_id, c.io, c.commodity_name, c.commodity_id, cast(c.quantity as float) quantity, c.units,
            c.phase_of_matter, s.input_commodity, cast(s.output_ratio as float) output_ratio
            from candidate_process_commodities c
            LEFT OUTER JOIN (select o.commodity_id, o.process_id, i.commodity_id as input_commodity,
            cast(o.quantity as float) output_quantity, cast(i.quantity as float) input_quantity,
            (cast(o.quantity as float)/cast(i.quantity as float)) as output_ratio from candidate_process_commodities o,
            candidate_process_commodities i
            where o.io = 'o'
            and i.io = 'i'
            and o.process_id = i.process_id) s
            on (c.commodity_id = s.commodity_id and c.process_id = s.process_id and c.io = 'o')
            ;

            drop table if exists candidate_process_commodities;

            create table candidate_process_commodities as
            select c.*, b.process_id as best_process_id
            from candidate_process_commodities_temp c LEFT OUTER JOIN
            (select commodity_id, input_commodity, process_id, max(output_ratio) best_output_ratio
            from candidate_process_commodities_temp
            where io = 'o'
            group by commodity_id, input_commodity) b  ON
              (c.commodity_id = b.commodity_id and c.process_id = b.process_id and c.io = 'o')
            ;
            
            drop table IF EXISTS candidate_process_commodities_temp;


            """.format(multi_commodity_name, multi_commodity_name)
                                  )

    return


# ===============================================================================


def schedule_avg_availabilities(the_scenario, schedule_dict, schedule_length, logger):
    avg_availabilities = {}

    for sched_id, sched_array in schedule_dict.items():
        # find average availability over all schedule days
        # populate dictionary of one day schedules w/ availability = avg_availability
        avg_availability = sum(sched_array)/schedule_length
        avg_availabilities[sched_id] = [avg_availability]

    return avg_availabilities

# ===============================================================================


def generate_all_edges_from_source_facilities(the_scenario, schedule_length, logger):
    logger.info("START: generate_all_edges_from_source_facilities")

    # plan to generate start and end days based on nx edge time to traverse and schedule
    # can still have route_id, but only for storage routes now; nullable
    # should only run for commodities with a max commodity constraint

    multi_commodity_name = "multicommodity"
    # initializations - all of these get updated if >0 edges exist
    edges_requiring_children = 0
    endcap_edges = 0
    edges_resolved = 0

    with sqlite3.connect(the_scenario.main_db) as main_db_con:

        db_cur = main_db_con.cursor()
        transport_edges_created = 0
        nx_edge_count = 0
        source_based_edges_created = 0

        commodity_mode_data = main_db_con.execute("select * from commodity_mode;")
        commodity_mode_data = commodity_mode_data.fetchall()
        commodity_mode_dict = {}
        for row in commodity_mode_data:
            mode = row[0]
            commodity_id = int(row[1])
            commodity_phase = row[2]
            vehicle_label = row[3]
            allowed_yn = row[4]
            commodity_mode_dict[mode, commodity_id] = allowed_yn

        for row_d in db_cur.execute("""select count(distinct e.edge_id), count(distinct e.nx_edge_id)
        from edges e where e.edge_type = 'transport'
        and children_created in ('N', 'Y', 'E');"""):
            source_based_edges_created = row_d[0]

        for row_d in db_cur.execute("""select count(distinct e.edge_id), count(distinct e.nx_edge_id)
        from edges e where e.edge_type = 'transport';"""):
            transport_edges_created = row_d[0]
            nx_edge_count = row_d[1]

        current_edge_data = db_cur.execute("""select count(distinct edge_id), children_created
             from edges
             where edge_type = 'transport'
             group by children_created
             order by children_created asc;""")
        current_edge_data = current_edge_data.fetchall()
        for row in current_edge_data:
            if row[1] == 'N':
                edges_requiring_children = row[0]
            elif row[1] == 'Y':
                edges_resolved = row[0]
            elif row[1] == 'E':
                endcap_edges = row[0]
                logger.debug('{} endcap edges designated for candidate generation step'.format(endcap_edges))
            if source_based_edges_created == edges_resolved + endcap_edges:
                edges_requiring_children = 0
            if source_based_edges_created == edges_requiring_children + endcap_edges:
                edges_resolved = 0
            if source_based_edges_created == edges_requiring_children + edges_resolved:
                endcap_edges = 0

        logger.info(
            '{} transport edges created; {} require children'.format(transport_edges_created, edges_requiring_children))

        counter = 0

        #       set up a table to keep track of endcap nodes
        sql = """
        drop table if exists endcap_nodes;
        create table if not exists endcap_nodes(
        node_id integer NOT NULL,

        location_id integer,

        --mode of edges that it's an endcap for
        mode_source text NOT NULL,

        --facility it's an endcap for
        source_facility_id integer NOT NULL,

        --commodities it's an endcap for
        commodity_id integer NOT NULL,
         
        --must have a viable process for this commodity to flag as endcap
        process_id integer,
        
        --is this an endcap to allow conversion at destination, that should not be cleaned up?
        destination_yn text,

        CONSTRAINT endcap_key PRIMARY KEY (node_id, mode_source, source_facility_id, commodity_id))
        --the combination of these four (excluding location_id) should be unique,
        --and all fields except location_id should be filled
        ;"""
        db_cur.executescript(sql)

        # create transport edges, only between storage vertices and nodes, based on networkx graph
        # never touch primary vertices; either or both vertices can be null (or node-type) if it's a mid-route link
        # iterate through nx edges: if neither node has a location, create 1 edge per viable commodity
        # should also be per day, subject to nx edge schedule
        # before creating an edge, check: commodity allowed by nx and max transport distance if not null
        # will need nodes per day and commodity? or can I just check that with constraints?
        # select data for transport edges
        # ****1**** Only edges coming from RMP/source storage vertices
        # set distance travelled to miles of edges; set indicator for newly created edges to 'N'
        # edge_count_from_source = 1
        # ****2**** only nx_edges coming from entries in edges (check connector nodes)
        # set distance travelling to miles of new endge plus existing input edge; set indicator for processed edges
        # in edges to 'Y'
        # only create new edge if distance travelled is less than allowed
        # repeat 2 while there are 'N' edges, for transport edges only
        # count loops
        # this is now per-vertex - rework it to not all be done in loop, but in sql block
        # connector and storage edges can be done exactly as before, in fact need to be done first, now in separate
        # method

        while_count = 0
        edge_into_facility_counter = 0

        destination_fac_type = main_db_con.execute("""select facility_type_id from facility_type_id 
            where facility_type = 'ultimate_destination';""")
        destination_fac_type = int(destination_fac_type.fetchone()[0])
        logger.debug("ultimate_Destination type id: {}".format(destination_fac_type))

        while edges_requiring_children > 0:

            while_count = while_count + 1

            # --get nx edges that align with the existing "in edges" - data from nx to create new edges
            # --for each of those nx_edges, if they connect to more than one "in edge" in this batch,
            # only consider connecting to the shortest
            # -- if there is a valid nx_edge to build, the crossing node is not an endcap
            # if total miles of the new route is over max transport distance, then endcap
            # what if one child goes over max transport and another doesn't
            # then the node will get flagged as an endcap, and another path may continue off it, allow both for now
            # --check that day and commodity are permitted by nx

            potential_edge_data = main_db_con.execute("""
            select
            ch.edge_id as ch_nx_edge_id,
            ifnull(CAST(chfn.location_id as integer), 'NULL') fn_location_id,
            ifnull(CAST(chtn.location_id as integer), 'NULL') tn_location_id,
            ch.mode_source,
            p.phase_of_matter,
            nec.route_cost,
            ch.from_node_id,
            ch.to_node_id,
            nec.dollar_cost,
            ch.miles,
            ch.capacity,
            ch.artificial,
            ch.mode_source_oid,
            --parent edge into
            p.commodity_id,
            p.end_day,
             --parent's dest. vertex if exists
            ifnull(p.d_vertex_id,0) o_vertex,
            p.source_facility_id,
            p.leadin_miles_travelled,

            (p.edge_count_from_source +1) as new_edge_count,
            (p.total_route_cost + nec.route_cost) new_total_route_cost,
            p.edge_id leadin_edge,
            p.nx_edge_id leadin_nx_edge,

            --new destination vertex if exists
            ifnull(chtv.vertex_id,0) d_vertex,

            c.max_transport_distance,
            ifnull(proc.best_process_id, 0),
             --type of new destination vertex if exists
            ifnull(chtv.facility_type_id,0) d_vertex_type

            from
            (select count(edge_id) parents,
             min(miles_travelled) leadin_miles_travelled,
             *
            from edges
            where children_created = 'N'
            -----------------do not mess with this "group by"
            group by to_node_id, source_facility_id, commodity_id, end_day
            ------------------it affects which columns we're checking over for min miles travelled
            --------------so that we only get the parent edges we want
            order by parents desc
            ) p, --parent edges to use in this batch
            networkx_edges ch,
            networkx_edge_costs nec,
            commodities c,
            networkx_nodes chfn,
            networkx_nodes chtn
            left outer join vertices chtv
            on (CAST(chtn.location_id as integer) = chtv.location_id
            and (p.source_facility_id = chtv.source_facility_id or chtv.source_facility_id = 0)
            and (chtv.commodity_id = p.commodity_id or chtv.facility_type_id = {})
            and p.end_day = chtv.schedule_day
            and chtv.iob = 'i'
            and chtv.storage_vertex = 1)
            left outer join candidate_process_commodities proc on
            (c.commodity_id = proc.input_commodity and best_process_id is not null)


            where p.to_node_id = ch.from_node_id
            --and p.mode = ch.mode_source --build across modes, control at conservation of flow
            and ch.to_node_id = chtn.node_id
            and ch.from_node_id = chfn.node_id
            and p.phase_of_matter = nec.phase_of_matter_id
            and ch.edge_id = nec.edge_id
            and ifnull(ch.capacity, 1) > 0
            and p.commodity_id = c.commodity_id
            ;""".format(destination_fac_type))
            # --should only get a single leadin edge per networkx/source/commodity/day combination
            # leadin edge should be in route_data, current set of min. identifiers
            # if i'm trying to add an edge that has an entry in route_data, new miles travelled must be less

            potential_edge_data = potential_edge_data.fetchall()

            main_db_con.execute("update edges set children_created = 'Y' where children_created = 'N';")


            # this deliberately includes updating "parent" edges that did not get chosen because they weren't the
            # current shortest path
            # those edges are still "resolved" by this batch

            for row_a in potential_edge_data:
                nx_edge_id = row_a[0]
                from_location = row_a[1]
                to_location = row_a[2]
                mode = row_a[3]
                phase_of_matter = row_a[4]
                route_cost = row_a[5]
                from_node = row_a[6]
                to_node = row_a[7]
                dollar_cost = row_a[8]
                miles = row_a[9]
                mode_oid = row_a[12]
                commodity_id = row_a[13]
                origin_day = row_a[14]
                vertex_id = row_a[15]
                source_facility_id = row_a[16]
                leadin_edge_miles_travelled = row_a[17]
                new_edge_count = row_a[18]
                total_route_cost = row_a[19]
                leadin_edge_id = row_a[20]
                to_vertex = row_a[22]
                max_commodity_travel_distance = row_a[23]
                input_commodity_process_id = row_a[24]
                to_vertex_type = row_a[25]

                # end_day = origin_day + fixed_route_duration
                new_miles_travelled = miles + leadin_edge_miles_travelled
                if mode in the_scenario.permittedModes and (mode, commodity_id) in commodity_mode_dict.keys()\
                        and commodity_mode_dict[mode, commodity_id] == 'Y':
                    if to_vertex_type ==2:
                        logger.debug('edge {} goes in to location {} at '
                                    'node {} with vertex {}'.format(leadin_edge_id, to_location, to_node, to_vertex))

                    if ((new_miles_travelled > max_commodity_travel_distance and input_commodity_process_id != 0)
                            or to_vertex_type == destination_fac_type):
                        #False ):
                        # designate leadin edge as endcap
                        children_created = 'E'
                        destination_yn = 'M'
                        if to_vertex_type == destination_fac_type:
                            destination_yn = 'Y'
                        else:
                            destination_yn = 'N'
                        # update the incoming edge to indicate it's an endcap
                        db_cur.execute(
                            "update edges set children_created = '{}' where edge_id = {}".format(children_created,
                                                                                                 leadin_edge_id))
                        if from_location != 'NULL':
                            db_cur.execute("""insert or ignore into endcap_nodes(
                            node_id, location_id, mode_source, source_facility_id, commodity_id, process_id, destination_yn)
                            VALUES ({}, {}, '{}', {}, {},{},'{}');
                            """.format(from_node, from_location, mode, source_facility_id, commodity_id,
                                       input_commodity_process_id, destination_yn))
                        else:
                            db_cur.execute("""insert or ignore into endcap_nodes(
                            node_id, mode_source, source_facility_id, commodity_id, process_id, destination_yn)
                            VALUES ({}, '{}', {}, {},{},'{}');
                            """.format(from_node, mode, source_facility_id, commodity_id, input_commodity_process_id, destination_yn))


                        # designate leadin edge as endcap
                        # this does, deliberately, allow endcap status to be overwritten if we've found a shorter
                        # path to a previous endcap

                    # create new edge
                    elif new_miles_travelled <= max_commodity_travel_distance:

                        simple_mode = row_a[3].partition('_')[0]
                        tariff_id = 0
                        if simple_mode == 'pipeline':

                            # find tariff_ids

                            sql = "select mapping_id from pipeline_mapping where id = {} and id_field_name = " \
                                  "'source_OID' and source = '{}' and mapping_id is not null;".format(mode_oid, mode)
                            for tariff_row in db_cur.execute(sql):
                                tariff_id = tariff_row[0]

                                # if there are no edges yet for this day, nx, subc combination,
                                # AND this is the shortest existing leadin option for this day, nx, subc combination
                                # we'd be creating an edge for (otherwise wait for the shortest option)
                                # at this step, some leadin edge should always exist

                        if origin_day in range(1, schedule_length + 1):
                            if (origin_day + fixed_route_duration <= schedule_length):  # if link is
                                # traversable in the timeframe
                                if simple_mode != 'pipeline' or tariff_id >= 0:
                                    # for allowed commodities

                                    if from_location == 'NULL' and to_location == 'NULL':
                                        # for each day and commodity, get the corresponding origin vertex id to
                                        # include with the edge info
                                        # origin vertex must not be "ultimate_destination
                                        # transport link outgoing from facility - checking fc.io is more thorough
                                        # than checking if facility type is 'ultimate destination'
                                        # new for bsc, only connect to vertices with matching source_Facility_id

                                        main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                            start_day, end_day, commodity_id,
                                            min_edge_capacity,edge_flow_cost, edge_flow_cost2,
                                            edge_type, nx_edge_id, mode, mode_oid, miles, simple_mode, tariff_id,
                                            phase_of_matter,
                                            source_facility_id,
                                            miles_travelled, children_created, edge_count_from_source, 
                                            total_route_cost) VALUES ({}, {},
                                            {}, {}, {},
                                            {}, {}, {},
                                            '{}',{},'{}',{},
                                            {},'{}',{},'{}',
                                            {},
                                            {},'{}',{},{});
                                            """.format(from_node, to_node,
                                                       origin_day, origin_day + fixed_route_duration, commodity_id,
                                                       default_min_capacity, route_cost, dollar_cost,
                                                       'transport', nx_edge_id, mode, mode_oid,
                                                       miles, simple_mode, tariff_id, phase_of_matter,
                                                       source_facility_id,
                                                       new_miles_travelled, 'N', new_edge_count, total_route_cost))
                                    # only create edge going into a location if an appropriate vertex exists
                                    elif from_location == 'NULL' and to_location != 'NULL' and to_vertex > 0:
                                        edge_into_facility_counter = edge_into_facility_counter + 1

                                        main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                            start_day, end_day, commodity_id,
                                            d_vertex_id,
                                            min_edge_capacity, edge_flow_cost, edge_flow_cost2,
                                            edge_type, nx_edge_id, mode, mode_oid,
                                            miles, simple_mode, tariff_id, phase_of_matter,
                                            source_facility_id,
                                            miles_travelled, children_created, edge_count_from_source, 
                                            total_route_cost) VALUES ({}, {},
                                            {}, {}, {},
                                            {},
                                            {}, {}, {},
                                            '{}',{},'{}', {},
                                            {},'{}',{},'{}',
                                            {},
                                            {},'{}',{},{});
                                            """.format(from_node, to_node,
                                                       origin_day, origin_day + fixed_route_duration, commodity_id,
                                                       to_vertex,
                                                       default_min_capacity, route_cost, dollar_cost,
                                                       'transport', nx_edge_id, mode, mode_oid,
                                                       miles, simple_mode, tariff_id, phase_of_matter,
                                                       source_facility_id,
                                                       new_miles_travelled, 'N', new_edge_count, total_route_cost))

                                    elif from_location != 'NULL' and to_location == 'NULL':
                                        # for each day and commodity, get the corresponding origin vertex id to
                                        # include with the edge info
                                        # origin vertex must not be "ultimate_destination
                                        # transport link outgoing from facility - checking fc.io is more thorough
                                        # than checking if facility type is 'ultimate destination'
                                        # new for bsc, only connect to vertices with matching source facility id (
                                        # only limited for RMP vertices)

                                        main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                            start_day, end_day, commodity_id,
                                            o_vertex_id,
                                            min_edge_capacity,edge_flow_cost, edge_flow_cost2,
                                            edge_type, nx_edge_id, mode, mode_oid, miles, simple_mode, tariff_id,
                                            phase_of_matter,
                                            source_facility_id,
                                            miles_travelled, children_created, edge_count_from_source, 
                                            total_route_cost) VALUES ({}, {},
                                            {}, {}, {},
                                            {},
                                            {}, {}, {},
                                            '{}',{},'{}',{},
                                            {},'{}',{},'{}',
                                            {},
                                            {},'{}',{},{});
                                            """.format(from_node, to_node,
                                                       origin_day, origin_day + fixed_route_duration, commodity_id,
                                                       vertex_id,
                                                       default_min_capacity, route_cost, dollar_cost,
                                                       'transport', nx_edge_id, mode, mode_oid,
                                                       miles, simple_mode, tariff_id, phase_of_matter,
                                                       source_facility_id,
                                                       new_miles_travelled, 'N', new_edge_count, total_route_cost))

                                    elif from_location != 'NULL' and to_location != 'NULL':
                                        # for each day and commodity, get the corresponding origin and destination
                                        # vertex ids to include with the edge info

                                        main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                                start_day, end_day, commodity_id,
                                                o_vertex_id, d_vertex_id,
                                                min_edge_capacity, edge_flow_cost, edge_flow_cost2,
                                                edge_type, nx_edge_id, mode, mode_oid, miles, simple_mode, tariff_id, 
                                                phase_of_matter,
                                                source_facility_id,
                                                miles_travelled, children_created, edge_count_from_source, 
                                                total_route_cost) VALUES ({}, {},
                                                {}, {}, {},
                                                {}, {},
                                                {}, {}, {},
                                                '{}',{},'{}', {},
                                                {},'{}',{},'{}',
                                                {},
                                                {},'{}',{},{});
                                                """.format(from_node, to_node,
                                                           origin_day, origin_day + fixed_route_duration, commodity_id,
                                                           vertex_id, to_vertex,
                                                           default_min_capacity, route_cost, dollar_cost,
                                                           'transport', nx_edge_id, mode, mode_oid,
                                                           miles, simple_mode, tariff_id, phase_of_matter,
                                                           source_facility_id,
                                                           new_miles_travelled, 'N', new_edge_count, total_route_cost))

            for row_d in db_cur.execute("""select count(distinct e.edge_id), count(distinct e.nx_edge_id)
            from edges e where e.edge_type = 'transport'
            and children_created in ('N', 'Y', 'E');"""):
                source_based_edges_created = row_d[0]

            for row_d in db_cur.execute("""select count(distinct e.edge_id), count(distinct e.nx_edge_id)
            from edges e where e.edge_type = 'transport';"""):
                transport_edges_created = row_d[0]
                nx_edge_count = row_d[1]

            current_edge_data = db_cur.execute("""select count(distinct edge_id), children_created
                 from edges
                 where edge_type = 'transport'
                 group by children_created
                 order by children_created asc;""")
            current_edge_data = current_edge_data.fetchall()
            for row in current_edge_data:
                if row[1] == 'N':
                    edges_requiring_children = row[0]
                elif row[1] == 'Y':
                    edges_resolved = row[0]
                elif row[1] == 'E':
                    endcap_edges = row[0]
                    logger.debug('{} endcap edges designated for candidate generation step'.format(endcap_edges))
                if source_based_edges_created == edges_resolved + endcap_edges:
                    edges_requiring_children = 0
                if source_based_edges_created == edges_requiring_children + endcap_edges:
                    edges_resolved = 0
                if source_based_edges_created == edges_requiring_children + edges_resolved:
                    endcap_edges = 0

            if counter % 10 == 0 or edges_requiring_children == 0:
                logger.info(
                    '{} transport edges on {} nx edges,  created in {} loops, {} edges_requiring_children'.format(
                        transport_edges_created, nx_edge_count, while_count, edges_requiring_children))
            counter = counter + 1

        logger.info('{} transport edges on {} nx edges,  created in {} loops, {} edges_requiring_children'.format(
            transport_edges_created, nx_edge_count, while_count, edges_requiring_children))
        logger.info("all source-based transport edges created")

        logger.info("create an index for the edges table")
        for row in db_cur.execute("select count(*) from endcap_nodes"):
            logger.debug("{} endcap nodes before cleanup".format(row))

        sql = ("""
        CREATE INDEX IF NOT EXISTS edge_index ON edges (
        edge_id, route_id, from_node_id, to_node_id, commodity_id,
        start_day, end_day, commodity_id, o_vertex_id, d_vertex_id,
        max_edge_capacity, min_edge_capacity, edge_flow_cost, edge_flow_cost2,
        edge_type, nx_edge_id, mode, mode_oid, miles, source_facility_id);
        """)
        db_cur.execute(sql)
        return


# ===============================================================================


def clean_up_endcaps(the_scenario, logger):
    logger.info("START: clean_up_endcaps")

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        logger.info("clean up endcap node flagging")

        sql = ("""
        drop table if exists e2;
        
        create table e2 as 
        select e.edge_id    ,
        e.route_id    ,
        e.from_node_id    ,
        e.to_node_id    ,
        e.start_day    ,
        e.end_day    ,
        e.commodity_id    ,
        e.o_vertex_id    ,
        e.d_vertex_id    ,
        e.max_edge_capacity    ,
        e.volume    ,
        e.capac_minus_volume_zero_floor    ,
        e.min_edge_capacity    ,
        e.capacity_units    ,
        e.units_conversion_multiplier    ,
        e.edge_flow_cost    ,
        e.edge_flow_cost2    ,
        e.edge_type    ,
        e.nx_edge_id    ,
        e.mode    ,
        e.mode_oid    ,
        e.miles    ,
        e.simple_mode    ,
        e.tariff_id    ,
        e.phase_of_matter    ,
        e.source_facility_id    ,
        e.miles_travelled    ,
        (case when w.cleaned_children = 'C' then w.cleaned_children else e.children_created end) as children_created,
         edge_count_from_source    ,
        total_route_cost
        from edges e
        left outer join 
        (select 'C' as cleaned_children, t.edge_id
        from edges,
        (select e.edge_id
        from edges e, edges e2, endcap_nodes en
        where e.children_created = 'E'
        and e.to_node_id = e2.from_node_id
        and e.source_facility_id = e2.source_facility_id
        and e.commodity_id = e2.commodity_id
        and e.miles_travelled < e2.miles_travelled
        and e.to_node_id = en.node_id 
        and e.source_facility_id = en.source_facility_id
        and e.commodity_id = en.commodity_id
        and en.destination_yn = 'N'
        group by  e.edge_id
        order by e.edge_id
        ) t
        where edges.edge_id = t.edge_id
        and edges.children_created = 'E'
        ) w on e.edge_id = w.edge_id
        ;
        
        delete from edges;
       
        insert into edges select * from e2;
        
        drop table e2;
        """)
        logger.info("calling sql on update end caps in the edges table")
        db_cur.executescript(sql)

        logger.info("clean up the endcap_nodes table")

        sql = ("""
        drop table if exists en_clean;
        
        create table en_clean
        as select en.*
        from endcap_nodes en, edges e
        where e.to_node_id = en.node_id
        and e.commodity_id = en.commodity_id
        and e.source_facility_id = en.source_facility_id
        and e.children_created = 'E'
        ;
        
        drop table endcap_nodes;
        create table endcap_nodes as select * from en_clean;
        
        drop table en_clean;

        """)
        logger.info("calling sql to clean up the endcap_nodes table")
        db_cur.executescript(sql)

        for row in db_cur.execute("select count(*) from endcap_nodes;"):
            logger.debug("{} endcap nodes after cleanup".format(row))

    return


# ===============================================================================


def generate_all_edges_without_max_commodity_constraint(the_scenario, schedule_length, logger):
    logger.info("START: generate_all_edges_without_max_commodity_constraint")
    # make sure this covers edges from an RMP if the commodity has no max transport distance

    multi_commodity_name = "multicommodity"

    with sqlite3.connect(the_scenario.main_db) as main_db_con:

        db_cur = main_db_con.cursor()

        commodity_mode_data = main_db_con.execute("select * from commodity_mode;")
        commodity_mode_data = commodity_mode_data.fetchall()
        commodity_mode_dict = {}
        for row in commodity_mode_data:
            mode = row[0]
            commodity_id = int(row[1])
            commodity_phase = row[2]
            vehicle_label = row[3]
            allowed_yn = row[4]
            commodity_mode_dict[mode, commodity_id] = allowed_yn

        counter = 0

        # for all commodities with no max transport distance or output by a candidate process
        source_facility_id = 0

        # create transport edges, only between storage vertices and nodes, based on networkx graph
        # never touch primary vertices; either or both vertices can be null (or node-type) if it's a mid-route link
        # iterate through nx edges: create 1 edge per viable commodity as allowed by nx phase_of_matter or allowed
        # commodities. Should also be per day, subject to nx edge schedule
        # if there is a max transport distance on a candidate process output, disregard for now because we don't know
        # where the candidates will be located

        # select data for transport edges

        sql = """select
        ne.edge_id,
        ifnull(fn.location_id, 'NULL'),
        ifnull(tn.location_id, 'NULL'),
        ne.mode_source,
        ifnull(nec.phase_of_matter_id, 'NULL'),
        nec.route_cost,
        ne.from_node_id,
        ne.to_node_id,
        nec.dollar_cost,
        ne.miles,
        ne.capacity,
        ne.artificial,
        ne.mode_source_oid
        from networkx_edges ne, networkx_nodes fn, networkx_nodes tn, networkx_edge_costs nec

        where ne.from_node_id = fn.node_id
        and ne.to_node_id = tn.node_id
        and ne.edge_id = nec.edge_id
        and ifnull(ne.capacity, 1) > 0
        ;"""
        nx_edge_data = main_db_con.execute(sql)
        nx_edge_data = nx_edge_data.fetchall()
        for row_a in nx_edge_data:

            nx_edge_id = row_a[0]
            from_location = row_a[1]
            to_location = row_a[2]
            mode = row_a[3]
            phase_of_matter = row_a[4]
            route_cost = row_a[5]
            from_node = row_a[6]
            to_node = row_a[7]
            dollar_cost = row_a[8]
            miles = row_a[9]
            mode_oid = row_a[12]
            simple_mode = row_a[3].partition('_')[0]

            counter = counter + 1

            tariff_id = 0
            if simple_mode == 'pipeline':

                # find tariff_ids
                sql = "select mapping_id from pipeline_mapping where id = {} and id_field_name = 'source_OID' and " \
                      "source = '{}' and mapping_id is not null;".format(mode_oid, mode)
                for tariff_row in db_cur.execute(sql):
                    tariff_id = tariff_row[0]

            if mode in the_scenario.permittedModes  and (mode, commodity_id) in commodity_mode_dict.keys()\
                    and commodity_mode_dict[mode, commodity_id] == 'Y':

                # Edges are placeholders for flow variables
                # if both ends have no location, iterate through viable commodities and days, create edge
                # for all days (restrict by link schedule if called for)
                # for all allowed commodities, as currently defined by link phase of matter
                for day in range(1, schedule_length + 1):
                    if (day + fixed_route_duration <= schedule_length):  # if link is traversable in the
                        # timeframe
                        if simple_mode != 'pipeline' or tariff_id >= 0:
                            # for allowed commodities that can be output by some facility or process in the scenario
                            for row_c in db_cur.execute("""select commodity_id
                            from source_commodity_ref 
                            where phase_of_matter = '{}'
                            and max_transport_distance_flag = 'N'
                            group by commodity_id
                            union 
                            select commodity_id 
                            from candidate_process_commodities
                            where phase_of_matter = '{}'
                            and io = 'o'
                            group by commodity_id""".format(phase_of_matter, phase_of_matter)):
                                db_cur4 = main_db_con.cursor()
                                commodity_id = row_c[0]
                                # source_facility_id = row_c[1] # fixed to 0 for all edges created by this method

                                if from_location == 'NULL' and to_location == 'NULL':
                                    main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                        start_day, end_day, commodity_id,
                                        min_edge_capacity, edge_flow_cost, edge_flow_cost2,
                                        edge_type, nx_edge_id, mode, mode_oid, miles, simple_mode, tariff_id, 
                                        phase_of_matter, source_facility_id) VALUES ({}, {},
                                        {}, {}, {},
                                        {}, {}, {},
                                        '{}',{},'{}',{},{},'{}',{},'{}',{});
                                        """.format(from_node, to_node,
                                                   day, day + fixed_route_duration, commodity_id,
                                                   default_min_capacity, route_cost, dollar_cost,
                                                   'transport', nx_edge_id, mode, mode_oid, miles, simple_mode,
                                                   tariff_id, phase_of_matter, source_facility_id))

                                elif from_location != 'NULL' and to_location == 'NULL':
                                    # for each day and commodity, get the corresponding origin vertex id to include
                                    # with the edge info
                                    # origin vertex must not be "ultimate_destination
                                    # transport link outgoing from facility - checking fc.io is more thorough than
                                    # checking if facility type is 'ultimate destination'
                                    # new for bsc, only connect to vertices with matching source_facility_id (only
                                    # limited for RMP vertices)
                                    for row_d in db_cur4.execute("""select vertex_id
                                    from vertices v, facility_commodities fc
                                    where v.location_id = {} and v.schedule_day = {} and v.commodity_id = {} and 
                                    v.source_facility_id = {}
                                    and v.storage_vertex = 1
                                    and v.facility_id = fc.facility_id
                                    and v.commodity_id = fc.commodity_id
                                    and fc.io = 'o'""".format(from_location, day, commodity_id, source_facility_id)):
                                        from_vertex_id = row_d[0]
                                        main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                            start_day, end_day, commodity_id,
                                            o_vertex_id,
                                            min_edge_capacity,edge_flow_cost, edge_flow_cost2,
                                            edge_type, nx_edge_id, mode, mode_oid, miles, simple_mode, tariff_id,
                                            phase_of_matter, source_facility_id) VALUES ({}, {},
                                            {}, {}, {},
                                            {},
                                            {}, {}, {},
                                            '{}',{},'{}',{},{},'{}',{},'{}',{});
                                            """.format(from_node, to_node,
                                                       day, day + fixed_route_duration, commodity_id,
                                                       from_vertex_id,
                                                       default_min_capacity, route_cost, dollar_cost,
                                                       'transport', nx_edge_id, mode, mode_oid, miles, simple_mode,
                                                       tariff_id, phase_of_matter, source_facility_id))
                                elif from_location == 'NULL' and to_location != 'NULL':
                                    # for each day and commodity, get the corresponding destination vertex id to
                                    # include with the edge info
                                    for row_d in db_cur4.execute("""select vertex_id
                                    from vertices v, facility_commodities fc
                                    where v.location_id = {} and v.schedule_day = {} and v.commodity_id = {} and 
                                    v.source_facility_id = {}
                                    and v.storage_vertex = 1
                                    and v.facility_id = fc.facility_id
                                    and v.commodity_id = fc.commodity_id
                                    and fc.io = 'i'""".format(to_location, day, commodity_id, source_facility_id)):
                                        to_vertex_id = row_d[0]
                                        main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                            start_day, end_day, commodity_id,
                                            d_vertex_id,
                                            min_edge_capacity, edge_flow_cost, edge_flow_cost2,
                                            edge_type, nx_edge_id, mode, mode_oid, miles, simple_mode, tariff_id, 
                                            phase_of_matter, source_facility_id) VALUES ({}, {},
                                            {}, {}, {},
                                            {},
                                            {}, {}, {},
                                            '{}',{},'{}',{},{},'{}',{},'{}',{});
                                            """.format(from_node, to_node,
                                                       day, day + fixed_route_duration, commodity_id,
                                                       to_vertex_id,
                                                       default_min_capacity, route_cost, dollar_cost,
                                                       'transport', nx_edge_id, mode, mode_oid, miles, simple_mode,
                                                       tariff_id, phase_of_matter, source_facility_id))
                                elif from_location != 'NULL' and to_location != 'NULL':
                                    # for each day and commodity, get the corresponding origin and destination vertex
                                    # ids to include with the edge info
                                    for row_d in db_cur4.execute("""select vertex_id
                                    from vertices v, facility_commodities fc
                                    where v.location_id = {} and v.schedule_day = {} and v.commodity_id = {} and 
                                    v.source_facility_id = {}
                                    and v.storage_vertex = 1
                                    and v.facility_id = fc.facility_id
                                    and v.commodity_id = fc.commodity_id
                                    and fc.io = 'o'""".format(from_location, day, commodity_id, source_facility_id)):
                                        from_vertex_id = row_d[0]
                                        db_cur5 = main_db_con.cursor()
                                        for row_e in db_cur5.execute("""select vertex_id
                                    from vertices v, facility_commodities fc
                                    where v.location_id = {} and v.schedule_day = {} and v.commodity_id = {} and 
                                    v.source_facility_id = {}
                                    and v.storage_vertex = 1
                                    and v.facility_id = fc.facility_id
                                    and v.commodity_id = fc.commodity_id
                                    and fc.io = 'i'""".format(to_location, day, commodity_id, source_facility_id)):
                                            to_vertex_id = row_d[0]
                                            main_db_con.execute("""insert or ignore into edges (from_node_id, 
                                            to_node_id,
                                                start_day, end_day, commodity_id,
                                                o_vertex_id, d_vertex_id,
                                                min_edge_capacity, edge_flow_cost, edge_flow_cost2,
                                                edge_type, nx_edge_id, mode, mode_oid, miles, simple_mode, tariff_id, 
                                                phase_of_matter, source_facility_id) VALUES ({}, {},
                                                {}, {}, {},
                                                {}, {},
                                                {}, {}, {},
                                                '{}',{},'{}', {},{},'{}',{},'{}',{});
                                                """.format(from_node, to_node,
                                                           day, day + fixed_route_duration, commodity_id,
                                                           from_vertex_id, to_vertex_id,
                                                           default_min_capacity, route_cost, dollar_cost,
                                                           'transport', nx_edge_id, mode, mode_oid, miles, simple_mode,
                                                           tariff_id, phase_of_matter, source_facility_id))

        logger.debug("all transport edges created")

        logger.info("all edges created")
        logger.info("create an index for the edges table by nodes")
        index_start_time = datetime.datetime.now()
        sql = ("""CREATE INDEX IF NOT EXISTS edge_index ON edges (
        edge_id, route_id, from_node_id, to_node_id, commodity_id,
        start_day, end_day, commodity_id, o_vertex_id, d_vertex_id,
        max_edge_capacity, min_edge_capacity, edge_flow_cost, edge_flow_cost2,
        edge_type, nx_edge_id, mode, mode_oid, miles, source_facility_id);""")
        db_cur.execute(sql)
        logger.info("edge_index Total Runtime (HMS): \t{} \t ".format(get_total_runtime_string(index_start_time)))
    return


# ===============================================================================


def pre_setup_pulp(logger, the_scenario):
    logger.info("START: pre_setup_pulp for candidate generation step")
    from ftot_pulp import commodity_mode_setup

    commodity_mode_setup(the_scenario, logger)

    check_max_transport_distance_for_OC_step(the_scenario, logger)

    source_as_subcommodity_setup(the_scenario, logger)

    from ftot_pulp import generate_schedules
    logger.debug("----- Using generate_all_vertices method imported from ftot_pulp ------")
    schedule_dict, schedule_length = generate_schedules(the_scenario, logger)

    # Re-create schedule dictionary with one-day schedules with availability = average availability
    schedule_avg = schedule_avg_availabilities(the_scenario, schedule_dict, schedule_length, logger)
    schedule_avg_length = 1

    from ftot_pulp import generate_all_vertices
    logger.debug("----- Using generate_all_vertices method imported from ftot_pulp ------")
    generate_all_vertices(the_scenario, schedule_avg, schedule_avg_length, logger)

    from ftot_pulp import add_storage_routes
    logger.debug("----- Using add_storage_routes method imported from ftot_pulp ------")
    add_storage_routes(the_scenario, logger)

    from ftot_pulp import generate_connector_and_storage_edges
    logger.debug("----- Using generate_connector_and_storage_edges method imported from ftot_pulp ------")
    generate_connector_and_storage_edges(the_scenario, logger)

    from ftot_pulp import generate_first_edges_from_source_facilities
    logger.debug("----- Using generate_first_edges_from_source_facilities method imported from ftot_pulp ------")
    generate_first_edges_from_source_facilities(the_scenario, schedule_avg_length, logger)

    generate_all_edges_from_source_facilities(the_scenario, schedule_avg_length, logger)

    clean_up_endcaps(the_scenario, logger)

    generate_all_edges_without_max_commodity_constraint(the_scenario, schedule_avg_length, logger)

    logger.info("Edges generated for modes: {}".format(the_scenario.permittedModes))

    from ftot_pulp import set_edges_volume_capacity
    logger.debug("----- Using set_edges_volume_capacity method imported from ftot_pulp ------")
    set_edges_volume_capacity(the_scenario, logger)

    return


# ===============================================================================


def create_flow_vars(the_scenario, logger):
    logger.info("START: create_flow_vars")

    # call helper method to get list of unique IDs from the Edges table.
    # use a the rowid as a simple unique integer index
    edge_list = []

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        edge_list_cur = db_cur.execute("""select edge_id--, commodity_id, start_day, source_facility_id
        from edges;""")
        edge_list_data = edge_list_cur.fetchall()
        counter = 0
        for row in edge_list_data:
            if counter % 500000 == 0:
                logger.info(
                    "processed {:,.0f} records. size of edge_list {:,.0f}".format(counter, sys.getsizeof(edge_list)))
            counter += 1
            # create an edge for each commodity allowed on this link - this construction may change as specific
            # commodity restrictions are added
            edge_list.append((row[0]))

    # flow_var is the flow on each arc, being determined; this can be defined any time after all_arcs is defined
    flow_var = LpVariable.dicts("Edge", edge_list, 0, None)
    logger.detailed_debug("DEBUG: Size of flow_var: {:,.0f}".format(sys.getsizeof(flow_var)))

    return flow_var


# ===============================================================================


def create_unmet_demand_vars(the_scenario, logger):
    logger.info("START: create_unmet_demand_vars")
    demand_var_list = []
    # may create vertices with zero demand, but only for commodities that the facility has demand for at some point
    #checks material incoming from storage vertex

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        for row in db_cur.execute("""select v.facility_id, v.schedule_day, ifnull(c.supertype, c.commodity_name) 
        top_level_commodity_name, v.udp
         from vertices v, commodities c, facility_type_id ft, facilities f
         where v.commodity_id = c.commodity_id
         and ft.facility_type = "ultimate_destination"
         and v.storage_vertex = 0
         and v.facility_type_id = ft.facility_type_id
         and v.facility_id = f.facility_id
         and f.ignore_facility = 'false'
         group by v.facility_id, v.schedule_day, ifnull(c.supertype, c.commodity_name)
         ;""".format('')):
            # facility_id, day, and simplified commodity name, udp
            demand_var_list.append((row[0], row[1], row[2], row[3]))

    unmet_demand_var = LpVariable.dicts("UnmetDemand", demand_var_list, 0, None)

    return unmet_demand_var


# ===============================================================================


def create_candidate_processor_build_vars(the_scenario, logger):
    logger.info("START: create_candidate_processor_build_vars")
    processors_build_list = []

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        for row in db_cur.execute(
                "select f.facility_id from facilities f, facility_type_id ft where f.facility_type_id = "
                "ft.facility_type_id and facility_type = 'processor' and candidate = 1 and ignore_facility = 'false' "
                "group by facility_id;"):
            # grab all candidate processor facility IDs
            processors_build_list.append(row[0])

    processor_build_var = LpVariable.dicts("BuildProcessor", processors_build_list, 0, None, 'Binary')

    return processor_build_var


# ===============================================================================


def create_binary_processor_vertex_flow_vars(the_scenario, logger):
    logger.info("START: create_binary_processor_vertex_flow_vars")
    processors_flow_var_list = []

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        for row in db_cur.execute("""select v.facility_id, v.schedule_day
        from vertices v, facility_type_id ft
        where v.facility_type_id = ft.facility_type_id
        and facility_type = 'processor'
        and storage_vertex = 0
        group by v.facility_id, v.schedule_day;"""):
            # facility_id, day
            processors_flow_var_list.append((row[0], row[1]))

    processor_flow_var = LpVariable.dicts("ProcessorDailyFlow", processors_flow_var_list, 0, None, 'Binary')

    return processor_flow_var


# ===============================================================================


def create_processor_excess_output_vars(the_scenario, logger):
    logger.info("START: create_processor_excess_output_vars")
    excess_var_list = []
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        xs_cur = db_cur.execute("""
        select vertex_id, commodity_id
        from vertices v, facility_type_id ft
        where v.facility_type_id = ft.facility_type_id
        and facility_type = 'processor'
        and storage_vertex = 1;""")
        # facility_id, day, and simplified commodity name
        xs_data = xs_cur.fetchall()
        for row in xs_data:
            excess_var_list.append(row[0])

    excess_var = LpVariable.dicts("XS", excess_var_list, 0, None)

    return excess_var


# ===============================================================================


def create_opt_problem(logger, the_scenario, unmet_demand_vars, flow_vars, processor_build_vars):
    logger.debug("START: create_opt_problem")
    prob = LpProblem("Flow assignment", LpMinimize)

    logger.detailed_debug("DEBUG: length of unmet_demand_vars: {}".format(len(unmet_demand_vars)))
    logger.detailed_debug("DEBUG: length of flow_vars: {}".format(len(flow_vars)))
    logger.detailed_debug("DEBUG: length of processor_build_vars: {}".format(len(processor_build_vars)))

    unmet_demand_costs = []
    flow_costs = {}
    logger.detailed_debug("DEBUG: start loop through sql to append unmet_demand_costs")
    for u in unmet_demand_vars:
        udp = u[3]
        unmet_demand_costs.append(udp * unmet_demand_vars[u])
    logger.detailed_debug("DEBUG: finished loop through sql to append unmet_demand_costs. total records: {}".format(
        len(unmet_demand_costs)))

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        logger.detailed_debug("DEBUG: start sql execute to get flow cost data")
        # Flow cost memory improvements: only get needed data; dict instead of list; narrow in lpsum
        flow_cost_var = db_cur.execute("select edge_id, edge_flow_cost from edges e group by edge_id;")
        logger.detailed_debug("DEBUG: start the fetchall")
        flow_cost_data = flow_cost_var.fetchall()
        logger.detailed_debug(
            "DEBUG: start iterating through {:,.0f} flow_cost_data records".format(len(flow_cost_data)))
        counter = 0
        for row in flow_cost_data:
            edge_id = row[0]
            edge_flow_cost = row[1]
            counter += 1

            # flow costs cover transportation and storage
            flow_costs[edge_id] = edge_flow_cost
        logger.detailed_debug(
            "DEBUG: finished loop through sql to append flow costs: total records: {:,.0f}".format(len(flow_costs)))

    logger.detailed_debug("debug: start prob+= unmet_demand_costs + flow cost + no processor build costs in"
                          "candidate generation step")
    prob += (lpSum(unmet_demand_costs) + lpSum(
        flow_costs[k] * flow_vars[k] for k in flow_costs)), "Total Cost of Transport, storage, and penalties"
    logger.detailed_debug("debug: done prob+= unmet_demand_costs + flow cost + no processor build costs in "
                          "candidate generation step")

    logger.debug("FINISHED: create_opt_problem")
    return prob


# ===============================================================================


def create_constraint_unmet_demand(logger, the_scenario, prob, flow_var, unmet_demand_var):
    logger.debug("START: create_constraint_unmet_demand")

    # apply activity_level to get corresponding actual demand for var

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        # var has form(facility_name, day, simple_fuel)
        # unmet demand commodity should be simple_fuel = supertype
        logger.detailed_debug("DEBUG: length of unmet_demand_vars: {}".format(len(unmet_demand_var)))

        demand_met_dict = defaultdict(list)
        actual_demand_dict = {}

        # want to specify that all edges leading into this vertex + unmet demand = total demand
        # demand primary (non-storage)  vertices

        db_cur = main_db_con.cursor()
        # each row_a is a primary vertex whose edges in contributes to the met demand of var
        # will have one row for each fuel subtype in the scenario
        unmet_data = db_cur.execute("""select v.vertex_id, v.commodity_id,
        v.demand, ifnull(c.proportion_of_supertype, 1), ifnull(v.activity_level, 1), v.source_facility_id,
        v.facility_id, v.schedule_day, ifnull(c.supertype, c.commodity_name), v.udp, e.edge_id
        from vertices v, commodities c, facility_type_id ft, facilities f, edges e
        where v.facility_id = f.facility_id
        and ft.facility_type = 'ultimate_destination'
        and f.facility_type_id = ft.facility_type_id
        and f.ignore_facility = 'false'
        and v.facility_type_id = ft.facility_type_id
        and v.storage_vertex = 0
        and c.commodity_id = v.commodity_id
        and e.d_vertex_id = v.vertex_id
        group by v.vertex_id, v.commodity_id,
        v.demand, ifnull(c.proportion_of_supertype, 1), ifnull(v.activity_level, 1), v.source_facility_id,
        v.facility_id, v.schedule_day, ifnull(c.supertype, c.commodity_name), v.udp, e.edge_id
        ;""")

        unmet_data = unmet_data.fetchall()
        for row_a in unmet_data:
            var_full_demand = row_a[2]
            proportion_of_supertype = row_a[3]
            var_activity_level = row_a[4]
            facility_id = row_a[6]
            day = row_a[7]
            top_level_commodity = row_a[8]
            udp = row_a[9]
            edge_id = row_a[10]
            var_actual_demand = var_full_demand * var_activity_level

            # next get inbound edges and apply appropriate modifier proportion to get how much of var's demand they
            # satisfy
            demand_met_dict[(facility_id, day, top_level_commodity, udp)].append(
                flow_var[edge_id] * proportion_of_supertype)
            actual_demand_dict[(facility_id, day, top_level_commodity, udp)] = var_actual_demand

        for key in unmet_demand_var:
            if key in demand_met_dict:
                # then there are some edges in
                prob += lpSum(demand_met_dict[key]) == actual_demand_dict[key] - unmet_demand_var[
                    key], "constraint set unmet demand variable for facility {}, day {}, commodity {}".format(key[0],
                                                                                                              key[1],
                                                                                                              key[2])
            else:
                if key not in actual_demand_dict:
                    pdb.set_trace()
                # no edges in, so unmet demand equals full demand
                prob += actual_demand_dict[key] == unmet_demand_var[
                    key], "constraint set unmet demand variable for facility {}, day {}, commodity {} -  " \
                          "no edges able to meet demand".format(
                    key[0], key[1], key[2])

    logger.debug("FINISHED: create_constraint_unmet_demand and return the prob ")
    return prob


# ===============================================================================


def create_constraint_max_flow_out_of_supply_vertex(logger, the_scenario, prob, flow_var):
    logger.debug("STARTING:  create_constraint_max_flow_out_of_supply_vertex")
    logger.debug("Length of flow_var: {}".format(len(list(flow_var.items()))))
    # force flow out of origins to be <= supply

    # for each primary (non-storage) supply vertex
    # flow out of a vertex <= supply of the vertex, true for every day and commodity
    # Assumption - each RMP produces a single commodity
    # Assumption - only one vertex exists per day per RMP (no multi commodity or subcommodity)
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
            actual_vertex_supply = activity_level * max_daily_supply

            flow_out = []
            db_cur2 = main_db_con.cursor()
            # select all edges leaving that vertex and sum their flows
            # should be a single connector edge
            for row_b in db_cur2.execute("select edge_id from edges where o_vertex_id = {};".format(supply_vertex_id)):
                edge_id = row_b[0]
                flow_out.append(flow_var[edge_id])

            prob += lpSum(flow_out) <= actual_vertex_supply, "constraint max flow of {} out of origin vertex {}".format(
                actual_vertex_supply, supply_vertex_id)

    logger.debug("FINISHED:  create_constraint_max_flow_out_of_supply_vertex")
    return prob


# ===============================================================================


def create_primary_processor_vertex_constraints(logger, the_scenario, prob, flow_var):
    logger.info("STARTING:  create_primary_processor_vertex_constraints - capacity and conservation of flow")
    # for all of these vertices, flow in always  == flow out

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()

        # total flow in == total flow out, subject to conversion;
        # summed over commodities but not days; does not check commodity proportions
        # dividing by "required quantity" functionally converts all commodities to the same "processor-specific units"

        # processor primary vertices with input commodity and  quantity needed to produce specified output quantities
        # 2 sets of constraints; one for the primary processor vertex to cover total flow in and out
        # one for each input and output commodity (sum over sources) to ensure its ratio matches facility_commodities

        logger.debug("conservation of flow and commodity ratios, primary processor vertices:")
        sql = """select v.vertex_id,
        (case when e.o_vertex_id = v.vertex_id then 'out' when e.d_vertex_id = v.vertex_id then 'in' else 'error' 
        end) in_or_out_edge,
        (case when e.o_vertex_id = v.vertex_id then start_day when e.d_vertex_id = v.vertex_id then end_day else 0 
        end) constraint_day,
        e.commodity_id,
        e.mode,
        e.edge_id,
        nx_edge_id, fc.quantity, v.facility_id, c.commodity_name,
        fc.io,
        v.activity_level,
        ifnull(f.candidate, 0) candidate_check,
        e.source_facility_id,
        v.source_facility_id,
        v.commodity_id
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
        e.source_facility_id,
        v.commodity_id,
        v.source_facility_id
        order by v.facility_id, e.source_facility_id, v.vertex_id, fc.io, e.edge_id
        ;"""

        logger.info("Starting the execute")
        execute_start_time = datetime.datetime.now()
        sql_data = db_cur.execute(sql)
        logger.info("Done with the execute fetch all for :")
        logger.info(
            "execute for processor primary vertices, with their in and out edges - Total Runtime (HMS): \t{} \t "
            "".format(
                get_total_runtime_string(execute_start_time)))

        logger.info("Starting the fetchall")
        fetchall_start_time = datetime.datetime.now()
        sql_data = sql_data.fetchall()
        logger.info(
            "fetchall processor primary vertices, with their in and out edges - Total Runtime (HMS): \t{} \t ".format(
                get_total_runtime_string(fetchall_start_time)))

        # Nested dictionaries
        # flow_in_lists[primary_processor_vertex_id] = dict of commodities handled by that processor vertex

        # flow_in_lists[primary_processor_vertex_id][commodity1] =
        # list of edge ids that flow that commodity into that vertex

        # flow_in_lists[vertex_id].values() to get all flow_in edges for all commodities, a list of lists

        flow_in_lists = {}
        flow_out_lists = {}

        for row_a in sql_data:

            vertex_id = row_a[0]
            in_or_out_edge = row_a[1]
            commodity_id = row_a[3]
            edge_id = row_a[5]
            quantity = float(row_a[7])

            if in_or_out_edge == 'in':
                flow_in_lists.setdefault(vertex_id, {})  # if the vertex isn't in the main dict yet, add it
                flow_in_lists[vertex_id].setdefault((commodity_id, quantity), []).append(flow_var[edge_id])
                # flow_in_lists[vertex_id] is itself a dict keyed on commodity and quantity;
                # value is a list of edge ids into that vertex of that commodity

            elif in_or_out_edge == 'out':
                flow_out_lists.setdefault(vertex_id, {})  # if the vertex isn't in the main dict yet, add it
                flow_out_lists[vertex_id].setdefault((commodity_id, quantity), []).append(flow_var[edge_id])

            # Because we keyed on commodity, source facility tracking is merged as we pass through the processor vertex

            # 1) for each output commodity, check against an input to ensure correct ratio - only need one input
            # 2) for each input commodity, check against an output to ensure correct ratio - only need one output;
            # 2a) first sum sub-flows over input commodity
            # 3)-x- handled by daily processor capacity constraint [calculate total processor input to ensure
            # compliance with capacity constraint

        # 1----------------------------------------------------------------------

        for key, value in iteritems(flow_out_lists):
            vertex_id = key
            zero_in = False
            if vertex_id in flow_in_lists:
                compare_input_list = []
                in_quantity = 0
                in_commodity_id = 0
                for ikey, ivalue in iteritems(flow_in_lists[vertex_id]):
                    in_commodity_id = ikey[0]
                    in_quantity = ikey[1]
                    # edge_list = value2
                    compare_input_list = ivalue
            else:
                zero_in = True

            # value is a dict - we loop once here for each output commodity at the vertex
            for key2, value2 in iteritems(value):
                out_commodity_id = key2[0]
                out_quantity = key2[1]
                # edge_list = value2
                flow_var_list = value2
                if zero_in:
                    prob += lpSum(
                        flow_var_list) == 0, "processor flow, vertex {} has zero in so zero out of commodity {}".format(
                        vertex_id, out_commodity_id)
                else:
                    # ratio constraint for this output commodity relative to total input
                    required_flow_out = lpSum(flow_var_list) / out_quantity
                    # check against an input dict
                    prob += required_flow_out == lpSum(
                        compare_input_list) / in_quantity, "processor flow, vertex {}, commodity {} output quantity " \
                                                           "checked against commodity {} input quantity".format(
                        vertex_id, out_commodity_id, in_commodity_id)

        # 2----------------------------------------------------------------------
        for key, value in iteritems(flow_in_lists):
            vertex_id = key
            zero_out = False
            if vertex_id in flow_out_lists:
                compare_output_list = []
                out_quantity = 0
                for okey, ovalue in iteritems(flow_out_lists[vertex_id]):
                    out_commodity_id = okey[0]
                    out_quantity = okey[1]
                    # edge_list = value2
                    compare_output_list = ovalue
            else:
                zero_out = True

            # value is a dict - we loop once here for each input commodity at the vertex
            for key2, value2 in iteritems(value):
                in_commodity_id = key2[0]
                in_quantity = key2[1]
                # edge_list = value2
                flow_var_list = value2
                if zero_out:
                    prob += lpSum(
                        flow_var_list) == 0, "processor flow, vertex {} has zero out so zero in of commodity {}".format(
                        vertex_id, in_commodity_id)
                else:
                    # ratio constraint for this output commodity relative to total input
                    required_flow_in = lpSum(flow_var_list) / in_quantity
                    # check against an input dict
                    prob += required_flow_in == lpSum(
                        compare_output_list) / out_quantity, "processor flow, vertex {}, commodity {} input quantity " \
                                                             "checked against commodity {} output quantity".format(
                        vertex_id, in_commodity_id, out_commodity_id)

    logger.debug("FINISHED:  create_primary_processor_conservation_of_flow_constraints")
    return prob


# ===============================================================================


def create_constraint_conservation_of_flow_storage_vertices(logger, the_scenario, prob, flow_var,
                                                            processor_excess_vars):
    logger.debug("STARTING:  create_constraint_conservation_of_flow_storage_vertices")
    storage_vertex_constraint_counter = 0

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()

        logger.info("conservation of flow, storage vertices:")
        # storage vertices, any facility type
        # these have at most one direction of transport edges, so no need to track mode
        sql = """select v.vertex_id,
        (case when e.o_vertex_id = v.vertex_id then 'out' when e.d_vertex_id = v.vertex_id then 'in' else 'error' 
        end) in_or_out_edge,
        (case when e.o_vertex_id = v.vertex_id then start_day when e.d_vertex_id = v.vertex_id then end_day else 0 
        end) constraint_day,
        v.commodity_id,
        e.edge_id,
        nx_edge_id, v.facility_id, c.commodity_name,
        v.activity_level,
        ft.facility_type

        from vertices v, facility_type_id ft, commodities c, facilities f
        join edges e on ((v.vertex_id = e.o_vertex_id or v.vertex_id = e.d_vertex_id) and (e.o_vertex_id = 
        v.vertex_id or e.d_vertex_id = v.vertex_id) and v.commodity_id = e.commodity_id)

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
        logger.info("execute for storage vertices, with their in and out edges - Total Runtime (HMS): \t{} \t ".format(
            get_total_runtime_string(execute_start_time)))

        logger.info("Starting the fetchall")
        fetchall_start_time = datetime.datetime.now()
        vertexid_data = vertexid_data.fetchall()
        logger.info("fetchall storage vertices, with their in and out edges - Total Runtime (HMS): \t{} \t ".format(
            get_total_runtime_string(fetchall_start_time)))

        flow_in_lists = {}
        flow_out_lists = {}
        for row_v in vertexid_data:
            vertex_id = row_v[0]
            in_or_out_edge = row_v[1]
            constraint_day = row_v[2]
            commodity_id = row_v[3]
            edge_id = row_v[4]
            facility_type = row_v[9]

            if in_or_out_edge == 'in':
                flow_in_lists.setdefault((vertex_id, commodity_id, constraint_day, facility_type), []).append(
                    flow_var[edge_id])
            elif in_or_out_edge == 'out':
                flow_out_lists.setdefault((vertex_id, commodity_id, constraint_day, facility_type), []).append(
                    flow_var[edge_id])

        logger.info("adding processor excess variables to conservation of flow")
        for key, value in iteritems(flow_out_lists):
            vertex_id = key[0]
            facility_type = key[3]
            if facility_type == 'processor':
                flow_out_lists.setdefault(key, []).append(processor_excess_vars[vertex_id])

        for key, value in iteritems(flow_out_lists):

            if key in flow_in_lists:
                prob += lpSum(flow_out_lists[key]) == lpSum(
                    flow_in_lists[key]), "conservation of flow, vertex {}, commodity {},  day {}".format(key[0], key[1],
                                                                                                         key[2])
                storage_vertex_constraint_counter = storage_vertex_constraint_counter + 1
            else:
                prob += lpSum(flow_out_lists[key]) == lpSum(
                    0), "conservation of flow (zero out),  vertex {}, commodity {},  day {}".format(key[0], key[1],
                                                                                                    key[2])
                storage_vertex_constraint_counter = storage_vertex_constraint_counter + 1

        for key, value in iteritems(flow_in_lists):

            if key not in flow_out_lists:
                prob += lpSum(flow_in_lists[key]) == lpSum(
                    0), "conservation of flow (zero in),  vertex {}, commodity {},  day {}".format(key[0], key[1],
                                                                                                   key[2])
                storage_vertex_constraint_counter = storage_vertex_constraint_counter + 1

        logger.info("total conservation of flow constraints created on storage vertices: {}".format(
            storage_vertex_constraint_counter))

    return prob


# ===============================================================================

def create_constraint_conservation_of_flow_endcap_nodes(logger, the_scenario, prob, flow_var,
                                                        processor_excess_vars):
    # This creates constraints for all non-vertex nodes, with variant rules for endcaps nodes
    logger.debug("STARTING:  create_constraint_conservation_of_flow_endcap_nodes")
    node_constraint_counter = 0
    passthrough_constraint_counter = 0
    other_endcap_constraint_counter = 0
    endcap_no_reverse_counter = 0

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        logger.info("conservation of flow, nx_nodes, with endcaps:")
        # non-vertex nodes, no facility type, connect 2 transport edges over matching days and commodities
        # for each day, get all edges in and out of the node. Sort them by commodity and whether they're going in or
        # out of the node

        # retrieve candiate process information
        # process_dict[input commodity id] = list [proc_id, quantity, tuples (output comm. id, quantity)]
        # initialze Value as list containing process id, then add outputs
        process_dict = {}
        process_outputs_dict = {}

        # list each output with its input to key off; don't use process id
        # nested dictionary: first key is input commodity id
        # second key is output commodity id; value is list  with preferred process:
        # # process_id,input quant, output quantity, output ratio, with the largest output ratio for that commodity pair
        # list each output with its input to key on
        sql = """select process_id, commodity_id, quantity
        from candidate_process_commodities
        where io = 'i';"""
        process_inputs = db_cur.execute(sql)
        process_inputs = process_inputs.fetchall()

        for row in process_inputs:
            process_id = row[0]
            commodity_id = row[1]
            quantity = row[2]
            process_dict.setdefault(commodity_id, [process_id, quantity])
            process_dict[commodity_id] = [process_id, quantity]


        sql = """select o.process_id, o.commodity_id, o.quantity, i.commodity_id
        from candidate_process_commodities o, candidate_process_commodities i
        where o.io = 'o'
        and i.process_id = o.process_id
        and i.io = 'i';"""
        process_outputs = db_cur.execute(sql)
        process_outputs = process_outputs.fetchall()

        for row in process_outputs:
            process_id = row[0]
            output_commodity_id = row[1]
            quantity = row[2]
            input_commodity_id = row[3]

            process_dict[input_commodity_id].append((output_commodity_id, quantity))
            process_outputs_dict.setdefault(process_id, []).append(output_commodity_id)

        sql = """select nn.node_id,
            (case when e.from_node_id = nn.node_id then 'out' when e.to_node_id = nn.node_id then 'in' else 'error' 
            end) in_or_out_edge,
            (case when e.from_node_id = nn.node_id then start_day when e.to_node_id = nn.node_id then end_day else 0 
            end) constraint_day,
            e.commodity_id,
            ifnull(mode, 'NULL'),
            e.edge_id, nx_edge_id,
            miles,
            (case when ifnull(nn.source, 'N') == 'intermodal' then 'Y' else 'N' end) intermodal_flag,
            e.source_facility_id,
            e.commodity_id,
            (case when en.node_id is null then 'N' else 'E' end) as endcap,
            (case when en.node_id is null then 0 else en.process_id end) as endcap_process,
            (case when en.node_id is null then 0 else en.source_facility_id end) as endcap_source_facility
            from networkx_nodes nn
            join edges e on (nn.node_id = e.from_node_id or nn.node_id = e.to_node_id)
            left outer join endcap_nodes en on (nn.node_id = en.node_id and e.commodity_id = en.commodity_id and 
            e.source_facility_id = en.source_facility_id)
            where nn.location_id is null
            order by nn.node_id, e.commodity_id,
            (case when e.from_node_id = nn.node_id then start_day when e.to_node_id = nn.node_id then end_day else 0 
            end),
            in_or_out_edge, e.source_facility_id, e.commodity_id
            ;"""

        # get the data from sql and see how long it takes.
        logger.info("Starting the long step:")

        logger.info("Starting the execute")
        execute_start_time = datetime.datetime.now()
        nodeid_data = db_cur.execute(sql)
        logger.info("Done with the execute fetch all for :")
        logger.info(
            "execute for  nodes with no location id, with their in and out edges - Total Runtime (HMS): \t{} \t "
            "".format(
                get_total_runtime_string(execute_start_time)))

        logger.info("Starting the fetchall")
        fetchall_start_time = datetime.datetime.now()
        nodeid_data = nodeid_data.fetchall()
        logger.info(
            "fetchall nodes with no location id, with their in and out edges - Total Runtime (HMS): \t{} \t ".format(
                get_total_runtime_string(fetchall_start_time)))

        flow_in_lists = {}
        flow_out_lists = {}
        endcap_ref = {}

        for row_a in nodeid_data:
            node_id = row_a[0]
            in_or_out_edge = row_a[1]
            constraint_day = row_a[2]
            commodity_id = row_a[3]
            mode = row_a[4]
            edge_id = row_a[5]
            nx_edge_id = row_a[6]
            miles = row_a[7]
            intermodal = row_a[8]
            source_facility_id = row_a[9]
            commodity_id = row_a[10]
            endcap_flag = row_a[11]  # is 'E' if endcap, else 'Y' or 'null'
            endcap_input_process = row_a[12]
            endcap_source_facility = row_a[13]
            # endcap ref is keyed on node_id, makes a list of [commodity_id, list, list];
            # first list will be source facilities
            if endcap_source_facility > 0:
                endcap_ref.setdefault(node_id, [endcap_input_process, commodity_id, [], []])
                if endcap_source_facility not in endcap_ref[node_id][2]:
                    endcap_ref[node_id][2].append(endcap_source_facility)

            # if node is not intermodal, conservation of flow holds per mode;
            # if intermodal, then across modes
            # if node is endcap, conservation of flow holds across source facilities
            if intermodal == 'N':
                if in_or_out_edge == 'in':
                    flow_in_lists.setdefault((node_id, intermodal, source_facility_id, constraint_day, commodity_id,
                                              endcap_input_process, mode), []).append(flow_var[edge_id])
                elif in_or_out_edge == 'out':
                    flow_out_lists.setdefault((node_id, intermodal, source_facility_id, constraint_day, commodity_id,
                                               endcap_input_process, mode), []).append(flow_var[edge_id])
            else:
                if in_or_out_edge == 'in':
                    flow_in_lists.setdefault(
                        (node_id, intermodal, source_facility_id, constraint_day, commodity_id, endcap_input_process),
                        []).append(flow_var[edge_id])
                elif in_or_out_edge == 'out':
                    flow_out_lists.setdefault(
                        (node_id, intermodal, source_facility_id, constraint_day, commodity_id, endcap_input_process),
                        []).append(flow_var[edge_id])

        endcap_dict = {}

        for node, value in iteritems(endcap_ref):
            # add output commodities to endcap_ref[node][3]
            # endcap_ref[node][2] is a list of source facilities this endcap matches for the input commodity
            process_id = value[0]
            if process_id > 0:
                endcap_ref[node][3] = process_outputs_dict[process_id]

        # if this node has at least one edge flowing out
        for key, value in iteritems(flow_in_lists):

            if node_constraint_counter < 50000:
                if node_constraint_counter % 5000 == 0:
                    logger.info("flow constraint created for {} nodes".format(node_constraint_counter))
                    # logger.info('{} edges constrained'.format(edge_counter)

            if node_constraint_counter % 50000 == 0:
                if node_constraint_counter == 50000:
                    logger.info("5000 nodes constrained, switching to log every 50000 instead of 5000")
                logger.info("flow constraint created for {} nodes".format(node_constraint_counter))
                # logger.info('{} edges constrained'.format(edge_counter))

            node_id = key[0]
            intermodal_flag = key[1]
            source_facility_id = key[2]
            day = key[3]
            commodity_id = key[4]
            endcap_input_process = key[5]

            if len(key) == 7:
                node_mode = key[6]
            else:
                node_mode = 'intermodal'

            # if this node is an endcap
            # if this commodity and source match the inputs for the endcap process
            # or if this output commodity matches an input process for this node, enter the first loop
            # if output commodity, don't do anything - will be handled via input commodity
            if (node_id in endcap_ref and
                    ((endcap_input_process == endcap_ref[node_id][0] and commodity_id == endcap_ref[node_id][
                        1] and source_facility_id in endcap_ref[node_id][2])
                     or commodity_id in endcap_ref[node_id][3])):
                # if we need to handle this commodity & source according to the endcap process
                # and it is an input or output of the current process
                # if this is an output of a different process, it fails the above "if" and gets a standard constraint
                # below
                if commodity_id in process_dict and (node_id, day) not in endcap_dict:

                    # tracking which endcap nodes we've already made constraints for
                    endcap_dict[node_id, day] = commodity_id

                    # else, this is an output of this candidate process, and it's endcap constraint is created by the
                    # input commodity
                    # which must have edges in, or this would never have been tagged as an endcap node as long as the
                    # input commodity has some flow in

                    # for this node, on this day, with this source facility - intermodal and mode and encapflag
                    # unchanged, commodity may not
                    # for endcap nodes, can't check for flow in exactly the same way because commodity may change
                    input_quantity = process_dict[commodity_id][1]
                    # since we're working with input to an endcap, need all matching inputs, which may include other
                    # source facilities as specified in endcap_ref[node_id][2]
                    in_key_list = [key]
                    if len(endcap_ref[node_id][2]) > 1:
                        for alt_source in endcap_ref[node_id][2]:
                            if alt_source != source_facility_id:

                                if intermodal_flag == 'N':
                                    new_key = (
                                        node_id, intermodal_flag, alt_source, day, commodity_id, endcap_input_process,
                                        node_mode)
                                else:
                                    new_key = (
                                        node_id, intermodal_flag, alt_source, day, commodity_id, endcap_input_process)

                                in_key_list.append(new_key)

                    # this is an endcap for this commodity and source, so unprocessed flow out is not allowed
                    # unless it's from a different source

                    prob += lpSum(flow_out_lists[k] for k in in_key_list if k in flow_out_lists) == lpSum(
                        0), "conservation of flow (zero allowed out, must be processed), endcap, nx node {}, " \
                            "source facility {},  commodity {}, day {}, mode {}".format(
                        node_id, source_facility_id, commodity_id, day, node_mode)

                    outputs_dict = {}
                    # starting with the 3rd item in the list, the first output commodity tuple
                    for i in range(2, len(process_dict[commodity_id])):
                        output_commodity_id = process_dict[commodity_id][i][0]
                        output_quantity = process_dict[commodity_id][i][1]
                        outputs_dict[output_commodity_id] = output_quantity
                        # now we have a dict of allowed outputs for this input; construct keys for matching flows
                        # allow cross-modal flow?no, these are not candidates yet, want to use exists routes

                    for o_commodity_id, quant in iteritems(outputs_dict):
                        # creating one constraint per output commodity, per endcap node
                        node_constraint_counter = node_constraint_counter + 1
                        output_source_facility_id = 0
                        output_process_id = 0

                        # setting up the outflow edges to check
                        if node_mode == 'intermodal':
                            out_key = (
                                node_id, intermodal_flag, output_source_facility_id, day, o_commodity_id,
                                output_process_id)
                        else:
                            out_key = (
                                node_id, intermodal_flag, output_source_facility_id, day, o_commodity_id,
                                output_process_id, node_mode)

                        if out_key not in flow_out_lists:
                            # node has edges flowing in, but no valid out edges; restrict flow in to zero
                            prob += lpSum(flow_in_lists[k] for k in in_key_list if k in flow_in_lists) == lpSum(
                                0), "conservation of flow (zero allowed in), endcap, nx node {}, source facility {}, " \
                                    "commodity {}, day {}, mode {}".format(
                                node_id, source_facility_id, commodity_id, day, node_mode)
                            other_endcap_constraint_counter = other_endcap_constraint_counter + 1
                        else:
                            # out_key is in flow_out_lists
                            # endcap functioning as virtual processor
                            # now aggregate flows for this commodity
                            # processsed_flow_out = lpSum(flow_out_lists[out_key])/quant
                            # divide by /input_quantity for the required processing ratio

                            # if key is in flow_out_lists, some input material may pass through unprocessed
                            # if out_key is in flow_in_lists, some output material may pass through unprocessed
                            # difference is the amount of input material processed: lpSum(flow_in_lists[key]) -
                            # lpSum(flow_out_lists[key])
                            agg_inflow_lists = []
                            for k in in_key_list:
                                if k in flow_in_lists:
                                    for l in flow_in_lists[k]:
                                        if l not in agg_inflow_lists:
                                            agg_inflow_lists.append(l)

                            if out_key in flow_in_lists:
                                prob += lpSum(agg_inflow_lists) / input_quantity == (
                                        lpSum(flow_out_lists[out_key]) - lpSum(flow_in_lists[out_key])) / quant, \
                                        "conservation of flow, endcap as processor, passthrough processed,nx node {}," \
                                        "source facility {}, in commodity {}, out commodity {}, day {},mode {}".format(
                                            node_id, source_facility_id, commodity_id, o_commodity_id, day, node_mode)
                                passthrough_constraint_counter = passthrough_constraint_counter + 1
                            else:
                                prob += lpSum(agg_inflow_lists) / input_quantity == lpSum(flow_out_lists[
                                                                                              out_key]) / quant, \
                                        "conservation of flow, endcap as processor, no passthrough, nx node {}, " \
                                        "source facility {},  in commodity {}, out commodity {}, day {}, " \
                                        "mode {}".format(
                                            node_id, source_facility_id, commodity_id, o_commodity_id, day,
                                            node_mode)
                                other_endcap_constraint_counter = other_endcap_constraint_counter + 1





            else:
                # if this node is not an endcap, or has no viable candidate process, handle as standard
                # if this node has at least one edge flowing in, and out, ensure flow is compliant
                # here is where we need to add the processor ratios and allow conversion, for endcaps
                if key in flow_out_lists:
                    prob += lpSum(flow_in_lists[key]) == lpSum(flow_out_lists[
                                                                   key]), "conservation of flow, nx node {}, " \
                                                                          "source facility {}, commodity {},  day {}, " \
                                                                          "mode {}".format(
                        node_id, source_facility_id, commodity_id, day, node_mode)
                    node_constraint_counter = node_constraint_counter + 1
                # node has edges flowing in, but not out; restrict flow in to zero
                else:
                    prob += lpSum(flow_in_lists[key]) == lpSum(
                        0), "conservation of flow (zero allowed in), nx node {}, source facility {},  commodity {}, " \
                            "day {}, mode {}".format(
                        node_id, source_facility_id, commodity_id, day, node_mode)
                    node_constraint_counter = node_constraint_counter + 1

        for key, value in iteritems(flow_out_lists):
            node_id = key[0]
            source_facility_id = key[2]
            day = key[3]
            commodity_id = key[4]
            if len(key) == 7:
                node_mode = key[6]
            else:
                node_mode = 'intermodal'

            # node has edges flowing out, but not in; restrict flow out to zero
            if key not in flow_in_lists:
                prob += lpSum(flow_out_lists[key]) == lpSum(
                    0), "conservation of flow (zero allowed out), nx node {}, source facility {}, commodity {},  " \
                        "day {}, mode {}".format(
                    node_id, source_facility_id, commodity_id, day, node_mode)
                node_constraint_counter = node_constraint_counter + 1

        logger.info(
            "total non-endcap conservation of flow constraints created on nodes: {}".format(node_constraint_counter))

    # Note: no consesrvation of flow for primary vertices for supply & demand - they have unique constraints

    logger.debug("endcap constraints: passthrough {}, other {}, total no reverse {}".format(
        passthrough_constraint_counter, other_endcap_constraint_counter, endcap_no_reverse_counter))
    logger.info("FINISHED:  create_constraint_conservation_of_flow_endcap_nodes")

    return prob


# ===============================================================================


def create_constraint_pipeline_capacity(logger, the_scenario, prob, flow_var):
    logger.debug("STARTING:  create_constraint_pipeline_capacity")
    logger.debug("Length of flow_var: {}".format(len(list(flow_var.items()))))
    logger.info("modes with background flow turned on: {}".format(the_scenario.backgroundFlowModes))
    logger.info("minimum available capacity floor set at: {}".format(the_scenario.minCapacityLevel))

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()

        # capacity for pipeline tariff routes
        # with source tracking, may have multiple flows per segment, slightly diff commodities
        sql = """select e.edge_id, e.tariff_id, l.link_id, l.capac, e.start_day, l.capac-l.background_flow 
        allowed_flow, l.source, e.mode, instr(e.mode, l.source)
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
        logger.info("execute for edges for transport edge capacity - Total Runtime (HMS): \t{} \t ".format(
            get_total_runtime_string(execute_start_time)))

        logger.info("Starting the fetchall")
        fetchall_start_time = datetime.datetime.now()
        pipeline_capac_data = pipeline_capac_data.fetchall()
        logger.info("fetchall edges for transport edge capacity - Total Runtime (HMS): \t{} \t ".format(
            get_total_runtime_string(fetchall_start_time)))

        flow_lists = {}

        for row_a in pipeline_capac_data:
            edge_id = row_a[0]
            tariff_id = row_a[1]
            link_id = row_a[2]
            # Link capacity is recorded in "thousand barrels per day"; 1 barrel = 42 gall
            # Link capacity * 42 is now in kgal per day, to match flow in kgal
            link_capacity_kgal_per_day = THOUSAND_GALLONS_PER_THOUSAND_BARRELS * row_a[3]
            start_day = row_a[4]
            capac_minus_background_flow_kgal = max(THOUSAND_GALLONS_PER_THOUSAND_BARRELS * row_a[5], 0)
            min_restricted_capacity = max(capac_minus_background_flow_kgal,
                                          link_capacity_kgal_per_day * the_scenario.minCapacityLevel)

            capacity_nodes_mode_source = row_a[6]
            edge_mode = row_a[7]
            mode_match_check = row_a[8]
            if 'pipeline' in the_scenario.backgroundFlowModes:
                link_use_capacity = min_restricted_capacity
            else:
                link_use_capacity = link_capacity_kgal_per_day

            # add flow from all relevant edges, for one start; may be multiple tariffs
            flow_lists.setdefault((link_id, link_use_capacity, start_day, edge_mode), []).append(flow_var[edge_id])

        for key, flow in iteritems(flow_lists):
            prob += lpSum(flow) <= key[1], "constraint max flow on pipeline link {} for mode {} for day {}".format(
                key[0], key[3], key[2])

        logger.debug("pipeline capacity constraints created for all transport routes")

    logger.debug("FINISHED:  create_constraint_pipeline_capacity")
    return prob


# ===============================================================================


def setup_pulp_problem_candidate_generation(the_scenario, logger):
    logger.info("START: setup PuLP problem")

    # flow_var is the flow on each edge by commodity and day.
    # the optimal value of flow_var will be solved by PuLP
    flow_vars = create_flow_vars(the_scenario, logger)

    # unmet_demand_var is the unmet demand at each destination, being determined
    unmet_demand_vars = create_unmet_demand_vars(the_scenario, logger)

    # processor_build_vars is the binary variable indicating whether a candidate processor is used and thus its build
    # cost charged
    processor_build_vars = create_candidate_processor_build_vars(the_scenario, logger)

    # binary tracker variables for whether a processor is used
    # if used, it must abide by capacity constraints, and include build cost if it is a candidate
    processor_vertex_flow_vars = create_binary_processor_vertex_flow_vars(the_scenario, logger)

    # tracking unused production
    processor_excess_vars = create_processor_excess_output_vars(the_scenario, logger)

    # THIS IS THE OBJECTIVE FUNCTION FOR THE OPTIMIZATION
    # ==================================================

    prob = create_opt_problem(logger, the_scenario, unmet_demand_vars, flow_vars, processor_build_vars)
    logger.detailed_debug("DEBUG: size of prob: {}".format(sys.getsizeof(prob)))

    prob = create_constraint_unmet_demand(logger, the_scenario, prob, flow_vars, unmet_demand_vars)
    logger.detailed_debug("DEBUG: size of prob: {}".format(sys.getsizeof(prob)))

    prob = create_constraint_max_flow_out_of_supply_vertex(logger, the_scenario, prob, flow_vars)

    from ftot_pulp import create_constraint_daily_processor_capacity
    logger.debug("----- Using create_constraint_daily_processor_capacity method imported from ftot_pulp ------")
    prob = create_constraint_daily_processor_capacity(logger, the_scenario, prob, flow_vars, processor_build_vars,
                                                      processor_vertex_flow_vars)

    prob = create_primary_processor_vertex_constraints(logger, the_scenario, prob, flow_vars)

    prob = create_constraint_conservation_of_flow_storage_vertices(logger, the_scenario, prob,
                                                                                   flow_vars, processor_excess_vars)

    prob = create_constraint_conservation_of_flow_endcap_nodes(logger, the_scenario, prob, flow_vars,
                                                               processor_excess_vars)

    if the_scenario.capacityOn:
        logger.info("calling create_constraint_max_route_capacity")
        logger.debug('using create_constraint_max_route_capacity method from ftot_pulp')
        from ftot_pulp import create_constraint_max_route_capacity
        prob = create_constraint_max_route_capacity(logger, the_scenario, prob, flow_vars)

        logger.info("calling create_constraint_pipeline_capacity")
        prob = create_constraint_pipeline_capacity(logger, the_scenario, prob, flow_vars)

    del unmet_demand_vars

    del flow_vars

    # SCENARIO SPECIFIC CONSTRAINTS

    # The problem data is written to an .lp file
    prob.writeLP(os.path.join(the_scenario.scenario_run_directory, "debug", "LP_output_c2.lp"))
    logger.info("FINISHED: setup PuLP problem for candidate generation")

    return prob


# ===============================================================================

def record_pulp_candidate_gen_solution(the_scenario, logger, zero_threshold):
    logger.info("START: record_pulp_candidate_gen_solution")
    non_zero_variable_count = 0

    with sqlite3.connect(the_scenario.main_db) as db_con:

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
            null as source_facility_id,
            null as source_facility_name,
            null as units,
            variable_name,
            null as  nx_edge_id,
            null as mode,
            null as mode_oid,
            null as miles,
            null as edge_count_from_source,
            null as  miles_travelled
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
            edges.source_facility_id,
            s.source_facility_name,
            commodities.units,
            variable_name,
            edges.nx_edge_id,
            edges.mode,
            edges.mode_oid,
            edges.miles,
            edges.edge_count_from_source,
            edges.miles_travelled
            from optimal_solution
            join edges on edges.edge_id = cast(substr(variable_name, 6) as int)
            join commodities on edges.commodity_id = commodities.commodity_ID
            left outer join vertices as ov on edges.o_vertex_id = ov.vertex_id
            left outer join vertices as dv on edges.d_vertex_id = dv.vertex_id
            left outer join source_commodity_ref as s on edges.source_facility_id = s.source_facility_id
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
            null as source_facility_id,
            null as source_facility_name,
            null as units,
            variable_name,
            null as  nx_edge_id,
            null as mode,
            null as mode_oid,
            null as miles,
            null as edge_count_from_source,
            null as miles_travelled
            from optimal_solution
            where variable_name like 'Build%';
            """
        db_con.execute("drop table if exists optimal_variables;")
        db_con.executescript(sql)


        sql = """
            create table optimal_variables_c as
            select * from optimal_variables
            ;
             drop table if exists candidate_nodes;

            create table candidate_nodes as
            select *
            from

            (select pl.minsize, pl.process_id, ov.to_node_id, ov.mode_oid, ov.mode, ov.commodity_id,
            sum(variable_value) agg_value, count(variable_value) num_aggregated
            from optimal_variables ov, candidate_process_list pl, candidate_process_commodities pc

            where pc.io = 'i'
            and pl.process_id = pc.process_id
            and ov.commodity_id = pc.commodity_id
            and ov.edge_type = 'transport'
            and variable_value > {}
            group by ov.mode_oid, ov.to_node_id, ov.commodity_id, pl.process_id, pl.minsize, ov.mode) i,

            (select pl.min_aggregation, pl.process_id, ov.from_node_id, ov.commodity_id,
            sum(variable_value) agg_value, count(variable_value) num_aggregated, min(edge_count_from_source) as 
            edge_count,
            o_vertex_id
            from optimal_variables ov, candidate_process_list pl, candidate_process_commodities pc
            where pc.io = 'i'
            and pl.process_id = pc.process_id
            and ov.commodity_id = pc.commodity_id
            and ov.edge_type = 'transport'
            group by ov.from_node_id, ov.commodity_id, pl.process_id, pl.min_aggregation, ov.o_vertex_id) o
            left outer join networkx_nodes nn
            on o.from_node_id = nn.node_id

            where (i.to_node_id = o.from_node_id
            and i.process_id = o.process_id
            and i.commodity_id = o.commodity_id
            and o.agg_value > i.agg_value
            and o.agg_value >= o.min_aggregation)
            or (o.o_vertex_id is not null and  o.agg_value >= o.min_aggregation)

            group by o.min_aggregation, o.process_id, o.from_node_id, o.commodity_id,  o.agg_value, o.num_aggregated, 
            o.o_vertex_id
            order by o.agg_value, edge_count
            ;

           
            """.format(zero_threshold)
        db_con.execute("drop table if exists optimal_variables_c;")
        db_con.executescript(sql)

        sql_check_candidate_node_count = "select count(*) from candidate_nodes"
        db_cur = db_con.execute(sql_check_candidate_node_count)
        candidate_node_count = db_cur.fetchone()[0]
        if candidate_node_count > 0:
            logger.info("number of candidate nodes identified: {}".format(candidate_node_count))
        else:
            logger.warning("the candidate nodes table is empty. Hints: "
                           "(1) increase the maximum raw material transport distance to allow additional flows to "
                           "aggregate and meet the minimum candidate facility size"
                           "(2) increase the ammount of material available to flow from raw material producers")

    logger.info("FINISH: record_pulp_candidate_gen_solution")


# ===============================================================================


def parse_optimal_solution_db(the_scenario, logger):
    logger.info("starting parse_optimal_solution")

    optimal_processors = []
    optimal_processor_flows = []
    optimal_route_flows = {}
    optimal_unmet_demand = {}
    optimal_storage_flows = {}
    optimal_excess_material = {}

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

            variable_value = edge[1]

            route_id = edge[3]

            time_period = edge[4]

            commodity_flowed = edge[5]

            od_pair_name = "{}, {}".format(edge[8], edge[9])

            if route_id not in optimal_route_flows:  # first time route_id is used on a day or commodity
                optimal_route_flows[route_id] = [[od_pair_name, time_period, commodity_flowed, variable_value]]

            else:  # subsequent times route is used on different day or for other commodity
                optimal_route_flows[route_id].append([od_pair_name, time_period, commodity_flowed, variable_value])

        # do the processors
        sql = "select variable_name, variable_value from optimal_solution where variable_name like 'BuildProcessor%';"
        data = db_con.execute(sql)
        optimal_candidates_processors = data.fetchall()
        for proc in optimal_candidates_processors:
            optimal_processors.append(proc)

        # do the processor vertex flows
        sql = "select variable_name, variable_value from optimal_solution where variable_name like " \
              "'ProcessorVertexFlow%';"
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


    logger.info("length of optimal_processors list: {}".format(len(optimal_processors)))  # a list of optimal processors
    logger.info("length of optimal_processor_flows list: {}".format(
        len(optimal_processor_flows)))  # a list of optimal processor flows
    logger.info("length of optimal_route_flows dict: {}".format(
        len(optimal_route_flows)))  # a dictionary of routes keys and commodity flow values
    logger.info("length of optimal_unmet_demand dict: {}".format(
        len(optimal_unmet_demand)))  # a dictionary of route keys and unmet demand values

    return optimal_processors, optimal_route_flows, optimal_unmet_demand, optimal_storage_flows, optimal_excess_material
