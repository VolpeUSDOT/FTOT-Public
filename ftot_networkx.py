# ------------------------------------------------------------------------------
# Name: ftot_networkx.py
#
# Purpose: the purpose of this module is to handle all the NetworkX methods and operations
# necessary to go between FTOT layers: GIS, sqlite DB, etc.
# Revised: 6/15/21
# ------------------------------------------------------------------------------

import networkx as nx
import sqlite3
from shutil import rmtree
import datetime
import ftot_supporting
import arcpy
import os
import multiprocessing
import math
from ftot_pulp import commodity_mode_setup


# -----------------------------------------------------------------------------


def graph(the_scenario, logger):

    # check for permitted modes before creating nX graph
    check_permitted_modes(the_scenario, logger)

    # export the assets from GIS export_fcs_from_main_gdb
    export_fcs_from_main_gdb(the_scenario, logger)

    # create the networkx multidigraph
    G = make_networkx_graph(the_scenario, logger)

    # clean up the networkx graph to preserve connectivity
    clean_networkx_graph(the_scenario, G, logger)

    # cache the digraph to the db and store the route_cost_scaling factor
    digraph_to_db(the_scenario, G, logger)

    # cost the network in the db
    set_network_costs_in_db(the_scenario, logger)

    # generate shortest paths through the network
    presolve_network(the_scenario, G, logger)

    # eliminate the graph and related shape files before moving on
    delete_shape_files(the_scenario, logger)
    G = nx.null_graph()


# -----------------------------------------------------------------------------


def delete_shape_files(the_scenario, logger):
    # delete temporary files
    logger.debug("start: delete the temp_networkx_shp_files dir")
    input_path = the_scenario.networkx_files_dir
    rmtree(input_path)
    logger.debug("finish: delete the temp_networkx_shp_files dir")


# -----------------------------------------------------------------------------


# Scan the XML and input_data to ensure that pipelines are permitted and relevant
def check_permitted_modes(the_scenario, logger):
    logger.debug("start: check permitted modes")
    commodity_mode_dict = commodity_mode_setup(the_scenario, logger)
    with sqlite3.connect(the_scenario.main_db) as db_cur:
        # get pipeline records with an allow_yn == y
        sql = "select * from commodity_mode where mode like 'pipeline%' and allowed_yn like 'y';"
        pipeline_allowed = db_cur.execute(sql).fetchall()
        if not pipeline_allowed:
            logger.info("pipelines are not allowed")
            new_permitted_mode_list = []
            for mode in the_scenario.permittedModes:
                if 'pipeline' not in mode:
                    new_permitted_mode_list.append(mode)
                elif 'pipeline' in mode:
                    continue  # we don't want to include pipeline fcs if no csv exists to specify product flow
            the_scenario.permittedModes = new_permitted_mode_list
    logger.debug("finish: check permitted modes")


# -----------------------------------------------------------------------------


def make_networkx_graph(the_scenario, logger):
    # High level work flow:
    # ------------------------
    # make_networkx_graph
    # create the multidigraph
    # convert the node labels to integers
    # reverse the graph and compose with self

    logger.info("start: make_networkx_graph")
    start_time = datetime.datetime.now()

    # read the shapefiles in the customized read_shp method
    input_path = the_scenario.networkx_files_dir

    logger.debug("start: read_shp")
    G = read_shp(input_path, logger, simplify=True,
                 geom_attrs=False, strict=True)  # note this is custom and not nx.read_shp()

    # cleanup the node labels
    logger.debug("start: convert node labels")
    G = nx.convert_node_labels_to_integers(G, first_label=0, ordering='default', label_attribute="x_y_location")

    # create a reversed graph
    logger.debug("start: reverse G graph to H")
    H = G.reverse()  # this is a reversed version of the graph.

    # set a new attribute for every edge that says its a "reversed" link
    # we will use this to delete edges that shouldn't be reversed later.
    logger.debug("start: set 'reversed' attribute in H")
    nx.set_edge_attributes(H, 1, "REVERSED")

    # add the two graphs together
    logger.debug("start: compose G and H")
    G = nx.compose(G, H)

    # print out some stats on the Graph
    logger.info("Number of nodes in the raw graph: {}".format(G.order()))
    logger.info("Number of edges in the raw graph: {}".format(G.size()))

    logger.debug(
        "finished: make_networkx_graph: Runtime (HMS): \t{}".format(
            ftot_supporting.get_total_runtime_string(start_time)))

    return G


# -----------------------------------------------------------------------------


def presolve_network(the_scenario, G, logger):
    logger.debug("start: presolve_network")

    # Check NDR conditions before calculating shortest paths
    update_ndr_parameter(the_scenario, logger)

    # Create a table to hold the shortest edges
    with sqlite3.connect(the_scenario.main_db) as db_cur:
        # clean up the db
        sql = "drop table if exists shortest_edges;"
        db_cur.execute(sql)

        sql = """
            create table if not exists shortest_edges (from_node_id INT, to_node_id INT, edge_id INT, 
            CONSTRAINT unique_from_to_edge_id_tuple UNIQUE (from_node_id, to_node_id, edge_id));
            """
        db_cur.execute(sql)

    # Create a table to hold routes
    with sqlite3.connect(the_scenario.main_db) as db_cur:
        # clean up the db
        sql = "drop table if exists route_edges;"
        db_cur.execute(sql)

        sql = """
            create table if not exists route_edges (from_node_id INT, to_node_id INT, edge_id INT,
            scenario_rt_id INT, rt_order_ind INT);
            """
        db_cur.execute(sql)

    # If NDR_On = False (default case), then skip calculating shortest paths
    if not the_scenario.ndrOn:
        with sqlite3.connect(the_scenario.main_db) as db_cur:
            sql = """
                insert or ignore into shortest_edges
                select from_node_id, to_node_id, edge_id
                from networkx_edges;
                """
            logger.debug("NDR skipped and all edges added to shortest_edges table")
            db_cur.execute(sql)
            db_cur.commit()
        logger.debug("finish: presolve_network")
        return

    # Get phases_of_matter in the scenario
    phases_of_matter_in_scenario = get_phases_of_matter_in_scenario(the_scenario, logger)

    # Determine the weights for each phase of matter in scenario associated with the edges in the nX graph
    nx_graph_weighting(the_scenario, G, phases_of_matter_in_scenario, logger)

    # Make subgraphs for combination of permitted modes
    commodity_subgraph_dict = make_mode_subgraphs(the_scenario, G, logger)

    # Create a dictionary of edge_ids from the database which is used later to uniquely identify edges
    edge_id_dict = find_edge_ids(the_scenario, logger)

    # Make origin-destination pairs where od_pairs is a dictionary keyed off [commodity_id, target, source],
    # value is a list of [scenario_rt_id, phase_of_matter] for that key
    od_pairs = make_od_pairs(the_scenario, logger)

    # Use multi-processing to determine shortest_paths for each target in the od_pairs dictionary
    manager = multiprocessing.Manager()
    all_route_edges = manager.list()
    no_path_pairs = manager.list()
    logger.debug("multiprocessing.cpu_count() =  {}".format(multiprocessing.cpu_count()))

    # To parallelize computations, assign a set of targets to be passed to each processor
    stuff_to_pass = []
    logger.debug("start: identify shortest_path between each o-d pair by commodity")
    # by commodity and destination
    for commodity_id in od_pairs:
        phase_of_matter = od_pairs[commodity_id]['phase_of_matter']
        allowed_modes = commodity_subgraph_dict[commodity_id]['modes']
        for a_target in od_pairs[commodity_id]['targets'].keys():
            stuff_to_pass.append([commodity_subgraph_dict[commodity_id]['subgraph'],
                                  od_pairs[commodity_id]['targets'][a_target],
                                  a_target, all_route_edges, no_path_pairs,
                                  edge_id_dict, phase_of_matter, allowed_modes])

    # Allow multiprocessing, with no more than 75% of cores to be used, rounding down if necessary
    logger.info("start: the multiprocessing route solve.")
    processors_to_save = int(math.ceil(multiprocessing.cpu_count() * 0.25))
    processors_to_use = multiprocessing.cpu_count() - processors_to_save
    logger.info("number of CPUs to use = {}".format(processors_to_use))

    pool = multiprocessing.Pool(processes=processors_to_use)
    try:
        pool.map(multi_shortest_paths, stuff_to_pass)
    except Exception as e:
        pool.close()
        pool.terminate()
        logger.error("FAIL: {} ".format(e))
        raise Exception("FAIL: {}".format(e))
    pool.close()
    pool.join()

    logger.info("end: identify shortest_path between each o-d pair")

    # Log any origin-destination pairs without a shortest path
    if no_path_pairs:
        logger.warning("Cannot identify shortest paths for {} o-d pairs; see log file list".format(len(no_path_pairs)))
        for i in no_path_pairs:
            s, t, rt_id = i
            logger.debug("Missing shortest path for source {}, target {}, scenario_route_id {}".format(s, t, rt_id))

    with sqlite3.connect(the_scenario.main_db) as db_cur:
        sql = """
            insert into route_edges
            (from_node_id, to_node_id, edge_id, scenario_rt_id, rt_order_ind)
            values (?,?,?,?,?);
            """

        logger.debug("start: update route_edges table")
        db_cur.executemany(sql, all_route_edges)
        db_cur.commit()
        logger.debug("end: update route_edges table")

    with sqlite3.connect(the_scenario.main_db) as db_cur:
        sql = """
            insert or ignore into shortest_edges
            (from_node_id, to_node_id, edge_id)
            select distinct from_node_id, to_node_id, edge_id
            from route_edges;
            """
        logger.debug("start: update shortest_edges table")
        db_cur.execute(sql)
        db_cur.commit()
        logger.debug("end: update shortest_edges table")

    logger.debug("finish: presolve_network")


# -----------------------------------------------------------------------------


# Ensure shortest paths not calculated in presence of candidate generation, capacity, max transport distance
def update_ndr_parameter(the_scenario, logger):
    logger.debug("start: check NDR conditions")
    new_ndr_parameter = the_scenario.ndrOn

    # if capacity is enabled, then skip NDR
    if the_scenario.capacityOn:
        new_ndr_parameter = False
        logger.debug("NDR de-activated due to capacity enforcement")

    # if candidate generation is active, then skip NDR
    if the_scenario.processors_candidate_slate_data != 'None':
        new_ndr_parameter = False
        logger.debug("NDR de-activated due to candidate generation")

    # if max_transport_distance field is used in commodities table, then skip NDR
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        # get count of commodities with a specified max_transport_distance
        sql = "select count(commodity_id) from commodities where max_transport_distance is not null;"
        db_cur = main_db_con.execute(sql)
        count = db_cur.fetchone()[0]
        if count > 0:
            new_ndr_parameter = False
            logger.debug("NDR de-activated due to use of max transport distance")

    the_scenario.ndrOn = new_ndr_parameter
    logger.debug("finish: check NDR conditions")


# -----------------------------------------------------------------------------


# This method uses a shortest_path algorithm from the nx library to flag edges in the
# network that are a part of the shortest path connecting an origin to a destination
# for each commodity
def multi_shortest_paths(stuff_to_pass):
    global all_route_edges, no_path_pairs
    G, sources, target, all_route_edges, no_path_pairs, edge_id_dict, phase_of_matter, allowed_modes = stuff_to_pass
    t = target

    shortest_paths_to_t = nx.shortest_path(G, target=t, weight='{}_weight'.format(phase_of_matter))
    for a_source in sources:
        s = a_source
        # This accounts for when a_source may not be connected to t,
        # as is the case when certain modes may not be permitted
        if a_source not in shortest_paths_to_t:
            for i in sources[a_source]:
                rt_id = i
                no_path_pairs.append((s, t, rt_id))
            continue
        for i in sources[a_source]:
            rt_id = i
            for index, from_node in enumerate(shortest_paths_to_t[s]):
                if index < (len(shortest_paths_to_t[s]) - 1):
                    to_node = shortest_paths_to_t[s][index + 1]
                    # find the correct edge_id on the shortest path
                    min_route_cost = 999999999
                    min_edge_id = None
                    for j in edge_id_dict[to_node][from_node]:
                        edge_id, mode_source, route_cost = j
                        if mode_source in allowed_modes:
                            if route_cost < min_route_cost:
                                min_route_cost = route_cost
                                min_edge_id = edge_id
                    if min_edge_id is None:
                        error = """something went wrong finding the edge_id from node {} to node {}
                                for scenario_rt_id {} in shortest path algorithm""".format(from_node, to_node, rt_id)
                        raise Exception(error)
                    all_route_edges.append((from_node, to_node, min_edge_id, rt_id, index + 1))


# -----------------------------------------------------------------------------


# Assigns for each link in the networkX graph a weight for each phase of matter
# which mirrors the cost of each edge for the optimizer
def nx_graph_weighting(the_scenario, G, phases_of_matter_in_scenario, logger):

    # pull the route cost for all edges in the graph
    logger.debug("start: assign edge weights to networkX graph")
    for phase_of_matter in phases_of_matter_in_scenario:
        # initialize the edge weight variable to something large to be overwritten in the loop
        nx.set_edge_attributes(G, 999999999, name='{}_weight'.format(phase_of_matter))

    for (u, v, c, d) in G.edges(keys=True, data='route_cost_scaling', default=False):
        from_node_id = u
        to_node_id = v
        route_cost_scaling = G.edges[(u, v, c)]['route_cost_scaling']
        mileage = G.edges[(u, v, c)]['MILES']
        source = G.edges[(u, v, c)]['source']
        artificial = G.edges[(u, v, c)]['Artificial']

        # calculate route_cost for all edges and phases of matter to mirror network costs in db
        for phase_of_matter in phases_of_matter_in_scenario:
            weight = get_network_link_cost(the_scenario, phase_of_matter, source, artificial, logger)
            if 'pipeline' not in source:
                if artificial == 0:
                    G.edges[(u, v, c)]['{}_weight'.format(phase_of_matter)] = mileage * route_cost_scaling * weight
                elif artificial == 1:
                    G.edges[(u, v, c)]['{}_weight'.format(phase_of_matter)] = weight
                elif artificial == 2:
                    G.edges[(u, v, c)]['{}_weight'.format(phase_of_matter)] = weight / 2.00
                else:
                    logger.warning("artificial code of {} is not supported!".format(artificial))
            else:
                # inflate the cost of pipelines for 'solid' to avoid forming a shortest path with them
                if phase_of_matter == 'solid':
                    G.edges[(u, v, c)]['{}_weight'.format(phase_of_matter)] = 1000000
                else:
                    if artificial == 0:
                        G.edges[(u, v, c)]['{}_weight'.format(phase_of_matter)] = route_cost_scaling
                    elif artificial == 1:
                        G.edges[(u, v, c)]['{}_weight'.format(phase_of_matter)] = mileage * weight
                    elif artificial == 2:
                        G.edges[(u, v, c)]['{}_weight'.format(phase_of_matter)] = weight / 2.00
                    else:
                        logger.warning("artificial code of {} is not supported!".format(artificial))

    logger.debug("end: assign edge weights to networkX graph")


# -----------------------------------------------------------------------------


# Returns a dictionary of NetworkX graphs keyed off commodity_id with 'modes' and 'subgraph' keys
def make_mode_subgraphs(the_scenario, G, logger):
    logger.debug("start: create mode subgraph dictionary")

    logger.debug("start: pull commodity mode from SQL")
    with sqlite3.connect(the_scenario.main_db) as db_cur:
        sql = "select mode, commodity_id from commodity_mode where allowed_yn like 'y';"
        commodity_mode_data = db_cur.execute(sql).fetchall()
        commodity_subgraph_dict = {}
        for row in commodity_mode_data:
            mode = row[0]
            commodity_id = int(row[1])
            if commodity_id not in commodity_subgraph_dict:
                commodity_subgraph_dict[commodity_id] = {}
                commodity_subgraph_dict[commodity_id]['modes'] = []
            commodity_subgraph_dict[commodity_id]['modes'].append(mode)
    logger.debug("end: pull commodity mode from SQL")

    for k in commodity_subgraph_dict:
        commodity_subgraph_dict[k]['modes'] = sorted(commodity_subgraph_dict[k]['modes'])

    # check if commodity mode input file exists
    if not os.path.exists(the_scenario.commodity_mode_data):
        # all commodities are allowed on all permitted modes and use graph G
        logger.debug("no commodity_mode_data file: {}".format(the_scenario.commodity_mode_data))
        logger.debug("all commodities allowed on all permitted modes")
        for k in commodity_subgraph_dict:
            commodity_subgraph_dict[k]['subgraph'] = G
    else:
        # generate subgraphs for each unique combination of allowed modes
        logger.debug("creating subgraphs for each unique set of allowed modes")
        # create a dictionary of subgraphs indexed by 'modes'
        subgraph_dict = {}
        for k in commodity_subgraph_dict:
            allowed_modes = str(commodity_subgraph_dict[k]['modes'])
            if allowed_modes not in subgraph_dict:
                subgraph_dict[allowed_modes] = nx.DiGraph(((source, target, attr) for source, target, attr in
                                                           G.edges(data=True) if attr['MODE_TYPE'] in
                                                           commodity_subgraph_dict[k]['modes']))

        # assign subgraphs to commodities
        for k in commodity_subgraph_dict:
            commodity_subgraph_dict[k]['subgraph'] = subgraph_dict[str(commodity_subgraph_dict[k]['modes'])]

    logger.debug("end: create mode subgraph dictionary")

    return commodity_subgraph_dict


# -----------------------------------------------------------------------------


# Returns a dictionary of edge_ids keyed off (to_node_id, from_node_id)
def find_edge_ids(the_scenario, logger):
    logger.debug("start: create edge_id dictionary")
    logger.debug("start: pull edge_ids from SQL")
    with sqlite3.connect(the_scenario.main_db) as db_cur:
        sql = """
            select ne.from_node_id, ne.to_node_id, ne.edge_id, ne.mode_source, nec.route_cost
            from networkx_edges ne
            left join networkx_edge_costs nec
            on ne.edge_id = nec.edge_id;
            """
        sql_list = db_cur.execute(sql).fetchall()
    logger.debug("end: pull edge_ids from SQL")

    edge_id_dict = {}

    for row in sql_list:
        from_node = row[0]
        to_node = row[1]
        edge_id = row[2]
        mode_source = row[3]
        route_cost = row[4]
        if to_node not in edge_id_dict:
            edge_id_dict[to_node] = {}
        if from_node not in edge_id_dict[to_node].keys():
            edge_id_dict[to_node][from_node] = []
        edge_id_dict[to_node][from_node].append([edge_id, mode_source, route_cost])

    logger.debug("end: create edge_id dictionary")

    return edge_id_dict


# -----------------------------------------------------------------------------


# Creates a dictionary of all feasible origin-destination pairs, including:
# RMP-DEST, RMP-PROC, PROC-DEST, etc., indexed by [commodity_id][target][source]
def make_od_pairs(the_scenario, logger):
    with sqlite3.connect(the_scenario.main_db) as db_cur:
        # Create a table for od_pairs in the database
        logger.info("start: create o-d pairs table")
        sql = "drop table if exists od_pairs;"
        db_cur.execute(sql)

        sql = '''
        create table od_pairs(scenario_rt_id INTEGER PRIMARY KEY, from_location_id integer, to_location_id integer,
        from_facility_id integer, to_facility_id integer, commodity_id integer, phase_of_matter text, route_status text,
        from_node_id INTEGER, to_node_id INTEGER, from_location_1 INTEGER, to_location_1 INTEGER);
        '''
        db_cur.execute(sql)

        # Populate the od_pairs table from a temporary table that collects both origins and destinations
        sql = "drop table if exists tmp_connected_facilities_with_commodities;"
        db_cur.execute(sql)

        sql = '''
        create table tmp_connected_facilities_with_commodities as
        select
        facilities.facility_id,
        facilities.location_id,
        facilities.facility_name,
        facility_type_id.facility_type,
        ignore_facility,
        facility_commodities.commodity_id,
        facility_commodities.io,
        commodities.phase_of_matter,
        networkx_nodes.node_id,
        networkx_nodes.location_1
        from facilities
        join facility_commodities on
        facility_commodities.facility_id = facilities.facility_ID
        join commodities on
        facility_commodities.commodity_id = commodities.commodity_id
        join facility_type_id on
        facility_type_id.facility_type_id = facilities.facility_type_id
        join networkx_nodes on
        networkx_nodes.location_id = facilities.location_id
        where ignore_facility = 'false';
        '''
        db_cur.execute(sql)

        sql = "drop table if exists tmp_od_pairs;"
        db_cur.execute(sql)

        sql = '''
        create table tmp_od_pairs as
        select distinct
        origin.location_id AS from_location_id,
        destination.location_id AS to_location_id,
        origin.facility_id AS from_facility_id,
        destination.facility_id AS to_facility_id,
        origin.commodity_id AS commodity_id,
        origin.phase_of_matter AS phase_of_matter,
        origin.node_id AS from_node_id,
        destination.node_id AS to_node_id,
        origin.location_1 AS from_location_1,
        destination.location_1 AS to_location_1
        from
        tmp_connected_facilities_with_commodities as origin
        inner join
        tmp_connected_facilities_with_commodities as destination ON
        CASE
        WHEN origin.facility_type <> 'processor' or destination.facility_type <> 'processor' -- THE NORMAL CASE, RMP->PROC, RMP->DEST, or PROC->DEST
        THEN
        origin.facility_type <> destination.facility_type                  -- not the same facility_type
        and
        origin.commodity_id = destination.commodity_id                     -- match on commodity
        and
        origin.facility_id <> destination.facility_id                      -- not the same facility
        and
        origin.facility_type <> "ultimate_destination"                     -- restrict origin types
        and
        destination.facility_type <> "raw_material_producer"               -- restrict destination types
        and
        destination.facility_type <> "raw_material_producer_as_processor"  -- restrict other destination types  TODO - MNP - 12/6/17 MAY NOT NEED THIS IF WE MAKE TEMP CANDIDATES AS THE RMP
        and
        origin.location_1 like '%_OUT' 									   -- restrict to the correct out/in node_id's
        AND destination.location_1 like '%_IN'
        ELSE -- THE CASE WHEN PROCESSORS ARE SENDING STUFF TO OTHER PROCESSORS
        origin.io = 'o' 													-- make sure processors origins send outputs
        and
        destination.io = 'i' 												-- make sure processors destinations receive inputs
        and
        origin.commodity_id = destination.commodity_id                     -- match on commodity
        and
        origin.facility_id <> destination.facility_id                      -- not the same facility
        and
        origin.facility_type <> "ultimate_destination"                     -- restrict origin types
        and
        destination.facility_type <> "raw_material_producer"               -- restrict destination types
        and
        destination.facility_type <> "raw_material_producer_as_processor"  -- restrict other destination types  TODO - MNP - 12/6/17 MAY NOT NEED THIS IF WE MAKE TEMP CANDIDATES AS THE RMP
        and
        origin.location_1 like '%_OUT' 									   -- restrict to the correct out/in node_id's
        AND destination.location_1 like '%_IN'
        END;
        '''
        db_cur.execute(sql)

        sql = '''
        insert into od_pairs (from_location_id, to_location_id, from_facility_id, to_facility_id, commodity_id, 
        phase_of_matter, from_node_id, to_node_id, from_location_1, to_location_1)
        select distinct
        odp.from_location_id,
        odp.to_location_id,
        odp.from_facility_id,
        odp.to_facility_id,
        odp.commodity_id,
        odp.phase_of_matter,
        odp.from_node_id,
        odp.to_node_id,
        odp.from_location_1,
        odp.to_location_1
        from tmp_od_pairs odp
        inner join networkx_edges ne1 on
        odp.from_node_id = ne1.from_node_id
        inner join networkx_edges ne2 on
        odp.to_node_id = ne2.to_node_id
        inner join (select commodity_id, mode
        from commodity_mode
        where allowed_yn = 'Y') cm1 on
        odp.commodity_id = cm1.commodity_id
        and ne1.mode_source = cm1.mode
        inner join (select commodity_id, mode
        from commodity_mode
        where allowed_yn = 'Y') cm2 on
        odp.commodity_id = cm2.commodity_id
        and ne2.mode_source = cm2.mode;
        '''
        db_cur.execute(sql)

        logger.debug("drop the tmp_connected_facilities_with_commodities table")
        db_cur.execute("drop table if exists tmp_connected_facilities_with_commodities;")

        logger.debug("drop the tmp_od_pairs table")
        db_cur.execute("drop table if exists tmp_od_pairs;")

        logger.info("end: create o-d pairs table")

        # Fetch all od-pairs, ordered by target
        sql = '''
        SELECT to_node_id, from_node_id, scenario_rt_id, commodity_id, phase_of_matter
        FROM od_pairs ORDER BY to_node_id DESC;
        '''
        sql_list = db_cur.execute(sql).fetchall()

        # Loop through the od_pairs
        od_pairs = {}
        for row in sql_list:
            target = row[0]
            source = row[1]
            scenario_rt_id = row[2]
            commodity_id = row[3]
            phase_of_matter = row[4]
            if commodity_id not in od_pairs:
                od_pairs[commodity_id] = {}
                od_pairs[commodity_id]['phase_of_matter'] = phase_of_matter
                od_pairs[commodity_id]['targets'] = {}
            if target not in od_pairs[commodity_id]['targets'].keys():
                od_pairs[commodity_id]['targets'][target] = {}
            if source not in od_pairs[commodity_id]['targets'][target].keys():
                od_pairs[commodity_id]['targets'][target][source] = []
            od_pairs[commodity_id]['targets'][target][source].append(scenario_rt_id)

    return od_pairs


# -----------------------------------------------------------------------------


def export_fcs_from_main_gdb(the_scenario, logger):
    # export fcs from the main.GDB to individual shapefiles
    logger.info("start: export_fcs_from_main_gdb")
    start_time = datetime.datetime.now()

    # export network and locations fc's to shapefiles
    main_gdb = the_scenario.main_gdb
    output_path = the_scenario.networkx_files_dir
    input_features = []

    logger.debug("start: create temp_networkx_shp_files dir")
    if os.path.exists(output_path):
        logger.debug("deleting pre-existing temp_networkx_shp_files dir")
        rmtree(output_path)

    if not os.path.exists(output_path):
        logger.debug("creating new temp_networkx_shp_files dir")
        os.makedirs(output_path)

    location_list = ['\\locations', '\\network\\intermodal', '\\network\\locks']

    # only add shape files associated with modes that are permitted in the scenario file
    for mode in the_scenario.permittedModes:
        location_list.append('\\network\\{}'.format(mode))

    # get the locations and network feature layers
    for fc in location_list:
        input_features.append(main_gdb + fc)
    arcpy.FeatureClassToShapefile_conversion(Input_Features=input_features, Output_Folder=output_path)

    logger.debug("finished: export_fcs_from_main_gdb: Runtime (HMS): \t{}".format(
        ftot_supporting.get_total_runtime_string(start_time)))


# ------------------------------------------------------------------------------


def clean_networkx_graph(the_scenario, G, logger):
    # -------------------------------------------------------------------------
    # renamed clean_networkx_graph ()
    # remove reversed links for pipeline
    # selectively remove links for location _IN and _OUT nodes
    # preserve the route_cost_scaling factor in an attribute by phase of matter

    logger.info("start: clean_networkx_graph")
    start_time = datetime.datetime.now()

    logger.debug("Processing the {} edges in the uncosted graph.".format(G.size()))

    # use the artificial and reversed attribute to determine if
    # the link is kept
    # -------------------------------------------------------------
    edge_attrs = {}  # for storing the edge attributes which are set all at once
    deleted_edge_count = 0

    for u, v, keys, artificial in list(G.edges(data='Artificial', keys=True)):

        # initialize the route_cost_scaling variable to something
        # absurd so we know if its getting set properly in the loop:
        route_cost_scaling = -999999999

        # check if the link is reversed
        if 'REVERSED' in G.edges[u, v, keys]:
            reversed_link = G.edges[u, v, keys]['REVERSED']
        else:
            reversed_link = 0

        # check if capacity is 0 - not currently considered here
        # Network Edges - artificial == 0
        # -----------------------------------
        if artificial == 0:

            # check the mode type
            # ----------------------
            mode_type = G.edges[u, v, keys]['MODE_TYPE']

            # set the mode specific weights
            # -----------------------------

            if mode_type == "rail":
                d_code = G.edges[u, v, keys]["DENSITY_CO"]
                if d_code in [7]:
                    route_cost_scaling = the_scenario.rail_dc_7
                elif d_code in [6]:
                    route_cost_scaling = the_scenario.rail_dc_6
                elif d_code in [5]:
                    route_cost_scaling = the_scenario.rail_dc_5
                elif d_code in [4]:
                    route_cost_scaling = the_scenario.rail_dc_4
                elif d_code in [3]:
                    route_cost_scaling = the_scenario.rail_dc_3
                elif d_code in [2]:
                    route_cost_scaling = the_scenario.rail_dc_2
                elif d_code in [1]:
                    route_cost_scaling = the_scenario.rail_dc_1
                elif d_code in [0]:
                    route_cost_scaling = the_scenario.rail_dc_0
                else:
                    logger.warning("The d_code {} is not supported".format(d_code))

            elif mode_type == "water":

                # get the total vol of water traffic
                tot_vol = G.edges[u, v, keys]['TOT_UP_DWN']
                if tot_vol >= 10000000:
                    route_cost_scaling = the_scenario.water_high_vol
                elif 1000000 <= tot_vol < 10000000:
                    route_cost_scaling = the_scenario.water_med_vol
                elif 1 <= tot_vol < 1000000:
                    route_cost_scaling = the_scenario.water_low_vol
                else:
                    route_cost_scaling = the_scenario.water_no_vol

            elif mode_type == "road":

                # get fclass
                fclass = G.edges[u, v, keys]['FCLASS']
                if fclass in [1]:
                    route_cost_scaling = the_scenario.truck_interstate
                elif fclass in [2, 3]:
                    route_cost_scaling = the_scenario.truck_pr_art
                elif fclass in [4]:
                    route_cost_scaling = the_scenario.truck_m_art
                else:
                    route_cost_scaling = the_scenario.truck_local

            elif 'pipeline' in mode_type:
                if reversed_link == 1:
                    G.remove_edge(u, v, keys)
                    deleted_edge_count += 1
                    continue  # move on to the next edge
                else:
                    route_cost_scaling = (((float(G.edges[u, v, keys]['base_rate']) / 100) / 42.0) * 1000.0)

        # Intermodal Edges - artificial == 2
        # ------------------------------------
        elif artificial == 2:
            # set it to 1 because we'll multiply by the appropriate
            # link_cost later for transloading
            route_cost_scaling = 1

        # Artificial Edge - artificial == 1
        # ----------------------------------
        # need to check if its an IN location or an OUT location and delete selectively.
        # assume always connecting from the node to the network.
        # so _OUT locations should delete the reversed link
        # _IN locations should delete the non-reversed link.
        elif artificial == 1:
            # delete edges we dont want

            try:
                if G.edges[u, v, keys]['LOCATION_1'].find("_OUT") > -1 and reversed_link == 1:
                    G.remove_edge(u, v, keys)
                    deleted_edge_count += 1
                    continue  # move on to the next edge
                elif G.edges[u, v, keys]['LOCATION_1'].find("_IN") > -1 and reversed_link == 0:
                    G.remove_edge(u, v, keys)
                    deleted_edge_count += 1
                    continue  # move on to the next edge

                # there is no scaling of artificial links.
                # the cost_penalty is calculated in get_network_link_cost()
                else:
                    route_cost_scaling = 1
            except:
                logger.warning("the following keys didn't work:u - {}, v- {}".format(u, v))
        else:
            logger.warning("found an edge without artificial attribute: {} ")
            continue

        edge_attrs[u, v, keys] = {
            'route_cost_scaling': route_cost_scaling
        }

    nx.set_edge_attributes(G, edge_attrs)

    # print out some stats on the Graph
    logger.info("Number of nodes in the clean graph: {}".format(G.order()))
    logger.info("Number of edges in the clean graph: {}".format(G.size()))

    logger.debug("finished: clean_networkx_graph: Runtime (HMS): \t{}".format(
        ftot_supporting.get_total_runtime_string(start_time)))

    return G


# ------------------------------------------------------------------------------


def get_network_link_cost(the_scenario, phase_of_matter, mode, artificial, logger):
    # three types of artificial links:
    # (0 = network edge, 2 = intermodal, 1 = artificial link btw facility location and network edge)
    # add the appropriate cost to the network edges based on phase of matter

    if phase_of_matter == "solid":
        # set the mode costs
        truck_base_cost = the_scenario.solid_truck_base_cost.magnitude
        railroad_class_1_cost = the_scenario.solid_railroad_class_1_cost.magnitude
        barge_cost = the_scenario.solid_barge_cost.magnitude
        transloading_cost = the_scenario.solid_transloading_cost.magnitude
        rail_short_haul_penalty = the_scenario.solid_rail_short_haul_penalty.magnitude
        water_short_haul_penalty = the_scenario.solid_water_short_haul_penalty.magnitude

    elif phase_of_matter == "liquid":
        # set the mode costs
        truck_base_cost = the_scenario.liquid_truck_base_cost.magnitude
        railroad_class_1_cost = the_scenario.liquid_railroad_class_1_cost.magnitude
        barge_cost = the_scenario.liquid_barge_cost.magnitude
        transloading_cost = the_scenario.liquid_transloading_cost.magnitude
        rail_short_haul_penalty = the_scenario.liquid_rail_short_haul_penalty.magnitude
        water_short_haul_penalty = the_scenario.liquid_water_short_haul_penalty.magnitude

    else:
        logger.error("the phase of matter: -- {} -- is not supported. returning")
        raise NotImplementedError

    if artificial == 1:
        # add a fixed cost penalty to the routing cost for artificial links
        if mode == "rail":
            link_cost = rail_short_haul_penalty / 2.0
            # Divide by 2 is to ensure the penalty is not doubled-- it is applied on artificial links on both ends
        elif mode == "water":
            link_cost = water_short_haul_penalty / 2.0
            # Divide by 2 is to ensure the penalty is not doubled-- it is applied on artificial links on both ends
        elif 'pipeline' in mode:
            # this cost penalty was calculated by looking at the average per mile base rate.
            link_cost = 0.19
            # no mileage multiplier here for pipeline as unlike rail/water, we do not want to disproportionally
            # discourage short movements
            # Multiplier will be applied based on actual link mileage when scenario costs are actually set
        else:
            link_cost = the_scenario.truck_local * truck_base_cost # for road

    elif artificial == 2:
        # phase of mater is determined above
        link_cost = transloading_cost

    elif artificial == 0:
        if mode == "road":
            link_cost = truck_base_cost
        elif mode == "rail":
            link_cost = railroad_class_1_cost
        elif mode == "water":
            link_cost = barge_cost
        elif mode == "pipeline_crude_trf_rts":
            link_cost = 1  # so we multiply by the base_rate
        elif mode == "pipeline_prod_trf_rts":
            link_cost = 1  # so we multiply by base_rate

    return link_cost


# ----------------------------------------------------------------------------


def get_phases_of_matter_in_scenario(the_scenario, logger):
    logger.debug("start: get_phases_of_matter_in_scenario()")

    phases_of_matter_in_scenario = []

    with sqlite3.connect(the_scenario.main_db) as main_db_con:

        sql = "select count(distinct phase_of_matter) from commodities where phase_of_matter is not null;"
        db_cur = main_db_con.execute(sql)

        count = db_cur.fetchone()[0]
        logger.debug("phases_of_matter in the scenario: {}".format(count))

        if not count:
            error = "No phases of matter in the scenario: {}...returning".format(count)
            logger.error(error)
            raise Exception(error)

        elif count:
            sql = "select phase_of_matter from commodities where phase_of_matter is not null group by phase_of_matter"
            db_cur = main_db_con.execute(sql)
            for row in db_cur:
                phases_of_matter_in_scenario.append(row[0])
        else:
            logger.warning("Something went wrong in get_phases_of_matter_in_scenario()")
            error = "Count phases of matter to route: {}".format(str(count))
            logger.error(error)
            raise Exception(error)

    logger.debug("end: get_phases_of_matter_in_scenario()")
    return phases_of_matter_in_scenario


# -----------------------------------------------------------------------------


# set the network costs in the db by phase_of_matter
def set_network_costs_in_db(the_scenario, logger):

    logger.info("start: set_network_costs_in_db")
    with sqlite3.connect(the_scenario.main_db) as db_con:
        # clean up the db
        sql = "drop table if exists networkx_edge_costs"
        db_con.execute(sql)

        sql = "create table if not exists networkx_edge_costs " \
              "(edge_id INTEGER, phase_of_matter_id INT, route_cost REAL, dollar_cost REAL)"
        db_con.execute(sql)

        # build up the network edges cost by phase of matter
        edge_cost_list = []

        # get phases_of_matter in the scenario
        phases_of_matter_in_scenario = get_phases_of_matter_in_scenario(the_scenario, logger)

        # loop through each edge in the networkx_edges table
        sql = "select edge_id, mode_source, artificial, miles, route_cost_scaling from networkx_edges"
        db_cur = db_con.execute(sql)
        for row in db_cur:

            edge_id = row[0]
            mode_source = row[1]
            artificial = row[2]
            miles = row[3]
            route_cost_scaling = row[4]

            for phase_of_matter in phases_of_matter_in_scenario:

                # skip pipeline and solid phase of matter
                if phase_of_matter == 'solid' and 'pipeline' in mode_source:
                    continue

                # otherwise, go ahead and get the link cost
                link_cost = get_network_link_cost(the_scenario, phase_of_matter, mode_source, artificial, logger)

                if artificial == 0:
                    # road, rail, and water
                    if 'pipeline' not in mode_source:
                        dollar_cost = miles * link_cost  # link_cost is taken from the scenario file
                        route_cost = dollar_cost * route_cost_scaling  # this includes impedance

                    else:
                        # if artificial = 0, route_cost_scaling = base rate
                        # we use the route_cost_scaling for this since its set in the GIS
                        # not in the scenario xml file.

                        dollar_cost = route_cost_scaling  # this is the base rate
                        route_cost = route_cost_scaling  # this is the base rate

                elif artificial == 1:
                    # we don't want to add the cost penalty to the dollar cost for artificial links
                    dollar_cost = 0

                    if 'pipeline' not in mode_source:
                        # the routing_cost should have the artificial link penalty
                        # and artificial link penalties shouldn't be scaled by mileage.
                        route_cost = link_cost

                    else:
                        # For pipeline we don't want to penalize short movements disproportionately,
                        # so scale penalty by miles. This gives art links for pipeline a modest routing cost
                        route_cost = link_cost * miles

                elif artificial == 2:
                    dollar_cost = link_cost / 2.00  # this is the transloading fee
                    # For now dividing by 2 to ensure that the transloading fee is not applied twice
                    # (e.g. on way in and on way out)
                    route_cost = link_cost / 2.00  # same as above.

                else:
                    logger.warning("artificial code of {} is not supported!".format(artificial))

                edge_cost_list.append([edge_id, phase_of_matter, route_cost, dollar_cost])

        if edge_cost_list:
            update_sql = """
                INSERT into networkx_edge_costs
                values (?,?,?,?)
                ;"""

            db_con.executemany(update_sql, edge_cost_list)
            logger.debug("start: networkx_edge_costs commit")
            db_con.commit()
            logger.debug("finish: networkx_edge_costs commit")

    logger.debug("finished: set_network_costs_in_db")


# -----------------------------------------------------------------------------


def digraph_to_db(the_scenario, G, logger):
    # moves the networkX digraph into the database for the pulp handshake

    logger.info("start: digraph_to_db")
    with sqlite3.connect(the_scenario.main_db) as db_con:

        # clean up the db
        sql = "drop table if exists networkx_nodes"
        db_con.execute(sql)

        sql = "create table if not exists networkx_nodes (node_id INT, source TEXT, source_OID integer, location_1 " \
              "TEXT, location_id TEXT, shape_x REAL, shape_y REAL)"
        db_con.execute(sql)

        # loop through the nodes in the digraph and set them in the db
        # nodes will be either locations (with a location_id), or nodes connecting
        # network edges (with no location info).
        node_list = []

        for node in G.nodes():
            source = None
            source_oid = None
            location_1 = None
            location_id = None
            shape_x = None
            shape_y = None

            if 'source' in G.nodes[node]:
                source = G.nodes[node]['source']

            if 'source_OID' in G.nodes[node]:
                source_oid = G.nodes[node]['source_OID']

            if 'location_1' in G.nodes[node]:  # for locations
                location_1 = G.nodes[node]['location_1']
                location_id = G.nodes[node]['location_i']

            if 'x_y_location' in G.nodes[node]:
                shape_x = G.nodes[node]['x_y_location'][0]
                shape_y = G.nodes[node]['x_y_location'][1]

            node_list.append([node, source, source_oid, location_1, location_id, shape_x, shape_y])

        if node_list:
            update_sql = """
                INSERT into networkx_nodes
                values (?,?,?,?,?,?,?)
                ;"""

            db_con.executemany(update_sql, node_list)
            db_con.commit()
            logger.debug("finished network_x nodes commit")

    # loop through the edges in the digraph and insert them into the db.
    # -------------------------------------------------------------------
    edge_list = []
    with sqlite3.connect(the_scenario.main_db) as db_con:

        # clean up the db
        sql = "drop table if exists networkx_edges"
        db_con.execute(sql)

        sql = "create table if not exists networkx_edges (edge_id INTEGER PRIMARY KEY, from_node_id INT, to_node_id " \
              "INT, artificial INT, mode_source TEXT, mode_source_oid INT, miles REAL, route_cost_scaling REAL, " \
              "capacity INT, volume REAL, VCR REAL)"
        db_con.execute(sql)

        for (u, v, c, d) in G.edges(keys=True, data='route_cost_scaling', default=False):
            from_node_id = u
            to_node_id = v
            miles = G.edges[(u, v, c)]['MILES']
            artificial = G.edges[(u, v, c)]['Artificial']
            mode_source = G.edges[(u, v, c)]['MODE_TYPE']
            mode_source_oid = G.edges[(u, v, c)]['source_OID']

            if mode_source in ['rail', 'road']:
                volume = G.edges[(u, v, c)]['Volume']
                vcr = G.edges[(u, v, c)]['VCR']
                capacity = G.edges[(u, v, c)]['Capacity']
            else:
                volume = None
                vcr = None
                capacity = None

            if capacity == 0:
                capacity = None
                logger.detailed_debug("link capacity == 0, setting to None".format(G.edges[(u, v, c)]))

            if 'route_cost_scaling' in G.edges[(u, v, c)]:
                route_cost_scaling = G.edges[(u, v, c)]['route_cost_scaling']
            else:
                logger.warning(
                    "EDGE: {}, {}, {} - mode: {} - artificial {} -- "
                    "does not have key route_cost_scaling".format(u, v, c, mode_source, artificial))

            edge_list.append(
                [from_node_id, to_node_id, artificial, mode_source, mode_source_oid, miles, route_cost_scaling,
                 capacity, volume, vcr])

        # the node_id will be used to explode the edges by commodity and time period
        if edge_list:
            update_sql = """
                INSERT into networkx_edges
                values (null,?,?,?,?,?,?,?,?,?,?)
                ;"""
            # Add one more question mark here
            db_con.executemany(update_sql, edge_list)
            db_con.commit()
            logger.debug("finished network_x edges commit")


# ----------------------------------------------------------------------------


def read_shp(path, logger, simplify=True, geom_attrs=True, strict=True):
    # the modified read_shp() multidigraph code
    logger.debug("start: read_shp -- simplify: {}, geom_attrs: {}, strict: {}".format(simplify, geom_attrs, strict))

    try:
        from osgeo import ogr
    except ImportError:
        logger.error("read_shp requires OGR: http://www.gdal.org/")
        raise ImportError("read_shp requires OGR: http://www.gdal.org/")

    if not isinstance(path, str):
        return
    net = nx.MultiDiGraph()
    shp = ogr.Open(path)
    if shp is None:
        logger.error("Unable to open {}".format(path))
        raise RuntimeError("Unable to open {}".format(path))
    for lyr in shp:
        count = lyr.GetFeatureCount()
        logger.debug("processing layer: {} - feature_count: {} ".format(lyr.GetName(), count))

        fields = [x.GetName() for x in lyr.schema]
        logger.debug("f's in layer: {}".format(len(lyr)))
        f_counter = 0
        time_counter_string = ""
        for f in lyr:

            f_counter += 1
            if f_counter % 2000 == 0:
                time_counter_string += ' .'

            if f_counter % 20000 == 0:
                logger.debug("lyr: {} - feature counter: {} / {}".format(lyr.GetName(), f_counter, count))
            if f_counter == count:
                logger.debug("lyr: {} - feature counter: {} / {}".format(lyr.GetName(), f_counter, count))
                logger.debug(time_counter_string + 'done.')

            g = f.geometry()
            if g is None:
                if strict:
                    logger.error("Bad data: feature missing geometry")
                    raise nx.NetworkXError("Bad data: feature missing geometry")
                else:
                    continue
            fld_data = [f.GetField(f.GetFieldIndex(x)) for x in fields]
            attributes = dict(list(zip(fields, fld_data)))
            attributes["ShpName"] = lyr.GetName()
            # Note: Using layer level geometry type
            if g.GetGeometryType() == ogr.wkbPoint:
                net.add_node(g.GetPoint_2D(0), **attributes)
            elif g.GetGeometryType() in (ogr.wkbLineString,
                                         ogr.wkbMultiLineString):
                for edge in edges_from_line(g, attributes, simplify,
                                            geom_attrs):
                    e1, e2, attr = edge
                    net.add_edge(e1, e2)
                    key = len(list(net[e1][e2].keys())) - 1
                    net[e1][e2][key].update(attr)
            else:
                if strict:
                    logger.error("GeometryType {} not supported".
                                 format(g.GetGeometryType()))
                    raise nx.NetworkXError("GeometryType {} not supported".
                                           format(g.GetGeometryType()))

    return net


# ----------------------------------------------------------------------------


def edges_from_line(geom, attrs, simplify=True, geom_attrs=True):
    """
    Generate edges for each line in geom
    Written as a helper for read_shp

    Parameters
    ----------

    geom:  ogr line geometry
        To be converted into an edge or edges

    attrs:  dict
        Attributes to be associated with all geoms

    simplify:  bool
        If True, simplify the line as in read_shp

    geom_attrs:  bool
        If True, add geom attributes to edge as in read_shp


    Returns
    -------
     edges:  generator of edges
        each edge is a tuple of form
        (node1_coord, node2_coord, attribute_dict)
        suitable for expanding into a networkx Graph add_edge call
    """
    try:
        from osgeo import ogr
    except ImportError:
        raise ImportError("edges_from_line requires OGR: http://www.gdal.org/")

    if geom.GetGeometryType() == ogr.wkbLineString:
        if simplify:
            edge_attrs = attrs.copy()
            last = geom.GetPointCount() - 1
            if geom_attrs:
                edge_attrs["Wkb"] = geom.ExportToWkb()
                edge_attrs["Wkt"] = geom.ExportToWkt()
                edge_attrs["Json"] = geom.ExportToJson()
            yield (geom.GetPoint_2D(0), geom.GetPoint_2D(last), edge_attrs)
        else:
            for i in range(0, geom.GetPointCount() - 1):
                pt1 = geom.GetPoint_2D(i)
                pt2 = geom.GetPoint_2D(i + 1)
                edge_attrs = attrs.copy()
                if geom_attrs:
                    segment = ogr.Geometry(ogr.wkbLineString)
                    segment.AddPoint_2D(pt1[0], pt1[1])
                    segment.AddPoint_2D(pt2[0], pt2[1])
                    edge_attrs["Wkb"] = segment.ExportToWkb()
                    edge_attrs["Wkt"] = segment.ExportToWkt()
                    edge_attrs["Json"] = segment.ExportToJson()
                    del segment
                yield (pt1, pt2, edge_attrs)

    elif geom.GetGeometryType() == ogr.wkbMultiLineString:
        for i in range(geom.GetGeometryCount()):
            geom_i = geom.GetGeometryRef(i)
            for edge in edges_from_line(geom_i, attrs, simplify, geom_attrs):
                yield edge

