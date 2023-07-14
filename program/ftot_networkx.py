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
import csv
from heapq import heappush, heappop
from itertools import count
from ftot_pulp import commodity_mode_setup
from ftot import Q_

# -----------------------------------------------------------------------------


def graph(the_scenario, logger):

    # check for permitted modes before creating nX graph
    check_permitted_modes(the_scenario, logger)

    # export the assets from GIS export_fcs_from_main_gdb
    export_fcs_from_main_gdb(the_scenario, logger)

    # create the networkx multidigraph
    G = make_networkx_graph(the_scenario, logger)

    # clean up the networkx graph to preserve connectivity
    G = clean_networkx_graph(the_scenario, G, logger)

    # cache the digraph to the db and store the route_cost_scaling factor
    G = digraph_to_db(the_scenario, G, logger)

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

    # clean up the node labels
    logger.debug("start: convert node labels")
    G = nx.convert_node_labels_to_integers(G, first_label=0, ordering='default', label_attribute="x_y_location")

    # create a reversed graph
    logger.debug("start: reverse G graph to H")
    H = G.reverse()  # this is a reversed version of the graph

    # set a new attribute for every edge that says it is a "reversed" link
    # we will use this to delete edges that shouldn't be reversed later
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

    # if MTD, then make rmp-specific subgraphs with only nodes reachable in MTD
    commodity_subgraph_dict = make_max_transport_distance_subgraphs(the_scenario, logger, commodity_subgraph_dict)

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

        if 'targets' in od_pairs[commodity_id].keys():
            for a_target in od_pairs[commodity_id]['targets'].keys():
                stuff_to_pass.append([commodity_subgraph_dict[commodity_id]['subgraph'],
                                    od_pairs[commodity_id]['targets'][a_target],
                                    a_target, all_route_edges, no_path_pairs,
                                    edge_id_dict, phase_of_matter, allowed_modes, 'target'])
        else:    
            for a_source in od_pairs[commodity_id]['sources'].keys():
                if 'facility_subgraphs' in commodity_subgraph_dict[commodity_id].keys():
                    subgraph = commodity_subgraph_dict[commodity_id]['facility_subgraphs'][a_source]
                else:
                    subgraph = commodity_subgraph_dict[commodity_id]['subgraph']
                stuff_to_pass.append([subgraph,
                                    od_pairs[commodity_id]['sources'][a_source],
                                    a_source, all_route_edges, no_path_pairs,
                                    edge_id_dict, phase_of_matter, allowed_modes, 'source'])

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


# Ensure shortest paths not calculated in presence of capacity
def update_ndr_parameter(the_scenario, logger):
    logger.debug("start: check NDR conditions")
    new_ndr_parameter = the_scenario.ndrOn

    # if capacity is enabled, then skip NDR
    if the_scenario.capacityOn:
        new_ndr_parameter = False
        logger.debug("NDR de-activated due to capacity enforcement")

    the_scenario.ndrOn = new_ndr_parameter
    logger.debug("finish: check NDR conditions")


# -----------------------------------------------------------------------------


# This method uses a shortest_path algorithm from the nx library to flag edges in the
# network that are a part of the shortest path connecting an origin to a destination
# for each commodity
def multi_shortest_paths(stuff_to_pass):

    global all_route_edges, no_path_pairs
    if stuff_to_pass[8] == 'target':
        G, sources, target, all_route_edges, no_path_pairs, edge_id_dict, phase_of_matter, allowed_modes, st_dummy = stuff_to_pass
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
    else:
        G, targets, source, all_route_edges, no_path_pairs, edge_id_dict, phase_of_matter, allowed_modes, st_dummy = stuff_to_pass
        s = source
        shortest_paths_from_s = nx.shortest_path(G, source=s, weight='{}_weight'.format(phase_of_matter))
        for a_target in targets:
            t = a_target
            # This accounts for when a_target may not be connected to s,
            # as is the case when certain modes may not be permitted
            if a_target not in shortest_paths_from_s:
                for i in targets[a_target]:
                    rt_id = i
                    no_path_pairs.append((s, t, rt_id))
                continue
            for i in targets[a_target]:
                rt_id = i
                for index, from_node in enumerate(shortest_paths_from_s[t]):
                    if index < (len(shortest_paths_from_s[t]) - 1):
                        to_node = shortest_paths_from_s[t][index + 1]
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
        route_cost_scaling = float(G.edges[(u, v, c)]['route_cost_scaling'])
        length = G.edges[(u, v, c)]['Length']
        source = G.edges[(u, v, c)]['source']
        artificial = G.edges[(u, v, c)]['Artificial']
         
        # calculate route_cost for all edges and phases of matter to mirror network costs in db
        for phase_of_matter in phases_of_matter_in_scenario:
            weight = get_network_link_cost(the_scenario, phase_of_matter, source, artificial, logger)
            if 'pipeline' not in source:
                if artificial == 0:
                    G.edges[(u, v, c)]['{}_weight'.format(phase_of_matter)] = length * route_cost_scaling * weight

                elif artificial == 1:
                    #  note: here 'weight' is the truck base cost and 'route_cost_scaling' is the local road impedance value

                    if source == "rail" and phase_of_matter == "solid":
                        penalty = (the_scenario.solid_truck_base_cost * route_cost_scaling - the_scenario.solid_railroad_class_1_cost) * the_scenario.rail_short_haul_penalty
                        G.edges[(u, v, c)]['{}_weight'.format(phase_of_matter)] = length * route_cost_scaling * weight + penalty.magnitude/2

                    elif source == "rail" and phase_of_matter == "liquid":
                        penalty = (the_scenario.liquid_truck_base_cost * route_cost_scaling - the_scenario.liquid_railroad_class_1_cost) * the_scenario.rail_short_haul_penalty
                        G.edges[(u, v, c)]['{}_weight'.format(phase_of_matter)] = length * route_cost_scaling * weight + penalty.magnitude/2

                    elif source == "water" and phase_of_matter == "solid":
                        penalty = (the_scenario.solid_truck_base_cost * route_cost_scaling - the_scenario.solid_barge_cost) * the_scenario.water_short_haul_penalty
                        G.edges[(u, v, c)]['{}_weight'.format(phase_of_matter)] = length * route_cost_scaling * weight + penalty.magnitude/2

                    elif source == "water" and phase_of_matter == "liquid":
                        penalty = (the_scenario.liquid_truck_base_cost * route_cost_scaling - the_scenario.liquid_barge_cost) * the_scenario.water_short_haul_penalty
                        G.edges[(u, v, c)]['{}_weight'.format(phase_of_matter)] = length * route_cost_scaling * weight + penalty.magnitude/2


                    else:
                        # road, no penalty
                        G.edges[(u, v, c)]['{}_weight'.format(phase_of_matter)] = length * route_cost_scaling * weight


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
                        G.edges[(u, v, c)]['{}_weight'.format(phase_of_matter)] = length * weight * route_cost_scaling # matches road
                    elif artificial == 2:
                        G.edges[(u, v, c)]['{}_weight'.format(phase_of_matter)] = weight / 2.00
                    else:
                        logger.warning("artificial code of {} is not supported!".format(artificial))

    logger.debug("end: assign edge weights to networkX graph")


# -----------------------------------------------------------------------------


def check_modes_candidate_generation(the_scenario, logger): 
    # this method checks whether there are different modes between the input commodity/ies
    # and output commodity/ies of a candidate process and returns a dictionary keyed
    # by process id of which modes' intermodal facilities should be selected as endcaps
    # We only pick modes that aren't available to all commodities, and will only select
    # intermodal facilities that move between two of such modes, since other mode switches
    # can happen at other places in the network.

    # if commodity mode is not supplied, then all processes have symmetrical mode availability
    if not os.path.exists(the_scenario.commodity_mode_data):
        return {}
     
    # pull commodity mode and process data from DB
    logger.debug("start: pull commodity mode & candidate process from SQL")
    with sqlite3.connect(the_scenario.main_db) as db_cur:
        sql = """select 
            cpc.process_id,
            cpc.commodity_id,
            cm.mode
            from commodity_mode cm
            join candidate_process_commodities cpc
            on cm.commodity_id = cpc.commodity_id
            where cm.allowed_yn like 'y';"""
        commodity_mode_data = db_cur.execute(sql).fetchall()
    
        logger.debug("end: pull commodity mode from SQL")

    # add commodities to two dictionaries:
    # diff modes[process_id][mode] - lists of modes available to some but not all commodities for a process
    # candidate_processes[process_id] - lists of commodity ids involved in each process
    diff_modes = {}
    candidate_processes = {}
    for row in commodity_mode_data:
        process_id = int(row[0])
        commodity_id = int(row[1])
        mode = row[2]
        
        if process_id not in diff_modes :
            diff_modes[process_id] = {}
        if mode not in diff_modes[process_id] :
            diff_modes[process_id][mode] = []
        diff_modes[process_id][mode].append(commodity_id)

        if process_id not in candidate_processes :
            candidate_processes[process_id] = []
        if commodity_id not in candidate_processes[process_id]:
            candidate_processes[process_id].append(commodity_id)

    # remove modes that are available to all commodities in a process
    to_remove = []
    for process, modes in diff_modes.items() :
        for mode, commodity_ids in modes.items() :
            commodity_ids.sort()
            candidate_processes[process].sort()
            if commodity_ids == candidate_processes[process] :
                # del diff_modes[process][mode]
                to_remove.append((process,mode))

    for (process, mode) in to_remove :
        del diff_modes[process][mode]

    return diff_modes


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
                subgraph_dict[allowed_modes] = nx.MultiDiGraph(((source, target, attr) for source, target, attr in
                                                           G.edges(data=True) if attr['Mode_Type'] in
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

        if not os.path.exists(the_scenario.processor_candidates_commodity_data) and the_scenario.processors_candidate_slate_data != 'None':
            # Create temp table for RMP-endcap

            sql = "drop table if exists tmp_rmp_to_endcap;"
            db_cur.execute(sql)

            sql = "drop table if exists tmp_endcap_to_other;"
            db_cur.execute(sql)
            
            sql = '''
            create table tmp_rmp_to_endcap as
            select distinct en.node_id as endcap_node,
            en.commodity_id,
            origin.location_id as rmp_location,
            origin.facility_id as rmp_facility,
            origin.node_id as rmp_node,
            origin.phase_of_matter
            from
            endcap_nodes en
            join tmp_connected_facilities_with_commodities as origin on
            origin.node_id = en.source_node_id
            '''
            db_cur.execute(sql)

            # Create temp table for endcap-proc or endcap-dest
            # Join endcap_nodes with candidate_process_commodities for process_id
            # then endcap_nodes with facility_commodities for facilities that take commodity as input

            sql = '''
            create table tmp_endcap_to_other as
            select distinct
            en.node_id as endcap_node,
            fc.facility_id as facility_id,
            fc.location_id as location_id,
            cpc.io,
            cpc.commodity_id,
            tmp.node_id as dest_node,
            tmp.phase_of_matter
            from
            endcap_nodes en
            join candidate_process_commodities cpc on en.process_id = cpc.process_id
            join facility_commodities fc on
            case
            when cpc.io = 'o'
            then
            cpc.commodity_id = fc.commodity_id
            and fc.io= 'i'
            end
            join tmp_connected_facilities_with_commodities tmp on
            fc.facility_id = tmp.facility_id
            and tmp.io = 'i'
            and tmp.location_1 like '%_in'
            ;
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

        if not os.path.exists(the_scenario.processor_candidates_commodity_data) and the_scenario.processors_candidate_slate_data != 'None':
            sql = '''
            insert into od_pairs (from_location_id, from_facility_id, commodity_id, phase_of_matter, from_node_id, to_node_id)
            select
            tmp.rmp_location,
            tmp.rmp_facility,
            tmp.commodity_id,
            tmp.phase_of_matter,
            tmp.rmp_node,
            tmp.endcap_node
            from tmp_rmp_to_endcap as tmp
            '''
            db_cur.execute(sql)

            sql = '''
            insert into od_pairs (to_location_id, to_facility_id, commodity_id, phase_of_matter, from_node_id, to_node_id)
            select
            tmp.location_id,
            tmp.facility_id,
            tmp.commodity_id,
            tmp.phase_of_matter,
            tmp.endcap_node,
            tmp.dest_node
            from tmp_endcap_to_other as tmp
            '''
            db_cur.execute(sql)

        logger.debug("drop the tmp_connected_facilities_with_commodities table")
        db_cur.execute("drop table if exists tmp_connected_facilities_with_commodities;")

        logger.debug("drop the tmp_od_pairs table")
        db_cur.execute("drop table if exists tmp_od_pairs;")

        logger.debug("drop the tmp_rmp_to_endcap table")
        db_cur.execute("drop table if exists tmp_rmp_to_endcap")

        logger.debug("drop the tmp_endcap_to_other table")
        db_cur.execute("drop table if exists tmp_endcap_to_other")

        logger.info("end: create o-d pairs table")

        # Fetch all od-pairs, ordered by target
        sql = '''
        SELECT to_node_id, from_node_id, scenario_rt_id, commodity_id, phase_of_matter
        FROM od_pairs ORDER BY to_node_id DESC;
        '''
        sql_list = db_cur.execute(sql).fetchall()

        # If commodity associated with a max transport distance, always make shortest paths from source
        sql = '''
        select odp.commodity_id, count(distinct odp.from_node_id), count(distinct odp.to_node_id), cm.max_transport_distance
        from od_pairs odp
        join commodities cm on odp.commodity_id = cm.commodity_id
        group by odp.commodity_id
        '''
        od_count = db_cur.execute(sql)

        commodity_st = {}
        for row in od_count:
            commodity_id = row[0]
            count_source = row[1]
            count_target = row[2]
            MTD = row[3]
            if MTD is not None:
                commodity_st[commodity_id] = 'source'
            else:
                if count_source < count_target:
                    commodity_st[commodity_id] = 'source'
                else:
                    commodity_st[commodity_id] = 'target'

    # Loop through the od_pairs
    od_pairs = {}
    for row in sql_list:
        target = row[0]
        source = row[1]
        scenario_rt_id = row[2]
        commodity_id = row[3]
        phase_of_matter = row[4]
        if commodity_st[commodity_id] == 'target':
            if commodity_id not in od_pairs:
                od_pairs[commodity_id] = {}
                od_pairs[commodity_id]['phase_of_matter'] = phase_of_matter
                od_pairs[commodity_id]['targets'] = {}
            if target not in od_pairs[commodity_id]['targets'].keys():
                od_pairs[commodity_id]['targets'][target] = {}
            if source not in od_pairs[commodity_id]['targets'][target].keys():
                od_pairs[commodity_id]['targets'][target][source] = []
            od_pairs[commodity_id]['targets'][target][source].append(scenario_rt_id)
        else:
            if commodity_id not in od_pairs:
                od_pairs[commodity_id] = {}
                od_pairs[commodity_id]['phase_of_matter'] = phase_of_matter
                od_pairs[commodity_id]['sources'] = {}
            if source not in od_pairs[commodity_id]['sources'].keys():
                od_pairs[commodity_id]['sources'][source] = {}
            if target not in od_pairs[commodity_id]['sources'][source].keys():
                od_pairs[commodity_id]['sources'][source][target] = []
            od_pairs[commodity_id]['sources'][source][target].append(scenario_rt_id)
    
    return od_pairs


# -----------------------------------------------------------------------------


def make_max_transport_distance_subgraphs(the_scenario, logger, commodity_subgraph_dict):
    
    # Get facility, commodity, and MTD info from the db
    logger.info("start: pull facility/commodity MTD from SQL")
    with sqlite3.connect(the_scenario.main_db) as db_cur:
        sql = """select cm.commodity_id, nn.node_id, c.max_transport_distance, f.facility_type_id, fc.io
               from commodity_mode cm
               join facility_commodities fc on cm.commodity_id = fc.commodity_id 
               join facilities f on fc.facility_id = f.facility_id  --likely need facility's node id from od_pairs instead
               join networkx_nodes nn on nn.location_id = f.location_id
               join commodities c on cm.commodity_id = c.commodity_id
               where cm.allowed_yn like 'Y' 
               and fc.io = 'o' 
               and nn.location_1 like '%_out'
               and f.ignore_facility = 'false'
			   group by nn.node_id;"""
        commodity_mtd_data = db_cur.execute(sql).fetchall()

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
    logger.info("end: pull commodity mode from SQL")

    # Find which facilities have max transport distance defined
    mtd_count = 0
    for row in commodity_mtd_data:
        commodity_id = int(row[0])
        facility_node_id = int(row[1])
        commodity_mtd = row[2]
        if commodity_mtd is not None:
            mtd_count = mtd_count + 1
            commodity_subgraph_dict[commodity_id]['MTD'] = commodity_mtd
            if 'facilities' not in commodity_subgraph_dict[commodity_id]:
                commodity_subgraph_dict[commodity_id]['facilities'] = []
            commodity_subgraph_dict[commodity_id]['facilities'].append(facility_node_id)

    if mtd_count == 0:
        return commodity_subgraph_dict
    
    if not os.path.exists(the_scenario.processor_candidates_commodity_data) and the_scenario.processors_candidate_slate_data != 'None':
        # get destination facility information
        with sqlite3.connect(the_scenario.main_db) as db_cur:
            sql = '''
            select
            cp_i.process_id,  
            cp_i.commodity_id,
            cp_o.commodity_id,
            dest.node_id
            from 
            candidate_process_commodities as cp_i 
            join 
            candidate_process_commodities as cp_o
            on 
            cp_i.io = 'i'
            and 
            cp_o.io = 'o'
            and
            cp_i.process_id = cp_o.process_id
            inner JOIN
            tmp_connected_facilities_with_commodities as dest
            on
            dest.commodity_id = cp_o.commodity_id
            and 
            dest.io = 'i'
            AND
            dest.location_1 like '%_IN'
            '''
            dest_facility_data = db_cur.execute(sql).fetchall()
        
        candidate_processes = {}
        for row in dest_facility_data:
            process_id = int(row[0])
            i_commodity_id = int(row[1])
            o_commodity_id = int(row[2])
            dest_facility_node_id = int(row[3])
            if process_id not in candidate_processes:
                candidate_processes[process_id] = []
            if i_commodity_id not in candidate_processes[process_id]:
                candidate_processes[process_id].append(i_commodity_id) 
            if 'dest_facilities' not in commodity_subgraph_dict[i_commodity_id]:
                commodity_subgraph_dict[i_commodity_id]['dest_facilities'] = []
            # get dest adjacent nodes from the output commodity's subgraph - 
            dest_adjacent_nodes = commodity_subgraph_dict[o_commodity_id]['subgraph'].predecessors(dest_facility_node_id)
            commodity_subgraph_dict[i_commodity_id]['dest_facilities'].extend(list(dest_adjacent_nodes))

        # get other mode information
        diff_modes = check_modes_candidate_generation(the_scenario, logger)

        with sqlite3.connect(the_scenario.main_db) as db_cur:
            if len(diff_modes.keys()) > 0 :
                for process_id, modes in diff_modes.items():
                    for mode, commodity_ids in diff_modes.items() :    
                        # pull relevant intermodal nodes from the DB
                        sql = """select 
                        nn.node_id,
                        ne1.mode_source,
                        ne2.mode_source
                        from networkx_edges ne1 
                        join networkx_edges ne2
                        on ne1.to_node_id = ne2.from_node_id
                        join networkx_nodes nn
                        on ne1.to_node_id = nn.node_id
                        where nn.source = "intermodal" 
                        and ne1.mode_source in ({}) 
                        and ne2.mode_source in ({}) 
                        and ne1.mode_source != ne2.mode_source;""".format(str(list(modes.keys()))[1:-1],str(list(modes.keys()))[1:-1])

                        node_mode_data = db_cur.execute(sql).fetchall()

                        for row in node_mode_data:
                            node_id = row[0]
                            in_edge_mode = row[1]
                            out_edge_mode = row[2]
                            for commodity_id in candidate_processes[process_id] :
                                if 'intermodal_facilities' not in commodity_subgraph_dict[commodity_id]:
                                    commodity_subgraph_dict[commodity_id]['intermodal_facilities'] = []
                                if node_id not in commodity_subgraph_dict[commodity_id]['intermodal_facilities'] :
                                    commodity_subgraph_dict[commodity_id]['intermodal_facilities'].append(node_id)

    # Store nodes that can be reached from an RMP with MTD
    ends = {}
    for commodity_id, commodity_dict in commodity_subgraph_dict.items():
        # For facility, MTD in commodities_with_mtd[commodity_id]
        if 'MTD' in commodity_dict:
            MTD = commodity_dict['MTD']
            for facility_node_id in commodity_dict['facilities']:
                # If 'facility_subgraphs' dictionary for commodity_subgraph_dict[commodity_id] doesn't exist, add it
                if 'facility_subgraphs' not in commodity_subgraph_dict[commodity_id]:
                    commodity_subgraph_dict[commodity_id]['facility_subgraphs'] = {}
                
                # Use shortest path to find the nodes that are within MTD
                logger.info("start: dijkstra for facility node ID " + str(facility_node_id))
                G = commodity_subgraph_dict[commodity_id]['subgraph']
                # distances: key = node ids within cutoff, value = length of paths
                # endcaps: key = node ids within cutoff, value = list of nodes labeled as endcaps
                # modeled after NetworkX single_source_dijkstra:
                # distances, paths = nx.single_source_dijkstra(G, facility_node_id, cutoff = MTD, weight = 'Length')
                fn_length = lambda u, v, d: min(attr.get('Length', 1) for attr in d.values())
                distances, endcaps = dijkstra(G, facility_node_id, fn_length, cutoff=MTD)

                logger.info("start: distances/paths for facility node ID " + str(facility_node_id))
                
                # Creates a subgraph of the nodes and edges that are reachable from the facility
                commodity_subgraph_dict[commodity_id]['facility_subgraphs'][facility_node_id] = G.subgraph(distances.keys()).copy()

                # If in G1 step for candidate generation, find endcaps
                if not os.path.exists(the_scenario.processor_candidates_commodity_data) and the_scenario.processors_candidate_slate_data != 'None':
                    ends[facility_node_id] = {}
                    ends[facility_node_id]['ends'] = endcaps
                    ends[facility_node_id]['ends'].extend([node for node in commodity_subgraph_dict[commodity_id]['dest_facilities'] if node in commodity_subgraph_dict[commodity_id]['facility_subgraphs'][facility_node_id]])
                    if 'intermodal_facilities' in commodity_subgraph_dict[commodity_id]:
                        ends[facility_node_id]['ends'].extend([node for node in commodity_subgraph_dict[commodity_id]['intermodal_facilities'] if node in commodity_subgraph_dict[commodity_id]['facility_subgraphs'][facility_node_id]])
                    # for node in commodity_subgraph_dict[commodity_id]['dest_facilities']:
                    #     if node in commodity_subgraph_dict[commodity_id]['facility_subgraphs'][facility_node_id] :
                    #         ends[facility_node_id]['ends'].append(node)
                    ends[facility_node_id]['commodity_id'] = commodity_id     

                logger.info("end: distances/paths for facility node ID " + str(facility_node_id))

    # If in G1 step for candidate generation, add endcaps to endcap_nodes table
    if not os.path.exists(the_scenario.processor_candidates_commodity_data) and the_scenario.processors_candidate_slate_data != 'None':                 
        with sqlite3.connect(the_scenario.main_db) as db_cur:
            # Create temp table to hold endcap nodes
            sql = """
                drop table if exists tmp_endcap_nodes;"""
            db_cur.execute(sql)

            sql = """
                create table tmp_endcap_nodes(
                node_id integer NOT NULL,
                location_id integer,
                mode_source default null,
                source_node_id integer,
                commodity_id integer NOT NULL,
                destination_yn text,
                CONSTRAINT endcap_key PRIMARY KEY (node_id, source_node_id, commodity_id))
            ;"""
            db_cur.execute(sql)

            for facility_node_id in ends:
                logger.info("Updating endcap_nodes for facility node ID " + str(facility_node_id))
                commodity_id = ends[facility_node_id]['commodity_id']
                end_list = ends[facility_node_id]['ends']
                for i in range(len(end_list)):
                    sql = """insert or replace into tmp_endcap_nodes (node_id, location_id, mode_source, source_node_id, commodity_id, destination_yn)
                             values({}, NULL, NULL, {}, {}, NULL);""".format(end_list[i], facility_node_id, commodity_id)
                    db_cur.execute(sql)
                    db_cur.commit()
            
            sql = """alter table tmp_endcap_nodes
                     add column source_facility_id;"""
            db_cur.execute(sql)

            sql = """update tmp_endcap_nodes
                     set source_facility_id = (select temp.source_facility_id
                                               from (select en.source_node_id, f.facility_id as source_facility_id
									                 from tmp_endcap_nodes en
									                 join networkx_nodes n on en.source_node_id = n.node_id
                                                     join facilities f on f.location_id = n.location_id) temp
									           where tmp_endcap_nodes.source_node_id = temp.source_node_id)"""
            db_cur.execute(sql)
            
            sql = """drop table if exists endcap_nodes;"""
            db_cur.execute(sql)

            sql = """
                create table endcap_nodes(
                node_id integer NOT NULL,
                location_id integer,
                mode_source default null,
                source_node_id integer,
                source_facility_id integer NOT NULL,
                commodity_id integer NOT NULL,
                process_id integer,
                shape_x integer,
                shape_y integer,
                destination_yn text,
                CONSTRAINT endcap_key PRIMARY KEY (node_id, source_facility_id, commodity_id, process_id))
            ;"""
            db_cur.execute(sql)

            # Join temp endcap nodes with process id and edge shapes and insert into endcap nodes table
            sql = """insert into endcap_nodes (node_id, location_id, mode_source, source_node_id, source_facility_id, commodity_id, process_id, shape_x, shape_y, destination_yn)
                        SELECT
                        tmp.node_id,
                        tmp.location_id,
                        tmp.mode_source,
                        tmp.source_node_id,
                        tmp.source_facility_id,
                        tmp.commodity_id,
                        cpc.process_id,
                        nwx.shape_x,
                        nwx.shape_y,
                        tmp.destination_yn
                        from tmp_endcap_nodes tmp
                        join candidate_process_commodities cpc on tmp.commodity_id = cpc.commodity_id
                        join networkx_nodes nwx on tmp.node_id = nwx.node_id"""
            db_cur.execute(sql)

            sql = "drop table if exists tmp_endcap_nodes;"
            db_cur.execute(sql)

    return commodity_subgraph_dict


# -----------------------------------------------------------------------------


def dijkstra(G, source, get_weight, pred=None, paths=None, cutoff=None,
             target=None):
    """Implementation of Dijkstra's algorithm
    Parameters
    ----------
    G : NetworkX graph
    source : node label
        Starting node for path.
    get_weight : function
        Function for getting edge weight.
    pred : list, optional (default=None)
        List of predecessors of a node.
    paths : dict, optional (default=None)
        Path from the source to a target node.
    cutoff : integer or float, optional (default=None)
        Depth to stop the search. Only paths of length <= cutoff are returned.
    target : node label, optional (default=None)
        Ending node for path.
    Returns
    -------
    distance, endcaps : dictionaries
        Returns a tuple of two dictionaries keyed by node.
        The first dictionary stores distance from the source.
        The second stores all endcap nodes for that node.
    """
    G_succ = G.succ if G.is_directed() else G.adj

    push = heappush
    pop = heappop
    dist = {}  # dictionary of final distances
    seen = {source: 0}
    c = count()
    fringe = []  # use heapq with (distance,label) tuples
    endcaps = []  # MARK MOD
    push(fringe, (0, next(c), source))

    while fringe:
        (d, _, v) = pop(fringe)
        if v in dist:
            continue  # already searched this node.
        dist[v] = d
        if v == target:
            break
        added = 0  # MARK MOD
        too_far = 0  # MARK MOD
        for u, e in G_succ[v].items():
            cost = get_weight(v, u, e)
            if cost is None:
                continue
            vu_dist = dist[v] + get_weight(v, u, e)
            if cutoff is not None:
                if vu_dist > cutoff:
                    too_far += 1  # MARK MOD
                    # endcaps.append(v)  # MARK MOD
                    continue
            if u in dist:
                if vu_dist < dist[u]:
                    raise ValueError('Contradictory paths found:',
                                     'negative weights?')
            elif u not in seen or vu_dist < seen[u]:
                added += 1  # MARK MOD
                seen[u] = vu_dist
                push(fringe, (vu_dist, next(c), u))
                if paths is not None:
                    paths[u] = paths[v] + [u]
                if pred is not None:
                    pred[u] = [v]
            elif vu_dist == seen[u]:
                if pred is not None:
                    pred[u].append(v)
        
        if added == 0 and too_far >= 1:  # MARK MOD
            endcaps.append(v)  # MARK MOD

    return (dist, endcaps)  # MARK MOD


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
def get_impedances(the_scenario, logger):

    road_impedance_weights_dict = {}
    rail_impedance_weights_dict = {}
    water_impedance_weights_dict = {}

    with open(the_scenario.impedance_weights_data, 'rt') as f:
        reader = csv.DictReader(f)
        for row in reader:
            mode = str(row["mode"])
            link_type = str(row["link_type"])
            weight = float(row["weight"])

            # road
            if mode == 'road':
                if link_type not in list(road_impedance_weights_dict.keys()):
                    road_impedance_weights_dict[link_type] = weight

            # rail
            if mode == 'rail':
                if link_type not in list(rail_impedance_weights_dict.keys()):
                    rail_impedance_weights_dict[link_type] = weight

            # water
            if mode == 'water':
                if link_type not in list(water_impedance_weights_dict.keys()):
                    water_impedance_weights_dict[link_type] = weight

    return road_impedance_weights_dict, rail_impedance_weights_dict, water_impedance_weights_dict

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

    # Create, road, rail, and water impedance dictionaries
    road_impedance_weights_dict, rail_impedance_weights_dict, water_impedance_weights_dict = get_impedances(the_scenario, logger)

    # save highest impedance from non-blank road types
    # will use this as the route scaling factor for artificial links (all modes) to represent first/last mile by road
    road_values = [road_impedance_weights_dict[lt] for lt in road_impedance_weights_dict if lt != '']
    truck_local = max(road_values)

    # use the artificial and reversed attribute to determine if
    # the link is kept
    # -------------------------------------------------------------
    edge_attrs = {}  # for storing the edge attributes which are set all at once
    deleted_edge_count = 0

    for u, v, keys, artificial in list(G.edges(data='Artificial', keys=True)):

        # initialize the route_cost_scaling variable to something
        # absurd so we know if it is getting set properly in the loop:
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

            # Handle directionality for all modes
            direction = G.edges[u, v, keys]["Dir_Flag"]
            if direction == 1 and reversed_link == 1:
                G.remove_edge(u, v, keys)
                deleted_edge_count += 1
                continue  # move on to the next edge
            elif direction == -1 and reversed_link == 0:
                G.remove_edge(u, v, keys)
                deleted_edge_count += 1
                continue  # move on to the next edge

            # check the mode type
            # ----------------------
            mode_type = G.edges[u, v, keys]['Mode_Type']
            unique_id = G.edges[u, v, keys]['source_OID']

            # set the mode specific weights
            # -----------------------------

            if mode_type == "rail":

                link_type = G.edges[u, v, keys]["Link_Type"]
                # Set any nulls to blanks
                if link_type is None:
                    link_type = ''
                if link_type in rail_impedance_weights_dict:
                    route_cost_scaling = rail_impedance_weights_dict[link_type]
                else:
                    # Give it the maximum impedance weight
                    route_cost_scaling = max(rail_impedance_weights_dict)
                    logger.debug(("Link_Type {} from OBJECTID {} in {} ".format(link_type, unique_id, mode_type) +
                                  "does not appear in impedance weight CSV. " +
                                  "Will be given maximum impedance weight of {} for {}".format(max(rail_impedance_weights_dict), mode_type)))

            elif mode_type == "water":

                link_type = G.edges[u, v, keys]['Link_Type']
                # Set any nulls to blanks
                if link_type is None:
                    link_type = ''
                if link_type in water_impedance_weights_dict:
                    route_cost_scaling = water_impedance_weights_dict[link_type]
                else:
                    # Give it the maximum impedance weight
                    route_cost_scaling = max(water_impedance_weights_dict)
                    logger.debug(("Link_Type {} from OBJECTID {} in {} ".format(link_type, unique_id, mode_type) +
                                  "does not appear in impedance weight CSV. " +
                                  "Will be given maximum impedance weight of {} for {}".format(max(water_impedance_weights_dict), mode_type)))

            elif mode_type == "road":

                link_type = G.edges[u, v, keys]['Link_Type']
                # Set any nulls to blanks
                if link_type is None:
                    link_type = ''
                if link_type in road_impedance_weights_dict:
                    route_cost_scaling = road_impedance_weights_dict[link_type]
                else:
                    # Give it the maximum impedance weight
                    route_cost_scaling = max(road_impedance_weights_dict)
                    logger.debug(("Link_Type {} from OBJECTID {} in {} ".format(link_type, unique_id, mode_type) +
                                  "does not appear in impedance weight CSV. " +
                                  "Will be given maximum impedance weight of {} for {}".format(max(road_impedance_weights_dict), mode_type)))

            elif 'pipeline' in mode_type:
                # convert pipeline tariff costs
                pipeline_tariff_cost = "{} {}/barrel".format(float(G.edges[u, v, keys]['base_rate']), the_scenario.default_units_currency)
                converted_pipeline_tariff_cost = "{}/{}".format(the_scenario.default_units_currency, the_scenario.default_units_liquid_phase)
                route_cost_scaling = Q_(pipeline_tariff_cost).to(converted_pipeline_tariff_cost).magnitude

        # Intermodal Edges - artificial == 2
        # ------------------------------------
        elif artificial == 2:
            # set it to 1 because we'll multiply by the appropriate
            # link_cost later for transloading
            route_cost_scaling = 1

        # Artificial Link Edges - artificial == 1
        # ----------------------------------
        # need to check if it is an IN location or an OUT location and delete selectively
        # assume always connecting from the node to the network
        # so _OUT locations should delete the reversed link
        # _IN locations should delete the non-reversed link
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

                # artificial links are scaled by the local road impedance
                # use the highest impedance from non-blank road types as a proxy
                # the cost_penalty is calculated in get_network_link_cost()
                else:
                    route_cost_scaling = truck_local
            except:
                logger.warning("the following keys didn't work: u - {}, v - {}".format(u, v))
        else:
            logger.warning("found an edge without artificial attribute: u - {}, v - {}".format(u, v))
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

    # Create, road, rail, and water impedance dictionaries
    road_impedance_weights_dict, rail_impedance_weights_dict, water_impedance_weights_dict = get_impedances(the_scenario, logger)
    
    # Set truck local weight equal to the highest road impedance value, ignoring any blank link types
    road_values = [road_impedance_weights_dict[lt] for lt in road_impedance_weights_dict if lt != '']
    truck_local = max(road_values)

    if phase_of_matter == "solid":
        # set the mode costs
        truck_base_cost = the_scenario.solid_truck_base_cost.magnitude
        railroad_class_1_cost = the_scenario.solid_railroad_class_1_cost.magnitude
        barge_cost = the_scenario.solid_barge_cost.magnitude
        transloading_cost = the_scenario.solid_transloading_cost.magnitude

    elif phase_of_matter == "liquid":
        # set the mode costs
        truck_base_cost = the_scenario.liquid_truck_base_cost.magnitude
        railroad_class_1_cost = the_scenario.liquid_railroad_class_1_cost.magnitude
        barge_cost = the_scenario.liquid_barge_cost.magnitude
        transloading_cost = the_scenario.liquid_transloading_cost.magnitude

    else:
        logger.error("the phase of matter: -- {} -- is not supported. returning")
        raise NotImplementedError

    if artificial == 1:
        # Use truck for first/last mile on all artificial links, regardless of mode
        link_cost = truck_base_cost

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
              "(edge_id INTEGER, phase_of_matter_id INT, route_cost REAL, transport_cost REAL)"
        db_con.execute(sql)

        # build up the network edges cost by phase of matter
        edge_cost_list = []

        # get phases_of_matter in the scenario
        phases_of_matter_in_scenario = get_phases_of_matter_in_scenario(the_scenario, logger)

        # loop through each edge in the networkx_edges table
        sql = "select edge_id, mode_source, artificial, length, route_cost_scaling from networkx_edges"
        db_cur = db_con.execute(sql)
        for row in db_cur:

            edge_id = row[0]
            mode_source = row[1]
            artificial = row[2]
            length = row[3]
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
                        transport_cost = length * link_cost  # link_cost is taken from the scenario file
                        route_cost = transport_cost * route_cost_scaling  # this includes impedance

                    else:
                        # if artificial = 0, route_cost_scaling = base rate
                        # we use the route_cost_scaling for this since its set in the GIS
                        # not in the scenario xml file.

                        transport_cost = route_cost_scaling  # this is the base rate
                        route_cost = route_cost_scaling  # this is the base rate

                elif artificial == 1:

                    # use road transport cost for first/last mile regardless of mode
                    transport_cost = length * link_cost 

                    # set short haul penalty for rail and water
                    if mode_source == "rail" and phase_of_matter == "solid":
                        penalty = ((the_scenario.solid_truck_base_cost * route_cost_scaling - the_scenario.solid_railroad_class_1_cost) * the_scenario.rail_short_haul_penalty).magnitude
                    elif mode_source == "rail" and phase_of_matter == "liquid":
                        penalty = ((the_scenario.liquid_truck_base_cost * route_cost_scaling - the_scenario.liquid_railroad_class_1_cost) * the_scenario.rail_short_haul_penalty).magnitude
                    elif mode_source == "water" and phase_of_matter == "solid":
                        penalty = ((the_scenario.solid_truck_base_cost * route_cost_scaling - the_scenario.solid_barge_cost) * the_scenario.water_short_haul_penalty).magnitude
                    elif mode_source == "water" and phase_of_matter == "liquid":
                        penalty = ((the_scenario.liquid_truck_base_cost * route_cost_scaling - the_scenario.liquid_barge_cost) * the_scenario.water_short_haul_penalty).magnitude
                    else:
                        # road or pipeline: no short hual penalty
                        penalty = 0
                    
                    # routing cost for artificial links
                    route_cost = transport_cost * route_cost_scaling + penalty/2

                elif artificial == 2:
                    transport_cost = link_cost / 2.00  # this is the transloading fee
                    # For now dividing by 2 to ensure that the transloading fee is not applied twice
                    # (e.g. on way in and on way out)
                    route_cost = link_cost / 2.00  # same as above.

                else:
                    logger.warning("artificial code of {} is not supported!".format(artificial))

                edge_cost_list.append([edge_id, phase_of_matter, route_cost, transport_cost])

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
              "INT, artificial INT, mode_source TEXT, mode_source_oid INT, length REAL, route_cost_scaling REAL, " \
              "capacity INT, volume REAL, VCR REAL)"
        db_con.execute(sql)

        for (u, v, c, d) in G.edges(keys=True, data='route_cost_scaling', default=False):
            from_node_id = u
            to_node_id = v
            length = G.edges[(u, v, c)]['Length']
            artificial = G.edges[(u, v, c)]['Artificial']
            mode_source = G.edges[(u, v, c)]['Mode_Type']
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
                logger.detailed_debug("EDGE: {}, {}, {} - link capacity == 0, setting to None".format(u, v, c))

            if 'route_cost_scaling' in G.edges[(u, v, c)]:
                route_cost_scaling = G.edges[(u, v, c)]['route_cost_scaling']
            else:
                logger.warning(
                    "EDGE: {}, {}, {} - mode: {} - artificial {} -- "
                    "does not have key route_cost_scaling".format(u, v, c, mode_source, artificial))

            edge_list.append(
                [from_node_id, to_node_id, artificial, mode_source, mode_source_oid, length, route_cost_scaling,
                 capacity, volume, vcr])

        # the node_id will be used to explode the edges by commodity and time period
        if edge_list:
            update_sql = """
                INSERT into networkx_edges
                values (null,?,?,?,?,?,?,?,?,?,?)
                ;"""
            db_con.executemany(update_sql, edge_list)
            db_con.commit()
            logger.debug("finished network_x edges commit")

    return G

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

