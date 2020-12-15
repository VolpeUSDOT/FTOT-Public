# ------------------------------------------------------------------------------
# ftot_networkx.py
# Purpose: the purpose of this module is to handle all the NetworkX methods and operations
# necessary to go between FTOT layers: GIS, sqlite DB, etc.
# Revised: 1/15/19
# ------------------------------------------------------------------------------

import networkx as nx
import sqlite3
from shutil import rmtree
import datetime
import ftot_supporting
import arcpy
import os


# -----------------------------------------------------------------------------


def graph(the_scenario, logger):

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

# -----------------------------------------------------------------------------


def make_networkx_graph(the_scenario, logger):
    # High level work flow:
    # ------------------------
    # make_networkx_graph
    # create the multidigraph
    # convert the node labels to integers
    # reverse the graph and compose with self
    # delete temporary files

    logger.info("start: make_networkx_graph")
    start_time = datetime.datetime.now()

    # read the shapefiles in the customized read_shp method
    input_path = the_scenario.lyr_files_dir

    logger.debug("start: read_shp")
    G = read_shp(input_path, logger, simplify=True,
                 geom_attrs=False, strict=True)  # note this custom and not nx.read_shp()

    # cleanup the node labels
    logger.debug("start: convert node labels")
    G = nx.convert_node_labels_to_integers(G, first_label=0, ordering='default', label_attribute="x_y_location")

    # create a reversed graph
    logger.debug("start: reverse G graph to H")
    H = G.reverse()  # this is a reversed version of the graph.

    # set the a new attribute for every edge that says its a "reversed" link
    # we will use this to delete edges that shouldn't be reversed later.
    logger.debug("start: set 'reversed' attribute in H")
    nx.set_edge_attributes(H, 1, "REVERSED")

    # add the two graphs together
    logger.debug("start: compose G and H")
    G = nx.compose(G, H)

    # DF changed: DO NOT delete temporary files
    # logger.debug("start: delete the temp_networkx_shp_files dir")
    # rmtree(input_path)

    # print out some stats on the Graph
    logger.info("Number of nodes in the raw graph: {}".format(G.order()))
    logger.info("Number of edges in the raw graph: {}".format(G.size()))

    logger.debug(
        "finished: make_networkx_graph: Runtime (HMS): \t{}".format(
            ftot_supporting.get_total_runtime_string(start_time)))

    return G

# -----------------------------------------------------------------------------


def export_fcs_from_main_gdb(the_scenario, logger):
    # export fcs from the main.GDB to individual shapefiles
    logger.info("start: export_fcs_from_main_gdb")
    start_time = datetime.datetime.now()

    # export network and locations fc's to shapefiles
    main_gdb = the_scenario.main_gdb
    output_path = the_scenario.lyr_files_dir
    input_features = "\""

    logger.debug("start: create temp_networkx_shp_files dir")
    if os.path.exists(output_path):
        logger.debug("deleting pre-existing temp_networkx_shp_files dir")
        rmtree(output_path)

    if not os.path.exists(output_path):
        logger.debug("creating new temp_networkx_shp_files dir")
        os.makedirs(output_path)

    # get the locations and network feature layers
    for fc in ['\\locations;', '\\network\\intermodal;', '\\network\\locks;', '\\network\\pipeline_prod_trf_rts;',
               '\\network\\pipeline_crude_trf_rts;', '\\network\\water;', '\\network\\rail;', '\\network\\road']:
        input_features += main_gdb + fc
    input_features += "\""
    arcpy.FeatureClassToShapefile_conversion(Input_Features=input_features, Output_Folder=output_path)

    logger.debug("finished: export_fcs_from_main_gdb: Runtime (HMS): \t{}".format(
        ftot_supporting.get_total_runtime_string(start_time)))


# ------------------------------------------------------------------------------


def clean_networkx_graph(the_scenario, G, logger):
    # -------------------------------------------------------------------------
    # renamed clean_networkx_graph ()
    # remove reversed links for pipeline
    # selectivity remove links for location _IN and _OUT nodes
    # preserve the route_cost_scaling factor in an attribute by phase of matter

    logger.info("start: clean_networkx_graph")
    start_time = datetime.datetime.now()

    logger.debug("Processing the {} edges in the uncosted graph.".format(G.size()))

    # use the artificial and reversed attribute to determine if
    # the link is kept
    # -------------------------------------------------------------
    edge_attrs = {}  # for storing the edge attributes which are set all at once
    deleted_edge_count = 0

    for u, v, keys, artificial in G.edges(data='Artificial', keys=True):

        # initialize the route_cost_scaling variable to something
        # absurd so we know if its getting set properly in the loop:
        route_cost_scaling = -999999999

        # check if the link is reversed
        if 'REVERSED' in G.edges[u, v, keys]:
            reversed_link = G.edges[u, v, keys]['REVERSED']
        else:
            reversed_link = 0

        # check if capacity is 0
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
    # (0 = network edge, 2  = intermodal, 1 = artificial link btw facility location and network edge)
    # add the appropriate cost to the network edges based on phase of matter

    if phase_of_matter == "solid":
        # set the mode costs
        truck_base_cost = the_scenario.solid_truck_base_cost
        railroad_class_1_cost = the_scenario.solid_railroad_class_1_cost
        barge_cost = the_scenario.solid_barge_cost
        transloading_cost = the_scenario.transloading_dollars_per_ton

    elif phase_of_matter == "liquid":
        # set the mode costs
        truck_base_cost = the_scenario.liquid_truck_base_cost
        railroad_class_1_cost = the_scenario.liquid_railroad_class_1_cost
        barge_cost = the_scenario.liquid_barge_cost
        transloading_cost = the_scenario.transloading_dollars_per_thousand_gallons
    else:
        logger.error("the phase of matter: -- {} -- is not supported. returning")
        raise NotImplementedError

    if artificial == 1:

        # add a cost penalty to the routing cost for rail and water artificial links
        # to prevent them from taking short trips on these modes instead of road.
        # currently, taking the difference between the local road and water or road rate
        # and multiplying for 100 miles regardless of the artificial link distance.
        if mode == "rail":
            link_cost = ((the_scenario.truck_local * truck_base_cost) - railroad_class_1_cost) * 100 / 2.0
            # Divide by 2 is to ensure the penalty is not doubled-- it is applied on artificial links on both ends
        elif mode == "water":
            link_cost = ((the_scenario.truck_local * truck_base_cost) - barge_cost) * 100 / 2.0
            # Divide by 2 is to ensure the penalty is not doubled-- it is applied on artificial links on both ends
        elif 'pipeline' in mode:
            # this cost penalty was calculated by looking at the average per mile base rate.
            link_cost = 0.19
            # no mileage multiplier here for pipeline as unlike rail/water, we do not want to disproportionally
            # discourage short movements
            # Multiplier will be applied based on actual link mileage when scenario costs are actually set
        else:
            link_cost = the_scenario.truck_local * truck_base_cost  # for road-- providing a local road per mile penalty

    elif artificial == 2:
        # phase of mater is determined above
        link_cost = transloading_cost

    elif artificial == 0:

        if phase_of_matter == "solid":
            if mode == "road":
                link_cost = the_scenario.solid_truck_base_cost
            elif mode == "rail":
                link_cost = the_scenario.solid_railroad_class_1_cost
            elif mode == "water":
                link_cost = the_scenario.solid_barge_cost
            else:
                logger.warning("MODE: {} IS NOT SUPPORTED! for PHASE_OF_MATTER: {} ".format(mode, phase_of_matter))
                link_cost = 9999

        elif phase_of_matter == "liquid":
            if mode == "road":
                link_cost = the_scenario.liquid_truck_base_cost
            elif mode == "rail":
                link_cost = the_scenario.liquid_railroad_class_1_cost
            elif mode == "water":
                link_cost = the_scenario.liquid_barge_cost
            elif mode == "pipeline_crude_trf_rts":
                link_cost = 1  # so we multiply by the base_rate
            elif mode == "pipeline_prod_trf_rts":
                link_cost = 1  # so we multiply by base_rate
            else:
                logger.warning("MODE: {} IS NOT SUPPORTED!".format(mode))
        else:
            logger.warning("PHASE OF MATTER {} IS NOT SUPPORTED!".format(phase_of_matter))

    else:
        logger.warning("the commodity phase is not supported: {}".format(phase_of_matter))

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


def set_network_costs_in_db(the_scenario, logger):

    # set the network costs in the db by phase_of_matter
    # dollar_cost = mileage * phase_of_matter_cost
    # routing_cost = dollar_cost * route_cost_scaling (factor in the networkx_edge table)

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

        # loop through each edge in the network_edges table
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
            attributes = dict(zip(fields, fld_data))
            attributes["ShpName"] = lyr.GetName()
            # Note:  Using layer level geometry type
            if g.GetGeometryType() == ogr.wkbPoint:
                net.add_node(g.GetPoint_2D(0), **attributes)
            elif g.GetGeometryType() in (ogr.wkbLineString,
                                         ogr.wkbMultiLineString):
                for edge in edges_from_line(g, attributes, simplify,
                                            geom_attrs):

                    e1, e2, attr = edge
                    net.add_edge(e1, e2)
                    key = len(net[e1][e2].keys()) - 1
                    net[e1][e2][key].update(attr)
            else:
                if strict:
                    logger.error("GeometryType {} not supported".
                                 format(g.GetGeometryType()))
                    raise nx.NetworkXError("GeometryType {} not supported".
                                           format(g.GetGeometryType()))

    return net


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
