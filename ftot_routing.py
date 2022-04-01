# ------------------------------------------------------------------------------
# ftot_routing.py
# Purpose: the purpose of this module is to clean up,
# create the locations_fc,
# hook locations into the network,
# ignore locations not connected to the network,
# export capacity information to the main.db,
# export the assets from GIS export_fcs_from_main_gdb

# Revised: 1/15/19 - MNP
# ------------------------------------------------------------------------------

import os
import arcpy
import sqlite3
import ftot_supporting_gis
from ftot import Q_

LCC_PROJ = arcpy.SpatialReference('USA Contiguous Lambert Conformal Conic')


# ========================================================================


def connectivity(the_scenario, logger):

        checks_and_cleanup(the_scenario, logger)

        # create the locations_fc
        create_locations_fc(the_scenario, logger)

        # use MBG to subset the road network to a buffer around the locations FC
        minimum_bounding_geometry(the_scenario, logger)

        # hook locations into the network
        hook_locations_into_network(the_scenario, logger)

        # ignore locations not connected to the network
        ignore_locations_not_connected_to_network(the_scenario, logger)

        # report out material missing after connecting to the network
        from ftot_facilities import db_report_commodity_potentials
        db_report_commodity_potentials(the_scenario, logger)

        # export capacity information to the main.db
        cache_capacity_information(the_scenario, logger)


# =========================================================================


def checks_and_cleanup(the_scenario, logger):
    logger.info("start: checks_and_cleanup")

    scenario_gdb = the_scenario.main_gdb
    if not os.path.exists(scenario_gdb):
        error = "can't find scenario gdb {}".format(scenario_gdb)
        raise IOError(error)

    # check for scenario DB
    # ---------------------------------
    scenario_db = the_scenario.main_db
    if not arcpy.Exists(scenario_db):
        raise Exception("scenario_db not found {} ".format(scenario_db))

    logger.debug("finish: checks_and_cleanup")


# ==============================================================================


def create_locations_fc(the_scenario, logger):
    logger.info("start: create_locations_fc")
    co_location_offet = 0.1
    logger.debug("co-location offset is necessary to prevent the locations from being treated as intermodal "
                 "facilities.")
    logger.debug("collocation off-set: {} meters".format(co_location_offet))

    locations_fc = the_scenario.locations_fc

    # delete the old location FC before we create it
    from ftot_facilities import gis_clear_feature_class
    gis_clear_feature_class(locations_fc, logger)

    # create the feature class
    arcpy.CreateFeatureclass_management(the_scenario.main_gdb, "locations", "POINT", "#", "DISABLED", "DISABLED",
                                        ftot_supporting_gis.LCC_PROJ)

    # add the location_id field
    arcpy.AddField_management(locations_fc, "location_id", "TEXT")

    # add the location_id_name field
    arcpy.AddField_management(locations_fc, "location_id_name", "TEXT")

    # add the connects_road field
    arcpy.AddField_management(locations_fc, "connects_road", "SHORT")

    # add the connects_rail field
    arcpy.AddField_management(locations_fc, "connects_rail", "SHORT")

    # add the connects_water field
    arcpy.AddField_management(locations_fc, "connects_water", "SHORT")

    # add the connects_pipeline_prod field
    arcpy.AddField_management(locations_fc, "connects_pipeline_prod_trf_rts", "SHORT")

    # add the connects_pipeline_crude field
    arcpy.AddField_management(locations_fc, "connects_pipeline_crude_trf_rts", "SHORT")

    # add the ignore field
    arcpy.AddField_management(locations_fc, "ignore", "SHORT")

    # start an edit session for the insert cursor
    edit = arcpy.da.Editor(the_scenario.main_gdb)
    edit.startEditing(False, False)
    edit.startOperation()

    # create insert cursor
    with arcpy.da.InsertCursor(locations_fc, ["location_id_name", "location_id", "SHAPE@"]) as insert_cursor:

        # loop through DB and populate the fc
        with sqlite3.connect(the_scenario.main_db) as db_con:

            sql = "select * from locations;"
            db_cur = db_con.execute(sql)
            for row in db_cur:
                location_id = row[0]

                # create a point for each location "out"
                location_point = arcpy.Point()
                location_point.X = row[1] + co_location_offet
                location_point.Y = row[2] + co_location_offet
                location_point_geom = arcpy.PointGeometry(location_point, LCC_PROJ)

                insert_cursor.insertRow([str(location_id) + "_OUT", location_id, location_point_geom])

                # create a point for each location "in"
                location_point = arcpy.Point()
                location_point.X = row[1] - co_location_offet
                location_point.Y = row[2] - co_location_offet
                location_point_geom = arcpy.PointGeometry(location_point, LCC_PROJ)

                insert_cursor.insertRow([str(location_id) + "_IN", location_id, location_point_geom])

    edit.stopOperation()
    edit.stopEditing(True)

    logger.debug("finish: create_locations_fc")


# ==============================================================================


def get_xy_location_id_dict(the_scenario, logger):
    logger.debug("start: get_xy_location_id_dict")

    with sqlite3.connect(the_scenario.main_db) as db_con:

        sql = "select location_id, shape_x, shape_y from locations;"
        db_cur = db_con.execute(sql)

        xy_location_id_dict = {}
        for row in db_cur:
            location_id = row[0]
            shape_x = row[1]
            shape_y = row[2]
            xy_location_id_dict[location_id] = "[{}, {}]".format(shape_x, shape_y)

    logger.debug("finish: get_xy_location_id_dict")

    return xy_location_id_dict


# ==============================================================================


def get_location_id_name_dict(the_scenario, logger):
    logger.debug("start: get_location_id_name_dict")

    location_id_name_dict = {}

    with arcpy.da.SearchCursor(the_scenario.locations_fc, ["location_id_name", "OBJECTID"]) as scursor:

        for row in scursor:
            location_id_name = row[0]
            objectid = row[1]
            #            shape = row[2]
            location_id_name_dict[objectid] = location_id_name

    logger.debug("finish: get_location_id_name_dict")

    return location_id_name_dict


# ===============================================================================


def delete_old_artificial_link(the_scenario, logger):
    logger.debug("start: delete_old_artificial_link")
    for mode in ["road", "rail", "water", "pipeline_crude_trf_rts", "pipeline_prod_trf_rts"]:
        edit = arcpy.da.Editor(the_scenario.main_gdb)
        edit.startEditing(False, False)
        edit.startOperation()

        with arcpy.da.UpdateCursor(os.path.join(the_scenario.main_gdb, 'network', mode), ["artificial"],
                                   where_clause="Artificial = 1") as cursor:
            for row in cursor:
                cursor.deleteRow()

        edit.stopOperation()
        edit.stopEditing(True)
    logger.debug("finish: delete_old_artificial_link")


# ===============================================================================


def cut_lines(line_list, point_list, split_lines):
    for line in line_list:
        is_cut = "Not Cut"
        if line.length > 0.0:  # Make sure it's not an empty geometry.
            for point in point_list:
                # Even "coincident" points can show up as spatially non-coincident in their
                # floating-point XY values, so we set up a tolerance.
                if line.distanceTo(point) < 1.0:
                    # To ensure coincidence, snap the point to the line before proceeding.
                    snap_point = line.snapToLine(point).firstPoint
                    # Make sure the point isn't on a line endpoint, otherwise cutting will produce
                    # an empty geometry.
                    if not (snap_point.equals(line.lastPoint) and snap_point.equals(line.firstPoint)):
                        # Cut the line. Try it a few different ways to try increase the likelihood it will actually cut
                        cut_line_1, cut_line_2 = line.cut(arcpy.Polyline(arcpy.Array(
                            [arcpy.Point(snap_point.X + 10.0, snap_point.Y + 10.0),
                             arcpy.Point(snap_point.X - 10.0, snap_point.Y - 10.0)]), LCC_PROJ))
                        if cut_line_1.length == 0 or cut_line_2.length == 0:
                            cut_line_1, cut_line_2 = line.cut(arcpy.Polyline(arcpy.Array(
                                [arcpy.Point(snap_point.X - 10.0, snap_point.Y + 10.0),
                                 arcpy.Point(snap_point.X + 10.0, snap_point.Y - 10.0)]), LCC_PROJ))
                        if cut_line_1.length == 0 or cut_line_2.length == 0:
                            cut_line_1, cut_line_2 = line.cut(arcpy.Polyline(arcpy.Array(
                                [arcpy.Point(snap_point.X + 10.0, snap_point.Y),
                                 arcpy.Point(snap_point.X - 10.0, snap_point.Y)]), LCC_PROJ))
                        if cut_line_1.length == 0 or cut_line_2.length == 0:
                            cut_line_1, cut_line_2 = line.cut(arcpy.Polyline(arcpy.Array(
                                [arcpy.Point(snap_point.X, snap_point.Y + 10.0),
                                 arcpy.Point(snap_point.X, snap_point.Y - 10.0)]), LCC_PROJ))
                        # Make sure both descendents have non-zero geometry.
                        if cut_line_1.length > 0.0 and cut_line_2.length > 0.0:
                            # Feed the cut lines back into the "line" list as candidates to be cut again.
                            line_list.append(cut_line_1)
                            line_list.append(cut_line_2)
                            line_list.remove(line)
                            # The cut loop will only exit when all lines cannot be cut smaller without producing
                            # zero-length geometries
                            is_cut = "Cut"
                            # break the loop because we've cut a line into two now and need to start over.
                            break
                point_list.remove(point)

        if is_cut == "Not Cut" and len(point_list) == 0:
            split_lines.append(line)
            line_list.remove(line)

        if len(line_list) == 0 and len(point_list) == 0:
            continue_iteration = 'done'
        else:
            continue_iteration = 'continue running'
        return line_list, point_list, split_lines, continue_iteration

# ===============================================================================


def hook_locations_into_network(the_scenario, logger):

    # Add artificial links from the locations feature class into the network
    # -----------------------------------------------------------------------
    logger.info("start: hook_location_into_network")

    scenario_gdb = the_scenario.main_gdb
    if not os.path.exists(scenario_gdb):
        error = "can't find scenario gdb {}".format(scenario_gdb)
        raise IOError(error)

    # LINKS TO/FROM LOCATIONS
    # ---------------------------
    road_max_artificial_link_distance_miles = str(the_scenario.road_max_artificial_link_dist.magnitude) + " Miles"
    rail_max_artificial_link_distance_miles = str(the_scenario.rail_max_artificial_link_dist.magnitude) + " Miles"
    water_max_artificial_link_distance_miles = str(the_scenario.water_max_artificial_link_dist.magnitude) + " Miles"
    pipeline_crude_max_artificial_link_distance_miles = str(the_scenario.pipeline_crude_max_artificial_link_dist.magnitude) + " Miles"
    pipeline_prod_max_artificial_link_distance_miles = str(the_scenario.pipeline_prod_max_artificial_link_dist.magnitude) + " Miles"

    # cleanup any old artificial links
    delete_old_artificial_link(the_scenario, logger)

    # LINKS TO/FROM LOCATIONS
    # ---------------------------
    locations_fc = the_scenario.locations_fc

    arcpy.AddField_management(os.path.join(scenario_gdb, locations_fc), "connects_road", "SHORT")
    arcpy.CalculateField_management(os.path.join(scenario_gdb, locations_fc), "connects_road", 0, "PYTHON_9.3")

    arcpy.AddField_management(os.path.join(scenario_gdb, locations_fc), "connects_rail", "SHORT")
    arcpy.CalculateField_management(os.path.join(scenario_gdb, locations_fc), "connects_rail", 0, "PYTHON_9.3")

    arcpy.AddField_management(os.path.join(scenario_gdb, locations_fc), "connects_water", "SHORT")
    arcpy.CalculateField_management(os.path.join(scenario_gdb, locations_fc), "connects_water", 0, "PYTHON_9.3")

    arcpy.AddField_management(os.path.join(scenario_gdb, locations_fc), "connects_pipeline_prod_trf_rts", "SHORT")
    arcpy.CalculateField_management(os.path.join(scenario_gdb, locations_fc), "connects_pipeline_prod_trf_rts", 0,
                                    "PYTHON_9.3")

    arcpy.AddField_management(os.path.join(scenario_gdb, locations_fc), "connects_pipeline_crude_trf_rts", "SHORT")
    arcpy.CalculateField_management(os.path.join(scenario_gdb, locations_fc), "connects_pipeline_crude_trf_rts", 0,
                                    "PYTHON_9.3")

    # check for permitted modes before creating artificial links
    from ftot_networkx import check_permitted_modes
    check_permitted_modes(the_scenario, logger)

    if 'road' in the_scenario.permittedModes:
        locations_add_links(logger, the_scenario, "road", road_max_artificial_link_distance_miles)
    if 'rail' in the_scenario.permittedModes:
        locations_add_links(logger, the_scenario, "rail", rail_max_artificial_link_distance_miles)
    if 'water' in the_scenario.permittedModes:
        locations_add_links(logger, the_scenario, "water", water_max_artificial_link_distance_miles)
    if 'pipeline_crude_trf_rts' in the_scenario.permittedModes:
        locations_add_links(logger, the_scenario, "pipeline_crude_trf_rts",
                            pipeline_crude_max_artificial_link_distance_miles)
    if 'pipeline_prod_trf_rts' in the_scenario.permittedModes:
        locations_add_links(logger, the_scenario, "pipeline_prod_trf_rts",
                            pipeline_prod_max_artificial_link_distance_miles)

    # ADD THE SOURCE AND SOURCE_OID FIELDS SO WE CAN MAP THE LINKS IN THE NETWORK TO THE GRAPH EDGES.
    # -----------------------------------------------------------------------------------------------
    for fc in ['road', 'rail', 'water', 'pipeline_crude_trf_rts', 'pipeline_prod_trf_rts', 'locations', 'intermodal',
               'locks']:
        logger.debug("start: processing source and source_OID for: {}".format(fc))
        arcpy.DeleteField_management(os.path.join(scenario_gdb, fc), "source")
        arcpy.DeleteField_management(os.path.join(scenario_gdb, fc), "source_OID")
        arcpy.AddField_management(os.path.join(scenario_gdb, fc), "source", "TEXT")
        arcpy.AddField_management(os.path.join(scenario_gdb, fc), "source_OID", "LONG")
        arcpy.CalculateField_management(in_table=os.path.join(scenario_gdb, fc), field="source",
                                        expression='"{}"'.format(fc), expression_type="PYTHON_9.3", code_block="")
        arcpy.CalculateField_management(in_table=os.path.join(scenario_gdb, fc), field="source_OID",
                                        expression="!OBJECTID!", expression_type="PYTHON_9.3", code_block="")
        logger.debug("finish: processing source_OID for: {}".format(fc))

    logger.debug("finish: hook_location_into_network")


# ==============================================================================


def cache_capacity_information(the_scenario, logger):
    logger.info("start: cache_capacity_information")

    logger.debug(
        "export the capacity, volume, and vcr data to the main.db for the locks, pipelines, and intermodal fcs")

    # need to cache out the pipeline, locks, and intermodal facility capacity information
    # note source_oid is master_oid for pipeline capacities
    # capacity_table: source, id_field_name, source_oid, capacity, volume, vcr
    with sqlite3.connect(the_scenario.main_db) as main_db_con:

        # drop the table
        sql = "drop table if exists capacity_nodes"
        main_db_con.execute(sql)
        # create the table
        sql = "create table capacity_nodes(" \
              "source text, " \
              "id_field_name text, " \
              "source_OID integer, " \
              "capacity real, " \
              "volume real, " \
              "vcr real" \
              ");"
        main_db_con.execute(sql)

    for fc in ['locks', 'intermodal', 'pipeline_crude', 'pipeline_prod']:
        capacity_update_list = []  # initialize the list at the beginning of every fc.
        logger.debug("start: processing fc: {} ".format(fc))
        if 'pipeline' in fc:
            id_field_name = 'MASTER_OID'
            fields = ['MASTER_OID', 'Capacity', 'Volume', 'VCR']
        else:
            id_field_name = 'source_OID'
            fields = ['source_OID', 'Capacity', 'Volume', 'VCR']

        # do the search cursor:
        logger.debug("start: search cursor on: fc: {} with fields: {}".format(fc, fields))
        with arcpy.da.SearchCursor(os.path.join(the_scenario.main_gdb, fc), fields) as cursor:
            for row in cursor:
                source = fc
                source_OID = row[0]
                capacity = row[1]
                volume = row[2]
                vcr = row[3]
                capacity_update_list.append([source, id_field_name, source_OID, capacity, volume, vcr])

        logger.debug("start: execute many on: fc: {} with len: {}".format(fc, len(capacity_update_list)))
        if len(capacity_update_list) > 0:
            sql = "insert or ignore into capacity_nodes " \
                  "(source, id_field_name, source_oid, capacity, volume, vcr) " \
                  "values (?, ?, ?, ?, ?, ?);"
            main_db_con.executemany(sql, capacity_update_list)
            main_db_con.commit()

    # now get the mapping fields from the *_trf_rts and *_trf_sgmnts tables
    # db table will have the form: source, id_field_name, id, mapping_id_field_name, mapping_id
    with sqlite3.connect(the_scenario.main_db) as main_db_con:

        # drop the table
        sql = "drop table if exists pipeline_mapping"
        main_db_con.execute(sql)
        # create the table
        sql = "create table pipeline_mapping(" \
              "source text, " \
              "id_field_name text, " \
              "id integer, " \
              "mapping_id_field_name text, " \
              "mapping_id integer);"
        main_db_con.execute(sql)

    capacity_update_list = []
    for fc in ['pipeline_crude_trf_rts', 'pipeline_crude_trf_sgmts', 'pipeline_prod_trf_rts',
               'pipeline_prod_trf_sgmts']:

        logger.debug("start: processing fc: {} ".format(fc))
        if '_trf_rts' in fc:
            id_field_name = 'source_OID'
            mapping_id_field_name = 'tariff_ID'
            fields = ['source_OID', 'tariff_ID']
        else:
            id_field_name = 'tariff_ID'
            mapping_id_field_name = 'MASTER_OID'
            fields = ['tariff_ID', 'MASTER_OID']

        # do the search cursor:
        logger.debug("start: search cursor on: fc: {} with fields: {}".format(fc, fields))
        with arcpy.da.SearchCursor(os.path.join(the_scenario.main_gdb, fc), fields) as cursor:
            for row in cursor:

                id = row[0]
                mapping_id = row[1]
                capacity_update_list.append([fc, id_field_name, id, mapping_id_field_name, mapping_id])

    logger.debug("start: execute many on: fc: {} with len: {}".format(fc, len(capacity_update_list)))

    if len(capacity_update_list) > 0:
        sql = "insert or ignore into pipeline_mapping  " \
              "(source, " \
              "id_field_name, " \
              "id, " \
              "mapping_id_field_name, " \
              "mapping_id) " \
              "values (?, ?, ?, ?, ?);"

        main_db_con.executemany(sql, capacity_update_list)
        main_db_con.commit()


# ==============================================================================

def locations_add_links(logger, the_scenario, modal_layer_name, max_artificial_link_distance_miles):

    # ADD LINKS LOGIC
    # first we near the mode to the locations fc
    # then we iterate through the near table and build up a dictionary of links and all the near XYs on that link.
    # then we split the links on the mode (except pipeline) and preserve the data of that link.
    # then we near the locations to the nodes on the now split links.
    # we ignore locations with near dist == 0 on those nodes.
    # then we add the artificial link and note which locations got links.
    # then we set the connects_to field if the location was connected.

    logger.debug("start: locations_add_links for mode: {}".format(modal_layer_name))

    scenario_gdb = the_scenario.main_gdb
    fp_to_modal_layer = os.path.join(scenario_gdb, "network", modal_layer_name)

    locations_fc = the_scenario.locations_fc
    arcpy.DeleteField_management(fp_to_modal_layer, "LOCATION_ID")
    arcpy.AddField_management(os.path.join(scenario_gdb, modal_layer_name), "LOCATION_ID", "long")

    arcpy.DeleteField_management(fp_to_modal_layer, "LOCATION_ID_NAME")
    arcpy.AddField_management(os.path.join(scenario_gdb, modal_layer_name), "LOCATION_ID_NAME", "text")

    if float(max_artificial_link_distance_miles.strip(" Miles")) < 0.0000001:
        logger.warning("Note: ignoring mode {}. User specified artificial link distance of {}".format(
            modal_layer_name, max_artificial_link_distance_miles))
        logger.debug("Setting the definition query to artificial = 99999, so we get an empty dataset for the "
                     "make_feature_layer and subsequent near analysis")

        definition_query = "Artificial = 999999"  # something to return an empty set
    else:
        definition_query = "Artificial = 0"  # the normal def query.

    if "pipeline" in modal_layer_name:

        if arcpy.Exists(os.path.join(scenario_gdb, "network", fp_to_modal_layer + "_points")):
            arcpy.Delete_management(os.path.join(scenario_gdb, "network", fp_to_modal_layer + "_points"))

        # limit near to end points
        if arcpy.CheckProduct("ArcInfo") == "Available":
            arcpy.FeatureVerticesToPoints_management(in_features=fp_to_modal_layer,
                                                     out_feature_class=fp_to_modal_layer + "_points",
                                                     point_location="BOTH_ENDS")
        else:
            logger.warning("The Advanced/ArcInfo license level of ArcGIS is not available. Modified feature vertices "
                           "process will be run.")
            arcpy.AddGeometryAttributes_management(fp_to_modal_layer, "LINE_START_MID_END")
            arcpy.MakeXYEventLayer_management(fp_to_modal_layer, "START_X", "START_Y",
                                              "modal_start_points_lyr", LCC_PROJ)

            arcpy.MakeXYEventLayer_management(fp_to_modal_layer, "END_X", "END_Y",
                                              "modal_end_points_lyr", LCC_PROJ)

            # Due to tool design, must define the feature class location and name slightly differently (separating
            # scenario gdb from feature class name. fp_to_modal_layer is identical to scenario_gdb + "network"
            # + modal_layer_name
            arcpy.FeatureClassToFeatureClass_conversion("modal_start_points_lyr",
                                                        scenario_gdb,
                                                        os.path.join("network", modal_layer_name + "_points"))

            arcpy.Append_management(["modal_end_points_lyr"],
                                    fp_to_modal_layer + "_points", "NO_TEST")

            arcpy.Delete_management("modal_start_points_lyr")
            arcpy.Delete_management("modal_end_points_lyr")
            arcpy.DeleteField_management(fp_to_modal_layer, "START_X")
            arcpy.DeleteField_management(fp_to_modal_layer, "START_Y")
            arcpy.DeleteField_management(fp_to_modal_layer, "MID_X")
            arcpy.DeleteField_management(fp_to_modal_layer, "MID_Y")
            arcpy.DeleteField_management(fp_to_modal_layer, "END_X")
            arcpy.DeleteField_management(fp_to_modal_layer, "END_Y")

        logger.debug("start:  make_feature_layer_management")
        arcpy.MakeFeatureLayer_management(fp_to_modal_layer + "_points", "modal_lyr_" + modal_layer_name,
                                          definition_query)

    else:
        logger.debug("start:  make_feature_layer_management")
        arcpy.MakeFeatureLayer_management(fp_to_modal_layer, "modal_lyr_" + modal_layer_name, definition_query)

    logger.debug("adding links between locations_fc and mode {} with max dist of {}".format(modal_layer_name,
                                                                                            max_artificial_link_distance_miles))

    if arcpy.Exists(os.path.join(scenario_gdb, "tmp_near")):
        logger.debug("start:  delete tmp near")
        arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_near"))

    logger.debug("start:  generate_near")
    if arcpy.CheckProduct("ArcInfo") == "Available":
        arcpy.GenerateNearTable_analysis(locations_fc, "modal_lyr_" + modal_layer_name,
                                         os.path.join(scenario_gdb, "tmp_near"),
                                         max_artificial_link_distance_miles, "LOCATION", "NO_ANGLE", "CLOSEST")

    else:
        logger.warning("The Advanced/ArcInfo license level of ArcGIS is not available. Modified generate_near process "
                       "will be run.")
        # Spatial Join
        # Workaround for GenerateNearTable not being available for lower-level ArcGIS licenses.

        if arcpy.Exists(os.path.join(scenario_gdb, "tmp_spatial_join")):
            arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_spatial_join"))

        # First, add field to capture joined FID
        arcpy.AddField_management("modal_lyr_" + modal_layer_name, "Join_FID", "LONG")
        arcpy.CalculateField_management("modal_lyr_" + modal_layer_name, "Join_FID", "!OBJECTID!",
                                        "PYTHON_9.3")

        arcpy.SpatialJoin_analysis(locations_fc, "modal_lyr_" + modal_layer_name,
                                   os.path.join(scenario_gdb, "tmp_spatial_join"),
                                   match_option="CLOSEST", search_radius=max_artificial_link_distance_miles)

        arcpy.DeleteField_management("modal_lyr_" + modal_layer_name, "Join_FID")

        # queryPointAndDistance on the original point and corresponding spatial join match
        # For line in spatial_join:
        result_dict = {}

        with arcpy.da.SearchCursor(os.path.join(scenario_gdb, "tmp_spatial_join"),
                                   ["Target_FID", "Join_FID", "SHAPE@"]) as scursor1:
            for row1 in scursor1:
                with arcpy.da.SearchCursor("modal_lyr_" + modal_layer_name,
                                           ["OBJECTID", "SHAPE@"]) as scursor2:
                    for row2 in scursor2:
                        if row1[1] == row2[0]:
                            if "pipeline" in modal_layer_name:
                                result = row2[1].angleAndDistanceTo(row1[2], "PLANAR")
                                # Capture the point geometry of the nearest point on the polyline to the location point
                                # and the minimum distance between the line and the point
                                # result_dict[in_fid] = [near_fid, from_xy, near_xy, near_dist
                                result_dict[row1[0]] = [row1[1], row1[2], row2[1], result[1]]
                            else:
                                result = row2[1].queryPointAndDistance(row1[2], False)
                                # Capture the point geometry of the nearest point on the polyline to the location point
                                # and the minimum distance between the line and the point
                                # result_dict[in_fid] = [near_fid, from_xy, near_xy, near_dist
                                result_dict[row1[0]] = [row1[1], row1[2], result[0], result[2]]

        # Write to a tmp_near table equivalent to what is create by Generate Near Table tool
        arcpy.CreateTable_management(scenario_gdb, "tmp_near")
        arcpy.AddField_management("tmp_near", "IN_FID", "LONG")
        arcpy.AddField_management("tmp_near", "NEAR_FID", "LONG")
        arcpy.AddField_management("tmp_near", "NEAR_DIST", "LONG")
        arcpy.AddField_management("tmp_near", "FROM_X", "DOUBLE")
        arcpy.AddField_management("tmp_near", "FROM_Y", "DOUBLE")
        arcpy.AddField_management("tmp_near", "NEAR_X", "DOUBLE")
        arcpy.AddField_management("tmp_near", "NEAR_Y", "DOUBLE")

        # insert the relevant data into the table
        icursor = arcpy.da.InsertCursor("tmp_near", ['IN_FID', 'NEAR_FID', 'NEAR_DIST', 'FROM_X', 'FROM_Y',
                                        'NEAR_X', 'NEAR_Y'])

        for in_fid in result_dict:
            near_fid = result_dict[in_fid][0]
            near_distance = result_dict[in_fid][3]
            from_x = result_dict[in_fid][1].firstPoint.X
            from_y = result_dict[in_fid][1].firstPoint.Y
            near_x = result_dict[in_fid][2].firstPoint.X
            near_y = result_dict[in_fid][2].firstPoint.Y
            icursor.insertRow([in_fid, near_fid, near_distance, from_x, from_y, near_x, near_y])

        del icursor

    arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_spatial_join"))

    edit = arcpy.da.Editor(os.path.join(scenario_gdb))
    edit.startEditing(False, False)
    edit.startOperation()

    id_fieldname = arcpy.Describe(os.path.join(scenario_gdb, modal_layer_name)).OIDFieldName

    seenids = {}

    # SPLIT LINKS LOGIC
    # 1) first search through the tmp_near fc and add points from the near on that link.
    # 2) next we query the mode layer and get the mode specific data using the near FID.
    # 3) then we split the old link, and use insert cursor to populate mode specific data into fc for the two new links.
    # 4) then we delete the old unsplit link
    logger.debug("start:  split links")

    if arcpy.CheckProduct("ArcInfo") != "Available":
        # Adding warning here rather than within the search cursor loop
        logger.warning(
            "The Advanced/ArcInfo license level of ArcGIS is not available. Modified split links process "
            "will be run.")

    with arcpy.da.SearchCursor(os.path.join(scenario_gdb, "tmp_near"),
                               ["NEAR_FID", "NEAR_X", "NEAR_Y", "NEAR_DIST"]) as scursor:

        for row in scursor:

            # if the near distance is 0, then its connected and we don't need to split the line
            if row[3] == 0:
                # only give debug warning if not pipeline
                if "pipeline" not in modal_layer_name:
                    logger.warning(
                        "Split links code: LOCATION MIGHT BE ON THE NETWORK. Ignoring NEAR_FID {} with NEAR_DIST {}".format(
                            row[0], row[3]))

            if not row[3] == 0:

                # STEP 1: point geoms where to split from the near XY
                # ---------------------------------------------------
                # get the line ID to split
                theIdToGet = str(row[0])  # this is the link id we need

                if not theIdToGet in seenids:
                    seenids[theIdToGet] = []

                point = arcpy.Point()
                point.X = float(row[1])
                point.Y = float(row[2])
                point_geom = arcpy.PointGeometry(point, ftot_supporting_gis.LCC_PROJ)
                seenids[theIdToGet].append(point_geom)

        # STEP 2 -- get mode specific data from the link
        # ------------------------------------------------
        if 'pipeline' not in modal_layer_name:

            for theIdToGet in seenids:

                # initialize the variables so we dont get any gremlins
                in_line = None  # the shape geometry
                in_capacity = None  # road + rail
                in_volume = None  # road + rail
                in_vcr = None  # road + rail | volume to capacity ratio
                in_fclass = None  # road | fclass
                in_speed = None  # road | rounded speed
                in_stracnet = None  # rail
                in_density_code = None  # rail
                in_tot_up_dwn = None  # water

                if modal_layer_name == 'road':
                    for row in arcpy.da.SearchCursor(os.path.join(scenario_gdb, modal_layer_name),
                                                     ["SHAPE@", "Capacity", "Volume", "VCR", "FCLASS", "ROUNDED_SPEED"],
                                                     where_clause=id_fieldname + " = " + theIdToGet):
                        in_line = row[0]
                        in_capacity = row[1]
                        in_volume = row[2]
                        in_vcr = row[3]
                        in_fclass = row[4]
                        in_speed = row[5]

                if modal_layer_name == 'rail':
                    for row in arcpy.da.SearchCursor(os.path.join(scenario_gdb, modal_layer_name),
                                                     ["SHAPE@", "Capacity", "Volume", "VCR", "STRACNET",
                                                      "DENSITY_CODE"], where_clause=id_fieldname + " = " + theIdToGet):
                        in_line = row[0]
                        in_capacity = row[1]
                        in_volume = row[2]
                        in_vcr = row[3]
                        in_stracnet = row[4]
                        in_density_code = row[5]

                if modal_layer_name == 'water':
                    for row in arcpy.da.SearchCursor(os.path.join(scenario_gdb, modal_layer_name),
                                                     ["SHAPE@", "Capacity", "Volume", "VCR", "TOT_UP_DWN"],
                                                     where_clause=id_fieldname + " = " + theIdToGet):
                        in_line = row[0]
                        in_capacity = row[1]
                        in_volume = row[2]
                        in_vcr = row[3]
                        in_tot_up_dwn = row[4]

                # STEP 3: Split and populate with mode specific data from old link
                # ------------------------------------------------------------------
                if arcpy.CheckProduct("ArcInfo") == "Available":
                    split_lines = arcpy.management.SplitLineAtPoint(in_line, seenids[theIdToGet], arcpy.Geometry(), 1)

                else:
                    # This is the alternative approach for those without an Advanced/ArcInfo license
                    point_list = seenids[theIdToGet]
                    line_list = [in_line]
                    split_lines = []
                    continue_iteration = 'continue running'

                    while continue_iteration == 'continue running':
                        line_list, point_list, split_lines, continue_iteration = cut_lines(line_list, point_list, split_lines)

                if not len(split_lines) == 1:

                    # ROAD
                    if modal_layer_name == 'road':

                        icursor = arcpy.da.InsertCursor(os.path.join(scenario_gdb, modal_layer_name),
                                                        ['SHAPE@', 'Artificial', 'MODE_TYPE', 'MILES', 'FCLASS',
                                                         'ROUNDED_SPEED', 'Volume', 'Capacity', 'VCR'])

                        # Insert new links that include the mode-specific attributes
                        for new_line in split_lines:
                            len_in_miles = Q_(new_line.length, "meters").to("miles").magnitude
                            icursor.insertRow(
                                [new_line, 0, modal_layer_name, len_in_miles, in_fclass, in_speed, in_volume,
                                 in_capacity, in_vcr])

                        # Delete cursor object
                        del icursor

                    elif modal_layer_name == 'rail':
                        icursor = arcpy.da.InsertCursor(os.path.join(scenario_gdb, modal_layer_name),
                                                        ['SHAPE@', 'Artificial', 'MODE_TYPE', 'MILES', 'STRACNET',
                                                         'DENSITY_CODE', 'Volume', 'Capacity', 'VCR'])

                        # Insert new rows that include the mode-specific attributes
                        for new_line in split_lines:
                            len_in_miles = Q_(new_line.length, "meters").to("miles").magnitude
                            icursor.insertRow(
                                [new_line, 0, modal_layer_name, len_in_miles, in_stracnet, in_density_code, in_volume,
                                 in_capacity, in_vcr])

                        # Delete cursor object
                        del icursor

                    elif modal_layer_name == 'water':

                        icursor = arcpy.da.InsertCursor(os.path.join(scenario_gdb, modal_layer_name),
                                                        ['SHAPE@', 'Artificial', 'MODE_TYPE', 'MILES', 'TOT_UP_DWN',
                                                         'Volume', 'Capacity', 'VCR'])

                        # Insert new rows that include the mode-specific attributes
                        for new_line in split_lines:
                            len_in_miles = Q_(new_line.length, "meters").to("miles").magnitude
                            icursor.insertRow(
                                [new_line, 0, modal_layer_name, len_in_miles, in_tot_up_dwn, in_volume, in_capacity,
                                 in_vcr])

                        # Delete cursor object
                        del icursor

                    else:
                        logger.warning("Modal_layer_name: {} is not supported.".format(modal_layer_name))

                    # STEP 4:  Delete old unsplit data
                    with arcpy.da.UpdateCursor(os.path.join(scenario_gdb, modal_layer_name), ['OID@'],
                                               where_clause=id_fieldname + " = " + theIdToGet) as ucursor:
                        for row in ucursor:
                            ucursor.deleteRow()

                # if the split doesn't work
                else:
                    logger.detailed_debug(
                        "the line split didn't work for ID: {}. "
                        "Might want to investigate. "
                        "Could just be an artifact from the near result being the end of a line.".format(
                            theIdToGet))

    edit.stopOperation()
    edit.stopEditing(True)

    # delete the old features
    # ------------------------
    logger.debug("start:  delete old features (tmp_near, tmp_near_2, tmp_nodes)")
    if arcpy.Exists(os.path.join(scenario_gdb, "tmp_near")):
        arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_near"))

    if arcpy.Exists(os.path.join(scenario_gdb, "tmp_near_2")):
        arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_near_2"))

    if arcpy.Exists(os.path.join(scenario_gdb, "tmp_nodes")):
        arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_nodes"))

    # add artificial links
    # now that the lines have been split add lines from the from points to the nearest node
    # --------------------------------------------------------------------------------------
    logger.debug("start:  add artificial links now w/ definition_query: {}".format(definition_query))
    logger.debug("start:  make_featurelayer 2")
    fp_to_modal_layer = os.path.join(scenario_gdb, "network", modal_layer_name)
    arcpy.MakeFeatureLayer_management(fp_to_modal_layer, "modal_lyr_" + modal_layer_name + "2", definition_query)
    logger.debug("start:  feature vertices to points 2")
    if arcpy.CheckProduct("ArcInfo") == "Available":
        arcpy.FeatureVerticesToPoints_management(in_features="modal_lyr_" + modal_layer_name + "2",
                                                 out_feature_class=os.path.join(scenario_gdb, "tmp_nodes"),
                                                 point_location="BOTH_ENDS")
    else:
        logger.warning("The Advanced/ArcInfo license level of ArcGIS is not available. Modified feature vertices "
                       "process will be run.")
        arcpy.AddGeometryAttributes_management("modal_lyr_" + modal_layer_name + "2", "LINE_START_MID_END")
        arcpy.MakeXYEventLayer_management("modal_lyr_" + modal_layer_name + "2", "START_X", "START_Y",
                                          "modal_start_points_lyr", LCC_PROJ)

        arcpy.MakeXYEventLayer_management("modal_lyr_" + modal_layer_name + "2", "END_X", "END_Y",
                                          "modal_end_points_lyr", LCC_PROJ)

        arcpy.FeatureClassToFeatureClass_conversion("modal_start_points_lyr",
                                                    scenario_gdb,
                                                    "tmp_nodes")

        arcpy.Append_management(["modal_end_points_lyr"],
                                "tmp_nodes", "NO_TEST")

        arcpy.Delete_management("modal_start_points_lyr")
        arcpy.Delete_management("modal_end_points_lyr")
        arcpy.DeleteField_management(fp_to_modal_layer, "START_X")
        arcpy.DeleteField_management(fp_to_modal_layer, "START_Y")
        arcpy.DeleteField_management(fp_to_modal_layer, "MID_X")
        arcpy.DeleteField_management(fp_to_modal_layer, "MID_Y")
        arcpy.DeleteField_management(fp_to_modal_layer, "END_X")
        arcpy.DeleteField_management(fp_to_modal_layer, "END_Y")

    logger.debug("start:  generate near table 2")
    if arcpy.CheckProduct("ArcInfo") == "Available":
        arcpy.GenerateNearTable_analysis(locations_fc, os.path.join(scenario_gdb, "tmp_nodes"),
                                         os.path.join(scenario_gdb, "tmp_near_2"),
                                         max_artificial_link_distance_miles, "LOCATION", "NO_ANGLE", "CLOSEST")

    else:
        logger.warning("The Advanced/ArcInfo license level of ArcGIS is not available. Modified generate_near process "
                       "will be run.")
        # Spatial Join
        # Workaround for GenerateNearTable not being available for lower-level ArcGIS licenses

        if arcpy.Exists(os.path.join(scenario_gdb, "tmp_spatial_join_2")):
            arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_spatial_join_2"))

        # First, add field to capture joined FID
        arcpy.AddField_management("tmp_nodes", "Join_FID", "LONG")
        arcpy.CalculateField_management("tmp_nodes", "Join_FID", "!OBJECTID!", "PYTHON_9.3")

        arcpy.SpatialJoin_analysis(locations_fc, "tmp_nodes",
                                   os.path.join(scenario_gdb, "tmp_spatial_join_2"),
                                   match_option="CLOSEST", search_radius=max_artificial_link_distance_miles)

        # queryPointAndDistance on the original point and corresponding spatial join match
        # For line in spatial_join:
        result_dict = {}

        with arcpy.da.SearchCursor(os.path.join(scenario_gdb, "tmp_spatial_join_2"),
                                   ["Target_FID", "Join_FID", "SHAPE@"]) as scursor1:
            for row1 in scursor1:
                with arcpy.da.SearchCursor("tmp_nodes",
                                           ["OBJECTID", "SHAPE@"]) as scursor2:
                    for row2 in scursor2:
                        if row1[1] == row2[0]:
                            result = row2[1].angleAndDistanceTo(row1[2], "PLANAR")
                            # Capture the point geometry of the nearest point on the polyline to the location point
                            # and the minimum distance between the line and the point
                            # result_dict[in_fid] = [near_fid, from_xy, near_xy, near_dist
                            result_dict[row1[0]] = [row1[1], row1[2], row2[1], result[1]]

        # Write to a tmp_near table equivalent to what is create by Generate Near Table tool
        arcpy.CreateTable_management(scenario_gdb, "tmp_near_2")
        arcpy.AddField_management("tmp_near_2", "IN_FID", "LONG")
        arcpy.AddField_management("tmp_near_2", "NEAR_FID", "LONG")
        arcpy.AddField_management("tmp_near_2", "NEAR_DIST", "LONG")
        arcpy.AddField_management("tmp_near_2", "FROM_X", "DOUBLE")
        arcpy.AddField_management("tmp_near_2", "FROM_Y", "DOUBLE")
        arcpy.AddField_management("tmp_near_2", "NEAR_X", "DOUBLE")
        arcpy.AddField_management("tmp_near_2", "NEAR_Y", "DOUBLE")

        # insert the relevant data into the table
        icursor = arcpy.da.InsertCursor("tmp_near_2", ['IN_FID', 'NEAR_FID', 'NEAR_DIST', 'FROM_X', 'FROM_Y',
                                                       'NEAR_X', 'NEAR_Y'])

        for in_fid in result_dict:
            near_fid = result_dict[in_fid][0]
            near_distance = result_dict[in_fid][3]
            from_x = result_dict[in_fid][1].firstPoint.X
            from_y = result_dict[in_fid][1].firstPoint.Y
            near_x = result_dict[in_fid][2].firstPoint.X
            near_y = result_dict[in_fid][2].firstPoint.Y
            icursor.insertRow([in_fid, near_fid, near_distance, from_x, from_y, near_x, near_y])

        del icursor

    arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_spatial_join_2"))

    logger.debug("start:  delete tmp_nodes")
    arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_nodes"))

    logger.debug("start:  start editor")
    edit = arcpy.da.Editor(os.path.join(scenario_gdb))
    edit.startEditing(False, False)
    edit.startOperation()

    icursor = arcpy.da.InsertCursor(os.path.join(scenario_gdb, modal_layer_name),
                                    ['SHAPE@', 'Artificial', 'MODE_TYPE', 'MILES', 'LOCATION_ID',
                                     'LOCATION_ID_NAME'])  # add location_id for setting flow restrictions

    location_id_name_dict = get_location_id_name_dict(the_scenario, logger)
    connected_location_ids = []
    connected_location_id_names = []
    logger.debug("start:  search cursor on tmp_near_2")
    with arcpy.da.SearchCursor(os.path.join(scenario_gdb, "tmp_near_2"),
                               ["FROM_X", "FROM_Y", "NEAR_X", "NEAR_Y", "NEAR_DIST", "IN_FID"]) as scursor:

        for row in scursor:

            if not row[4] == 0:

                # use the unique objectid (in_fid) from the near to determine
                # if we have an in or an out location.
                # then set the flow restrictions appropriately.

                in_fid = row[5]
                location_id_name = location_id_name_dict[in_fid]
                location_id = location_id_name.split("_")[0]
                connected_location_ids.append(location_id)
                connected_location_id_names.append(location_id_name)

                coordList = []
                coordList.append(arcpy.Point(row[0], row[1]))
                coordList.append(arcpy.Point(row[2], row[3]))
                polyline = arcpy.Polyline(arcpy.Array(coordList))

                len_in_miles = Q_(polyline.length, "meters").to("miles").magnitude

                # insert artificial link attributes
                icursor.insertRow([polyline, 1, modal_layer_name, len_in_miles, location_id, location_id_name])

            else:
                logger.warning("Artificial Link code: Ignoring NEAR_FID {} with NEAR_DIST {}".format(row[0], row[4]))

    del icursor
    logger.debug("start:  stop editing")
    edit.stopOperation()
    edit.stopEditing(True)

    arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_near_2"))

    # ALSO SET CONNECTS_X FIELD IN POINT LAYER
    # -----------------------------------------
    logger.debug("start:  connect_x")

    edit = arcpy.da.Editor(scenario_gdb)
    edit.startEditing(False, False)
    edit.startOperation()
    with arcpy.da.UpdateCursor(os.path.join(scenario_gdb, locations_fc),
                               ["LOCATION_ID_NAME", "connects_" + modal_layer_name]) as cursor:

        for row in cursor:

            if row[0] in connected_location_id_names:
                row[1] = 1
                cursor.updateRow(row)

    edit.stopOperation()
    edit.stopEditing(True)

    logger.debug("finish: locations_add_links")


# ==============================================================================


def ignore_locations_not_connected_to_network(the_scenario, logger):
    logger.info("start: ignore_locations_not_connected_to_network")
    logger.debug("flag locations which don't connect to the network")

    # flag locations which dont connect to the network in the GIS
    # -----------------------------------------------------------

    # add the ignore field to the fc
    scenario_gdb = the_scenario.main_gdb
    locations_fc = the_scenario.locations_fc

    edit = arcpy.da.Editor(scenario_gdb)
    edit.startEditing(False, False)
    edit.startOperation()

    query = "connects_road = 0 and " \
            "connects_rail = 0 and " \
            "connects_water = 0 and " \
            "connects_pipeline_prod_trf_rts = 0 and " \
            "connects_pipeline_crude_trf_rts = 0"

    list_of_ignored_locations = []
    with arcpy.da.UpdateCursor(locations_fc, ['location_id', 'ignore'], where_clause=query) as ucursor:
        for row in ucursor:
            list_of_ignored_locations.append(row[0])
            logger.debug('Location ID {} does not connect to the network and will be ignored'.format(row[0]))
            row[1] = 1
            ucursor.updateRow(row)

    if len(list_of_ignored_locations) > 0:

        logger.result("# of locations not connected to network and ignored: \t{}".format(len(
            list_of_ignored_locations)/2.0))
        logger.info("note: check the log files for additional debug information.")

    edit.stopOperation()
    edit.stopEditing(True)

    # flag locations which dont connect to the network in the DB
    # -----------------------------------------------------------
    scenario_db = the_scenario.main_db

    if os.path.exists(scenario_db):

        with sqlite3.connect(scenario_db) as db_con:

            logger.debug("connected to the db")

            db_cur = db_con.cursor()

            # iterate through the locations table and set ignore flag
            # if location_id is in the list_of_ignored_facilities
            # ------------------------------------------------------------

            logger.debug("setting ignore fields for location in the DB locations and facilities tables")

            sql = "select location_id, ignore_location from locations;"

            db_cur.execute(sql)
            for row in db_cur:

                if str(row[0]) in list_of_ignored_locations:  # cast as string since location_id field is a string
                    ignore_flag = 'network'
                else:
                    ignore_flag = 'false'
                sql = "update locations set ignore_location = '{}' where location_id = '{}';".format(ignore_flag,
                                                                                                     row[0])
                db_con.execute(sql)

                # iterate through the facilities table,
                # and check if location_id is in the list_of_ignored_facilities
                # ------------------------------------------------------------
                sql = "update facilities set ignore_facility = '{}' where location_id = '{}';".format(ignore_flag,
                                                                                                      row[0])

                db_con.execute(sql)

    logger.debug("finished: ignore_locations_not_connected_to_network")
    return 1

# ======================================================================================================================

# this code subsets the road network so that only links within the minimum bounding geometry (MBG) buffer are
# preserved. This method ends with the arcpy.Compact_management() call that reduces the size of the geodatabase and
# increases overall performance when hooking facilities into the network, and exporting the road fc to a shapefile
# for networkX.


def minimum_bounding_geometry(the_scenario, logger):
    logger.info("start: minimum_bounding_geometry")
    arcpy.env.workspace = the_scenario.main_gdb

    # Clean up any left of layers from a previous run
    if arcpy.Exists("road_lyr"):
        arcpy.Delete_management("road_lyr")
    if arcpy.Exists("rail_lyr"):
        arcpy.Delete_management("rail_lyr")
    if arcpy.Exists("water_lyr"):
        arcpy.Delete_management("water_lyr")
    if arcpy.Exists("pipeline_prod_trf_rts_lyr"):
        arcpy.Delete_management("pipeline_prod_trf_rts_lyr")
    if arcpy.Exists("pipeline_crude_trf_rts_lyr"):
        arcpy.Delete_management("pipeline_crude_trf_rts_lyr")
    if arcpy.Exists("Locations_MBG"):
        arcpy.Delete_management("Locations_MBG")
    if arcpy.Exists("Locations_MBG_Buffered"):
        arcpy.Delete_management("Locations_MBG_Buffered")

    # Determine the minimum bounding geometry of the scenario
    # The advanced license is required to use the convex hull method. If not available, default to rectangle_by_area
    # which will not subset things quite as small but is still better than no subsetting at all
    if arcpy.CheckProduct("ArcInfo") == "Available":
        arcpy.MinimumBoundingGeometry_management("Locations", "Locations_MBG", "CONVEX_HULL")

    else:
        arcpy.MinimumBoundingGeometry_management("Locations", "Locations_MBG", "RECTANGLE_BY_AREA")
        logger.warning("The Advanced/ArcInfo license level of ArcGIS is not available. A slight modification to "
                       "the minimum bounding geometry process is necessary to ensure FTOT can successfully run.")

    # Buffer the minimum bounding geometry of the scenario
    arcpy.Buffer_analysis("Locations_MBG", "Locations_MBG_Buffered", "100 Miles", "FULL", "ROUND", "NONE", "",
                          "GEODESIC")

    # Select the roads within the buffer
    # -----------------------------------
    arcpy.MakeFeatureLayer_management("road", "road_lyr")
    arcpy.SelectLayerByLocation_management("road_lyr", "INTERSECT", "Locations_MBG_Buffered")

    result = arcpy.GetCount_management("road")
    count_all_roads = float(result.getOutput(0))

    result = arcpy.GetCount_management("road_lyr")
    count_roads_subset = float(result.getOutput(0))

    ## CHANGE
    if count_all_roads > 0:
        roads_percentage = count_roads_subset / count_all_roads
    else:
        roads_percentage = 0

    # Only subset if the subset will result in substantial reduction of the road network size
    if roads_percentage < 0.75:
        # Switch selection to identify what's outside the buffer
        arcpy.SelectLayerByAttribute_management("road_lyr", "SWITCH_SELECTION")

        # Add in FC 1 roadways (going to keep all interstate highways)
        arcpy.SelectLayerByAttribute_management("road_lyr", "REMOVE_FROM_SELECTION", "FCLASS = 1")

        # Delete the features outside the buffer
        with arcpy.da.UpdateCursor('road_lyr', ['OBJECTID']) as ucursor:
            for ucursor_row in ucursor:
                ucursor.deleteRow()

        arcpy.Delete_management("road_lyr")

        # # Select the rail within the buffer
        # # ---------------------------------

    arcpy.Delete_management("Locations_MBG")
    arcpy.Delete_management("Locations_MBG_Buffered")

    # finally, compact the geodatabase so the MBG has an effect on runtime.
    arcpy.Compact_management(the_scenario.main_gdb)
    logger.debug("finish: minimum_bounding_geometry")

