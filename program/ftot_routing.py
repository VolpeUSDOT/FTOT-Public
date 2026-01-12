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


from pint import UnitRegistry
ureg = UnitRegistry()

# ========================================================================


def connectivity(the_scenario, logger):

        checks_and_cleanup(the_scenario, logger)

        # create the locations_fc
        create_locations_fc(the_scenario, logger)

        # use MBG to subset the road network to a buffer around the locations FC
        minimum_bounding_geometry(the_scenario, logger)

        # set up vehicle types and commodity mode table in db
        from ftot_networkx import vehicle_type_setup
        from ftot_networkx import commodity_mode_setup
        vehicle_type_setup(the_scenario, logger)
        commodity_mode_setup(the_scenario, logger) # needed to check permitted modes in next step

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
    co_location_offset = 0.1
    logger.debug("co-location offset is necessary to prevent the locations from being treated as intermodal "
                 "facilities.")
    logger.debug("collocation off-set: {} meters".format(co_location_offset))

    locations_fc = the_scenario.locations_fc

    # delete the old location FC before we create it
    from ftot_facilities import gis_clear_feature_class
    gis_clear_feature_class(locations_fc, logger)

    # create the feature class
    scenario_proj = ftot_supporting_gis.get_coordinate_system(the_scenario)
    arcpy.CreateFeatureclass_management(the_scenario.main_gdb, "locations", "POINT", "#", "DISABLED", "DISABLED",
                                        scenario_proj)

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
                location_point.X = row[1] + co_location_offset
                location_point.Y = row[2] + co_location_offset
                location_point_geom = arcpy.PointGeometry(location_point, scenario_proj)

                insert_cursor.insertRow([str(location_id) + "_OUT", location_id, location_point_geom])

                # create a point for each location "in"
                location_point = arcpy.Point()
                location_point.X = row[1] - co_location_offset
                location_point.Y = row[2] - co_location_offset
                location_point_geom = arcpy.PointGeometry(location_point, scenario_proj)

                insert_cursor.insertRow([str(location_id) + "_IN", location_id, location_point_geom])

    edit.stopOperation()
    edit.stopEditing(True)

    loop_counter = 0
    flag_list = ['placeholder'] # Have one item in it to begin with so it enters the loop
    while len(flag_list) > 0:
        loop_counter += 1
        flag_list = [] # empty the list so if on first pass none are too close, it exits

        for mode in the_scenario.permittedModes:
            if arcpy.Exists(os.path.join(the_scenario.main_gdb, "tmp_{}_near".format(mode))):
                arcpy.Delete_management(os.path.join(the_scenario.main_gdb, "tmp_{}_near".format(mode)))

            arcpy.GenerateNearTable_analysis(locations_fc, os.path.join(the_scenario.base_network_gdb, 'network', mode),
                                             os.path.join(the_scenario.main_gdb, "tmp_{}_near".format(mode)),
                                             '0.1 Meters', "LOCATION", "NO_ANGLE", "CLOSEST")

            # make list of objectIDs of features that are too close
            with arcpy.da.SearchCursor(os.path.join(the_scenario.main_gdb, "tmp_{}_near".format(mode)), ['IN_FID', 'NEAR_DIST']) as scursor:
                for row in scursor:
                    if row[1] < .1: # Or near 0, whatever the tolerance needs to be
                        flag_list.append(row[0])

        if len(flag_list) > 0: # if any locations are too close, move them
            logger.info('Iteration counter = {}'.format(loop_counter))
            logger.info('Facility locations too close to modal networks. List to move: {}'.format(flag_list))
            # Move the ones that are too close
            with arcpy.da.UpdateCursor(locations_fc, ['OBJECTID', 'SHAPE@']) as ucursor:
                for row in ucursor:
                    if row[0] in flag_list:
                        logger.debug('Facility OID: {}. Original location: X {}, Y {}. New location X {}, Y {}.'.format(row[0], row[1].centroid.X, row[1].centroid.Y, row[1].centroid.X + co_location_offset, row[1].centroid.Y - co_location_offset))
                        new_point = arcpy.Point(row[1].centroid.X + co_location_offset, row[1].centroid.Y - co_location_offset)
                        new_geometry = arcpy.PointGeometry(new_point, row[1].spatialReference)
                        row[1] = new_geometry            

                        ucursor.updateRow(row)
        
        if loop_counter == 25:
            error = "Code loop to move locations off the network has run 25 times, review locations_fc layer. Exiting."
            logger.error(error)
            raise Exception(error)

    # If there were any that were too close, this loop will now repeat and perform the check again
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
    for mode in the_scenario.permittedModes:
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


def cut_lines(line_list, point_list, split_lines, scenario_proj):
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
                             arcpy.Point(snap_point.X - 10.0, snap_point.Y - 10.0)]), scenario_proj))
                        if cut_line_1.length == 0 or cut_line_2.length == 0:
                            cut_line_1, cut_line_2 = line.cut(arcpy.Polyline(arcpy.Array(
                                [arcpy.Point(snap_point.X - 10.0, snap_point.Y + 10.0),
                                 arcpy.Point(snap_point.X + 10.0, snap_point.Y - 10.0)]), scenario_proj))
                        if cut_line_1.length == 0 or cut_line_2.length == 0:
                            cut_line_1, cut_line_2 = line.cut(arcpy.Polyline(arcpy.Array(
                                [arcpy.Point(snap_point.X + 10.0, snap_point.Y),
                                 arcpy.Point(snap_point.X - 10.0, snap_point.Y)]), scenario_proj))
                        if cut_line_1.length == 0 or cut_line_2.length == 0:
                            cut_line_1, cut_line_2 = line.cut(arcpy.Polyline(arcpy.Array(
                                [arcpy.Point(snap_point.X, snap_point.Y + 10.0),
                                 arcpy.Point(snap_point.X, snap_point.Y - 10.0)]), scenario_proj))
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
    # Temporarily convert to miles to prevent arcpy errors with other unit strings
    # ---------------------------
    road_max_artificial_link_distance_miles = str(the_scenario.road_max_artificial_link_dist.to(ureg.miles).magnitude) + " miles"
    rail_max_artificial_link_distance_miles = str(the_scenario.rail_max_artificial_link_dist.to(ureg.miles).magnitude) + " miles"
    water_max_artificial_link_distance_miles = str(the_scenario.water_max_artificial_link_dist.to(ureg.miles).magnitude) + " miles"
    pipeline_crude_max_artificial_link_distance_miles = str(the_scenario.pipeline_crude_max_artificial_link_dist.to(ureg.miles).magnitude) + " miles"
    pipeline_prod_max_artificial_link_distance_miles = str(the_scenario.pipeline_prod_max_artificial_link_dist.to(ureg.miles).magnitude) + " miles"

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
    for fc in the_scenario.permittedModes + ['locations', 'intermodal', 'locks']:
        if arcpy.Exists(os.path.join(scenario_gdb, 'network', fc)):
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
        
        if not arcpy.Exists(os.path.join(the_scenario.main_gdb, 'network', fc)):
            continue # if doesn't exist, move to next fc
        elif fc == 'pipeline_crude' and 'pipeline_crude_trf_rts' not in the_scenario.permittedModes:
            continue # if not permitted, move to next fc
        elif fc == 'pipeline_prod' and 'pipeline_prod_trf_rts' not in the_scenario.permittedModes:
            continue # if not permitted, move to next fc
 

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

        if not arcpy.Exists(os.path.join(the_scenario.main_gdb, 'network', fc)):
            continue # if doesn't exist, move to next fc
        elif 'pipeline_crude' in fc and 'pipeline_crude_trf_rts' not in the_scenario.permittedModes:
            continue # if not permitted, move to next fc
        elif 'pipeline_prod' in fc and 'pipeline_prod_trf_rts' not in the_scenario.permittedModes:
            continue # if not permitted, move to next fc

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

    logger.debug("start: execute many on list of len: {}".format(len(capacity_update_list)))

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
    arcpy.env.workspace = the_scenario.main_gdb
    fp_to_modal_layer = os.path.join(scenario_gdb, "network", modal_layer_name)
    scenario_proj = ftot_supporting_gis.get_coordinate_system(the_scenario)

    locations_fc = the_scenario.locations_fc
    arcpy.DeleteField_management(fp_to_modal_layer, "LOCATION_ID")
    arcpy.AddField_management(os.path.join(scenario_gdb, modal_layer_name), "LOCATION_ID", "long")

    arcpy.DeleteField_management(fp_to_modal_layer, "LOCATION_ID_NAME")
    arcpy.AddField_management(os.path.join(scenario_gdb, modal_layer_name), "LOCATION_ID_NAME", "text")

    # For flagging links that end up being split
    arcpy.DeleteField_management(fp_to_modal_layer, "SPLIT_LINK")
    arcpy.AddField_management(os.path.join(scenario_gdb, modal_layer_name), "SPLIT_LINK", "short")

    if float(max_artificial_link_distance_miles.strip(" miles")) < 0.0000001:
        logger.warning("Note: ignoring mode {}. User specified artificial link distance of {}".format(
            modal_layer_name, max_artificial_link_distance_miles))
        logger.debug("Setting the definition query to artificial = 99999, so we get an empty dataset for the "
                     "make_feature_layer and subsequent near analysis")

        definition_query = "Artificial = 999999"  # something to return an empty set
    else:
        definition_query = "Artificial = 0"  # the normal def query.

    if "pipeline" in modal_layer_name:

        if arcpy.Exists(os.path.join(scenario_gdb, "network", modal_layer_name + "_points")):
            arcpy.Delete_management(os.path.join(scenario_gdb, "network", modal_layer_name + "_points"))

        if arcpy.Exists(os.path.join(scenario_gdb, "network", modal_layer_name + "_points_dissolved")):
            arcpy.Delete_management(os.path.join(scenario_gdb, "network", modal_layer_name + "_points_dissolved"))

        # limit near to end points
        if arcpy.ProductInfo() == "ArcInfo":
            arcpy.FeatureVerticesToPoints_management(in_features=fp_to_modal_layer,
                                                     out_feature_class=modal_layer_name + "_points",
                                                     point_location="BOTH_ENDS")
        else:
            logger.warning("The Advanced/ArcInfo license level of ArcGIS Pro is not available. Modified feature "
                           "vertices process is being automatically run.")
            arcpy.AddGeometryAttributes_management(fp_to_modal_layer, "LINE_START_MID_END")
            arcpy.MakeXYEventLayer_management(fp_to_modal_layer, "START_X", "START_Y",
                                              "modal_start_points_lyr", scenario_proj)

            arcpy.MakeXYEventLayer_management(fp_to_modal_layer, "END_X", "END_Y",
                                              "modal_end_points_lyr", scenario_proj)

            # Due to tool design, must define the feature class location and name slightly differently (separating
            # scenario gdb from feature class name. fp_to_modal_layer is identical to scenario_gdb + "network"
            # + modal_layer_name
            arcpy.FeatureClassToFeatureClass_conversion("modal_start_points_lyr",
                                                        scenario_gdb,
                                                        os.path.join("network", modal_layer_name + "_points"))

            arcpy.Append_management(["modal_end_points_lyr"],
                                    modal_layer_name + "_points", "NO_TEST")

            arcpy.Delete_management("modal_start_points_lyr")
            arcpy.Delete_management("modal_end_points_lyr")
            arcpy.DeleteField_management(fp_to_modal_layer, "START_X")
            arcpy.DeleteField_management(fp_to_modal_layer, "START_Y")
            arcpy.DeleteField_management(fp_to_modal_layer, "MID_X")
            arcpy.DeleteField_management(fp_to_modal_layer, "MID_Y")
            arcpy.DeleteField_management(fp_to_modal_layer, "END_X")
            arcpy.DeleteField_management(fp_to_modal_layer, "END_Y")

        logger.debug("start:  make_feature_layer_management")
        arcpy.MakeFeatureLayer_management(modal_layer_name + "_points", "modal_lyr_tmp_" + modal_layer_name,
                                          definition_query)

        # Dissolve ensures that we don't create duplicate art links to tariffs that start/end at same points
        arcpy.Dissolve_management("modal_lyr_tmp_" + modal_layer_name,
                                  modal_layer_name + "_points_dissolved", "", "", "SINGLE_PART")

        arcpy.MakeFeatureLayer_management(modal_layer_name + "_points_dissolved",
                                          "modal_lyr_" + modal_layer_name)

    else:
        logger.debug("start:  make_feature_layer_management")
        arcpy.MakeFeatureLayer_management(fp_to_modal_layer, "modal_lyr_" + modal_layer_name,
                                          definition_query)

    logger.debug("adding links between locations_fc and mode {} with max dist of {}".format(modal_layer_name,
                                                                                            Q_(max_artificial_link_distance_miles).to(the_scenario.default_units_distance)))

    if arcpy.Exists(os.path.join(scenario_gdb, "tmp_near")):
        logger.debug("start:  delete tmp near")
        arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_near"))

    if arcpy.Exists(os.path.join(scenario_gdb, "tmp_near_limited_access_fallback")):
        logger.debug("start:  delete tmp near limited access")
        arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_near_limited_access_fallback"))

    logger.debug("start:  generate_near")

    if modal_layer_name == 'road':

        # In order to prioritize hooking into non-limited access roads, do not included limited access roads in the first near
        arcpy.SelectLayerByAttribute_management("modal_lyr_" + modal_layer_name, "NEW_SELECTION", "Artificial = 0 and (Limited_Access = 0 or Limited_Access IS NULL or Limited_Access = -9999)")
        arcpy.GenerateNearTable_analysis(locations_fc, "modal_lyr_" + modal_layer_name,
                                         os.path.join(scenario_gdb, "tmp_near"),
                                         max_artificial_link_distance_miles, "LOCATION", "NO_ANGLE", "CLOSEST")

        # Also running separate near to only limited access roadways so that facilities can hook into them as a last resort
        arcpy.SelectLayerByAttribute_management("modal_lyr_" + modal_layer_name, "NEW_SELECTION", "Artificial = 0 and Limited_Access = 1")
        arcpy.GenerateNearTable_analysis(locations_fc, "modal_lyr_" + modal_layer_name,
                                         os.path.join(scenario_gdb, "tmp_near_limited_access_fallback"),
                                         max_artificial_link_distance_miles, "LOCATION", "NO_ANGLE", "CLOSEST")

    else:
        arcpy.GenerateNearTable_analysis(locations_fc, "modal_lyr_" + modal_layer_name,
                                         os.path.join(scenario_gdb, "tmp_near"),
                                         max_artificial_link_distance_miles, "LOCATION", "NO_ANGLE", "CLOSEST")

    edit = arcpy.da.Editor(os.path.join(scenario_gdb))
    edit.startEditing(False, False)
    edit.startOperation()

    id_fieldname = arcpy.Describe(os.path.join(scenario_gdb, modal_layer_name)).OIDFieldName

    seenids = {}
    neared_facilities = {}

    # SPLIT LINKS LOGIC
    # 1) first search through the tmp_near (or tmp_near_limited_access_fallback) fc and add points from the near on that link.
    # 2) next we query the mode layer and get the mode specific data using the near FID.
    # 3) then we split the old link, and use insert cursor to populate mode specific data into fc for the two new links.
    # 4) then we delete the old unsplit link.
    logger.debug("start:  split links")
    if arcpy.ProductInfo() != "ArcInfo":
        # Adding warning here rather than within the search cursor loop
        logger.warning(
            "The Advanced/ArcInfo license level of ArcGIS Pro is not available. Modified split links process "
            "is being automatically run.")

    # Below allows road process to hook into non-limited access roads first, and then limited access roads as fallback
    if modal_layer_name == 'road':
        near_fc_list = ["tmp_near", "tmp_near_limited_access_fallback"]
    else:
        near_fc_list = ["tmp_near"]

    for near_fc in near_fc_list:

        count = 0
        with arcpy.da.SearchCursor(os.path.join(scenario_gdb, near_fc),
                                   ["NEAR_FID", "NEAR_X", "NEAR_Y", "NEAR_DIST", "IN_FID"]) as scursor:

            for row in scursor:

                if row[4] not in neared_facilities:
                    neared_facilities[row[4]] = True
                    count += 1
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

                        if theIdToGet not in seenids:
                            seenids[theIdToGet] = []

                        point = arcpy.Point()
                        point.X = float(row[1])
                        point.Y = float(row[2])
                        point_geom = arcpy.PointGeometry(point, scenario_proj)
                        seenids[theIdToGet].append(point_geom)
        if modal_layer_name == 'road':
            if near_fc == 'tmp_near':
                logger.info("{} facilities hooking into the non-highway (or full, if limited access not defined) road network".format(count//2))
            elif near_fc == 'tmp_near_limited_access_fallback':
                logger.info("{} facilities hooking into the limited access road network because a non-limited access road is not within the artificial link distance".format(count//2))
        else:
            logger.info("{} facilities hooking into the {} network".format(count//2, modal_layer_name))

    # STEP 2 -- get mode specific data from the link
    # ----------------------------------------------
    if 'pipeline' not in modal_layer_name:

        for theIdToGet in seenids:

            # Get field objects from source FC
            dsc = arcpy.Describe(os.path.join(scenario_gdb, modal_layer_name))
            fields = dsc.fields

            # List all field names except the OID field and geometry fields
            # Replace 'SHAPE' with 'SHAPE@'
            out_fields = [dsc.OIDFieldName, dsc.lengthFieldName, dsc.areaFieldName]
            fieldnames = [field.name if field.name.lower() != 'shape' else 'SHAPE@' for field in fields if field.name not in out_fields]
            # Make sure SHAPE@ is in front so we specifically know where it is
            fieldnames.insert(0, fieldnames.pop(fieldnames.index('SHAPE@')))
            fieldnames.insert(1, fieldnames.pop(fieldnames.index('Length')))
            fieldnames.insert(2, fieldnames.pop(fieldnames.index('SPLIT_LINK')))

            # Create cursors and insert new rows

            for search_row in arcpy.da.SearchCursor(os.path.join(scenario_gdb, modal_layer_name),
                                                    [fieldnames],
                                                    where_clause=id_fieldname + " = " + theIdToGet):
                in_line = search_row[0]

            # STEP 3: Split and populate with mode specific data from old link
            # ----------------------------------------------------------------
            if arcpy.ProductInfo() == "ArcInfo":
                split_lines = arcpy.management.SplitLineAtPoint(in_line, seenids[theIdToGet], arcpy.Geometry(), 1)

            else:
                # This is the alternative approach for those without an Advanced/ArcInfo license
                point_list = seenids[theIdToGet]
                line_list = [in_line]
                split_lines = []
                continue_iteration = 'continue running'

                while continue_iteration == 'continue running':
                    line_list, point_list, split_lines, continue_iteration = cut_lines(line_list, point_list, split_lines, scenario_proj)

            if not len(split_lines) == 1:

                if modal_layer_name in ['road', 'rail', 'water']:

                    icursor = arcpy.da.InsertCursor(os.path.join(scenario_gdb, modal_layer_name),
                                                    fieldnames)

                    # Insert new links that include the mode-specific attributes
                    for new_line in split_lines:
                        len_in_default_units = Q_(new_line.length, "meters").to(the_scenario.default_units_distance).magnitude
                        new_line_values = [new_line, len_in_default_units, 1]
                        for x in range(len(fieldnames)):
                            # First three values (0, 1 and 2) need to NOT be inherited
                            if x > 2:
                                new_line_values.append(search_row[x])

                        icursor.insertRow(
                            new_line_values)

                    # Delete cursor object
                    del icursor

                else:
                    logger.warning("Modal_layer_name: {} is not supported.".format(modal_layer_name))

                # STEP 4:  Delete old unsplit data
                # --------------------------------
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
    logger.debug("start:  delete old features (tmp_near, tmp_near_limited_access_fallback)")
    if arcpy.Exists(os.path.join(scenario_gdb, "tmp_near")):
        arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_near"))

    if arcpy.Exists(os.path.join(scenario_gdb, "tmp_near_limited_access_fallback")):
        arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_near_limited_access_fallback"))

    if arcpy.Exists(os.path.join(scenario_gdb, "tmp_nodes")):
        arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_nodes"))

    if arcpy.Exists(os.path.join(scenario_gdb, "tmp_near_2")):
        arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_near_2"))

    # add artificial links
    # now that the lines have been split add lines from the from points to the nearest node
    # --------------------------------------------------------------------------------------
    logger.debug("start:  add artificial links now w/ definition_query: {}".format(definition_query))
    logger.debug("start:  make_featurelayer 2")
    fp_to_modal_layer = os.path.join(scenario_gdb, "network", modal_layer_name)
    arcpy.MakeFeatureLayer_management(fp_to_modal_layer, "modal_lyr_" + modal_layer_name + "2", definition_query)
    logger.debug("start:  make fc from artificial nodes")
    arcpy.management.CreateFeatureclass(scenario_gdb, "tmp_nodes", "POINT",
                                        spatial_reference=fp_to_modal_layer)
    with arcpy.da.InsertCursor("tmp_nodes", ["SHAPE@XY"]) as cursor:
        for k, point_geom_list in seenids.items():
            for point_geom in point_geom_list:
                cursor.insertRow([point_geom])

    logger.debug("start:  generate near table 2")
    arcpy.GenerateNearTable_analysis(locations_fc, os.path.join(scenario_gdb, "tmp_nodes"),
                                     os.path.join(scenario_gdb, "tmp_near_2"),
                                     max_artificial_link_distance_miles, "LOCATION", "NO_ANGLE", "CLOSEST")

    logger.debug("start:  start editor")
    edit = arcpy.da.Editor(os.path.join(scenario_gdb))
    edit.startEditing(False, False)
    edit.startOperation()

    icursor = arcpy.da.InsertCursor(os.path.join(scenario_gdb, modal_layer_name),
                                    ['SHAPE@', 'Artificial', 'Mode_Type', 'Length', 'LOCATION_ID',
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

                len_in_default_units = Q_(polyline.length, "meters").to(the_scenario.default_units_distance).magnitude

                # insert artificial link attributes
                icursor.insertRow([polyline, 1, modal_layer_name, len_in_default_units, location_id, location_id_name])

            else:
                logger.warning("Artificial Link code: Ignoring NEAR_FID {} with NEAR_DIST {}".format(row[0], row[4]))

    del icursor
    logger.debug("start:  stop editing")
    edit.stopOperation()
    edit.stopEditing(True)

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

    # Cleanup
    logger.debug("start:  cleanup tmp_fcs")
    if arcpy.Exists(os.path.join(scenario_gdb, "tmp_nodes")):
        arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_nodes"))
    if arcpy.Exists(os.path.join(scenario_gdb, "tmp_near_2")):
        arcpy.Delete_management(os.path.join(scenario_gdb, "tmp_near_2"))

    if "pipeline" in modal_layer_name:
        if arcpy.Exists(modal_layer_name + "_points_dissolved"):
            arcpy.Delete_management(modal_layer_name + "_points_dissolved")
        if arcpy.Exists(modal_layer_name + "_points"):
            arcpy.Delete_management(modal_layer_name + "_points")

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

    list_of_all_locations = []
    with arcpy.da.SearchCursor(locations_fc, ['location_id']) as scursor:
        for row in scursor:
            list_of_all_locations.append(row[0])

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

    if len(list_of_ignored_locations) == len(list_of_all_locations):
        error = "No facilities are connected to the network. Ensure that your facilities are located within the artificial link tolerance of each relevant mode"
        logger.error(error)
        raise Exception(error)

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
    arcpy.MinimumBoundingGeometry_management("Locations", "Locations_MBG", "CONVEX_HULL")

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

    if count_all_roads > 0:
        roads_percentage = count_roads_subset / count_all_roads
    else:
        roads_percentage = 0

    # Only subset if the subset will result in substantial reduction of the road network size
    if roads_percentage < 0.75:
        # Switch selection to identify what's outside the buffer
        arcpy.SelectLayerByAttribute_management("road_lyr", "SWITCH_SELECTION")

        # Additionally keep any limited access roadways regardless of whether they fall within the selection
        # Note this won't do anything if limited access isn't populated in the network
        arcpy.SelectLayerByAttribute_management("road_lyr", "REMOVE_FROM_SELECTION", "Limited_Access = 1")

        # Delete the features outside the buffer
        with arcpy.da.UpdateCursor('road_lyr', ['OBJECTID']) as ucursor:
            for ucursor_row in ucursor:
                ucursor.deleteRow()

    arcpy.Delete_management("road_lyr")

    arcpy.Delete_management("Locations_MBG")
    arcpy.Delete_management("Locations_MBG_Buffered")

    # finally, compact the geodatabase so the MBG has an effect on runtime.
    arcpy.Compact_management(the_scenario.main_gdb)
    logger.debug("finish: minimum_bounding_geometry")
