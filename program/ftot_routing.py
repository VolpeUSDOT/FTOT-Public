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
    """
    Orchestrates the connectivity process for the scenario.

    This high-level function calls a sequence of sub-routines to prepare locations,
    clean up the network, hook facilities into the transportation network, and
    cache capacity data.

    :param the_scenario: The scenario object containing configuration and paths.
    :param logger: The logger object for writing status and debug messages.
    :return: None
    """
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
    """
    Verifies the existence of required databases and geodatabases.

    Checks if the scenario's main geodatabase and main SQLite database exist.
    Raises an error if either is missing.

    :param the_scenario: The scenario object containing configuration and paths.
    :param logger: The logger object.
    :return: None
    :raises IOError: If the scenario GDB is not found.
    :raises Exception: If the scenario SQLite DB is not found.
    """
    logger.info("start: checks_and_cleanup")

    scenario_gdb = the_scenario.main_gdb
    if not os.path.exists(scenario_gdb):
        error = "can't find scenario gdb {}".format(scenario_gdb)
        raise IOError(error)

    scenario_db = the_scenario.main_db
    if not arcpy.Exists(scenario_db):
        raise Exception("scenario_db not found {} ".format(scenario_db))

    logger.debug("finish: checks_and_cleanup")


# ==============================================================================


def create_locations_fc(the_scenario, logger):
    """
    Creates the locations feature class in the scenario geodatabase.

    Reads location data (ID, X, Y) from the `locations` table in the SQLite database
    and creates a point feature class. It generates two points for each location
    (IN and OUT) slightly offset from the original coordinates. It also handles
    moving locations that are too close to the modal networks to prevent topology errors.

    **Database Interactions:**
        - Reads from `locations` table in `the_scenario.main_db`.
        - Creates and modifies `locations` feature class in `the_scenario.main_gdb`.

    :param the_scenario: The scenario object.
    :param logger: The logger object.
    :return: None
    """
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
    """
    Retrieves a dictionary of location IDs and their XY coordinates.

    **Database Interactions:**
        - Reads `location_id`, `shape_x`, and `shape_y` from the `locations` table
          in the scenario SQLite database.

    :param the_scenario: The scenario object.
    :param logger: The logger object.
    :return: A dictionary where keys are location IDs and values are string representations
             of coordinates "[x, y]".
    """
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
    """
    Retrieves a dictionary mapping ObjectIDs to Location ID names.

    Scans the locations feature class in the geodatabase to build a mapping
    between the GIS Object ID and the custom `location_id_name` field.

    :param the_scenario: The scenario object.
    :param logger: The logger object.
    :return: A dictionary mapping {ObjectID (int): Location_ID_Name (str)}.
    """
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
    """
    Deletes existing artificial links from the network layers.

    Iterates through all permitted modes and removes network features where
    `Artificial` is set to 1. This cleans up the network before creating new
    connectivity links.

    :param the_scenario: The scenario object.
    :param logger: The logger object.
    :return: None
    """
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
    """
    Splits lines at specified points using geometry operations.

    Iterates through a list of lines and points, attempting to cut lines at points
    that fall within a specific tolerance. This is an alternative to standard arcpy
    split tools, often used when advanced licensing is unavailable.

    :param line_list: List of arcpy.Polyline geometries to be cut.
    :param point_list: List of arcpy.Point objects where cuts should occur.
    :param split_lines: List to accumulate the resulting split line geometries.
    :param scenario_proj: The spatial reference object for the project.
    :return: A tuple containing (updated_line_list, updated_point_list, split_lines, status_string).
    """
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
    """
    Connects location points to the modal transportation networks.

    This function manages the creation of "artificial" links that connect facility
    locations to the nearest road, rail, water, or pipeline networks. It initializes
    connection fields, verifies permitted modes, and calls mode-specific linking
    logic. It also ensures `source` and `source_OID` fields are populated for graph edge mapping.

    :param the_scenario: The scenario object.
    :param logger: The logger object.
    :return: None
    """
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
    """
    Exports capacity, volume, and VCR data to the SQLite database.

    Reads network feature classes (locks, intermodal, pipelines) from the geodatabase,
    extracts capacity-related fields, and populates the `capacity_nodes` table in the
    SQLite database. It also handles mapping table creation for pipelines (`pipeline_mapping`).

    **Database Interactions:**
        - Creates and writes to `capacity_nodes` table in `the_scenario.main_db`.
        - Creates and writes to `pipeline_mapping` table in `the_scenario.main_db`.
        - Reads from feature classes in `the_scenario.main_gdb`.

    :param the_scenario: The scenario object.
    :param logger: The logger object.
    :return: None
    """
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
    """
    Performs the GIS logic to physically connect locations to a specific mode.

    Process overview:
    1. Create temporary "Center" points from the main DB (ignoring the offsets).
    2. Near the MODE to these CENTERS. This finds a SINGLE network point for the location.
    3. Split the network links at those single points.
    4. Connect the actual locations_fc (IN/OUT) to those split points.

    Handles special logic for pipelines (points vs lines) and roads (limited access fallbacks).

    :param logger: The logger object.
    :param the_scenario: The scenario object.
    :param modal_layer_name: The name of the mode layer (e.g., "road", "rail").
    :param max_artificial_link_distance_miles: String defining the max distance for connection (e.g. "5 miles").
    :return: None
    """
    logger.debug("start: locations_add_links for mode: {}".format(modal_layer_name))

    scenario_gdb = the_scenario.main_gdb
    arcpy.env.workspace = the_scenario.main_gdb
    fp_to_modal_layer = os.path.join(scenario_gdb, "network", modal_layer_name)
    scenario_proj = ftot_supporting_gis.get_coordinate_system(the_scenario)

    locations_fc = the_scenario.locations_fc
    
    # --------------------------------------------------------------------------
    # PREP NETWORK FIELDS
    # --------------------------------------------------------------------------
    arcpy.DeleteField_management(fp_to_modal_layer, "LOCATION_ID")
    arcpy.AddField_management(os.path.join(scenario_gdb, modal_layer_name), "LOCATION_ID", "long")

    arcpy.DeleteField_management(fp_to_modal_layer, "LOCATION_ID_NAME")
    arcpy.AddField_management(os.path.join(scenario_gdb, modal_layer_name), "LOCATION_ID_NAME", "text")

    # For flagging links that end up being split
    arcpy.DeleteField_management(fp_to_modal_layer, "SPLIT_LINK")
    arcpy.AddField_management(os.path.join(scenario_gdb, modal_layer_name), "SPLIT_LINK", "short")

    # --------------------------------------------------------------------------
    # STEP 1: CREATE TEMPORARY CENTER POINTS
    # We use these for the Near analysis so both IN and OUT share a target.
    # --------------------------------------------------------------------------
    logger.debug("Creating temporary center points for routing consistency")
    temp_centers_fc = "in_memory/temp_routing_centers"
    if arcpy.Exists(temp_centers_fc):
        arcpy.Delete_management(temp_centers_fc)
    
    arcpy.CreateFeatureclass_management("in_memory", "temp_routing_centers", "POINT", spatial_reference=scenario_proj)
    arcpy.AddField_management(temp_centers_fc, "location_id", "TEXT")
    
    # Populate temp centers from the database (original un-offset coordinates)
    with arcpy.da.InsertCursor(temp_centers_fc, ["location_id", "SHAPE@XY"]) as center_cursor:
        with sqlite3.connect(the_scenario.main_db) as db_con:
            sql = "select location_id, shape_x, shape_y from locations;"
            for row in db_con.execute(sql):
                center_cursor.insertRow([str(row[0]), (row[1], row[2])])

    # --------------------------------------------------------------------------
    # PREPARE MODAL LAYERS (Handling pipelines/points vs lines)
    # --------------------------------------------------------------------------
    if float(max_artificial_link_distance_miles.strip(" miles")) < 0.0000001:
        logger.warning("Note: ignoring mode {}. User specified artificial link distance of {}".format(
            modal_layer_name, max_artificial_link_distance_miles))
        definition_query = "Artificial = 999999" 
    else:
        definition_query = "Artificial = 0"

    if "pipeline" in modal_layer_name:
        if arcpy.Exists(os.path.join(scenario_gdb, "network", modal_layer_name + "_points")):
            arcpy.Delete_management(os.path.join(scenario_gdb, "network", modal_layer_name + "_points"))
        if arcpy.Exists(os.path.join(scenario_gdb, "network", modal_layer_name + "_points_dissolved")):
            arcpy.Delete_management(os.path.join(scenario_gdb, "network", modal_layer_name + "_points_dissolved"))

        if arcpy.ProductInfo() == "ArcInfo":
            arcpy.FeatureVerticesToPoints_management(in_features=fp_to_modal_layer,
                                                     out_feature_class=modal_layer_name + "_points",
                                                     point_location="BOTH_ENDS")
        else:

            
            # Fallback for Basic/Standard license because FeatureVerticesToPoints is not supported
            # Add start and end point coordinate fields to modal layer
            arcpy.AddGeometryAttributes_management(fp_to_modal_layer, "LINE_START_MID_END")
            # Make point layer using line start point coordinates
            arcpy.MakeXYEventLayer_management(fp_to_modal_layer, "START_X", "START_Y", "modal_start_points_lyr", scenario_proj)
            # Make point layer using line end point coordinates
            arcpy.MakeXYEventLayer_management(fp_to_modal_layer, "END_X", "END_Y", "modal_end_points_lyr", scenario_proj)
            # XYEventLayer outputs are temporary on-desk layers, so export to permanent layer using the start points then append the end points to it
            out_dataset_path = os.path.join(scenario_gdb, "network")
            out_fc_name = modal_layer_name + "_points"
            arcpy.FeatureClassToFeatureClass_conversion("modal_start_points_lyr", out_dataset_path, out_fc_name)
            target_fc_path = os.path.join(out_dataset_path, out_fc_name)
            arcpy.Append_management(["modal_end_points_lyr"], target_fc_path, "NO_TEST")
            arcpy.Delete_management("modal_start_points_lyr")
            arcpy.Delete_management("modal_end_points_lyr")
            # Field cleanup
            for fld in ["START_X", "START_Y", "MID_X", "MID_Y", "END_X", "END_Y"]:
                try: arcpy.DeleteField_management(fp_to_modal_layer, fld)
                except: pass

        arcpy.MakeFeatureLayer_management(modal_layer_name + "_points", "modal_lyr_tmp_" + modal_layer_name, definition_query)
        arcpy.Dissolve_management("modal_lyr_tmp_" + modal_layer_name, modal_layer_name + "_points_dissolved", "", "", "SINGLE_PART")
        arcpy.MakeFeatureLayer_management(modal_layer_name + "_points_dissolved", "modal_lyr_" + modal_layer_name)
    else:
        arcpy.MakeFeatureLayer_management(fp_to_modal_layer, "modal_lyr_" + modal_layer_name, definition_query)

    logger.debug("adding links between locations and mode {} with max dist of {}".format(modal_layer_name,
                                                                                                Q_(max_artificial_link_distance_miles).to(the_scenario.default_units_distance)))

    # Cleanup temp tables
    for tmp in ["tmp_near", "tmp_near_limited_access_fallback"]:
        if arcpy.Exists(os.path.join(scenario_gdb, tmp)):
            arcpy.Delete_management(os.path.join(scenario_gdb, tmp))

    # --------------------------------------------------------------------------
    # STEP 2: GENERATE NEAR TABLE (Using temp_centers_fc)
    # --------------------------------------------------------------------------
    logger.debug("start: generate_near using ORIGINAL CENTER POINTS")

    if modal_layer_name == 'road':
        # 1. Non-Limited Access
        arcpy.SelectLayerByAttribute_management("modal_lyr_" + modal_layer_name, "NEW_SELECTION", "Artificial = 0 and (Limited_Access = 0 or Limited_Access IS NULL or Limited_Access = -9999)")
        arcpy.GenerateNearTable_analysis(temp_centers_fc, "modal_lyr_" + modal_layer_name,
                                         os.path.join(scenario_gdb, "tmp_near"),
                                         max_artificial_link_distance_miles, "LOCATION", "NO_ANGLE", "CLOSEST")
        
        # 2. Limited Access Fallback
        arcpy.SelectLayerByAttribute_management("modal_lyr_" + modal_layer_name, "NEW_SELECTION", "Artificial = 0 and Limited_Access = 1")
        arcpy.GenerateNearTable_analysis(temp_centers_fc, "modal_lyr_" + modal_layer_name,
                                         os.path.join(scenario_gdb, "tmp_near_limited_access_fallback"),
                                         max_artificial_link_distance_miles, "LOCATION", "NO_ANGLE", "CLOSEST")
    else:
        arcpy.GenerateNearTable_analysis(temp_centers_fc, "modal_lyr_" + modal_layer_name,
                                         os.path.join(scenario_gdb, "tmp_near"),
                                         max_artificial_link_distance_miles, "LOCATION", "NO_ANGLE", "CLOSEST")

    # --------------------------------------------------------------------------
    # STEP 3: SPLIT LINKS AND BUILD CONNECTION DICTIONARY
    # --------------------------------------------------------------------------
    
    edit = arcpy.da.Editor(os.path.join(scenario_gdb))
    edit.startEditing(False, False)
    edit.startOperation()

    id_fieldname = arcpy.Describe(os.path.join(scenario_gdb, modal_layer_name)).OIDFieldName
    
    # Store connections: { loc_id_string : arcpy.Point(network_x, network_y) }
    location_connection_points = {} 
    seenids = {} # For splitting logic

    if arcpy.ProductInfo() != "ArcInfo":
        logger.warning("Advanced license not available. Running modified split process.")

    near_fc_list = ["tmp_near", "tmp_near_limited_access_fallback"] if modal_layer_name == 'road' else ["tmp_near"]

    # Map temp_center OID back to location_id string
    oid_to_loc_id = {}
    with arcpy.da.SearchCursor(temp_centers_fc, ["OID@", "location_id"]) as cur:
        for row in cur:
            oid_to_loc_id[row[0]] = str(row[1])

    # Process Near results
    for near_fc in near_fc_list:
        if not arcpy.Exists(os.path.join(scenario_gdb, near_fc)): continue

        with arcpy.da.SearchCursor(os.path.join(scenario_gdb, near_fc),
                                   ["NEAR_FID", "NEAR_X", "NEAR_Y", "NEAR_DIST", "IN_FID"]) as scursor:
            for row in scursor:
                network_oid = str(row[0])
                near_x = row[1]
                near_y = row[2]
                near_dist = row[3]
                center_oid = row[4]

                if center_oid not in oid_to_loc_id:
                    continue 
                
                real_loc_id = oid_to_loc_id[center_oid]
                
                # If we haven't found a connection for this location yet (prioritize non-limited access for roads)
                if real_loc_id not in location_connection_points:
                    connection_pt = arcpy.Point(near_x, near_y)
                    location_connection_points[real_loc_id] = connection_pt
                    
                    # Only split if not exactly on the line (tolerance check)
                    if near_dist > 0.001: 
                        if network_oid not in seenids:
                            seenids[network_oid] = []
                        point_geom = arcpy.PointGeometry(connection_pt, scenario_proj)
                        seenids[network_oid].append(point_geom)

    # Execute Splits
    if 'pipeline' not in modal_layer_name:
        for theIdToGet in seenids:
            dsc = arcpy.Describe(os.path.join(scenario_gdb, modal_layer_name))
            fields = dsc.fields
            out_fields = [dsc.OIDFieldName, dsc.lengthFieldName, dsc.areaFieldName]
            fieldnames = [field.name if field.name.lower() != 'shape' else 'SHAPE@' for field in fields if field.name not in out_fields]
            # Ensure proper field order
            fieldnames.insert(0, fieldnames.pop(fieldnames.index('SHAPE@')))
            try: fieldnames.insert(1, fieldnames.pop(fieldnames.index('Length')))
            except: pass 
            if "SPLIT_LINK" in fieldnames:
                fieldnames.insert(2, fieldnames.pop(fieldnames.index('SPLIT_LINK')))

            # Get original line
            for search_row in arcpy.da.SearchCursor(os.path.join(scenario_gdb, modal_layer_name), [fieldnames], where_clause=id_fieldname + " = " + theIdToGet):
                in_line = search_row[0]

            # NEW ROBUST SPLIT LOGIC: Use line measures to slice the geometry cleanly
            points_to_split = seenids[theIdToGet]
            measures = []
            for pt_geom in points_to_split:
                m = in_line.measureOnLine(pt_geom.firstPoint)
                # Only split if the point isn't practically at an existing endpoint (0.01m tolerance)
                if 0.01 < m < in_line.length - 0.01:
                    measures.append(m)
            
            measures = sorted(list(set(measures)))
            split_lines = []
            
            if not measures:
                split_lines = [in_line]
            else:
                last_m = 0.0
                for m in measures:
                    seg = in_line.segmentAlongLine(last_m, m)
                    if seg.length > 0:
                        split_lines.append(seg)
                    last_m = m
                
                final_seg = in_line.segmentAlongLine(last_m, in_line.length)
                if final_seg.length > 0:
                    split_lines.append(final_seg)

            # Insert new lines if split occurred
            if len(split_lines) > 1:
                icursor = arcpy.da.InsertCursor(os.path.join(scenario_gdb, modal_layer_name), fieldnames)
                split_endpoints = []
                
                for new_line in split_lines:
                    len_in_default_units = Q_(new_line.length, "meters").to(the_scenario.default_units_distance).magnitude
                    
                    # Prepare row values
                    new_line_values = list(search_row)
                    new_line_values[0] = new_line # Update Geometry
                    
                    # Update Length field if present (usually index 1)
                    if fieldnames[1] == 'Length':
                        new_line_values[1] = len_in_default_units
                    
                    # Set SPLIT_LINK to 1 (usually index 2)
                    if fieldnames[2] == 'SPLIT_LINK':
                        new_line_values[2] = 1

                    icursor.insertRow(new_line_values)
                    
                    # Capture the exact endpoints generated by the cut
                    if new_line.firstPoint: split_endpoints.append(new_line.firstPoint)
                    if new_line.lastPoint: split_endpoints.append(new_line.lastPoint)
                        
                del icursor
                
                # Delete old unsplit line
                with arcpy.da.UpdateCursor(os.path.join(scenario_gdb, modal_layer_name), ['OID@'], where_clause=id_fieldname + " = " + theIdToGet) as ucursor:
                    for row in ucursor:
                        ucursor.deleteRow()
                        
                # ALIGN ARTIFICIAL LINKS: Force connection points to snap perfectly to the new network vertices
                for pt_geom in points_to_split:
                    orig_pt = pt_geom.firstPoint
                    best_dist = float('inf')
                    best_ep = None
                    
                    for ep in split_endpoints:
                        dist = (ep.X - orig_pt.X)**2 + (ep.Y - orig_pt.Y)**2
                        if dist < best_dist:
                            best_dist = dist
                            best_ep = ep
                            
                    if best_ep:
                        for loc_id, conn_pt in location_connection_points.items():
                            if abs(conn_pt.X - orig_pt.X) < 1e-3 and abs(conn_pt.Y - orig_pt.Y) < 1e-3:
                                location_connection_points[loc_id] = best_ep
    edit.stopOperation()
    edit.stopEditing(True)

    # --------------------------------------------------------------------------
    # STEP 4: CREATE ARTIFICIAL LINKS 
    # Connect Offset IN/OUT points to the identified Network Point
    # --------------------------------------------------------------------------
    logger.debug("start: add artificial links")

    edit = arcpy.da.Editor(os.path.join(scenario_gdb))
    edit.startEditing(False, False)
    edit.startOperation()

    icursor = arcpy.da.InsertCursor(os.path.join(scenario_gdb, modal_layer_name),
                                    ['SHAPE@', 'Artificial', 'Mode_Type', 'Length', 'LOCATION_ID', 'LOCATION_ID_NAME'])

    connected_location_id_names = []

    # Iterate through the OFFSET locations (IN and OUT)
    with arcpy.da.SearchCursor(locations_fc, ["location_id", "location_id_name", "SHAPE@XY"]) as loc_cursor:
        for row in loc_cursor:
            loc_id_str = str(row[0])
            loc_name = row[1] # e.g. "500_IN"
            start_x, start_y = row[2]

            # Check if this location's CENTER found a valid network point
            if loc_id_str in location_connection_points:
                
                target_point = location_connection_points[loc_id_str]
                
                # Create line from Offset Location -> Center's Network Point
                coordList = [arcpy.Point(start_x, start_y), target_point]
                polyline = arcpy.Polyline(arcpy.Array(coordList), scenario_proj)
                
                len_in_default_units = Q_(polyline.length, "meters").to(the_scenario.default_units_distance).magnitude

                # Insert Link
                icursor.insertRow([polyline, 1, modal_layer_name, len_in_default_units, loc_id_str, loc_name])
                
                connected_location_id_names.append(loc_name)
    
    del icursor
    edit.stopOperation()
    edit.stopEditing(True)

    # --------------------------------------------------------------------------
    # UPDATE METADATA (connects_x fields)
    # --------------------------------------------------------------------------
    logger.debug("start: connect_x")
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
    if arcpy.Exists(temp_centers_fc):
        arcpy.Delete_management(temp_centers_fc)
    for tmp in ["tmp_near", "tmp_near_limited_access_fallback"]:
        if arcpy.Exists(os.path.join(scenario_gdb, tmp)):
            arcpy.Delete_management(os.path.join(scenario_gdb, tmp))
    if "pipeline" in modal_layer_name:
        if arcpy.Exists(modal_layer_name + "_points_dissolved"):
            arcpy.Delete_management(modal_layer_name + "_points_dissolved")
        if arcpy.Exists(modal_layer_name + "_points"):
            arcpy.Delete_management(modal_layer_name + "_points")
        if arcpy.Exists(modal_layer_name + "_points_dissolved"):
            arcpy.Delete_management(modal_layer_name + "_points_dissolved")
        if arcpy.Exists(modal_layer_name + "_points"):
            arcpy.Delete_management(modal_layer_name + "_points")

    logger.debug("finish: locations_add_links")
    
# ==============================================================================


def ignore_locations_not_connected_to_network(the_scenario, logger):
    """
    Identifies and flags locations that failed to connect to any network.

    Scans the `connects_*` fields in the locations feature class. If a location
    is not connected to any mode, it sets the `ignore` field to 1 in the geodatabase
    and updates the `ignore_location` / `ignore_facility` fields in the SQLite database.

    **Database Interactions:**
        - Updates `locations` feature class in `the_scenario.main_gdb`.
        - Updates `locations` and `facilities` tables in `the_scenario.main_db`.

    :param the_scenario: The scenario object.
    :param logger: The logger object.
    :return: 1 (int) indicating completion.
    :raises Exception: If no facilities are connected to the network.
    """
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
    """
    Subsets the road network to the vicinity of facility locations.

    Creates a minimum bounding geometry (MBG) around all locations, buffers it,
    and then deletes road features falling outside this buffer. This optimization
    improves performance by reducing network size. It also compacts the geodatabase
    upon completion.

    **Database Interactions:**
        - Reads and modifies `road` feature class in `the_scenario.main_gdb`.
        - Creates temporary MBG and buffer layers in `the_scenario.main_gdb`.

    :param the_scenario: The scenario object.
    :param logger: The logger object.
    :return: None
    """
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
