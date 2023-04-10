
# ---------------------------------------------------------------------------------------------------
# Name: ftot_maps
# ---------------------------------------------------------------------------------------------------
import os
import arcpy
from shutil import copy
import imageio
import sqlite3
import datetime
from six import iteritems


# ===================================================================================================
def new_map_creation(the_scenario, logger, task):

    logger.info("start: maps")

    # create map directory
    timestamp_folder_name = 'maps_' + datetime.datetime.now().strftime("%Y_%m_%d_%H-%M-%S")

    if task == "m":
        basemap = "default_basemap"
    elif task == "mb":
        basemap = "gray_basemap"
    elif task == "mc":
        basemap = "topo_basemap"
    elif task == "md":
        basemap = "street_basemap"
    else:
        basemap = "default_basemap"

    the_scenario.mapping_directory = os.path.join(the_scenario.scenario_run_directory, "Maps",
                                                  basemap + "_" + timestamp_folder_name)
    scenario_gdb = the_scenario.main_gdb

    scenario_aprx_location = os.path.join(the_scenario.scenario_run_directory, "Maps", "ftot_maps.aprx")
    if not os.path.exists(the_scenario.mapping_directory):
        logger.debug("creating maps directory.")
        os.makedirs(the_scenario.mapping_directory)

    # Must delete existing scenario aprx because there can only be one per scenario (everything in the aprx references
    # the existing scenario main.gdb)
    if os.path.exists(scenario_aprx_location):
        os.remove(scenario_aprx_location)

    # set aprx locations: root is in the common data folder, and the scenario is in the local maps folder
    root_ftot_aprx_location = os.path.join(the_scenario.common_data_folder, "ftot_maps.aprx")
    base_layers_location = os.path.join(the_scenario.common_data_folder, "Base_Layers")

    # copy the aprx from the common data director to the scenario maps directory
    logger.debug("copying the root aprx from common data to the scenario maps directory.")
    copy(root_ftot_aprx_location, scenario_aprx_location)

    # load the map document into memory
    aprx = arcpy.mp.ArcGISProject(scenario_aprx_location)

    # Fix the non-base layers here
    broken_list = aprx.listBrokenDataSources()
    for broken_item in broken_list:
        if broken_item.supports("DATASOURCE"):
            if not broken_item.longName.find("Base") == 0:
                conprop = broken_item.connectionProperties
                conprop['connection_info']['database'] = scenario_gdb
                broken_item.updateConnectionProperties(broken_item.connectionProperties, conprop)

    list_broken_data_sources(aprx, base_layers_location, logger)

    reset_map_base_layers(aprx, logger, basemap)

    export_map_steps(aprx, the_scenario, logger, basemap)

    logger.info("maps located here: {}".format(the_scenario.mapping_directory))


# ===================================================================================================
def list_broken_data_sources(aprx, base_layers_location, logger):
    broken_list = aprx.listBrokenDataSources()
    for broken_item in broken_list:
        if broken_item.supports("DATASOURCE"):
            if broken_item.longName.find("Base") == 0:
                conprop = broken_item.connectionProperties
                conprop['connection_info']['database'] = base_layers_location
                broken_item.updateConnectionProperties(broken_item.connectionProperties, conprop)
                logger.debug("\t" + "broken base layer path fixed: " + broken_item.name)
            else:
                logger.debug("\t" + "broken aprx data source path: " + broken_item.name)


# ===================================================================================================
def reset_map_base_layers(aprx, logger, basemap):

    logger.debug("start:  reset_map_base_layers")
    # turn on base layers, turn off everything else.
    map = aprx.listMaps("ftot_map")[0]
    for lyr in map.listLayers():

        if lyr.longName.find("Base") == 0:
            lyr.visible = True

            # This group layer should be off unless mb step is being run
            if basemap not in ["gray_basemap"] and "World Light Gray" in lyr.longName and \
                    not lyr.longName.endswith("Light Gray Canvas Base"):
                lyr.visible = False

            # This group layer should be off unless mc step is being run
            if basemap not in ["topo_basemap"] and "USGSTopo" in lyr.longName and not lyr.longName.endswith("Layers"):
                lyr.visible = False

            # This group layer should be off unless md step is being run
            if basemap not in ["street_basemap"] and "World Street Map" in lyr.longName and not lyr.longName.endswith("Layers"):
                lyr.visible = False

        # These layers can't be turned off. So leave them on.
        elif lyr.longName.endswith("Light Gray Canvas Base"):
            lyr.visible = True
        elif lyr.longName == "Layers":
            lyr.visible = True

        else:
            lyr.visible = False

    layout = aprx.listLayouts("ftot_layout")[0]
    elm = layout.listElements("TEXT_ELEMENT")[0]
    elm.text = " "

    return


# ===================================================================================================
def get_layer_dictionary(aprx, logger):
    logger.debug("start: get_layer_dictionary")
    layer_dictionary = {}
    map = aprx.listMaps("ftot_map")[0]
    for lyr in map.listLayers():

        layer_dictionary[lyr.longName.replace(" ", "_").replace("\\", "_")] = lyr

    return layer_dictionary


# ===================================================================================================
def debug_layer_status(aprx, logger):

    layer_dictionary = get_layer_dictionary(aprx, logger)

    for layer_name, lyr in sorted(iteritems(layer_dictionary)):

        logger.info("layer: {}, visible: {}".format(layer_name, lyr.visible))


# ===================================================================================================-
def export_to_png(map_name, aprx, the_scenario, logger):

    file_name = str(map_name + ".png").replace(" ", "_").replace("\\", "_")

    file_location = os.path.join(the_scenario.mapping_directory, file_name)

    layout = aprx.listLayouts("ftot_layout")[0]

    # # Setup legend properties so everything refreshes as expected
    legend = layout.listElements("LEGEND_ELEMENT", "Legend")[0]
    legend.fittingStrategy = 'AdjustFrame'
    legend.syncLayerVisibility = True
    legend.syncLayerOrder = True
    aprx.save()

    layout.exportToPNG(file_location)

    logger.info("exporting: {}".format(map_name))


# ===================================================================================================
def export_map_steps(aprx, the_scenario, logger, basemap):

    # ------------------------------------------------------------------------------------

    # Project and zoom to extent of features
    # Get extent of the locations and optimized_route_segments FCs and zoom to buffered extent

    # A list of extents
    extent_list = []

    for fc in [os.path.join(the_scenario.main_gdb, "locations"),
               os.path.join(the_scenario.main_gdb, "optimized_route_segments")]:
        # Cycle through layers grabbing extents, converting them into
        # polygons and adding them to extentList
        desc = arcpy.Describe(fc)
        ext = desc.extent
        array = arcpy.Array([ext.upperLeft, ext.upperRight, ext.lowerRight, ext.lowerLeft])
        extent_list.append(arcpy.Polygon(array))
        sr = desc.spatialReference

    # Create a temporary FeatureClass from the polygons
    arcpy.CopyFeatures_management(extent_list, r"in_memory\temp")

    # Get extent of this temporary layer and zoom to its extent
    desc = arcpy.Describe(r"in_memory\temp")
    extent = desc.extent

    ll_geom = arcpy.PointGeometry(extent.lowerLeft, sr).projectAs(arcpy.SpatialReference(4326))
    ur_geom = arcpy.PointGeometry(extent.upperRight, sr).projectAs(arcpy.SpatialReference(4326))

    ll = ll_geom.centroid
    ur = ur_geom.centroid

    new_sr = create_custom_spatial_ref(ll, ur)

    set_extent(aprx, extent, sr, new_sr)

    # Clean up
    arcpy.Delete_management(r"in_memory\temp")
    del ext, desc, array, extent_list

    # Save aprx so that after step is run user can open the aprx at the right zoom/ extent to continue examining the data.
    aprx.save()

    # reset the map so we are working from a clean and known starting point.
    reset_map_base_layers(aprx, logger, basemap)

    # get a dictionary of all the layers in the aprx
    # might want to get a list of groups
    layer_dictionary = get_layer_dictionary(aprx, logger)

    logger.debug("layer_dictionary.keys(): \t {}".format(list(layer_dictionary.keys())))

    # create a variable for each layer so we can access each layer easily

    s_step_lyr = layer_dictionary["S_STEP"]
    f_step_rmp_lyr = layer_dictionary["F_STEP_RMP"]
    f_step_proc_lyr = layer_dictionary["F_STEP_PROC"]
    f_step_dest_lyr = layer_dictionary["F_STEP_DEST"]
    f_step_lyr = layer_dictionary["F_STEP"]
    f_step_rmp_labels_lyr = layer_dictionary["F_STEP_RMP_W_LABELS"]
    f_step_proc_labels_lyr = layer_dictionary["F_STEP_PROC_W_LABELS"]
    f_step_dest_labels_lyr = layer_dictionary["F_STEP_DEST_W_LABELS"]
    f_step_labels_lyr = layer_dictionary["F_STEP_W_LABELS"]
    o_step_opt_flow_lyr = layer_dictionary["O_STEP_FINAL_OPTIMAL_ROUTES_WITH_COMMODITY_FLOW"]
    o_step_opt_flow_no_labels_lyr = layer_dictionary["O_STEP_FINAL_OPTIMAL_ROUTES_WITH_COMMODITY_FLOW_NO_LABELS"]
    o_step_opt_flow_just_flow_lyr = layer_dictionary["O_STEP_FINAL_OPTIMAL_ROUTES_WITH_COMMODITY_FLOW_JUST_FLOW"]
    o_step_rmp_opt_vs_non_opt_lyr = layer_dictionary["O_STEP_RMP_OPTIMAL_VS_NON_OPTIMAL"]
    o_step_proc_opt_vs_non_opt_lyr = layer_dictionary["O_STEP_PROC_OPTIMAL_VS_NON_OPTIMAL"]
    o_step_dest_opt_vs_non_opt_lyr = layer_dictionary["O_STEP_DEST_OPTIMAL_VS_NON_OPTIMAL"]
    o_step_raw_producer_to_processor_lyr = layer_dictionary["O_STEP_RMP_TO_PROC"]
    o_step_processor_to_destination_lyr = layer_dictionary["O_STEP_PROC_TO_DEST"]
    f2_step_candidates_lyr = layer_dictionary["F2_STEP_CANDIDATES"]
    f2_step_merged_lyr = layer_dictionary["F2_STEP_MERGE"]
    f2_step_candidates_labels_lyr = layer_dictionary["F2_STEP_CANDIDATES_W_LABELS"]
    f2_step_merged_labels_lyr = layer_dictionary["F2_STEP_MERGE_W_LABELS"]

    # Custom user-created maps-- user can create group layers within this parent group layer containing different
    # features they would like to map. Code will look for this group layer name, which should not change.
    if "CUSTOM_USER_CREATED_MAPS" in layer_dictionary:
        custom_maps_parent_lyr = layer_dictionary["CUSTOM_USER_CREATED_MAPS"]
    else:
        custom_maps_parent_lyr = None

    # START MAKING THE MAPS!

#    turn off all the groups
#    turn on the group step
#    set caption information
#    Optimal Processor to Optimal Ultimate Destination Delivery Routes
#    opt_destinations_lyr.visible = True
#    map_name = ""
#    caption =  ""
#    call generate_map()
#    generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

    # S_STEP
    s_step_lyr.visible = True
    sublayers = s_step_lyr.listLayers()
    for sublayer in sublayers:
        sublayer.visible = True
    map_name = "01_S_Step_" + basemap
    caption = ""
    generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

    # F_STEP_RMP
    f_step_rmp_lyr.visible = True
    sublayers = f_step_rmp_lyr.listLayers()
    for sublayer in sublayers:
        sublayer.visible = True
    map_name = "02a_F_Step_RMP_" + basemap
    caption = ""
    generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

    # F_STEP_PROC
    f_step_proc_lyr.visible = True
    sublayers = f_step_proc_lyr.listLayers()
    for sublayer in sublayers:
        sublayer.visible = True
    map_name = "02b_F_Step_User_Defined_PROC_" + basemap
    caption = ""
    generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

    # F_STEP_DEST
    f_step_dest_lyr.visible = True
    sublayers = f_step_dest_lyr.listLayers()
    for sublayer in sublayers:
        sublayer.visible = True
    map_name = "02c_F_Step_DEST_" + basemap
    caption = ""
    generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

    # F_STEP
    f_step_lyr.visible = True
    sublayers = f_step_lyr.listLayers()
    for sublayer in sublayers:
        sublayer.visible = True
    map_name = "02d_F_Step_" + basemap
    caption = ""
    generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

    # F_STEP_RMP_LABELS
    f_step_rmp_labels_lyr.visible = True
    sublayers = f_step_rmp_labels_lyr.listLayers()
    for sublayer in sublayers:
        sublayer.visible = True
    map_name = "02a_F_Step_RMP_With_Labels_" + basemap
    caption = ""
    generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

    # F_STEP_PROC_LABELS
    f_step_proc_labels_lyr.visible = True
    sublayers = f_step_proc_labels_lyr.listLayers()
    for sublayer in sublayers:
        sublayer.visible = True
    map_name = "02b_F_Step_User_Defined_PROC_With_Labels_" + basemap
    caption = ""
    generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

    # F_STEP_DEST_LABELS
    f_step_dest_labels_lyr.visible = True
    sublayers = f_step_dest_labels_lyr.listLayers()
    for sublayer in sublayers:
        sublayer.visible = True
    map_name = "02c_F_Step_DEST_With_Labels_" + basemap
    caption = ""
    generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

    # F_STEP_LABELS
    f_step_labels_lyr.visible = True
    sublayers = f_step_labels_lyr.listLayers()
    for sublayer in sublayers:
        sublayer.visible = True
    map_name = "02d_F_Step_With_Labels_" + basemap
    caption = ""
    generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

    # O_STEP_FINAL_OPTIMAL_ROUTES_WITH_COMMODITY_FLOW
    # O_STEP - A -
    o_step_opt_flow_lyr.visible = True
    sublayers = o_step_opt_flow_lyr.listLayers()
    for sublayer in sublayers:
        sublayer.visible = True
    map_name = "04a_O_Step_Final_Optimal_Routes_With_Commodity_Flow_" + basemap
    caption = ""
    generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

    # O_STEP - B - same as above but with no labels to make it easier to see routes.
    o_step_opt_flow_no_labels_lyr.visible = True
    sublayers = o_step_opt_flow_no_labels_lyr.listLayers()
    for sublayer in sublayers:
        sublayer.visible = True
    map_name = "04b_O_Step_Final_Optimal_Routes_With_Commodity_Flow_NO_LABELS_" + basemap
    caption = ""
    generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

    # O_STEP - C - just flows. no Origins or Destinations
    o_step_opt_flow_just_flow_lyr.visible = True
    sublayers = o_step_opt_flow_just_flow_lyr.listLayers()
    for sublayer in sublayers:
        sublayer.visible = True
    map_name = "04c_O_Step_Final_Optimal_Routes_With_Commodity_Flow_JUST_FLOW_" + basemap
    caption = ""
    generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

    # O_STEP_OPTIMAL_AND_NON_OPTIMAL_RMP
    o_step_rmp_opt_vs_non_opt_lyr.visible = True
    sublayers = o_step_rmp_opt_vs_non_opt_lyr.listLayers()
    for sublayer in sublayers:
        sublayer.visible = True
    map_name = "04d_O_Step_Optimal_and_Non_Optimal_RMP_" + basemap
    caption = ""
    generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

    # O_STEP_OPTIMAL_AND_NON_OPTIMAL_DEST
    o_step_proc_opt_vs_non_opt_lyr.visible = True
    sublayers = o_step_proc_opt_vs_non_opt_lyr.listLayers()
    for sublayer in sublayers:
        sublayer.visible = True
    map_name = "04e_O_Step_Optimal_and_Non_Optimal_PROC_" + basemap
    caption = ""
    generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

    # O_STEP_OPTIMAL_AND_NON_OPTIMAL_DEST
    o_step_dest_opt_vs_non_opt_lyr.visible = True
    sublayers = o_step_dest_opt_vs_non_opt_lyr.listLayers()
    for sublayer in sublayers:
        sublayer.visible = True
    map_name = "04f_O_Step_Optimal_and_Non_Optimal_DEST_" + basemap
    caption = ""
    generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

    # check if processor fc exists. if it doesn't stop here since there won't be any more relevant maps to make

    processor_merged_fc = the_scenario.processors_fc
    processor_fc_feature_count = get_feature_count(processor_merged_fc, logger)

    if processor_fc_feature_count:
        logger.debug("Processor Feature Class has {} features....".format(processor_fc_feature_count))
        # F2_STEP_CANDIDATES
        f2_step_candidates_lyr.visible = True
        sublayers = f2_step_candidates_lyr.listLayers()
        for sublayer in sublayers:
            sublayer.visible = True
        map_name = "03a_F2_Step_Processor_Candidates_" + basemap
        caption = ""
        generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

        # F2_STEP_MERGE
        f2_step_merged_lyr.visible = True
        sublayers = f2_step_merged_lyr.listLayers()
        for sublayer in sublayers:
            sublayer.visible = True
        map_name = "03b_F2_Step_Processors_All_" + basemap
        caption = ""
        generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

        # F2_STEP_CANDIDATES_W_LABELS
        f2_step_candidates_labels_lyr.visible = True
        sublayers = f2_step_candidates_labels_lyr.listLayers()
        for sublayer in sublayers:
            sublayer.visible = True
        map_name = "03a_F2_Step_Processor_Candidates_With_Labels_" + basemap
        caption = ""
        generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

        # F2_STEP_MERGE_W_LABELS
        f2_step_merged_labels_lyr.visible = True
        sublayers = f2_step_merged_labels_lyr.listLayers()
        for sublayer in sublayers:
            sublayer.visible = True
        map_name = "03b_F2_Step_Processors_All_With_Labels_" + basemap
        caption = ""
        generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

        # O_STEP_RAW_MATERIAL_PRODUCER_TO_PROCESSOR
        o_step_raw_producer_to_processor_lyr.visible = True
        sublayers = o_step_raw_producer_to_processor_lyr.listLayers()
        for sublayer in sublayers:
            sublayer.visible = True
        map_name = "05a_O_Step_Raw_Material_Producer_To_Processor_" + basemap
        caption = ""
        generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

        # O_STEP_PROCESSOR_TO_ULTIMATE_DESTINATION
        o_step_processor_to_destination_lyr.visible = True
        sublayers = o_step_processor_to_destination_lyr.listLayers()
        for sublayer in sublayers:
            sublayer.visible = True
        map_name = "05b_O_Step_Processor_To_Ultimate_Destination_" + basemap
        caption = ""
        generate_map(caption, map_name, aprx, the_scenario, logger, basemap)

    else:
        logger.info("processor layer has {} features. skipping the rest of the mapping".format
                    (processor_fc_feature_count))

    # CUSTOM_MAPS
    if "CUSTOM_USER_CREATED_MAPS" in layer_dictionary:
        sub_groups_custom_maps = custom_maps_parent_lyr.listLayers()
        for sub_group in sub_groups_custom_maps:
            # Need to limit maps to just the group layers which are NOT the parent group layer.
            if sub_group.isGroupLayer and sub_group != custom_maps_parent_lyr:
                # First ensure that the group layer is on as this as all layers are turned off at beginning by default
                custom_maps_parent_lyr.visible = True
                # Then turn on the individual custom map
                sub_group.visible = True
                count = 0
                sublayers = sub_group.listLayers()
                for sublayer in sublayers:
                    sublayer.visible = True
                    count += 1
                map_name = str(sub_group) + "_" + basemap
                caption = ""
                # Only generate map if there are actual features inside the group layer
                if count > 0:
                    generate_map(caption, map_name, aprx, the_scenario, logger, basemap)


# ===================================================================================================
def generate_map(caption, map_name, aprx, the_scenario, logger, basemap):
    import datetime

    # create a text element on the aprx for the caption
    layout = aprx.listLayouts("ftot_layout")[0]
    elm = layout.listElements("TEXT_ELEMENT")[0]

    # set the caption text
    dt = datetime.datetime.now()

    elm.text = "Scenario Name: " + the_scenario.scenario_name + " -- Date: " + str(dt.strftime("%b %d, %Y")) + "\n" + \
               caption

    # programmatically set the caption height
    if len(caption) < 180:
        elm.elementHeight = 0.5

    else:
        elm.elementHeight = 0.0017*len(caption) + 0.0

    # Force the Y position to 1 inch as ArcGIS Pro 2.9 seems to have a bug of moving elements around.
    elm.elementPositionY = 1

    # export that map to png
    export_to_png(map_name, aprx, the_scenario, logger)

    # reset the map layers
    reset_map_base_layers(aprx, logger, basemap)


# ===================================================================================================
def prepare_time_commodity_subsets_for_mapping(the_scenario, logger, task):

    logger.info("start: time and commodity maps")

    if task == "m2":
        basemap = "default_basemap"
    elif task == "m2b":
        basemap = "gray_basemap"
    elif task == "m2c":
        basemap = "topo_basemap"
    elif task == "m2d":
        basemap = "street_basemap"
    else:
        basemap = "default_basemap"

    timestamp_folder_name = 'maps_' + datetime.datetime.now().strftime("%Y_%m_%d_%H-%M-%S")
    the_scenario.mapping_directory = os.path.join(the_scenario.scenario_run_directory, "Maps_Time_Commodity",
                                                  basemap + "_" + timestamp_folder_name)
    scenario_aprx_location = os.path.join(the_scenario.scenario_run_directory, "Maps_Time_Commodity", "ftot_maps.aprx")
    scenario_gdb = the_scenario.main_gdb
    arcpy.env.workspace = scenario_gdb

    if not os.path.exists(the_scenario.mapping_directory):
        logger.debug("creating maps directory.")
        os.makedirs(the_scenario.mapping_directory)

    if os.path.exists(scenario_aprx_location):
        os.remove(scenario_aprx_location)

    # set aprx locations: root is in the common data folder, and the scenario is in the local maps folder
    root_ftot_aprx_location = os.path.join(the_scenario.common_data_folder, "ftot_maps.aprx")
    base_layers_location = os.path.join(the_scenario.common_data_folder, "Base_Layers")

    # copy the aprx from the common data director to the scenario maps directory
    logger.debug("copying the root aprx from common data to the scenario maps directory.")
    copy(root_ftot_aprx_location, scenario_aprx_location)

    # load the map document into memory
    aprx = arcpy.mp.ArcGISProject(scenario_aprx_location)

    # Fix the non-base layers here
    broken_list = aprx.listBrokenDataSources()
    for broken_item in broken_list:
        if broken_item.supports("DATASOURCE"):
            if not broken_item.longName.find("Base") == 0:
                conprop = broken_item.connectionProperties
                conprop['connection_info']['database'] = scenario_gdb
                broken_item.updateConnectionProperties(broken_item.connectionProperties, conprop)

    list_broken_data_sources(aprx, base_layers_location, logger)

    # Project and zoom to extent of features
    # Get extent of the locations and optimized_route_segments FCs and zoom to buffered extent

    # A list of extents
    extent_list = []

    for fc in [os.path.join(the_scenario.main_gdb, "locations"),
               os.path.join(the_scenario.main_gdb, "optimized_route_segments")]:
        # Cycle through layers grabbing extents, converting them into
        # polygons and adding them to extentList
        desc = arcpy.Describe(fc)
        ext = desc.extent
        array = arcpy.Array([ext.upperLeft, ext.upperRight, ext.lowerRight, ext.lowerLeft])
        extent_list.append(arcpy.Polygon(array))
        sr = desc.spatialReference

    # Create a temporary FeatureClass from the polygons
    arcpy.CopyFeatures_management(extent_list, r"in_memory\temp")

    # Get extent of this temporary layer and zoom to its extent
    desc = arcpy.Describe(r"in_memory\temp")
    extent = desc.extent

    ll_geom = arcpy.PointGeometry(extent.lowerLeft, sr).projectAs(arcpy.SpatialReference(4326))
    ur_geom = arcpy.PointGeometry(extent.upperRight, sr).projectAs(arcpy.SpatialReference(4326))

    ll = ll_geom.centroid
    ur = ur_geom.centroid

    new_sr = create_custom_spatial_ref(ll, ur)

    set_extent(aprx, extent, sr, new_sr)

    # Clean up
    arcpy.Delete_management(r"in_memory\temp")
    del ext, desc, array, extent_list

    # Save aprx so that after step is run user can open the aprx at the right zoom/ extent to continue examining data.
    aprx.save()

    logger.info("start: building dictionaries of time steps and commodities occurring within the scenario")
    commodity_dict = {}
    time_dict = {}

    flds = ["COMMODITY", "TIME_PERIOD"]

    with arcpy.da.SearchCursor(os.path.join(scenario_gdb, 'optimized_route_segments'), flds) as cursor:
        for row in cursor:
            if row[0] not in commodity_dict:
                commodity_dict[row[0]] = True
            if row[1] not in time_dict:
                time_dict[row[1]] = True

    # Add Flag Fields to all of the feature classes which will need to be mapped (first delete if they already exist).

    if len(arcpy.ListFields("optimized_route_segments", "Include_Map")) > 0:
        arcpy.DeleteField_management("optimized_route_segments", ["Include_Map"])
    if len(arcpy.ListFields("raw_material_producers", "Include_Map")) > 0:
        arcpy.DeleteField_management("raw_material_producers", ["Include_Map"])
    if len(arcpy.ListFields("processors", "Include_Map")) > 0:
        arcpy.DeleteField_management("processors", ["Include_Map"])
    if len(arcpy.ListFields("ultimate_destinations", "Include_Map")) > 0:
        arcpy.DeleteField_management("ultimate_destinations", ["Include_Map"])

    arcpy.AddField_management(os.path.join(scenario_gdb, "optimized_route_segments"), "Include_Map", "SHORT")
    arcpy.AddField_management(os.path.join(scenario_gdb, "raw_material_producers"), "Include_Map", "SHORT")
    arcpy.AddField_management(os.path.join(scenario_gdb, "processors"), "Include_Map", "SHORT")
    arcpy.AddField_management(os.path.join(scenario_gdb, "ultimate_destinations"), "Include_Map", "SHORT")

    # Iterate through each commodity
    for commodity in commodity_dict:
        layer_name = "commodity_" + commodity
        logger.info("Processing " + layer_name)
        image_name = "optimal_flows_commodity_" + commodity + "_" + basemap
        sql_where_clause = "COMMODITY = '" + commodity + "'"

        # ID route segments and facilities associated with the subset
        link_subset_to_route_segments_and_facilities(sql_where_clause, the_scenario)

        # Make dissolved (aggregate) fc for this commodity
        dissolve_optimal_route_segments_feature_class_for_commodity_mapping(layer_name, sql_where_clause,
                                                                            the_scenario, logger)

        # Make map
        make_time_commodity_maps(aprx, image_name, the_scenario, logger, basemap)

        # Clear flag fields
        clear_flag_fields(the_scenario)

    # Iterate through each time period
    for time in time_dict:
        layer_name = "time_period_" + str(time)
        logger.info("Processing " + layer_name)
        image_name = "optimal_flows_time_" + str(time) + "_" + basemap
        sql_where_clause = "TIME_PERIOD = '" + str(time) + "'"

        # ID route segments and facilities associated with the subset
        link_subset_to_route_segments_and_facilities(sql_where_clause, the_scenario)

        # Make dissolved (aggregate) fc for this commodity
        dissolve_optimal_route_segments_feature_class_for_commodity_mapping(layer_name, sql_where_clause,
                                                                            the_scenario, logger)

        # Make map
        make_time_commodity_maps(aprx, image_name, the_scenario, logger, basemap)

        # Clear flag fields
        clear_flag_fields(the_scenario)

    # Iterate through each commodity/time period combination
    for commodity in commodity_dict:
        for time in time_dict:
            layer_name = "commodity_" + commodity + "_time_period_" + str(time)
            logger.info("Processing " + layer_name)
            image_name = "optimal_flows_commodity_" + commodity + "_time_" + str(time) + "_" + basemap
            sql_where_clause = "COMMODITY = '" + commodity + "' AND TIME_PERIOD = '" + str(time) + "'"

            # ID route segments and facilities associated with the subset
            link_subset_to_route_segments_and_facilities(sql_where_clause, the_scenario)

            # Make dissolved (aggregate) fc for this commodity
            dissolve_optimal_route_segments_feature_class_for_commodity_mapping(layer_name, sql_where_clause,
                                                                                the_scenario, logger)

            # Make map
            make_time_commodity_maps(aprx, image_name, the_scenario, logger, basemap)

            # Clear flag fields
            clear_flag_fields(the_scenario)

    # Create time animation out of time step maps
    map_animation(the_scenario, logger)

    logger.info("time and commodity maps located here: {}".format(the_scenario.mapping_directory))


# ===================================================================================================
def link_subset_to_route_segments_and_facilities(sql_where_clause, the_scenario):

    scenario_gdb = the_scenario.main_gdb

    # Create dictionaries for tracking facilities
    facilities_dict = {}
    segment_dict = {}

    # Initiate search cursor to ID route subset

    # get a list of the optimal facilities that share the commodity/time period of interest
    with sqlite3.connect(the_scenario.main_db) as db_con:
        sql = """select facility_name from optimal_facilities where {};""".format(sql_where_clause)
        db_cur = db_con.execute(sql)
        sql_facility_data = db_cur.fetchall()
        for facilities in sql_facility_data:
            facilities_dict[facilities[0]] = True
        # print facilities_dict

    # Flag raw_material_suppliers, processors, and destinations that are used for the subset of routes

    for facility_type in ["raw_material_producers", "processors", "ultimate_destinations"]:
        with arcpy.da.UpdateCursor(os.path.join(scenario_gdb, facility_type),
                                   ['facility_name', 'Include_Map']) as ucursor1:
            for row in ucursor1:
                if row[0] in facilities_dict:
                    row[1] = 1
                else:
                    row[1] = 0
                ucursor1.updateRow(row)

    # Flag route segments that occur within the subset of routes
    with arcpy.da.SearchCursor(os.path.join(scenario_gdb, "optimized_route_segments"),
                               ['ObjectID', 'Include_Map', 'Commodity', 'Time_Period'], sql_where_clause) as scursor1:
        for row in scursor1:
            segment_dict[row[0]] = True
    with arcpy.da.UpdateCursor(os.path.join(scenario_gdb, "optimized_route_segments"),
                               ['ObjectID', 'Include_Map', 'Commodity', 'Time_Period'],) as ucursor2:
        for row in ucursor2:
            if row[0] in segment_dict:
                row[1] = 1
            else:
                row[1] = 0
            ucursor2.updateRow(row)

    # Clean up dictionary
    del facilities_dict
    del segment_dict


# ===================================================================================================
def make_time_commodity_maps(aprx, image_name, the_scenario, logger, basemap):

    scenario_gdb = the_scenario.main_gdb

    # Check if route segment layer has any data--
    # Currently only mapping if there is route data
    if arcpy.Exists("route_segments_lyr"):
        arcpy.Delete_management("route_segments_lyr")
    arcpy.MakeFeatureLayer_management(os.path.join(scenario_gdb, 'optimized_route_segments'),
                                      "route_segments_lyr", "Include_Map = 1")
    result = arcpy.GetCount_management("route_segments_lyr")
    count = int(result.getOutput(0))

    if count > 0:

        # reset the map so we are working from a clean and known starting point.
        reset_map_base_layers(aprx, logger, basemap)

        # get a dictionary of all the layers in the aprx
        # might want to get a list of groups
        layer_dictionary = get_layer_dictionary(aprx, logger)

        # create a variable for for each layer of interest for the time and commodity mapping
        # so we can access each layer easily
        time_commodity_segments_lyr = layer_dictionary["TIME_COMMODITY"]

        # START MAKING THE MAPS!

        # Establish definition queries to define the subset for each layer,
        # turn off if there are no features for that particular subset.
        for groupLayer in [time_commodity_segments_lyr]:
            sublayers = groupLayer.listLayers()
            for sublayer in sublayers:
                if sublayer.supports("DATASOURCE"):
                    if sublayer.dataSource == os.path.join(scenario_gdb, 'optimized_route_segments'):
                        sublayer.definitionQuery = "Include_Map = 1"

                    if sublayer.dataSource == os.path.join(scenario_gdb, 'raw_material_producers'):
                        sublayer.definitionQuery = "Include_Map = 1"
                        rmp_count = get_feature_count(sublayer, logger)

                    if sublayer.dataSource == os.path.join(scenario_gdb, 'processors'):
                        sublayer.definitionQuery = "Include_Map = 1"
                        proc_count = get_feature_count(sublayer, logger)

                    if sublayer.dataSource == os.path.join(scenario_gdb, 'ultimate_destinations'):
                        sublayer.definitionQuery = "Include_Map = 1"
                        dest_count = get_feature_count(sublayer, logger)

        # Actually export map to file
        time_commodity_segments_lyr.visible = True
        sublayers = time_commodity_segments_lyr.listLayers()
        for sublayer in sublayers:
            sublayer.visible = True
        caption = ""
        generate_map(caption, image_name, aprx, the_scenario, logger, basemap)

        # Clean up aprx
        del aprx

    # No mapping if there are no routes
    else:
        logger.info("no routes for this combination of time steps and commodities... skipping mapping...")


# ===================================================================================================
def clear_flag_fields(the_scenario):

    scenario_gdb = the_scenario.main_gdb

    # Everything is set to 1 for cleanup
    # 0's are only set in link_route_to_route_segments_and_facilities, when fc's are subset by commodity/time step
    arcpy.CalculateField_management(os.path.join(scenario_gdb, "optimized_route_segments"), "Include_Map", 1,
                                    'PYTHON_9.3')
    arcpy.CalculateField_management(os.path.join(scenario_gdb, "raw_material_producers"), "Include_Map", 1,
                                    'PYTHON_9.3')
    arcpy.CalculateField_management(os.path.join(scenario_gdb, "processors"), "Include_Map", 1, 'PYTHON_9.3')
    arcpy.CalculateField_management(os.path.join(scenario_gdb, "ultimate_destinations"), "Include_Map", 1, 'PYTHON_9.3')


# ===================================================================================================
def map_animation(the_scenario, logger):

    # Below animation is currently only set up to animate scenario time steps
    # NOT commodities or a combination of commodity and time steps.

    # Clean Up-- delete existing gif if it exists already

    try:
        os.remove(os.path.join(the_scenario.mapping_directory, 'optimal_flows_time.gif'))
        logger.debug("deleted existing time animation gif")
    except OSError:
        pass

    images = []

    logger.info("start: creating time step animation gif")
    for a_file in os.listdir(the_scenario.mapping_directory):
        if a_file.startswith("optimal_flows_time"):
            images.append(imageio.imread(os.path.join(the_scenario.mapping_directory, a_file)))

    if len(images) > 0:
        imageio.mimsave(os.path.join(the_scenario.mapping_directory, 'optimal_flows_time.gif'), images, duration=2)


# ===================================================================================================
def get_feature_count(fc, logger):
    result = arcpy.GetCount_management(fc)
    count = int(result.getOutput(0))
    logger.debug("number of features in fc {}: \t{}".format(fc, count))
    return count


# ===================================================================================================
def create_custom_spatial_ref(ll, ur):

    # prevent errors by setting to default USA Contiguous Lambert Conformal Conic projection if there are problems
    # basing projection off of the facilities
    try:
        central_meridian = str(((ur.X - ll.X) / 2) + ll.X)
    except:
        central_meridian = -96.0
    try:
        stand_par_1 = str(((ur.Y - ll.Y) / 6) + ll.Y)
    except:
        stand_par_1 = 33.0
    try:
        stand_par_2 = str(ur.Y - ((ur.Y - ll.Y) / 6))
    except:
        stand_par_2 = 45.0
    try:
        lat_orig = str(((ur.Y - ll.Y) / 2) + ll.Y)
    except:
        lat_orig = 39.0

    projection = "PROJCS['Custom_Lambert_Conformal_Conic'," \
                 "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]]," \
                 "PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]," \
                 "PROJECTION['Lambert_Conformal_Conic']," \
                 "PARAMETER['False_Easting',0.0]," \
                 "PARAMETER['False_Northing',0.0]," \
                 "PARAMETER['Central_Meridian'," + central_meridian + "]," \
                 "PARAMETER['Standard_Parallel_1'," + stand_par_1 + "]," \
                 "PARAMETER['Standard_Parallel_2'," + stand_par_2 + "]," \
                 "PARAMETER['Latitude_Of_Origin'," + lat_orig + "]," \
                 "UNIT['Meter',1.0]]"

    new_sr = arcpy.SpatialReference()
    new_sr.loadFromString(projection)

    return new_sr


# ===================================================================================================
def set_extent(aprx, extent, sr, new_sr):

    map = aprx.listMaps("ftot_map")[0]
    map_layout = aprx.listLayouts("ftot_layout")[0]
    map_frame = map_layout.listElements("MAPFRAME_ELEMENT", "FTOT")[0]

    map.spatialReference = new_sr

    ll_geom, lr_geom, ur_geom, ul_geom = [arcpy.PointGeometry(extent.lowerLeft, sr).projectAs(new_sr),
                                          arcpy.PointGeometry(extent.lowerRight, sr).projectAs(new_sr),
                                          arcpy.PointGeometry(extent.upperRight, sr).projectAs(new_sr),
                                          arcpy.PointGeometry(extent.upperLeft, sr).projectAs(new_sr)]

    ll = ll_geom.centroid
    lr = lr_geom.centroid
    ur = ur_geom.centroid
    ul = ul_geom.centroid

    ext_buff_dist_x = ((int(abs(ll.X - lr.X))) * .15)
    ext_buff_dist_y = ((int(abs(ll.Y - ul.Y))) * .15)
    ext_buff_dist = max([ext_buff_dist_x, ext_buff_dist_y])
    orig_extent_pts = arcpy.Array()
    # Array to hold points for the bounding box for initial extent
    for coords in [ll, lr, ur, ul, ll]:
        orig_extent_pts.add(coords)

    polygon_tmp_1 = arcpy.Polygon(orig_extent_pts)
    # buffer the temporary poly by 10% of width or height of extent as calculated above
    buff_poly = polygon_tmp_1.buffer(ext_buff_dist)
    new_extent = buff_poly.extent

    map_frame.camera.setExtent(new_extent)


# ===================================================================================================
def dissolve_optimal_route_segments_feature_class_for_commodity_mapping(layer_name, sql_where_clause, the_scenario,
                                                                        logger):

    # Make a dissolved version of fc for mapping aggregate flows
    logger.info("start: dissolve_optimal_route_segments_feature_class_for_commodity_mapping")

    scenario_gdb = the_scenario.main_gdb

    arcpy.env.workspace = scenario_gdb

    # Delete previous fcs if they exist
    for fc in ["optimized_route_segments_dissolved_tmp", "optimized_route_segments_split_tmp",
               "optimized_route_segments_dissolved_tmp2", "optimized_route_segments_dissolved_tmp2",
               "dissolved_segments_lyr", "optimized_route_segments_dissolved_commodity",
               "optimized_route_segments_dissolved_" + layer_name]:
        if arcpy.Exists(fc):
            arcpy.Delete_management(fc)

    arcpy.MakeFeatureLayer_management("optimized_route_segments", "optimized_route_segments_lyr")
    arcpy.SelectLayerByAttribute_management(in_layer_or_view="optimized_route_segments_lyr",
                                            selection_type="NEW_SELECTION", where_clause=sql_where_clause)

    # Dissolve
    arcpy.Dissolve_management("optimized_route_segments_lyr", "optimized_route_segments_dissolved_tmp",
                              ["NET_SOURCE_NAME", "NET_SOURCE_OID", "ARTIFICIAL"],
                              [['COMMODITY_FLOW', 'SUM']], "SINGLE_PART", "DISSOLVE_LINES")

    # Second dissolve needed to accurately show aggregate pipeline flows
    arcpy.FeatureToLine_management("optimized_route_segments_dissolved_tmp", "optimized_route_segments_split_tmp")

    arcpy.AddGeometryAttributes_management("optimized_route_segments_split_tmp", "LINE_START_MID_END")

    arcpy.Dissolve_management("optimized_route_segments_split_tmp", "optimized_route_segments_dissolved_tmp2",
                              ["NET_SOURCE_NAME", "Shape_Length", "MID_X", "MID_Y", "ARTIFICIAL"],
                              [["SUM_COMMODITY_FLOW", "SUM"]], "SINGLE_PART", "DISSOLVE_LINES")

    arcpy.AddField_management(in_table="optimized_route_segments_dissolved_tmp2", field_name="SUM_COMMODITY_FLOW",
                              field_type="DOUBLE", field_precision="", field_scale="", field_length="", field_alias="",
                              field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="")
    arcpy.CalculateField_management(in_table="optimized_route_segments_dissolved_tmp2", field="SUM_COMMODITY_FLOW",
                                    expression="!SUM_SUM_COMMODITY_FLOW!", expression_type="PYTHON_9.3", code_block="")
    arcpy.DeleteField_management(in_table="optimized_route_segments_dissolved_tmp2",
                                 drop_field="SUM_SUM_COMMODITY_FLOW")
    arcpy.DeleteField_management(in_table="optimized_route_segments_dissolved_tmp2", drop_field="MID_X")
    arcpy.DeleteField_management(in_table="optimized_route_segments_dissolved_tmp2", drop_field="MID_Y")

    # Sort for mapping order
    arcpy.AddField_management(in_table="optimized_route_segments_dissolved_tmp2", field_name="SORT_FIELD",
                              field_type="SHORT")
    arcpy.MakeFeatureLayer_management("optimized_route_segments_dissolved_tmp2", "dissolved_segments_lyr")
    arcpy.SelectLayerByAttribute_management(in_layer_or_view="dissolved_segments_lyr", selection_type="NEW_SELECTION",
                                            where_clause="NET_SOURCE_NAME = 'road'")
    arcpy.CalculateField_management(in_table="dissolved_segments_lyr", field="SORT_FIELD",
                                    expression=1, expression_type="PYTHON_9.3")
    arcpy.SelectLayerByAttribute_management(in_layer_or_view="dissolved_segments_lyr", selection_type="NEW_SELECTION",
                                            where_clause="NET_SOURCE_NAME = 'rail'")
    arcpy.CalculateField_management(in_table="dissolved_segments_lyr", field="SORT_FIELD",
                                    expression=2, expression_type="PYTHON_9.3")
    arcpy.SelectLayerByAttribute_management(in_layer_or_view="dissolved_segments_lyr", selection_type="NEW_SELECTION",
                                            where_clause="NET_SOURCE_NAME = 'water'")
    arcpy.CalculateField_management(in_table="dissolved_segments_lyr", field="SORT_FIELD",
                                    expression=3, expression_type="PYTHON_9.3")
    arcpy.SelectLayerByAttribute_management(in_layer_or_view="dissolved_segments_lyr", selection_type="NEW_SELECTION",
                                            where_clause="NET_SOURCE_NAME LIKE 'pipeline%'")
    arcpy.CalculateField_management(in_table="dissolved_segments_lyr", field="SORT_FIELD",
                                    expression=4, expression_type="PYTHON_9.3")

    arcpy.Sort_management("optimized_route_segments_dissolved_tmp2", "optimized_route_segments_dissolved_commodity",
                          [["SORT_FIELD", "ASCENDING"]])

    # Delete temp fc's
    arcpy.Delete_management("optimized_route_segments_dissolved_tmp")
    arcpy.Delete_management("optimized_route_segments_split_tmp")
    arcpy.Delete_management("optimized_route_segments_dissolved_tmp2")
    arcpy.Delete_management("optimized_route_segments_lyr")
    arcpy.Delete_management("dissolved_segments_lyr")

    # Copy to permanent fc (unique to commodity name)
    arcpy.CopyFeatures_management("optimized_route_segments_dissolved_commodity",
                                  "optimized_route_segments_dissolved_" + layer_name)
