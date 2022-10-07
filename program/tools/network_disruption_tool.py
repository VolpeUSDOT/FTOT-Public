import arcpy
import os
import datetime
import csv

# The following code takes a GIS-based raster dataset representing exposure data (such as a flood depth grid dataset
# from HAZUS or some other source) and determines the maximum exposure value for each network segment
# within a user-specified tolerance. This is then converted to a level of disruption (defined as link
# availability). The current version of the tool simply has a binary result (link is either fully exposed or not
# exposed). Segments with no exposure are not included in the output. This output can also be generated manually if it
# is easier to manually identify exposed segments. This tool only currently works with the rail and road modes.


# ==============================================================================

def run_network_disruption_tool():
    print("FTOT network disruption tool")
    print("-------------------------------")
    print("")
    print("")

    if arcpy.CheckExtension("Spatial") == "Available":
        print("Spatial Analyst is available. Network Disruption tool will run.")
    else:
        error = ("Unable to get ArcGIS Pro spatial analyst extension, which is required to run this tool")
        raise Exception(error)

    network_disruption_prep()


# ==============================================================================

def network_disruption_prep():

    start_time = datetime.datetime.now()
    print('\nStart at ' + str(start_time))

    # SETUP
    print('Prompting for configuration...')

    # Get path to input network
    network = get_network()

    road_y_n, rail_y_n = get_mode_list()

    mode_list = []
    if road_y_n == 'y':
        mode_list.append("road")
    if rail_y_n == 'y':
        mode_list.append("rail")

    input_exposure_grid = get_input_exposure_data()

    input_exposure_grid_field = get_input_exposure_data_field()

    search_tolerance = get_search_tolerance()

    output_dir = get_output_dir()

    # Hard-coding this for now, if more options are added, this can be revisited
    link_availability_approach = 'binary'

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    output_gdb = 'disruption_analysis.gdb'
    full_path_to_output_gdb = os.path.join(output_dir, output_gdb)

    if arcpy.Exists(full_path_to_output_gdb):
        arcpy.Delete_management(full_path_to_output_gdb)
        print('Deleted existing ' + full_path_to_output_gdb)
    arcpy.CreateFileGDB_management(output_dir, output_gdb)

    arcpy.env.workspace = full_path_to_output_gdb

    # MAIN
    # ---------------------------------------------------------------------------

    txt_output_fields = ['mode', 'unique_link_id', 'link_availability', input_exposure_grid_field]

    # Export to txt file
    csv_out = os.path.join(output_dir, "disruption.csv")

    with open(csv_out, "w", newline='') as f:
        wr = csv.writer(f)
        wr.writerow(txt_output_fields)

    for mode in mode_list:

        # Extract raster cells that overlap the modes of interest
        print('Extracting exposure values that overlap network for mode: {}...'.format(mode))
        arcpy.CheckOutExtension("Spatial")
        output_extract_by_mask = arcpy.sa.ExtractByMask(input_exposure_grid, os.path.join(network, mode))
        output_extract_by_mask.save(mode + "_exposure_grid_extract")
        arcpy.CheckInExtension("Spatial")

        # Export raster to point
        print('Converting raster to point for mode: {}...'.format(mode))
        arcpy.RasterToPoint_conversion(mode + "_exposure_grid_extract", os.path.join(
            full_path_to_output_gdb, mode + "_exposure_grid_points"), input_exposure_grid_field)

        # Setup field mapping so that maximum exposure at each network segment is captured
        fms = arcpy.FieldMappings()

        fm = arcpy.FieldMap()
        fm.addInputField(mode + "_exposure_grid_points", "grid_code")
        fm.mergeRule = 'Maximum'

        fms.addFieldMap(fm)

        # Spatial join to network, selecting highest exposure value for each network segment
        print('Identifying maximum exposure value for each network segment for mode: {}...'.format(mode))
        arcpy.SpatialJoin_analysis(os.path.join(network, mode), mode + "_exposure_grid_points",
                                   mode + "_with_exposure",
                                   "JOIN_ONE_TO_ONE", "KEEP_ALL",
                                   fms,
                                   "WITHIN_A_DISTANCE_GEODESIC", search_tolerance)

        # Add new field to store extent of exposure
        print('Calculating exposure levels for mode: {}...'.format(mode))

        arcpy.AddField_management(mode + "_with_exposure", "link_availability", "Float")
        arcpy.AddField_management(mode + "_with_exposure", "comments", "Text")

        arcpy.MakeFeatureLayer_management(mode + "_with_exposure", mode + "_with_exposure_lyr")

        # Convert NULLS to 0 first
        arcpy.SelectLayerByAttribute_management(mode + "_with_exposure_lyr", "NEW_SELECTION", "grid_code IS NULL")
        arcpy.CalculateField_management(mode + "_with_exposure_lyr", "grid_code", 0, "PYTHON_9.3")
        arcpy.SelectLayerByAttribute_management(mode + "_with_exposure_lyr", "CLEAR_SELECTION")

        if link_availability_approach == 'binary':
            # 0 = full exposure/not traversable, 1 = no exposure/link fully available
            arcpy.SelectLayerByAttribute_management(mode + "_with_exposure_lyr", "NEW_SELECTION", "grid_code > 0")
            arcpy.CalculateField_management(mode + "_with_exposure_lyr", "link_availability", 0, "PYTHON_9.3")
            arcpy.SelectLayerByAttribute_management(mode + "_with_exposure_lyr", "SWITCH_SELECTION")
            arcpy.CalculateField_management(mode + "_with_exposure_lyr", "link_availability", 1, "PYTHON_9.3")

        print('Finalizing outputs... for mode: {}...'.format(mode))

        # Rename grid_code back to the original exposure grid field provided in raster dataset
        arcpy.AlterField_management(mode + "_with_exposure", 'grid_code', input_exposure_grid_field)

        fields = ['OBJECTID', 'link_availability', input_exposure_grid_field]

        # Only worry about disrupted links--
        # anything with a link availability of 1 is not disrupted and doesn't need to be included
        arcpy.SelectLayerByAttribute_management(mode + "_with_exposure_lyr", "NEW_SELECTION", "link_availability <> 1")

        with open(csv_out, "a", newline='') as csv_file:
            with arcpy.da.SearchCursor(mode + "_with_exposure_lyr", fields) as cursor:
                for row in cursor:
                    converted_list = [str(element) for element in row]
                    joined_string = ",".join(converted_list)
                    csv_file.write("{},{}\n".format(mode, joined_string))

    end_time = datetime.datetime.now()
    total_run_time = end_time - start_time
    print("\nEnd at {}. Total run time {}".format(end_time, total_run_time))


# ==============================================================================

def get_network():
    while True:
        # Network Disruption Tool FTOT Network Path
        print("network disruption tool | step 1/6:")
        print("-------------------------------")
        print("FTOT network path: ")
        print("Determines the network to use for the disruption analysis")
        network_gdb = input('----------------------> ')
        print("USER INPUT: the FTOT network gdb path: {}".format(network_gdb))
        if not os.path.exists(network_gdb):
            print("The following path is not valid. Please enter a valid path to an FTOT network gdb")
            print("os.path.exists == False for: {}".format(network_gdb))
            continue
        else:
            # Valid value
            break
    return network_gdb


# ==============================================================================

def get_mode_list():
    while True:
        print("network disruption tool | step 2/6:")
        print("-------------------------------")
        print("apply disruption to road?: y or n")
        # Only allowing this tool to work on road/rail for now
        road_y_n = input('----------------------> ')
        print("USER INPUT: disrupt road: {}".format(road_y_n))
        if road_y_n not in ["y", "n", "Y", "N"]:
            print("must type y or n")
            continue
        else:
            # Valid value
            break

    while True:
        print("apply disruption to rail?: y or n")
        rail_y_n = input('----------------------> ')
        print("USER INPUT: disrupt rail: {}".format(rail_y_n))
        if rail_y_n not in ["y", "n", "Y", "N"]:
            print("must type y or n")
            continue
        else:
            # Valid value
            break

    return road_y_n, rail_y_n


# ==============================================================================

def get_input_exposure_data():
    while True:
        # Network Disruption Tool Input Exposure Data
        print("network disruption tool | step 3/6:")
        print("-------------------------------")
        print("FTOT gridded disruption data: ")
        print("Determines the disruption data to be used for the disruption analysis (e.g., gridded flood exposure data)")
        exposure_data = input('----------------------> ')
        print("USER INPUT: the FTOT network exposure data: {}".format(exposure_data))
        if not arcpy.Exists(exposure_data):
            print("The following path is not valid. Please enter a valid path to an FTOT disruption dataset.")
            print("arcpy.exists == False for: {}".format(exposure_data))
        else:
            # Valid value
            break

    return exposure_data


# ==============================================================================

def get_input_exposure_data_field():
    # Network Disruption Tool Input Exposure Data Field
    print("network disruption tool | step 4/6:")
    print("-------------------------------")
    print("Name of field which stores disruption data: ")
    print("Typically this is Value but it may be something else")
    exposure_data_field = input('----------------------> ')
    print("USER INPUT: the FTOT network exposure data field: {}".format(exposure_data_field))

    return exposure_data_field


# ==============================================================================

def get_search_tolerance():
    # Network Disruption Tool Input Exposure Data Field
    print("network disruption tool | step 5/6:")
    print("-------------------------------")
    print("Search Tolerance for determining exposure level of road segment--a good default is 50% of the grid size")
    print("Include units (e.g., meters, feet)")
    search_tolerance = input('----------------------> ')
    print("USER INPUT: the FTOT network search tolerance: {}".format(search_tolerance))

    return search_tolerance


# ==============================================================================

def get_output_dir():
    # Network Disruption Tool FTOT Network Path
    print("network disruption tool | step 6/6:")
    print("-------------------------------")
    print("Output path: ")
    print("Location where output data will be stored")
    output_dir = input('----------------------> ')
    print("USER INPUT: the output path: {}".format(output_dir))

    return output_dir

