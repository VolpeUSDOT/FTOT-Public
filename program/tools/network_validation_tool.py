# -------------------------------------------------------------------------------
# Name:        Network Validation Tool
# Purpose:     Confirms a user-provided network meets basic requirements and prints issues
# -------------------------------------------------------------------------------

import arcpy
import os
from six.moves import input
from shutil import rmtree


# ==============================================================================


def get_network_path():

    print("gdb file with network to validate (drag and drop is fine here):")
    gdb_path = ""
    while not os.path.exists(gdb_path):
        gdb_path = input('----------------------> ').strip('\"')
        print("USER INPUT ----------------->:  {}".format(gdb_path))
        if not os.path.exists(gdb_path):
            print("Path is not valid. Please enter a valid gdb file path.")
    return gdb_path


# ==============================================================================


def validate_connectivity_config():

    while True:
        print("Validate connectivity of full network plus road, rail, water subnetworks? Y/N (Can increase run times from ~ one minute to 30+ minutes):")
        # Only allowing this tool to work on road/rail for now
        validate_connectivity = input('----------------------> ')
        print("USER INPUT: validate connectivity: {}".format(validate_connectivity))
        if validate_connectivity not in ["y", "n", "Y", "N"]:
            print("must type y or n")
            continue
        else:
            # Valid value
            break

    return validate_connectivity.lower()


# ==============================================================================


def print_summary_stats(mode_fc):

    mode = mode_fc.split('\\')[-1]

    # Row count
    count = arcpy.management.GetCount(mode_fc)

    # Total length
    length = 0
    with arcpy.da.SearchCursor(mode_fc, "Length") as cursor:
        for row in cursor:
            length += row[0]

    # Actual link (non-artifial) row count
    arcpy.MakeFeatureLayer_management(mode_fc, mode + "_lyr")
    arcpy.SelectLayerByAttribute_management(mode + "_lyr", "NEW_SELECTION",
                                            "ARTIFICIAL = 0")
    actual_count = arcpy.management.GetCount(mode + "_lyr")

    # Actual link total length
    actual_length = 0
    with arcpy.da.SearchCursor(mode + "_lyr", "Length") as cursor:
        for row in cursor:
            actual_length += row[0]

    # Artificial link row count
    arcpy.SelectLayerByAttribute_management(mode + "_lyr", "SWITCH_SELECTION")                                            
    artificial_count = arcpy.management.GetCount(mode + "_lyr")

    # Artificial link total length
    artificial_length = 0
    with arcpy.da.SearchCursor(mode + "_lyr", "Length") as cursor:
        for row in cursor:
            artificial_length += row[0]

    print('{} total links --> {} rows, {} total length'.format(mode, count, length))
    print('{} actual (non-artificial) network links --> {} rows, {} total length'.format(mode, actual_count, actual_length))
    print('{} artificial links --> {} rows, {} total length'.format(mode, artificial_count, artificial_length))

    if mode in ['rail', 'road', 'water']:
        # Breakdown by Link Type
        # First subset to non-artificial links
        arcpy.SelectLayerByAttribute_management(mode + "_lyr", "NEW_SELECTION",
                                                "ARTIFICIAL = 0")
        values = [row[0] for row in arcpy.da.SearchCursor(mode + "_lyr", ('Link_Type'))]
        # Sort and also ensure nulls don't crash the code here
        unique_values = sorted(set(values), key=lambda x: (x is None, x))
        for link_type in unique_values:
            # Link Type Row Count
            field = arcpy.ListFields(mode + "_lyr", 'Link_Type')[0]
            if link_type is None:
                # Handles nones/nulls
                arcpy.SelectLayerByAttribute_management(mode + "_lyr", "NEW_SELECTION",
                                                        "ARTIFICIAL = 0 and Link_Type is NULL")
            elif field.type == 'String':
                # Handles strings
                arcpy.SelectLayerByAttribute_management(mode + "_lyr", "NEW_SELECTION",
                                                        "ARTIFICIAL = 0 and Link_Type = '{}'".format(link_type))
            else:
                # handles numeric link type
                arcpy.SelectLayerByAttribute_management(mode + "_lyr", "NEW_SELECTION",
                                                        "ARTIFICIAL = 0 and Link_Type = {}".format(link_type))                

            link_type_count = arcpy.management.GetCount(mode + "_lyr")

            # Link Type Total Length
            link_type_length = 0
            with arcpy.da.SearchCursor(mode + "_lyr", "Length") as cursor:
                for row in cursor:
                    link_type_length += row[0]
            print('{} Link Type {} --> {} rows, {} total length'.format(mode, link_type, link_type_count, link_type_length))
            del (link_type_count, link_type_length)

    arcpy.Delete_management(mode + "_lyr")

# ==============================================================================


def network_validation():

    print("start: network_validation")
    gdb_path = get_network_path()

    validate_connectivity = validate_connectivity_config()

    # Check GDB contains feature dataset named 'network'
    # Raise error if not
    print('\nChecking GDB contains feature dataset named \'network\'.')
    feature_dataset = os.path.join(gdb_path, "network")
    if not arcpy.Exists(feature_dataset):
        error = ("The scenario network geodatabase must contain a feature dataset named 'network'. Please ensure " +
                 "that {} includes all network components inside that feature dataset\n".format(gdb_path))
        print(error)
        raise IOError(error)
    else:
        print('----> PASS')

    # Check that a meters-based coordinate system is associated with that feature dataset
    # Raise error if not
    print('\nChecking feature dataset uses a meters-based coordinate system.')
    scenario_proj = arcpy.Describe(feature_dataset).spatialReference
    print("Finding: The scenario projection utilized by the base_network_gdb is {}, ".format(scenario_proj.name) +
          "WKID {} with units of {}.".format(scenario_proj.factoryCode, scenario_proj.linearUnitName.lower()))

    if scenario_proj.linearUnitName.lower() != 'meter':
        error = ("The scenario network geodatabase must be in a meters-based coordinate system. Please ensure " +
                 "that {} is in a meters-based coordinate system\n".format(gdb_path))
        print(error)
        raise Exception(error)
    else:
        print('----> PASS')

    # Check for FTOT-permitted modes
    print('\nChecking for FTOT-enabled modes among feature classes.')
    all_modes = ['road', 'rail', 'water', 'pipeline_crude_trf_rts', 'pipeline_prod_trf_rts']
    feature_classes = []
    for mode in all_modes:
        mode_fc = os.path.join(feature_dataset, mode)
        if not arcpy.Exists(mode_fc):
            print('feature class not found: {}'.format(mode_fc))
        else:
            feature_classes.append(mode)

    print('The following feature classes were recognized: {}'.format(feature_classes))
    if len(feature_classes) == 5:
        print('Missing modes: None')
    else:
        print('Missing modes: {}'.format([m for m in all_modes if m not in feature_classes]))

    # Check for required fields in the GIS network
    # Give error if not there
    print('\nChecking for required fields.')

    for mode in feature_classes:
        missing_fields = []
        mode_fc = os.path.join(feature_dataset, mode)
        check_fields = [field.name.lower() for field in arcpy.ListFields(mode_fc)]

        for field in ["OBJECTID", "SHAPE", "Mode_Type", "Artificial"]:
            if field.lower() not in check_fields:
                missing_fields.append(field)

        if mode in ["road", "water", "rail"]:
            for field in ["Length"]:
                if field.lower() not in check_fields:
                    missing_fields.append(field)

        if mode in ["pipeline_crude_trf_rts", "pipeline_prod_trf_rts"]:
            for field in ["Base_Rate", "Tariff_ID", "Dir_Flag"]:
                if field.lower() not in check_fields:
                    missing_fields.append(field)

        if len(missing_fields) > 0:
            error = ("Feature class {} is missing the following fields: {}".format(mode_fc, missing_fields))
            print(error)
            print("Please refer to the Network Specification in the FTOT User Guide.\n")
            raise Exception(error)
        else:
            print('{} ----> PASS'.format(mode))

    # Print some summary stats
    print('\nNETWORK SUMMARY:')
    for mode in feature_classes:
        mode_fc = os.path.join(feature_dataset, mode)
        print_summary_stats(mode_fc)

    if validate_connectivity == 'y':
        network_connectivity(gdb_path)

    return


# ==============================================================================

def network_connectivity(gdb_path):

    print('\nChecking network connectivity.')

    dir_name = os.path.dirname(gdb_path)

    # Uniquely name connectivity reports based on network gdb name
    base_name = os.path.basename(os.path.normpath(gdb_path))
    base_name_clean = base_name.replace(".gdb", "")

    out_overview = os.path.join(dir_name, base_name_clean + '_connectivity_report.csv')

    with open(out_overview, 'w') as wf:
        wf.write("MODE,NUMBER_OF_GROUPS,LENGTH,NUMBER_OF_SEGMENTS\n")

    connectivity_modes = ['full_network', 'road', 'rail', 'water']
    for mode in connectivity_modes:

        out_gdb = gdb_path.replace(".gdb", "_connectivity_analysis_" + mode + ".gdb")

        if os.path.exists(out_gdb):
            rmtree(out_gdb)

        arcpy.Copy_management(gdb_path, out_gdb)

        arcpy.env.workspace = out_gdb

        feature_dataset = os.path.join(out_gdb, "network")

        mode_fc = os.path.join(feature_dataset, mode)

        # Prep a full network combined feature class for easier connectivity analysis
        if mode == 'full_network':
            list_of_modes = []
            all_modes = ['road', 'rail', 'water']
            # skipping 'pipeline_crude_trf_rts', 'pipeline_prod_trf_rts' for now as this complicates analysis dramatically
            for network_component in all_modes:
                network_component_fc = os.path.join(feature_dataset, network_component)
                if arcpy.Exists(network_component_fc):
                    list_of_modes.append(network_component_fc)
            arcpy.management.Merge(list_of_modes, os.path.join(feature_dataset, 'full_network'))

        # Delete everything but the modal layer (since we copied the whole gdb)
        # This cleans up the gdb for the actual network connectivity work
        for fc in arcpy.ListFeatureClasses(feature_dataset="network"):
            if fc != mode:
                arcpy.Delete_management(fc)
        for fc in arcpy.ListFeatureClasses():
            if fc != mode:
                arcpy.Delete_management(fc)
        for table in arcpy.ListTables():
            if table != mode:
                arcpy.Delete_management(table)

        if arcpy.Exists(mode_fc):

            network = os.path.join(feature_dataset, mode)

            # Add fields to store ID connectivity_group and segment percentage
            arcpy.AddField_management(mode_fc, "connectivity_group", "SHORT")
            arcpy.AddField_management(mode_fc, "percentage", "DOUBLE")

            print("Creating " + mode + " trace network...")
            # Create trace network
            arcpy.tn.CreateTraceNetwork(feature_dataset, "trace_nw_" + mode, None, [[mode, "SIMPLE_EDGE"]])

            print("Enabling " + mode + " topology...")
            # Note that due to arcpy defect, have to disable and then enable network topology.
            # However, this doesn't seem to be working around issue,
            # so now there are separate geodatabases for each trace network
            # See https://community.esri.com/t5/trace-network-questions/trace-network-enabling-problem/td-p/714268
            arcpy.tn.DisableNetworkTopology(os.path.join(feature_dataset, 'trace_nw_' + mode))
            arcpy.tn.EnableNetworkTopology(os.path.join(feature_dataset, 'trace_nw_' + mode), 1000000)
            print("Finished " + mode + " topology...")

            print("Processing " + mode + " connectivity...")
            # Reset connectivity group and length for each mode
            connectivity_group = 1
            length = 0

            # Search cursor to iterate through each row of the network to check connectivity
            # --------------------------------------------------------------------------------------

            # Prep for search cursor, first identifying the feature class that you will be iterating through and
            # counting # of segments in each fc (the denominator for percentages)

            in_features = os.path.join(out_gdb, mode_fc)
            count_result = arcpy.GetCount_management(in_features)
            total_segment_count = int(count_result.getOutput(0))

            print("Summing " + mode + " length...")
            # Sum length of each connectivity group
            with arcpy.da.SearchCursor(in_features, ['Length']) as scursor_length:
                for row in scursor_length:
                    if not row[0] is None:
                        length += row[0]
                    else:
                        print('null lengths encountered')

            print("Connectivity Grouping for " + mode + "...")
            # Initiate search cursor
            with arcpy.da.SearchCursor(in_features, ['SHAPE@', 'connectivity_group']) as scursor_group:
                for row in scursor_group:

                    # If no connectivity group is already assigned
                    if row[1] is None:

                        # Create blank fc for connectivity group x
                        arcpy.CreateFeatureclass_management(
                                os.path.join(out_gdb, "network"),
                                mode + "_connectivity_group_" + str(connectivity_group),
                                "Point", "", "No", "No", network)

                        # ID a point along the current feature
                        point_x = row[0].firstPoint.X
                        point_y = row[0].firstPoint.Y
                        point = (arcpy.Point(point_x, point_y))

                        # Insert that point into the new connectivity group point fc
                        with arcpy.da.InsertCursor(os.path.join(out_gdb, "network", mode + "_connectivity_group_" + str(connectivity_group)),
                                                   ['SHAPE@']) as icursor:

                            icursor.insertRow([point])
                        arcpy.Trace_tn("network/trace_nw_" + mode, "CONNECTED",
                                       os.path.join(out_gdb, "network", mode + "_connectivity_group_" + str(connectivity_group)),
                                       validate_consistency='DO_NOT_VALIDATE_CONSISTENCY',
                                       result_types=['NETWORK_LAYERS'],
                                       out_network_layer='trace_con_lyr')

                        arcpy.CalculateField_management(
                                'trace_con_lyr/' + mode,
                                "connectivity_group",
                                connectivity_group,
                                'PYTHON_9.3')

                        # Count number of segments in the connectivity group for reporting
                        result = arcpy.GetCount_management('trace_con_lyr/' + mode)
                        group_segment_count = int(result.getOutput(0))

                        # Using the total fc feature count and group feature count, calculate the percentage of segments
                        # for each connectivity group and add this value to the relevant segments in the feature class

                        pct = (group_segment_count * 1.0 / total_segment_count * 1.0) * 100
                        arcpy.CalculateField_management('trace_con_lyr/' + mode, "percentage", pct, 'PYTHON_9.3')

                        arcpy.Delete_management('trace_con_lyr')

                        # Go to next connectivity group
                        connectivity_group += 1

            # Summarize the number of connectivity groups for each mode in a csv (this is the
            # broad level overview of each mode).  Note that actual number of connectivity
            # groups will always be one less than the counter based on current script
            # --------------------------------------------------------------------------------------

            with open(out_overview, 'a') as wf:
                wf.write('{},{},{},{}\n'.format(
                    mode, connectivity_group-1, length, total_segment_count)
                    )
            # Delete the trace network so that it does not cause issues when subsequent trace networks are created...
            arcpy.Delete_management(os.path.join(feature_dataset, "trace_nw_" + mode))

    print("Connectivity Report Complete... Open {} to review the connectivity report which lists the number of connectivity groups by mode".format(dir_name))
    print("The GIS data identifying distinct connectivity groups is saved in separate geodatabases for each mode plus the full network (road, rail, and water)")
    print("Note that some disconnected portions of the network may be expected-- e.g., islands, isolated rail networks, and navigable waterways separated from other waterways by dams")
# ==============================================================================


def run():
    os.system('cls')
    print("FTOT network validation tool")
    print("-------------------------------")
    print("")
    print("")
    network_validation()
