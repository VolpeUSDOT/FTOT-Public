# -------------------------------------------------------------------------------
# Name:        Network Validation Tool
# Purpose:     Confirms a user-provided network meets basic requirements and prints issues
# -------------------------------------------------------------------------------

import arcpy
import os
from six.moves import input
from pathlib import Path
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


def create_temp_shps(gdb_path, temp_dir):

    # export network and locations fc's to shapefiles

    # if temp directory already exists, ask user if ok to remove
    if os.path.exists(temp_dir):
        print('temp directory already exists: {}'.format(temp_dir))
        print('do you want to delete the temp directory and proceed?')
        choice = input(">> ")
        if choice.lower() == 'y' or choice.lower() == 'yes':
            pass
        elif choice.lower() == 'n' or choice.lower() == 'no':
            return
        print("deleting temp directory")
        rmtree(temp_dir)

    # make temporary directory
    if not os.path.exists(temp_dir):
        print("creating temp shapefile directory")
        os.makedirs(temp_dir)

    # get the network feature layers and export as shapefile
    print('beginning shapefile export. this may take a few minutes...')
    for mode in ['road', 'rail', 'water', 'pipeline_crude', 'pipeline_prod']:
        fc = os.path.join(gdb_path, 'network', mode)
        print('checking for mode: {}'.format(mode))
        try:
            arcpy.FeatureClassToShapefile_conversion(Input_Features=fc, Output_Folder=temp_dir)
        except:
            print('no feature class: {}'.format(fc))
            continue


# ==============================================================================


def print_summary_stats(mode_fc):
    
    mode = mode_fc.split('\\')[-1]

    # Row count
    count = arcpy.management.GetCount(mode_fc)

    # Total length
    summed_total = 0
    with arcpy.da.SearchCursor(mode_fc, "Length") as cursor:
        for row in cursor:
            summed_total = summed_total + row[0]
    
    print('{} --> {} rows, {} total length'.format(mode, count, summed_total))


# ==============================================================================


def network_validation():

    print("start: network_validation")
    gdb_path = get_network_path()

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
            error = ("Feature class {} is missing the following fields: {}".format(mode_fc, field))
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

    return


# ==============================================================================


def run():
    os.system('cls')
    print("FTOT network validation tool")
    print("-------------------------------")
    print("")
    print("")
    network_validation()

