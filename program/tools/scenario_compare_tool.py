#-------------------------------------------------------------------------------
# Name:        FTOT Scenario Compare Tool
# Purpose:     To compare scenario-wide optimal flows from two distinct scenarios.

# it is a command-line-interface (CLI) tool that prompts the user for the following information:
# - Location of Scenario 1 to compare
# - Location of Scenario 2 to compare
# - Location of directory in which to store scenario comparison output
# -------------------------------------------------------------------------------

import arcpy
import os


# ==============================================================================
def run_scenario_comparison_tool():
    print("FTOT scenario comparison tool")
    print("-------------------------------")
    print("")
    print("")

    config_params = get_user_configs()

    scenario_comparison(config_params)


# ==============================================================================
def get_user_configs():

    # Scenario 1 Directory
    print("scenario comparison tool | step 1/3:")
    print("-------------------------------")
    print("Scenario 1 Directory Location: ")
    scenario_1 = ""
    while not os.path.exists(scenario_1):
        scenario_1 = raw_input('----------------------> ')
        print("USER INPUT: the scenario 1 path: {}".format(scenario_1))
        if not os.path.exists(scenario_1):
            print("the following path is not valid. Please enter a valid path to a scenario directory")
            print("os.path.exists == False for: {}".format(scenario_1))

    # Scenario 2 Directory
    print("scenario comparison tool | step 2/3:")
    print("-------------------------------")
    print("Scenario 2 Directory Location: ")
    scenario_2 = ""
    while not os.path.exists(scenario_2):
        scenario_2 = raw_input('----------------------> ')
        print("USER INPUT: the scenario 2 path: {}".format(scenario_2))
        if not os.path.exists(scenario_2):
            print("the following path is not valid. Please enter a valid path to a scenario directory")
            print("os.path.exists == False for: {}".format(scenario_2))

    # Scenario Comparison Output Directory
    print("scenario comparison tool | step 3/3:")
    print("-------------------------------")
    print("scenario comparison output directory: ")
    scenario_output= ""
    while not os.path.exists(scenario_output):
        scenario_output = raw_input('----------------------> ')
        print("USER INPUT: the scenario output path: {}".format(scenario_output))
        if not os.path.exists(scenario_output):
            print("the following path is not valid. Please enter a valid path to a scenario output directory")
            print("os.path.exists == False for: {}".format(scenario_output))

    return [scenario_1, scenario_2, scenario_output]


# ======================================================================================================================
def scenario_comparison(config_params):

    # SETUP
    # -------------------------------------------------------------------------------

    # unpack the config parameters
    scenario_1_dir, scenario_2_dir, output_dir = config_params

    comp_gdb = 'comparison.gdb'

    full_path_to_gdb = os.path.join(output_dir, comp_gdb)

    if arcpy.Exists(full_path_to_gdb):
        arcpy.Delete_management(full_path_to_gdb)
        print 'Deleted existing ' + full_path_to_gdb

    arcpy.CreateFileGDB_management(output_dir, comp_gdb)

    arcpy.env.workspace = full_path_to_gdb

    # MAIN
    # -------------------------------------------------------------------------------

    # Prepare the data
    print "Preparing Data ..."

    # Copy result feature classes to compare
    arcpy.CopyFeatures_management(os.path.join(scenario_1_dir, "main.gdb", "optimized_route_segments_dissolved"),
                                  "optimized_route_segments_scenario_1")
    arcpy.CopyFeatures_management(os.path.join(scenario_2_dir, "main.gdb", "optimized_route_segments_dissolved"),
                                  "optimized_route_segments_scenario_2")

    arcpy.AlterField_management("optimized_route_segments_scenario_1", 'NET_SOURCE_NAME', 'NET_SOURCE_NAME_1',
                                'NET_SOURCE_NAME_1')
    arcpy.AlterField_management("optimized_route_segments_scenario_1", 'ARTIFICIAL', 'ARTIFICIAL_1', 'ARTIFICIAL_1')
    arcpy.AlterField_management("optimized_route_segments_scenario_1", 'SUM_COMMODITY_FLOW', 'SUM_COMMODITY_FLOW_1',
                                'SUM_COMMODITY_FLOW_1')
    arcpy.AlterField_management("optimized_route_segments_scenario_1", 'PHASE_OF_MATTER', 'PHASE_OF_MATTER_1',
                                'PHASE_OF_MATTER_1')
    arcpy.AlterField_management("optimized_route_segments_scenario_1", 'UNITS', 'UNITS_1', 'UNITS_1')
    arcpy.AlterField_management("optimized_route_segments_scenario_2", 'NET_SOURCE_NAME', 'NET_SOURCE_NAME_2',
                                'NET_SOURCE_NAME_2')
    arcpy.AlterField_management("optimized_route_segments_scenario_2", 'ARTIFICIAL', 'ARTIFICIAL_2', 'ARTIFICIAL_2')
    arcpy.AlterField_management("optimized_route_segments_scenario_2", 'SUM_COMMODITY_FLOW', 'SUM_COMMODITY_FLOW_2',
                                'SUM_COMMODITY_FLOW_2')
    arcpy.AlterField_management("optimized_route_segments_scenario_2", 'PHASE_OF_MATTER', 'PHASE_OF_MATTER_2',
                                'PHASE_OF_MATTER_2')
    arcpy.AlterField_management("optimized_route_segments_scenario_2", 'UNITS', 'UNITS_2', 'UNITS_2')

    print "Comparing Scenarios ..."
    # Find segments that appear in both scenario-- change of flow is Scenario 1- Scenario 2
    arcpy.Intersect_analysis(["optimized_route_segments_scenario_1", "optimized_route_segments_scenario_2"],
                             "optimized_route_segments_intersect", "ALL", "", "")
    arcpy.AddField_management("optimized_route_segments_intersect", 'Change_Commodity_Flow', 'Double')
    arcpy.CalculateField_management("optimized_route_segments_intersect", "Change_Commodity_Flow",
                                    "!SUM_COMMODITY_FLOW_2! - !SUM_COMMODITY_FLOW_1!", 'PYTHON_9.3')

    # Find segments that only appear in Scenario 1-- change of flow is inverse of the Scenario 1 Flow
    arcpy.Erase_analysis("optimized_route_segments_scenario_1", "optimized_route_segments_scenario_2",
                         "optimized_route_segments_scenario_1_only")
    arcpy.AddField_management("optimized_route_segments_scenario_1_only", 'Change_Commodity_Flow', 'Double')
    arcpy.CalculateField_management("optimized_route_segments_scenario_1_only", "Change_Commodity_Flow",
                                    "!SUM_COMMODITY_FLOW_1! * -1", 'PYTHON_9.3')

    # Find segments that only appear in Scenario 2-- change of flow is same as Scenario 2 Flow
    arcpy.Erase_analysis("optimized_route_segments_scenario_2", "optimized_route_segments_scenario_1",
                         "optimized_route_segments_scenario_2_only")
    arcpy.AddField_management("optimized_route_segments_scenario_2_only", 'Change_Commodity_Flow', 'Double')
    arcpy.CalculateField_management("optimized_route_segments_scenario_2_only", "Change_Commodity_Flow",
                                    "!SUM_COMMODITY_FLOW_2!", 'PYTHON_9.3')

    print "Finalizing Comparison ..."
    arcpy.Merge_management(["optimized_route_segments_intersect", "optimized_route_segments_scenario_1_only",
                            "optimized_route_segments_scenario_2_only"], "optimized_route_segments_compare")

    arcpy.AddField_management("optimized_route_segments_compare", 'NET_SOURCE_NAME', 'Text')
    arcpy.CalculateField_management("optimized_route_segments_compare", "NET_SOURCE_NAME", "!NET_SOURCE_NAME_1!",
                                    'PYTHON_9.3')
    arcpy.MakeFeatureLayer_management("optimized_route_segments_compare", "optimized_route_segments_compare_lyr")
    arcpy.SelectLayerByAttribute_management("optimized_route_segments_compare_lyr", "NEW_SELECTION",
                                            "NET_SOURCE_NAME is NULL")
    arcpy.CalculateField_management("optimized_route_segments_compare_lyr", "NET_SOURCE_NAME", "!NET_SOURCE_NAME_2!",
                                    'PYTHON_9.3')

    arcpy.AddField_management("optimized_route_segments_compare", 'UNITS', 'Text')
    arcpy.CalculateField_management("optimized_route_segments_compare", "UNITS", "!UNITS_1!", 'PYTHON_9.3')
    arcpy.SelectLayerByAttribute_management("optimized_route_segments_compare_lyr", "NEW_SELECTION", "UNITS is NULL")
    arcpy.CalculateField_management("optimized_route_segments_compare_lyr", "UNITS", "!UNITS_2!", 'PYTHON_9.3')

    # Add Unique ID field
    arcpy.AddField_management("optimized_route_segments_compare", 'ID', 'Long')

    codeblock = '''
rec=0 
def autoIncrement(): 
    global rec 
    pStart = 1 
    pInterval = 1 
    if (rec == 0): 
        rec = pStart
    else:
        rec += pInterval
    return rec
'''

    arcpy.CalculateField_management("optimized_route_segments_compare", "ID", "autoIncrement()", "PYTHON_9.3", codeblock)


