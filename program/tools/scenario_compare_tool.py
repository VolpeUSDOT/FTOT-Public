# -------------------------------------------------------------------------------
# Name:        FTOT Scenario Compare Tool
# Purpose:     Concatenates Tableau reports and gdb contained in multiple directories.

# it is a command-line-interface (CLI) tool that prompts the user for the following information:
# - Location of Scenarios to compare
# - Location of directory in which to store scenario comparison output
# -------------------------------------------------------------------------------

import arcpy
import os
import csv
import sys
import zipfile
import datetime
import ntpath
import glob
from shutil import copy, rmtree

# ==============================================================================


def run_scenario_compare_prep_tool():
    print("FTOT scenario comparison tool")
    print("-------------------------------")
    print("")
    print("")

    scenario_compare_prep()


# ==============================================================================


def scenario_compare_prep():

    # get output directory from user:
    output_dir = get_output_dir()
    # open for user
    os.startfile(output_dir)

    # create output gdb
    print("start: create tableau gdb")
    output_gdb_name = "tableau_output"
    os.path.join(output_dir, "tableau_output")
    arcpy.CreateFileGDB_management(out_folder_path=output_dir,
                                   out_name=output_gdb_name, out_version="CURRENT")
    output_gdb = os.path.join(output_dir, output_gdb_name + ".gdb")

    # create output csv
    print("start: Tableau results report")
    report_file_name = 'tableau_report.csv'
    report_file = os.path.join(output_dir, report_file_name)
    # write the first header line
    wf = open(report_file, 'w')
    header_line = 'scenario_name, table_name, commodity, facility_name, measure, mode, value, units, notes\n'
    wf.write(header_line)

    # get user directory to search
    # returns list of dirs
    scenario_dirs = get_input_dirs()

    # for each dir, get all of the report folders matching the pattern
    for a_dir in scenario_dirs:
        print("processing: {}".format(a_dir))
        report_dirs = glob.glob(os.path.join(a_dir, "reports", \
            "tableau_report_*_*_*_*-*-*"))

        # skip empty scenarios without report folders
        if len(report_dirs) == 0:
            print("skipping: {}".format(a_dir))
            continue

        report_dir_dict = []
        for report in report_dirs:

            # skip the csv files...we just want the report folders
            if report.endswith('.csv'):
                continue

            # inspect the reports folder
            path_to, the_file_name = ntpath.split(report)

            the_date = datetime.datetime.strptime(the_file_name[14:], "_%Y_%m_%d_%H-%M-%S")

            report_dir_dict.append((the_file_name, the_date))

        # find the newest reports folder
        # sort by datetime so the most recent is first.
        report_dir_dict = sorted(report_dir_dict, key=lambda tup: tup[1], reverse=True)
        most_recent_report_file = os.path.join(path_to, report_dir_dict[0][0])

        #  - unzip twbx locally to a temp folder
        temp_folder = os.path.join(most_recent_report_file, "temp")
        with zipfile.ZipFile(os.path.join(most_recent_report_file, 'tableau_dashboard.twbx'), 'r') as zipObj:
            # Extract all the contents of zip file in current directory
            zipObj.extractall(temp_folder)

        # copy the TWB and triskelion image file from the unzipped packaged workbook to
        # the scenario compare output folder
        print("copying the template twb and  image files")
        copy_list = ["volpeTriskelion.gif", "tableau_dashboard.twb", "parameters_icon.png"]
        for a_file in copy_list:
            source_loc = os.path.join(temp_folder, a_file)
            dest_loc = os.path.join(output_dir, a_file)
            try:
                copy(source_loc, dest_loc)
            except:
                print("warning: file {} does not exists.".format(a_file))

        #  - concat tableau_report.csv
        print("time to look at the csv file and import ")
        csv_in = open(os.path.join(temp_folder,"tableau_report.csv"))
        for line in csv_in:
            if line.startswith(header_line):
                continue
            wf.write(line)
        csv_in.close()

        #  - unzip gdb.zip locally
        with zipfile.ZipFile(os.path.join(temp_folder, 'tableau_output.gdb.zip'),
                         'r') as zipObj:
            # Extract all the contents of zip file in current directory
            zipObj.extractall(os.path.join(temp_folder,'tableau_output.gdb'))

        # concat the fc into the output gdb
        input_gdb = os.path.join(temp_folder,'tableau_output.gdb')
        input_fcs = ["facilities_merge", "optimized_route_segments_dissolved"]
        # try to append the facilities_merge fc.
        # if it throws ExecuteError, the FC doesn't exist yet, so do a copy instead.
        # the copy only need to be done once to create the first fc.
        for fc in input_fcs:
            print("processing fc: {}".format(fc))
            try:
                # print("try processing fc with append: {} for scenario {}".format(fc, a_dir))
                arcpy.Append_management(os.path.join(input_gdb, fc),
                                        os.path.join(output_gdb, fc), "NO_TEST")
            except arcpy.ExecuteError:
                # print("except processing fc with copy: {} for scenario {} ".format(fc, a_dir))
                arcpy.CopyFeatures_management(os.path.join(input_gdb, fc),
                                       os.path.join(output_gdb, fc))

        # remove the temp folder
        print("cleaning up temp folder")
        rmtree(temp_folder)

    # close merged csv
    wf.close()

    # package up the concat files into a compare.twbx
    # Create the zip file for writing compressed data

    print('creating archive')
    zip_gdb_filename = output_gdb + ".zip"
    zf = zipfile.ZipFile(zip_gdb_filename, 'w', zipfile.ZIP_DEFLATED)

    zipgdb(output_gdb, zf)
    zf.close()
    print('all done zipping')

    # now delete the unzipped gdb
    arcpy.Delete_management(output_gdb)
    print("deleted unzipped gdb")

    twbx_dashboard_filename = os.path.join(output_dir, "tableau_dashboard.twbx")
    zipObj = zipfile.ZipFile(twbx_dashboard_filename, 'w', zipfile.ZIP_DEFLATED)

    # Package the workbook
    # need to specify the archive name parameter to avoid the whole path to the file being added to the archive
    file_list = ["tableau_dashboard.twb", "tableau_output.gdb.zip", "tableau_report.csv", "volpeTriskelion.gif",
                 "parameters_icon.png"]

    for a_file in file_list:

        # write the temp file to the zip
        zipObj.write(os.path.join(output_dir, a_file), a_file)

        # delete the temp file so its nice and clean.
        os.remove(os.path.join(output_dir, a_file))

    # close the Zip File
    zipObj.close()


# ==============================================================================


def get_input_dirs():
    # recursive? or user specified
    # returns list of dirs
    print("scenario comparison tool | step 2/2:")
    print("-------------------------------")
    print("Option 1: recursive directory search ")
    print("Option 2: user-specified directories ")
    user_choice = raw_input('Enter 1 or 2 or quit: >> ')
    if int(user_choice) == 1:
        return get_immediate_subdirectories()
    elif int(user_choice) == 2:
        return get_user_specified_directories()
    else:
        print("WARNING: not a valid choice! please press enter to continue.")
        raw_input("press any key to quit")


# ==============================================================================


def get_immediate_subdirectories():
    top_dir = ""
    print("enter top level directory")
    top_dir = raw_input('----------------------> ')
    return [os.path.join(top_dir, name) for name in os.listdir(top_dir)
            if os.path.isdir(os.path.join(top_dir, name))]


# ==============================================================================


def get_user_specified_directories():
    # Scenario Directories
    list_of_dirs = []
    scenario_counter = 1
    get_dirs = True
    while get_dirs:
        print("Enter Directory Location for scenario {}:".format(scenario_counter))
        print("Type 'done' if you have entered all of your scenarios")
        a_scenario = ""
        while not os.path.exists(a_scenario):
            a_scenario = raw_input('----------------------> ')
            print("USER INPUT: the scenario path: {}".format(a_scenario))
            if a_scenario.lower() == 'done':
                if scenario_counter < 2:
                    print("must define at least two scenario directories for comparison.")
                else:
                    get_dirs = False
                    break
            if not os.path.exists(a_scenario):
                print("the following path is not valid. Please enter a valid path to a scenario directory")
                print("os.path.exists == False for: {}".format(a_scenario))
            else:
                list_of_dirs.append(a_scenario)
                scenario_counter += 1
    return list_of_dirs


# ==============================================================================


def get_output_dir():
    # Scenario Comparison Output Directory
    print("scenario comparison tool | step 1/2:")
    print("-------------------------------")
    print("scenario comparison output directory: ")
    scenario_output = ""
    scenario_output = raw_input('----------------------> ')
    print("USER INPUT: the scenario output path: {}".format(scenario_output))
    if not os.path.exists(scenario_output):
        os.makedirs(scenario_output)
    return scenario_output


# ==============================================================================


def clean_file_name(value):
    deletechars = '\/:*?"<>|'
    for c in deletechars:
        value = value.replace(c, '')
    return value;

# ==============================================================================


# Function for zipping files
def zipgdb(path, zip_file):
    isdir = os.path.isdir

    # Check the contents of the workspace, if it the current
    # item is a directory, gets its contents and write them to
    # the zip file, otherwise write the current file item to the
    # zip file
    for each in os.listdir(path):
        fullname = path + "/" + each
        if not isdir(fullname):
            # If the workspace is a file geodatabase, avoid writing out lock
            # files as they are unnecessary
            if not each.endswith('.lock'):
                # Write out the file and give it a relative archive path
                try: zip_file.write(fullname, each)
                except IOError: None # Ignore any errors in writing file
        else:
            # Branch for sub-directories
            for eachfile in os.listdir(fullname):
                if not isdir(eachfile):
                    if not each.endswith('.lock'):
                        gp.AddMessage("Adding " + eachfile + " ...")
                        # Write out the file and give it a relative archive path
                        try: zip_file.write(fullname + "/" + eachfile, os.path.basename(fullname) + "/" + eachfile)
                        except IOError: None # Ignore any errors in writing file

#
# Alternative Implementation
# # ==============================================================================
#
#
# def run_scenario_comparison_tool():
#     print("FTOT scenario comparison tool")
#     print("-------------------------------")
#     print("")
#     print("")
#
#     config_params = get_user_configs()
#
#     scenario_comparison(config_params)
#
#
# # ==============================================================================
#
#
# def get_user_configs():
#     # Scenario Directories
#     list_of_dirs = []
#     scenario_counter = 1
#     get_dirs = True
#     print("scenario comparison tool | step 1/2:")
#     print("-------------------------------")
#     while get_dirs:
#         print("Enter Directory Location for scenario {}:".format(scenario_counter))
#         print("Type 'done' if you have entered all of your scenarios")
#         a_scenario = ""
#         while not os.path.exists(a_scenario):
#             a_scenario = raw_input('----------------------> ')
#             print("USER INPUT: the scenario path: {}".format(a_scenario))
#             if a_scenario.lower() == 'done':
#                 if scenario_counter < 2:
#                     print("must define at least two scenario directories for comparison.")
#                 else:
#                     get_dirs = False
#                     break
#             if not os.path.exists(a_scenario):
#                 print("the following path is not valid. Please enter a valid path to a scenario directory")
#                 print("os.path.exists == False for: {}".format(a_scenario))
#             else:
#                 list_of_dirs.append(a_scenario)
#                 scenario_counter += 1
#
#     # Scenario Comparison Output Directory
#     print("scenario comparison tool | step 2/2:")
#     print("-------------------------------")
#     print("scenario comparison output directory: ")
#     scenario_output = ""
#     while not os.path.exists(scenario_output):
#         scenario_output = raw_input('----------------------> ')
#         print("USER INPUT: the scenario output path: {}".format(scenario_output))
#         if not os.path.exists(scenario_output):
#             print("the following path is not valid. Please enter a valid path to a scenario output directory")
#             print("os.path.exists == False for: {}".format(scenario_output))
#
#     return [list_of_dirs, scenario_output]
#
#
# # ======================================================================================================================
# def scenario_comparison(config_params):
#     # SETUP
#     # -------------------------------------------------------------------------------
#
#     # unpack the config parameters
#     list_of_dirs, output_dir = config_params
#
#     comp_gdb = 'comparison.gdb'
#
#     full_path_to_gdb = os.path.join(output_dir, comp_gdb)
#
#     if arcpy.Exists(full_path_to_gdb):
#         arcpy.Delete_management(full_path_to_gdb)
#         print 'Deleted existing ' + full_path_to_gdb
#
#     arcpy.CreateFileGDB_management(output_dir, comp_gdb)
#
#     arcpy.env.workspace = full_path_to_gdb
#
#     # MAIN
#     # -------------------------------------------------------------------------------
#
#     # Prepare the data
#     print "Preparing Data ..."
#
#     merge_list = []
#
#     for dir in list_of_dirs:
#         # TODO: GET SCENARIO NAME-- for now getting base_dir name but need to read xml in future
#         scenario_name = os.path.basename(dir)
#         arcpy.CopyFeatures_management(os.path.join(dir, "main.gdb", "optimized_route_segments"),
#                                       "optimized_route_segments_" + scenario_name)
#         merge_list.append("optimized_route_segments_" + scenario_name)
#
#         # Add field to store scenario name
#         arcpy.AddField_management("optimized_route_segments_" + scenario_name, "scenario_name", "TEXT")
#         arcpy.CalculateField_management("optimized_route_segments_" + scenario_name, "scenario_name",
#                                         '"' + scenario_name + '"', "PYTHON_9.3")
#
#     print "Merge Data ..."
#
#     arcpy.Merge_management(merge_list, "optimized_route_segments_all")
#
#     print "Finalize Data ..."
#
#     # Export actual flows to csv for Tableau to read easily. Gdb is simply for storing geometries of features
#     csv_out = os.path.join(output_dir, "optimized_route_segments_all_flows.csv")
#     fields = [x.name for x in arcpy.ListFields("optimized_route_segments_all") if x.name in
#               ["NET_SOURCE_NAME", "NET_SOURCE_OID", "TIME_PERIOD", "COMMODITY", "COMMODITY_FLOW", "UNITS",
#                "scenario_name"]]
#
#     counter = 0
#
#     # For Python 2.7
#     if sys.version_info[0] < 3:
#         with open(csv_out, "wb") as f:
#             wr = csv.writer(f)
#             wr.writerow(fields)
#             with arcpy.da.SearchCursor("optimized_route_segments_all", fields) as cursor:
#                 for row in cursor:
#                     counter += 1
#                     wr.writerow(row)
#
#     # For forward compatibility with Python 3
#     else:
#         with open(csv_out, "w", newline='') as f:
#             wr = csv.writer(f)
#             wr.writerow(fields)
#             with arcpy.da.SearchCursor("optimized_route_segments_all", fields) as cursor:
#                 for row in cursor:
#                     counter += 1
#                     wr.writerow(row)
#
#     fields = ["NET_SOURCE_NAME", "NET_SOURCE_OID"]
#     arcpy.DeleteIdentical_management("optimized_route_segments_all", fields)
#
#     # Delete gdb fields that are not needed
#     drop_fields = [x.name for x in arcpy.ListFields("optimized_route_segments_all") if x.name not in
#                    ["NET_SOURCE_NAME", "NET_SOURCE_OID", "Shape", "OBJECTID", "Shape_Length"]]
#     arcpy.DeleteField_management("optimized_route_segments_all", drop_fields)
#
