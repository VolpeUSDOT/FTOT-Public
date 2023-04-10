# -------------------------------------------------------------------------------
# Name:        FTOT Scenario Compare Tool
# Purpose:     Concatenates Tableau reports and gdb contained in multiple directories.

# It is a command-line-interface (CLI) tool that prompts the user for the following information:
# - Location of scenarios to compare
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
from six.moves import input

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

    # create output report csv
    print("start: Tableau results report")
    report_file_name = 'tableau_report.csv'
    report_file = os.path.join(output_dir, report_file_name)
    wf = open(report_file, 'w')
    header_line = 'scenario_name, table_name, commodity, facility_name, measure, mode, value, units, notes\n'
    wf.write(header_line)

    # create output all_routes csv
    routes_file_name = 'all_routes.csv'
    routes_file = os.path.join(output_dir, routes_file_name)
    rf = open(routes_file, 'w')
    routes_header = 'scenario_name,route_id,from_facility,from_facility_type,to_facility,to_facility_type,commodity_name,phase,mode,transport_cost,routing_cost,length,in_solution\n'
    rf.write(routes_header)
    isEmpty=True # track if anything written to all_routes

    # get user directory to search
    # returns list of dirs
    scenario_dirs = get_input_dirs()

    # for each dir, get all of the report folders matching the pattern
    for a_dir in scenario_dirs:
        print("processing: {}".format(a_dir))
        report_dirs = glob.glob(os.path.join(a_dir, "reports", \
            "reports_*_*_*_*-*-*"))

        # skip empty scenarios without report folders
        if len(report_dirs) == 0:
            print("skipping: {}".format(a_dir))
            continue

        report_dir_dict = []
        for report in report_dirs:

            # skip the csv files... we just want the report folders
            if report.endswith('.csv'):
                continue

            # inspect the reports folder
            path_to, the_file_name = ntpath.split(report)

            the_date = datetime.datetime.strptime(the_file_name[7:], "_%Y_%m_%d_%H-%M-%S")

            report_dir_dict.append((the_file_name, the_date))

        # find the newest reports folder
        # sort by datetime so the most recent is first
        report_dir_dict = sorted(report_dir_dict, key=lambda tup: tup[1], reverse=True)
        most_recent_report_file = os.path.join(path_to, report_dir_dict[0][0])

        # unzip twbx locally to a temp folder
        temp_folder = os.path.join(most_recent_report_file, "temp")
        with zipfile.ZipFile(os.path.join(most_recent_report_file, 'tableau_dashboard.twbx'), 'r') as zipObj:
            # extract all the contents of zip file in current directory
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

        # concat tableau_report.csv
        print("time to look at the csv file and import ")
        csv_in = open(os.path.join(temp_folder,"tableau_report.csv"))
        for line in csv_in:
            if line.startswith(header_line):
                continue
            wf.write(line)
        csv_in.close()

        # concat all_routes.csv
        print("look at routes csv and import")
        rf_in = open(os.path.join(temp_folder,"all_routes.csv"))
        for line in rf_in:
            if line == 'Run scenario with NDR on to view this dashboard.':
                continue # NDR off. No content.
            elif line.startswith(routes_header):
                continue
            else:
                rf.write(line)
                isEmpty = False
        rf_in.close()

         # unzip gdb.zip locally
        with zipfile.ZipFile(os.path.join(temp_folder, 'tableau_output.gdb.zip'),
                         'r') as zipObj:
            # extract all the contents of zip file in current directory
            zipObj.extractall(os.path.join(temp_folder,'tableau_output.gdb'))

        # concat the fc into the output gdb
        input_gdb = os.path.join(temp_folder,'tableau_output.gdb')
        input_fcs = ["facilities_merge", "optimized_route_segments_dissolved", "optimized_route_segments"]
        # try to append the facilities_merge fc
        # if it throws ExecuteError, the FC doesn't exist yet, so do a copy instead
        # the copy only need to be done once to create the first fc
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

    # add text line to routes report if empty, then close
    if isEmpty:
        line = 'Run scenario with NDR on to view this dashboard.'
        rf.write(line)
    rf.close()

    # package up the concat files into a compare.twbx
    # create the zip file for writing compressed data

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

    # package the workbook
    # need to specify the archive name parameter to avoid the whole path to the file being added to the archive
    file_list = ["tableau_dashboard.twb", "tableau_output.gdb.zip", "tableau_report.csv", "all_routes.csv",
                 "volpeTriskelion.gif", "parameters_icon.png"]

    for a_file in file_list:

        # write the temp file to the zip
        zipObj.write(os.path.join(output_dir, a_file), a_file)

        # delete the temp file so its nice and clean
        os.remove(os.path.join(output_dir, a_file))

    # close the zip file
    zipObj.close()


# ==============================================================================

def get_input_dirs():
    # recursive or user specified
    # returns list of dirs
    print("scenario comparison tool | step 2/2:")
    print("-------------------------------")
    print("Option 1: recursive directory search ")
    print("Option 2: user-specified directories ")
    user_choice = input('Enter 1 or 2 or quit: >> ')
    if int(user_choice) == 1:
        return get_immediate_subdirectories()
    elif int(user_choice) == 2:
        return get_user_specified_directories()
    else:
        print("WARNING: not a valid choice! please press enter to continue.")
        input("press any key to quit")


# ==============================================================================

def get_immediate_subdirectories():
    top_dir = ""
    print("enter top level directory")
    top_dir = input('----------------------> ')
    return [os.path.join(top_dir, name) for name in os.listdir(top_dir)
            if os.path.isdir(os.path.join(top_dir, name))]


# ==============================================================================

def get_user_specified_directories():
    # scenario directories
    list_of_dirs = []
    scenario_counter = 1
    get_dirs = True
    while get_dirs:
        print("Enter Directory Location for scenario {}:".format(scenario_counter))
        print("Type 'done' if you have entered all of your scenarios")
        a_scenario = ""
        while not os.path.exists(a_scenario):
            a_scenario = input('----------------------> ')
            print("USER INPUT: the scenario path: {}".format(a_scenario))
            if a_scenario.lower() == 'done':
                if scenario_counter < 2:
                    print("Must define at least two scenario directories for comparison.")
                else:
                    get_dirs = False
                    break
            if not os.path.exists(a_scenario):
                print("The following path is not valid. Please enter a valid path to a scenario directory.")
                print("os.path.exists == False for: {}".format(a_scenario))
            else:
                list_of_dirs.append(a_scenario)
                scenario_counter += 1
    return list_of_dirs


# ==============================================================================

def get_output_dir():
    # scenario comparison output directory
    print("scenario comparison tool | step 1/2:")
    print("-------------------------------")
    print("scenario comparison output directory: ")
    scenario_output = ""
    scenario_output = input('----------------------> ')
    print("USER INPUT: the scenario output path: {}".format(scenario_output))
    if not os.path.exists(scenario_output):
        os.makedirs(scenario_output)
    return scenario_output


# ==============================================================================

def clean_file_name(value):
    deletechars = '\/:*?"<>|'
    for c in deletechars:
        value = value.replace(c, '')
    return value


# ==============================================================================

# function for zipping files
def zipgdb(path, zip_file):
    isdir = os.path.isdir

    # check the contents of the workspace, if it the current
    # item is a directory, gets its contents and write them to
    # the zip file, otherwise write the current file item to the
    # zip file
    for each in os.listdir(path):
        fullname = path + "/" + each
        if not isdir(fullname):
            # if the workspace is a file geodatabase, avoid writing out lock
            # files as they are unnecessary
            if not each.endswith('.lock'):
                # write out the file and give it a relative archive path
                try:
                    zip_file.write(fullname, each)
                except IOError:
                    None  # ignore any errors in writing file
        else:
            # branch for sub-directories
            for eachfile in os.listdir(fullname):
                if not isdir(eachfile):
                    if not each.endswith('.lock'):
                        gp.AddMessage("Adding " + eachfile + " ...")
                        # write out the file and give it a relative archive path
                        try:
                            zip_file.write(fullname + "/" + eachfile, os.path.basename(fullname) + "/" + eachfile)
                        except IOError:
                            None  # ignore any errors in writing file

