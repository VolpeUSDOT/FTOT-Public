# ---------------------------------------------------------------------------------------------------
# Name: ftot_report
#
# Purpose: parses the most current log file for each step and assembles  two reports.
# the first is a human readable report that groups log messages by runtime, result, config, and warnings.
# The second report is formatted for FTOT tableau dashboard as a simple CSV and comes from a few tables in the db.
#
# ---------------------------------------------------------------------------------------------------

import os
import datetime
import glob
import ntpath
import zipfile
import sqlite3
import csv
from ftot_supporting_gis import zipgdb
from ftot_supporting import clean_file_name
from shutil import copy

from ftot import FTOT_VERSION

TIMESTAMP = datetime.datetime.now()

# ==================================================================


def prepare_tableau_assets(timestamp_directory, report_file, the_scenario, logger):
    logger.info("start: prepare_tableau_assets")

    # make tableau_output.gdb file
    # -----------------------------------
    logger.debug("start: create tableau gdb")
    import arcpy
    arcpy.CreateFileGDB_management(out_folder_path=timestamp_directory,
                                   out_name="tableau_output", out_version="CURRENT")

    # add Dataset field (raw_material_producers, processors, ultimate_destinations)
    arcpy.AddField_management(the_scenario.rmp_fc, "Dataset", "TEXT")
    arcpy.CalculateField_management(in_table=the_scenario.rmp_fc, field="Dataset",
                                    expression='"raw_material_producers"',
                                    expression_type="PYTHON_9.3", code_block="")
    arcpy.AddField_management(the_scenario.processors_fc, "Dataset", "TEXT")
    arcpy.CalculateField_management(in_table=the_scenario.processors_fc, field="Dataset",
                                    expression='"processors"',
                                    expression_type="PYTHON_9.3", code_block="")
    arcpy.AddField_management(the_scenario.destinations_fc, "Dataset", "TEXT")
    arcpy.CalculateField_management(in_table=the_scenario.destinations_fc, field="Dataset",
                                    expression='"ultimate_destinations"',
                                    expression_type="PYTHON_9.3", code_block="")

    # merge facility type FCs
    facilities_merge_fc = os.path.join(timestamp_directory, "tableau_output.gdb", "facilities_merge")
    arcpy.Merge_management([the_scenario.destinations_fc, the_scenario.rmp_fc, the_scenario.processors_fc],
                           facilities_merge_fc)

    # add the scenario_name in the facilities_merge
    arcpy.AddField_management(facilities_merge_fc,
                               "Scenario_Name",
                               "TEXT")
    arcpy.CalculateField_management(in_table=facilities_merge_fc, field="Scenario_Name",
                                    expression='"{}".format(the_scenario.scenario_name)',
                                    expression_type="PYTHON_9.3", code_block="")



    # copy optimized_route_segments_disolved (aka: ORSD)
    output_ORSD = os.path.join(timestamp_directory, "tableau_output.gdb", "optimized_route_segments_dissolved")
    arcpy.Copy_management(
        in_data=os.path.join(the_scenario.main_gdb, "optimized_route_segments_dissolved"),
        out_data=output_ORSD,
        data_type="FeatureClass")
    # add field "record_id"
    arcpy.AddField_management(output_ORSD, "record_id", "TEXT")

    # field calculator; netsource + netsource_oid for unique field
    arcpy.CalculateField_management(in_table=output_ORSD, field="record_id",
                                    expression='"{}_{} ".format(!OBJECTID!, !NET_SOURCE_NAME!)',
                                    expression_type="PYTHON_9.3", code_block="")

    # add the scenario_name in the optimized_route_segments_dissolved_fc (output_ORSD)
    arcpy.AddField_management(output_ORSD,
                               "Scenario_Name",
                               "TEXT")
    arcpy.CalculateField_management(in_table=output_ORSD, field="Scenario_Name",
                                    expression='"{}".format(the_scenario.scenario_name)',
                                    expression_type="PYTHON_9.3", code_block="")

    # copy optimized_route_segments (ORS)
    # this contains commodity info at the link level
    output_ORS = os.path.join(timestamp_directory, "tableau_output.gdb", "optimized_route_segments")
    arcpy.Copy_management(
        in_data=os.path.join(the_scenario.main_gdb, "optimized_route_segments"),
        out_data=output_ORS,
        data_type="FeatureClass")
    # add field "record_id"
    arcpy.AddField_management(output_ORS, "record_id", "TEXT")

    # field calculator; netsource + netsource_oid for unique field
    arcpy.CalculateField_management(in_table=output_ORS, field="record_id",
                                    expression='"{}_{} ".format(!OBJECTID!, !NET_SOURCE_NAME!)',
                                    expression_type="PYTHON_9.3", code_block="")

    # add the scenario_name in the optimized_route_segments_fc (output_ORS)
    arcpy.AddField_management(output_ORS,
                               "Scenario_Name",
                               "TEXT")
    arcpy.CalculateField_management(in_table=output_ORS, field="Scenario_Name",
                                    expression='"{}".format(the_scenario.scenario_name)',
                                    expression_type="PYTHON_9.3", code_block="")

    # Create the zip file for writing compressed data

    logger.debug('creating archive')
    gdb_filename = os.path.join(timestamp_directory, "tableau_output.gdb")
    zip_gdb_filename = gdb_filename + ".zip"
    zf = zipfile.ZipFile(zip_gdb_filename, 'w', zipfile.ZIP_DEFLATED)

    zipgdb(gdb_filename, zf)
    zf.close()
    logger.debug('all done zipping')

    # now delete the unzipped gdb
    arcpy.Delete_management(gdb_filename)
    logger.debug("deleted unzipped {} gdb".format("tableau_output.gdb"))

    # copy the relative path tableau TWB file from the common data director to
    # the timestamped tableau report directory
    logger.debug("copying the twb file from common data to the timestamped tableau report folder.")
    root_twb_location = os.path.join(the_scenario.common_data_folder, "tableau_dashboard.twb")
    root_graphic_location = os.path.join(the_scenario.common_data_folder, "volpeTriskelion.gif")
    root_config_parameters_graphic_location = os.path.join(the_scenario.common_data_folder, "parameters_icon.png")
    scenario_twb_location = os.path.join(timestamp_directory, "tableau_dashboard.twb")
    scenario_graphic_location = os.path.join(timestamp_directory, "volpeTriskelion.gif")
    scenario_config_parameters_graphic_location = os.path.join(timestamp_directory, "parameters_icon.png")
    copy(root_twb_location, scenario_twb_location)
    copy(root_graphic_location, scenario_graphic_location)
    copy(root_config_parameters_graphic_location, scenario_config_parameters_graphic_location)

    # copy tableau report to the assets location
    latest_generic_path = os.path.join(timestamp_directory, "tableau_report.csv")
    logger.debug("copying the latest tableau report csv file to the timestamped tableau report directory")
    copy(report_file, latest_generic_path)

    # create packaged workbook for tableau reader compatibility
    twbx_dashboard_filename = os.path.join(timestamp_directory, "tableau_dashboard.twbx")
    zipObj = zipfile.ZipFile(twbx_dashboard_filename, 'w', zipfile.ZIP_DEFLATED)

    # Add multiple files to the zip
    # need to specify the arcname parameter to avoid the whole path to the file being added to the archive
    zipObj.write(os.path.join(timestamp_directory, "tableau_dashboard.twb"), "tableau_dashboard.twb")
    zipObj.write(os.path.join(timestamp_directory, "tableau_report.csv"), "tableau_report.csv")
    zipObj.write(os.path.join(timestamp_directory, "volpeTriskelion.gif"), "volpeTriskelion.gif")
    zipObj.write(os.path.join(timestamp_directory, "parameters_icon.png"), "parameters_icon.png")
    zipObj.write(os.path.join(timestamp_directory, "tableau_output.gdb.zip"), "tableau_output.gdb.zip")

    # close the Zip File
    zipObj.close()

    # delete the other four files so its nice an clean.
    os.remove(os.path.join(timestamp_directory, "tableau_dashboard.twb"))
    os.remove(os.path.join(timestamp_directory, "tableau_report.csv"))
    os.remove(os.path.join(timestamp_directory, "volpeTriskelion.gif"))
    os.remove(os.path.join(timestamp_directory, "parameters_icon.png"))
    os.remove(os.path.join(timestamp_directory, "tableau_output.gdb.zip"))


# ==============================================================================================

def generate_edges_from_routes_summary(timestamp_directory, the_scenario, logger):

    logger.info("start: generate_edges_from_routes_summary")
    report_file_name = 'optimal_routes_' + TIMESTAMP.strftime("%Y_%m_%d_%H-%M-%S") + ".csv"
    report_file_name = clean_file_name(report_file_name)
    report_file = os.path.join(timestamp_directory, report_file_name)
    
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        summary_route_data = main_db_con.execute("""select rr.route_id, f1.facility_name as from_facility, 
                            f2.facility_name as to_facility, rr.phase_of_matter, rr.dollar_cost, rr.cost, rr.miles 
                            FROM route_reference rr 
                            join facilities f1 on rr.from_facility_id = f1.facility_id
                            join facilities f2 on rr.to_facility_id = f2.facility_id; """)
        
        # Print route data to file in debug folder
        with open(report_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['route_id','from_facility','to_facility','phase','dollar_cost','routing_cost','miles'])
            writer.writerows(summary_route_data)


# ==============================================================================================

def generate_artificial_link_summary(timestamp_directory, the_scenario, logger):
    
    logger.info("start: generate_artificial_link_summary")
    report_file_name = 'artificial_links_' + TIMESTAMP.strftime("%Y_%m_%d_%H-%M-%S") + ".csv"
    report_file_name = clean_file_name(report_file_name)
    report_file = os.path.join(timestamp_directory, report_file_name)

    # query the facility and network tables for artificial links and report out the results
    with sqlite3.connect(the_scenario.main_db) as db_con:
        sql = """select fac.facility_name, fti.facility_type, ne.mode_source, round(ne.miles, 3) as miles
                 from facilities fac
                 left join facility_type_id fti on fac.facility_type_id = fti.facility_type_id
                 left join networkx_nodes nn on fac.location_id = nn.location_id
                 left join networkx_edges ne on nn.node_id = ne.from_node_id
                 where nn.location_1 like '%OUT%'
                 and ne.artificial = 1
                 ;"""
        db_cur = db_con.execute(sql)
        db_data = db_cur.fetchall()

        artificial_links = {}
        for row in db_data:
            facility_name = row[0]
            facility_type = row[1]
            mode_source = row[2]
            link_length = row[3]

            if facility_name not in artificial_links:
                # add new facility to dictionary and start list of artificial links by mode
                artificial_links[facility_name] = {'fac_type': facility_type, 'link_lengths': {}}

            if mode_source not in artificial_links[facility_name]['link_lengths']:
                artificial_links[facility_name]['link_lengths'][mode_source] = link_length
            else:
                # there should only be one artificial link for a facility for each mode
                error = "Multiple artificial links should not be found for a single facility for a particular mode."
                logger.error(error)
                raise Exception(error)

    # create structure for artificial link csv table
    output_table = {'facility_name': [], 'facility_type': []}
    for permitted_mode in the_scenario.permittedModes:
        output_table[permitted_mode] = []

    # iterate through every facility, add a row to csv table
    for k in artificial_links:
        output_table['facility_name'].append(k)
        output_table['facility_type'].append(artificial_links[k]['fac_type'])
        for permitted_mode in the_scenario.permittedModes:
            if permitted_mode in artificial_links[k]['link_lengths']:
                output_table[permitted_mode].append(artificial_links[k]['link_lengths'][permitted_mode])
            else:
                output_table[permitted_mode].append('NA')

    # print artificial link data for each facility to file in debug folder
    with open(report_file, 'w', newline='') as f:
        writer = csv.writer(f)
        output_fields = ['facility_name', 'facility_type'] + the_scenario.permittedModes
        writer.writerow(output_fields)
        writer.writerows(zip(*[output_table[key] for key in output_fields]))

    logger.debug("finish: generate_artificial_link_summary")

# ==============================================================================================

def generate_detailed_emissions_summary(timestamp_directory, the_scenario, logger):
    
    logger.info("start: generate_detailed_emissions_summary")
    report_file_name = 'detailed_emissions_' + TIMESTAMP.strftime("%Y_%m_%d_%H-%M-%S") + ".csv"
    report_file_name = clean_file_name(report_file_name)
    report_file = os.path.join(timestamp_directory, report_file_name)

    # query the emissions tables and report out the results
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        emissions_data = main_db_con.execute("select * from detailed_emissions;")
        emissions_data = emissions_data.fetchall()
        
        # print emissions data to new report file
        with open(report_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['commodity','mode','pollutant','value','units'])
            writer.writerows(emissions_data)
    
    logger.debug("finish: generate_detailed_emissions_summary")

# ==================================================================


def generate_reports(the_scenario, logger):
    logger.info("start: parse log operation for reports")
    
    from ftot_networkx import update_ndr_parameter

    # Check NDR conditions before reporting optimal routes
    update_ndr_parameter(the_scenario, logger)

    # make overall Reports directory
    report_directory = os.path.join(the_scenario.scenario_run_directory, "Reports")
    if not os.path.exists(report_directory):
        os.makedirs(report_directory)

    # make subdirectory for individual run
    timestamp_folder_name = 'reports_' + TIMESTAMP.strftime("%Y_%m_%d_%H-%M-%S")
    timestamp_directory = os.path.join(the_scenario.scenario_run_directory, "Reports", timestamp_folder_name)
    if not os.path.exists(timestamp_directory):
        os.makedirs(timestamp_directory)

    filetype_list = ['s_', 'f_', 'f2', 'c_', 'c2', 'g_', 'g2', 'o', 'o1', 'o2', 'oc', 'oc1', 'oc2', 'oc3', 'os', 'p_']
    # init the dictionary to hold them by type.  for the moment ignoring other types.
    log_file_dict = {}
    for x in filetype_list:
        log_file_dict[x] = []

    # get all of the log files matching the pattern
    log_files = glob.glob(os.path.join(the_scenario.scenario_run_directory, "logs", "*_log_*_*_*_*-*-*.log"))

    # add log file name and date to dictionary.  each entry in the array
    # will be a tuple of (log_file_name, datetime object)

    for log_file in log_files:

        path_to, the_file_name = ntpath.split(log_file)
        the_type = the_file_name[:2]

        # one letter switch 's_log_', 'd_log_', etc.
        if the_file_name[5] == "_":
            the_date = datetime.datetime.strptime(the_file_name[5:25], "_%Y_%m_%d_%H-%M-%S")

        # two letter switch 'c2_log_', 'g2_log_', etc.
        elif the_file_name[6] == "_":
            the_date = datetime.datetime.strptime(the_file_name[6:26], "_%Y_%m_%d_%H-%M-%S")
        else:
            logger.warning("The filename: {} is not supported in the logging".format(the_file_name))

        if the_type in log_file_dict:
            log_file_dict[the_type].append((the_file_name, the_date))

    # sort each log type list by datetime so the most recent is first.
    for x in filetype_list:
        log_file_dict[x] = sorted(log_file_dict[x], key=lambda tup: tup[1], reverse=True)

    # create a list of log files to include in report by grabbing the latest version.
    # these will be in order by log type (i.e. s, f, c, g, etc)
    most_recent_log_file_set = []

    if len(log_file_dict['s_']) > 0:
        most_recent_log_file_set.append(log_file_dict['s_'][0])

    if len(log_file_dict['f_']) > 0:
        most_recent_log_file_set.append(log_file_dict['f_'][0])

    if len(log_file_dict['c_']) > 0:
        most_recent_log_file_set.append(log_file_dict['c_'][0])

    if len(log_file_dict['g_']) > 0:
        most_recent_log_file_set.append(log_file_dict['g_'][0])

    if len(log_file_dict['oc']) == 0:
        if len(log_file_dict['o']) > 0:
            most_recent_log_file_set.append(log_file_dict['o'][0])

        if len(log_file_dict['o1']) > 0:
            most_recent_log_file_set.append(log_file_dict['o1'][0])

        if len(log_file_dict['o2']) > 0:
            most_recent_log_file_set.append(log_file_dict['o2'][0])

        if len(log_file_dict['os']) > 0:
            most_recent_log_file_set.append(log_file_dict['os'][0])

    if len(log_file_dict['oc']) > 0:
        most_recent_log_file_set.append(log_file_dict['oc'][0])

    if len(log_file_dict['f2']) > 0:
        most_recent_log_file_set.append(log_file_dict['f2'][0])

    if len(log_file_dict['c2']) > 0:
        most_recent_log_file_set.append(log_file_dict['c2'][0])

    if len(log_file_dict['g2']) > 0:
        most_recent_log_file_set.append(log_file_dict['g2'][0])

        if len(log_file_dict['o']) > 0:
            most_recent_log_file_set.append(log_file_dict['o'][0])

        if len(log_file_dict['o1']) > 0:
            most_recent_log_file_set.append(log_file_dict['o1'][0])

        if len(log_file_dict['o2']) > 0:
            most_recent_log_file_set.append(log_file_dict['o2'][0])

    if len(log_file_dict['p_']) > 0:
        most_recent_log_file_set.append(log_file_dict['p_'][0])

    # figure out the last index of most_recent_log_file_set to include
    # by looking at dates.  if a subsequent step is seen to have an older
    # log than a preceding step, no subsequent logs will be used.
    # --------------------------------------------------------------------

    last_index_to_include = 0

    for i in range(1, len(most_recent_log_file_set)):
        # print most_recent_log_file_set[i]
        if i == 1:
            last_index_to_include += 1
        elif i > 1:
            if most_recent_log_file_set[i][1] > most_recent_log_file_set[i - 1][1]:
                last_index_to_include += 1
            else:
                break

    # print last_index_to_include
    # --------------------------------------------------------

    message_dict = {
        'RESULT': [],
        'CONFIG': [],
        'ERROR': [],
        'WARNING': [],
        'RUNTIME': []
    }

    for i in range(0, last_index_to_include + 1):

        in_file = os.path.join(the_scenario.scenario_run_directory, "logs", most_recent_log_file_set[i][0])

        # s, p, b, r
        record_src = most_recent_log_file_set[i][0][:2].upper()

        with open(in_file, 'r') as rf:
            for line in rf:
                recs = line.strip()[19:].split(' ', 1)
                if recs[0] in message_dict:                    
                    if len(recs) > 1: # RE: Issue #182 - exceptions at the end of the log will cause this to fail.
                        message_dict[recs[0]].append((record_src, recs[1].strip()))

    # dump to file
    # ---------------
    
    report_file_name = 'report_' + TIMESTAMP.strftime("%Y_%m_%d_%H-%M-%S") + ".txt"

    report_file = os.path.join(timestamp_directory, report_file_name)
    with open(report_file, 'w') as wf:

        wf.write('SCENARIO\n')
        wf.write('---------------------------------------------------------------------\n')
        wf.write('Scenario Name\t:\t{}\n'.format(the_scenario.scenario_name))
        wf.write('Timestamp\t:\t{}\n'.format(TIMESTAMP.strftime("%Y-%m-%d %H:%M:%S")))
        wf.write('FTOT Version\t:\t{}\n'.format(FTOT_VERSION))

        wf.write('\nRUNTIME\n')
        wf.write('---------------------------------------------------------------------\n')
        for x in message_dict['RUNTIME']:
            wf.write('{}\t:\t{}\n'.format(x[0], x[1]))

        wf.write('\nRESULTS\n')
        wf.write('---------------------------------------------------------------------\n')
        for x in message_dict['RESULT']:
            wf.write('{}\t:\t{}\n'.format(x[0], x[1]))

        wf.write('\nCONFIG\n')
        wf.write('---------------------------------------------------------------------\n')
        for x in message_dict['CONFIG']:
            wf.write('{}\t:\t{}\n'.format(x[0], x[1]))

        if len(message_dict['ERROR']) > 0:
            wf.write('\nERROR\n')
            wf.write('---------------------------------------------------------------------\n')
            for x in message_dict['ERROR']:
                wf.write('{}\t:\t\t{}\n'.format(x[0], x[1]))

        if len(message_dict['WARNING']) > 0:
            wf.write('\nWARNING\n')
            wf.write('---------------------------------------------------------------------\n')
            for x in message_dict['WARNING']:
                wf.write('{}\t:\t\t{}\n'.format(x[0], x[1]))

    logger.info("Done Parse Log Operation")

    # -------------------------------------------------------------

    logger.info("start: main results report")
    report_file_name = 'report_' + TIMESTAMP.strftime("%Y_%m_%d_%H-%M-%S") + ".csv"
    report_file_name = clean_file_name(report_file_name)
    report_file = os.path.join(timestamp_directory, report_file_name)
    
    with open(report_file, 'w', newline='') as wf:
        writer = csv.writer(wf)
        writer.writerow(['scenario_name', 'table_name', 'commodity', 'facility_name', 'measure', 'mode', 'value', 'units', 'notes'])
        
        import sqlite3
        with sqlite3.connect(the_scenario.main_db) as db_con:

            # query the optimal scenario results table and report out the results
            # -------------------------------------------------------------------------
            sql = "select * from optimal_scenario_results order by table_name, commodity, measure, mode;"
            db_cur = db_con.execute(sql)
            data = db_cur.fetchall()

            for row in data:
                writer.writerow([the_scenario.scenario_name, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]])
            # Scenario Total Supply and Demand, and Available Processing Capacity
            # note: processor input and outputs are based on facility size and reflect a processing capacity,
            # not a conversion of the scenario feedstock supply
            # -------------------------------------------------------------------------
            sql = """   select c.commodity_name, fti.facility_type, io, sum(fc.quantity), fc.units  
                              from facility_commodities fc
                              join commodities c on fc.commodity_id = c.commodity_id
                              join facilities f on f.facility_id = fc.facility_id
                              join facility_type_id fti on fti.facility_type_id = f.facility_type_id
                              group by c.commodity_name, fc.io, fti.facility_type, fc.units
                              order by commodity_name, io desc;"""
            db_cur = db_con.execute(sql)
            db_data = db_cur.fetchall()
            for row in db_data:
                facility_type = row[1]
                io = row[2]
                if facility_type == "raw_material_producer":
                    measure = "supply"
                if facility_type == "processor" and io == 'o':
                    measure = "processing output capacity"
                if facility_type == "processor" and io == 'i':
                    measure = "processing input capacity"
                if facility_type == "ultimate_destination":
                    measure = "demand"
                writer.writerow([the_scenario.scenario_name, "total_supply_demand_proc", row[0], "all_{}".format(row[1]), measure, io, row[3], row[4], None])

            # Scenario Stranded Supply, Demand, and Processing Capacity
            # note: stranded supply refers to facilities that are ignored from the analysis.")
            # -------------------------------------------------------------------------
            sql = """ select c.commodity_name, fti.facility_type, io, sum(fc.quantity), fc.units, f.ignore_facility 
                            from facility_commodities fc
                            join commodities c on fc.commodity_id = c.commodity_id
                            join facilities f on f.facility_id = fc.facility_id
                            join facility_type_id fti on fti.facility_type_id = f.facility_type_id
                            where f.ignore_facility != 'false'
                            group by c.commodity_name, fc.io, fti.facility_type, fc.units, f.ignore_facility
                            order by commodity_name, io asc;"""
            db_cur = db_con.execute(sql)
            db_data = db_cur.fetchall()

            for row in db_data:
                facility_type = row[1]
                io = row[2]
                if facility_type == "raw_material_producer":
                    measure = "stranded supply"
                if facility_type == "processor" and io == 'o':
                    measure = "stranded processing output capacity"
                if facility_type == "processor" and io == 'i':
                    measure = "stranded processing input capacity"
                if facility_type == "ultimate_destination":
                    measure = "stranded demand"
                writer.writerow([the_scenario.scenario_name, "stranded_supply_demand_proc", row[0], "stranded_{}".format(row[1]), measure, io, row[3], row[4], row[5]]) # ignore_facility note

            # report out net quantities with ignored facilities removed from the query
            # note: net supply, demand, and processing capacity ignores facilities not connected to the network
            # -------------------------------------------------------------------------
            sql = """   select c.commodity_name, fti.facility_type, io, sum(fc.quantity), fc.units  
                              from facility_commodities fc
                              join commodities c on fc.commodity_id = c.commodity_id
                              join facilities f on f.facility_id = fc.facility_id
                              join facility_type_id fti on fti.facility_type_id = f.facility_type_id
                              where f.ignore_facility == 'false'
                              group by c.commodity_name, fc.io, fti.facility_type, fc.units
                              order by commodity_name, io desc;"""
            db_cur = db_con.execute(sql)
            db_data = db_cur.fetchall()
            for row in db_data:
                facility_type = row[1]
                io = row[2]
                if facility_type == "raw_material_producer":
                    measure = "net supply"
                if facility_type == "processor" and io == 'o':
                    measure = "net processing output capacity"
                if facility_type == "processor" and io == 'i':
                    measure = "net processing input capacity"
                if facility_type == "ultimate_destination":
                    measure = "net demand"
                writer.writerow([the_scenario.scenario_name, "net_supply_demand_proc", row[0], "net_{}".format(row[1]), measure, io, row[3], row[4], None])

        # REPORT OUT CONFIG FOR O2 STEP AND RUNTIMES FOR ALL STEPS
        # Loop through the list of configurations records in the message_dict['config'] and ['runtime'].
        # Note that this list is of the format: ['step', "xml_record, value"], and additional parsing is required.
        step_to_export = 'O2'  # the step we want to export
        logger.info("output the configuration for step: {}".format(step_to_export))
        for config_record in message_dict['CONFIG']:
            step = config_record[0]
            if step != step_to_export:
                continue
            else:
                # parse the xml record and value from the
                xml_record_and_value = config_record[1]
                xml_record = xml_record_and_value.split(":", 1)[0].replace("xml_", "")
                value = xml_record_and_value.split(":", 1)[1].strip()  # only split on the first colon to prevent paths from being split
                writer.writerow([the_scenario.scenario_name, "config", '', '', xml_record, '', '', '', value])

        for x in message_dict['RUNTIME']:
            # message_dict['RUNTIME'][0]
            # ('S_', 's Step - Total Runtime (HMS): \t00:00:21')
            step, runtime = x[1].split('\t')
            writer.writerow([the_scenario.scenario_name, "runtime", '', '', step, '', '', '', runtime])

    logger.debug("finish: main results report operation")

    prepare_tableau_assets(timestamp_directory, report_file, the_scenario, logger)
    
    # -------------------------------------------------------------

    # artificial link summary
    generate_artificial_link_summary(timestamp_directory, the_scenario, logger)

    # routes summary
    if the_scenario.ndrOn:
        generate_edges_from_routes_summary(timestamp_directory, the_scenario, logger)

    # emissions summary
    if the_scenario.detailed_emissions:
        generate_detailed_emissions_summary(timestamp_directory, the_scenario, logger)

    logger.result("Reports located here: {}".format(timestamp_directory))
