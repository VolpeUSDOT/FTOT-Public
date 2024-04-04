# ---------------------------------------------------------------------------------------------------
# Name: ftot_report.py
#
# Purpose: Parses the most current log file for each step and assembles two reports.
# The first is a human readable report that groups log messages by runtime, result, config, and warnings.
# The second report is formatted for the FTOT Tableau dashboard as a simple CSV and comes from a few tables in the db.
#
# ---------------------------------------------------------------------------------------------------

import os
import datetime
import glob
import ntpath
import zipfile
import sqlite3
import csv
from ftot_supporting_gis import zipgdb, get_commodity_vehicle_attributes_dict
from ftot_supporting import clean_file_name
from shutil import copy

from ftot import FTOT_VERSION

TIMESTAMP = datetime.datetime.now()


# ==================================================================


def prepare_tableau_assets(timestamp_directory, the_scenario, logger):
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

    # create the zip file for writing compressed data

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
    ftot_program_directory = os.path.dirname(os.path.realpath(__file__))
    root_twb_location = os.path.join(ftot_program_directory, "lib",  "tableau_dashboard.twb")
    scenario_twb_location = os.path.join(timestamp_directory, "tableau_dashboard.twb")
    copy(root_twb_location, scenario_twb_location)

    # copy tableau report to the assets location
    latest_generic_path = os.path.join(timestamp_directory, "tableau_report.csv")
    logger.debug("copying the latest tableau report csv file to the timestamped tableau report directory")
    report_file_name = 'report_' + TIMESTAMP.strftime("%Y_%m_%d_%H-%M-%S") + ".csv"
    report_file_name = clean_file_name(report_file_name)
    report_file = os.path.join(timestamp_directory, report_file_name)
    copy(report_file, latest_generic_path)

    # add all_routes report to assets location
    logger.debug("adding routes report csv file to the tableau report directory")
    if the_scenario.ndrOn:
        # if NDR On, copy existing routes report
        latest_routes_path = os.path.join(timestamp_directory, "all_routes.csv")
        routes_file_name = 'all_routes_' + TIMESTAMP.strftime("%Y_%m_%d_%H-%M-%S") + ".csv"
        routes_file_name = clean_file_name(routes_file_name)
        routes_file = os.path.join(timestamp_directory, routes_file_name)
        copy(routes_file, latest_routes_path)
    else:
        # if NDR Off, generate placeholder all_routes report
        routes_file = os.path.join(timestamp_directory, "all_routes.csv")
        with open(routes_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Run scenario with NDR on to view this dashboard.'])
   
    # create packaged workbook for tableau reader compatibility
    twbx_dashboard_filename = os.path.join(timestamp_directory, "tableau_dashboard.twbx")
    zipObj = zipfile.ZipFile(twbx_dashboard_filename, 'w', zipfile.ZIP_DEFLATED)

    # add multiple files to the zip
    # need to specify the arcname parameter to avoid the whole path to the file being added to the archive
    zipObj.write(os.path.join(timestamp_directory, "tableau_dashboard.twb"), "tableau_dashboard.twb")
    zipObj.write(os.path.join(timestamp_directory, "tableau_report.csv"), "tableau_report.csv")
    zipObj.write(os.path.join(timestamp_directory, "tableau_output.gdb.zip"), "tableau_output.gdb.zip")
    zipObj.write(os.path.join(timestamp_directory, "all_routes.csv"), "all_routes.csv")

    # close the zip file
    zipObj.close()

    # delete the other files so it is nice and clean
    os.remove(os.path.join(timestamp_directory, "tableau_dashboard.twb"))
    os.remove(os.path.join(timestamp_directory, "tableau_report.csv"))
    os.remove(os.path.join(timestamp_directory, "tableau_output.gdb.zip"))
    os.remove(os.path.join(timestamp_directory, "all_routes.csv"))


# ==============================================================================================


def generate_edges_from_routes_summary(timestamp_directory, the_scenario, logger):

    logger.info("start: generate_edges_from_routes_summary")
    report_file_name = 'all_routes_' + TIMESTAMP.strftime("%Y_%m_%d_%H-%M-%S") + '.csv'
    report_file_name = clean_file_name(report_file_name)
    report_file = os.path.join(timestamp_directory, report_file_name)
    
    with sqlite3.connect(the_scenario.main_db) as db_con:
        # drop the routes summary table
        sql = "drop table if exists all_routes_results"
        db_con.execute(sql)

        # create the routes summary table
        sql = """
              create table all_routes_results(
                                              scenario_name text,
                                              route_id integer,
                                              from_facility text,
                                              from_facility_type text,
                                              to_facility text,
                                              to_facility_type text,
                                              commodity text,
                                              phase text,
                                              mode text,
                                              transport_cost real,
                                              routing_cost real,
                                              length real,
                                              co2 real,
                                              in_solution text
                                             );"""
        db_con.execute(sql)

        db_cur = db_con.cursor()
        if the_scenario.report_with_artificial:
            summary_route_data = db_con.execute("""select rr.route_id, f1.facility_name as from_facility, fti1.facility_type as from_facility_type,
                f2.facility_name as to_facility, fti2.facility_type as to_facility_type,
                c.commodity_name, c.phase_of_matter, m.mode, rr.transport_cost, rr.cost, rr.length, rr.co2,
                case when ors.scenario_rt_id is NULL then "N" else "Y" end as in_solution
                from route_reference rr
                join facilities f1 on rr.from_facility_id = f1.facility_id
                join facility_type_id fti1 on f1.facility_type_id = fti1.facility_type_id
                join facilities f2 on rr.to_facility_id = f2.facility_id
                join facility_type_id fti2 on f2.facility_type_id = fti2.facility_type_id
                join commodities c on rr.commodity_id = c.commodity_ID
                left join (select temp.scenario_rt_id, case when temp.modes_list like "%,%" then "multimodal" else temp.modes_list end as mode
                           from (select re.scenario_rt_id, group_concat(distinct(nx_e.mode_source)) as modes_list
                                 from route_edges re
                                 left join networkx_edges nx_e on re.edge_id = nx_e.edge_id
                                 group by re.scenario_rt_id) temp) m on rr.scenario_rt_id = m.scenario_rt_id
                left join (select distinct scenario_rt_id from optimal_route_segments) ors on rr.scenario_rt_id = ors.scenario_rt_id;""")

        else:
            # subtract artificial link values from results, except for routing cost
            summary_route_data = db_con.execute("""select rr.route_id, f1.facility_name as from_facility, fti1.facility_type as from_facility_type,
                f2.facility_name as to_facility, fti2.facility_type as to_facility_type,
                c.commodity_name, c.phase_of_matter, m.mode,
                rr.transport_cost - (nec1.transport_cost + nec2.transport_cost) as transport_cost,
                rr.cost, -- always keep artificial links in routing cost
                rr.length - (ne1.length + ne2.length) as length,
                rr.co2 - (nec1.co2_cost + nec2.co2_cost) / {} as co2,
                case when ors.scenario_rt_id is NULL then "N" else "Y" end as in_solution
                from route_reference rr
                join facilities f1 on rr.from_facility_id = f1.facility_id
                join facility_type_id fti1 on f1.facility_type_id = fti1.facility_type_id
                join facilities f2 on rr.to_facility_id = f2.facility_id
                join facility_type_id fti2 on f2.facility_type_id = fti2.facility_type_id
                join commodities c on rr.commodity_id = c.commodity_ID
                join networkx_edge_costs nec1 on rr.first_nx_edge_id = nec1.edge_id and rr.phase_of_matter = nec1.phase_of_matter_id
                join networkx_edge_costs nec2 on rr.last_nx_edge_id = nec2.edge_id and rr.phase_of_matter = nec2.phase_of_matter_id
                join networkx_edges ne1 on rr.first_nx_edge_id = ne1.edge_id
                join networkx_edges ne2 on rr.last_nx_edge_id = ne2.edge_id
                left join (select temp.scenario_rt_id, case when temp.modes_list like "%,%" then "multimodal" else temp.modes_list end as mode
                           from (select re.scenario_rt_id, group_concat(distinct(nx_e.mode_source)) as modes_list
                                 from route_edges re
                                 left join networkx_edges nx_e on re.edge_id = nx_e.edge_id
                                 group by re.scenario_rt_id) temp) m on rr.scenario_rt_id = m.scenario_rt_id
                left join (select distinct scenario_rt_id from optimal_route_segments) ors on rr.scenario_rt_id = ors.scenario_rt_id;""".format(the_scenario.co2_unit_cost.magnitude))
        
        summary_route_data =  summary_route_data.fetchall()
        
        # Prepare row to write to DB and CSV report
        all_routes_list = []
        with open(report_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['scenario_name', 'route_id', 'from_facility', 'from_facility_type', 'to_facility', 'to_facility_type',
                             'commodity_name', 'phase', 'mode', 'transport_cost', 'routing_cost', 'length', 'co2', 'in_solution'])

            for row in summary_route_data:
                route_id = row[0]
                from_facility = row[1]
                from_facility_type = row[2]
                to_facility = row[3]
                to_facility_type = row[4]
                commodity_name = row[5]
                phase = row[6]
                mode = row[7]
                transport_cost = row[8]
                routing_cost = row[9]
                length = row[10]
                co2 = row[11]
                in_solution = row[12]

                # append to list for batch update of DB table
                all_routes_list.append([the_scenario.scenario_name, route_id, from_facility, from_facility_type, to_facility, to_facility_type,
                                 commodity_name, phase, mode, transport_cost, routing_cost, length, co2, in_solution])

                # print route data to file in Reports folder
                writer.writerow([the_scenario.scenario_name, route_id, from_facility, from_facility_type, to_facility, to_facility_type,
                                 commodity_name, phase, mode, transport_cost, routing_cost, length, co2, in_solution])
        
        # Update the DB table
        insert_sql = """
                INSERT into all_routes_results
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ;"""

        db_con.executemany(insert_sql, all_routes_list)

    logger.info("finish: generate_edges_from_routes_summary")


# ==============================================================================================


def generate_artificial_link_summary(timestamp_directory, the_scenario, logger):
    
    logger.info("start: generate_artificial_link_summary")
    report_file_name = 'artificial_links_' + TIMESTAMP.strftime("%Y_%m_%d_%H-%M-%S") + '.csv'
    report_file_name = clean_file_name(report_file_name)
    report_file = os.path.join(timestamp_directory, report_file_name)

    # create artificial links results table in db
    with sqlite3.connect(the_scenario.main_db) as db_con:

        # drop the table
        sql = "drop table if exists artificial_link_results"
        db_con.execute(sql)

        # create the table
        sql = """
              create table artificial_link_results(
                                                   facility_name text,
                                                   facility_type text,
                                                   commodity text,
                                                   measure text,
                                                   mode text,
                                                   in_solution text,
                                                   value text,
                                                   units text
                                                   );"""
        db_con.execute(sql)

        # artificial link lengths by mode
        sql_length_art = """ -- artificial link length
                         select fac.facility_name, fti.facility_type,
                         ne.mode_source, round(ne.length, 3) as length,
                         --case when ors.from_node_id is not NULL then 'Y' else 'N' end as in_solution
                         'NA' as in_solution
                         from facilities fac
                         left join facility_type_id fti on fac.facility_type_id = fti.facility_type_id
                         left join networkx_nodes nn on fac.location_id = nn.location_id
                         left join networkx_edges ne on nn.node_id = ne.from_node_id
                         left join (select distinct from_node_id, to_node_id from optimal_route_segments) ors
                         on ne.from_node_id = ors.from_node_id and ne.to_node_id = ors.to_node_id
                         where nn.location_id_name like '%OUT%'
                         and ne.artificial = 1
                         ;"""
        db_cur = db_con.execute(sql_length_art)
        db_data = db_cur.fetchall()

        artificial_links = {}
        artificial_link_lengths = []

        for row in db_data:
            facility_name = row[0]
            facility_type = row[1]
            mode_source = row[2]
            link_length = row[3]
            in_solution = row[4]

            if facility_name not in artificial_links:
                # add new facility to dictionary and start list of artificial links by mode
                artificial_links[facility_name] = {'fac_type': facility_type, 'link_lengths': {}, 'in_solution': in_solution}

            if mode_source not in artificial_links[facility_name]['link_lengths']:
                artificial_links[facility_name]['link_lengths'][mode_source] = link_length
            else:
                # there should only be one artificial link for a facility for each mode
                error = "Multiple artificial links should not be found for a single facility for a particular mode."
                logger.error(error)
                raise Exception(error)

        # iterate through every facility, append a row for each facility-mode pair
        for k in artificial_links:
            for permitted_mode in the_scenario.permittedModes:
                if permitted_mode in artificial_links[k]['link_lengths']:
                    art_link_length = artificial_links[k]['link_lengths'][permitted_mode]
                else:
                    art_link_length = 'NA'
                artificial_link_lengths.append([k, artificial_links[k]['fac_type'], 'NA', 'length',
                                                permitted_mode, artificial_links[k]['in_solution'],
                                                art_link_length, str(the_scenario.default_units_distance)])

        # insert into artificial link results db table
        sql = """
              insert into artificial_link_results
              (facility_name, facility_type, commodity, measure, mode, in_solution, value, units)
              values (?,?,?,?,?,?,?,?);
              """

        logger.debug("start: update artificial_link_results table with lengths")
        db_con.executemany(sql, artificial_link_lengths)
        db_con.commit()
        logger.debug("end: update artificial_link_results table with lengths")

        # transport and routing costs on artificial links only
        logger.debug("start: calculate transport and routing costs for artificial links")
        sql_transport_costs_art = """ -- artificial link transport cost
                                  insert into artificial_link_results
                                  select facility_name, facility_type, commodity,
                                  'transport_cost', mode, 'Y',
                                  sum(commodity_flow*link_transport_cost),
                                  '{}'
                                  from (select
                                        case when fac1.facility_name is not null and fac2.facility_name is null then fac1.facility_name
                                        when fac1.facility_name is null and fac2.facility_name is not null then fac2.facility_name
                                        else 'NA' end as facility_name,
                                        case when fac1.facility_name is not null and fac2.facility_name is null then fac1.facility_type
                                        when fac1.facility_name is null and fac2.facility_name is not null then fac2.facility_type
                                        else 'NA' end as facility_type,
                                        ors.commodity_name as commodity,
                                        ors.network_source_id as mode,
                                        ors.commodity_flow,
                                        ors.link_transport_cost
                                        from optimal_route_segments ors
                                        left join (select fac.facility_name, fti.facility_type, nn.node_id from facilities fac
                                        left join facility_type_id fti on fac.facility_type_id = fti.facility_type_id
                                        left join networkx_nodes nn on fac.location_id = nn.location_id) fac1 on ors.from_node_id = fac1.node_id
                                        left join (select fac.facility_name, fti.facility_type, nn.node_id from facilities fac
                                        left join facility_type_id fti on fac.facility_type_id = fti.facility_type_id
                                        left join networkx_nodes nn on fac.location_id = nn.location_id) fac2 on ors.to_node_id = fac2.node_id
                                        where ors.artificial = 1)
                                  group by facility_name, facility_type, commodity, mode
                                  ;""".format(the_scenario.default_units_currency)
        db_con.execute(sql_transport_costs_art)

        sql_routing_costs_art = """ -- artificial link routing cost
                                insert into artificial_link_results
                                select facility_name, facility_type, commodity,
                                'routing_cost', mode, 'Y',
                                sum(commodity_flow*link_routing_cost),
                                '{}'
                                from (select
                                      case when fac1.facility_name is not null and fac2.facility_name is null then fac1.facility_name
                                      when fac1.facility_name is null and fac2.facility_name is not null then fac2.facility_name
                                      else 'NA' end as facility_name,
                                      case when fac1.facility_name is not null and fac2.facility_name is null then fac1.facility_type
                                      when fac1.facility_name is null and fac2.facility_name is not null then fac2.facility_type
                                      else 'NA' end as facility_type,
                                      ors.commodity_name as commodity,
                                      ors.network_source_id as mode,
                                      ors.commodity_flow,
                                      ors.link_routing_cost
                                      from optimal_route_segments ors
                                      left join (select fac.facility_name, fti.facility_type, nn.node_id from facilities fac
                                      left join facility_type_id fti on fac.facility_type_id = fti.facility_type_id
                                      left join networkx_nodes nn on fac.location_id = nn.location_id) fac1 on ors.from_node_id = fac1.node_id
                                      left join (select fac.facility_name, fti.facility_type, nn.node_id from facilities fac
                                      left join facility_type_id fti on fac.facility_type_id = fti.facility_type_id
                                      left join networkx_nodes nn on fac.location_id = nn.location_id) fac2 on ors.to_node_id = fac2.node_id
                                      where ors.artificial = 1)
                                group by facility_name, facility_type, commodity, mode
                                ;""".format(the_scenario.default_units_currency)
        db_con.execute(sql_routing_costs_art)
        logger.debug("end: calculate transport and routing costs for artificial links")

        # length of network used on artificial links only
        logger.debug("start: calculate network used for artificial links")
        sql_network_used_art = """ -- artificial link network used
                               insert into artificial_link_results
                               select distinct facility_name, facility_type, commodity,
                               'network_used', mode, 'Y',
                               length,
                               '{}'
                               from (select
                                     case when fac1.facility_name is not null and fac2.facility_name is null then fac1.facility_name
                                     when fac1.facility_name is null and fac2.facility_name is not null then fac2.facility_name
                                     else 'NA' end as facility_name,
                                     case when fac1.facility_name is not null and fac2.facility_name is null then fac1.facility_type
                                     when fac1.facility_name is null and fac2.facility_name is not null then fac2.facility_type
                                     else 'NA' end as facility_type,
                                     ors.commodity_name as commodity,
                                     ors.network_source_id as mode,
                                     round(ors.length, 3) as length
                                     from optimal_route_segments ors
                                     left join (select fac.facility_name, fti.facility_type, nn.node_id from facilities fac
                                     left join facility_type_id fti on fac.facility_type_id = fti.facility_type_id
                                     left join networkx_nodes nn on fac.location_id = nn.location_id) fac1 on ors.from_node_id = fac1.node_id
                                     left join (select fac.facility_name, fti.facility_type, nn.node_id from facilities fac
                                     left join facility_type_id fti on fac.facility_type_id = fti.facility_type_id
                                     left join networkx_nodes nn on fac.location_id = nn.location_id) fac2 on ors.to_node_id = fac2.node_id
                                     where ors.artificial = 1)
                                  ;""".format(the_scenario.default_units_distance)
        db_con.execute(sql_network_used_art)
        logger.debug("end: calculate network used for artificial links")

        # get optimal modes and commodities
        sql_mode_commodity = "select network_source_id, commodity_name from optimal_route_segments group by network_source_id, commodity_name;"
        db_cur = db_con.execute(sql_mode_commodity)
        mode_and_commodity_list = db_cur.fetchall()

        # use method from ftot_supporting_gis
        attributes_dict = get_commodity_vehicle_attributes_dict(the_scenario, logger)

        # CO2 on artificial links only
        logger.debug("start: summarize emissions for artificial links")

        # Loop through CO2
        for row in mode_and_commodity_list:
            mode = row[0]
            commodity_name = row[1]

            co2_val = attributes_dict[commodity_name][mode]['co2']['artificial'].magnitude
            vehicle_payload = attributes_dict[commodity_name][mode]['Load']['artificial'].magnitude
            sql_co2_art = """ -- artificial link co2 emissions
                          insert into artificial_link_results
                          select facility_name, facility_type, commodity,
                          'co2', mode, 'Y',
                          sum({} * length * commodity_flow/{}), -- value (emissions scalar * vmt)
                          'grams'
                          from (select
                                case when fac1.facility_name is not null and fac2.facility_name is null then fac1.facility_name
                                when fac1.facility_name is null and fac2.facility_name is not null then fac2.facility_name
                                else 'NA' end as facility_name,
                                case when fac1.facility_name is not null and fac2.facility_name is null then fac1.facility_type
                                when fac1.facility_name is null and fac2.facility_name is not null then fac2.facility_type
                                else 'NA' end as facility_type,
                                ors.commodity_name as commodity,
                                ors.network_source_id as mode,
                                ors.length as length,
                                ors.commodity_flow as commodity_flow
                                from optimal_route_segments ors
                                left join (select fac.facility_name, fti.facility_type, nn.node_id from facilities fac
                                left join facility_type_id fti on fac.facility_type_id = fti.facility_type_id
                                left join networkx_nodes nn on fac.location_id = nn.location_id) fac1 on ors.from_node_id = fac1.node_id
                                left join (select fac.facility_name, fti.facility_type, nn.node_id from facilities fac
                                left join facility_type_id fti on fac.facility_type_id = fti.facility_type_id
                                left join networkx_nodes nn on fac.location_id = nn.location_id) fac2 on ors.to_node_id = fac2.node_id
                                where ors.artificial == 1)
                          where mode = '{}' and commodity = '{}'
                          group by facility_name, facility_type, commodity, mode
                          ;""".format(co2_val, vehicle_payload, mode, commodity_name)
            db_con.execute(sql_co2_art)

        logger.debug("end: summarize emissions for artificial links")

        # VMT on artificial links only
        # Do not report for pipeline
        logger.debug("start: summarize vehicle-distance traveled for artificial links")
        for row in mode_and_commodity_list:
            mode = row[0]
            commodity_name = row[1]

            if 'pipeline' in mode:
                pass
            else:
                vehicle_payload = attributes_dict[commodity_name][mode]['Load']['general'].magnitude
                # vmt is loads multiplied by distance
                # we know the flow on a link we can calculate the loads on that link
                # and multiply by the distance to get VMT
                sql_vmt_art = """ -- artificial link vehicle-distance traveled by mode and commodity
                              insert into artificial_link_results
                              select facility_name, facility_type, commodity,
                              'vehicle-distance_traveled', mode, 'Y',
                              sum(length * commodity_flow/{}),
                              'vehicle-{}'
                              from (select
                                    case when fac1.facility_name is not null and fac2.facility_name is null then fac1.facility_name
                                    when fac1.facility_name is null and fac2.facility_name is not null then fac2.facility_name
                                    else 'NA' end as facility_name,
                                    case when fac1.facility_name is not null and fac2.facility_name is null then fac1.facility_type
                                    when fac1.facility_name is null and fac2.facility_name is not null then fac2.facility_type
                                    else 'NA' end as facility_type,
                                    ors.commodity_name as commodity,
                                    ors.network_source_id as mode,
                                    ors.length as length,
                                    ors.commodity_flow as commodity_flow
                                    from optimal_route_segments ors
                                    left join (select fac.facility_name, fti.facility_type, nn.node_id from facilities fac
                                    left join facility_type_id fti on fac.facility_type_id = fti.facility_type_id
                                    left join networkx_nodes nn on fac.location_id = nn.location_id) fac1 on ors.from_node_id = fac1.node_id
                                    left join (select fac.facility_name, fti.facility_type, nn.node_id from facilities fac
                                    left join facility_type_id fti on fac.facility_type_id = fti.facility_type_id
                                    left join networkx_nodes nn on fac.location_id = nn.location_id) fac2 on ors.to_node_id = fac2.node_id
                                    where ors.artificial == 1)
                              where mode = '{}' and commodity = '{}'
                              group by facility_name, facility_type, commodity, mode
                              ;""".format(vehicle_payload, the_scenario.default_units_distance,
                                          mode, commodity_name)
                db_con.execute(sql_vmt_art)

        logger.debug("end: summarize vehicle-distance traveled for artificial links")

        # Fuel burn
        # convert VMT to fuel burn
        # truck, rail, barge, no pipeline
        # Same as VMT but divide result by fuel efficiency
        logger.debug("start: summarize vehicle loads for artificial links")
        for row in mode_and_commodity_list:
            mode = row[0]
            commodity_name = row[1]

            if 'pipeline' in mode:
                pass
            else:
                vehicle_payload = attributes_dict[commodity_name][mode]['Load']['general'].magnitude
                fuel_efficiency = attributes_dict[commodity_name][mode]['Fuel_Efficiency'].magnitude # mi/gal

                # vmt is loads multiplied by length
                # we know the flow on a link we can calculate the loads on that link
                # and multiply by the length to get VMT, then divide by fuel efficiency to get fuel burn
                sql_fuel_burn_art = """ -- artificial link fuel burn by mode and commodity
                                    insert into artificial_link_results
                                    select facility_name, facility_type, commodity,
                                    'fuel_burn', mode, 'Y',
                                    sum(length * commodity_flow/{}  / {}),
                                    'gallons'
                                    from (select
                                          case when fac1.facility_name is not null and fac2.facility_name is null then fac1.facility_name
                                          when fac1.facility_name is null and fac2.facility_name is not null then fac2.facility_name
                                          else 'NA' end as facility_name,
                                          case when fac1.facility_name is not null and fac2.facility_name is null then fac1.facility_type
                                          when fac1.facility_name is null and fac2.facility_name is not null then fac2.facility_type
                                          else 'NA' end as facility_type,
                                          ors.commodity_name as commodity,
                                          ors.network_source_id as mode,
                                          ors.length as length,
                                          ors.commodity_flow as commodity_flow
                                          from optimal_route_segments ors
                                          left join (select fac.facility_name, fti.facility_type, nn.node_id from facilities fac
                                          left join facility_type_id fti on fac.facility_type_id = fti.facility_type_id
                                          left join networkx_nodes nn on fac.location_id = nn.location_id) fac1 on ors.from_node_id = fac1.node_id
                                          left join (select fac.facility_name, fti.facility_type, nn.node_id from facilities fac
                                          left join facility_type_id fti on fac.facility_type_id = fti.facility_type_id
                                          left join networkx_nodes nn on fac.location_id = nn.location_id) fac2 on ors.to_node_id = fac2.node_id
                                          where ors.artificial == 1)
                                    where mode = '{}' and commodity = '{}'
                                    group by facility_name, facility_type, commodity, mode
                                    ;""".format(vehicle_payload, fuel_efficiency,
                                                mode, commodity_name)
                db_con.execute(sql_fuel_burn_art)

        # print artificial link data for each facility to file in Reports folder
        with open(report_file, 'w', newline='') as wf:
            writer = csv.writer(wf)
            writer.writerow(['facility_name', 'facility_type', 'commodity', 'measure', 'mode', 'in_solution', 'value', 'units'])
        
            # query the artificial link results table and report out the results
            # -------------------------------------------------------------------------
            sql = "select * from artificial_link_results order by facility_name, commodity, measure, mode;"
            db_cur = db_con.execute(sql)
            data = db_cur.fetchall()

            for row in data:
                writer.writerow([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]])

    logger.info("finish: generate_artificial_link_summary")


# ==============================================================================================


def generate_detailed_emissions_summary(timestamp_directory, the_scenario, logger):
    
    logger.info("start: generate_detailed_emissions_summary")
    report_file_name = 'detailed_emissions_' + TIMESTAMP.strftime("%Y_%m_%d_%H-%M-%S") + ".csv"
    report_file_name = clean_file_name(report_file_name)
    report_file = os.path.join(timestamp_directory, report_file_name)

    # query the detailed emissions DB table
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        emissions_data = main_db_con.execute("select * from detailed_emissions;")
        emissions_data = emissions_data.fetchall()
        
        # print emissions data to new report file
        with open(report_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['commodity','mode','pollutant','value','units','notes'])
            writer.writerows(emissions_data)
    
    logger.debug("finish: generate_detailed_emissions_summary")


# ==================================================================


def generate_reports(the_scenario, logger):
    logger.info("start: parse log operation for reports")
    
    from ftot_networkx import update_ndr_parameter

    # Check NDR conditions before reporting routes
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

    filetype_list = ['s', 'f', 'f2', 'c', 'c2', 'g', 'g2', 'o', 'o1', 'o2', 'oc', 'oc1', 'oc2', 'oc3', 'os', 'p']
    # init the dictionary to hold them by type. for the moment ignoring other types.
    log_file_dict = {}
    for x in filetype_list:
        log_file_dict[x] = []

    # get all of the log files matching the pattern
    log_files = glob.glob(os.path.join(the_scenario.scenario_run_directory, "logs", "*_log_*_*_*_*-*-*.log"))

    # add log file name and date to dictionary
    # each entry in the array will be a tuple of (log_file_name, datetime object)

    for log_file in log_files:

        path_to, the_file_name = ntpath.split(log_file)

        # split file name into type, "log", and date
        file_parts = the_file_name.split("_",2)
        the_type = file_parts[0]
        the_date = datetime.datetime.strptime(file_parts[2], "%Y_%m_%d_%H-%M-%S.log")

        if the_type in log_file_dict:
            log_file_dict[the_type].append((the_file_name, the_date))
        else:
            logger.warning("The filename: {} is not supported in the logging".format(the_file_name))

    # sort each log type list by datetime so the most recent is first
    for x in filetype_list:
        log_file_dict[x] = sorted(log_file_dict[x], key=lambda tup: tup[1], reverse=True)

    # create a list of log files to include in report by grabbing the latest version
    # these will be in order by log type (i.e. s, f, c, g, etc)
    most_recent_log_file_set = []

    if len(log_file_dict['s']) > 0:
        most_recent_log_file_set.append(log_file_dict['s'][0])

    if len(log_file_dict['f']) > 0:
        most_recent_log_file_set.append(log_file_dict['f'][0])

    if len(log_file_dict['c']) > 0:
        most_recent_log_file_set.append(log_file_dict['c'][0])

    if len(log_file_dict['g']) > 0:
        most_recent_log_file_set.append(log_file_dict['g'][0])

    if len(log_file_dict['oc']) == 0 and len(log_file_dict['oc1']) == 0:
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
    
    if len(log_file_dict['oc1']) > 0:
        most_recent_log_file_set.append(log_file_dict['oc1'][0])
        
        if len(log_file_dict['oc2']) > 0:
            most_recent_log_file_set.append(log_file_dict['oc2'][0])

        if len(log_file_dict['oc3']) > 0:
            most_recent_log_file_set.append(log_file_dict['oc3'][0])

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

    if len(log_file_dict['p']) > 0:
        most_recent_log_file_set.append(log_file_dict['p'][0])

    # figure out the last index of most_recent_log_file_set to include
    # by looking at dates. if a subsequent step is seen to have an older
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

    subheader_dict = {'SCENARIO': 'Scenario Summary',
                      'COMMODITY': 'Commodity Summary',
                      'FACILITY': 'Facility Summary'}

    for i in range(0, last_index_to_include + 1):

        in_file = os.path.join(the_scenario.scenario_run_directory, "logs", most_recent_log_file_set[i][0])

        # s, p, b, r
        record_src = most_recent_log_file_set[i][0].split("_",1)[0].upper()

        with open(in_file, 'r') as rf:
            for line in rf:
                recs = line.strip()[19:].split(' ', 1)
                if recs[0] in message_dict:                    
                    if len(recs) > 1: # RE: Issue #182 - exceptions at the end of the log will cause this to fail
                        if recs[1].split('_',2)[0].strip() in subheader_dict:
                            # Separate out section name
                            recs_parsed = recs[1].split('_',2)
                            message_dict[recs[0]].append((record_src, recs_parsed[2].strip(), subheader_dict[recs_parsed[0].strip()]))
                        else:
                            message_dict[recs[0]].append((record_src, recs[1].strip(), None))

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
        current_subheader = ""
        for x in message_dict['RESULT']:

            if x[2] in subheader_dict.values() and x[2] != current_subheader:
                # Create new subheader
                wf.write('{}\t:\t---------------------------------------------------------------------\n'.format(x[0]))
                wf.write('{}\t:\t{}\n'.format(x[0],x[2]))
                wf.write('{}\t:\t---------------------------------------------------------------------\n'.format(x[0]))
                current_subheader = x[2]
            
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
            sql = """   select c.commodity_name, fti.facility_type, io, sum(fc.quantity), fc.units,
                              (case when fc.scaled_quantity is null then 'Unconstrained' else sum(fc.scaled_quantity) end)
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
                    value = row[3]
                if facility_type == "processor" and io == 'o':
                    measure = "processing_output_capacity"
                    value = row[5]
                if facility_type == "processor" and io == 'i':
                    measure = "processing_input_capacity"
                    value = row[5]
                if facility_type == "ultimate_destination":
                    measure = "demand"
                    value = row[3]
                writer.writerow([the_scenario.scenario_name, "total_supply_demand_proc", row[0], "all_{}".format(row[1]), measure, io, value, row[4], None])

            # Scenario Stranded Supply, Demand, and Processing Capacity
            # note: stranded supply refers to facilities that are ignored from the analysis.")
            # -------------------------------------------------------------------------
            sql = """ select c.commodity_name, fti.facility_type, io, sum(fc.quantity), fc.units, f.ignore_facility,
                      (case when fc.scaled_quantity is null then 'Unconstrained' else sum(fc.scaled_quantity) end)
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
                    measure = "stranded_supply"
                    value = row[3]
                if facility_type == "processor" and io == 'o':
                    measure = "stranded_processing_output_capacity"
                    value = row[6]
                if facility_type == "processor" and io == 'i':
                    measure = "stranded_processing_input_capacity"
                    value = row[6]
                if facility_type == "ultimate_destination":
                    measure = "stranded_demand"
                    value = row[3]
                writer.writerow([the_scenario.scenario_name, "stranded_supply_demand_proc", row[0], "stranded_{}".format(row[1]), measure, io, value, row[4], row[5]]) # ignore_facility note

            # report out net quantities with ignored facilities removed from the query
            # note: net supply, demand, and processing capacity ignores facilities not connected to the network
            # -------------------------------------------------------------------------
            sql = """   select c.commodity_name, fti.facility_type, io, sum(fc.quantity), fc.units,
                        (case when fc.scaled_quantity is null then 'Unconstrained' else sum(fc.scaled_quantity) end) 
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
                    measure = "net_supply"
                    value = row[3]
                if facility_type == "processor" and io == 'o':
                    measure = "net_processing_output_capacity"
                    value = row[5]
                if facility_type == "processor" and io == 'i':
                    measure = "net_processing_input_capacity"
                    value = row[5]
                if facility_type == "ultimate_destination":
                    measure = "net_demand"
                    value = row[3]
                writer.writerow([the_scenario.scenario_name, "net_supply_demand_proc", row[0], "net_{}".format(row[1]), measure, io, value, row[4], None])

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
    
    # -------------------------------------------------------------

    # artificial link summary
    generate_artificial_link_summary(timestamp_directory, the_scenario, logger)

    # routes summary
    if the_scenario.ndrOn:
        generate_edges_from_routes_summary(timestamp_directory, the_scenario, logger)

    # emissions summary
    if the_scenario.detailed_emissions_data != 'None':
        generate_detailed_emissions_summary(timestamp_directory, the_scenario, logger)

    # tableau workbook
    prepare_tableau_assets(timestamp_directory, the_scenario, logger)

    logger.result("Reports located here: {}".format(timestamp_directory))

