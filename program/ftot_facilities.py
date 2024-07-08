# ---------------------------------------------------------------------------------------------------
# Name: ftot_facilities.py
#
# Purpose: Setup for raw material producers (RMP), processors, and ultimate destinations.
# Adds demand for destinations. Adds supply for RMP.
# Hooks facilities into the network using artificial links.
# Special consideration for how pipeline links are added.
#
# ---------------------------------------------------------------------------------------------------

import ftot_supporting
import ftot_supporting_gis
from ftot_pulp import generate_schedules
import arcpy
import datetime
import os
import csv
import sqlite3
from ftot import ureg, Q_
from six import iteritems


# ===============================================================================


def facilities(the_scenario, logger):
    gis_clean_fc(the_scenario, logger)
    gis_populate_fc(the_scenario, logger)

    db_cleanup_tables(the_scenario, logger)
    db_populate_tables(the_scenario, logger)
    db_report_commodity_potentials(the_scenario, logger)

    if the_scenario.processors_candidate_slate_data != 'None':
        # make candidate_process_list and candidate_process_commodities tables
        from ftot_processor import generate_candidate_processor_tables
        generate_candidate_processor_tables(the_scenario, logger)


# ===============================================================================


def db_drop_table(the_scenario, table_name, logger):
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        logger.debug("drop the {} table".format(table_name))
        main_db_con.execute("drop table if exists {};".format(table_name))


# ==============================================================================


def db_cleanup_tables(the_scenario, logger):
    with sqlite3.connect(the_scenario.main_db) as main_db_con:

        # DB CLEAN UP
        # ------------
        logger.info("start: db_cleanup_tables")

        # a new run is a new scenario in the main.db
        # so drop and create the following tables if they exists
        # --------------------------------------------

        # locations table
        logger.debug("drop the locations table")
        main_db_con.execute("drop table if exists locations;")
        logger.debug("create the locations table")
        main_db_con.executescript(
            "create table locations(location_ID INTEGER PRIMARY KEY, shape_x real, shape_y real, ignore_location text);")

        # tmp_facility_locations table
        # a temp table used to map the facility_name from the facility_commodities data csv
        # to the location_id. its used to populate the facility_commodities table
        # and deleted after it is populated.
        logger.debug("drop the tmp_facility_locations table")
        main_db_con.execute("drop table if exists tmp_facility_locations;")
        logger.debug("create the tmp_facility_locations table")
        main_db_con.executescript(
            "create table tmp_facility_locations(location_ID INTEGER, facility_name text PRIMARY KEY);")

        # facilities table
        logger.debug("drop the facilities table")
        main_db_con.execute("drop table if exists facilities;")
        logger.debug("create the facilities table")
        main_db_con.executescript(
            """create table facilities(facility_ID INTEGER PRIMARY KEY, location_id integer, facility_name text, facility_type_id integer, 
            ignore_facility text, candidate integer, schedule_id integer, max_capacity_ratio numeric, build_cost numeric, min_capacity_ratio numeric, 
            availability numeric, capacity_scaling numeric, scaling_factor numeric);""")

        # facility_type_id table
        logger.debug("drop the facility_type_id table")
        main_db_con.execute("drop table if exists facility_type_id;")
        logger.debug("create the facility_type_id table")
        main_db_con.executescript("create table facility_type_id(facility_type_id INTEGER PRIMARY KEY, facility_type text);")

        # phase_of_matter_id table
        logger.debug("drop the phase_of_matter_id table")
        main_db_con.execute("drop table if exists phase_of_matter_id;")
        logger.debug("create the phase_of_matter_id table")
        main_db_con.executescript(
            "create table phase_of_matter_id(phase_of_matter_id INTEGER PRIMARY KEY, phase_of_matter text);")

        # facility_commodities table
        # tracks the relationship of facility and commodities in or out
        logger.debug("drop the facility_commodities table")
        main_db_con.execute("drop table if exists facility_commodities;")
        logger.debug("create the facility_commodities table")
        main_db_con.executescript(
            "create table facility_commodities(facility_id integer, location_id integer, commodity_id interger, "
            "quantity numeric, units text, io text, share_max_transport_distance text, scaled_quantity numeric);")

        # commodities table
        logger.debug("drop the commodities table")
        main_db_con.execute("drop table if exists commodities;")
        logger.debug("create the commodities table")
        main_db_con.executescript(
            """create table commodities(commodity_ID INTEGER PRIMARY KEY, commodity_name text, supertype text, subtype text,
            units text, phase_of_matter text, max_transport_distance numeric, proportion_of_supertype numeric,
            share_max_transport_distance text, CONSTRAINT unique_name UNIQUE(commodity_name) );""")
        # proportion_of_supertype specifies how much demand is satisfied by this subtype relative to the "pure"
        # fuel/commodity. this will depend on the process

        # schedule_names table
        logger.debug("drop the schedule names table")
        main_db_con.execute("drop table if exists schedule_names;")
        logger.debug("create the schedule names table")
        main_db_con.executescript(
            """create table schedule_names(schedule_id INTEGER PRIMARY KEY, schedule_name text, tot_availability numeric);""")

        # schedules table
        logger.debug("drop the schedules table")
        main_db_con.execute("drop table if exists schedules;")
        logger.debug("create the schedules table")
        main_db_con.executescript(
            """create table schedules(schedule_id integer, day integer, availability numeric);""")

        # coprocessing reference table
        logger.debug("drop the coprocessing table")
        main_db_con.execute("drop table if exists coprocessing;")
        logger.debug("create the coprocessing table")
        main_db_con.executescript(
            """create table coprocessing(coproc_id integer, label text, description text);""")

        logger.debug("finished: main.db cleanup")


# ===============================================================================


def db_populate_tables(the_scenario, logger):
    logger.info("start: db_populate_tables")

    # populate schedules table
    populate_schedules_table(the_scenario, logger)

    # populate locations table
    populate_locations_table(the_scenario, logger)

    # populate coprocessing table
    populate_coprocessing_table(the_scenario, logger)

    # populate the facilities, commodities, and facility_commodities table with the input CSVs
    # Note: processor_candidates_commodity_data is generated for FTOT generated candidate
    # processors at the end of the candidate generation step
    facility_commodities_dict = {"rmp": the_scenario.rmp_commodity_data,
                                 "dest": the_scenario.destinations_commodity_data,
                                 "proc": the_scenario.processors_commodity_data,
                                 "proc_ftot": the_scenario.processor_candidates_commodity_data}

    for input_file_type in facility_commodities_dict:
        commodity_input_file = facility_commodities_dict[input_file_type]
        # this should just catch processors not specified
        if str(commodity_input_file).lower() == "null" or str(commodity_input_file).lower() == "none":
            logger.debug("Null or None Commodity Input Data specified in the XML: {}".format(commodity_input_file))
            continue

        else:
            populate_facility_commodities_table(the_scenario, input_file_type, commodity_input_file, logger)

    # check if there are multiple input commodities for a processor
    # re issue #109--this is a good place to check if there are multiple input commodities for a processor
    db_check_multiple_input_commodities_for_processor(the_scenario, logger)

    # delete the tmp_facility_locations table
    db_drop_table(the_scenario, "tmp_facility_locations", logger)

    logger.debug("finished: db_populate_tables")



# ===================================================================================================    


def db_report_commodity_potentials(the_scenario, logger):
    logger.info("start: db_report_commodity_potentials")

    # This query pulls the total quantity of each commodity from the facility_commodities table.
    # It groups by commodity_name, facility type, and units. The io field is included to help the user
    # determine the potential supply, demand, and processing capabilities in the scenario.

    # -----------------------------------
    with sqlite3.connect(the_scenario.main_db) as db_con:
        sql = """ select c.commodity_name, fti.facility_type, fc.io,
                  (case when sum(case when fc.scaled_quantity is null then 1 else 0 end) = 0 then sum(fc.scaled_quantity)
                  else 'Unconstrained' end) as scaled_quantity,
                  fc.units
                  from facility_commodities fc
                  join commodities c on fc.commodity_id = c.commodity_id
                  join facilities f on f.facility_id = fc.facility_id
                  join facility_type_id fti on fti.facility_type_id = f.facility_type_id
                  group by c.commodity_name, fti.facility_type, fc.io, fc.units
                  order by c.commodity_name, fc.io asc;"""
        db_cur = db_con.execute(sql)

        db_data = db_cur.fetchall()
        logger.result("-------------------------------------------------------------------")
        logger.result("Scenario Total Supply and Demand, and Available Processing Capacity")
        logger.result("-------------------------------------------------------------------")
        logger.result("note: processor inputs and outputs are based on facility size and ")
        logger.result("reflect a processing capacity, not a conversion of the scenario feedstock supply")
        logger.result("commodity_name | facility_type | io |    quantity   |     units     ")
        logger.result("---------------|---------------|----|---------------|---------------")
        for row in db_data:
            if (type(row[3]) == str):
                logger.result("{:15.15} {:15.15} {:4.1} {:15.15} {:15.15}".format(row[0], row[1], row[2], row[3], row[4]))
            else:
                logger.result("{:15.15} {:15.15} {:4.1} {:15,.2f} {:15.15}".format(row[0], row[1], row[2], row[3], row[4]))
        logger.result("-------------------------------------------------------------------")
        # Add the ignored processing capacity.
        # Note this doesn't happen until the bx step.
        # -------------------------------------------
        sql = """ select c.commodity_name, fti.facility_type, fc.io,
                  (case when sum(case when fc.scaled_quantity is null then 1 else 0 end) = 0 then sum(fc.scaled_quantity)
                  else 'Unconstrained' end) as scaled_quantity,
                  fc.units, f.ignore_facility
                  from facility_commodities fc
                  join commodities c on fc.commodity_id = c.commodity_id
                  join facilities f on f.facility_id = fc.facility_id
                  join facility_type_id fti on fti.facility_type_id = f.facility_type_id
                  where f.ignore_facility != 'false'
                  group by c.commodity_name, fti.facility_type, fc.io, fc.units, f.ignore_facility
                  order by c.commodity_name, fc.io asc;"""
        db_cur = db_con.execute(sql)

        db_data = db_cur.fetchall()
        if len(db_data) > 0:
            logger.result("-------------------------------------------------------------------")
            logger.result("Scenario Stranded Supply, Demand, and Processing Capacity")
            logger.result("-------------------------------------------------------------------")
            logger.result("note: stranded supply refers to facilities that are ignored from the analysis")
            logger.result("commodity_name | facility_type | io |    quantity   |     units     | ignored  ")
            logger.result("---------------|---------------|----|---------------|---------------|----------")
            for row in db_data:
                if (type(row[3]) == str):
                    logger.result("{:15.15} {:15.15} {:4.1} {:15.15} {:15.15} {:15.10}".format(row[0], row[1], row[2], row[3], row[4], row[5]))
                else:
                    logger.result("{:15.15} {:15.15} {:4.1} {:15,.2f} {:15.15} {:15.10}".format(row[0], row[1], row[2], row[3], row[4], row[5]))
            logger.result("-------------------------------------------------------------------")

            # Report out net quantities with ignored facilities removed from the query
            # -------------------------------------------------------------------------
            sql = """ select c.commodity_name, fti.facility_type, fc.io,
                      (case when sum(case when fc.scaled_quantity is null then 1 else 0 end) = 0 then sum(fc.scaled_quantity)
                      else 'Unconstrained' end) as scaled_quantity,
                      fc.units
                      from facility_commodities fc
                      join commodities c on fc.commodity_id = c.commodity_id
                      join facilities f on f.facility_id = fc.facility_id
                      join facility_type_id fti on fti.facility_type_id = f.facility_type_id
                      where f.ignore_facility == 'false'
                      group by c.commodity_name, fti.facility_type, fc.io, fc.units
                      order by c.commodity_name, fc.io asc;"""
            db_cur = db_con.execute(sql)

            db_data = db_cur.fetchall()
            logger.result("-------------------------------------------------------------------")
            logger.result("Scenario Net Supply and Demand, and Available Processing Capacity")
            logger.result("-------------------------------------------------------------------")
            logger.result("note: net supply, demand, and processing capacity ignore facilities not connected to the network")
            logger.result("commodity_name | facility_type | io |    quantity   |     units     ")
            logger.result("---------------|---------------|----|---------------|---------------")
            for row in db_data:
                if (type(row[3]) == str):
                    logger.result("{:15.15} {:15.15} {:4.1} {:15.15} {:15.15}".format(row[0], row[1], row[2], row[3], row[4]))
                else:
                    logger.result("{:15.15} {:15.15} {:4.1} {:15,.2f} {:15.15}".format(row[0], row[1], row[2], row[3], row[4]))
            logger.result("-------------------------------------------------------------------")


# ===================================================================================================


def load_schedules_input_data(schedule_input_file, logger):

    logger.debug("start: load_schedules_input_data")

    if str(schedule_input_file).lower() == "null" or str(schedule_input_file).lower() == "none":
        logger.info('schedule file not specified.')
        return {'default': {0: 1}}  # return dict with global value of default schedule
    elif not os.path.exists(schedule_input_file):
        logger.warning("warning: cannot find schedule file: {}".format(schedule_input_file))
        return {'default': {0: 1}}

    # create temp dict to store schedule input
    schedules = {}

    # read through schedules input CSV
    with open(schedule_input_file, 'rt') as f:

        reader = csv.DictReader(f)

        schedule_headers = reader.fieldnames
        if ('schedule' not in schedule_headers or 'day' not in schedule_headers or 'availability' not in schedule_headers):
            logger.error("schedule file is missing required columns: schedule, day, availability")
            raise Exception("schedule file is missing required columns: schedule, day, availability")

        for index, row in enumerate(reader):

            schedule_name = str(row['schedule']).lower()    # convert schedule to lowercase
            day = int(row['day'])                           # cast day to an int
            availability = float(row['availability'])       # cast availability to float

            if schedule_name in list(schedules.keys()):
                schedules[schedule_name][day] = availability
            else:
                schedules[schedule_name] = {day: availability}  # initialize sub-dict

    # enforce default schedule req. and default availability req. for all schedules
    # if user has not defined 'default' schedule
    if 'default' not in schedules:
        logger.debug("Default schedule not found. Adding 'default' with default availability of 1.")
        schedules['default'] = {0: 1}
    # if schedule does not have a default value (value assigned to day 0), then add as 1
    for schedule_name in list(schedules.keys()):
        if 0 not in list(schedules[schedule_name].keys()):
            logger.debug("Schedule {} missing default value. Adding default availability of 1.".format(schedule_name))
            schedules[schedule_name][0] = 1

    return schedules


# ===================================================================================================


def populate_schedules_table(the_scenario, logger):

    logger.info("start: populate_schedules_table")

    schedules_dict = load_schedules_input_data(the_scenario.schedule, logger)

    # connect to db
    with sqlite3.connect(the_scenario.main_db) as db_con:
        id_num = 0

        for schedule_name, schedule_data in iteritems(schedules_dict):
            id_num += 1  # 1-index

            # add schedule name into schedule_names table
            sql = "insert into schedule_names " \
                  "(schedule_id, schedule_name) " \
                  "values ({},'{}');".format(id_num, schedule_name)
            db_con.execute(sql)

            # add each day into schedules table
            for day, availability in iteritems(schedule_data):
                sql = "insert into schedules " \
                      "(schedule_id, day, availability) " \
                      "values ({},{},{});".format(id_num, day, availability)
                db_con.execute(sql)

    # calculate average availabilities and update table
    schedule_dict, days = generate_schedules(the_scenario,logger)
    with sqlite3.connect(the_scenario.main_db) as db_con:
        for sched_id, sched_array in schedule_dict.items():
            # Find total availability over all schedule days
            availability = sum(sched_array)

            sql_add_tot_avail = """update schedule_names
                     set tot_availability={}
                     where schedule_id={};""".format(availability, sched_id)
            db_con.execute(sql_add_tot_avail)

    logger.debug("finished: populate_schedules_table")


# ==============================================================================


def check_for_input_error(input_file_type, input_type, input_val, filename, index, logger, units=None):
    """
    :param input_file_type: a string with the type of input file ('rmp', 'dest', 'proc', 'proc_cand', 'proc_ftot')
    :param input_type: a string with the type of input (e.g., 'io', 'facility_name', etc.)
    :param input_val: a string from the CSV with the actual input value
    :param filename: the name of the file containing the row
    :param index: the row index
    :param logger: logger object to record error
    :param units: string, units used -- only used if input_type == 'commodity_phase'
    :return: None if data is valid or error message otherwise (but should raise Exception before returning)
    """
    error_message = None
    index = index+2  # accounts for header row and 0-indexing (Python) conversion to 1-indexing (Excel)
    if input_type == 'io':
        if input_file_type == 'rmp':
            if not (input_val in ['o']):
                error_message = "There is an error in the io entry in row {} of {}. " \
                                "Entries should be 'o' for {} CSV files.".format(index, filename, input_file_type)
        elif input_file_type == 'dest':
            if not (input_val in ['i']):
                error_message = "There is an error in the io entry in row {} of {}. " \
                                "Entries should be 'i' for {} CSV files.".format(index, filename, input_file_type)
        else:  # processor
            if not (input_val in ['i', 'o']):
                error_message = "There is an error in the io entry in row {} of {}. " \
                                "Entries should be 'i' or 'o' for {} CSV files.".format(index, filename, input_file_type)
    elif input_type == 'facility_type':
        if input_file_type == 'rmp':
            if not (input_val in ['raw_material_producer']):
                error_message = "There is an error in the facility_type entry in row {} of {}. " \
                                "The entry is not 'raw_material_producer'.".format(index, filename)
        elif input_file_type == 'dest':
            if not (input_val in ['ultimate_destination']):
                error_message = "There is an error in the facility_type entry in row {} of {}. " \
                                "The entry is not 'ultimate_destination'.".format(index, filename)
        else:  # processor
            if not (input_val in ['processor']):
                error_message = "There is an error in the facility_type entry in row {} of {}. " \
                                "The entry is not 'processor'.".format(index, filename)
    elif input_type == 'commodity_phase':
        # make sure units are specified
        if units is None or units == '':
            error_message = "The units in row {} of {} are not specified. Note that solids must have units of mass " \
                            "and liquids must have units of volume.".format(index, filename)
        elif input_val == 'solid':
            # check if units are valid units for solid (dimension of units must be mass)
            try:
                if not str(ureg(units).dimensionality) == '[mass]':
                    error_message = "The phase_of_matter entry in row {} of {} is solid, but the units are {}" \
                                    " which is not a valid unit for this phase of matter. Solids must be measured in " \
                                    "units of mass.".format(index, filename, units)
            except:
                error_message = "The phase_of_matter entry in row {} of {} is solid, but the units are unable" \
                                " to be interpreted. Check input for errors. Solids must be measured in " \
                                "units of mass.".format(index, filename)
        elif input_val == 'liquid':
            # check if units are valid units for liquid (dimension of units must be volume, aka length^3)
            try:
                if not str(ureg(units).dimensionality) == '[length] ** 3':
                    error_message = "The phase_of_matter entry in row {} of {} is liquid, but the units are {}" \
                                    " which is not a valid unit for this phase of matter. Liquids must be measured" \
                                    " in units of volume.".format(index, filename, units)
            except:
                error_message = "The phase_of_matter entry in row {} of {} is liquid, but the units are unable" \
                                " to be interpreted. Check input for errors. Liquids must be measured" \
                                " in units of volume.".format(index, filename)
        else:
            # throw error that phase is neither solid nor liquid
            error_message = "There is an error in the phase_of_matter entry in row {} of {}. " \
                            "The entry is not one of 'solid' or 'liquid'.".format(index, filename)
    elif input_type == 'commodity_quantity':
        try:
            float(input_val)
        except ValueError:
            error_message = "There is an error in the value entry in row {} of {}. " \
                            "The entry is non-numeric (check for extraneous characters)." \
                            .format(index, filename)
    elif input_type == 'max_capacity' or input_type == 'min_capacity':
        if "proc" in input_file_type:
            try:
                float(input_val)
            except ValueError:
                error_message = "There is an error in the {} entry in row {} of {}. " \
                                "The entry is non-numeric (check for extraneous characters)." \
                                .format(input_type, index, filename)
        else:
            warning_message = "The '{}' column is only applicable to processor " \
                              "facilities. Ignoring entry in row {} of {}." \
                              .format(input_type, index, filename)
            logger.warning(warning_message)
    elif input_type == 'max_processor_input' or input_type == 'min_processor_input':
        if "proc" in input_file_type:
            try:
                float(input_val)
            except ValueError:
                error_message = "There is an error in the {} entry in row {} of {}. " \
                                "The entry is non-numeric (check for extraneous characters)." \
                                .format(input_type, index, filename)
        else:
            warning_message = "The '{}' column is only applicable to processor " \
                              "facilities. Ignoring entry in row {} of {}." \
                              .format(input_type, index, filename)
            logger.warning(warning_message)
    elif input_type == 'commodity_max_transport_distance':
        if "rmp" in input_file_type:
            try:
                float(input_val)
            except ValueError:
                error_message = "There is an error in the {} entry in row {} of {}. " \
                                "The entry is non-numeric (check for extraneous characters)." \
                                .format(input_type, index, filename)
        else:
            warning_message = "The '{}' column is only applicable to raw material producer " \
                              "facilities. Ignoring entry in row {} of {}." \
                              .format(input_type, index, filename)
            logger.warning(warning_message)
    elif input_type == 'share_max_transport_distance':
        if "proc" in input_file_type:
            if not (input_val in ['Y', 'N']):
                error_message = "There is an error in the {} entry in row {} of {}. " \
                                "Entries should be 'Y' or 'N'.".format(input_type, index, filename)
        else:
            warning_message = "The '{}' column is only applicable to processor " \
                              "facilities. Ignoring entry in row {} of {}." \
                              .format(input_type, index, filename)
            logger.warning(warning_message)
    elif input_type == 'build_cost':
        if "proc" in input_file_type:
            try:
                float(input_val)
            except ValueError:
                error_message = "There is an error in the build_cost entry in row {} of {}. " \
                                "The entry is empty or non-numeric (check for extraneous characters)." \
                                .format(index, filename)
        else:
            warning_message = "The 'build_cost' column is only applicable to processor " \
                              "facilities. Ignoring entry in row {} of {}.".format(index, filename)
            logger.warning(warning_message)

    if error_message:
        logger.error(error_message)
        raise Exception(error_message)


# ==============================================================================


def load_facility_commodities_input_data(the_scenario, input_file_type, commodity_input_file, logger):
    # input_file_type takes values: rmp, dest, proc, proc_cand
    # input_file_type is used for input validation checks
    logger.debug("start: load_facility_commodities_input_data")

    # create a temp dict to store values from CSV
    temp_facility_commodities_dict = {}

    # create empty dictionary to manage schedule input
    facility_schedule_dict = {}

    # read through facility_commodities input CSV
    with open(commodity_input_file, 'rt') as f:

        reader = csv.DictReader(f)

        # fieldnames facility_name and value already checked in previous method
        for field in ["io", "facility_type", "commodity", "units", "phase_of_matter"]:
            if field not in reader.fieldnames:
                error = "The facility_commodities input CSV {} must have field {}.".format(commodity_input_file, field)
                logger.error(error)
                raise Exception(error)

        # row index is used to alert user to which row their input error is in
        for index, row in enumerate(reader):
            # if facility_name is empty, skip the row
            if str(row["facility_name"]) == '':
                logger.debug('The CSV file has a blank in the "facility_name" column. Skipping this line: {}'.format(
                    list(row.values())))
                continue

            # DictReader reads empty cells as empty strings (e.g., for proc_cand-specific "non-commodities" rows)
            facility_name      = str(row["facility_name"])
            facility_type      = str(row["facility_type"]).strip().lower()  # remove spaces and make lowercase
            io                 = str(row["io"]).strip().lower()  # remove spaces and make lowercase
            commodity_name     = str(row["commodity"]).strip().lower()  # remove spaces and make lowercase
            commodity_quantity = row["value"]
            commodity_unit     = str(row["units"]).strip().replace(' ', '_').lower()  # remove all spaces and make lowercase
            commodity_phase    = str(row["phase_of_matter"]).strip().lower()  # remove spaces and make lowercase

            # check for proc_cand-specific "non-commodities" to ignore validation and process proc-specific "total" rows
            non_commodities = ['minsize', 'maxsize', 'cost_formula', 'min_aggregation', 'total']

            # set to 1 if there is a max_processor_input column that should be recorded as total capacity
            # this occurs in addition to the normal processing of the commodity on that row
            record_min_max_processor_input_as_total = 0

            # input data validation
            if commodity_name not in non_commodities:
                # test facility type
                check_for_input_error(input_file_type, "facility_type", facility_type, commodity_input_file, index, logger)
                # test io
                check_for_input_error(input_file_type, "io", io, commodity_input_file, index, logger)
                # test commodity quantity
                check_for_input_error(input_file_type, "commodity_quantity", commodity_quantity,
                                      commodity_input_file, index, logger)
                # test commodity unit and phase
                check_for_input_error(input_file_type, "commodity_phase", commodity_phase,
                                      commodity_input_file, index, logger, units=commodity_unit)
            elif commodity_name == 'total':
                # test facility type
                check_for_input_error(input_file_type, "facility_type", facility_type, commodity_input_file, index, logger)
                # test io
                check_for_input_error(input_file_type, "io", io, commodity_input_file, index, logger)
                # test commodity unit and phase
                check_for_input_error(input_file_type, "commodity_phase", commodity_phase,
                                      commodity_input_file, index, logger, units=commodity_unit)
                # warn for liquid total - cannot check matching commodities until all are read in
                if commodity_phase == "liquid":
                    logger.warning("Note: Liquid units for total capacity requires that all contributing {} commodities also have liquid units".format(io))
                # prevent type errors by filling blank value with 0 and capacity errors by forcing any entries to 0
                commodity_quantity = "0.0"
            else:
                logger.debug("Skipping input validation on special candidate processor row: {}".format(commodity_name))

            if "max_capacity" in list(row.keys()) and row["max_capacity"]:
                max_capacity = row["max_capacity"]
                check_for_input_error(input_file_type, "max_capacity", max_capacity,
                                      commodity_input_file, index, logger)
            else:
                max_capacity = "Null"

            if "min_capacity" in list(row.keys()) and row["min_capacity"]:
                min_capacity = row["min_capacity"]
                check_for_input_error(input_file_type, "min_capacity", min_capacity,
                                      commodity_input_file, index, logger)
            else:
                min_capacity = "Null"

            if "max_processor_input" in list(row.keys()) and row["max_processor_input"]:
                max_processor_input = row["max_processor_input"]
                check_for_input_error(input_file_type, "max_processor_input", max_processor_input,
                                      commodity_input_file, index, logger)
                record_min_max_processor_input_as_total = 1
            else:
                max_processor_input = "Null"

            if "min_processor_input" in list(row.keys()) and row["min_processor_input"]:
                min_processor_input = row["min_processor_input"]
                check_for_input_error(input_file_type, "min_processor_input", min_processor_input,
                                      commodity_input_file, index, logger)
                record_min_max_processor_input_as_total = 1
            else:
                min_processor_input = "Null"

            if "max_transport_distance" in list(row.keys()) and row["max_transport_distance"]:
                commodity_max_transport_distance = row["max_transport_distance"]
                check_for_input_error(input_file_type, "commodity_max_transport_distance",
                                      commodity_max_transport_distance, commodity_input_file,
                                      index, logger)
            else:
                commodity_max_transport_distance = "Null"

            if "share_max_transport_distance" in list(row.keys()) and row["share_max_transport_distance"]:
                share_max_transport_distance = row["share_max_transport_distance"]
                check_for_input_error(input_file_type, "share_max_transport_distance",
                                      share_max_transport_distance, commodity_input_file,
                                      index, logger)
            else:
                share_max_transport_distance = 'N'
            
            # set to 0 if blank, otherwise convert to numerical after checking for extra characters
            if "build_cost" in list(row.keys()) and row["build_cost"]:
                build_cost = row["build_cost"]
                check_for_input_error(input_file_type, "build_cost", build_cost, commodity_input_file,
                                      index, logger, units=commodity_unit)
                build_cost = float(build_cost)
            else:
                build_cost = 0   
         
            if build_cost > 0:
                candidate_flag = 1
            else:
                candidate_flag = 0

            # add schedule_id if available
            if "schedule" in list(row.keys()) and row["schedule"]:
                schedule_name = str(row["schedule"]).lower()

                # schedule name "none" should be cast to default
                if schedule_name == "none":
                    schedule_name = "default"
            else:
                schedule_name = "default"

            # manage facility_schedule_dict
            if facility_name not in facility_schedule_dict:
                facility_schedule_dict[facility_name] = schedule_name
            elif facility_schedule_dict[facility_name] != schedule_name:
                logger.warning("Schedule name '{}' does not match previously entered schedule '{}' for facility '{}'".
                               format(schedule_name, facility_schedule_dict[facility_name], facility_name))
                schedule_name = facility_schedule_dict[facility_name]

            # use pint to set the quantity and units
            if commodity_name == 'cost_formula':  # first check currency units
                currency = commodity_unit.split("/")[0]
                assert currency.lower() == the_scenario.default_units_currency, "Cost formula must use default currency units from the scenario XML."

            commodity_quantity_and_units = Q_(float(commodity_quantity), commodity_unit)
            
            if max_capacity != 'Null':
                max_capacity_quantity_and_units = Q_(float(max_capacity), commodity_unit)
            if min_capacity != 'Null':
                min_capacity_quantity_and_units = Q_(float(min_capacity), commodity_unit)
            if max_processor_input != 'Null':
                max_processor_input_quantity_and_units = Q_(float(max_processor_input), commodity_unit)
            if min_processor_input != 'Null':
                min_processor_input_quantity_and_units = Q_(float(min_processor_input), commodity_unit)

            if commodity_phase == 'liquid':
                commodity_unit = the_scenario.default_units_liquid_phase
            if commodity_phase == 'solid':
                commodity_unit = the_scenario.default_units_solid_phase

            if commodity_name == 'cost_formula':  # handle cost formula units
                commodity_unit = commodity_unit.split("/")[-1]  # get the denominator from string version
                if str(ureg(commodity_unit).dimensionality) == '[length] ** 3':  # if denominator unit looks like a volume
                    commodity_phase = 'liquid'  # then phase is liquid
                    commodity_unit = Q_(1, the_scenario.default_units_currency).units/the_scenario.default_units_liquid_phase  # and cost unit phase should use default liquid
                elif str(ureg(commodity_unit).dimensionality) == '[mass]':  # if denominator unit looks like a mass
                    commodity_phase = 'solid'  # then phase is solid
                    commodity_unit = Q_(1, the_scenario.default_units_currency).units/the_scenario.default_units_solid_phase  # and cost unit phase should use default solid
                else:
                    error = "Cost formula must be provided per unit of mass or volume, depending on phase of matter of candidate process input commodity."
                    logger.error(error)
                    raise Exception(error)
            
            # now that we have handled cost_formula, all properties can use this (with try statement in case)
            try:
                commodity_quantity = commodity_quantity_and_units.to(commodity_unit).magnitude

                if max_capacity != 'Null':
                    max_capacity = max_capacity_quantity_and_units.to(commodity_unit).magnitude
                if min_capacity != 'Null':
                    min_capacity = min_capacity_quantity_and_units.to(commodity_unit).magnitude
                if max_processor_input != 'Null':
                    max_processor_input = max_processor_input_quantity_and_units.to(commodity_unit).magnitude
                if min_processor_input != 'Null':
                    min_processor_input = min_processor_input_quantity_and_units.to(commodity_unit).magnitude

            except Exception as e:
                logger.error("FAIL: {} ".format(e))
                raise Exception("FAIL: {}".format(e))
            
            # add to the dictionary of facility_commodities mapping
            if facility_name not in list(temp_facility_commodities_dict.keys()):
                temp_facility_commodities_dict[facility_name] = []

            temp_facility_commodities_dict[facility_name].append([facility_type, commodity_name, commodity_quantity,
                                                                  commodity_unit, commodity_phase,
                                                                  commodity_max_transport_distance, io,
                                                                  share_max_transport_distance, min_capacity,
                                                                  candidate_flag, build_cost, max_capacity,
                                                                  schedule_name])
            
            if record_min_max_processor_input_as_total == 1:
                temp_facility_commodities_dict[facility_name].append([facility_type, 'total', '',
                                                                      commodity_unit, commodity_phase,
                                                                      "Null", io,
                                                                      'N', min_processor_input,
                                                                      0, 0, max_processor_input,
                                                                      schedule_name])
            # reset flag for recording legacy total
            record_min_max_processor_input_as_total = 0
            
    logger.debug("finished: load_facility_commodities_input_data")
    return temp_facility_commodities_dict


# ==============================================================================


def populate_facility_commodities_table(the_scenario, input_file_type, commodity_input_file, logger):
    # input_file_type takes values: rmp, dest, proc, proc_cand
    # input_file_type is used for input validation checks
    logger.debug("start: populate_facility_commodities_table for {}".format(commodity_input_file))

    if not os.path.exists(commodity_input_file):
        logger.debug("note: cannot find commodity_input_file: {}".format(commodity_input_file))
        return

    facility_commodities_dict = load_facility_commodities_input_data(the_scenario, input_file_type, commodity_input_file, logger)

    # recognize generated candidate processors
    candidate = 0
    generated_candidate = 0
    if os.path.split(commodity_input_file)[1].find("ftot_generated_processor_candidates") > -1:
        generated_candidate = 1

    # connect to main.db and add values to table
    # ---------------------------------------------------------
    with sqlite3.connect(the_scenario.main_db) as db_con:
        for facility_name, facility_data in iteritems(facility_commodities_dict):
            logger.debug("starting DB processing for facility_name: {}".format(facility_name))
            
            # unpack the facility_type (should be the same for all entries)
            facility_type = facility_data[0][0]
            facility_type_id = get_facility_id_type(the_scenario, db_con, facility_type, logger)

            location_id = get_facility_location_id(the_scenario, db_con, facility_name, logger)

            # get schedule id from the db
            schedule_name = facility_data[0][-1]
            schedule_id, total_availability = get_schedule_id(the_scenario, db_con, schedule_name, logger, get_avail=True)

            build_cost = facility_data[0][-3]
            
            # recognize candidate processors from proc.csv or generated file
            candidate_flag = facility_data[0][-4]
            if candidate_flag == 1 or generated_candidate == 1:
                candidate = 1
            else:
                candidate = 0

            # placeholders for capacity calculation
            overall_min_ratio = 'Null'
            overall_max_ratio = 'Null'
            running_total = {('i', 'solid'): Q_(0, the_scenario.default_units_solid_phase)}
            running_total['o', 'solid'] = Q_(0, the_scenario.default_units_solid_phase)
            running_total['i', 'liquid'] = Q_(0, the_scenario.default_units_liquid_phase)
            running_total['o', 'liquid'] = Q_(0, the_scenario.default_units_liquid_phase)
            check_total = {}

            # get the facility_id from the db (add the facility if it doesn't exist)
            # and set up entry in facility_id table
            facility_id = get_facility_id(the_scenario, db_con, location_id, facility_name, facility_type_id, candidate, schedule_id, total_availability, overall_max_ratio, build_cost, overall_min_ratio, logger)

            # iterate through each commodity
            for commodity_data in facility_data:

                [facility_type, commodity_name, commodity_quantity, commodity_units, commodity_phase, commodity_max_transport_distance, io, share_max_transport_distance, min_capacity, candidate_data, build_cost, max_capacity, schedule_id] = commodity_data
                
                if commodity_name != 'total':
                    # get commodity_id (add the commodity if it doesn't exist)
                    commodity_id = get_commodity_id(the_scenario, db_con, commodity_data, logger)

                    if not commodity_quantity == "0.0":  # skip anything with no material
                        sql = "insert into facility_commodities " \
                              "(facility_id, location_id, commodity_id, quantity, units, io, share_max_transport_distance) " \
                              "values ('{}', '{}', '{}', '{}', '{}', '{}', '{}');".format(facility_id, location_id,
                                                                                          commodity_id, commodity_quantity,
                                                                                          commodity_units, io,
                                                                                          share_max_transport_distance)
                        db_con.execute(sql)

                        # if the capacity for this commodity is more constraining than any to date, update overall ratio
                        if min_capacity != 'Null':
                            if overall_min_ratio == 'Null':
                                overall_min_ratio = min_capacity/commodity_quantity
                            else:
                                if min_capacity/commodity_quantity > overall_min_ratio:
                                    overall_min_ratio = min_capacity/commodity_quantity
                            
                        if max_capacity != 'Null':
                            if overall_max_ratio == 'Null':
                                overall_max_ratio = max_capacity/commodity_quantity
                            else:
                                if max_capacity/commodity_quantity < overall_max_ratio:
                                    overall_max_ratio = max_capacity/commodity_quantity
                    else:
                        logger.debug("skipping commodity_data {} because quantity: {}".format(commodity_name, commodity_quantity))
                else:  # total row
                    # if total row is liquid, all contributing commodities must be liquid else throw error 
                    if min_capacity != 'Null':
                        check_total[io, commodity_phase] = {'min': Q_(float(min_capacity), commodity_units)}
                    if max_capacity != 'Null':
                        check_total[io, commodity_phase] = {'max': Q_(float(max_capacity), commodity_units)}

            db_con.execute("""update commodities
            set share_max_transport_distance = 
            (select 'Y' from facility_commodities fc
            where commodities.commodity_id = fc.commodity_id
            and fc.share_max_transport_distance = 'Y')
            where exists             (select 'Y' from facility_commodities fc
            where commodities.commodity_id = fc.commodity_id
            and fc.share_max_transport_distance = 'Y')
                ;"""
            )
            
            # cannot load density dict until commodities table is populated, at least for a given facility  
            if len(check_total) > 0:
                density_dict = ftot_supporting_gis.make_commodity_density_dict(the_scenario, logger)
                all_liquid = {'i': True, 'o': True}
                
                for row in db_con.execute("""select fc.io, fc.units, fc.quantity, c.commodity_name, c.phase_of_matter
                from facility_commodities fc, commodities c
                where fc.facility_id = {}
                and c.commodity_id = fc.commodity_id
                ;""".format(facility_id)):
                    io = row[0]
                    commodity_units = row[1]
                    commodity_quantity = row[2]
                    commodity_name = row[3]
                    phase_of_matter = row[4]
                    running_total[io, phase_of_matter] = running_total[io, phase_of_matter] + Q_(commodity_quantity, commodity_units)
                    if commodity_units == the_scenario.default_units_solid_phase:
                        all_liquid[io] = False
                    if commodity_units == the_scenario.default_units_liquid_phase:
                        # if liquid, convert and add to solid total
                        # this is double counting, but effectively it gives us one total for just liquids and one for everything (with converted liquids)
                        running_total[io, 'solid'] = running_total[io, 'solid'] + density_dict[commodity_name]*Q_(commodity_quantity, commodity_units)

                # if the capacity for this commodity is more constraining than any to date, update overall ratio
                for (io_phase_key, capacity_dict) in check_total.items():
                    capacity_io = io_phase_key[0]
                    capacity_phase = io_phase_key[1]
                    if capacity_phase == 'liquid' and not all_liquid[capacity_io]:
                        raise Exception("Error, processor {} min or max capacity with liquid units requires that all contributing commodities also be liquid".format(facility_name))
                    # check capacities
                    for (m_key, capacity_value) in capacity_dict.items():
                        if m_key == 'min':
                            if overall_min_ratio == 'Null' and running_total[io_phase_key].magnitude > 0:
                                overall_min_ratio = capacity_value.magnitude/(running_total[io_phase_key]).magnitude
                            else:
                                if capacity_value.magnitude/(running_total[io_phase_key]).magnitude > overall_min_ratio:
                                    overall_min_ratio = capacity_value.magnitude/(running_total[io_phase_key]).magnitude

                        elif m_key == 'max':
                            if overall_max_ratio == 'Null' and running_total[io_phase_key].magnitude > 0:
                                overall_max_ratio = capacity_value.magnitude/(running_total[io_phase_key]).magnitude
                            else:
                                if capacity_value.magnitude/(running_total[io_phase_key]).magnitude < overall_max_ratio:
                                    overall_max_ratio = capacity_value.magnitude/(running_total[io_phase_key]).magnitude                 
            
            
            db_con.execute("update facilities set max_capacity_ratio = {} where facility_id = {};".format(overall_max_ratio, facility_id))
            db_con.execute("update facilities set min_capacity_ratio = {} where facility_id = {};".format(overall_min_ratio, facility_id))
            
            if facility_type == "processor":
                capacity_scaling = overall_max_ratio
            else:
                capacity_scaling = 1
            db_con.execute("update facilities set capacity_scaling = {} where facility_id = {};".format(capacity_scaling, facility_id))
            
            if capacity_scaling != 'Null' :
                scaling_factor = total_availability * capacity_scaling
                db_con.execute("update facilities set scaling_factor = {} where facility_id = {};".format(scaling_factor, facility_id))
                db_con.execute("update facility_commodities set scaled_quantity = {} * quantity where facility_id = {};".format(scaling_factor, facility_id))

    logger.debug("finished: populate_facility_commodities_table")


# ==============================================================================


def db_check_multiple_input_commodities_for_processor(the_scenario, logger):
    # connect to main.db and add values to table
    # ---------------------------------------------------------
    with sqlite3.connect(the_scenario.main_db) as db_con:
        sql = """select f.facility_name, count(*) 
                    from facility_commodities fc
                    join facilities f on f.facility_ID = fc.facility_id
                    join facility_type_id fti on fti.facility_type_id = f.facility_type_id  
                    where fti.facility_type like 'processor' and fc.io like 'i'
                    group by f.facility_name
                    having count(*) > 1;"""
        db_cur = db_con.execute(sql)
        data = db_cur.fetchall()

    if len(data) > 0:
        for multi_input_processor in data:
            logger.debug("Processor: {} has {} input commodities specified.".format(multi_input_processor[0],
                                                                                    multi_input_processor[1]))
            # should check that shared max transport distance has a 'Y' first or look for max_transport_distance field
            logger.warning("Multiple processor inputs are not supported in the same scenario as shared max transport "
                           "distance")


# ==============================================================================


def populate_coprocessing_table(the_scenario, logger):

    logger.debug("start: populate_coprocessing_table")

    # connect to db
    with sqlite3.connect(the_scenario.main_db) as db_con:

        # should be filled from the file coprocessing.csv, which needs to be added to xml still
        # I would place this file in the common data folder probably, since it is a fixed reference table
        # for now, filling the db table here manually with the data from the csv file
        sql = """INSERT INTO coprocessing (coproc_id, label, description)
                 VALUES
                 (1, 'single', 'code should throw error if processor has more than one input commodity'),
                 (2, 'fixed combination', 'every input commodity listed for the processor is required, in the ratio ' ||
                                          'specified by their quantities. Output requires all inputs to be present; '),
                 (3, 'substitutes allowed', 'any one of the input commodities listed for the processor can be used ' ||
                                            'to generate the output, with the ratios specified by quantities. A ' ||
                                            'combination of inputs is allowed. '),
                 (4, 'external input', 'assumes all specified inputs are required, in that ratio, with the addition ' ||
                                       'of an External or Non-Transported input commodity. This is included in the ' ||
                                       'ratio and as part of the input capacity, but is available in unlimited ' ||
                                       'quantity at the processor location. ')
                 ; """
        db_con.execute(sql)

    logger.debug("not yet implemented: populate_coprocessing_table")


# =============================================================================


def populate_locations_table(the_scenario, logger):

    logger.info("start: populate_locations_table")

    # connect to db
    with sqlite3.connect(the_scenario.main_db) as db_con:

        # iterate through the GIS and get the facility_name, shape_x and shape_y
        with arcpy.da.Editor(the_scenario.main_gdb) as edit:

            logger.debug("iterating through the FC and create a facility_location mapping with x,y coord.")

            for fc in [the_scenario.rmp_fc, the_scenario.destinations_fc, the_scenario.processors_fc]:

                logger.debug("iterating through the FC: {}".format(fc))
                with arcpy.da.SearchCursor(fc, ["facility_name", "SHAPE@X", "SHAPE@Y"]) as cursor:
                    for row in cursor:
                        facility_name = row[0]
                        shape_x = round(row[1], 2)
                        shape_y = round(row[2], 2)

                        # check if location_id exists exists for snap_x and snap_y
                        location_id = get_location_id(the_scenario, db_con, shape_x, shape_y, logger)

                        if location_id > 0:
                            # map facility_name to the location_id in the tmp_facility_locations table
                            db_con.execute("insert or ignore into tmp_facility_locations "
                                           "(facility_name, location_id) "
                                           "values ('{}', '{}');".format(facility_name, location_id))
                        else:
                            error = "no location_id exists for shape_x {} and shape_y {}".format(shape_x, shape_y)
                            logger.error(error)
                            raise Exception(error)

    logger.info("finished: populate_locations_table")


# =============================================================================


def get_location_id(the_scenario, db_con, shape_x, shape_y, logger):

    location_id = None

    # get location_id
    for row in db_con.execute("""
                              select 
                              location_id 
                              from locations l 
                              where l.shape_x = '{}' and l.shape_y = '{}';""".format(shape_x, shape_y)):

        location_id = row[0]

    # if no ID, add it
    if location_id is None:
        # if it doesn't exist, add to locations table and generate a location id
        db_cur = db_con.execute("insert into locations (shape_x, shape_y) values ('{}', '{}');".format(shape_x, shape_y))
        location_id = db_cur.lastrowid

    # check again, we should have the ID now, if we don't throw an error
    if location_id is None:
        error = "something went wrong getting location_id shape_x: {}, shape_y: {} location_id ".format(shape_x, shape_y)
        logger.error(error)
        raise Exception(error)
    else:
        return location_id


# =============================================================================


def get_facility_location_id(the_scenario, db_con, facility_name, logger):

    # get location_id
    db_cur = db_con.execute("select location_id from tmp_facility_locations l where l.facility_name = '{}';".format(str(facility_name)))
    location_id = db_cur.fetchone()
    if not location_id:
        warning = "location_id for tmp_facility_name: {} is not found.".format(facility_name)
        logger.debug(warning)
    else:
        return location_id[0]


# =============================================================================


def get_facility_id(the_scenario, db_con, location_id, facility_name, facility_type_id, candidate, schedule_id, total_availability, overall_max_ratio, build_cost, overall_min_ratio, logger):

    # if it doesn't exist, add to facilities table and generate a facility id
    if build_cost > 0:
        # Specify ignore_facility = 'false'. Otherwise, the input-from-file candidates get ignored like excess generated candidates.
        ignore_facility = 'false'
        db_con.execute("insert or ignore into facilities "
                   "(location_id, facility_name, facility_type_id, ignore_facility, candidate, schedule_id, max_capacity_ratio, build_cost, min_capacity_ratio, availability) "
                   "values ('{}', '{}', {}, '{}',{}, {}, {}, {}, {}, {});".format(location_id, facility_name, facility_type_id, ignore_facility, candidate, schedule_id, overall_max_ratio, build_cost, overall_min_ratio, total_availability))
    else:
        db_con.execute("insert or ignore into facilities "
                   "(location_id, facility_name, facility_type_id, candidate, schedule_id, max_capacity_ratio, build_cost, min_capacity_ratio, availability) "
                   "values ('{}', '{}', {},  {}, {}, {}, {}, {}, {});".format(location_id, facility_name, facility_type_id, candidate, schedule_id, overall_max_ratio, build_cost, overall_min_ratio, total_availability))

    # get facility_id
    db_cur = db_con.execute("select facility_id "
                            "from facilities f "
                            "where f.facility_name = '{}' and facility_type_id = {};".format(facility_name, facility_type_id))
    facility_id = db_cur.fetchone()[0]

    if not facility_id:
        error = "something went wrong getting {} facility_id ".format(facility_id)
        logger.error(error)
        raise Exception(error)
    else:
        return facility_id


# ===================================================================================================


def get_facility_id_type(the_scenario, db_con, facility_type, logger):
    facility_type_id = None

    # get facility_id_type
    for row in db_con.execute("select facility_type_id "
                              "from facility_type_id f "
                              "where facility_type = '{}';".format(facility_type)):
        facility_type_id = row[0]

    # if it doesn't exist, add to facility_type table and generate a facility_type_id
    if facility_type_id is None:
        db_cur = db_con.execute("insert into facility_type_id (facility_type) values ('{}');".format(facility_type))
        facility_type_id = db_cur.lastrowid

    # check again if we have facility_type_id
    if facility_type_id is None:
        error = "something went wrong getting {} facility_type_id ".format(facility_type)
        logger.error(error)
        raise Exception(error)
    else:
        return facility_type_id


# ===================================================================================================


def get_commodity_id(the_scenario, db_con, commodity_data, logger):

    [facility_type, commodity_name, commodity_quantity, commodity_unit, commodity_phase, 
     commodity_max_transport_distance, io, share_max_transport_distance, max_capacity_ratio, candidate, build_cost, min_capacity_ratio, schedule_id] = commodity_data

    # get the commodity_id
    db_cur = db_con.execute("select commodity_id "
                            "from commodities c "
                            "where c.commodity_name = '{}';".format(commodity_name))

    commodity_id = db_cur.fetchone()  # don't index the list, since it might not exist

    if not commodity_id:
        # if it doesn't exist, add the commodity to the commodities table and generate a commodity id
        if commodity_max_transport_distance in ['Null', '', 'None']:
            sql = "insert into commodities " \
                  "(commodity_name, units, phase_of_matter, share_max_transport_distance) " \
                  "values ('{}', '{}', '{}', '{}');".format(commodity_name, commodity_unit, commodity_phase,
                                                          share_max_transport_distance)
        else:
            sql = "insert into commodities " \
                  "(commodity_name, units, phase_of_matter, max_transport_distance, share_max_transport_distance) " \
                  "values ('{}', '{}', '{}', {}, '{}');".format(commodity_name, commodity_unit, commodity_phase,
                                                                commodity_max_transport_distance,
                                                                share_max_transport_distance)
        db_con.execute(sql)

    # get the commodity_id
    db_cur = db_con.execute("select commodity_id "
                            "from commodities c "
                            "where c.commodity_name = '{}';".format(commodity_name))
    commodity_id = db_cur.fetchone()[0]  # index the first (and only) data in the list

    if not commodity_id:
        error = "something went wrong adding the commodity {} " \
                "to the commodities table and getting a commodity_id".format(commodity_name)
        logger.error(error)
        raise Exception(error)
    else:
        return commodity_id


# ===================================================================================================


def get_schedule_id(the_scenario, db_con, schedule_name, logger, get_avail=False):
    # get location_id
    db_cur = db_con.execute(
        "select schedule_id, tot_availability from schedule_names s where s.schedule_name = '{}';".format(str(schedule_name)))
    schedule_data = db_cur.fetchone()

    if not schedule_data:
        # if schedule id is not found, replace with the default schedule
        warning = 'schedule_id for schedule_name: {} is not found. Replace with default'.format(schedule_name)
        logger.info(warning)
        db_cur = db_con.execute(
            "select schedule_id, tot_availability from schedule_names s where s.schedule_name = 'default';")
        schedule_data = db_cur.fetchone()
    
    schedule_id = schedule_data[0]
    availability = schedule_data[1]

    if get_avail:
        return (schedule_id, availability)
    
    return schedule_id


# ===================================================================================================


def gis_clean_fc(the_scenario, logger):

    logger.info("start: gis_clean_fc")

    start_time = datetime.datetime.now()

    # clear the destinations
    gis_clear_feature_class(the_scenario.destinations_fc, logger)

    # clear the RMPs
    gis_clear_feature_class(the_scenario.rmp_fc, logger)

    # clear the processors
    gis_clear_feature_class(the_scenario.processors_fc, logger)

    # clear the locations
    gis_clear_feature_class(the_scenario.locations_fc, logger)

    logger.info("finished: gis_clean_fc: Runtime (HMS): \t{}".format(ftot_supporting.get_total_runtime_string(start_time)))


# ==============================================================================


def gis_clear_feature_class(fc, logger):
    logger.debug("start: gis_clear_feature_class for fc {}".format(os.path.split(fc)[1]))
    if arcpy.Exists(fc):
        arcpy.Delete_management(fc)
        logger.debug("finished: deleted existing fc {}".format(os.path.split(fc)[1]))


# ===================================================================================================


def gis_get_feature_count(fc):
    result = arcpy.GetCount_management(fc)
    count = int(result.getOutput(0))
    return count


# ===================================================================================================


def gis_populate_fc(the_scenario, logger):

    logger.info("start: gis_populate_fc")

    start_time = datetime.datetime.now()

    # populate the destinations fc in main.gdb
    gis_ultimate_destinations_setup_fc(the_scenario, logger)

    # populate the RMPs fc in main.gdb
    gis_rmp_setup_fc(the_scenario, logger)

    # populate the processors fc in main.gdb
    gis_processors_setup_fc(the_scenario, logger)

    logger.info("finished: gis_populate_fc: Runtime (HMS): \t{}".format(ftot_supporting.get_total_runtime_string(start_time)))


# ------------------------------------------------------------


def gis_ultimate_destinations_setup_fc(the_scenario, logger):

    logger.info("start: gis_ultimate_destinations_setup_fc")

    start_time = datetime.datetime.now()

    # copy the destination from the baseline layer to the scenario gdb
    # --------------------------------------------------------------
    if not arcpy.Exists(the_scenario.base_destination_layer):
        error = "Can't find baseline data destinations layer {}".format(the_scenario.base_destination_layer)
        logger.error(error)
        raise IOError(error)

    destinations_fc = the_scenario.destinations_fc
    scenario_proj = ftot_supporting_gis.get_coordinate_system(the_scenario)
    arcpy.Project_management(the_scenario.base_destination_layer, destinations_fc, scenario_proj)

    # Check for required field 'Facility_Name' in FC
    check_fields = [field.name for field in arcpy.ListFields(destinations_fc)]
    if 'Facility_Name' not in check_fields:
        error = "The destinations feature class {} must have the field 'Facility_Name'.".format(destinations_fc)
        logger.error(error)
        raise Exception(error)

    # Delete features with no data in CSV -- cleans up GIS output and eliminates unnecessary GIS processing
    # --------------------------------------------------------------
    # create a temp dict to store values from CSV
    temp_facility_commodities_dict = {}
    counter = 0

    # check if dest CSV exists happens in S step

    # read through facility_commodities input CSV
    with open(the_scenario.destinations_commodity_data, 'rt') as f:
        reader = csv.DictReader(f)

        # check required fieldnames in facility_commodities input CSV
        for field in ["facility_name", "value"]:
            if field not in reader.fieldnames:
                error = "The destinations commodity data CSV {} must have field {}.".format(the_scenario.destinations_commodity_data, field)
                logger.error(error)
                raise Exception(error)

        for row in reader:
            facility_name = str(row["facility_name"])
            commodity_quantity = float(row["value"])

            if facility_name not in list(temp_facility_commodities_dict.keys()):
                if commodity_quantity > 0:
                    temp_facility_commodities_dict[facility_name] = True

    # create a temp dict to store values from FC
    temp_gis_facilities_dict = {}

    with arcpy.da.UpdateCursor(destinations_fc, ['Facility_Name']) as cursor:
        for row in cursor:
            if row[0] not in list(temp_gis_facilities_dict.keys()):
                temp_gis_facilities_dict[row[0]] = True

            if row[0] in temp_facility_commodities_dict:
                pass
            else:
                cursor.deleteRow()
                counter += 1
    del cursor
    logger.info("Number of Destinations removed due to lack of commodity data: \t{}".format(counter))

    result = gis_get_feature_count(destinations_fc)
    logger.info("Number of Destinations: \t{}".format(result))

    # check facilities in facility_commodities input CSV have matching data in FC
    for facility in temp_facility_commodities_dict:
        if facility not in list(temp_gis_facilities_dict.keys()):
            logger.warning("Could not match facility {} in input CSV file to data in Base_Destination_Layer".format(facility))

    # if zero destinations from CSV matched to FC, error out
    if result == 0:
        error = "Destinations feature class contains zero facilities in CSV file {}".format(the_scenario.destinations_commodity_data)
        logger.error(error)
        raise IOError(error)

    logger.info("finished: gis_ultimate_destinations_setup_fc: Runtime (HMS): \t{}".format(ftot_supporting.get_total_runtime_string(start_time)))


# =============================================================================


def gis_rmp_setup_fc(the_scenario, logger):

    logger.info("start: gis_rmp_setup_fc")
    start_time = datetime.datetime.now()

    # copy the rmp from the baseline data to the working gdb
    # ----------------------------------------------------------------
    if not arcpy.Exists(the_scenario.base_rmp_layer):
        error = "Can't find baseline data rmp layer {}".format(the_scenario.base_rmp_layer)
        logger.error(error)
        raise IOError(error)

    rmp_fc = the_scenario.rmp_fc
    scenario_proj = ftot_supporting_gis.get_coordinate_system(the_scenario)
    arcpy.Project_management(the_scenario.base_rmp_layer, rmp_fc, scenario_proj)

    # Check for required field 'Facility_Name' in FC
    check_fields = [field.name for field in arcpy.ListFields(rmp_fc)]
    if 'Facility_Name' not in check_fields:
        error = "The rmp feature class {} must have the field 'Facility_Name'.".format(rmp_fc)
        logger.error(error)
        raise Exception(error)

    # Delete features with no data in CSV -- cleans up GIS output and eliminates unnecessary GIS processing
    # --------------------------------------------------------------
    # create a temp dict to store values from CSV
    temp_facility_commodities_dict = {}
    counter = 0

    # check if rmp CSV exists happens in S step
        
    # read through facility_commodities input CSV
    with open(the_scenario.rmp_commodity_data, 'rt') as f:
        reader = csv.DictReader(f)

        # check required fieldnames in facility_commodities input CSV
        for field in ["facility_name", "value"]:
            if field not in reader.fieldnames:
                error = "The rmp commodity data CSV {} must have field {}.".format(the_scenario.rmp_commodity_data, field)
                logger.error(error)
                raise Exception(error)

        for row in reader:
            facility_name = str(row["facility_name"])
            commodity_quantity = float(row["value"])

            if facility_name not in list(temp_facility_commodities_dict.keys()):
                if commodity_quantity > 0:
                    temp_facility_commodities_dict[facility_name] = True

    # create a temp dict to store values from FC
    temp_gis_facilities_dict = {}

    with arcpy.da.UpdateCursor(rmp_fc, ['Facility_Name']) as cursor:
        for row in cursor:
            if row[0] not in list(temp_gis_facilities_dict.keys()):
                temp_gis_facilities_dict[row[0]] = True

            if row[0] in temp_facility_commodities_dict:
                pass
            else:
                cursor.deleteRow()
                counter += 1
    del cursor
    logger.info("Number of RMPs removed due to lack of commodity data: \t{}".format(counter))

    result = gis_get_feature_count(rmp_fc)
    logger.info("Number of RMPs: \t{}".format(result))

    # check facilities in facility_commodities input CSV have matching data in FC
    for facility in temp_facility_commodities_dict:
        if facility not in list(temp_gis_facilities_dict.keys()):
            logger.warning("Could not match facility {} in input CSV file to data in Base_RMP_Layer".format(facility))

    # if zero RMPs from CSV matched to FC, error out
    if result == 0:
        error = "Raw material producer feature class contains zero facilities in CSV file {}".format(the_scenario.rmp_commodity_data)
        logger.error(error)
        raise IOError(error)

    logger.info("finished: gis_rmp_setup_fc: Runtime (HMS): \t{}".format(ftot_supporting.get_total_runtime_string(start_time)))


# =============================================================================


def gis_processors_setup_fc(the_scenario, logger):

    logger.info("start: gis_processors_setup_fc")
    start_time = datetime.datetime.now()

    scenario_proj = ftot_supporting_gis.get_coordinate_system(the_scenario)

    if str(the_scenario.processors_commodity_data).lower() != "null" and \
       str(the_scenario.processors_commodity_data).lower() != "none":
        # check if proc CSV exists happens in S step
        # read through facility_commodities input CSV
        with open(the_scenario.processors_commodity_data, 'rt') as f:
            reader = csv.DictReader(f)
            row_count = sum(1 for row in reader) 

    if str(the_scenario.base_processors_layer).lower() == "null" or \
       str(the_scenario.base_processors_layer).lower() == "none":
        # create an empty processors layer
        # -------------------------
        processors_fc = the_scenario.processors_fc

        if arcpy.Exists(processors_fc):
            arcpy.Delete_management(processors_fc)
            logger.debug("deleted existing {} layer".format(processors_fc))

        arcpy.CreateFeatureclass_management(the_scenario.main_gdb, "processors", "POINT", "#", "DISABLED", "DISABLED",
                                            scenario_proj, "#", "0", "0", "0")

        arcpy.AddField_management(processors_fc, "Facility_Name", "TEXT", "#", "#", "50", "#", "NULLABLE",
                                  "NON_REQUIRED", "#")
        arcpy.AddField_management(processors_fc, "Candidate", "SHORT")


        # check if there is a discrepancy with proc CSV
        if os.path.exists(the_scenario.processors_commodity_data):
            if row_count > 0:
                error = "Facility data are provided in input CSV file but Base_Processors_Layer is not specified; set CSV file path to None or provide GIS layer"
                logger.error(error)
                raise IOError(error)

    else:
        # copy the processors from the baseline data to the working gdb
        # ----------------------------------------------------------------
        if not arcpy.Exists(the_scenario.base_processors_layer):
            error = "Can't find baseline data processors layer {}".format(the_scenario.base_processors_layer)
            logger.error(error)
            raise IOError(error)

        processors_fc = the_scenario.processors_fc
        arcpy.Project_management(the_scenario.base_processors_layer, processors_fc, scenario_proj)

        arcpy.AddField_management(processors_fc, "Candidate", "SHORT")

        # Check for required field 'Facility_Name' in FC
        check_fields = [field.name for field in arcpy.ListFields(processors_fc)]
        if 'Facility_Name' not in check_fields:
            error = "The processors feature class {} must have the field 'Facility_Name'.".format(processors_fc)
            logger.error(error)
            raise Exception(error)

        # Delete features with no data in CSV -- cleans up GIS output and eliminates unnecessary GIS processing
        # --------------------------------------------------------------
        # create a temp dict to store values from CSV
        temp_facility_commodities_dict = {}
        counter = 0

        if str(the_scenario.processors_commodity_data).lower() != "null" and \
           str(the_scenario.processors_commodity_data).lower() != "none":
            # read through facility_commodities input CSV
            with open(the_scenario.processors_commodity_data, 'rt') as f:
                reader = csv.DictReader(f)

                # check required fieldnames in facility_commodities input CSV
                for field in ["facility_name", "value"]:
                    if field not in reader.fieldnames:
                        error = "The processors commodity data CSV {} must have field {}.".format(the_scenario.processors_commodity_data, field)
                        logger.error(error)
                        raise Exception(error)

                for row in reader:
                    facility_name = str(row["facility_name"])
                    # This check for blank values is necessary to handle "total" processor rows which specify only capacity
                    if row["value"]:
                        commodity_quantity = float(row["value"])
                    else:
                        commodity_quantity = float(0)

                    if facility_name not in list(temp_facility_commodities_dict.keys()):
                        if commodity_quantity > 0:
                            temp_facility_commodities_dict[facility_name] = True

        # create a temp dict to store values from FC
        temp_gis_facilities_dict = {}

        with arcpy.da.UpdateCursor(processors_fc, ['Facility_Name']) as cursor:
            for row in cursor:
                if row[0] not in list(temp_gis_facilities_dict.keys()):
                    temp_gis_facilities_dict[row[0]] = True

                if row[0] in temp_facility_commodities_dict:
                    pass
                else:
                    cursor.deleteRow()
                    counter += 1

        del cursor
        logger.info("Number of processors removed due to lack of commodity data: \t{}".format(counter))

        # check facilities in facility_commodities input CSV have matching data in FC
        for facility in temp_facility_commodities_dict:
            if facility not in list(temp_gis_facilities_dict.keys()):
                logger.warning("Could not match facility {} in input CSV file to data in Base_Processors_Layer".format(facility))

    # check for candidates or other processors specified in XML
    layers_to_merge = []

    # add the candidates_for_merging if they exists
    if arcpy.Exists(the_scenario.processor_candidates_fc):
        logger.info("adding {} candidate processors to the processors fc".format(
            gis_get_feature_count(the_scenario.processor_candidates_fc)))
        layers_to_merge.append(the_scenario.processor_candidates_fc)
        gis_merge_processor_fc(the_scenario, layers_to_merge, logger)

    result = gis_get_feature_count(processors_fc)
    logger.info("Number of Processors: \t{}".format(result))

    # if processors in FC are zero and proc CSV file exists with data, then error out
    if result == 0:
        if os.path.exists(the_scenario.processors_commodity_data):
            if row_count > 0:
                error = "Processor feature class contains zero facilities from processor CSV file {}".format(the_scenario.processors_commodity_data)
                logger.error(error)
                raise IOError(error)

    logger.info("finished: gis_processors_setup_fc: Runtime (HMS): \t{}".format(ftot_supporting.get_total_runtime_string(start_time)))


# =======================================================================


def gis_merge_processor_fc(the_scenario, layers_to_merge, logger):

    logger.info("start: merge candidates and existing processors together")

    scenario_gdb = the_scenario.main_gdb

    # -------------------------------------------------------------------------
    # append the processors from the layers_to_merge data to the working gdb
    # -------------------------------------------------------------------------
    if len(layers_to_merge) == 0:
        logger.warning("Length of layers_to_merge for processors is zero. No candidates will be added to processors FC.")

    processors_fc = the_scenario.processors_fc

    # Open an edit session and start an edit operation
    with arcpy.da.Editor(scenario_gdb) as edit:
        # Report out some information about how many processors are in each layer to merge
        for layer in layers_to_merge:

            processor_count = gis_get_feature_count(layer)
            logger.debug("Importing {} processors from {}".format(processor_count, os.path.split(layer)[0]))
            logger.info("Append candidates into the processors FC")

            arcpy.Append_management(layer, processors_fc, "NO_TEST")

    total_processor_count = gis_get_feature_count(processors_fc)
    logger.debug("Total count: {} records in {}".format(total_processor_count, os.path.split(processors_fc)[0]))

    logger.info("finished: merge candidates and existing processors together")

    return

