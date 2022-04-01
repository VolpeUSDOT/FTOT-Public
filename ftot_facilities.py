# ---------------------------------------------------------------------------------------------------
# Name: ftot_facilities
#
# Purpose: setup for raw material producers (RMP) and ultimate destinations.
# adds demand for destinations. adds supply for RMP.
# hooks facilities into the network using artificial links.
# special consideration for how pipeline links are added.
#
# ---------------------------------------------------------------------------------------------------

import ftot_supporting
import ftot_supporting_gis
import arcpy
import datetime
import os
import sqlite3
from ftot import ureg, Q_
from six import iteritems
LCC_PROJ = arcpy.SpatialReference('USA Contiguous Lambert Conformal Conic')


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
            "create table tmp_facility_locations(location_ID INTEGER , facility_name text PRIMARY KEY);")

        # facilities table
        logger.debug("drop the facilities table")
        main_db_con.execute("drop table if exists facilities;")
        logger.debug("create the facilities table")
        main_db_con.executescript(
            """create table facilities(facility_ID INTEGER PRIMARY KEY, location_id integer, facility_name text, facility_type_id integer, 
            ignore_facility text, candidate binary, schedule_id integer, max_capacity float, build_cost float);""")

        # facility_type_id table
        logger.debug("drop the facility_type_id table")
        main_db_con.execute("drop table if exists facility_type_id;")
        main_db_con.executescript("create table facility_type_id(facility_type_id integer primary key, facility_type text);")

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
            "quantity numeric, units text, io text, share_max_transport_distance text);")

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
            """create table schedule_names(schedule_id INTEGER PRIMARY KEY, schedule_name text);""")

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

    # populate the facilities, commodities, and facility_commodities table
    # with the input CSVs.
    # Note: processor_candidate_commodity_data is generated for FTOT generated candidate
    # processors at the end of the candidate generation step.

    for commodity_input_file in [the_scenario.rmp_commodity_data,
                                 the_scenario.destinations_commodity_data,
                                 the_scenario.processors_commodity_data,
                                 the_scenario.processor_candidates_commodity_data]:
        # this should just catch processors not specified.
        if str(commodity_input_file).lower() == "null" or str(commodity_input_file).lower() == "none":
            logger.debug("Commodity Input Data specified in the XML: {}".format(commodity_input_file))
            continue

        else:
            populate_facility_commodities_table(the_scenario, commodity_input_file, logger)

    # re issue #109- this is a good place to check if there are multiple input commodities for a processor.
    db_check_multiple_input_commodities_for_processor(the_scenario, logger)

    # can delete the tmp_facility_locations table now
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
        sql = """   select c.commodity_name, fti.facility_type, io, sum(fc.quantity), fc.units  
                    from facility_commodities fc
                    join commodities c on fc.commodity_id = c.commodity_id
                    join facilities f on f.facility_id = fc.facility_id
                    join facility_type_id fti on fti.facility_type_id = f.facility_type_id
                    group by c.commodity_name, fc.io, fti.facility_type, fc.units
                    order by commodity_name, io desc;"""
        db_cur = db_con.execute(sql)

        db_data = db_cur.fetchall()
        logger.result("-------------------------------------------------------------------")
        logger.result("Scenario Total Supply and Demand, and Available Processing Capacity")
        logger.result("-------------------------------------------------------------------")
        logger.result("note: processor input and outputs are based on facility size and \n reflect a processing "
                      "capacity, not a conversion of the scenario feedstock supply")
        logger.result("commodity_name | facility_type | io |    quantity   |   units  ")
        logger.result("---------------|---------------|----|---------------|----------")
        for row in db_data:
            logger.result("{:15.15} {:15.15} {:4.1} {:15,.1f} {:15.10}".format(row[0], row[1], row[2], row[3],
                                                                                 row[4]))
        logger.result("-------------------------------------------------------------------")
        # add the ignored processing capacity
        # note this doesn't happen until the bx step.
        # -------------------------------------------
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
        if len(db_data) > 0:
            logger.result("-------------------------------------------------------------------")
            logger.result("Scenario Stranded Supply, Demand, and Processing Capacity")
            logger.result("-------------------------------------------------------------------")
            logger.result("note: stranded supply refers to facilities that are ignored from the analysis.")
            logger.result("commodity_name | facility_type | io |    quantity   | units | ignored ")
            logger.result("---------------|---------------|----|---------------|-------|---------")
            for row in db_data:
                logger.result("{:15.15} {:15.15} {:2.1} {:15,.1f} {:10.10} {:10.10}".format(row[0], row[1], row[2], row[3],
                                                                                   row[4], row[5]))
            logger.result("-------------------------------------------------------------------")

            # report out net quantities with ignored facilities removed from the query
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
            logger.result("-------------------------------------------------------------------")
            logger.result("Scenario Net Supply and Demand, and Available Processing Capacity")
            logger.result("-------------------------------------------------------------------")
            logger.result("note: net supply, demand, and processing capacity ignores facilities not connected to the "
                          "network.")
            logger.result("commodity_name | facility_type | io |    quantity   |   units  ")
            logger.result("---------------|---------------|----|---------------|----------")
            for row in db_data:
                logger.result("{:15.15} {:15.15} {:4.1} {:15,.1f} {:15.10}".format(row[0], row[1], row[2], row[3],
                                                                                   row[4]))
            logger.result("-------------------------------------------------------------------")


# ===================================================================================================


def load_schedules_input_data(schedule_input_file, logger):

    logger.debug("start: load_schedules_input_data")

    import os
    if schedule_input_file == "None":
        logger.info('schedule file not specified.')
        return {'default': {0: 1}}  # return dict with global value of default schedule
    elif not os.path.exists(schedule_input_file):
        logger.warning("warning: cannot find schedule file: {}".format(schedule_input_file))
        return {'default': {0: 1}}

    # create temp dict to store schedule input
    schedules = {}

    # read through facility_commodities input CSV
    import csv
    with open(schedule_input_file, 'rt') as f:

        reader = csv.DictReader(f)
        # adding row index for issue #220 to alert user on which row their error is in
        for index, row in enumerate(reader):

            schedule_name = str(row['schedule']).lower()    # convert schedule to lowercase
            day = int(row['day'])                           # cast day to an int
            availability = float(row['availability'])       # cast availability to float

            if schedule_name in list(schedules.keys()):
                schedules[schedule_name][day] = availability
            else:
                schedules[schedule_name] = {day: availability}  # initialize sub-dict

    # Enforce default schedule req. and default availability req. for all schedules.
    # if user has not defined 'default' schedule
    if 'default' not in schedules:
        logger.debug("Default schedule not found. Adding 'default' with default availability of 1.")
        schedules['default'] = {0: 1}
    # if schedule does not have a default value (value assigned to day 0), then add as 1.
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
            id_num += 1     # 1-index

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

    logger.debug("finished: populate_locations_table")


# ==============================================================================


def check_for_input_error(input_type, input_val, filename, index, units=None):
    """
    :param input_type: a string with the type of input (e.g. 'io', 'facility_name', etc.
    :param input_val: a string from the csv with the actual input
    :param index: the row index
    :param filename: the name of the file containing the row
    :param units: string, units used -- only if type == commodity_phase
    :return: None if data is valid, or proper error message otherwise
    """
    error_message = None
    index = index+2  # account for header and 0-indexing (python) conversion to 1-indexing (excel)
    if input_type == 'io':
        if not (input_val in ['i', 'o']):
            error_message = "There is an error in the io entry in row {} of {}. " \
                            "Entries should be 'i' or 'o'.".format(index, filename)
    elif input_type == 'facility_type':
        if not (input_val in ['raw_material_producer', 'processor', 'ultimate_destination']):
            error_message = "There is an error in the facility_type entry in row {} of {}. " \
                            "The entry is not one of 'raw_material_producer', 'processor', or " \
                            "'ultimate_destination'." \
                            .format(index, filename)
    elif input_type == 'commodity_phase':
        # make sure units specified
        if units is None:
            error_message = "The units in row {} of {} are not specified. Note that solids must have units of mass " \
                            "and liquids must have units of volume." \
                            .format(index, filename)
        elif input_val == 'solid':
            # check if units are valid units for solid (dimension of units must be mass)
            try:
                if not str(ureg(units).dimensionality) == '[mass]':
                    error_message = "The phase_of_matter entry in row {} of {} is solid, but the units are {}" \
                                    " which is not a valid unit for this phase of matter. Solids must be measured in " \
                                    "units of mass." \
                                    .format(index, filename, units)
            except:
                error_message = "The phase_of_matter entry in row {} of {} is solid, but the units are {}" \
                                " which is not a valid unit for this phase of matter. Solids must be measured in " \
                                "units of mass." \
                                .format(index, filename, units)
        elif input_val == 'liquid':
            # check if units are valid units for liquid (dimension of units must be volume, aka length^3)
            try:
                if not str(ureg(units).dimensionality) == '[length] ** 3':
                    error_message = "The phase_of_matter entry in row {} of {} is liquid, but the units are {}" \
                                    " which is not a valid unit for this phase of matter. Liquids must be measured" \
                                    " in units of volume." \
                                    .format(index, filename, units)
            except:
                error_message = "The phase_of_matter entry in row {} of {} is liquid, but the units are {}" \
                                " which is not a valid unit for this phase of matter. Liquids must be measured" \
                                " in units of volume." \
                                .format(index, filename, units)
        else:
            # throw error that phase is neither solid nor liquid
            error_message = "There is an error in the phase_of_matter entry in row {} of {}. " \
                            "The entry is not one of 'solid' or 'liquid'." \
                            .format(index, filename)

    elif input_type == 'commodity_quantity':
        try:
            float(input_val)
        except ValueError:
            error_message = "There is an error in the value entry in row {} of {}. " \
                            "The entry is empty or non-numeric (check for extraneous characters)." \
                            .format(index, filename)

    elif input_type == 'build_cost':
        try:
            float(input_val)
        except ValueError:
            error_message = "There is an error in the build_cost entry in row {} of {}. " \
                            "The entry is empty or non-numeric (check for extraneous characters)." \
                            .format(index, filename)

    return error_message


# ==============================================================================


def load_facility_commodities_input_data(the_scenario, commodity_input_file, logger):
    logger.debug("start: load_facility_commodities_input_data")
    if not os.path.exists(commodity_input_file):
        logger.warning("warning: cannot find commodity_input file: {}".format(commodity_input_file))
        return

    # create a temp dict to store values from CSV
    temp_facility_commodities_dict = {}

    # create empty dictionary to manage schedule input
    facility_schedule_dict = {}

    # read through facility_commodities input CSV
    import csv
    with open(commodity_input_file, 'rt') as f:

        reader = csv.DictReader(f)
        # adding row index for issue #220 to alert user on which row their error is in
        for index, row in enumerate(reader):
            # re: issue #149 -- if the line is empty, just skip it
            if list(row.values())[0] == '':
                logger.debug('the CSV file has a blank in the first column. Skipping this line: {}'.format(
                    list(row.values())))
                continue
            # {'units': 'kgal', 'facility_name': 'd:01053', 'phase_of_matter': 'liquid', 'value': '9181.521484',
            # 'commodity': 'diesel', 'io': 'o', 'share_max_transport_distance'; 'Y'}
            io                  = row["io"]
            facility_name       = str(row["facility_name"])
            facility_type       = row["facility_type"]
            commodity_name      = row["commodity"].lower()  # re: issue #131 - make all commodities lower case
            commodity_quantity  = row["value"]
            commodity_unit      = str(row["units"]).replace(' ', '_').lower()  # remove spaces and make units lower case
            commodity_phase     = row["phase_of_matter"]

            # check for proc_cand-specific "non-commodities" to ignore validation (issue #254)
            non_commodities = ['minsize', 'maxsize', 'cost_formula', 'min_aggregation']

            # input data validation
            if commodity_name not in non_commodities:  # re: issue #254 only test actual commodities
                # test io
                io = io.lower()  # convert 'I' and 'O' to 'i' and 'o'
                error_message = check_for_input_error("io", io, commodity_input_file, index)
                if error_message:
                    raise Exception(error_message)
                # test facility type
                error_message = check_for_input_error("facility_type", facility_type, commodity_input_file, index)
                if error_message:
                    raise Exception(error_message)
                # test commodity quantity
                error_message = check_for_input_error("commodity_quantity", commodity_quantity, commodity_input_file, index)
                if error_message:
                    raise Exception(error_message)
                # test commodity phase
                error_message = check_for_input_error("commodity_phase", commodity_phase, commodity_input_file, index,
                                                      units=commodity_unit)
                if error_message:
                    raise Exception(error_message)
            else:
                logger.debug("Skipping input validation on special candidate processor commodity: {}"
                             .format(commodity_name))

            if "max_processor_input" in list(row.keys()):
                max_processor_input = row["max_processor_input"]
            else:
                max_processor_input = "Null"

            if "max_transport_distance" in list(row.keys()):
                commodity_max_transport_distance = row["max_transport_distance"]
            else:
                commodity_max_transport_distance = "Null"

            if "share_max_transport_distance" in list(row.keys()):
                share_max_transport_distance = row["share_max_transport_distance"]
            else:
                share_max_transport_distance = 'N'
            
            # set to 0 if blank, otherwise convert to numerical after checkign for extra characters
            if "build_cost" in list(row.keys()):
                build_cost = row["build_cost"]
                if build_cost == '':
                    build_cost = 0
                else:
                    error_message = check_for_input_error("build_cost", build_cost, 
                        commodity_input_file, index, units=commodity_unit)
                    if error_message:
                        raise Exception(error_message)
                    else:
                        build_cost = float(build_cost)
            else:
                build_cost = 0   
         
            if build_cost > 0:
                candidate_flag = 1
            else:
                candidate_flag = 0

            # add schedule_id, if available
            if "schedule" in list(row.keys()):
                schedule_name = str(row["schedule"]).lower()

                # blank schedule name should be cast to default
                if schedule_name == "none":
                    schedule_name = "default"
            else:
                schedule_name = "default"

            # manage facility_schedule_dict
            if facility_name not in facility_schedule_dict:
                facility_schedule_dict[facility_name] = schedule_name
            elif facility_schedule_dict[facility_name] != schedule_name:
                logger.info("Schedule name '{}' does not match previously entered schedule '{}' for facility '{}'".
                            format(schedule_name, facility_schedule_dict[facility_name], facility_name))
                schedule_name = facility_schedule_dict[facility_name]

            # use pint to set the quantity and units
            commodity_quantity_and_units = Q_(float(commodity_quantity), commodity_unit)
            if max_processor_input != 'Null':
                max_input_quantity_and_units = Q_(float(max_processor_input), commodity_unit)

            if commodity_phase.lower() == 'liquid':
                commodity_unit = the_scenario.default_units_liquid_phase
            if commodity_phase.lower() == 'solid':
                commodity_unit = the_scenario.default_units_solid_phase

            if commodity_name == 'cost_formula':
                pass
            else:
                commodity_quantity = commodity_quantity_and_units.to(commodity_unit).magnitude

            if max_processor_input != 'Null':
                max_processor_input = max_input_quantity_and_units.to(commodity_unit).magnitude

            # add to the dictionary of facility_commodities mapping
            if facility_name not in list(temp_facility_commodities_dict.keys()):
                temp_facility_commodities_dict[facility_name] = []

            temp_facility_commodities_dict[facility_name].append([facility_type, commodity_name, commodity_quantity,
                                                                  commodity_unit, commodity_phase,
                                                                  commodity_max_transport_distance, io,
                                                                  share_max_transport_distance, candidate_flag, build_cost, max_processor_input,
                                                                  schedule_name])

    logger.debug("finished: load_facility_commodities_input_data")
    return temp_facility_commodities_dict


# ==============================================================================


def populate_facility_commodities_table(the_scenario, commodity_input_file, logger):

    logger.debug("start: populate_facility_commodities_table {}".format(commodity_input_file))

    if not os.path.exists(commodity_input_file):
        logger.debug("note: cannot find commodity_input file: {}".format(commodity_input_file))
        return

    facility_commodities_dict = load_facility_commodities_input_data(the_scenario, commodity_input_file, logger)

    #recognize generated candidate processors
    candidate = 0
    generated_candidate = 0
    if os.path.split(commodity_input_file)[1].find("ftot_generated_processor_candidates") > -1:
        generated_candidate = 1

    # connect to main.db and add values to table
    # ---------------------------------------------------------
    with sqlite3.connect(the_scenario.main_db) as db_con:
        for facility_name, facility_data in iteritems(facility_commodities_dict):
            
            # unpack the facility_type (should be the same for all entries)
            facility_type = facility_data[0][0]
            facility_type_id = get_facility_id_type(the_scenario, db_con, facility_type, logger)

            location_id = get_facility_location_id(the_scenario, db_con, facility_name, logger)

            # get schedule id from the db
            schedule_name = facility_data[0][-1]
            schedule_id = get_schedule_id(the_scenario, db_con, schedule_name, logger)

            max_processor_input = facility_data[0][-2]

            build_cost = facility_data[0][-3]
            
            #recognize candidate processors from proc.csv or generated file
            candidate_flag = facility_data[0][-4]
            if candidate_flag == 1 or generated_candidate == 1:
                candidate = 1
            else:
                candidate = 0

            # get the facility_id from the db (add the facility if it doesn't exists)
            # and set up entry in facility_id table
            facility_id = get_facility_id(the_scenario, db_con, location_id, facility_name, facility_type_id, candidate, schedule_id, max_processor_input, build_cost, logger)

            # iterate through each commodity
            for commodity_data in facility_data:

                # get commodity_id. (adds commodity if it doesn't exist)
                commodity_id = get_commodity_id(the_scenario, db_con, commodity_data, logger)

                [facility_type, commodity_name, commodity_quantity, commodity_units, commodity_phase, commodity_max_transport_distance, io, share_max_transport_distance, candidate_data, build_cost, unused_var_max_processor_input, schedule_id] = commodity_data

                if not commodity_quantity == "0.0":  # skip anything with no material
                    sql = "insert into facility_commodities " \
                          "(facility_id, location_id, commodity_id, quantity, units, io, share_max_transport_distance) " \
                          "values ('{}','{}', '{}', '{}', '{}', '{}', '{}');".format(
                            facility_id, location_id, commodity_id, commodity_quantity, commodity_units, io, share_max_transport_distance)
                    db_con.execute(sql)
                else:
                    logger.debug("skipping commodity_data {} because quantity: {}".format(commodity_name, commodity_quantity))
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
            logger.warning("Processor: {} has {} input commodities specified.".format(multi_input_processor[0],
                                                                                      multi_input_processor[1]))
            logger.warning("Multiple processor inputs are not supported in the same scenario as shared max transport "
                           "distance")
        # logger.info("make adjustments to the processor commodity input file: {}".format(the_scenario.processors_commodity_data))
        # error = "Multiple input commodities for processors is not supported in FTOT"
        # logger.error(error)
        # raise Exception(error)


# ==============================================================================


def populate_coprocessing_table(the_scenario, logger):

    logger.info("start: populate_coprocessing_table")

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

        # then iterate through the GIS and get the facility_name, shape_x and shape_y
        with arcpy.da.Editor(the_scenario.main_gdb) as edit:

            logger.debug("iterating through the FC and create a facility_location mapping with x,y coord.")

            for fc in [the_scenario.rmp_fc, the_scenario.destinations_fc, the_scenario.processors_fc]:

                logger.debug("iterating through the FC: {}".format(fc))
                with arcpy.da.SearchCursor(fc, ["facility_name", "SHAPE@X", "SHAPE@Y"]) as cursor:
                    for row in cursor:
                        facility_name = row[0]
                        shape_x = round(row[1], -2)
                        shape_y = round(row[2], -2)

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

    logger.debug("finished: populate_locations_table")


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

    # if no ID, add it.
    if location_id is None:
        #  if it doesn't exist, add to locations table and generate a location id.
        db_cur = db_con.execute("insert into locations (shape_x, shape_y) values ('{}', '{}');".format(shape_x, shape_y))
        location_id = db_cur.lastrowid

    # check again. we should have the ID now. if we don't throw an error.
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


def get_facility_id(the_scenario, db_con, location_id, facility_name, facility_type_id, candidate, schedule_id, max_processor_input, build_cost, logger):

    #  if it doesn't exist, add to facilities table and generate a facility id.
    if build_cost > 0:
        # specify ignore_facility = 'false'. Otherwise, the input-from-file candidates get ignored like excess generated candidates
        ignore_facility = 'false'
        db_con.execute("insert or ignore into facilities "
                   "(location_id, facility_name, facility_type_id, ignore_facility, candidate, schedule_id, max_capacity, build_cost) "
                   "values ('{}', '{}', {}, '{}',{}, {}, {}, {});".format(location_id, facility_name, facility_type_id, ignore_facility, candidate, schedule_id, max_processor_input, build_cost))
    else:
        db_con.execute("insert or ignore into facilities "
                   "(location_id, facility_name, facility_type_id, candidate, schedule_id, max_capacity, build_cost) "
                   "values ('{}', '{}', {},  {}, {}, {}, {});".format(location_id, facility_name, facility_type_id, candidate, schedule_id, max_processor_input, build_cost))

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

    #  if it doesn't exist, add to facility_type table and generate a facility_type_id.
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
     commodity_max_transport_distance, io, share_max_transport_distance, candidate, build_cost, max_processor_input, schedule_id] = commodity_data

    # get the commodity_id.
    db_cur = db_con.execute("select commodity_id "
                            "from commodities c "
                            "where c.commodity_name = '{}';".format(commodity_name))

    commodity_id = db_cur.fetchone() # don't index the list, since it might not exists.

    if not commodity_id:
        # if it doesn't exist, add the commodity to the commodities table and generate a commodity id
        if commodity_max_transport_distance in ['Null', '', 'None']:
            sql = "insert into commodities " \
                  "(commodity_name, units, phase_of_matter, share_max_transport_distance) " \
                  "values ('{}', '{}', '{}','{}');".format(commodity_name, commodity_unit, commodity_phase,
                                                          share_max_transport_distance)
        else:
            sql = "insert into commodities " \
                  "(commodity_name, units, phase_of_matter, max_transport_distance, share_max_transport_distance) " \
                  "values ('{}', '{}', '{}', {}, '{}');".format(commodity_name, commodity_unit, commodity_phase,
                                                                commodity_max_transport_distance,
                                                                share_max_transport_distance)
        db_con.execute(sql)

    # get the commodity_id.
    db_cur = db_con.execute("select commodity_id "
                            "from commodities c "
                            "where c.commodity_name = '{}';".format(commodity_name))
    commodity_id = db_cur.fetchone()[0] # index the first (and only) data in the list

    if not commodity_id:
        error = "something went wrong adding the commodity {} " \
                "to the commodities table and getting a commodity_id".format(commodity_name)
        logger.error(error)
        raise Exception(error)
    else:
        return commodity_id


# ===================================================================================================


def get_schedule_id(the_scenario, db_con, schedule_name, logger):
    # get location_id
    db_cur = db_con.execute(
        "select schedule_id from schedule_names s where s.schedule_name = '{}';".format(str(schedule_name)))
    schedule_id = db_cur.fetchone()
    if not schedule_id:
        # if schedule id is not found, replace with the default schedule
        warning = 'schedule_id for schedule_name: {} is not found. Replace with default'.format(schedule_name)
        logger.info(warning)
        db_cur = db_con.execute(
            "select schedule_id from schedule_names s where s.schedule_name = 'default';")
        schedule_id = db_cur.fetchone()

    return schedule_id[0]


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

    # clear the processors
    gis_clear_feature_class(the_scenario.locations_fc, logger)

    logger.debug("finished: gis_clean_fc: Runtime (HMS): \t{}".format
                 (ftot_supporting.get_total_runtime_string(start_time)))


# ==============================================================================


def gis_clear_feature_class(fc, logger):
    logger.debug("start: clear_feature_class for fc {}".format(os.path.split(fc)[1]))
    if arcpy.Exists(fc):
        arcpy.Delete_management(fc)
        logger.debug("finished: deleted existing fc {}".format(os.path.split(fc)[1]))


# ===================================================================================================


def gis_get_feature_count(fc, logger):
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

    logger.debug("finished: gis_populate_fc: Runtime (HMS): \t{}".format
                 (ftot_supporting.get_total_runtime_string(start_time)))


# ------------------------------------------------------------


def gis_ultimate_destinations_setup_fc(the_scenario, logger):

    logger.info("start: gis_ultimate_destinations_setup_fc")

    start_time = datetime.datetime.now()

    # copy the destination from the baseline layer to the scenario gdb
    # --------------------------------------------------------------
    if not arcpy.Exists(the_scenario.base_destination_layer):
        error = "can't find baseline data destinations layer {}".format(the_scenario.base_destination_layer)
        raise IOError(error)

    destinations_fc = the_scenario.destinations_fc
    arcpy.Project_management(the_scenario.base_destination_layer, destinations_fc, ftot_supporting_gis.LCC_PROJ)

    # Delete features with no data in csv -- cleans up GIS output and eliminates unnecessary GIS processing
    # --------------------------------------------------------------
    # create a temp dict to store values from CSV
    temp_facility_commodities_dict = {}
    counter = 0

    # read through facility_commodities input CSV
    import csv
    with open(the_scenario.destinations_commodity_data, 'rt') as f:
        reader = csv.DictReader(f)
        for row in reader:
            facility_name = str(row["facility_name"])
            commodity_quantity = float(row["value"])

            if facility_name not in list(temp_facility_commodities_dict.keys()):
                if commodity_quantity > 0:
                    temp_facility_commodities_dict[facility_name] = True

    with arcpy.da.UpdateCursor(destinations_fc, ['Facility_Name']) as cursor:
        for row in cursor:
            if row[0] in temp_facility_commodities_dict:
                pass
            else:
                cursor.deleteRow()
                counter += 1
    del cursor
    logger.config("Number of Destinations removed due to lack of commodity data: \t{}".format(counter))

    with arcpy.da.SearchCursor(destinations_fc, ['Facility_Name', 'SHAPE@X', 'SHAPE@Y']) as scursor:
        for row in scursor:
            # Check if coordinates of facility are roughly within North America
            if -6500000 < row[1] < 6500000 and -3000000 < row[2] < 5000000:
                pass
            else:
                logger.warning("Facility: {} is not located in North America.".format(row[0]))
                logger.info("remove the facility from the scenario or make adjustments to the facility's location in"
                            " the destinations feature class: {}".format(the_scenario.base_destination_layer))
                error = "Facilities outside North America are not supported in FTOT"
                logger.error(error)
                raise Exception(error)

    result = gis_get_feature_count(destinations_fc, logger)
    logger.config("Number of Destinations: \t{}".format(result))

    logger.debug("finish: gis_ultimate_destinations_setup_fc: Runtime (HMS): \t{}".format
                 (ftot_supporting.get_total_runtime_string(start_time)))


# =============================================================================


def gis_rmp_setup_fc(the_scenario, logger):

    logger.info("start: gis_rmp_setup_fc")
    start_time = datetime.datetime.now()

    # copy the rmp from the baseline data to the working gdb
    # ----------------------------------------------------------------
    if not arcpy.Exists(the_scenario.base_rmp_layer):
        error = "can't find baseline data rmp layer {}".format(the_scenario.base_rmp_layer)
        raise IOError(error)

    rmp_fc = the_scenario.rmp_fc
    arcpy.Project_management(the_scenario.base_rmp_layer, rmp_fc, ftot_supporting_gis.LCC_PROJ)

    # Delete features with no data in csv-- cleans up GIS output and eliminates unnecessary GIS processing
    # --------------------------------------------------------------
    # create a temp dict to store values from CSV
    temp_facility_commodities_dict = {}
    counter = 0

    # read through facility_commodities input CSV
    import csv
    with open(the_scenario.rmp_commodity_data, 'rt') as f:

        reader = csv.DictReader(f)
        for row in reader:
            facility_name = str(row["facility_name"])
            commodity_quantity = float(row["value"])

            if facility_name not in list(temp_facility_commodities_dict.keys()):
                if commodity_quantity > 0:
                    temp_facility_commodities_dict[facility_name] = True

    with arcpy.da.UpdateCursor(rmp_fc, ['Facility_Name']) as cursor:
        for row in cursor:
            if row[0] in temp_facility_commodities_dict:
                pass
            else:
                cursor.deleteRow()
                counter +=1
    del cursor
    logger.config("Number of RMPs removed due to lack of commodity data: \t{}".format(counter))

    with arcpy.da.SearchCursor(rmp_fc, ['Facility_Name', 'SHAPE@X', 'SHAPE@Y']) as scursor:
        for row in scursor:
            # Check if coordinates of facility are roughly within North America
            if -6500000 < row[1] < 6500000 and -3000000 < row[2] < 5000000:
                pass
            else:
                logger.warning("Facility: {} is not located in North America.".format(row[0]))
                logger.info("remove the facility from the scenario or make adjustments to the facility's location in "
                            "the RMP feature class: {}".format(the_scenario.base_rmp_layer))
                error = "Facilities outside North America are not supported in FTOT"
                logger.error(error)
                raise Exception(error)

    del scursor

    result = gis_get_feature_count(rmp_fc, logger)
    logger.config("Number of RMPs: \t{}".format(result))

    logger.debug("finished: gis_rmp_setup_fc: Runtime (HMS): \t{}".format(ftot_supporting.get_total_runtime_string(start_time)))


# =============================================================================


def gis_processors_setup_fc(the_scenario, logger):

    logger.info("start: gis_processors_setup_fc")
    start_time = datetime.datetime.now()

    if str(the_scenario.base_processors_layer).lower() == "null" or \
       str(the_scenario.base_processors_layer).lower() == "none":
        # create an empty processors layer
        # -------------------------
        processors_fc = the_scenario.processors_fc

        if arcpy.Exists(processors_fc):
            arcpy.Delete_management(processors_fc)
            logger.debug("deleted existing {} layer".format(processors_fc))

        arcpy.CreateFeatureclass_management(the_scenario.main_gdb, "processors", "POINT", "#", "DISABLED", "DISABLED",
                                            ftot_supporting_gis.LCC_PROJ, "#", "0", "0", "0")

        arcpy.AddField_management(processors_fc, "Facility_Name", "TEXT", "#", "#", "25", "#", "NULLABLE",
                                  "NON_REQUIRED", "#")
        arcpy.AddField_management(processors_fc, "Candidate", "SHORT")

    else:
        # copy the processors from the baseline data to the working gdb
        # ----------------------------------------------------------------
        if not arcpy.Exists(the_scenario.base_processors_layer):
            error = "can't find baseline data processors layer {}".format(the_scenario.base_processors_layer)
            raise IOError(error)

        processors_fc = the_scenario.processors_fc
        arcpy.Project_management(the_scenario.base_processors_layer, processors_fc, ftot_supporting_gis.LCC_PROJ)

        arcpy.AddField_management(processors_fc, "Candidate", "SHORT")

        # Delete features with no data in csv-- cleans up GIS output and eliminates unnecessary GIS processing
        # --------------------------------------------------------------
        # create a temp dict to store values from CSV
        temp_facility_commodities_dict = {}
        counter = 0

        # read through facility_commodities input CSV
        import csv
        with open(the_scenario.processors_commodity_data, 'rt') as f:

            reader = csv.DictReader(f)
            for row in reader:
                facility_name = str(row["facility_name"])
                commodity_quantity = float(row["value"])

                if facility_name not in list(temp_facility_commodities_dict.keys()):
                    if commodity_quantity > 0:
                        temp_facility_commodities_dict[facility_name] = True

        with arcpy.da.UpdateCursor(processors_fc, ['Facility_Name']) as cursor:
            for row in cursor:
                if row[0] in temp_facility_commodities_dict:
                    pass
                else:
                    cursor.deleteRow()
                    counter += 1

        del cursor
        logger.config("Number of processors removed due to lack of commodity data: \t{}".format(counter))

        with arcpy.da.SearchCursor(processors_fc, ['Facility_Name', 'SHAPE@X', 'SHAPE@Y']) as scursor:
            for row in scursor:
                # Check if coordinates of facility are roughly within North America
                if -6500000 < row[1] < 6500000 and -3000000 < row[2] < 5000000:
                    pass
                else:
                    logger.warning("Facility: {} is not located in North America.".format(row[0]))
                    logger.info("remove the facility from the scenario or make adjustments to the facility's location "
                                "in the processors feature class: {}".format(the_scenario.base_processors_layer))
                    error = "Facilities outside North America are not supported in FTOT"
                    logger.error(error)
                    raise Exception(error)

        del scursor

    # check for candidates or other processors specified in either XML or
    layers_to_merge = []

    # add the candidates_for_merging if they exists.
    if arcpy.Exists(the_scenario.processor_candidates_fc):
        logger.info("adding {} candidate processors to the processors fc".format(
            gis_get_feature_count(the_scenario.processor_candidates_fc, logger)))
        layers_to_merge.append(the_scenario.processor_candidates_fc)
        gis_merge_processor_fc(the_scenario, layers_to_merge, logger)

    result = gis_get_feature_count(processors_fc, logger)

    logger.config("Number of Processors: \t{}".format(result))

    logger.debug("finish: gis_processors_setup_fc: Runtime (HMS): \t{}".format(ftot_supporting.get_total_runtime_string(start_time)))


# =======================================================================


def gis_merge_processor_fc(the_scenario, layers_to_merge, logger):

    logger.info("merge the candidates and processors together. ")

    scenario_gdb = the_scenario.main_gdb

    # -------------------------------------------------------------------------
    # append the processors from the kayers to merge data to the working gdb
    # -------------------------------------------------------------------------
    if len(layers_to_merge) == 0:
        error = "len of layers_to_merge for processors is {}".format(layers_to_merge)
        raise IOError(error)

    processors_fc = the_scenario.processors_fc

    # Open an edit session and start an edit operation
    with arcpy.da.Editor(scenario_gdb) as edit:
        # Report out some information about how many processors are in each layer to merge
        for layer in layers_to_merge:

            processor_count = str(arcpy.GetCount_management(layer))
            logger.debug("Importing {} processors from {}".format(processor_count, os.path.split(layer)[0]))
            logger.info("Append candidates into the Processors FC")

            arcpy.Append_management(layer, processors_fc, "NO_TEST")

    total_processor_count = str(arcpy.GetCount_management(processors_fc))
    logger.debug("Total count: {} records in {}".format(total_processor_count, os.path.split(processors_fc)[0]))

    return

