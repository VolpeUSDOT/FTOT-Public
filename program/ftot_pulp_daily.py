# ---------------------------------------------------------------------------------------------------
# Name: ftot_pulp
#
# Purpose: PulP optimization - create and run a modified facility location problem.
# Take NetworkX and GIS scenario data as recorded in main.db and convert to a structure of edges, nodes, vertices.
# Create variables for flow over edges, unmet demand, processor use, and candidate processors to build if present
# Solve cost minimization for unmet demand, transportation, and facility build costs
# Constraints ensure compliance with scenario requirements (e.g. max_route_capacity)
# as well as general problem structure (e.g. conservation_of_flow)
# ---------------------------------------------------------------------------------------------------

import datetime
import pdb
import re
import sqlite3
import numpy as np
from collections import defaultdict

from pulp import *

import ftot_supporting
import test_aftot_pulp
from ftot_supporting import get_total_runtime_string

# =================== constants=============
storage = 1
primary = 0
fixed_schedule_id = 2
fixed_route_duration = 0

THOUSAND_GALLONS_PER_THOUSAND_BARRELS = 42

default_sched = test_aftot_pulp.schedule_full_availability()
last_day_sched = test_aftot_pulp.schedule_last_day_only()
storage_cost_1 = 0.01
storage_cost_2 = 0.05
facility_onsite_storage_max = 10000000000
facility_onsite_storage_min = 0

default_max_capacity = 10000000000
default_min_capacity = 0


#=============load parameters from newly developed Python files======================
# Load earthquake scenario matrix  
#earthquake_scenario = np.load("earthquake_scenario.npy")

# Load edge capacity index
edge_cap = np.load("edge_cap.npy")  
# load bridge damage state index
bridge_DS = np.load("bridge_DS.npy")
# load facility capacity index 
facility_cap = np.load("facility_cap.npy")
facility_cap_noEarthquake = np.load("facility_cap_noEarthquake.npy") 
# load facility damage state index 
facility_DS = np.load("facility_DS.npy") 
# load repair cost array:
repair_costs = np.load("repair_costs.npy")
repair_costs = repair_costs
# load reair time array:
repair_time_facility = np.load("repair_time_facility.npy")
repair_time_edge = np.load("repair_time_edge.npy")
# ----------------------------------------------------------------------------------------------
# planning horizon in this example: 20 years
plan_horizon = 20 # unit: year
# total scenario amount: "N", here we consider 10 scenarios
N = 10
operation_parameter = 50 # unit:$/ton

# initial costs (daily )
initial_cost_daily = 29712 # run optimization without considering any risk factors


#=========================================================================================

def o1(the_scenario, logger):
    # create vertices, then edges for permitted modes, then set volume & capacity on edges
    #i = np.load("scenario_num.npy")
    #t = np.load("time_horizon.npy")
    #j = np.load("earthquake_day.npy")
    #logger.info("scenario {}".format(i))
    #logger.info("time horizon {}".format(t))
    #logger.info("earthquake day {}".format(j))
    pre_setup_pulp(logger, the_scenario)


def o2(the_scenario, logger):
    # create variables, problem to optimize, and constraints
    prob = setup_pulp_problem(the_scenario, logger)
    pickle_prob(prob, "constrained_prob.p", the_scenario, logger)
    loaded_prob = load_pickled_prob("constrained_prob.p", the_scenario, logger)
    loaded_prob = solve_pulp_problem(loaded_prob, the_scenario, logger)
    save_pulp_solution(the_scenario, loaded_prob, logger)
    from ftot_supporting import post_optimization_64_bit
    post_optimization_64_bit(the_scenario, 'o2', logger)


def o2b(the_scenario,logger):
    loaded_prob = load_pickled_prob("constrained_prob.p", the_scenario, logger)
    loaded_prob = solve_pulp_problem(loaded_prob, the_scenario, logger)
    save_pulp_solution(the_scenario, loaded_prob, logger)


def oc2b(the_scenario, logger):
    import ftot_pulp_candidate_generation
    loaded_prob = load_pickled_prob("constrained_candidate_prob.p", the_scenario, logger)
    loaded_prob = reload_objective_function(the_scenario, logger)
    loaded_prob = ftot_pulp_candidate_generation.solve_pulp_problem(loaded_prob, the_scenario, logger)
    ftot_pulp_candidate_generation.save_pulp_solution(the_scenario, loaded_prob, logger)

# helper function that reads in schedule data and returns dict of Schedule objects
def create_schedules_from_input(schedule_data, logger):
    logger.debug("start: create_schedules_from_input")

    from xtot_objects import Schedule
    import os  # amy added this
    if not os.path.exists(schedule_data):
        logger.warning("warning: cannot find schedule file: {}".format(schedule_data))
        return {'default': default_sched}  # return dict with global value of default schedule

    # create temp dict to store non-default days of a schedule
    day_availabilities = {}

    # create temp dict to store default values for a schedule
    default_values = {}

    # read through schedule CSV
    with open(schedule_data, 'r') as rf:
        line_num = 1

        for line in rf:

            if line_num > 1:
                flds = line.rstrip('\n').split(',')
                schedule_name = flds[0]
                day = int(flds[1])  # cast day to an int to determine max day/schedule length
                availability = flds[2]
                if day == 0:  # denotes the default value
                    default_values[schedule_name] = availability
                elif schedule_name in day_availabilities.keys():
                    day_availabilities[schedule_name][day] = availability
                else:
                    day_availabilities[schedule_name] = {day: availability}  # initialize sub-dict
            line_num += 1

    # make dictionary to store schedule objects
    schedule_dict = {}

    # after reading in csv, parse data into dictionary object
    for schedule_name in day_availabilities.keys():
        first_day = 1
        last_day = max(day_availabilities[schedule_name].keys())  # max value of days in schedule
        if schedule_name in default_values:
            default_availability = default_values[schedule_name]
        else:
            default_availability = '1'
            logger.info("No default value for schedule: " + schedule_name)
            logger.info("Setting default value to 1.")
        schedule_availabilities = ['0']
        schedule_availabilities.extend([default_availability for i in range(last_day)])
        # now change different days to actual availability instead of the default values
        for day in day_availabilities[schedule_name].keys():
            schedule_availabilities[day] = day_availabilities[schedule_name][day]
        # lastly, create Schedule object and add to dictionary
        schedule_dict[schedule_name] = Schedule(first_day, last_day, schedule_availabilities)

    # amy debugging
    for key in schedule_dict.keys():
        logger.debug("schedule name: " + key)
        logger.debug("availability: ")
        logger.debug(schedule_dict[key])
    logger.debug("finished: create_schedules_from_input")

    return schedule_dict


def commodity_mode_setup(the_scenario, logger):

    # helper method to read in the csv and make a dict
    def make_commodity_mode_dict(the_scenario, logger):
        logger.info("START: make_commodity_mode_dict")
        # check if path to table exists
        if not os.path.exists(the_scenario.commodity_mode_data):
            logger.warning("warning: cannot find commodity_mode_data file: {}".format(the_scenario.commodity_mode_data))
            return {}  # return empty dict
        # otherwise, initialize dict and read through commodity_mode CSV
        commodity_mode_dict = {}
        with open(the_scenario.commodity_mode_data, 'r') as rf:
            line_num = 1
            modes = None  # will assign within for loop
            for line in rf:
                if line_num == 1:
                    modes = line.rstrip('\n').split(',')
                    # # make a dict mapping col index to mode name
                    # mode_index = {}
                    # for i in range(len(modes)):
                    #     # mode name will point to index of that mode in the csv
                    #     mode_index[modes[i]] = i
                else:
                    flds = line.rstrip('\n').split(',')
                    commodity_name = flds[0]
                    allowed = flds[1:]
                    commodity_mode_dict[commodity_name] = dict(zip(modes[1:], allowed))
                    # now do a check
                    for mode in modes[1:]:
                        if commodity_mode_dict[commodity_name][mode] in ['Y', 'N']:
                            logger.info("Commodity: {}, Mode: {}, Allowed: {}".format(commodity_name, mode,
                                            commodity_mode_dict[commodity_name][mode]))
                        else:
                            # if val isn't Y or N, remove the key from the dict
                            del commodity_mode_dict[commodity_name][mode]
                            logger.info(
                                    "improper or no value in Commodity_Mode_Data csv for commodity: {} and mode: {}".\
                                    format(commodity_name, mode))
                            logger.info("default value will be used for commodity: {} and mode: {}".\
                                        format(commodity_name, mode))

                line_num += 1
        logger.info("FINISHED: make_commodity_mode_dict")
        return commodity_mode_dict

    logger.info("START: commodity_mode_setup")

    with sqlite3.connect(the_scenario.main_db) as main_db_con:

        main_db_con.executescript("""
        drop table if exists commodity_mode;
        
        create table commodity_mode(
        mode text,
        commodity_id text,
        commodity_phase text,
        allowed_yn text,
        CONSTRAINT unique_commodity_and_mode UNIQUE(commodity_id, mode))
        ;""")

        commod = main_db_con.execute("select commodity_name, commodity_id, phase_of_matter from commodities;")
        commod = commod.fetchall()
        commodities = {}
        for row in commod:
            commodity_name = row[0]
            commodity_id = row[1]
            phase_of_matter = row[2]
            commodities[commodity_name] = (commodity_id, phase_of_matter)

        # read in from csv file and insert those entries first, using commodity id
        # then, fill in for all other unspecified commodities and modes, pipeline liquid only
        commodity_mode_dict = make_commodity_mode_dict(the_scenario, logger)
        for mode in the_scenario.permittedModes:
            for k, v in commodities.iteritems():

                commodity_name = k
                commodity_id = v[0]
                phase_of_matter = v[1]
                # check commodity mode permissions for all modes. Apply resiction is specified, otherwise default to
                # allowed if not specified.
                if commodity_name in commodity_mode_dict and mode in commodity_mode_dict[commodity_name]:
                    allowed = commodity_mode_dict[commodity_name][mode]
                else:
                    allowed = 'Y'
                # pipeline is a special case. so double check if it is explicitly allowed, otherwise override to 'N'.
                # restrict solids on pipeline no matter what.
                if mode.partition('_')[0] == 'pipeline':
                    # 8/26/19 -- MNP -- note: that mode is the long name, and the dict is the short name
                    if mode == 'pipeline_crude_trf_rts':
                        short_name_mode = 'pipeline_crude'
                    elif mode == 'pipeline_prod_trf_rts':
                        short_name_mode = 'pipeline_prod'
                    else: logger.warning("a pipeline was specified that is not supported")
                    # restrict pipeline if its not explicitly allowed or if solid
                    if commodity_name in commodity_mode_dict and short_name_mode in commodity_mode_dict[commodity_name]:
                        allowed = commodity_mode_dict[commodity_name][short_name_mode]
                    else:
                        allowed = 'N'
                    if phase_of_matter != 'liquid':
                        allowed = 'N'

                main_db_con.execute("""
                insert or ignore into commodity_mode
                (mode, commodity_id, commodity_phase, allowed_yn) 
                VALUES 
                ('{}',{},'{}','{}')
                ;
                """.format(mode, commodity_id, phase_of_matter, allowed))

        return
# ===============================================================================


def source_tracking_setup(the_scenario, logger):
    logger.info("START: source_tracking_setup")

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        max_inputs = main_db_con.execute("""select max(inputs) from
        (select count(fc.commodity_id) inputs
        from facility_commodities fc, facility_type_id ft
        where ft.facility_type = 'processor'
        and fc.io = 'i'
        group by fc.facility_id)
        ;""")

        for row in max_inputs:
            if row[0] == None:
                max_inputs_to_a_processor = 0
            else:
                max_inputs_to_a_processor = int(row[0])
        if max_inputs_to_a_processor > 1:
            logger.warning("error, this version of the optimization step only functions correctly with a single input"
                           " commodity type per processor")

        main_db_con.executescript("""

            insert or ignore into commodities(commodity_name) values ('multicommodity');
            
            drop table if exists source_commodity_ref
            ;

            create table source_commodity_ref(id INTEGER PRIMARY KEY,
            source_facility_id integer,
            source_facility_name text,
            source_facility_type_id integer, --lets us differentiate spiderweb from processor
            commodity_id integer,
            commodity_name text,
            units text,
            phase_of_matter text,
            max_transport_distance numeric,
            max_transport_distance_flag text,
            share_max_transport_distance text,
            CONSTRAINT unique_source_and_name UNIQUE(commodity_id, source_facility_id))
            ;

            insert or ignore into source_commodity_ref (
            source_facility_id,
            source_facility_name,
            source_facility_type_id,
            commodity_id,
            commodity_name,
            units,
            phase_of_matter,
            max_transport_distance,
            max_transport_distance_flag,
            share_max_transport_distance)
            select
            f.facility_id,
            f.facility_name,
            f.facility_type_id,
            c.commodity_id,
            c.commodity_name,
            c.units,
            c.phase_of_matter,
            (case when c.max_transport_distance is not null then
            c.max_transport_distance else Null end) max_transport_distance,
            (case when c.max_transport_distance is not null then 'Y' else 'N' end) max_transport_distance_flag,
            (case when ifnull(c.share_max_transport_distance, 'N') = 'Y' then 'Y' else 'N' end) share_max_transport_distance

            from commodities c, facilities f, facility_commodities fc
            where f.facility_id = fc.facility_id
            and f.ignore_facility = 'false'
            and fc.commodity_id = c.commodity_id
            and fc.io = 'o'
            and ifnull(c.share_max_transport_distance, 'N') != 'Y'
            ;
            
            insert or ignore into source_commodity_ref (
            source_facility_id,
            source_facility_name,
            source_facility_type_id,
            commodity_id,
            commodity_name,
            units,
            phase_of_matter,
            max_transport_distance,
            max_transport_distance_flag,
            share_max_transport_distance)
            select
            sc.source_facility_id,
            sc.source_facility_name,
            sc.source_facility_type_id,
            o.commodity_id,
            c.commodity_name,
            c.units,
            c.phase_of_matter,
            sc.max_transport_distance,
            sc.max_transport_distance_flag,
            o.share_max_transport_distance
            from source_commodity_ref sc, facility_commodities i, facility_commodities o, commodities c
            where o.share_max_transport_distance = 'Y'
            and sc.commodity_id = i.commodity_id
            and o.facility_id = i.facility_id
            and o.io = 'o'
            and i.io = 'i'
            and o.commodity_id = c.commodity_id
            ;
            """
                                  )
        # source_commodity_ref only has commodities that can flow out of a facility
        # for example, commodities that only exist in a candidate process, not processor, are excluded
        # does include entries even if there's no max transport distance, has a flag for that

    return


# ===============================================================================


def generate_all_vertices(the_scenario, logger):
    i = np.load("scenario_num.npy")
    t = np.load("time_horizon.npy")
    j = np.load("earthquake_day.npy")
    logger.info("scenario {}".format(i))
    logger.info("time horizon {}".format(t))
    logger.info("earthquake day {}".format(j))

    # ===============================================================================
    # reassign default schedule to test user input schedules
    schedule_path = the_scenario.schedule
    schedule_dict = create_schedules_from_input(schedule_path, logger)
    default_sched = schedule_dict['default']  # should be same as previous default schedule
    # default_sched = schedule_dict['365b']  # only available on day 365
    # default_sched = schedule_dict['365c']  # available half-time, except full-time on days 10, 20, 100
    # default_sched = schedule_dict['2day']  # 2-day schedule with half availabilities on both days
    # default_sched = schedule_dict['harvest']  # 52-week schedule with 6 weeks of harvest availability
    # ===============================================================================


    logger.info("START: generate_all_vertices table")


    total_potential_production = {}
    multi_commodity_name = "multicommodity"

    storage_availability = 1

    with sqlite3.connect(the_scenario.main_db) as main_db_con:

        # create the vertices table
        main_db_con.executescript("""
        drop table if exists vertices
        ;

        create table if not exists vertices (
        vertex_id INTEGER PRIMARY KEY, location_id,
        facility_id integer, facility_name text, facility_type_id integer, schedule_day integer,
        commodity_id integer, activity_level numeric, storage_vertex binary,
        udp numeric, supply numeric, demand numeric,
        source_facility_id integer,
        iob text, --allows input, output, or both
        CONSTRAINT unique_vertex UNIQUE(facility_id, schedule_day, commodity_id, source_facility_id, storage_vertex))
        ;""")

        # create indexes for the networkx nodes and links tables
        main_db_con.executescript("""
        CREATE INDEX IF NOT EXISTS node_index ON networkx_nodes (node_id, location_id)
        ;
        
        create index if not exists nx_edge_index on
        networkx_edges(from_node_id, to_node_id,
        artificial, mode_source, mode_source_OID,
        miles, route_cost_scaling, capacity)
        ;
        """)

        # create the schedules table
        main_db_con.execute("""
            create table if not exists schedules (
            schedule_id INTEGER PRIMARY KEY, 
            schedule_name text, 
            default_value integer, 
            full_schedule text, 
            total_days integer)
            ;
            """)

        # create default schedules
        main_db_con.execute("""
        -- noinspection SqlResolve

        INSERT INTO schedules(schedule_name, default_value, total_days)
        select '{}', {}, {}
        WHERE NOT EXISTS(SELECT schedule_name from schedules WHERE schedule_name = '{}')
        ;""".format('default_schedule', 1, 2, 'default_schedule'))

        main_db_con.execute("""INSERT INTO schedules(schedule_name, default_value, total_days)
        select '{}', {}, {}
        WHERE NOT EXISTS(SELECT schedule_name from schedules WHERE schedule_name = '{}')
        ;""".format('default_1_day_schedule', 1, 1, 'default_1_day_schedule'))

        # --------------------------------

        db_cur = main_db_con.cursor()
        # nested cursor
        db_cur4 = main_db_con.cursor()
        counter = 0
        total_facilities = 0

        for row in db_cur.execute("select count(distinct facility_id) from  facilities;"):
            total_facilities = row[0]

        # create vertices for each non-ignored facility facility
        # facility_type can be "raw_material_producer", "ultimate_destination","processor";
        # get id from facility_type_id table
        # any other facility types are not currently handled

        facility_data = db_cur.execute("""
        select facility_id,
        facility_type,
        facility_name,
        location_id,
        f.facility_type_id

        from facilities f, facility_type_id ft
        where ignore_facility = '{}'
        and f.facility_type_id = ft.facility_type_id;
        """.format('false'))
        facility_data = facility_data.fetchall()
        for row_a in facility_data:

            db_cur2 = main_db_con.cursor()
            facility_id = row_a[0]
            facility_type = row_a[1]
            facility_name = row_a[2]
            facility_location_id = row_a[3]
            facility_type_id = row_a[4]
            if counter % 10000 == 1:
                logger.info("vertices created for {} facilities of {}".format(counter, total_facilities))
                for row_d in db_cur4.execute("select count(distinct vertex_id) from vertices;"):
                    logger.info('{} vertices created'.format(row_d[0]))
            counter = counter + 1

            if facility_type == "processor":
                # actual processors - will deal with endcaps in edges section

                # each processor facility should have 1 input commodity with 1 storage vertex, 1 or more output
                # commodities each with 1 storage vertex, and 1 primary processing vertex replicate by time,
                # create primary vertex, replicate by commodity to create storage vertices; can also create primary
                # vertex for input commodity do this for each subcommodity now instead of each commodity aggregate
                # subcommodities at primary processor vertex

                # create processor vertices for any commodities that do not inherit max transport distance
                proc_data = db_cur2.execute("""select fc.commodity_id,
               ifnull(fc.quantity, 0),
               fc.units,
               ifnull(c.supertype, c.commodity_name),
               fc.io,
               mc.commodity_id,
               c.commodity_name,
                ifnull(s.source_facility_id, 0)
               from facility_commodities fc, commodities c, commodities mc
                left outer join source_commodity_ref s 
                on (fc.commodity_id = s.commodity_id and s.max_transport_distance_flag = 'Y')
               where fc.facility_id = {}
               and fc.commodity_id = c.commodity_id
               and mc.commodity_name = '{}';""".format(facility_id, multi_commodity_name))

                proc_data = proc_data.fetchall()
                # entry for each incoming commodity and its potential sources
                # each outgoing commodity with this processor as their source IF there is a max commod distance
                for row_b in proc_data:

                    #=========================================Modification====================================================================
                    # Following part is modified to include time-varying facility capacity in the aftermath of earthquake
                    commodity_id = row_b[0]
                    # if day < repair time for this facility, 
                    if j < repair_time_facility[float(facility_id)-2][t+1][i]:
                        quantity = facility_cap[float(facility_id)-2][t+1][i]*row_b[1]
                    else:
                        quantity = facility_cap_noEarthquake[float(facility_id)-2][t+1][i]*row_b[1]
                    #========================================End=================================================================================
                        
                    # units = row_b[2]
                    # commodity_supertype = row_b[3]
                    io = row_b[4]
                    id_for_mult_commodities = row_b[5]
                    commodity_name = row_b[6]
                    source_facility_id = row_b[7]
                    new_source_facility_id = facility_id

                    day = default_sched.first_day
                    stop_day = default_sched.last_day
                    availability = default_sched.availability
                    # vertices for generic demand type, or any subtype specified by the destination
                    while day <= stop_day:
                        if io == 'i':
                            main_db_con.execute("""insert or ignore into vertices ( location_id, facility_id, 
                            facility_type_id, facility_name, schedule_day, commodity_id, activity_level, 
                            storage_vertex, source_facility_id, iob) values ({}, {}, {}, '{}', {}, {}, {}, {}, {}, 
                            '{}' );""".format(facility_location_id, facility_id, facility_type_id, facility_name, day,
                                              id_for_mult_commodities, availability[day], primary,
                                              new_source_facility_id, 'b'))

                            main_db_con.execute("""insert or ignore into vertices ( location_id, facility_id, 
                            facility_type_id, facility_name, schedule_day, commodity_id, activity_level, 
                            storage_vertex, demand, source_facility_id, iob) values ({}, {}, {}, '{}', {}, {}, {}, 
                            {}, {}, {}, '{}');""".format(facility_location_id, facility_id, facility_type_id,
                                                         facility_name,
                                                         day, commodity_id, storage_availability, storage, quantity,
                                                         source_facility_id, io))

                        else:
                            if commodity_name != 'total_fuel':
                                main_db_con.execute("""insert or ignore into vertices ( location_id, facility_id, 
                                facility_type_id, facility_name, schedule_day, commodity_id, activity_level, 
                                storage_vertex, supply, source_facility_id, iob) values ({}, {}, {}, '{}', {}, {}, 
                                {}, {}, {}, {}, '{}');""".format(
                                    facility_location_id, facility_id, facility_type_id, facility_name, day,
                                    commodity_id, storage_availability, storage, quantity, source_facility_id, io))
                        day = day + 1

            # for RMPSs specifically, source_facility_id must match facility_id; could do that conditionally within sql
            # if we bring storage facilities back (12-2018), we would want them split by source_facility
            elif facility_type == "raw_material_producer":
                rmp_data = db_cur.execute("""select fc.commodity_id, fc.quantity, fc.units,
                ifnull(s.source_facility_id, 0), io
                from facility_commodities fc
                left outer join source_commodity_ref s 
                on (fc.commodity_id = s.commodity_id 
                and s.max_transport_distance_flag = 'Y' 
                and s.source_facility_id = {})
                where fc.facility_id = {};""".format(facility_id, facility_id))

                rmp_data = rmp_data.fetchall()
              
                for row_b in rmp_data:
                    commodity_id = row_b[0]
                    #=========================================Modification====================================================================
                    # Following part is modified to include time-varying facility capacity in FTOT
                    if j < repair_time_facility[float(facility_id)-1][t+1][i]:
                        quantity = facility_cap[float(facility_id)-1][t+1][i]*row_b[1]
                    else:
                        quantity = facility_cap_noEarthquake[float(facility_id)-1][t+1][i]*row_b[1]
                  
                    #=========================================End=================================================================================
                        
                    # units = row_b[2]
                    source_facility_id = row_b[3]
                    iob = row_b[4]

                    if commodity_id in total_potential_production:
                        total_potential_production[commodity_id] = total_potential_production[commodity_id] + quantity
                    else:
                        total_potential_production[commodity_id] = quantity
                    day = default_sched.first_day
                    stop_day = default_sched.last_day
                    availability = default_sched.availability

                    while day <= stop_day:
                        main_db_con.execute("""insert or ignore into vertices (
                        location_id, facility_id, facility_type_id, facility_name, schedule_day, commodity_id, 
                        activity_level, storage_vertex, supply,
                            source_facility_id, iob)
                        values ({}, {}, {}, '{}', {}, {}, {}, {}, {},
                        {}, '{}');""".format(facility_location_id, facility_id, facility_type_id, facility_name, day,
                                             commodity_id, availability[day], primary, quantity,
                                             source_facility_id, iob))
                        main_db_con.execute("""insert or ignore into vertices (
                        location_id, facility_id, facility_type_id, facility_name, schedule_day, commodity_id, 
                        activity_level, storage_vertex, supply,
                            source_facility_id, iob)
                        values ({}, {}, {}, '{}', {}, {}, {}, {}, {},
                        {}, '{}');""".format(facility_location_id, facility_id, facility_type_id, facility_name, day,
                                             commodity_id, storage_availability, storage, quantity,
                                             source_facility_id, iob))
                        day = day + 1

            elif facility_type == "storage":  # storage facility
                # handle as such, exploding by time and commodity
                # print type(f.commodity_slate)
                # print type(f.commodity_slate.AllAllowedCommodities())
                storage_fac_data = db_cur.execute("""select
                fc.commodity_id,
                fc.quantity,
                fc.units,
                ifnull(s.source_facility_id, 0),
                io
                from facility_commodities fc
                left outer join source_commodity_ref s 
                on (fc.commodity_id = s.commodity_id and s.max_transport_distance_flag = 'Y')
                where fc.facility_id = {} ;""".format(facility_id))

                storage_fac_data = storage_fac_data.fetchall()

                for row_b in storage_fac_data:
                    commodity_id = row_b[0]
                    # quantity = row_b[1]
                    # units = row_b[2]
                    source_facility_id = row_b[3]  # 0 if not source-tracked
                    iob = row_b[4]

                    day = default_sched.first_day
                    stop_day = default_sched.last_day
                    # availability = default_sched.availability
                    while day <= stop_day:
                        main_db_con.execute("""insert or ignore into vertices (
                       location_id, facility_id, facility_type_id, facility_name, schedule_day, commodity_id, 
                       storage_vertex,
                            source_facility_id, iob)
                       values ({}, {}, {}, '{}', {}, {}, {},
                       {}, '{}');""".format(facility_location_id, facility_id, facility_type_id, facility_name, day,
                                            commodity_id, storage,
                                            source_facility_id, iob))
                        day = day + 1

            elif facility_type == "ultimate_destination":

                dest_data = db_cur2.execute("""select
                fc.commodity_id,
                ifnull(fc.quantity, 0),
                fc.units,
                fc.commodity_id,
                ifnull(c.supertype, c.commodity_name),
                ifnull(s.source_facility_id, 0),
                io
                from facility_commodities fc, commodities c
                left outer join source_commodity_ref s 
                on (fc.commodity_id = s.commodity_id and s.max_transport_distance_flag = 'Y')
                where fc.facility_id = {}
                and fc.commodity_id = c.commodity_id;""".format(facility_id))

                dest_data = dest_data.fetchall()

                for row_b in dest_data:
                    commodity_id = row_b[0]
                    #=========================================Modification====================================================================
                    # Following part is modified to include time-varying facility capacity in FTOT
                    if j < repair_time_facility[float(facility_id)][t+1][i]:
                        quantity = facility_cap[float(facility_id)][t+1][i]*row_b[1]
                    else:
                        quantity = row_b[1]
                    #=====================================End=====================================================================================
                    # units = row_b[2]
                    # commodity_id = row_b[3]
                    commodity_supertype = row_b[4]
                    source_facility_id = row_b[5]
                    iob = row_b[6]
                    zero_source_facility_id = 0  # material merges at primary vertex

                    # this is where the alternate schedule for destination demand can be set
                    day = default_sched.first_day
                    stop_day = default_sched.last_day
                    availability = default_sched.availability
                    # vertices for generic demand type, or any subtype specified by the destination
                    while day <= stop_day and quantity > 0:
                        main_db_con.execute("""insert or ignore into vertices (
                       location_id, facility_id, facility_type_id, facility_name, schedule_day, commodity_id, 
                       activity_level, storage_vertex, demand, udp,
                             source_facility_id, iob)
                       values ({}, {}, {}, '{}', {},
                        {}, {}, {}, {}, {},
                        {}, '{}');""".format(facility_location_id, facility_id, facility_type_id, facility_name, day,
                                             commodity_id, availability[day], primary, quantity,
                                             the_scenario.unMetDemandPenalty,
                                             zero_source_facility_id, iob))
                        main_db_con.execute("""insert or ignore into vertices (
                       location_id, facility_id, facility_type_id, facility_name, schedule_day, commodity_id, 
                       activity_level, storage_vertex, demand,
                            source_facility_id, iob)
                       values ({}, {}, {}, '{}', {}, {}, {}, {}, {},
                       {}, '{}');""".format(facility_location_id, facility_id, facility_type_id, facility_name, day,
                                            commodity_id, storage_availability, storage, quantity,
                                            source_facility_id, iob))
                        day = day + 1
                    # vertices for other fuel subtypes that match the destination's supertype
                    # if the subtype is in the commodity table, it is produced by some facility in the scenario
                    db_cur3 = main_db_con.cursor()
                    for row_c in db_cur3.execute("""select commodity_id, units from commodities
                    where supertype = '{}';""".format(commodity_supertype)):
                        new_commodity_id = row_c[0]
                        # new_units = row_c[1]
                        while day <= stop_day:
                            main_db_con.execute("""insert or ignore into vertices ( location_id, facility_id, 
                            facility_type_id, facility_name, schedule_day, commodity_id, activity_level, 
                            storage_vertex, demand, udp, source_facility_id, iob) values ({}, {}, {}, '{}', {}, {}, 
                            {}, {}, {}, {}, {}, '{}');""".format(facility_location_id, facility_id, facility_type_id,
                                                                 facility_name,
                                                                 day, new_commodity_id, availability[day], primary,
                                                                 quantity,
                                                                 the_scenario.unMetDemandPenalty,
                                                                 zero_source_facility_id, iob))
                            main_db_con.execute("""insert or ignore into vertices (
                              location_id, facility_id, facility_type_id, facility_name, schedule_day, commodity_id, 
                              activity_level, storage_vertex, demand,
                            source_facility_id, iob)
                              values ({}, {}, {}, '{}', {}, {}, {}, {}, {},
                              {}, '{}');""".format(facility_location_id, facility_id, facility_type_id, facility_name,
                                                   day, new_commodity_id, storage_availability, storage, quantity,
                                                   source_facility_id, iob))
                            day = day + 1


            else:
                logger.warning(
                    "error, unexpected facility_type: {}, facility_type_id: {}".format(facility_type, facility_type_id))

    for row_d in db_cur4.execute("select count(distinct vertex_id) from vertices;"):
        logger.info('{} vertices created'.format(row_d[0]))

    logger.debug("total possible production in scenario: {}".format(total_potential_production))


# ===============================================================================


def add_storage_routes(the_scenario, logger):
    logger.info("start: add_storage_routes")
    # these are loops to and from the same facility; when multiplied to edges,
    # they will connect primary to storage vertices, and storage vertices day to day
    # will always create edge for this route from storage to storage vertex
    # IF a primary vertex exists, will also create an edge connecting the storage vertex to the primary

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        logger.debug("create the storage_routes table")

        main_db_con.execute("drop table if exists storage_routes;")
        main_db_con.execute("""create table if not exists storage_routes as
        select facility_name || '_storage' as route_name,
        location_id,
        facility_id,
        facility_name as o_name,
        facility_name as d_name,
        {} as cost_1,
        {} as cost_2,
        1 as travel_time,
        {} as storage_max,
        0 as storage_min
        from facilities
        where ignore_facility = 'false'
        ;""".format(storage_cost_1, storage_cost_2, facility_onsite_storage_max))
        main_db_con.execute("""create table if not exists route_reference(
        route_id INTEGER PRIMARY KEY, route_type text, route_name text, scenario_rt_id integer,
        CONSTRAINT unique_routes UNIQUE(route_type, route_name, scenario_rt_id));""")
        main_db_con.execute(
            "insert or ignore into route_reference select null,'storage', route_name, 0 from storage_routes;")

    return


# ===============================================================================


def generate_connector_and_storage_edges(the_scenario, logger):
    logger.info("START: generate_connector_and_storage_edges")

    multi_commodity_name = "multicommodity"

    with sqlite3.connect(the_scenario.main_db) as main_db_con:

        db_cur = main_db_con.cursor()
        if ('pipeline_crude_trf_rts' in the_scenario.permittedModes) or (
                'pipeline_prod_trf_rts' in the_scenario.permittedModes):
            logger.info("create indices for the capacity_nodes and pipeline_mapping tables")
            main_db_con.executescript(
                """
            CREATE INDEX IF NOT EXISTS pm_index 
            ON pipeline_mapping (id, id_field_name, mapping_id_field_name, mapping_id);
            CREATE INDEX IF NOT EXISTS cn_index ON capacity_nodes (source, id_field_name, source_OID);
            """)

        for row in db_cur.execute(
                "select commodity_id from commodities where commodity_name = '{}';""".format(multi_commodity_name)):
            id_for_mult_commodities = row[0]

        # create storage & connector edges
        main_db_con.execute("drop table if exists edges;")
        main_db_con.executescript("""
        create table edges (edge_id INTEGER PRIMARY KEY,
        route_id integer,
        from_node_id integer,
        to_node_id integer,
        start_day integer,
        end_day integer,
        commodity_id integer,
        o_vertex_id integer,
        d_vertex_id integer,
        max_edge_capacity numeric,
        volume numeric,
        capac_minus_volume_zero_floor numeric,
        min_edge_capacity numeric,
        capacity_units text,
        units_conversion_multiplier numeric,
        edge_flow_cost numeric,
        edge_flow_cost2 numeric,
        edge_type text,
        nx_edge_id integer,
        mode text,
        mode_oid integer,
        miles numeric,
        simple_mode text,
        tariff_id numeric,
        phase_of_matter text,
        source_facility_id integer,
        miles_travelled numeric,
        children_created text,
        edge_count_from_source integer,
        total_route_cost numeric,
        CONSTRAINT unique_nx_subc_day UNIQUE(nx_edge_id, commodity_id, source_facility_id, start_day))
        ;

        insert or ignore into edges (route_id,
        start_day, end_day,
        commodity_id, o_vertex_id, d_vertex_id,
        max_edge_capacity, min_edge_capacity,
        edge_flow_cost, edge_flow_cost2,edge_type,
        source_facility_id)
         select o.route_id, o.schedule_day, d.schedule_day,
         o.commodity_id, o.vertex_id, d.vertex_id,
         o.storage_max, o.storage_min,
         o.cost_1, o.cost_2, 'storage',
         o.source_facility_id
        from vertices d,
        (select v.vertex_id, v.schedule_day,
        v.commodity_id, v.storage_vertex, v.source_facility_id,
        t.*
        from vertices v,
        (select sr.route_name, o_name, d_name, cost_1,cost_2, travel_time,
        storage_max, storage_min, rr.route_id, location_id, facility_id
        from storage_routes sr, route_reference rr where sr.route_name = rr.route_name) t
        where v.facility_id = t.facility_id
        and v.storage_vertex = 1) o
        where d.facility_id = o.facility_id
        and d.schedule_day = o.schedule_day+o.travel_time
        and d.commodity_id = o.commodity_id
        and o.vertex_id != d.vertex_id
        and d.storage_vertex = 1
        and d.source_facility_id = o.source_facility_id
        ;

        insert or ignore into edges  (route_id, start_day, end_day,
        commodity_id, o_vertex_id, d_vertex_id,
        edge_flow_cost, edge_type,
        source_facility_id)
         select s.route_id, s.schedule_day, p.schedule_day,
         (case when s.commodity_id = {} then p.commodity_id else s.commodity_id end) commodity_id,
         --inbound commodies start at storage and go into primary
         --outbound starts at primary and goes into storage
         --anything else is an error for a connector edge
         (case when fc.io = 'i' then s.vertex_id
         when fc.io = 'o' then p.vertex_id
         else 0 end) as o_vertex,
         (case when fc.io = 'i' then p.vertex_id
         when fc.io = 'o' then s.vertex_id
         else 0 end) as d_vertex,
         0, 'connector',
         s.source_facility_id
        from vertices p, facility_commodities fc,
        --s for storage vertex info, p for primary vertex info
        (select v.vertex_id, v.schedule_day,
        v.commodity_id, v.storage_vertex, v.source_facility_id,
        t.*
        from vertices v,
        (select sr.route_name, o_name, d_name, cost_1,cost_2, travel_time,
        storage_max, storage_min, rr.route_id, location_id, facility_id
        from storage_routes sr, route_reference rr where sr.route_name = rr.route_name) t --t is route data
        where v.facility_id = t.facility_id
        and v.storage_vertex = 1) s
        --from storage into primary, same day = inbound connectors
        where p.facility_id = s.facility_id
        and p.schedule_day = s.schedule_day
        and (p.commodity_id = s.commodity_id or p.commodity_id = {} )
        and p.facility_id = fc.facility_id
        and fc.commodity_id = s.commodity_id
        and p.storage_vertex = 0
        --either edge is inbound and aggregating, or kept separate by source, or primary vertex is not source tracked
        and 
        (p.source_facility_id = 0 or p.source_facility_id = p.facility_id or p.source_facility_id = s.source_facility_id)
        ;""".format(id_for_mult_commodities, id_for_mult_commodities))

        for row_d in db_cur.execute("select count(distinct edge_id) from edges where edge_type = 'connector';"):
            logger.info('{} connector edges created'.format(row_d[0]))
        # clear any transport edges from table
            db_cur.execute("delete from edges where edge_type = 'transport';")

    return


# ===============================================================================


def generate_first_edges_from_source_facilities(the_scenario, logger):
    # ===============================================================================
    # reassign default schedule to test user input schedules
    schedule_path = the_scenario.schedule
    schedule_dict = create_schedules_from_input(schedule_path, logger)
    default_sched = schedule_dict['default']  # should be same as previous default schedule
    # default_sched = schedule_dict['365b']  # only available on day 365
    # default_sched = schedule_dict['365c']  # available half-time, except full-time on days 10, 20, 100
    # default_sched = schedule_dict['2day']  # 2-day schedule with half availabilities on both days
    # default_sched = schedule_dict['harvest']  # 52-week schedule with 6 weeks of harvest availability
    # ===============================================================================

    logger.info("START: generate_first_edges_from_source_facilities")
    # create edges table
    # plan to generate start and end days based on nx edge time to traverse and schedule
    # can still have route_id, but only for storage routes now; nullable

    # multi_commodity_name = "multicommodity"
    transport_edges_created = 0

    with sqlite3.connect(the_scenario.main_db) as main_db_con:

        db_cur = main_db_con.cursor()
        edges_requiring_children = 0

        counter = 0
        # for row in ("select commodity_id from commodities where commodity_name = '{}';
        # """.format(multi_commodity_name)):
        #     id_for_mult_commodities = row[0]
        # for row in db_cur.execute("select count(*) from networkx_edges;"):
        #     total_transport_routes = row[0]

        # create transport edges, only between storage vertices and nodes, based on networkx graph
        # never touch primary vertices; either or both vertices can be null (or node-type) if it's a mid-route link
        # iterate through nx edges: if neither node has a location, create 1 edge per viable commodity
        # should also be per day, subject to nx edge schedule
        # before creating an edge, check: commodity allowed by nx and max transport distance if not null
        # will need nodes per day and commodity? or can I just check that with constraints?
        # select data for transport edges
        # ****1**** Only edges coming from RMP/source storage vertices
        # ** ONLY FOR COMMODITIES WITH A MAX TRANSPORT DISTANCE
        # set distance travelled to miles of edges; set indicator for newly created edges to 'N'
        # edge_count_from_source = 1
        # ****2**** only nx_edges coming from entries in edges (check connector nodes)
        # set distance travelling to miles of new endge plus existing input edge; set indicator for processed edges in
        # edges to 'Y'
        # only create new edge if distance travelled is less than allowed
        # repeat 2 while there are 'N' edges, for transport edges only
        # count loops
        # this is now per-vertex - rework it to not all be done in loop, but in sql block
        # connector and storage edges can be done exactly as before, in fact need to be done first,
        # now in separate method

        commodity_mode_data = main_db_con.execute("select * from commodity_mode;")
        commodity_mode_data = commodity_mode_data.fetchall()
        commodity_mode_dict = {}
        for row in commodity_mode_data:
            mode = row[0]
            commodity_id = int(row[1])
            commodity_phase = row[2]
            allowed_yn = row[3]
            commodity_mode_dict[mode, commodity_id] = allowed_yn


        source_edge_data = main_db_con.execute("""select
        ne.edge_id,
        ifnull(CAST(fn.location_id as integer), 'NULL'),
        ifnull(CAST(tn.location_id as integer), 'NULL'),
        ne.mode_source,
        ifnull(nec.phase_of_matter_id, 'NULL'),
        nec.route_cost,
        ne.from_node_id,
        ne.to_node_id,
        nec.dollar_cost,
        ne.miles,
        ne.capacity,
        ne.artificial,
        ne.mode_source_oid,
        v.commodity_id,
        v.schedule_day,
        v.vertex_id,
        v.source_facility_id,
        tv.vertex_id,
        ifnull(t.miles_travelled, 0),
        ifnull(t.edge_count_from_source, 0),
        t.mode
        from networkx_edges ne, networkx_nodes fn, networkx_nodes tn, networkx_edge_costs nec, vertices v, 
        facility_type_id ft,  facility_commodities fc
        left outer join (--get facilities with incoming transport edges with tracked mileage
        select vi.facility_id, min(ei.miles_travelled) miles_travelled, ei.source_facility_id, 
        ei.edge_count_from_source, ei.mode
         from edges ei, vertices vi
        where vi.vertex_id = ei.d_vertex_id
         and edge_type = 'transport'
         and ifnull(miles_travelled, 0) > 0
        group by vi.facility_id, ei.source_facility_id, ei.mode) t   
        on t.facility_id = v.facility_id and t.source_facility_id = v.source_facility_id and ne.mode_source = t.mode
        left outer join vertices tv
        on (CAST(tn.location_id as integer) = tv.location_id and v.source_facility_id = tv.source_facility_id)
        where v.location_id = CAST(fn.location_id as integer)
        and fc.facility_id = v.facility_id
        and fc.commodity_id = v.commodity_id
        and fc.io = 'o'
        and ft.facility_type_id = v.facility_type_id
        and (ft.facility_type = 'raw_material_producer' or t.facility_id = v.facility_id)
        and ne.from_node_id = fn.node_id
        and ne.to_node_id = tn.node_id
        and ne.edge_id = nec.edge_id
        and ifnull(ne.capacity, 1) > 0
        and v.storage_vertex = 1
        and v.source_facility_id != 0 --max commodity distance applies
        ;""")
        source_edge_data = source_edge_data.fetchall()
        for row_a in source_edge_data:

            nx_edge_id = row_a[0]
            from_location = row_a[1]
            to_location = row_a[2]
            mode = row_a[3]
            phase_of_matter = row_a[4]
            route_cost = row_a[5]
            from_node = row_a[6]
            to_node = row_a[7]
            dollar_cost = row_a[8]
            miles = row_a[9]
            # max_daily_capacity = row_a[10]
            # artificial = row_a[11]
            mode_oid = row_a[12]
            commodity_id = row_a[13]
            origin_day = row_a[14]
            vertex_id = row_a[15]
            source_facility_id = row_a[16]
            to_vertex = row_a[17]
            previous_miles_travelled = row_a[18]
            previous_edge_count = row_a[19]
            previous_mode = row_a[20]

            simple_mode = row_a[3].partition('_')[0]
            edge_count_from_source = 1 + previous_edge_count
            total_route_cost = route_cost
            miles_travelled = previous_miles_travelled + miles

            if counter % 10000 == 0:
                # logger.info("edges created for {} transport links of {}".format(counter, total_transport_routes))
                for row_d in db_cur.execute("select count(distinct edge_id) from edges;"):
                    logger.info('{} edges created'.format(row_d[0]))
            counter = counter + 1

            tariff_id = 0
            if simple_mode == 'pipeline':

                # find tariff_ids

                sql = "select mapping_id from pipeline_mapping " \
                      "where id = {} and id_field_name = 'source_OID' " \
                      "and source = '{}' and mapping_id is not null;".format(
                    mode_oid, mode)
                for tariff_row in db_cur.execute(sql):
                    tariff_id = tariff_row[0]


            if mode in the_scenario.permittedModes and (mode, commodity_id) in commodity_mode_dict.keys() \
                    and commodity_mode_dict[mode, commodity_id] == 'Y':

                # Edges are placeholders for flow variables
                # 4-17: if both ends have no location, iterate through viable commodities and days, create edge
                # for all days (restrict by link schedule if called for)
                # for all allowed commodities, as currently defined by link phase of matter

                if origin_day in range(default_sched.first_day, default_sched.last_day + 1):
                    if origin_day + fixed_route_duration <= default_sched.last_day:
                        # if link is traversable in the timeframe
                        if simple_mode != 'pipeline' or tariff_id >= 0:
                            # for allowed commodities
                            # step 1 from source is from non-Null location to (probably) null location

                            if from_location != 'NULL' and to_location == 'NULL':
                                # for each day and commodity,
                                # get the corresponding origin vertex id to include with the edge info
                                # origin vertex must not be "ultimate_destination
                                # transport link outgoing from facility - checking fc.io is more thorough
                                # than checking if facility type is 'ultimate destination'
                                # only connect to vertices with matching source_facility_id
                                # source_facility_id is zero for commodities without source tracking

                                main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                    start_day, end_day, commodity_id,
                                    o_vertex_id,
                                    min_edge_capacity,edge_flow_cost, edge_flow_cost2,
                                    edge_type, nx_edge_id, mode, mode_oid, miles, simple_mode, 
                                    tariff_id,phase_of_matter,
                                     source_facility_id,
                                    miles_travelled, children_created, edge_count_from_source, total_route_cost) 
                                    VALUES ({}, {},
                                    {}, {}, {},
                                    {},
                                    {}, {}, {},
                                    '{}',{},'{}',{},
                                    {},'{}',{},'{}',
                                    {},
                                    {},'{}',{},{});
                                    """.format(from_node, to_node,
                                               origin_day, origin_day + fixed_route_duration, commodity_id,
                                               vertex_id,
                                               default_min_capacity, route_cost, dollar_cost,
                                               'transport', nx_edge_id, mode, mode_oid,
                                               miles, simple_mode, tariff_id, phase_of_matter,
                                               source_facility_id,
                                               miles_travelled, 'N', edge_count_from_source, total_route_cost))


                            elif from_location != 'NULL' and to_location != 'NULL':
                                # for each day and commodity, get the corresponding origin and destination vertex ids
                                # to include with the edge info

                                main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                        start_day, end_day, commodity_id,
                                        o_vertex_id, d_vertex_id,
                                        min_edge_capacity, edge_flow_cost, edge_flow_cost2,
                                        edge_type, nx_edge_id, mode, mode_oid, miles, simple_mode, tariff_id, 
                                        phase_of_matter,
                                        source_facility_id,
                                        miles_travelled, children_created, edge_count_from_source, total_route_cost) 
                                        VALUES ({}, {},
                                        {}, {}, {},
                                        {}, {},
                                        {}, {}, {},
                                        '{}',{},'{}', {},
                                        {},'{}',{},'{}',
                                        {},
                                        {},'{}',{},{});
                                        """.format(from_node, to_node,
                                                   origin_day, origin_day + fixed_route_duration, commodity_id,
                                                   vertex_id, to_vertex,
                                                   default_min_capacity, route_cost, dollar_cost,
                                                   'transport', nx_edge_id, mode, mode_oid,
                                                   miles, simple_mode, tariff_id, phase_of_matter,
                                                   source_facility_id,
                                                   miles_travelled, 'N', edge_count_from_source, total_route_cost))

        for row in db_cur.execute("select count(distinct edge_id) from edges where edge_type = 'transport';"):
            transport_edges_created = row[0]
            logger.info('{} transport edges created'.format(transport_edges_created))
        for row in db_cur.execute("""select count(distinct edge_id), children_created from edges
         where edge_type = 'transport'
         group by children_created;"""):
            if row[1] == 'N':
                edges_requiring_children = row[0]
                # logger.info('{} transport edges that need children'.format(row[0]))

            elif row[1] == 'Y':
                logger.info('{} transport edges that have already been checked for children'.format(row[0]))
                edges_requiring_children = transport_edges_created - row[0]
            # edges_requiring_children is updated under either condition here, since one may not be triggered
            logger.info('{} transport edges that need children'.format(edges_requiring_children))

    return


# ===============================================================================


def generate_all_edges_from_source_facilities(the_scenario, logger):
    # method only runs for commodities with a max commodity constraint
    # ===============================================================================
    # reassign default schedule to test user input schedules
    schedule_path = the_scenario.schedule
    schedule_dict = create_schedules_from_input(schedule_path, logger)
    default_sched = schedule_dict['default']  # should be same as previous default schedule
    # default_sched = schedule_dict['365b']  # only available on day 365
    # default_sched = schedule_dict['365c']  # available half-time, except full-time on days 10, 20, 100
    # default_sched = schedule_dict['2day']  # 2-day schedule with half availabilities on both days
    # default_sched = schedule_dict['harvest']  # 52-week schedule with 6 weeks of harvest availability
    # ===============================================================================

    logger.info("START: generate_all_edges_from_source_facilities")

    multi_commodity_name = "multicommodity"
    # initializations - all of these get updated if >0 edges exist
    # transport_edges_created = 0
    edges_requiring_children = 0
    endcap_edges = 0
    edges_resolved = 0

    with sqlite3.connect(the_scenario.main_db) as main_db_con:

        db_cur = main_db_con.cursor()
        transport_edges_created = 0
        nx_edge_count = 0
        source_based_edges_created = 0

        for row_d in db_cur.execute("""select count(distinct e.edge_id), count(distinct e.nx_edge_id)
        from edges e where e.edge_type = 'transport'
        and children_created in ('N', 'Y', 'E');"""):
            source_based_edges_created = row_d[0]

        for row_d in db_cur.execute("""select count(distinct e.edge_id), count(distinct e.nx_edge_id)
        from edges e where e.edge_type = 'transport';"""):
            transport_edges_created = row_d[0]
            nx_edge_count = row_d[1]


        commodity_mode_data = main_db_con.execute("select * from commodity_mode;")
        commodity_mode_data = commodity_mode_data.fetchall()
        commodity_mode_dict = {}
        for row in commodity_mode_data:
            mode = row[0]
            commodity_id = int(row[1])
            commodity_phase = row[2]
            allowed_yn = row[3]
            commodity_mode_dict[mode, commodity_id] = allowed_yn

        current_edge_data = db_cur.execute("""select count(distinct edge_id), children_created
             from edges
             where edge_type = 'transport'
             group by children_created
             order by children_created asc;""")
        current_edge_data = current_edge_data.fetchall()
        for row in current_edge_data:
            if row[1] == 'N':
                edges_requiring_children = row[0]
            elif row[1] == 'Y':
                edges_resolved = row[0]
            elif row[1] == 'E':
                endcap_edges = row[0]
            if source_based_edges_created == edges_resolved + endcap_edges:
                edges_requiring_children = 0
            if source_based_edges_created == edges_requiring_children + endcap_edges:
                edges_resolved = 0
            if source_based_edges_created == edges_requiring_children + edges_resolved:
                endcap_edges = 0

        logger.info(
            '{} transport edges created; {} require children'.format(transport_edges_created, edges_requiring_children))

        # for row in ("select commodity_id from commodities where commodity_name = '{}';""".format(multi_commodity_name)):
        #     id_for_mult_commodities = row[0]
        # for row in db_cur.execute("select count(*) from networkx_edges;"):
        #     total_transport_routes = row[0]

        # set up a table to keep track of endcap nodes
        sql = """
        drop table if exists endcap_nodes;
        create table if not exists endcap_nodes(
        node_id integer NOT NULL,

        location_id integer,

        --mode of edges that it's an endcap for
        mode_source text NOT NULL,

        --facility it's an endcap for
        source_facility_id integer NOT NULL,

        --commodities it's an endcap for
        commodity_id integer NOT NULL,

        CONSTRAINT endcap_key PRIMARY KEY (node_id, mode_source, source_facility_id, commodity_id))
        --the combination of these four (excluding location_id) should be unique,
        --and all fields except location_id should be filled
        ;"""
        db_cur.executescript(sql)

        # create transport edges, only between storage vertices and nodes, based on networkx graph
        # never touch primary vertices; either or both vertices can be null (or node-type) if it's a mid-route link
        # iterate through nx edges: if neither node has a location, create 1 edge per viable commodity
        # should also be per day, subject to nx edge schedule
        # before creating an edge, check: commodity allowed by nx and max transport distance if not null
        # will need nodes per day and commodity? or can I just check that with constraints?
        # select data for transport edges
        # ****1**** Only edges coming from RMP/source storage vertices
        # set distance travelled to miles of edges; set indicator for newly created edges to 'N'
        # edge_count_from_source = 1
        # ****2**** only nx_edges coming from entries in edges (check connector nodes)
        # set distance travelling to miles of new endge plus existing input edge;
        # set indicator for processed edges in edges to 'Y'
        # only create new edge if distance travelled is less than allowed
        # repeat 2 while there are 'N' edges, for transport edges only

        edge_into_facility_counter = 0
        while_count = 0

        while edges_requiring_children > 0:
            while_count = while_count+1

            # --get nx edges that align with the existing "in edges" - data from nx to create new edges --for each of
            # those nx_edges, if they connect to more than one "in edge" in this batch, only consider connecting to
            # the shortest -- if there is a valid nx_edge to build, the crossing node is not an endcap if total miles
            # of the new route is over max transport distance, then endcap what if one child goes over max transport
            # and another doesn't then the node will get flagged as an endcap, and another path may continue off it,
            # allow both for now --check that day and commodity are permitted by nx

            potential_edge_data = main_db_con.execute("""
            select
            ch.edge_id as ch_nx_edge_id,
            ifnull(CAST(chfn.location_id as integer), 'NULL') fn_location_id,
            ifnull(CAST(chtn.location_id as integer), 'NULL') tn_location_id,
            ch.mode_source,
            p.phase_of_matter,
            nec.route_cost,
            ch.from_node_id,
            ch.to_node_id,
            nec.dollar_cost,
            ch.miles,
            ch.capacity,
            ch.artificial,
            ch.mode_source_oid,
            --parent edge into
            p.commodity_id,
            p.end_day,
             --parent's dest. vertex if exists
            ifnull(p.d_vertex_id,0) o_vertex,
            p.source_facility_id,
            p.leadin_miles_travelled,

            (p.edge_count_from_source +1) as new_edge_count,
            (p.total_route_cost + nec.route_cost) new_total_route_cost,
            p.edge_id leadin_edge,
            p.nx_edge_id leadin_nx_edge,

            --new destination vertex if exists
            ifnull(chtv.vertex_id,0) d_vertex,

            sc.max_transport_distance

            from
            (select count(edge_id) parents,
             min(miles_travelled) leadin_miles_travelled,
             *
            from edges
            where children_created = 'N'
            -----------------do not mess with this "group by"
            group by to_node_id, source_facility_id, commodity_id, end_day
            ------------------it affects which columns we're checking over for min miles travelled
            --------------so that we only get the parent edges we want
            order by parents desc
            ) p, --parent edges to use in this batch
            networkx_edges ch,
            networkx_edge_costs nec,
            source_commodity_ref sc,
            networkx_nodes chfn,
            networkx_nodes chtn
            left outer join vertices chtv
            on (CAST(chtn.location_id as integer) = chtv.location_id
            and p.source_facility_id = chtv.source_facility_id
            and chtv.commodity_id = p.commodity_id
            and p.end_day = chtv.schedule_day
            and chtv.iob = 'i')

            where p.to_node_id = ch.from_node_id
            --and p.mode = ch.mode_source --build across modes, control at conservation of flow
            and ch.to_node_id = chtn.node_id
            and ch.from_node_id = chfn.node_id
            and p.phase_of_matter = nec.phase_of_matter_id
            and ch.edge_id = nec.edge_id
            and ifnull(ch.capacity, 1) > 0
            and p.commodity_id = sc.commodity_id
            ;""")

            # --should only get a single leadin edge per networkx/source/commodity/day combination
            # leadin edge should be in route_data, current set of min. identifiers
            # if we're trying to add an edge that has an entry in route_data, new miles travelled must be less

            potential_edge_data = potential_edge_data.fetchall()
            # logger.info("potential_edge_data.fetchall() completed")
            # pdb.set_trace()

            main_db_con.execute("update edges set children_created = 'Y' where children_created = 'N';")
            # this deliberately includes updating "parent" edges that did not get chosen because they weren't the
            # current shortest path those edges are still "resolved" by this batch logger.info(
            # "edges.children_created updated to 'Y' for set about to be addressed")

            for row_a in potential_edge_data:
                nx_edge_id = row_a[0]
                from_location = row_a[1]
                to_location = row_a[2]
                mode = row_a[3]
                phase_of_matter = row_a[4]
                route_cost = row_a[5]
                from_node = row_a[6]
                to_node = row_a[7]
                dollar_cost = row_a[8]
                miles = row_a[9]
                # max_daily_capacity = row_a[10]
                # artificial = row_a[11]
                mode_oid = row_a[12]
                commodity_id = row_a[13]
                origin_day = row_a[14]
                vertex_id = row_a[15]
                source_facility_id = row_a[16]
                leadin_edge_miles_travelled = row_a[17]
                new_edge_count = row_a[18]
                total_route_cost = row_a[19]
                leadin_edge_id = row_a[20]
                # leadin_nx_edge_id = row_a[21]
                to_vertex = row_a[22]
                max_commodity_travel_distance = row_a[23]

                # end_day = origin_day + fixed_route_duration
                new_miles_travelled = miles + leadin_edge_miles_travelled

                if mode in the_scenario.permittedModes and (mode, commodity_id) in commodity_mode_dict.keys() \
                        and commodity_mode_dict[mode, commodity_id] == 'Y':

                    if new_miles_travelled > max_commodity_travel_distance:
                        # designate leadin edge as endcap
                        children_created = 'E'
                        # update the incoming edge to indicate it's an endcap
                        db_cur.execute(
                            "update edges set children_created = '{}' where edge_id = {}".format(children_created,
                                                                                                 leadin_edge_id))
                        if from_location != 'NULL':
                            db_cur.execute("""insert or ignore into endcap_nodes(
                            node_id, location_id, mode_source, source_facility_id, commodity_id)
                            VALUES ({}, {}, '{}', {}, {});
                            """.format(from_node, from_location, mode, source_facility_id, commodity_id))
                        else:
                            db_cur.execute("""insert or ignore into endcap_nodes(
                            node_id, mode_source, source_facility_id, commodity_id)
                            VALUES ({}, '{}', {}, {});
                            """.format(from_node, mode, source_facility_id, commodity_id))

                        # designate leadin edge as endcap
                        # this does, deliberately, allow endcap status to be
                        # overwritten if we've found a shorter path to a previous endcap

                    # create new edge
                    elif new_miles_travelled <= max_commodity_travel_distance:

                        simple_mode = row_a[3].partition('_')[0]
                        tariff_id = 0
                        if simple_mode == 'pipeline':

                            # find tariff_ids

                            sql = """select mapping_id 
                            from pipeline_mapping 
                            where id = {} 
                            and id_field_name = 'source_OID' 
                            and source = '{}' 
                            and mapping_id is not null;""".format(mode_oid, mode)
                            for tariff_row in db_cur.execute(sql):
                                tariff_id = tariff_row[0]

                                # if there are no edges yet for this day, nx, subc combination,
                                # AND this is the shortest existing leadin option for this day, nx, subc combination
                                # we'd be creating an edge for (otherwise wait for the shortest option)
                                # at this step, some leadin edge should always exist

                        if origin_day in range(default_sched.first_day, default_sched.last_day + 1):
                            if origin_day + fixed_route_duration <= default_sched.last_day:
                                # if link is traversable in the timeframe
                                if simple_mode != 'pipeline' or tariff_id >= 0:


                                    if from_location == 'NULL' and to_location == 'NULL':
                                        # for each day and commodity,
                                        # get the corresponding origin vertex id to include with the edge info
                                        # origin vertex must not be "ultimate_destination
                                        # transport link outgoing from facility - checking fc.io is more thorough than
                                        # checking if facility type is 'ultimate destination'

                                        main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                            start_day, end_day, commodity_id,
                                            min_edge_capacity,edge_flow_cost, edge_flow_cost2,
                                            edge_type, nx_edge_id, mode, mode_oid, miles, simple_mode, 
                                            tariff_id,phase_of_matter,
                                            source_facility_id,
                                            miles_travelled, children_created, edge_count_from_source, total_route_cost) 
                                            VALUES ({}, {},
                                            {}, {}, {},
                                            {}, {}, {},
                                            '{}',{},'{}',{},
                                            {},'{}',{},'{}',
                                            {},
                                            {},'{}',{},{});
                                            """.format(from_node, to_node,
                                                       origin_day, origin_day + fixed_route_duration, commodity_id,
                                                       default_min_capacity, route_cost, dollar_cost,
                                                       'transport', nx_edge_id, mode, mode_oid,
                                                       miles, simple_mode, tariff_id, phase_of_matter,
                                                       source_facility_id,
                                                       new_miles_travelled, 'N', new_edge_count, total_route_cost))
                                    # only create edge going into a location if an appropriate vertex exists
                                    elif from_location == 'NULL' and to_location != 'NULL' and to_vertex > 0:
                                        edge_into_facility_counter = edge_into_facility_counter + 1

                                        main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                            start_day, end_day, commodity_id,
                                            d_vertex_id,
                                            min_edge_capacity, edge_flow_cost, edge_flow_cost2,
                                            edge_type, nx_edge_id, mode, mode_oid,
                                            miles, simple_mode, tariff_id, phase_of_matter,
                                            source_facility_id,
                                            miles_travelled, children_created, edge_count_from_source, total_route_cost)
                                            VALUES ({}, {},
                                            {}, {}, {},
                                            {},
                                            {}, {}, {},
                                            '{}',{},'{}', {},
                                            {},'{}',{},'{}',
                                            {},
                                            {},'{}',{},{});
                                            """.format(from_node, to_node,
                                                       origin_day, origin_day + fixed_route_duration, commodity_id,
                                                       to_vertex,
                                                       default_min_capacity, route_cost, dollar_cost,
                                                       'transport', nx_edge_id, mode, mode_oid,
                                                       miles, simple_mode, tariff_id, phase_of_matter,
                                                       source_facility_id,
                                                       new_miles_travelled, 'N', new_edge_count, total_route_cost))

                                    elif from_location != 'NULL' and to_location == 'NULL':
                                        # for each day and commodity, get the corresponding origin vertex id
                                        # to include with the edge info
                                        # origin vertex must not be "ultimate_destination
                                        # transport link outgoing from facility - checking fc.io is more thorough than
                                        # checking if facility type is 'ultimate destination'
                                        # new for bsc, only connect to vertices with matching source facility id
                                        # (only limited for RMP vertices)

                                        main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                            start_day, end_day, commodity_id,
                                            o_vertex_id,
                                            min_edge_capacity,edge_flow_cost, edge_flow_cost2,
                                            edge_type, nx_edge_id, mode, mode_oid, miles, simple_mode, tariff_id,
                                            phase_of_matter,
                                            source_facility_id,
                                            miles_travelled, children_created, edge_count_from_source, total_route_cost) 
                                            VALUES ({}, {},
                                            {}, {}, {},
                                            {},
                                            {}, {}, {},
                                            '{}',{},'{}',{},
                                            {},'{}',{},'{}',
                                            {},
                                            {},'{}',{},{});
                                            """.format(from_node, to_node,
                                                       origin_day, origin_day + fixed_route_duration, commodity_id,
                                                       vertex_id,
                                                       default_min_capacity, route_cost, dollar_cost,
                                                       'transport', nx_edge_id, mode, mode_oid,
                                                       miles, simple_mode, tariff_id, phase_of_matter,
                                                       source_facility_id,
                                                       new_miles_travelled, 'N', new_edge_count, total_route_cost))


                                    elif from_location != 'NULL' and to_location != 'NULL':
                                        # for each day and commodity, get the corresponding origin and
                                        # destination vertex ids to include with the edge info

                                        main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                                start_day, end_day, commodity_id,
                                                o_vertex_id, d_vertex_id,
                                                min_edge_capacity, edge_flow_cost, edge_flow_cost2,
                                                edge_type, nx_edge_id, mode, mode_oid, miles, simple_mode, tariff_id, 
                                                phase_of_matter,
                                                source_facility_id,
                                                miles_travelled, children_created, edge_count_from_source, 
                                                total_route_cost) 
                                                VALUES ({}, {},
                                                {}, {}, {},
                                                {}, {},
                                                {}, {}, {},
                                                '{}',{},'{}', {},
                                                {},'{}',{},'{}',
                                                {},
                                                {},'{}',{},{});
                                                """.format(from_node, to_node,
                                                           origin_day, origin_day + fixed_route_duration, commodity_id,
                                                           vertex_id, to_vertex,
                                                           default_min_capacity, route_cost, dollar_cost,
                                                           'transport', nx_edge_id, mode, mode_oid,
                                                           miles, simple_mode, tariff_id, phase_of_matter,
                                                           source_facility_id,
                                                           new_miles_travelled, 'N', new_edge_count, total_route_cost))

            for row_d in db_cur.execute("""select count(distinct e.edge_id), count(distinct e.nx_edge_id)
            from edges e where e.edge_type = 'transport'
            and children_created in ('N', 'Y', 'E');"""):
                source_based_edges_created = row_d[0]

            for row_d in db_cur.execute("""select count(distinct e.edge_id), count(distinct e.nx_edge_id)
            from edges e where e.edge_type = 'transport';"""):
                transport_edges_created = row_d[0]
                nx_edge_count = row_d[1]

            current_edge_data = db_cur.execute("""select count(distinct edge_id), children_created
                 from edges
                 where edge_type = 'transport'
                 group by children_created
                 order by children_created asc;""")
            current_edge_data = current_edge_data.fetchall()
            for row in current_edge_data:
                if row[1] == 'N':
                    edges_requiring_children = row[0]
                elif row[1] == 'Y':
                    edges_resolved = row[0]
                elif row[1] == 'E':
                    endcap_edges = row[0]
                    logger.debug('{} endcap edges designated for candidate generation step'.format(endcap_edges))
                if source_based_edges_created == edges_resolved + endcap_edges:
                    edges_requiring_children = 0
                if source_based_edges_created == edges_requiring_children + endcap_edges:
                    edges_resolved = 0
                if source_based_edges_created == edges_requiring_children + edges_resolved:
                    endcap_edges = 0

            if while_count % 1000 == 0 or edges_requiring_children == 0:
                logger.info(
                    '{} transport edges on {} nx edges,  created in {} loops, {} edges_requiring_children'.format(
                        transport_edges_created, nx_edge_count, while_count, edges_requiring_children))

        #  edges going in to the facility by re-running "generate first edges
        # then re-run this method

        logger.info('{} transport edges on {} nx edges,  created in {} loops, {} edges_requiring_children'.format(
            transport_edges_created, nx_edge_count, while_count, edges_requiring_children))
        logger.info("all source-based transport edges created")

        logger.info("create an index for the edges table by nodes")

        sql = ("""CREATE INDEX IF NOT EXISTS edge_index ON edges (
        edge_id, route_id, from_node_id, to_node_id, commodity_id,
        start_day, end_day, commodity_id, o_vertex_id, d_vertex_id,
        max_edge_capacity, min_edge_capacity, edge_flow_cost, edge_flow_cost2,
        edge_type, nx_edge_id, mode, mode_oid, miles, source_facility_id);""")
        db_cur.execute(sql)

    return


# ===============================================================================


def generate_all_edges_without_max_commodity_constraint(the_scenario, logger):
    # ===============================================================================
    # reassign default schedule to test user input schedules
    schedule_path = the_scenario.schedule
    schedule_dict = create_schedules_from_input(schedule_path, logger)
    default_sched = schedule_dict['default']  # should be same as previous default schedule
    # default_sched = schedule_dict['365b']  # only available on day 365
    # default_sched = schedule_dict['365c']  # available half-time, except full-time on days 10, 20, 100
    # default_sched = schedule_dict['2day']  # 2-day schedule with half availabilities on both days
    # default_sched = schedule_dict['harvest']  # 52-week schedule with 6 weeks of harvest availability
    # ===============================================================================
    global total_transport_routes
    logger.info("START: generate_all_edges_without_max_commodity_constraint")

    multi_commodity_name = "multicommodity"

    with sqlite3.connect(the_scenario.main_db) as main_db_con:

        db_cur = main_db_con.cursor()

        commodity_mode_data = main_db_con.execute("select * from commodity_mode;")
        commodity_mode_data = commodity_mode_data.fetchall()
        commodity_mode_dict = {}
        for row in commodity_mode_data:
            mode = row[0]
            commodity_id = int(row[1])
            commodity_phase = row[2]
            allowed_yn = row[3]
            commodity_mode_dict[mode, commodity_id] = allowed_yn

        counter = 0
        # for row in db_cur.execute(
        #         "select commodity_id from commodities where commodity_name = '{}';""".format(multi_commodity_name)):
        #     id_for_mult_commodities = row[0]
        for row in db_cur.execute("select count(*) from networkx_edges;"):
            total_transport_routes = row[0]

        # for all commodities with no max transport distance
        source_facility_id = 0

        # create transport edges, only between storage vertices and nodes, based on networkx graph
        # never touch primary vertices; either or both vertices can be null (or node-type) if it's a mid-route link
        # iterate through nx edges: if neither node has a location, create 1 edge per viable commodity
        # should also be per day, subject to nx edge schedule
        # before creating an edge, check: commodity allowed by nx and max transport distance if not null
        # will need nodes per day and commodity? or can I just check that with constraints?
        # select data for transport edges

        sql = """select
        ne.edge_id,
        ifnull(fn.location_id, 'NULL'),
        ifnull(tn.location_id, 'NULL'),
        ne.mode_source,
        ifnull(nec.phase_of_matter_id, 'NULL'),
        nec.route_cost,
        ne.from_node_id,
        ne.to_node_id,
        nec.dollar_cost,
        ne.miles,
        ne.capacity,
        ne.artificial,
        ne.mode_source_oid
        from networkx_edges ne, networkx_nodes fn, networkx_nodes tn, networkx_edge_costs nec

        where ne.from_node_id = fn.node_id
        and ne.to_node_id = tn.node_id
        and ne.edge_id = nec.edge_id
        and ifnull(ne.capacity, 1) > 0
        ;"""
        nx_edge_data = main_db_con.execute(sql)
        nx_edge_data = nx_edge_data.fetchall()
        for row_a in nx_edge_data:

            nx_edge_id = row_a[0]
            from_location = row_a[1]
            to_location = row_a[2]
            mode = row_a[3]
            phase_of_matter = row_a[4]
            route_cost = row_a[5]
            from_node = row_a[6]
            to_node = row_a[7]
            dollar_cost = row_a[8]
            miles = row_a[9]
            # max_daily_capacity = row_a[10]
            mode_oid = row_a[12]
            simple_mode = row_a[3].partition('_')[0]

            # if counter % 10000 == 1:
            #     logger.info("edges created for {} transport links of {}".format(counter, total_transport_routes))
            #     for row_d in db_cur.execute("select count(distinct edge_id) from edges;"):
            #         logger.info('{} edges created'.format(row_d[0]))
            counter = counter + 1

            tariff_id = 0
            if simple_mode == 'pipeline':

                # find tariff_ids

                sql = "select mapping_id from pipeline_mapping " \
                      "where id = {} and id_field_name = 'source_OID' and source = '{}' " \
                      "and mapping_id is not null;".format(
                    mode_oid, mode)
                for tariff_row in db_cur.execute(sql):
                    tariff_id = tariff_row[0]


            if mode in the_scenario.permittedModes:

                # Edges are placeholders for flow variables
                # for all days (restrict by link schedule if called for)
                # for all allowed commodities, as currently defined by link phase of matter

                for day in range(default_sched.first_day, default_sched.last_day + 1):
                    if day + fixed_route_duration <= default_sched.last_day:
                        # if link is traversable in the timeframe
                        if simple_mode != 'pipeline' or tariff_id >= 0:
                            # for allowed commodities that can be output by some facility in the scenario
                            for row_c in db_cur.execute("""select commodity_id
                            from source_commodity_ref s
                            where phase_of_matter = '{}'
                            and max_transport_distance_flag = 'N'
                            and share_max_transport_distance = 'N'
                            group by commodity_id, source_facility_id""".format(phase_of_matter)):
                                db_cur4 = main_db_con.cursor()
                                commodity_id = row_c[0]
                                # source_facility_id = row_c[1] # fixed to 0 for all edges created by this method
                                if (mode, commodity_id) in commodity_mode_dict.keys() \
                                        and commodity_mode_dict[mode, commodity_id] == 'Y':
                                    if from_location == 'NULL' and to_location == 'NULL':
                                        main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                            start_day, end_day, commodity_id,
                                            min_edge_capacity, edge_flow_cost, edge_flow_cost2,
                                            edge_type, nx_edge_id, mode, mode_oid, miles, simple_mode, tariff_id, 
                                            phase_of_matter, source_facility_id) VALUES ({}, {},
                                            {}, {}, {},
                                            {}, {}, {},
                                            '{}',{},'{}',{},{},'{}',{},'{}',{});
                                            """.format(from_node, to_node,
                                                       day, day + fixed_route_duration, commodity_id,
                                                       default_min_capacity, route_cost, dollar_cost,
                                                       'transport', nx_edge_id, mode, mode_oid, miles, simple_mode,
                                                       tariff_id, phase_of_matter, source_facility_id))

                                    elif from_location != 'NULL' and to_location == 'NULL':
                                        # for each day and commodity, get the corresponding origin vertex id
                                        # to include with the edge info
                                        # origin vertex must not be "ultimate_destination
                                        # transport link outgoing from facility - checking fc.io is more thorough
                                        # than checking if facility type is 'ultimate destination'
                                        # new for bsc, only connect to vertices with matching source_facility_id
                                        for row_d in db_cur4.execute("""select vertex_id
                                        from vertices v, facility_commodities fc
                                        where v.location_id = {} and v.schedule_day = {} 
                                        and v.commodity_id = {} and v.source_facility_id = {}
                                        and v.storage_vertex = 1
                                        and v.facility_id = fc.facility_id
                                        and v.commodity_id = fc.commodity_id
                                        and fc.io = 'o'""".format(from_location, day, commodity_id, source_facility_id)):
                                            from_vertex_id = row_d[0]
                                            main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                                start_day, end_day, commodity_id,
                                                o_vertex_id,
                                                min_edge_capacity,edge_flow_cost, edge_flow_cost2,
                                                edge_type, nx_edge_id, mode, mode_oid, miles, simple_mode, tariff_id,
                                                phase_of_matter, source_facility_id) VALUES ({}, {},
                                                {}, {}, {},
                                                {},
                                                {}, {}, {},
                                                '{}',{},'{}',{},{},'{}',{},'{}',{});
                                                """.format(from_node, to_node,
                                                           day, day + fixed_route_duration, commodity_id,
                                                           from_vertex_id,
                                                           default_min_capacity, route_cost, dollar_cost,
                                                           'transport', nx_edge_id, mode, mode_oid, miles, simple_mode,
                                                           tariff_id, phase_of_matter, source_facility_id))
                                    elif from_location == 'NULL' and to_location != 'NULL':
                                        # for each day and commodity, get the corresponding destination vertex id
                                        # to include with the edge info
                                        for row_d in db_cur4.execute("""select vertex_id
                                        from vertices v, facility_commodities fc
                                        where v.location_id = {} and v.schedule_day = {} 
                                        and v.commodity_id = {} and v.source_facility_id = {}
                                        and v.storage_vertex = 1
                                        and v.facility_id = fc.facility_id
                                        and v.commodity_id = fc.commodity_id
                                        and fc.io = 'i'""".format(to_location, day, commodity_id, source_facility_id)):
                                            to_vertex_id = row_d[0]
                                            main_db_con.execute("""insert or ignore into edges (from_node_id, to_node_id,
                                                start_day, end_day, commodity_id,
                                                d_vertex_id,
                                                min_edge_capacity, edge_flow_cost, edge_flow_cost2,
                                                edge_type, nx_edge_id, mode, mode_oid, miles, simple_mode, tariff_id, 
                                                phase_of_matter, source_facility_id) VALUES ({}, {},
                                                {}, {}, {},
                                                {},
                                                {}, {}, {},
                                                '{}',{},'{}',{},{},'{}',{},'{}',{});
                                                """.format(from_node, to_node,
                                                           day, day + fixed_route_duration, commodity_id,
                                                           to_vertex_id,
                                                           default_min_capacity, route_cost, dollar_cost,
                                                           'transport', nx_edge_id, mode, mode_oid, miles, simple_mode,
                                                           tariff_id, phase_of_matter, source_facility_id))
                                    elif from_location != 'NULL' and to_location != 'NULL':
                                        # for each day and commodity, get the corresponding origin and destination vertex
                                        # ids to include with the edge info
                                        for row_d in db_cur4.execute("""select vertex_id
                                        from vertices v, facility_commodities fc
                                        where v.location_id = {} and v.schedule_day = {} 
                                        nd v.commodity_id = {} and v.source_facility_id = {}
                                        and v.storage_vertex = 1
                                        and v.facility_id = fc.facility_id
                                        and v.commodity_id = fc.commodity_id
                                        and fc.io = 'o'""".format(from_location, day, commodity_id, source_facility_id)):
                                            from_vertex_id = row_d[0]
                                            db_cur5 = main_db_con.cursor()
                                            for row_e in db_cur5.execute("""select vertex_id
                                        from vertices v, facility_commodities fc
                                        where v.location_id = {} and v.schedule_day = {} 
                                        and v.commodity_id = {} and v.source_facility_id = {}
                                        and v.storage_vertex = 1
                                        and v.facility_id = fc.facility_id
                                        and v.commodity_id = fc.commodity_id
                                        and fc.io = 'i'""".format(to_location, day, commodity_id, source_facility_id)):
                                                to_vertex_id = row_e[0]
                                                main_db_con.execute("""insert or ignore into edges (from_node_id, 
                                                to_node_id, start_day, end_day, commodity_id, o_vertex_id, d_vertex_id, 
                                                min_edge_capacity, edge_flow_cost, edge_flow_cost2, edge_type, 
                                                nx_edge_id, mode, mode_oid, miles, simple_mode, tariff_id, 
                                                phase_of_matter, source_facility_id) VALUES ({}, {}, {}, {}, {}, {}, {}, 
                                                {}, {}, {}, '{}',{},'{}', {},{},'{}',{},'{}',{}
                                                )""".format(from_node,
                                                            to_node, day,
                                                            day + fixed_route_duration,
                                                            commodity_id,
                                                            from_vertex_id,
                                                            to_vertex_id,
                                                            default_min_capacity,
                                                            route_cost,
                                                            dollar_cost,
                                                            'transport',
                                                            nx_edge_id, mode,
                                                            mode_oid, miles,
                                                            simple_mode,
                                                            tariff_id,
                                                            phase_of_matter,
                                                            source_facility_id))

        logger.debug("all transport edges created")

        logger.info("all edges created")
        logger.info("create an index for the edges table by nodes")
        index_start_time = datetime.datetime.now()
        sql = ("""CREATE INDEX IF NOT EXISTS edge_index ON edges (
        edge_id, route_id, from_node_id, to_node_id, commodity_id,
        start_day, end_day, commodity_id, o_vertex_id, d_vertex_id,
        max_edge_capacity, min_edge_capacity, edge_flow_cost, edge_flow_cost2,
        edge_type, nx_edge_id, mode, mode_oid, miles, source_facility_id);""")
        db_cur.execute(sql)
        logger.info("edge_index Total Runtime (HMS): \t{} \t ".format(get_total_runtime_string(index_start_time)))
    return


# ===============================================================================


def set_edges_volume_capacity(the_scenario, logger):
    logger.info("starting set_edges_volume_capacity")
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        logger.debug("starting to record volume and capacity for non-pipeline edges")

        main_db_con.execute(
            "update edges set volume = (select ifnull(ne.volume,0) from networkx_edges ne "
            "where ne.edge_id = edges.nx_edge_id ) where simple_mode in ('rail','road','water');")
        main_db_con.execute(
            "update edges set max_edge_capacity = (select ne.capacity from networkx_edges ne "
            "where ne.edge_id = edges.nx_edge_id) where simple_mode in ('rail','road','water');")
        logger.debug("volume and capacity recorded for non-pipeline edges")

        logger.debug("starting to record volume and capacity for pipeline edges")
        ##
        main_db_con.executescript("""update edges set volume =
        (select l.background_flow
         from pipeline_mapping pm,
         (select id_field_name, cn.source_OID as link_id, min(cn.capacity) capac,
        max(cn.volume) background_flow, source
        from capacity_nodes cn
        where cn.id_field_name = 'MASTER_OID'
        and ifnull(cn.capacity,0)>0
        group by link_id) l

        where edges.tariff_id = pm.id
        and pm.id_field_name = 'tariff_ID'
        and pm.mapping_id_field_name = 'MASTER_OID'
        and l.id_field_name = 'MASTER_OID'
        and pm.mapping_id = l.link_id
        and instr(edges.mode, l.source)>0)
         where simple_mode = 'pipeline'
        ;

        update edges set max_edge_capacity =
        (select l.capac
        from pipeline_mapping pm,
        (select id_field_name, cn.source_OID as link_id, min(cn.capacity) capac,
        max(cn.volume) background_flow, source
        from capacity_nodes cn
        where cn.id_field_name = 'MASTER_OID'
        and ifnull(cn.capacity,0)>0
        group by link_id) l

        where edges.tariff_id = pm.id
        and pm.id_field_name = 'tariff_ID'
        and pm.mapping_id_field_name = 'MASTER_OID'
        and l.id_field_name = 'MASTER_OID'
        and pm.mapping_id = l.link_id
        and instr(edges.mode, l.source)>0)
        where simple_mode = 'pipeline'
        ;""")
        logger.debug("volume and capacity recorded for pipeline edges")
        logger.debug("starting to record units and conversion multiplier")
        main_db_con.execute("""update edges
        set capacity_units =
        (case when simple_mode = 'pipeline' then 'kbarrels'
        when simple_mode = 'road' then 'truckload'
        when simple_mode = 'rail' then 'railcar'
        when simple_mode = 'water' then 'barge'
        else 'unexpected mode' end)
        ;""")
        main_db_con.execute("""update edges
        set units_conversion_multiplier =
        (case when simple_mode = 'pipeline' and phase_of_matter = 'liquid' then {}
        when simple_mode = 'road'  and phase_of_matter = 'liquid' then {}
        when simple_mode = 'road'  and phase_of_matter = 'solid' then {}
        when simple_mode = 'rail'  and phase_of_matter = 'liquid' then {}
        when simple_mode = 'rail'  and phase_of_matter = 'solid' then {}
        when simple_mode = 'water'  and phase_of_matter = 'liquid' then {}
        when simple_mode = 'water'  and phase_of_matter = 'solid' then {}
        else 1 end)
        ;""".format(THOUSAND_GALLONS_PER_THOUSAND_BARRELS,
                    the_scenario.truck_load_liquid.magnitude,
                    the_scenario.truck_load_solid.magnitude,
                    the_scenario.railcar_load_liquid.magnitude,
                    the_scenario.railcar_load_solid.magnitude,
                    the_scenario.barge_load_liquid.magnitude,
                    the_scenario.barge_load_solid.magnitude,
                    ))
        logger.debug("units and conversion multiplier recorded for all edges; starting capacity minus volume")
        main_db_con.execute("""update edges
        set capac_minus_volume_zero_floor =
        max((select (max_edge_capacity - ifnull(volume,0)) where  max_edge_capacity is not null),0)
        where  max_edge_capacity is not null
        ;""")
        logger.debug("capacity minus volume (minimum set to zero) recorded for all edges")
    return


# ===============================================================================


def pre_setup_pulp(logger, the_scenario):
    logger.info("START: pre_setup_pulp")

    commodity_mode_setup(the_scenario, logger)

    # create table to track source facility of commodities with a max transport distance set
    source_tracking_setup(the_scenario, logger)

    generate_all_vertices(the_scenario, logger)

    add_storage_routes(the_scenario, logger)
    generate_connector_and_storage_edges(the_scenario, logger)
    generate_first_edges_from_source_facilities(the_scenario, logger)

    # replicate all_routes by commodity and time into all_edges dictionary
    generate_all_edges_from_source_facilities(the_scenario, logger)  # commented out 4-16

    # start edges for commodities that inherit max transport distance
    generate_first_edges_from_source_facilities(the_scenario, logger)

    # replicate all_routes by commodity and time into all_edges dictionary
    generate_all_edges_from_source_facilities(the_scenario, logger)

    # replicate all_routes by commodity and time into all_edges dictionary
    generate_all_edges_without_max_commodity_constraint(the_scenario, logger)  # commented out 4-16
    logger.info("Edges generated for modes: {}".format(the_scenario.permittedModes))

    set_edges_volume_capacity(the_scenario, logger)

    return


# ===============================================================================


def create_flow_vars(the_scenario, logger):
    logger.info("START: create_flow_vars")

    # we have a table called edges.
    # call helper method to get list of unique IDs from the Edges table.
    # use the rowid as a simple unique integer index
    edge_list = []

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        edge_list_cur = db_cur.execute("""select edge_id--, commodity_id, start_day, source_facility_id
        from edges;""")
        edge_list_data = edge_list_cur.fetchall()
        counter = 0
        for row in edge_list_data:
            if counter % 500000 == 0:
                logger.info(
                    "processed {:,.0f} records. size of edge_list {:,.0f}".format(counter, sys.getsizeof(edge_list)))
            counter += 1
            # create an edge for each commodity allowed on this link - this construction may change
            # as specific commodity restrictions are added
            # TODO4-18 add days, but have no scheduel for links currently
            # running just with nodes for now, will add proper facility info and storage back soon
            edge_list.append((row[0]))

    logger.debug("MNP DEBUG: start assign flow_var with edge_list")

    flow_var = LpVariable.dicts("Edge", edge_list, 0, None)
    logger.debug("MNP DEBUG: Size of flow_var: {:,.0f}".format(sys.getsizeof(flow_var)))
    return flow_var


# ===============================================================================


def create_unmet_demand_vars(the_scenario, logger):
    logger.info("START: create_unmet_demand_vars")
    demand_var_list = []
    # may create vertices with zero demand, but only for commodities that the facility has demand for at some point

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        for row in db_cur.execute("""select v.facility_id, v.schedule_day, 
        ifnull(c.supertype, c.commodity_name) top_level_commodity_name, v.udp
         from vertices v, commodities c, facility_type_id ft, facilities f
         where v.commodity_id = c.commodity_id
         and ft.facility_type = "ultimate_destination"
         and v.storage_vertex = 0
         and v.facility_type_id = ft.facility_type_id
         and v.facility_id = f.facility_id
         and f.ignore_facility = 'false'
         group by v.facility_id, v.schedule_day, ifnull(c.supertype, c.commodity_name)
         ;""".format('')):
            # facility_id, day, and simplified commodity name
            demand_var_list.append((row[0], row[1], row[2], row[3]))

    unmet_demand_var = LpVariable.dicts("UnmetDemand", demand_var_list, None, None)

    return unmet_demand_var


# ===============================================================================


def create_candidate_processor_build_vars(the_scenario, logger):
    logger.info("START: create_candidate_processor_build_vars")
    processors_build_list = []

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        for row in db_cur.execute(
                """select f.facility_id from facilities f, facility_type_id ft
                where f.facility_type_id = ft.facility_type_id and facility_type = 'processor'
                and candidate = 1 and ignore_facility = 'false' group by facility_id;"""):
            # grab all candidate processor facility IDs
            processors_build_list.append(row[0])

    processor_build_var = LpVariable.dicts("BuildProcessor", processors_build_list, 0, None, 'Binary')

    return processor_build_var


# ===============================================================================


def create_binary_processor_vertex_flow_vars(the_scenario, logger):
    logger.info("START: create_binary_processor_vertex_flow_vars")
    processors_flow_var_list = []

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        for row in db_cur.execute("""select v.facility_id, v.schedule_day
        from vertices v, facility_type_id ft
        where v.facility_type_id = ft.facility_type_id
        and facility_type = 'processor'
        and storage_vertex = 0
        group by v.facility_id, v.schedule_day;"""):
            # facility_id, day
            processors_flow_var_list.append((row[0], row[1]))

    processor_flow_var = LpVariable.dicts("ProcessorDailyFlow", processors_flow_var_list, 0, None, 'Binary')

    return processor_flow_var


# ===============================================================================


def create_processor_excess_output_vars(the_scenario, logger):
    logger.info("START: create_processor_excess_output_vars")

    excess_var_list = []
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        xs_cur = db_cur.execute("""
        select vertex_id, commodity_id
        from vertices v, facility_type_id ft
        where v.facility_type_id = ft.facility_type_id
        and facility_type = 'processor'
        and storage_vertex = 1;""")
        # facility_id, day, and simplified commodity name
        xs_data = xs_cur.fetchall()
        for row in xs_data:
            excess_var_list.append(row[0])

    excess_var = LpVariable.dicts("XS", excess_var_list, 0, None)

    return excess_var


# ===============================================================================

def create_opt_problem(logger, the_scenario, unmet_demand_vars, flow_vars, processor_build_vars):
    i = np.load("scenario_num.npy")
    t = np.load("time_horizon.npy")
    j = np.load("earthquake_day.npy")
    logger.info("scenario {}".format(i))
    logger.info("time horizon {}".format(t))
    logger.info("earthquake day {}".format(j))
    
    logger.debug("START: create_opt_problem")
    prob = LpProblem("Flow assignment", LpMinimize)

    logger.debug("MNP: DEBUG: length of unmet_demand_vars: {}".format(len(unmet_demand_vars)))
    logger.debug("MNP: DEBUG: length of flow_vars: {}".format(len(flow_vars)))
    logger.debug("MNP: DEBUG: length of processor_build_vars: {}".format(len(processor_build_vars)))

    unmet_demand_costs = []
    flow_costs = {}
    processor_build_costs = []
    operation_cost = []
    non_operation_cost = []
    
    logger.debug("MNP: DEBUG: start loop through sql to append unmet_demand_costs")
    for u in unmet_demand_vars:
        # facility_id = u[0]
        # schedule_day = u[1]
        # demand_commodity_name = u[2]
        udp = u[3]
        unmet_demand_costs.append(udp * unmet_demand_vars[u])
        non_operation_cost.append(operation_parameter * unmet_demand_vars[u])
    logger.debug("MNP: DEBUG: finished loop through sql to append unmet_demand_costs. total records: {}".format(
        len(unmet_demand_costs)))

    #===============================================Modification==========================================================
    # Operation cost calculation
    
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        sql = """select fc.commodity_id, f.facility_id,
        ifnull(f.candidate, 0), fc.quantity, v.schedule_day, v.activity_level
        from facility_commodities fc, facility_type_id ft, facilities f, vertices v
        where ft.facility_type = 'processor'
        and ft.facility_type_id = f.facility_type_id
        and f.facility_id = fc.facility_id
        and fc.io = 'i'
        and v.facility_id = f.facility_id
        and v.storage_vertex = 0
        group by fc.commodity_id, f.facility_id,
        ifnull(f.candidate, 0), fc.quantity, v.schedule_day, v.activity_level
        ;
        """
        # iterate through processor facilities, one constraint per facility per day
        # different copies by subcommodity, summed for constraint as long as same day

        processor_facilities = db_cur.execute(sql)

        processor_facilities = processor_facilities.fetchall()

        for row_a in processor_facilities:

            # input_commodity_id = row_a[0]
            facility_id = row_a[1]
            is_candidate = row_a[2]
            #=====================================================================================
            
            quantity = facility_cap_noEarthquake[float(facility_id)-2][t+1][i]*row_a[3]
            
            #=====================================================================================
            day = row_a[4]
            activity_level = row_a[5]

            operation_cost.append(operation_parameter * quantity * activity_level)

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        logger.debug("MNP: DEBUG: start sql execute to get flow cost data")
        # Flow cost memory improvements: only get needed data; dict instead of list; narrow in lpsum
        flow_cost_var = db_cur.execute("select edge_id, edge_flow_cost from edges e group by edge_id;")
        logger.debug("MNP DEBUG: start the fetchall")
        flow_cost_data = flow_cost_var.fetchall()
        logger.debug("MNP DEBUG: start iterating through {:,.0f} flow_cost_data records".format(len(flow_cost_data)))
        counter = 0
        for row in flow_cost_data:
            edge_id = row[0]
            edge_flow_cost = row[1]
            counter += 1

            # flow costs cover transportation and storage
            flow_costs[edge_id] = edge_flow_cost
            # flow_costs.append(edge_flow_cost * flow_vars[(edge_id)])
        logger.debug("MNP: DEBUG: finished loop through sql to append flow costs: total records: {:,.0f}".format(
            len(flow_costs)))

        logger.info("check if candidate tables exist")
        sql = "SELECT name FROM sqlite_master WHERE type='table' " \
              "AND name in ('candidate_processors', 'candidate_process_list');"
        count = len(db_cur.execute(sql).fetchall())

        if count == 2:

            logger.debug("MNP: DEBUG: start execute sql for processor build costs")
            processor_build_cost = db_cur.execute("""
            select f.facility_id, (p.cost_formula*c.quantity) build_cost
            from facilities f, facility_type_id ft, candidate_processors c, candidate_process_list p
            where f.facility_type_id = ft.facility_type_id
            and facility_type = 'processor'
            and candidate = 1
            and ignore_facility = 'false'
            and f.facility_name = c.facility_name
            and c.process_id = p.process_id
            group by f.facility_id, build_cost;""")
            logger.debug("MNP: DEBUG: start the fetchall ")
            processor_build_cost_data = processor_build_cost.fetchall()
            logger.debug("MNP DEBUG: start iterating through the {} processor_build_cost records".format(
                len(processor_build_cost_data)))
            for row in processor_build_cost_data:
                candidate_proc_facility_id = row[0]
                proc_facility_build_cost = row[1]
                processor_build_costs.append(
                    proc_facility_build_cost * processor_build_vars[candidate_proc_facility_id])
            logger.debug("MNP: DEBUG: start loop through sql to append processor build costs. Total Records: {}".format(
                len(processor_build_costs)))
    # Resilience cost = -w1*R1-w2*R2-w3*R3
    # Weight factors for each resilience components, values based on decision-makers, 
    w1 = 1
    w2 = 1
    w3 = 1
    weight = 1
    
    logger.debug("MNP: debug: start prob+= initial_cost + weight*(unmet_demand_costs + flow cost + processor_build_costs + operation_cost-initial cost)")
    prob +=(initial_cost_daily + weight*w2*(lpSum(unmet_demand_costs) + lpSum(flow_costs[k] * flow_vars[k] for k in flow_costs) + lpSum(
        processor_build_costs)+ lpSum(operation_cost) - lpSum(non_operation_cost) - initial_cost_daily)), "Total Cost of Transport, storage, facility building, operation and penalties"
    logger.debug("MNP debug: done prob+= initial_cost + weight*(unmet_demand_costs + flow cost + processor_build_costs + operation_cost-initial cost)")

    logger.debug("FINISHED: create_opt_problem")
    return prob
 #=========================================End===============================================================

# ===============================================================================


def create_constraint_unmet_demand(logger, the_scenario, prob, flow_var, unmet_demand_var):
    logger.debug("START: create_constraint_unmet_demand")

    # apply activity_level to get corresponding actual demand for var

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        # var has form(facility_name, day, simple_fuel)
        # unmet demand commodity should be simple_fuel = supertype
        logger.debug("MNP: DEBUG: length of unmet_demand_vars: {}".format(len(unmet_demand_var)))

        demand_met_dict = defaultdict(list)
        actual_demand_dict = {}

        # demand_met = []
        # want to specify that all edges leading into this vertex + unmet demand = total demand
        # demand primary (non-storage)  vertices

        db_cur = main_db_con.cursor()
        # each row_a is a primary vertex whose edges in contributes to the met demand of var
        # will have one row for each fuel subtype in the scenario
        unmet_data = db_cur.execute("""select v.vertex_id, v.commodity_id,
        v.demand, ifnull(c.proportion_of_supertype, 1), ifnull(v.activity_level, 1), v.source_facility_id,
        v.facility_id, v.schedule_day, ifnull(c.supertype, c.commodity_name), v.udp, e.edge_id
        from vertices v, commodities c, facility_type_id ft, facilities f, edges e
        where v.facility_id = f.facility_id
        and ft.facility_type = 'ultimate_destination'
        and f.facility_type_id = ft.facility_type_id
        and f.ignore_facility = 'false'
        and v.facility_type_id = ft.facility_type_id
        and v.storage_vertex = 0
        and c.commodity_id = v.commodity_id
        and e.d_vertex_id = v.vertex_id
        group by v.vertex_id, v.commodity_id,
        v.demand, ifnull(c.proportion_of_supertype, 1), ifnull(v.activity_level, 1), v.source_facility_id,
        v.facility_id, v.schedule_day, ifnull(c.supertype, c.commodity_name), v.udp, e.edge_id
        ;""")

        unmet_data = unmet_data.fetchall()
        for row_a in unmet_data:
            # primary_vertex_id = row_a[0]
            # commodity_id = row_a[1]
            var_full_demand = row_a[2]
            proportion_of_supertype = row_a[3]
            var_activity_level = row_a[4]
            # source_facility_id = row_a[5]
            facility_id = row_a[6]
            day = row_a[7]
            top_level_commodity = row_a[8]
            udp = row_a[9]
            edge_id = row_a[10]
            var_actual_demand = var_full_demand * var_activity_level

            # next get inbound edges, apply appropriate modifier proportion to get how much of var's demand they satisfy
            demand_met_dict[(facility_id, day, top_level_commodity, udp)].append(
                flow_var[edge_id] * proportion_of_supertype)
            actual_demand_dict[(facility_id, day, top_level_commodity, udp)] = var_actual_demand

        for key in unmet_demand_var:
            if key in demand_met_dict:
                # then there are some edges in
                prob += lpSum(demand_met_dict[key]) == actual_demand_dict[key] - unmet_demand_var[
                    key], "constraint set unmet demand variable for facility {}, day {}, commodity {}".format(key[0],
                                                                                                              key[1],
                                                                                                              key[2])
            else:
                if key not in actual_demand_dict:
                    pdb.set_trace()
                # no edges in, so unmet demand equals full demand
                prob += actual_demand_dict[key] == unmet_demand_var[
                    key], "constraint set unmet demand variable for facility {}, day {}, " \
                          "commodity {} - no edges able to meet demand".format(
                    key[0], key[1], key[2])

    logger.debug("FINISHED: create_constraint_unmet_demand and return the prob ")
    return prob


# ===============================================================================


def create_constraint_max_flow_out_of_supply_vertex(logger, the_scenario, prob, flow_var):
    logger.debug("STARTING:  create_constraint_max_flow_out_of_supply_vertex")
    logger.debug("Length of flow_var: {}".format(len(flow_var.items())))

    # create_constraint_max_flow_out_of_supply_vertex
    # primary vertices only
    # flow out of a vertex <= supply of the vertex, true for every day and commodity

    # for each primary (non-storage) supply vertex
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        for row_a in db_cur.execute("""select vertex_id, activity_level, supply
        from vertices v, facility_type_id ft
        where v.facility_type_id = ft.facility_type_id
        and ft.facility_type = 'raw_material_producer'
        and storage_vertex = 0;"""):
            supply_vertex_id = row_a[0]
            activity_level = row_a[1]
            max_daily_supply = row_a[2]
            actual_vertex_supply = activity_level * max_daily_supply

            flow_out = []
            db_cur2 = main_db_con.cursor()
            # select all edges leaving that vertex and sum their flows
            # should be a single connector edge
            for row_b in db_cur2.execute("select edge_id from edges where o_vertex_id = {};".format(supply_vertex_id)):
                edge_id = row_b[0]
                flow_out.append(flow_var[edge_id])

            prob += lpSum(flow_out) <= actual_vertex_supply, "constraint max flow of {} out of origin vertex {}".format(
                actual_vertex_supply, supply_vertex_id)
        # could easily add human-readable vertex info to this if desirable

    logger.debug("FINISHED:  create_constraint_max_flow_out_of_supply_vertex")
    return prob


# ===============================================================================


def create_constraint_daily_processor_capacity(logger, the_scenario, prob, flow_var, processor_build_vars,
                                               processor_daily_flow_vars):
    logger.debug("STARTING:  create_constraint_daily_processor_capacity")
    # primary vertices only
    # flow out of a vertex <= nameplate capcity / 365, true for every day and totaled output commodities

    ### get primary processor vertex and its output quantity
    total_scenario_min_capacity = 0
    
    i = np.load("scenario_num.npy")
    t = np.load("time_horizon.npy")
    j = np.load("earthquake_day.npy")
    logger.info("scenario {}".format(i))
    logger.info("time horizon {}".format(t))
    logger.info("earthquake day {}".format(j))
    
    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        sql = """select fc.commodity_id, f.facility_id,
        ifnull(f.candidate, 0), fc.quantity, v.schedule_day, v.activity_level
        from facility_commodities fc, facility_type_id ft, facilities f, vertices v
        where ft.facility_type = 'processor'
        and ft.facility_type_id = f.facility_type_id
        and f.facility_id = fc.facility_id
        and fc.io = 'i'
        and v.facility_id = f.facility_id
        and v.storage_vertex = 0
        group by fc.commodity_id, f.facility_id,
        ifnull(f.candidate, 0), fc.quantity, v.schedule_day, v.activity_level
        ;
        """
        # iterate through processor facilities, one constraint per facility per day
        # different copies by subcommodity, summed for constraint as long as same day

        processor_facilities = db_cur.execute(sql)

        processor_facilities = processor_facilities.fetchall()

        for row_a in processor_facilities:

            # input_commodity_id = row_a[0]
            facility_id = row_a[1]
            is_candidate = row_a[2]
            #========================================Modification==========================================================================
            # In order to incorporate time-varying facility capacity into FTOT
            
            if j < repair_time_facility[float(facility_id)-2][t+1][i]:
                max_capacity = facility_cap[float(facility_id)-2][t+1][i]*row_a[3]
            else:
                max_capacity = facility_cap_noEarthquake[float(facility_id)-2][t+1][i]*row_a[3]
            
            #==================================================End========================================================================
            
            max_capacity = row_a[3]
            day = row_a[4]
            daily_activity_level = row_a[5]

            daily_inflow_max_capacity = float(max_capacity) * float(daily_activity_level)
            daily_inflow_min_capacity = daily_inflow_max_capacity / 2
            logger.debug(
                "processor {}, day {},  capacity min: {} max: {}".format(facility_id, day, daily_inflow_min_capacity,
                                                                         daily_inflow_max_capacity))
            total_scenario_min_capacity = total_scenario_min_capacity + daily_inflow_min_capacity
            flow_in = []

            # all edges that start in that processor facility, any primary vertex, on that day - so all subcommodities
            db_cur2 = main_db_con.cursor()
            for row_b in db_cur2.execute("""select edge_id from edges e, vertices v
            where e.start_day = {}
            and e.d_vertex_id = v.vertex_id
            and v.facility_id = {}
            and v.storage_vertex = 0
            group by edge_id""".format(day, facility_id)):
                input_edge_id = row_b[0]
                flow_in.append(flow_var[input_edge_id])

            logger.debug(
                "flow in for capacity constraint on processor facility {} day {}: {}".format(facility_id, day, flow_in))
            prob += lpSum(flow_in) <= daily_inflow_max_capacity * processor_daily_flow_vars[(facility_id, day)], \
                    "constraint max flow out of processor facility {}, day {}, flow var {}".format(
                        facility_id, day, processor_daily_flow_vars[facility_id, day])

            if is_candidate == 1:
                # forces processor build var to be correct
                # if there is flow through a candidate processor then it has to be built
                prob += processor_build_vars[facility_id] >= processor_daily_flow_vars[
                    (facility_id, day)], "constraint forces processor build var to be correct {}, {}".format(
                    facility_id, processor_build_vars[facility_id])

            prob += lpSum(flow_in) >= daily_inflow_min_capacity * processor_daily_flow_vars[
                (facility_id, day)], "constraint min flow into processor {}, day {}".format(facility_id, day)

    logger.debug("FINISHED:  create_constraint_daily_processor_capacity")
    return prob


# ===============================================================================


def create_primary_processor_vertex_constraints(logger, the_scenario, prob, flow_var):
    i = np.load("scenario_num.npy")
    t = np.load("time_horizon.npy")
    j = np.load("earthquake_day.npy")
    logger.info("scenario {}".format(i))
    logger.info("time horizon {}".format(t))
    logger.info("earthquake day {}".format(j))
    logger.debug("STARTING:  create_primary_processor_vertex_constraints - conservation of flow")
    # for all of these vertices, flow in always  == flow out
    # node_counter = 0
    # node_constraint_counter = 0

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()

        # total flow in == total flow out, subject to conversion;
        # dividing by "required quantity" functionally converts all commodities to the same "processor-specific units"

        # processor primary vertices with input commodity and  quantity needed to produce specified output quantities
        # 2 sets of contraints; one for the primary processor vertex to cover total flow in and out
        # one for each input and output commodity (sum over sources) to ensure its ratio matches facility_commodities

        # the current construction of this method is dependent on having only one input commodity type per processor
        # this limitation makes sharing max transport distance from the input to an output commodity feasible

        logger.debug("conservation of flow and commodity ratios, primary processor vertices:")
        sql = """select v.vertex_id,
        (case when e.o_vertex_id = v.vertex_id then 'out' 
        when e.d_vertex_id = v.vertex_id then 'in' else 'error' end) in_or_out_edge,
        (case when e.o_vertex_id = v.vertex_id then start_day 
        when e.d_vertex_id = v.vertex_id then end_day else 0 end) constraint_day,
        e.commodity_id,
        e.mode,
        e.edge_id,
        nx_edge_id, fc.quantity, v.facility_id, c.commodity_name,
        fc.io,
        v.activity_level,
        ifnull(f.candidate, 0) candidate_check,
        e.source_facility_id,
        v.source_facility_id,
        v.commodity_id,
        c.share_max_transport_distance
        from vertices v, facility_commodities fc, facility_type_id ft, commodities c, facilities f
        join edges e on (v.vertex_id = e.o_vertex_id or v.vertex_id = e.d_vertex_id)
        where ft.facility_type = 'processor'
        and v.facility_id = f.facility_id
        and ft.facility_type_id = v.facility_type_id
        and storage_vertex = 0
        and v.facility_id = fc.facility_id
        and fc.commodity_id = c.commodity_id
        and fc.commodity_id = e.commodity_id
        group by v.vertex_id,
        in_or_out_edge,
        constraint_day,
        e.commodity_id,
        e.mode,
        e.edge_id,
        nx_edge_id, fc.quantity, v.facility_id, c.commodity_name,
        fc.io,
        v.activity_level,
        candidate_check,
        e.source_facility_id,
        v.commodity_id,
        v.source_facility_id,
        ifnull(c.share_max_transport_distance, 'N')
        order by v.facility_id, e.source_facility_id, v.vertex_id, fc.io, e.edge_id
        ;"""

        logger.info("Starting the execute")
        execute_start_time = datetime.datetime.now()
        sql_data = db_cur.execute(sql)
        logger.info("Done with the execute fetch all for :")
        logger.info(
            "execute for processor primary vertices, with their in and out edges - Total Runtime (HMS): \t{} \t ".format(
                get_total_runtime_string(execute_start_time)))

        logger.info("Starting the fetchall")
        fetchall_start_time = datetime.datetime.now()
        sql_data = sql_data.fetchall()
        logger.info(
            "fetchall processor primary vertices, with their in and out edges - Total Runtime (HMS): \t{} \t ".format(
                get_total_runtime_string(fetchall_start_time)))

        # Nested dictionaries
        # flow_in_lists[primary_processor_vertex_id] = dict of commodities handled by that processor vertex

        # flow_in_lists[primary_processor_vertex_id][commodity1] =
        # list of edge ids that flow that commodity into that vertex

        # flow_in_lists[vertex_id].values() to get all flow_in edges for all commodities, a list of lists
        # if edge out commodity inherits transport distance, then source_facility id must match. if not, aggregate

        flow_in_lists = {}
        flow_out_lists = {}
        inherit_max_transport = {}
        # inherit_max_transport[commodity_id] = 'Y' or 'N'

        for row_a in sql_data:

            vertex_id = row_a[0]
            in_or_out_edge = row_a[1]
            # constraint_day = row_a[2]
            commodity_id = row_a[3]
            # mode = row_a[4]
            edge_id = row_a[5]
            # nx_edge_id = row_a[6]
            facility_id = row_a[8]
            #=========================================Modification====================================================================
            # Following part is modified to include time-varying facility capacity in FTOT
            if j < repair_time_facility[float(facility_id)-2][t+1][i]:
                quantity = facility_cap[float(facility_id)-2][t+1][i]*row_a[7]
            else:
                quantity = facility_cap_noEarthquake[float(facility_id)-2][t+1][i]*row_a[7]
           
            #===================================================End======================================================================
    
            # facility_id = row_a[8]
            # commodity_name = row_a[9]
            # fc_io_commodity = row_a[10]
            # activity_level = row_a[11]
            # is_candidate = row_a[12]
            edge_source_facility_id = row_a[13]
            vertex_source_facility_id = row_a[14]
            # v_commodity_id = row_a[15]
            inherit_max_transport_distance = row_a[16]
            if commodity_id not in inherit_max_transport.keys():
                if inherit_max_transport_distance == 'Y':
                    inherit_max_transport[commodity_id] = 'Y'
                else:
                    inherit_max_transport[commodity_id] = 'N'

            if in_or_out_edge == 'in':
                # if the vertex isn't in the main dict yet, add it
                # could have multiple source facilities
                # could also have more than one input commodity now
                flow_in_lists.setdefault(vertex_id, {})
                flow_in_lists[vertex_id].setdefault((commodity_id, quantity, edge_source_facility_id), []).append(flow_var[edge_id])
                # flow_in_lists[vertex_id] is itself a dict keyed on commodity, quantity (ratio) and edge_source_facility;
                # value is a list of edge ids into that vertex of that commodity and edge source

            elif in_or_out_edge == 'out':
                # for out-lists, could have multiple commodities as well as multiple sources
                # some may have a max transport distance, inherited or independent, some may not
                flow_out_lists.setdefault(vertex_id, {})  # if the vertex isn't in the main dict yet, add it
                flow_out_lists[vertex_id].setdefault((commodity_id, quantity, edge_source_facility_id), []).append(flow_var[edge_id])

            # Because we keyed on commodity, source facility tracking is merged as we pass through the processor vertex

            # 1) for each output commodity, check against an input to ensure correct ratio - only need one input
            # 2) for each input commodity, check against an output to ensure correct ratio - only need one output;
            # 2a) first sum sub-flows over input commodity

        # 1----------------------------------------------------------------------
        constrained_input_flow_vars = set([])
        # pdb.set_trace()

        for key, value in flow_out_lists.iteritems():
            #value is a dictionary with commodity & source as keys
            # set up a dictionary that will be filled with input lists to check ratio against
            compare_input_dict = {}
            compare_input_dict_commod = {}
            vertex_id = key
            zero_in = False
            #value is a dictionary keyed on output commodity, quantity required, edge source
            if vertex_id in flow_in_lists:
                in_quantity = 0
                in_commodity_id = 0
                in_source_facility_id = -1
                for ikey, ivalue in flow_in_lists[vertex_id].iteritems():
                    in_commodity_id = ikey[0]
                    in_quantity = ikey[1]
                    in_source = ikey[2]
                    # list of edges
                    compare_input_dict[in_source] = ivalue
                    # to accommodate and track multiple input commodities; does not keep sources separate
                    # aggregate lists over sources, by commodity
                    if in_commodity_id not in compare_input_dict_commod.keys():
                        compare_input_dict_commod[in_commodity_id] = set([])
                    for edge in ivalue:
                        compare_input_dict_commod[in_commodity_id].add(edge)
            else:
                zero_in = True


            # value is a dict - we loop once here for each output commodity and source at the vertex
            for key2, value2 in value.iteritems():
                out_commodity_id = key2[0]
                out_quantity = key2[1]
                out_source = key2[2]
                # edge_list = value2
                flow_var_list = value2
                # if we need to match source facility, there is only one set of input lists
                # otherwise, use all input lists - this aggregates sources
                # need to keep commodities separate, units may be different
                # known issue -  we could have double-counting problems if only some outputs have to inherit max
                # transport distance through this facility
                match_source = inherit_max_transport[out_commodity_id]
                compare_input_list = []
                if match_source == 'Y':
                    if len(compare_input_dict_commod.keys()) >1:
                        error = "Multiple input commodities for processors and shared max transport distance are" \
                                " not supported within the same scenario."
                        logger.error(error)
                        raise Exception(error)

                    if out_source in compare_input_dict.keys():
                        compare_input_list = compare_input_dict[out_source]
                # if no valid input edges - none for vertex, or if output needs to match source and there are no
                # matching source
                if zero_in or (match_source == 'Y' and len(compare_input_list) == 0):
                    prob += lpSum(
                        flow_var_list) == 0, "processor flow, vertex {} has zero in so zero out of commodity {} " \
                                             "with source {} if applicable".format(
                        vertex_id, out_commodity_id, out_source)
                else:
                    if match_source == 'Y':
                        # ratio constraint for this output commodity relative to total input of each commodity
                        required_flow_out = lpSum(flow_var_list) / out_quantity
                        # check against an input dict
                        prob += required_flow_out == lpSum(
                            compare_input_list) / in_quantity, "processor flow, vertex {}, source_facility {}," \
                                                               " commodity {} output quantity" \
                                                               " checked against single input commodity quantity".format(
                            vertex_id, out_source, out_commodity_id, in_commodity_id)
                        for flow_var in compare_input_list:
                            constrained_input_flow_vars.add(flow_var)
                    else:
                        for k, v in compare_input_dict_commod.iteritems():
                            # pdb.set_trace()
                            # as long as the input source doesn't match an output that needs to inherit
                            compare_input_list = list(v)
                            in_commodity_id = k
                            # ratio constraint for this output commodity relative to total input of each commodity
                            required_flow_out = lpSum(flow_var_list) / out_quantity
                            # check against an input dict
                            prob += required_flow_out == lpSum(
                                compare_input_list) / in_quantity, "processor flow, vertex {}, source_facility {}," \
                                                                   " commodity {} output quantity" \
                                                                   " checked against commodity {} input quantity".format(
                                vertex_id, out_source, out_commodity_id, in_commodity_id)
                            for flow_var in compare_input_list:
                                constrained_input_flow_vars.add(flow_var)

        for key, value in flow_in_lists.iteritems():
            vertex_id = key
            for key2, value2 in value.iteritems():
                commodity_id = key2[0]
                # out_quantity = key2[1]
                source = key2[2]
                # edge_list = value2
                flow_var_list = value2
                for flow_var in flow_var_list:
                    if flow_var not in constrained_input_flow_vars:
                        prob += flow_var == 0, "processor flow, vertex {} has no matching out edges so zero in of " \
                                               "commodity {} with source {}".format(
                            vertex_id, commodity_id, source)

    logger.debug("FINISHED:  create_primary_processor_conservation_of_flow_constraints")
    return prob


# ===============================================================================


def create_constraint_conservation_of_flow(logger, the_scenario, prob, flow_var, processor_excess_vars):
    logger.debug("STARTING:  create_constraint_conservation_of_flow")
    # node_counter = 0
    node_constraint_counter = 0
    storage_vertex_constraint_counter = 0

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()

        logger.info("conservation of flow, storage vertices:")
        # storage vertices, any facility type
        # these have at most one direction of transport edges, so no need to track mode
        sql = """select v.vertex_id,
        (case when e.o_vertex_id = v.vertex_id then 'out' 
        when e.d_vertex_id = v.vertex_id then 'in' else 'error' end) in_or_out_edge,
        (case when e.o_vertex_id = v.vertex_id then start_day 
        when e.d_vertex_id = v.vertex_id then end_day else 0 end) constraint_day,
        v.commodity_id,
        e.edge_id,
        nx_edge_id, v.facility_id, c.commodity_name,
        v.activity_level,
        ft.facility_type

        from vertices v, facility_type_id ft, commodities c, facilities f
        join edges e on ((v.vertex_id = e.o_vertex_id or v.vertex_id = e.d_vertex_id) 
        and (e.o_vertex_id = v.vertex_id or e.d_vertex_id = v.vertex_id) and v.commodity_id = e.commodity_id)

        where  v.facility_id = f.facility_id
        and ft.facility_type_id = v.facility_type_id
        and storage_vertex = 1
        and v.commodity_id = c.commodity_id

        group by v.vertex_id,
        in_or_out_edge,
        constraint_day,
        v.commodity_id,
        e.edge_id,
        nx_edge_id,v.facility_id, c.commodity_name,
        v.activity_level

        order by v.facility_id, v.vertex_id, e.edge_id
        ;"""

        # get the data from sql and see how long it takes.
        logger.info("Starting the long step:")

        logger.info("Starting the execute")
        execute_start_time = datetime.datetime.now()
        vertexid_data = db_cur.execute(sql)
        logger.info("Done with the execute fetch all for :")
        logger.info("execute for storage vertices, with their in and out edges - Total Runtime (HMS): \t{} \t ".format(
            get_total_runtime_string(execute_start_time)))

        logger.info("Starting the fetchall")
        fetchall_start_time = datetime.datetime.now()
        vertexid_data = vertexid_data.fetchall()
        logger.info(
            "fetchall nodes with no location id, with their in and out edges - Total Runtime (HMS): \t{} \t ".format(
                get_total_runtime_string(fetchall_start_time)))

        flow_in_lists = {}
        flow_out_lists = {}
        for row_v in vertexid_data:
            vertex_id = row_v[0]
            in_or_out_edge = row_v[1]
            constraint_day = row_v[2]
            commodity_id = row_v[3]
            edge_id = row_v[4]
            # nx_edge_id = row_v[5]
            # facility_id = row_v[6]
            # commodity_name = row_v[7]
            # activity_level = row_v[8]
            facility_type = row_v[9]

            if in_or_out_edge == 'in':
                flow_in_lists.setdefault((vertex_id, commodity_id, constraint_day, facility_type), []).append(
                    flow_var[edge_id])
            elif in_or_out_edge == 'out':
                flow_out_lists.setdefault((vertex_id, commodity_id, constraint_day, facility_type), []).append(
                    flow_var[edge_id])

        logger.info("adding processor excess variabless to conservation of flow")
        for key, value in flow_out_lists.iteritems():
            vertex_id = key[0]
            # commodity_id = key[1]
            # day = key[2]
            facility_type = key[3]
            if facility_type == 'processor':
                flow_out_lists.setdefault(key, []).append(processor_excess_vars[vertex_id])

        for key, value in flow_out_lists.iteritems():

            if key in flow_in_lists:
                prob += lpSum(flow_out_lists[key]) == lpSum(
                    flow_in_lists[key]), "conservation of flow, vertex {}, commodity {},  day {}".format(key[0], key[1],
                                                                                                         key[2])
                storage_vertex_constraint_counter = storage_vertex_constraint_counter + 1
            else:
                prob += lpSum(flow_out_lists[key]) == lpSum(
                    0), "conservation of flow (zero out),  vertex {}, commodity {},  day {}".format(key[0], key[1],
                                                                                                    key[2])
                storage_vertex_constraint_counter = storage_vertex_constraint_counter + 1

        for key, value in flow_in_lists.iteritems():

            if key not in flow_out_lists:
                prob += lpSum(flow_in_lists[key]) == lpSum(
                    0), "conservation of flow (zero in),  vertex {}, commodity {},  day {}".format(key[0], key[1],
                                                                                                   key[2])
                storage_vertex_constraint_counter = storage_vertex_constraint_counter + 1

        logger.info(
            "total conservation of flow constraints created on nodes: {}".format(storage_vertex_constraint_counter))

        logger.info("conservation of flow, nx_nodes:")

        # for each day, get all edges in and out of the node.
        # Sort edges by commodity and whether they're going in or out of the node
        sql = """select nn.node_id,
            (case when e.from_node_id = nn.node_id then 'out' 
            when e.to_node_id = nn.node_id then 'in' else 'error' end) in_or_out_edge,
            (case when e.from_node_id = nn.node_id then start_day 
            when e.to_node_id = nn.node_id then end_day else 0 end) constraint_day,
            e.commodity_id,
            ifnull(mode, 'NULL'),
            e.edge_id, nx_edge_id,
            miles,
            (case when ifnull(nn.source, 'N') == 'intermodal' then 'Y' else 'N' end) intermodal_flag,
            e.source_facility_id,
            e.commodity_id
            from networkx_nodes nn
            join edges e on (nn.node_id = e.from_node_id or nn.node_id = e.to_node_id)
            where nn.location_id is null
            order by nn.node_id, e.commodity_id,
            (case when e.from_node_id = nn.node_id then start_day 
            when e.to_node_id = nn.node_id then end_day else 0 end),
            in_or_out_edge, e.source_facility_id, e.commodity_id
            ;"""

        logger.info("Starting the long step:")

        logger.info("Starting the execute")
        execute_start_time = datetime.datetime.now()
        nodeid_data = db_cur.execute(sql)
        logger.info("Done with the execute fetch all for :")
        logger.info(
            "execute for  nodes with no location id, with their in and out edges - Total Runtime (HMS): \t{} \t "
            "".format(
                get_total_runtime_string(execute_start_time)))

        logger.info("Starting the fetchall")
        fetchall_start_time = datetime.datetime.now()
        nodeid_data = nodeid_data.fetchall()
        logger.info(
            "fetchall nodes with no location id, with their in and out edges - Total Runtime (HMS): \t{} \t ".format(
                get_total_runtime_string(fetchall_start_time)))

        flow_in_lists = {}
        flow_out_lists = {}

        for row_a in nodeid_data:
            node_id = row_a[0]
            in_or_out_edge = row_a[1]
            constraint_day = row_a[2]
            # commodity_id = row_a[3]
            mode = row_a[4]
            edge_id = row_a[5]
            # nx_edge_id = row_a[6]
            # miles = row_a[7]
            intermodal = row_a[8]
            source_facility_id = row_a[9]
            commodity_id = row_a[10]

            # node_counter = node_counter +1
            # if node is not intermodal, conservation of flow holds per mode;
            # if intermodal, then across modes
            if intermodal == 'N':
                if in_or_out_edge == 'in':
                    flow_in_lists.setdefault(
                        (node_id, intermodal, source_facility_id, constraint_day, commodity_id, mode), []).append(
                        flow_var[edge_id])
                elif in_or_out_edge == 'out':
                    flow_out_lists.setdefault(
                        (node_id, intermodal, source_facility_id, constraint_day, commodity_id, mode), []).append(
                        flow_var[edge_id])
            else:
                if in_or_out_edge == 'in':
                    flow_in_lists.setdefault((node_id, intermodal, source_facility_id, constraint_day, commodity_id),
                                             []).append(flow_var[edge_id])
                elif in_or_out_edge == 'out':
                    flow_out_lists.setdefault((node_id, intermodal, source_facility_id, constraint_day, commodity_id),
                                              []).append(flow_var[edge_id])

        for key, value in flow_out_lists.iteritems():
            node_id = key[0]
            # intermodal_flag = key[1]
            source_facility_id = key[2]
            day = key[3]
            commodity_id = key[4]
            if len(key) == 6:
                node_mode = key[5]
            else:
                node_mode = 'intermodal'
            if key in flow_in_lists:
                prob += lpSum(flow_out_lists[key]) == lpSum(flow_in_lists[
                                                                key]), "conservation of flow, nx node {}, " \
                                                                       "source facility {}, commodity {},  " \
                                                                       "day {}, mode {}".format(
                    node_id, source_facility_id, commodity_id, day, node_mode)
                node_constraint_counter = node_constraint_counter + 1
            else:
                prob += lpSum(flow_out_lists[key]) == lpSum(
                    0), "conservation of flow (zero out), nx node {}, source facility {},  commodity {}, day {}," \
                        " mode {}".format(node_id, source_facility_id, commodity_id, day, node_mode)
                node_constraint_counter = node_constraint_counter + 1

        for key, value in flow_in_lists.iteritems():
            node_id = key[0]
            # intermodal_flag = key[1]
            source_facility_id = key[2]
            day = key[3]
            commodity_id = key[4]
            if len(key) == 6:
                node_mode = key[5]
            else:
                node_mode = 'intermodal'

            if key not in flow_out_lists:
                prob += lpSum(flow_in_lists[key]) == lpSum(
                    0), "conservation of flow (zero in), nx node {}, source facility {}, commodity {},  day {}," \
                        " mode {}".format(node_id, source_facility_id, commodity_id, day, node_mode)
                node_constraint_counter = node_constraint_counter + 1

        logger.info("total conservation of flow constraints created on nodes: {}".format(node_constraint_counter))

        # Note: no consesrvation of flow for primary vertices for supply & demand - they have unique constraints

    logger.debug("FINISHED:  create_constraint_conservation_of_flow")

    return prob


# ===============================================================================


def create_constraint_max_route_capacity(logger, the_scenario, prob, flow_var):
    i = np.load("scenario_num.npy")
    t = np.load("time_horizon.npy")
    j = np.load("earthquake_day.npy")
    logger.info("scenario {}".format(i))
    logger.info("time horizon {}".format(t))
    logger.info("earthquake day {}".format(j))
    
    logger.info("STARTING:  create_constraint_max_route_capacity")
    logger.info("modes with background flow turned on: {}".format(the_scenario.backgroundFlowModes))
    # min_capacity_level must be a number from 0 to 1, inclusive
    # min_capacity_level is only relevant when background flows are turned on
    # it sets a floor to how much capacity can be reduced by volume.
    # min_capacity_level = .25 means route capacity will never be less than 25% of full capacity,
    # even if "volume" would otherwise restrict it further
    # min_capacity_level = 0 allows a route to be made unavailable for FTOT flow if base volume is too high
    # this currently applies to all modes
    logger.info("minimum available capacity floor set at: {}".format(the_scenario.minCapacityLevel))

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()
        # capacity for storage routes
        sql = """select
                rr.route_id, sr.storage_max, sr.route_name, e.edge_id, e.start_day
                from route_reference rr
                join storage_routes sr on sr.route_name = rr.route_name
                join edges e on rr.route_id = e.route_id
                ;"""
        # get the data from sql and see how long it takes.

        logger.info("Starting the execute")
        execute_start_time = datetime.datetime.now()
        storage_edge_data = db_cur.execute(sql)
        logger.info("Done with the execute fetch all for storage edges:")
        logger.info("execute for edges for storage - Total Runtime (HMS): \t{} \t ".format(
            get_total_runtime_string(execute_start_time)))

        logger.info("Starting the fetchall")
        fetchall_start_time = datetime.datetime.now()
        storage_edge_data = storage_edge_data.fetchall()
        logger.info("fetchall edges for storage - Total Runtime (HMS): \t{} \t ".format(
            get_total_runtime_string(fetchall_start_time)))

        flow_lists = {}

        for row_a in storage_edge_data:
            route_id = row_a[0]
            aggregate_storage_capac = row_a[1]
            storage_route_name = row_a[2]
            edge_id = row_a[3]
            start_day = row_a[4]

            flow_lists.setdefault((route_id, aggregate_storage_capac, storage_route_name, start_day), []).append(
                flow_var[edge_id])

        for key, flow in flow_lists.iteritems():
            prob += lpSum(flow) <= key[1], "constraint max flow on storage route {} named {} for day {}".format(key[0],
                                                                                                                key[2],
                                                                                                                key[3])

        logger.debug("route_capacity constraints created for all storage routes")

        # capacity for transport routes
        # Assumption - all flowing material is in kgal, all flow is summed on a single non-pipeline nx edge
        sql = """select e.edge_id, e.nx_edge_id, e.max_edge_capacity, e.start_day, e.simple_mode, e.phase_of_matter,
         e.capac_minus_volume_zero_floor
        from edges e
        where e.max_edge_capacity is not null
        and e.simple_mode != 'pipeline'
        ;"""
        # get the data from sql and see how long it takes.

        logger.info("Starting the execute")
        execute_start_time = datetime.datetime.now()
        route_capac_data = db_cur.execute(sql)
        logger.info("Done with the execute fetch all for transport edges:")
        logger.info("execute for non-pipeline edges for transport edge capacity - Total Runtime (HMS): \t{} \t ".format(
            get_total_runtime_string(execute_start_time)))

        logger.info("Starting the fetchall")
        fetchall_start_time = datetime.datetime.now()
        route_capac_data = route_capac_data.fetchall()
        logger.info("fetchall non-pipeline  edges for transport edge capacity - Total Runtime (HMS): \t{} \t ".format(
            get_total_runtime_string(fetchall_start_time)))

        flow_lists = {}

        for row_a in route_capac_data:
            edge_id = row_a[0]
            nx_edge_id = row_a[1]
            nx_edge_capacity = row_a[2]
            start_day = row_a[3]
            simple_mode = row_a[4]
            phase_of_matter = row_a[5]
            capac_minus_background_flow = max(row_a[6], 0)
            min_restricted_capacity = max(capac_minus_background_flow, nx_edge_capacity * the_scenario.minCapacityLevel)

            if simple_mode in the_scenario.backgroundFlowModes:
                use_capacity = min_restricted_capacity
            else:
                use_capacity = nx_edge_capacity

            # flow is in thousand gallons (kgal), for liquid, or metric tons, for solid
            # capacity is in truckload, rail car, barge, or pipeline movement per day
            # if mode is road and phase is liquid, capacity is in truckloads per day, we want it in kgal
            # ftot_supporting_gis tells us that there are 8 kgal per truckload,
            # so capacity * 8 gives us correct units or kgal per day
            # => use capacity * ftot_supporting_gis multiplier to get capacity in correct flow units

            multiplier = 1  # if units match, otherwise specified here
            if simple_mode == 'road':
                if phase_of_matter == 'liquid':
                    multiplier = the_scenario.truck_load_liquid.magnitude
                elif phase_of_matter == 'solid':
                    multiplier = the_scenario.truck_load_solid.magnitude
            elif simple_mode == 'water':
                if phase_of_matter == 'liquid':
                    multiplier = the_scenario.barge_load_liquid.magnitude
                elif phase_of_matter == 'solid':
                    multiplier = the_scenario.barge_load_solid.magnitude
            elif simple_mode == 'rail':
                if phase_of_matter == 'liquid':
                    multiplier = the_scenario.railcar_load_liquid.magnitude
                elif phase_of_matter == 'solid':
                    multiplier = the_scenario.railcar_load_solid.magnitude
            #=========================================Modification====================================================================
            # Following part is modified to include time-varying edge capacity in FTOT
            if float(edge_id) in edge_cap[:,1,1]:
               if j < repair_time_edge[float(edge_id)][t+1][i]:
                  capacity_reduction_index = edge_cap[float(edge_id)][t+1][i]*0
               else:
                  capacity_reduction_index = 1
            else:
                capacity_reduction_index = 1
                #if j < max(repair_time_edge[:,t+1,i]):
                #    capacity_reduction_index = 0.0001
                #else:
                #    capacity_reduction_index = 1
     
            converted_capacity = use_capacity * multiplier * capacity_reduction_index
             #===================================================End===================================================================

            flow_lists.setdefault((nx_edge_id, converted_capacity, start_day), []).append(flow_var[edge_id])

        for key, flow in flow_lists.iteritems():
            prob += lpSum(flow) <= key[1], "constraint max flow on nx edge {} for day {}".format(key[0], key[2])

        logger.debug("route_capacity constraints created for all non-pipeline  transport routes")

    logger.debug("FINISHED:  create_constraint_max_route_capacity")
    return prob


# ===============================================================================


def create_constraint_pipeline_capacity(logger, the_scenario, prob, flow_var):
    logger.debug("STARTING:  create_constraint_pipeline_capacity")
    logger.debug("Length of flow_var: {}".format(len(flow_var.items())))
    logger.info("modes with background flow turned on: {}".format(the_scenario.backgroundFlowModes))
    logger.info("minimum available capacity floor set at: {}".format(the_scenario.minCapacityLevel))

    with sqlite3.connect(the_scenario.main_db) as main_db_con:
        db_cur = main_db_con.cursor()

        # capacity for pipeline tariff routes
        # with sasc, may have multiple flows per segment, slightly diff commodities
        sql = """select e.edge_id, e.tariff_id, l.link_id, l.capac, e.start_day, l.capac-l.background_flow allowed_flow, 
        l.source, e.mode, instr(e.mode, l.source)
        from edges e, pipeline_mapping pm,
        (select id_field_name, cn.source_OID as link_id, min(cn.capacity) capac,
        max(cn.volume) background_flow, source
        from capacity_nodes cn
        where cn.id_field_name = 'MASTER_OID'
        and ifnull(cn.capacity,0)>0
        group by link_id) l

        where e.tariff_id = pm.id
        and pm.id_field_name = 'tariff_ID'
        and pm.mapping_id_field_name = 'MASTER_OID'
        and l.id_field_name = 'MASTER_OID'
        and pm.mapping_id = l.link_id
        and instr(e.mode, l.source)>0
        group by e.edge_id, e.tariff_id, l.link_id, l.capac, e.start_day, allowed_flow, l.source
        ;"""
        # capacity needs to be shared over link_id for any edge_id associated with that link

        logger.info("Starting the execute")
        execute_start_time = datetime.datetime.now()
        pipeline_capac_data = db_cur.execute(sql)
        logger.info("Done with the execute fetch all for transport edges:")
        logger.info("execute for edges for transport edge capacity - Total Runtime (HMS): \t{} \t ".format(
            get_total_runtime_string(execute_start_time)))

        logger.info("Starting the fetchall")
        fetchall_start_time = datetime.datetime.now()
        pipeline_capac_data = pipeline_capac_data.fetchall()
        logger.info("fetchall edges for transport edge capacity - Total Runtime (HMS): \t{} \t ".format(
            get_total_runtime_string(fetchall_start_time)))

        flow_lists = {}

        for row_a in pipeline_capac_data:
            edge_id = row_a[0]
            # tariff_id = row_a[1]
            link_id = row_a[2]
            # Link capacity is recorded in "thousand barrels per day"; 1 barrel = 42 gall
            # Link capacity * 42 is now in kgal per day, to match flow in kgal
            link_capacity_kgal_per_day = THOUSAND_GALLONS_PER_THOUSAND_BARRELS * row_a[3]
            start_day = row_a[4]
            capac_minus_background_flow_kgal = max(THOUSAND_GALLONS_PER_THOUSAND_BARRELS * row_a[5], 0)
            min_restricted_capacity = max(capac_minus_background_flow_kgal,
                                          link_capacity_kgal_per_day * the_scenario.minCapacityLevel)

            # capacity_nodes_mode_source = row_a[6]
            edge_mode = row_a[7]
            # mode_match_check = row_a[8]
            if 'pipeline' in the_scenario.backgroundFlowModes:
                link_use_capacity = min_restricted_capacity
            else:
                link_use_capacity = link_capacity_kgal_per_day

            # add flow from all relevant edges, for one start; may be multiple tariffs
            flow_lists.setdefault((link_id, link_use_capacity, start_day, edge_mode), []).append(flow_var[edge_id])

        for key, flow in flow_lists.iteritems():
            prob += lpSum(flow) <= key[1], "constraint max flow on pipeline link {} for mode {} for day {}".format(
                key[0], key[3], key[2])

        logger.debug("pipeline capacity constraints created for all transport routes")

    logger.debug("FINISHED:  create_constraint_pipeline_capacity")
    return prob


# ===============================================================================


def setup_pulp_problem(the_scenario, logger):
    logger.info("START: setup PuLP problem")

    # flow_var is the flow on each edge by commodity and day.
    # the optimal value of flow_var will be solved by PuLP
    flow_vars = create_flow_vars(the_scenario, logger)

    # unmet_demand_var is the unmet demand at each destination, being determined
    unmet_demand_vars = create_unmet_demand_vars(the_scenario, logger)

    # processor_build_vars is the binary variable indicating whether a candidate processor is used
    # and thus whether its build cost is charged
    processor_build_vars = create_candidate_processor_build_vars(the_scenario, logger)

    # binary tracker variables
    processor_vertex_flow_vars = create_binary_processor_vertex_flow_vars(the_scenario, logger)

    # tracking unused production
    processor_excess_vars = create_processor_excess_output_vars(the_scenario, logger)

    # THIS IS THE OBJECTIVE FUCTION FOR THE OPTIMIZATION
    # ==================================================

    prob = create_opt_problem(logger, the_scenario, unmet_demand_vars, flow_vars, processor_build_vars)
    logger.debug("MNP DEBUG: size of prob: {}".format(sys.getsizeof(prob)))

    prob = create_constraint_unmet_demand(logger, the_scenario, prob, flow_vars, unmet_demand_vars)
    logger.debug("MNP DEBUG: size of prob: {}".format(sys.getsizeof(prob)))

    prob = create_constraint_max_flow_out_of_supply_vertex(logger, the_scenario, prob, flow_vars)

    # This constraint is being excluded because 1) it is not used in current scenarios and 2) it is not supported by
    # this version - it conflicts with the change permitting multiple inputs
    prob = create_constraint_daily_processor_capacity(logger, the_scenario, prob, flow_vars, processor_build_vars,
                                                      processor_vertex_flow_vars)

    prob = create_primary_processor_vertex_constraints(logger, the_scenario, prob, flow_vars)

    prob = create_constraint_conservation_of_flow(logger, the_scenario, prob, flow_vars, processor_excess_vars)

    if the_scenario.capacityOn:
       prob = create_constraint_max_route_capacity(logger, the_scenario, prob, flow_vars)

       prob = create_constraint_pipeline_capacity(logger, the_scenario, prob, flow_vars)

    del unmet_demand_vars

    del flow_vars

    # The problem data is written to an .lp file
    prob.writeLP(os.path.join(the_scenario.scenario_run_directory, "debug", "LP_output_c2.lp"))

    logger.info("FINISHED: setup PuLP problem")
    return prob


# ===============================================================================


def solve_pulp_problem(prob_final, the_scenario, logger):
    import datetime

    logger.info("START: solve_pulp_problem")
    start_time = datetime.datetime.now()
    from os import dup, dup2, close
    f = open(os.path.join(the_scenario.scenario_run_directory, "debug", 'probsolve_capture.txt'), 'w')
    orig_std_out = dup(1)
    dup2(f.fileno(), 1)

    # status = prob_final.solve (PULP_CBC_CMD(maxSeconds = i_max_sec, fracGap = d_opt_gap, msg=1))
    #  CBC time limit and relative optimality gap tolerance
    status = prob_final.solve(PULP_CBC_CMD(msg=1))  # CBC time limit and relative optimality gap tolerance
    print('Completion code: %d; Solution status: %s; Best obj value found: %s' % (
        status, LpStatus[prob_final.status], value(prob_final.objective)))

    dup2(orig_std_out, 1)
    close(orig_std_out)
    f.close()
    # The problem is solved using PuLP's choice of Solver

    logger.info("completed calling prob.solve()")
    logger.info(
        "FINISH: prob.solve(): Runtime (HMS): \t{}".format(ftot_supporting.get_total_runtime_string(start_time)))

    # THIS IS THE SOLUTION

    # The status of the solution is printed to the screen
    ##LpStatus key    string value    numerical value
    ##LpStatusOptimal    ?Optimal?    1
    ##LpStatusNotSolved    ?Not Solved?    0
    ##LpStatusInfeasible    ?Infeasible?    -1
    ##LpStatusUnbounded    ?Unbounded?    -2
    ##LpStatusUndefined    ?Undefined?    -3
    logger.result("prob.Status: \t {}".format(LpStatus[prob_final.status]))

    logger.result(
        "Total Scenario Cost = (transportation + unmet demand penalty + processor construction): \t ${0:,.0f}".format(
            float(value(prob_final.objective))))

    return prob_final


# ===============================================================================


def pickle_prob(input_prob, save_file_name,the_scenario, logger):
    logger.info("START: pickle_prob")
    # save_file_name should be .p type, ex. "constrained_prob.p"
    import cPickle as pickle

    pickle.dump(input_prob, open(os.path.join(the_scenario.scenario_run_directory, "debug", save_file_name), "wb"), protocol=2)

    return


# ===============================================================================


def load_pickled_prob(save_file_name,the_scenario, logger):
    logger.info("START: load_pickled_prob")
    # save_file_name should be .p type, ex. "constrained_prob.p"
    import cPickle as pickle

    prob = pickle.load(open(os.path.join(the_scenario.scenario_run_directory, "debug", save_file_name), "rb"))

    return prob


# ===============================================================================

def save_pulp_solution(the_scenario, prob, logger):
    i = np.load("scenario_num.npy")
    t = np.load("time_horizon.npy")
    j = np.load("earthquake_day.npy")
    logger.info("scenario {}".format(i))
    logger.info("time horizon {}".format(t))
    logger.info("earthquake day {}".format(j))

    
    import datetime

    logger.info("START: save_pulp_solution")
    non_zero_variable_count = 0

    with sqlite3.connect(the_scenario.main_db) as db_con:

        db_cur = db_con.cursor()
        # drop  the optimal_solution table
        # -----------------------------
        db_cur.executescript("drop table if exists optimal_solution;")

        # create the optimal_solution table
        # -----------------------------
        db_cur.executescript("""
                                create table optimal_solution
                                (
                                    variable_name string,
                                    variable_value real
                                );
                                """)

        # insert the optimal data into the DB
        # -------------------------------------
        for v in prob.variables():
            sql = """insert into optimal_solution  (variable_name, variable_value) values ("{}", {});""".format(
                v.name, float(v.varValue))
            db_con.execute(sql)
            non_zero_variable_count = non_zero_variable_count + 1

        logger.info("number of solution variables non zero: {}".format(non_zero_variable_count))
        sql = """
            create table optimal_variables as
            select
            'UnmetDemand' as variable_type,
            cast(substr(variable_name, 13) as int) var_id,
            variable_value,
            null as converted_capacity,
            null as converted_volume,
            null as converted_capac_minus_volume,
            null as  edge_type,
            null as  commodity_name,
            null as o_facility,
            'placeholder' as d_facility,
            null as  o_vertex_id,
            null as  d_vertex_id,
            null as  from_node_id,
            null as  to_node_id,
            null as  time_period,
            null as commodity_id,
            null as source_facility_id,
            null as source_facility_name,
            null as units,
            variable_name,
            null as  nx_edge_id,
            null as mode,
            null as mode_oid,
            null as miles,
            null as original_facility,
            null as final_facility,
            null as prior_edge,
            null as miles_travelled
            from optimal_solution
            where variable_name like 'UnmetDemand%'
            union
            select
            'Edge' as variable_type,
            cast(substr(variable_name, 6) as int) var_id,
            variable_value,
            edges.max_edge_capacity*edges.units_conversion_multiplier as converted_capacity,
            edges.volume*edges.units_conversion_multiplier as converted_volume,
            edges.capac_minus_volume_zero_floor*edges.units_conversion_multiplier as converted_capac_minus_volume,
            edges.edge_type,
            commodities.commodity_name,
            ov.facility_name as o_facility,
            dv.facility_name as d_facility,
            o_vertex_id,
            d_vertex_id,
            from_node_id,
            to_node_id,
            start_day time_period,
            edges.commodity_id,
            edges.source_facility_id,
            s.source_facility_name,
            commodities.units,
            variable_name,
            edges.nx_edge_id,
            edges.mode,
            edges.mode_oid,
            edges.miles,
            null as original_facility,
            null as final_facility,
            null as prior_edge,
            edges.miles_travelled as miles_travelled
            from optimal_solution
            join edges on edges.edge_id = cast(substr(variable_name, 6) as int)
            join commodities on edges.commodity_id = commodities.commodity_ID
            left outer join vertices as ov on edges.o_vertex_id = ov.vertex_id
            left outer join vertices as dv on edges.d_vertex_id = dv.vertex_id
            left outer join source_commodity_ref as s on edges.source_facility_id = s.source_facility_id
            where variable_name like 'Edge%'
            union
            select
            'BuildProcessor' as variable_type,
            cast(substr(variable_name, 16) as int) var_id,
            variable_value,
            null as converted_capacity,
            null as converted_volume,
            null as converted_capac_minus_volume,
            null as  edge_type,
            null as  commodity_name,
            'placeholder' as o_facility,
            'placeholder' as d_facility,
            null as  o_vertex_id,
            null as  d_vertex_id,
            null as  from_node_id,
            null as  to_node_id,
            null as  time_period,
            null as commodity_id,
            null as source_facility_id,
            null as source_facility_name,
            null as units,
            variable_name,
            null as  nx_edge_id,
            null as mode,
            null as mode_oid,
            null as miles,
            null as original_facility,
            null as final_facility,
            null as prior_edge,
            null as miles_travelled
            from optimal_solution
            where variable_name like 'Build%';
            """
        db_con.execute("drop table if exists optimal_variables;")
        db_con.execute(sql)

        # query the optimal_solution table in the DB for each variable we care about
        # ----------------------------------------------------------------------------
        sql = "select count(variable_name) from optimal_solution where variable_name like 'BuildProcessor%';"
        data = db_con.execute(sql)
        optimal_processors_count = data.fetchone()[0]
        logger.info("number of optimal_processors: {}".format(optimal_processors_count))

        sql = "select count(variable_name) from optimal_solution where variable_name like 'UnmetDemand%';"
        data = db_con.execute(sql)
        optimal_unmet_demand_count = data.fetchone()[0]
        logger.info("number facilities with optimal_unmet_demand : {}".format(optimal_unmet_demand_count))
        sql = "select ifnull(sum(variable_value),0) from optimal_solution where variable_name like 'UnmetDemand%';"
        data = db_con.execute(sql)
        optimal_unmet_demand_sum = data.fetchone()[0]
        
        unmet_demand_amount = optimal_unmet_demand_sum
        #===========================================Save parameter========================================================
        np.save("unmet_demand_amount.npy", unmet_demand_amount)
        logger.info("Total Unmet Demand : {}".format(optimal_unmet_demand_sum))
        
        
        logger.info("Penalty per unit of Unmet Demand : ${0:,.0f}".format(the_scenario.unMetDemandPenalty))
        logger.info("Repair costs : ${0:,.0f}".format(repair_costs[j][t][i]))
        logger.info("Total Cost of Unmet Demand : \t ${0:,.0f}".format(
            optimal_unmet_demand_sum * the_scenario.unMetDemandPenalty))
        logger.info("Total Cost of resilience : \t ${0:,.0f}".format(
            float(value(prob.objective)) - initial_cost_daily +repair_costs[j][t][i]))
        
       #=============================================Modification=================================================================
       # The UDP will not be considered when UDR < 0
        if optimal_unmet_demand_sum < 0:
            unit_costs = float(value(prob.objective))+ repair_costs[j][t][i]- optimal_unmet_demand_sum * the_scenario.unMetDemandPenalty
            logger.result(
                "Total Scenario Cost = initial_cost + weight * resilience cost: \t ${0:,.0f}"
                "".format(float(value(prob.objective))- optimal_unmet_demand_sum * the_scenario.unMetDemandPenalty + repair_costs[j][t][i]))
        else:
            unit_costs = float(value(prob.objective))+ repair_costs[j][t][i]
            logger.result(
                "Total Scenario Cost = initial_cost + weight * resilience cost: \t ${0:,.0f}"
                "".format(float(value(prob.objective))+ repair_costs[j][t][i]))
        #===========================================Save parameter========================================================
        np.save("unit_costs.npy", unit_costs)
        #=================================================End=======================================================================


        sql = "select count(variable_name) from optimal_solution where variable_name like 'Edge%';"
        data = db_con.execute(sql)
        optimal_edges_count = data.fetchone()[0]
        logger.info("number of optimal edges: {}".format(optimal_edges_count))

    start_time = datetime.datetime.now()
    logger.info(
        "FINISH: save_pulp_solution: Runtime (HMS): \t{}".format(ftot_supporting.get_total_runtime_string(start_time)))


# ===============================================================================


def parse_optimal_solution_db(the_scenario, logger):
    logger.info("starting parse_optimal_solution")

    optimal_processors = []
    optimal_processor_flows = []
    optimal_route_flows = {}
    optimal_unmet_demand = {}
    optimal_storage_flows = {}
    optimal_excess_material = {}

    with sqlite3.connect(the_scenario.main_db) as db_con:

        # do the Storage Edges
        sql = "select variable_name, variable_value from optimal_solution where variable_name like 'Edge%_storage';"
        data = db_con.execute(sql)
        optimal_storage_edges = data.fetchall()
        for edge in optimal_storage_edges:
            optimal_storage_flows[edge] = optimal_storage_edges[edge]

        # do the Route Edges
        sql = """select
            variable_name, variable_value,
            cast(substr(variable_name, 6) as int) edge_id,
            route_id, start_day time_period, edges.commodity_id,
            o_vertex_id, d_vertex_id,
            v1.facility_id o_facility_id,
         v2.facility_id d_facility_id
            from optimal_solution
            join edges on edges.edge_id = cast(substr(variable_name, 6) as int)
            join vertices v1 on edges.o_vertex_id = v1.vertex_id
        join vertices v2 on edges.d_vertex_id = v2.vertex_id
            where variable_name like 'Edge%_' and variable_name not like 'Edge%_storage';
            """
        data = db_con.execute(sql)
        optimal_route_edges = data.fetchall()
        for edge in optimal_route_edges:

            variable_name = edge[0]

            variable_value = edge[1]

            edge_id = edge[2]

            route_id = edge[3]

            time_period = edge[4]

            commodity_flowed = edge[5]

            od_pair_name = "{}, {}".format(edge[8], edge[9])

            # first time route_id is used on a day or commodity
            if route_id not in optimal_route_flows:
                optimal_route_flows[route_id] = [[od_pair_name, time_period, commodity_flowed, variable_value]]

            else:  # subsequent times route is used on different day or for other commodity
                optimal_route_flows[route_id].append([od_pair_name, time_period, commodity_flowed, variable_value])

        # do the processors
        sql = "select variable_name, variable_value from optimal_solution where variable_name like 'BuildProcessor%';"
        data = db_con.execute(sql)
        optimal_candidates_processors = data.fetchall()
        for proc in optimal_candidates_processors:
            optimal_processors.append(proc)

        # do the processor vertex flows
        sql = "select variable_name, variable_value from optimal_solution where variable_name like 'ProcessorVertexFlow%';"
        data = db_con.execute(sql)
        optimal_processor_flows_sql = data.fetchall()
        for proc in optimal_processor_flows_sql:
            optimal_processor_flows.append(proc)

        # do the UnmetDemand
        sql = "select variable_name, variable_value from optimal_solution where variable_name like 'UnmetDemand%';"
        data = db_con.execute(sql)
        optimal_unmetdemand = data.fetchall()
        for ultimate_destination in optimal_unmetdemand:
            v_name = ultimate_destination[0]
            v_value = ultimate_destination[1]

            search = re.search('\(.*\)', v_name.replace("'", ""))

            if search:
                parts = search.group(0).replace("(", "").replace(")", "").split(",_")

                dest_name = parts[0]
                commodity_flowed = parts[2]
                if not dest_name in optimal_unmet_demand:
                    optimal_unmet_demand[dest_name] = {}

                if not commodity_flowed in optimal_unmet_demand[dest_name]:
                    optimal_unmet_demand[dest_name][commodity_flowed] = int(v_value)
                else:
                    optimal_unmet_demand[dest_name][commodity_flowed] += int(v_value)


    logger.info("length of optimal_processors list: {}".format(len(optimal_processors)))  # a list of optimal processors
    logger.info("length of optimal_processor_flows list: {}".format(
        len(optimal_processor_flows)))  # a list of optimal processor flows
    logger.info("length of optimal_route_flows dict: {}".format(
        len(optimal_route_flows)))  # a dictionary of routes keys and commodity flow values
    logger.info("length of optimal_unmet_demand dict: {}".format(
        len(optimal_unmet_demand)))  # a dictionary of route keys and unmet demand values

    return optimal_processors, optimal_route_flows, optimal_unmet_demand, optimal_storage_flows, optimal_excess_material


# ===============================================================================

#
# def load_lp_problem(the_scenario, logger):
#     logger.info("START: load_lp_problem")
#     pickle_file = os.path.join(the_scenario.scenario_run_directory, "debug", "final_lp_problem.p")
#     reconstituted_lp_file = pickle.load(open(pickle_file, "rb")).copy()
#
#     return reconstituted_lp_file
#
#
# # ===============================================================================
#
#
# def persist_lp_problem(the_scenario, prob, logger):
#     logger.info("START: persist_lp_problem")
#     # pickle the final lp problem object so it can be read back in later
#     pickle.dump(prob, open(os.path.join(the_scenario.scenario_run_directory, "debug", "final_lp_problem.p"), "wb"))
