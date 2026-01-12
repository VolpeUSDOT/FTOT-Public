# -------------------------------------------------------------------------------
# Name:        UDP Sensitivity Analysis Tool
# Purpose:     Performs multiple scenario runs to find the most optimal UDP value 
#               corresponding to user-defined metric thresholds for cost of delivery
# -------------------------------------------------------------------------------

import pandas as pd
import os
import re
from pint import UnitRegistry
import sqlite3
from IPython.display import display
from xml.dom import minidom
import shutil
import csv
import time

# To run script, run C:\FTOT\python3_env\python.exe C:\FTOT\program\tools\ftot_tools.py      C:\FTOT\scenarios\marine_fuels\Seattle\MSW


# ======================================================================================================================


# Define FTOT filepaths
PYTHON = r"C:\FTOT\python3_env\python.exe"
FTOT = r"C:\FTOT\program\ftot.py"

# Define unit registry variables
ureg = UnitRegistry()
Q_ = ureg.Quantity
ureg.define('thousand_gallon = kgal')
ureg.define('usd = [currency]')


# ==============================================================================


def get_scenario_dir():
    print("Provide a directory path for the FTOT scenario you would like to run")
    scenario_dir = ""
    scenario_dir = input('----------------------> ')
    print(f"USER INPUT: the scenario directory path provided is {scenario_dir}")

    if not os.path.exists(scenario_dir):
        raise Exception("This scenario directory path does not exist. Please specify an existing scenario directory path.")

    return scenario_dir


# ==============================================================================


def get_commodity_name(scen_path):
    print("Provide the commodity name to obtain metrics for")
    commodity_name = ""
    commodity_name = input('----------------------> ')
    print(f"USER INPUT: commodity name provided is {commodity_name}")

    XMLSCENARIO = os.path.join(scen_path, 'scenario.xml')
    xmlScenarioFile = minidom.parse(XMLSCENARIO)
    rmp_csv = xmlScenarioFile.getElementsByTagName('RMP_Commodity_Data')[0].firstChild.data
    dest_csv = xmlScenarioFile.getElementsByTagName('Destinations_Commodity_Data')[0].firstChild.data

    input_data_commodities = set()
    for csv_file in [dest_csv, rmp_csv]:
        with open(csv_file, 'rt', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                input_data_commodities.add(row['commodity'].lower())
                
    if commodity_name.lower() not in input_data_commodities:
        raise Exception(f"The commodity name is not in the input CSV data. Please specify one of the commodities in your input data: {input_data_commodities}")

    return commodity_name


# ==============================================================================


def get_metrics(commodity_name):
    print(f"Provide a metric lower bound threshold (in US dollars per unit of {commodity_name})")
    metric_lower_bound = ""
    metric_lower_bound = input('----------------------> ')
    print(f"USER INPUT: metric lower bound provided is {metric_lower_bound}")

    try: 
        metric_lower_bound = float(metric_lower_bound)
    except: 
        raise Exception("metric lower bound needs to be a numerical value")

    print(f"Provide a metric upper bound threshold (in US dollars per unit of {commodity_name})")
    metric_upper_bound = ""
    metric_upper_bound = input('----------------------> ')
    print(f"USER INPUT: metric upper bound provided is {metric_upper_bound}")

    try: 
        metric_upper_bound = float(metric_upper_bound)
    except: 
        raise Exception("metric upper bound needs to be a numerical value")

    return metric_lower_bound, metric_upper_bound


# ==============================================================================


def get_metric_flow_unit(scen_path, commodity_name):
    print("Provide the unit of the finished product delivered to the destination")
    metric_flow_unit = ""
    metric_flow_unit = input('----------------------> ')
    print(f"USER INPUT: finished product unit provided is {metric_flow_unit}")

    XMLSCENARIO = os.path.join(scen_path, 'scenario.xml')
    xmlScenarioFile = minidom.parse(XMLSCENARIO)
    rmp_csv = xmlScenarioFile.getElementsByTagName('RMP_Commodity_Data')[0].firstChild.data
    dest_csv = xmlScenarioFile.getElementsByTagName('Destinations_Commodity_Data')[0].firstChild.data

    input_data_commodities = dict()
    for csv_file in [dest_csv, rmp_csv]:
        with open(csv_file, 'rt', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                input_data_commodities[row['commodity'].lower()] = row['phase_of_matter'].lower()
    
    if not Q_(metric_flow_unit).check('[mass]'): # if liquid, must meet input data's phase of matter
        if input_data_commodities[commodity_name.lower()] != 'liquid':
            raise Exception(f"The specified unit must be a unit of mass to match the commodity.")
    else: # if solid
        if input_data_commodities[commodity_name.lower()] != 'solid':
            raise Exception(f"The specified unit must be a unit of volume to match the commodity.")

    return metric_flow_unit


# ==============================================================================


def get_max_scenario_runs():
    print("Provide the maximum number of scenario runs (recommend around 20 to 30)")
    max_scenario_runs = ""
    max_scenario_runs = input('----------------------> ')
    print(f"USER INPUT: maximum number of scenario runs provided is {max_scenario_runs}")

    try: 
        max_scenario_runs = int(max_scenario_runs)
    except:
        raise Exception("maximum number of scenario runs needs to be an integer")

    return max_scenario_runs


# ==============================================================================


def check_step_run_log(step, scen_path):
    log_path = os.path.join(scen_path, 'logs')
    log_list = []
    for log in os.listdir(log_path):
        if re.match(f'{step}_', log):
            log_list.append(log)
    latest_log = log_list[len(log_list)-1]

    # checks the last line of the each log and makes sure it gets the time
    total_flow_pattern = fr"{step} Step - Total Runtime \(HMS\): 	([0-9]+(:[0-9]+)+)"
    with open(os.path.join(log_path, latest_log), 'r') as textfile:
        for line in textfile:
            match = re.search(total_flow_pattern, line)
            if match:
                return 
    # if no matches are found
    raise Exception(f"Something may be wrong with the {step}-log file. Please check that log for errors!")

    return

# ==============================================================================


def run_all_steps(XMLSCENARIO, PYTHON, FTOT, scen_path, db_path, scaling_factor):
    """
    Run the FTOT model from the 'S' optimization steps on

    Runs s, f, c, g, o1, o2, p, d, and m steps. May run oc, f2, c2, and g2 steps if needed.
    This uses the output of an existing FTOT run, in particular the
    main.db, and re-runs the optimization, post-processing, reporting, and mapping steps. Not run are
    the setup, facilities, connectivity, and graph steps.

    s: SETUP: SETUP FTOT FOR THE SCENARIO
    f: FACILITIES: ADD FACILITIES LOCATIONS AND COMMODITY DATA FILES TO THE SCENARIO
    c: CONNECTIVITY: CONNECT THE FACILITIES TO THE NETWORK AND EXPORT FROM GIS TO NETWORKX MODULE
    g: GRAPH: CREATE THE NETWORKX GRAPHS FROM THE NETWORK AND FACILITIES
    oc: OPTIMIZATION: PRE-CANDIDATE GENERATION OPTIMIZATION
    o1: STEP OPTIMIZATION: SET UP THE OPTIMIZATION PROBLEM
    o2: STEP OPTIMIZATION: BUILD THE OPTIMIZATION PROBLEM AND SOLVE
    p: STEP POST-PROCESSING
    d: STEP REPORTING
    m: STEP MAPPING
    """

    # Initialize list 
    steps_to_run = ['s', 'f', 'c', 'g']

    # Add in candidate generation steps if proc_cand csv filepath exists in xml
    xmlScenarioFile = minidom.parse(XMLSCENARIO)
    if xmlScenarioFile.getElementsByTagName('Processors_Candidate_Commodity_Data')[0].firstChild.data.lower() != "none":
        steps_to_run.extend(['oc','f2', 'c2', 'g2'])

    steps_to_run.extend(['o1', 'o2', 'p', 'd', 'm'])

    for step in steps_to_run:
        print(f"Running {step}")

        cmd = PYTHON + ' ' + FTOT + ' ' + XMLSCENARIO + ' ' + step
        os.system(cmd)

        # create original table and update udp in udp_sensitivity_facility_commodities using scaling factor
        if step == 'f':
            initialize_original_udp_table(db_path)
            update_facilities_udp_with_scalar(scaling_factor, db_path)

        # after f and f2 step, update udp in facility_commodities
        if (step == 'f') or (step == 'f2'):
            with sqlite3.connect(db_path) as db_con:
                cursor = db_con.cursor()
                # copy the scaled udp score over to facility_commodities
                cursor.execute(f"""update facility_commodities
                    set udp = (
                        select udp from udp_sensitivity_facility_commodities
                        WHERE udp_sensitivity_facility_commodities.location_id = facility_commodities.location_id
                        AND udp_sensitivity_facility_commodities.commodity_id = facility_commodities.commodity_id
                );""")

        check_step_run_log(step, scen_path)

    return

# ==============================================================================


def update_facilities_udp_with_scalar(scaling_factor, db_path):
    with sqlite3.connect(db_path) as db_con:
        cursor = db_con.cursor()

        # udp column is original udp column * scaling factor
        cursor.execute(f"""update udp_sensitivity_facility_commodities
                    set udp = original_udp * {scaling_factor};""")     
    
    return

# ==============================================================================


def retrieve_flow_unit_delivered(scen_path, commodity_name):
    # use most recent p log file to extract total flow all modes for specified commodity_name
    log_path = os.path.join(scen_path, 'logs')
    log_list = []
    for log in os.listdir(log_path):
        if re.match('p_', log):
            log_list.append(log)
    latest_log = log_list[len(log_list)-1]

    # get the total flow of commodity
    total_flow_pattern = fr'(?:RESULT   COMMODITY_SUMMARY_{commodity_name.upper()}_TOTAL_FLOW_ALLMODES:\s+)(\d+(?:,\d+)*(?:\.\d+)?)\s*:\s*(\w+)'

    with open(os.path.join(log_path, latest_log), 'r') as textfile:
        for line in textfile:
            match = re.search(total_flow_pattern, line)
            if match:
                # extract number and take out commas
                number = match.group(1).replace(',', '')
                units = match.group(2)
                total_flow_delivered = f"{number} {units}"

                return total_flow_delivered # string of number and flow unit
            
    return None


# ==============================================================================


def retrieve_total_transport_cost(scen_path):
    # use most recent reports.txt file to extract total transport cost for all modes
    report_path = os.path.join(scen_path, 'Reports')
    report_list = []
    for report in os.listdir(report_path):
        if report.startswith("reports"):
            report_list.append(report)
    latest_report = report_list[-1]
    latest_report_file = f"report_{latest_report[8:]}.txt"
    
    transport_cost_pattern = r'P\s+:\s+TRANSPORT_COST_ALLMODES:\s*(\d+(?:,\d+)*(?:\.\d+)?)\s*:\s*(\w+)' # extracts every number before/after commas and periods
    with open(os.path.join(report_path, latest_report, latest_report_file), 'r') as textfile:
        for line in textfile:
            match = re.search(transport_cost_pattern, line)
            if match:
                # extract number and take out commas
                number = match.group(1).replace(',', '')
                units = match.group(2)
                transport_cost = f"{number} {units}"
                
                return transport_cost # string of number and currency unit
            
    return None


# ==============================================================================


def check_metric_and_update_scaling_factor(metric, metric_lower_bound, metric_upper_bound, scaling_factor, scaling_factor_lower_bound = 0, scaling_factor_upper_bound = 10000):
    # update the scaling factor that multiplies the udp depending on if the metric was higher or lower than what we want
    if metric is not None:
        # just returns same scaling factor if threshold is met
        if metric < metric_upper_bound and metric > metric_lower_bound:
            print(f"We found a metric that meets the threshold! The metric is {metric} where scaling factor is {scaling_factor}.")
            return scaling_factor, scaling_factor_lower_bound, scaling_factor_upper_bound
        else: 
            # set higher udp if metric is too high
            if metric > metric_upper_bound:
                print(f"metric is higher than metric upper bound. decreasing upper bound scaling factor from {scaling_factor_upper_bound} to {scaling_factor}.")
                scaling_factor_upper_bound = scaling_factor
            # lower the udp if metric too low
            elif metric < metric_lower_bound:
                print(f"metric is lower than metric lower bound. increasing lower bound scaling factor from {scaling_factor_lower_bound} to {scaling_factor}.")
                scaling_factor_lower_bound = scaling_factor

            scaling_factor = (scaling_factor_lower_bound + scaling_factor_upper_bound) / 2
    else:
        # if metric is no flow (None), raise the udp
        print(f"metric has a no flow solution. increasing lower bound scaling factor from {scaling_factor_lower_bound} to {scaling_factor}.")
        scaling_factor_lower_bound = scaling_factor
        scaling_factor = (scaling_factor_lower_bound + scaling_factor_upper_bound) / 2

    return scaling_factor, scaling_factor_lower_bound, scaling_factor_upper_bound


# ==============================================================================


def rename_report_and_map_directories(scen_path, num_scenario_runs):
    # rename the report folder
    report_path = os.path.join(scen_path, 'Reports')
    report_list = []
    for report in os.listdir(report_path):
        if report.startswith("reports"):
            report_list.append(report)
    old_report = report_list[-1]
    new_report_dir = f"report_step{num_scenario_runs}_{old_report[8:]}"

    # remove the old report_{num_scenario_runs} directory
    if os.path.exists(os.path.join(report_path, new_report_dir)):
        shutil.rmtree(os.path.join(report_path, new_report_dir))

    os.rename(os.path.join(report_path, old_report),
              os.path.join(report_path, new_report_dir))

    # rename the map folder
    map_path = os.path.join(scen_path, 'Maps')
    maps_list = []
    for map_dir in os.listdir(map_path):
        if map_dir.startswith("default_basemap_maps"):
            maps_list.append(map_dir)
    old_map_dir = maps_list[-1]
    new_map_dir = f"map_step{num_scenario_runs}_{old_map_dir[21:]}"

    # remove the old map_{num_scenario_runs} directory
    if os.path.exists(os.path.join(map_path, new_map_dir)):
        shutil.rmtree(os.path.join(map_path, new_map_dir))

    os.rename(os.path.join(map_path, old_map_dir),
              os.path.join(map_path, new_map_dir))
    
    return

# ==============================================================================

def rename_main_gdb(scen_path, num_scenario_runs):
    # rename the gdb folder
    gdb_path = os.path.join(scen_path, 'main.gdb')
    new_gdb_name = f"step_{num_scenario_runs}_main.gdb"
    new_gdb_path = os.path.join(scen_path, new_gdb_name)

    # remove the step_X_main.gdb directories for re-runs
    if os.path.exists(new_gdb_path):
        shutil.rmtree(new_gdb_path)

    # rename main.gdb to step_X_main.gdb
    os.rename(gdb_path, new_gdb_path)

    return

# ==============================================================================

def rename_main_db(scen_path, num_scenario_runs):
    # rename the db
    db_path = os.path.join(scen_path, 'main.db')
    new_db_name = f"step_{num_scenario_runs}_main.db"
    new_db_path = os.path.join(scen_path, new_db_name)

    # remove the step_X_main.db directories for re-runs
    if os.path.exists(new_db_path):
        os.remove(new_db_path)

    # rename main.db to step_X_main.db
    os.rename(db_path, new_db_path)

    return

# ==============================================================================
 

def initialize_original_udp_table(db_path):
    with sqlite3.connect(db_path) as db_con:
        cursor = db_con.cursor()

        # creating new table since the candidate steps rewrites the facility commodities table
        cursor.execute("create table udp_sensitivity_facility_commodities as select * from facility_commodities")
        cursor.execute("alter table udp_sensitivity_facility_commodities add column original_udp text;")
        cursor.execute("update udp_sensitivity_facility_commodities set original_udp = udp;")

    return

# ==============================================================================


def run_sensitivity_tool():
    # INITIALIZATION STEP
    scen_path = get_scenario_dir()
    XMLSCENARIO = os.path.join(scen_path, 'scenario.xml')
    db_path = os.path.join(scen_path, 'main.db')

    # store inputs as variables
    commodity_name = get_commodity_name(scen_path)
    metric_lower_bound, metric_upper_bound = get_metrics(commodity_name)
    max_scenario_runs = get_max_scenario_runs()
    metric_flow_unit = get_metric_flow_unit(scen_path, commodity_name)

    # hard coded inputs
    metric_currency_unit = 'usd'
    num_times_to_meet_threshold = 3
    num_scenario_runs = 1

    scaling_factor_lower_bound = 0
    scaling_factor_upper_bound = 10000
    
    # For udp updating - this always updates based on metric
    scaling_factor = 1

    # Initialized variables to find the lowest metric for the highest amount of flow
    best_metric = 99999
    best_flow = -1 

    # For Results.csv
    results_df = pd.DataFrame(columns=['scenario_run_number',
                                       'total_transport_cost',
                                       'total_flow_delivered',
                                       'metric'])

    # RUN TOOL STEP
    while num_scenario_runs <= max_scenario_runs:
        # Add delay to ensure connections are fully closed
        time.sleep(1)

        run_all_steps(XMLSCENARIO, PYTHON, FTOT, scen_path, db_path, scaling_factor)

        transport_cost = retrieve_total_transport_cost(scen_path)
        total_flow_delivered = retrieve_flow_unit_delivered(scen_path, commodity_name)

        if transport_cost is not None and total_flow_delivered is not None:
            # convert each to specified units
            metric_with_units = Q_(transport_cost).to(metric_currency_unit) / Q_(total_flow_delivered).to(metric_flow_unit)
            metric = metric_with_units.magnitude
        elif transport_cost is not None or total_flow_delivered is not None: # if either aren't None
            raise Exception("WARNING: only one of transport cost or total flow delivered is non-null - something may be off with retrieving one of these values. Check the p-log file or the report.txt file.")
        else:
            metric = None

        # append measures to results_df
        z1 = [num_scenario_runs, transport_cost, total_flow_delivered, scaling_factor, metric]
        results_df_step = pd.DataFrame([z1], columns = ['scenario_run_number', 'total_transport_cost',
                                                        'total_flow_delivered', 'scaling_factor', 'metric'])
        results_df = pd.concat([results_df, results_df_step])

        results_df.to_csv(os.path.join(scen_path, 'Results.csv'), index = False)

        # Show the results_df for the current scenario run
        print(results_df.iloc[[-1]])
        
        # rename directory names
        rename_report_and_map_directories(scen_path, num_scenario_runs)

        # Comment out to use for debugging purposes
        # rename_main_gdb(scen_path, num_scenario_runs)
        # rename_main_db(scen_path, num_scenario_runs)

        
        # If the scaling factor does not change from the check_metric_and_update_scaling_factor function, then the metric threshold is met
        old_scaling_factor = scaling_factor
        scaling_factor, scaling_factor_lower_bound, scaling_factor_upper_bound = check_metric_and_update_scaling_factor(metric, metric_lower_bound, metric_upper_bound, scaling_factor, scaling_factor_lower_bound, scaling_factor_upper_bound)

        # If we're within the goal metric bounds:
        if old_scaling_factor == scaling_factor:
            num_times_to_meet_threshold -= 1

            # the higher flow will be automatically set as best_transport_cost
            if Q_(total_flow_delivered).to(metric_flow_unit).magnitude > best_flow:
                best_flow = Q_(total_flow_delivered).to(metric_flow_unit).magnitude
                best_transport_cost = transport_cost
                best_metric = metric
                best_scenario_step = num_scenario_runs
            # if the current flow matches best flow, then keep lower metric/transport cost
            elif Q_(total_flow_delivered).to(metric_flow_unit).magnitude == best_flow and metric < best_metric:
                best_flow = Q_(total_flow_delivered).to(metric_flow_unit).magnitude
                best_transport_cost = transport_cost
                best_metric = metric
                best_scenario_step = num_scenario_runs

            if num_times_to_meet_threshold == 0:
                print("We hit the limit on meeting the threshold. Exiting loop.")
                break
            
            # run iterations after meeting threshold to see if we can get slightly higher metric
            print(f"running extra iterations to see if there's a better metric. increasing lower bound scaling factor from {scaling_factor_lower_bound} to {scaling_factor}.")
            scaling_factor_lower_bound = scaling_factor
            scaling_factor = (scaling_factor_lower_bound + scaling_factor_upper_bound) / 2

        num_scenario_runs += 1

        if num_scenario_runs > max_scenario_runs:
            print("\nFinal Result:")
            print(f"We did not manage to find an optimal routing solution in your specified interval.")
    
    # if we did get within the metrics then print optimal results
    if best_flow > 0:
        print("\nFinal Result:")
        print(f"In your specified interval, {best_transport_cost} is the lowest transport cost with the maximum amount delivered ({best_flow} {metric_flow_unit}).")
        print(f"Refer to step {best_scenario_step} for the optimal map and report.")
            

# ==============================================================================


def run():
    os.system('cls')
    print("FTOT UDP Sensitivity Analysis Tool")
    print("-------------------------------")
    print("")
    print("")
    run_sensitivity_tool()