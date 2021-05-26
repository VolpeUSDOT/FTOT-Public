# Test Code used to test the PuLP Optimization of AFTOT.
import sys
if  'xtot_objects' in sys.modules:
    del(sys.modules["xtot_objects"])

if  'aftot_pulp' in sys.modules:
    del(sys.modules["aftot_pulp"])

#if  'aftot_supporting' in sys.modules:
#   del(sys.modules["aftot_supporting"])


import os
import xtot_objects
import ftot_supporting
#import ftot_pulp


days = 1
# ==============Constants to define scenario=================
scenario = 6
# ===================================
def schedule_full_availability():
    availability = []
    # adding a 0 as the first entry in availabiltiy so that the list index corresponds to day
    availability.append(0)
    for x in range(1, days+1):
        availability.append(1)
##    start_day = 1
##    end_day = days
##    s = xtot_objects.Schedule(start_day, end_day, availability)
    s = xtot_objects.Schedule(1, days, availability)

    return s

def schedule_last_day_only():
    #meant to provide demand on the last day of a scenario only to test storage
    availability = []
    # adding a 0 as the first entry in availabiltiy so that the list index corresponds to day
    availability.append(0)
    for x in range(1, days):
        availability.append(0)
    for x in range(days, days+1):
        availability.append(1)
##    start_day = 1
##    end_day = days
##    s = xtot_objects.Schedule(start_day, end_day, availability)
    s = xtot_objects.Schedule(1, days, availability)

    return s

def schedule_partial_availability():
    availability = []
    # adding a 0 as the first entry in availabiltiy so that the list index corresponds to day
    availability.append(0)
    for x in range(1, days+1):
        if x not in (1,2,7,8):
            availability.append(1)
        else:
            availability.append(0)
    s = xtot_objects.Schedule(1, days, availability)

    return s


# @profile
def run_test_pulp_optimization():

    #below are calls to internal functions to generate all the facilities
    # and routes required to run a test simulation of the AFTOT without
    # requiring GIS.

    # initialize logger
    # CRITICAL       50
    # ERROR          40
    # WARNING        30
    # RESULT         25
    # CONFIG         24
    # INFO           20
    # DEBUG          10
    # DETAILED_DEBUG  5
    logger = ftot_supporting.create_loggers(os.getcwd(), "test_pulp")
    # logger.setLevel(logging.DEBUG)
##    logger.basicConfig(filemode='w', level=logging.DEBUG)

    # generate all test facilities
    logger.info("START: test_create_all_facilities")
    all_facilities = _test_create_all_facilities(logger)
    #assumptions_logger.write("All facilities: \n")
    #for f in all_facilities:
    #    assumptions_logger.write("{} \n".format(f.asText()))
##        if type(f).__name__ == "Intermediate_Processing_Facility":
##            assumptions_logger.write("corn quantity {}".format(f.getQuantity(logger, "corn")))
##            assumptions_logger.write("gasoline quantity {}".format(f.getQuantity(logger, "gasoline")))

    # get all_routes from route cache
    logger.info("START: _test_create_all_routes")
    if scenario == 1:
        all_routes = _test_create_all_routes(all_facilities)
    elif scenario == 2:
        all_routes = _test_create_all_routes_2(all_facilities)
    elif scenario == 3:
        all_routes = _test_create_all_routes_3(all_facilities)
    elif scenario == 4:
        all_routes = _test_create_all_routes_2(all_facilities)
    elif scenario == 5:
        all_routes = _test_create_all_routes_5(all_facilities)
    elif scenario == 6:
        all_routes = _test_create_all_routes_6(all_facilities)
    else:
        all_routes = _test_create_all_routes(all_facilities)
    #logger.debug("all routes {}".format(all_routes))

    # convert facilities to vertifices and routes to edges
    logger.info("START: pre_setup_pulp")
    all_routes, all_edges, vertex_edge_dictionary = \
    aftot_pulp.pre_setup_pulp(logger, all_facilities, all_routes)

    logger.info("START: setup_pulp_problem")
    prob, flow_vars, unmet_demand_vars = aftot_pulp.setup_pulp_problem(logger, \
    all_edges, vertex_edge_dictionary)
#    print "finished running setup_pulp_problem"

    # call solve_pulp_problem()
    logger.info("START: solve_pulp_problem")
    # solution = aftot_pulp.solve_pulp_problem(prob, logger)
    aftot_pulp.solve_pulp_problem(prob, flow_vars, unmet_demand_vars, all_edges, logger)

    # call PuLp post-processing
    logger.info("POST PROCESSING HOLDER")
    aftot_route.post_processing()
    #assumptions_logger.close()
    pass

#===============================================================================
def _test_create_all_facilities(logger):
    # create the final destination "alpha"
    destination_1 = _test_create_destination_alpha_object(logger)

    #create the supply facility "beta"
    supply_1, supply_2, supply_3 = _test_create_supply_beta_object(logger)
    # supply_2 only creates corn, requires a processor

    intermediate_1, intermediate_2, intermediate_3 = _test_create_intermediate_storage_facility(logger)

    processor_1, processor_2 = _test_create_processor_facility(logger)

    all_facilities = [supply_1, destination_1, intermediate_1]
    all_facilities_2 = [supply_1, destination_1, intermediate_1, intermediate_2]
    all_facilities_4 = [supply_1, destination_1, intermediate_1, intermediate_3]

    if scenario == 1:
        return all_facilities
    elif scenario ==2:
        return all_facilities_2
    elif scenario ==3:
        all_facilities_3 = [supply_2, destination_1, intermediate_1, intermediate_2, processor_1]
        return all_facilities_3
    elif scenario ==4:
        return all_facilities_4
    elif scenario ==5:
        all_facilities_5 = [supply_2, supply_3, intermediate_1, intermediate_2, processor_1, processor_2, destination_1]
        return all_facilities_5
    elif scenario ==6:
        all_facilities_6 = [supply_2, processor_1, destination_1]
        return all_facilities_6
    else:
        return all_facilities

def _test_create_commodity(name, units):
    return xtot_objects.Commodity(name, units)


#===============================================================================
# creates a demand object for the final_desitnation object
#===============================================================================


def _test_create_final_destination_slate():
    product_dict = {}

    gasoline_demand = 250000
    gasoline_units ="bbl"
    gasoline_udp = 10
    product_dict["gasoline"] = (xtot_objects.Commodity("gasoline", gasoline_units),
    gasoline_demand, gasoline_udp)

    diesel_demand = 150000
    diesel_units = "bbl"
    diesel_udp = 10
    product_dict["diesel"] = (xtot_objects.Commodity("diesel", diesel_units),
    diesel_demand, diesel_udp)

    return xtot_objects.Final_Destination_Slate(product_dict)

#===============================================================================
# destination_facility_object
#===============================================================================
# %%
def _test_create_destination_alpha_object(logger):

    schedule = schedule_full_availability()
    logger.info("Created a demand  schedule with {} days".format(days))



    # create a facility object
    facility_name       = "Destination_ALPHA"
    facility_description = "a test destination object."
    facility_location   = "EARTH"
    facility_candidate  = 0
    facility_capex      = 0
    facility_slate = _test_create_final_destination_slate()
    facility_nameplate_capacity = 1000 #refers to co-located storage
    facility_nameplate_capacity_units = "bbl"
    f = xtot_objects.Facility(facility_name,
                                facility_description,
                                facility_location,
                                facility_slate,
                                facility_nameplate_capacity,
                                facility_nameplate_capacity_units,
                                schedule)


    # create a final destination facility
    d = xtot_objects.Final_Destination(f)


    # logger.info("Created a destination facility {}".format(d.asText()))
    # print(d.asText())

    return d

# =============================================================================
# creates a "preprocessor" facility
# =============================================================================


def _test_create_feedstock_slate_1(logger):
    feedstock_dict = {}

    gasoline_supply = 100000
    gasoline_units ="bbl"
    feedstock_dict["gasoline"] = (xtot_objects.Commodity("gasoline", gasoline_units), gasoline_supply)

    diesel_supply = 100000
    diesel_units = "bbl"
    feedstock_dict["diesel"] = (xtot_objects.Commodity("diesel", diesel_units), diesel_supply)

    #logger.info(feedstock_dict)
    p_slate = xtot_objects.PreProcessor_Slate(feedstock_dict)
    #logger.info(p_slate.asText())
    #logger.info(p_slate)

    return p_slate

def _test_create_feedstock_slate_2(logger):
    feedstock_dict = {}

    corn_supply = 100000
    corn_units ="tons"
    feedstock_dict["corn"] = (xtot_objects.Commodity("corn", corn_units), corn_supply)

    wheat_supply = 100000
    wheat_units = "tons"
    feedstock_dict["wheat"] = (xtot_objects.Commodity("wheat", wheat_units), wheat_supply)

    p_slate = xtot_objects.PreProcessor_Slate(feedstock_dict)

    return p_slate

def _test_create_feedstock_slate(logger, commodity_name_1, units_name_1, daily_supply_1, commodity_name_2, units_name_2, daily_supply_2):
    feedstock_dict = {}

    feedstock_dict[commodity_name_1] = (xtot_objects.Commodity(commodity_name_1, units_name_1), daily_supply_1)
    feedstock_dict[commodity_name_2] = (xtot_objects.Commodity(commodity_name_2, units_name_2), daily_supply_2)

    p_slate = xtot_objects.PreProcessor_Slate(feedstock_dict)

    return p_slate

def _test_create_supply_beta_object(logger):

    # create an arbitrary scheudle.
    schedule = schedule_full_availability()
    logger.info("Created a supply schedule with {} days".format(days))

    #print(f.asText())

    # create a facility object
    facility_name_1       = "Supply_Beta"
    facility_description = "a test supply object produces gas and diesel"
    facility_location   = "EARTH"
    # facility_candidate  = 0 default
    # facility_capex      = 0 default
    facility_feedstock_slate_1 = _test_create_feedstock_slate_1(logger)
    facility_nameplate_capacity = 1000 #refers to colocated storage
    facility_nameplate_capacity_units = "bbl"
    facility_schedule = schedule
    f = xtot_objects.Facility(facility_name_1,
                                facility_description,
                                facility_location,
                                facility_feedstock_slate_1,
                                facility_nameplate_capacity,
                                facility_nameplate_capacity_units,
                                facility_schedule)


    # create a supply facility
    gd = xtot_objects.PreProcessor(f)

    facility_name_2       = "Supply_A"
    facility_description = "a test supply object produces corn and wheat"
    facility_feedstock_slate_2 = _test_create_feedstock_slate_2(logger)
    g = xtot_objects.Facility(facility_name_2,
                                facility_description,
                                facility_location,
                                facility_feedstock_slate_2,
                                facility_nameplate_capacity,
                                facility_nameplate_capacity_units,
                                facility_schedule)


    cw = xtot_objects.PreProcessor(g)

    facility_name_3       = "Supply_C"
    facility_description = "a test supply object produces corn & soy"
    facility_location   = "EARTH"
    facility_feedstock_slate_3 = _test_create_feedstock_slate(logger, "corn", "tons", 10000, "soy", "tons", 20000)
    facility_nameplate_capacity = 1000 #refers to colocated storage
    facility_nameplate_capacity_units = "bbl"
    facility_schedule = schedule
    h = xtot_objects.Facility(facility_name_3,
                                facility_description,
                                facility_location,
                                facility_feedstock_slate_3,
                                facility_nameplate_capacity,
                                facility_nameplate_capacity_units,
                                facility_schedule)


    # create a supply facility
    r = xtot_objects.PreProcessor(h)

    return gd, cw, r

# =============================================================================
# creates a "intermediate" facility that doesnt convert anything
# vis a vis a storage facility
# =============================================================================
def _test_create_storage_slate():
    commodity_dict = {}
    commodity_dict["gasoline"] = xtot_objects.Commodity("gasoline", "bbl")
    commodity_dict["diesel"] = xtot_objects.Commodity("diesel", "bbl")
    commodity_dict["corn"] = xtot_objects.Commodity("corn", "tons")
    commodity_dict["wheat"] = xtot_objects.Commodity("wheat", "tons")
    slate = xtot_objects.Storage_Slate(commodity_dict)

    return slate

def _test_create_storage_slate_feedstock_only():
    commodity_dict = {}
    commodity_dict["soy"] = xtot_objects.Commodity("soy", "tons")
    commodity_dict["corn"] = xtot_objects.Commodity("corn", "tons")
    commodity_dict["wheat"] = xtot_objects.Commodity("wheat", "tons")
    slate = xtot_objects.Storage_Slate(commodity_dict)

    return slate

def _test_create_intermediate_storage_facility(logger):

    # create an arbitrary scheudle for the facility
    schedule =  schedule_full_availability()
    logger.info("Created a test storage schedule with {} days".format(days))

    partial_schedule = schedule_partial_availability()
    logger.info("Created a test storage schedule with {} days".format(partial_schedule.total_available_days))
    logger.debug(partial_schedule.asText())



    # create supply objects for each commodity
	# i don' think we want to create supply and demand objects at intermediate facilities, only processing ratios
    # I don't think there needs to be a schedule for each commodity at an intermediate facility
    # feedstock and product may have schedules, commodity does not
    # specify the name and supply

    # create a facility object
    facility_name       = "Int_Storage_GAMMA"
    facility_description = "a test storage object. requires 1000 bbl each of diesel and gasoline at a time"
    facility_location   = "EARTH"
    # facility_candidate  = 0 default
    # facility_capex      = 0
    facility_storage_slate = _test_create_storage_slate()
    feedstock_storage_slate = _test_create_storage_slate_feedstock_only()
    facility_nameplate_capacity = 1000
    facility_nameplate_capacity_units = "bbl"
    facility_schedule = schedule
    f = xtot_objects.Facility(facility_name,
                                facility_description,
                                facility_location,
                                facility_storage_slate,
                                facility_nameplate_capacity,
                                facility_nameplate_capacity_units,
                                facility_schedule)

    g = xtot_objects.Facility("Int_Storage_DELTA",
                                facility_description,
                                facility_location,
                                feedstock_storage_slate,
                                facility_nameplate_capacity,
                                "tons",
                                facility_schedule)

    h = xtot_objects.Facility("Int_Storage_H",
                                facility_description,
                                facility_location,
                                facility_storage_slate,
                                facility_nameplate_capacity,
                                facility_nameplate_capacity_units,
                                partial_schedule)

    # create a supply facility
    s = xtot_objects.Storage(f)
    t = xtot_objects.Storage(g)
    u = xtot_objects.Storage(h)

    return s, t, u
#====================================================================
# create processor to turn corn and/or wheat into gas and/or diesel
#====================================================================

def _test_create_processor_slate():
    # creates 2bbl gasoline and 3 bbl diesel from 5 tons corn and 1 ton wheat
    output_commodity_dict = {}
    output_commodity_dict["gasoline"] = (xtot_objects.Commodity("gasoline", "bbl"), 2.0)
    output_commodity_dict["diesel"] = (xtot_objects.Commodity("diesel", "bbl"), 3.0)

    input_commodity_dict = {}
    input_commodity_dict["corn"] = (xtot_objects.Commodity("corn", "tons"), 5.0)
    input_commodity_dict["wheat"] = (xtot_objects.Commodity("wheat", "tons"), 1.0)
    slate = xtot_objects.Processor_Slate(input_commodity_dict, output_commodity_dict)
    return slate

def _test_create_processor_slate_soy():
    # creates 2bbl gasoline and 3 bbl diesel from 5 tons corn and 1 ton wheat
    output_commodity_dict = {}
    output_commodity_dict["gasoline"] = (xtot_objects.Commodity("gasoline", "bbl"), 3.0)
    output_commodity_dict["diesel"] = (xtot_objects.Commodity("diesel", "bbl"), 2.0)

    input_commodity_dict = {}
    input_commodity_dict["corn"] = (xtot_objects.Commodity("corn", "tons"), 4.0)
    input_commodity_dict["soy"] = (xtot_objects.Commodity("soy", "tons"), 1.0)
    slate = xtot_objects.Processor_Slate(input_commodity_dict, output_commodity_dict)
    return slate

#=========================================================================


def _test_create_processor_facility(logger):

    # create an arbitrary scheudle for the facility
    schedule =  schedule_full_availability()
    logger.debug("Created a processor schedule with {} days".format(days))
    partial_schedule = schedule_partial_availability()
    logger.debug(partial_schedule.asText())



    # create supply objects for each commodity
	# i don' think we want to create supply and demand objects at intermediate facilities, only processing ratios
    # I don't think there needs to be a schedule for each commodity at an intermediate facility
    # feedstock and product may have schedules, commodity does not
    # specify the name and supply
    # may need to add separate capacity and units for input storage, currently only
    # specifiying output storage

    # create a facility object
    facility_name       = "Processor_P"
    facility_description = "a test processor object. requires 1000 bbl each of diesel and gasoline at a time"
    facility_location   = "EARTH"
    facility_processor_slate = _test_create_processor_slate()
    facility_nameplate_capacity = 1000
    facility_nameplate_capacity_units = "bbl"
    facility_schedule = schedule
    f = xtot_objects.Facility(facility_name,
                                facility_description,
                                facility_location,
                                facility_processor_slate,
                                facility_nameplate_capacity,
                                facility_nameplate_capacity_units,
                                facility_schedule)


    # create a supply facility
    p = xtot_objects.Intermediate_Processing_Facility(f)

    # create a facility object
    facility_name       = "Processor_Q"
    facility_description = "a test processor object. requires 1000 bbl each of diesel and gasoline at a time"
    facility_location   = "EARTH"
    facility_candidate  = 0
    facility_capex      = 0
    facility_processor_slate = _test_create_processor_slate_soy()
    facility_nameplate_capacity = 1000
    facility_nameplate_capacity_units = "bbl"
    facility_schedule = schedule
    g = xtot_objects.Facility(facility_name,
                                facility_description,
                                facility_location,
                                facility_processor_slate,
                                facility_nameplate_capacity,
                                facility_nameplate_capacity_units,
                                facility_schedule)


    # create a supply facility
    q = xtot_objects.Intermediate_Processing_Facility(g)


    return p, q

## log when setting up processor
##    print "setting up processor slate"
##    print type(slate).__name__
##    print slate.commodities
##    print slate.AllAllowedCommodities()
##    return slate
#===============================================================================


def _test_create_commodity_slate():
    commodity_dict = {}
    commodity_dict["gasoline"] = xtot_objects.Commodity("gasoline", "bbl")
    commodity_dict["diesel"] = xtot_objects.Commodity("diesel", "bbl")
    commodity_dict["corn"] = xtot_objects.Commodity("corn", "tons")
    commodity_dict["wheat"] = xtot_objects.Commodity("wheat", "tons")
    c = xtot_objects.Commodity_Slate(commodity_dict)
    return c


def _test_create_commodity_slate2():
    commodity_dict = {}
    commodity_dict["jet"] = xtot_objects.Commodity("jet", "bbl")
    commodity_dict["diesel"] = xtot_objects.Commodity("diesel", "bbl")
    commodity_dict["corn"] = xtot_objects.Commodity("corn", "tons")
    commodity_dict["wheat"] = xtot_objects.Commodity("wheat", "tons")
    c = xtot_objects.Commodity_Slate(commodity_dict)
    return c

def _test_create_all_routes(all_facilities):

    # create and print two arcs
    # xtot_objects.Route("name", "origin_facility", "destination_facility", "cost_1", "cost_2", "min_capacity", "max_capacity", "travel_time")
    # testing to see if default capacity settings work

    origin = all_facilities[0]
    destination = all_facilities[1]
    storage_1 = all_facilities[2]

    commodity_slate = _test_create_commodity_slate()
    schedule = schedule_full_availability()
    all_routes_dict = {}


    all_routes_dict['rt1'] = xtot_objects.Route('rt1', origin, destination, 5, 2, commodity_slate, schedule)


    all_routes_dict['rt2'] = xtot_objects.Route('rt2', origin, destination, 3, 3, commodity_slate, schedule, "transport", 1, 0, 9999)


    all_routes_dict['rt3'] = xtot_objects.Route('rt3', origin, storage_1, 0.5, 3, commodity_slate, schedule, "transport", 1, 0, 9999)


    all_routes_dict['rt4'] = xtot_objects.Route('rt4', storage_1, destination, 0.5, 3, commodity_slate, schedule, "transport", 1, 0, 9999)
#    print route2.duration
#    print route2.asText()


    return all_routes_dict

#scenario with 2nd storage location
def _test_create_all_routes_2(all_facilities):


    # create and print two arcs
    # xtot_objects.Route("name", "origin_facility", "destination_facility", "cost_1", "cost_2", "min_capacity", "max_capacity", "travel_time")
    # testing to see if default capacity settings work

    origin = all_facilities[0]
    destination = all_facilities[1]
    storage_1 = all_facilities[2]
    storage_2 = all_facilities[3]

    commodity_slate = _test_create_commodity_slate()
    schedule = schedule_full_availability()
    all_routes_dict = {}

    all_routes_dict['rt1'] = xtot_objects.Route('rt1', origin, destination, 5, 2, commodity_slate, schedule)

    all_routes_dict['rt2'] = xtot_objects.Route('rt2', origin, destination, 3, 3, commodity_slate, schedule, "transport", 1, 0, 9999)

    all_routes_dict['rt3'] = xtot_objects.Route('rt3', origin, storage_1, 0.5, 3, commodity_slate, schedule, "transport", 1, 0, 9999)

    all_routes_dict['rt4'] = xtot_objects.Route('rt4', storage_1, storage_2, 0.5, 3, commodity_slate, schedule, "transport", 1, 0, 9999)

    all_routes_dict['rt5'] = xtot_objects.Route('rt5', storage_2, destination, 0.5, 3, commodity_slate, schedule)

#    print route2.duration
#    print route2.asText()


    return all_routes_dict

def _test_create_all_routes_3(all_facilities):


    # create and print two arcs
    # xtot_objects.Route("name", "origin_facility", "destination_facility", "cost_1", "cost_2", "min_capacity", "max_capacity", "travel_time")
    # testing to see if default capacity settings work

    origin = all_facilities[0]
    destination = all_facilities[1]
    storage_1 = all_facilities[2]
    storage_2 = all_facilities[3]
    processor = all_facilities[4]

    commodity_slate = _test_create_commodity_slate()
    schedule = schedule_full_availability()
    all_routes_dict = {}

    all_routes_dict['rt1'] = xtot_objects.Route('rt1', origin, destination, 5, 2, commodity_slate, schedule)

    all_routes_dict['rt2'] = xtot_objects.Route('rt2', origin, processor, 3, 3, commodity_slate, schedule, "transport", 1, 0, 9999)

    all_routes_dict['rt3'] = xtot_objects.Route('rt3', origin, storage_1, 0.5, 3, commodity_slate, schedule, "transport", 1, 0, 9999)

    all_routes_dict['rt4'] = xtot_objects.Route('rt4', storage_1, processor, 0.5, 3, commodity_slate, schedule, "transport", 1, 0, 9999)

    all_routes_dict['rt5'] = xtot_objects.Route('rt5', processor, destination, 0.5, 3, commodity_slate, schedule)

#    print route2.duration
#    print route2.asText()


    return all_routes_dict

def _test_create_all_routes_5(all_facilities):


    # create and print two arcs
    # xtot_objects.Route("name", "origin_facility", "destination_facility", "cost_1", "cost_2", "min_capacity", "max_capacity", "travel_time")
    # testing to see if default capacity settings work

    origin_1 = all_facilities[0]
    origin_2 = all_facilities[1]
    destination = all_facilities[6]
    storage_1 = all_facilities[2]
    storage_2 = all_facilities[3]
    processor_1 = all_facilities[4]
    processor_2 = all_facilities[5]

    commodity_slate = _test_create_commodity_slate()
    schedule = schedule_full_availability()
    all_routes_dict = {}

    all_routes_dict['rt1'] = xtot_objects.Route('rt1', origin_1, destination, 5, 2, commodity_slate, schedule)

    all_routes_dict['rt2'] = xtot_objects.Route('rt2', origin_1, processor_1, 3, 3, commodity_slate, schedule, "transport", 1, 0, 9999)

    all_routes_dict['rt3'] = xtot_objects.Route('rt3', origin_1, storage_1, 0.5, 3, commodity_slate, schedule, "transport", 1, 0, 9999)

    all_routes_dict['rt4'] = xtot_objects.Route('rt4', storage_1, processor_1, 0.5, 3, commodity_slate, schedule, "transport", 1, 0, 9999)

    all_routes_dict['rt5'] = xtot_objects.Route('rt5', processor_1, destination, 0.5, 3, commodity_slate, schedule)

    all_routes_dict['rt6'] = xtot_objects.Route('rt6', origin_1, processor_2, 0.5, 3, commodity_slate, schedule)

    all_routes_dict['rt7'] = xtot_objects.Route('rt7', processor_2, destination, 0.5, 3, commodity_slate, schedule)

    all_routes_dict['rt8'] = xtot_objects.Route('rt8', origin_1, storage_2, 0.2, 3, commodity_slate, schedule)

    all_routes_dict['rt9'] = xtot_objects.Route('rt9', origin_2, storage_2, 0.2, 3, commodity_slate, schedule)

    all_routes_dict['rt10'] = xtot_objects.Route('rt10', storage_2, processor_1, 0.2, 3, commodity_slate, schedule)

    all_routes_dict['rt11'] = xtot_objects.Route('rt11', storage_2, processor_2, 0.2, 3, commodity_slate, schedule)


    return all_routes_dict

def _test_create_all_routes_6(all_facilities):


    # create and print two arcs
    # xtot_objects.Route("name", "origin_facility", "destination_facility", "cost_1", "cost_2", "min_capacity", "max_capacity", "travel_time")
    # testing to see if default capacity settings work

    origin_1 = all_facilities[0]
    destination = all_facilities[2]
    processor_1 = all_facilities[1]

    commodity_slate = _test_create_commodity_slate()
    schedule = schedule_full_availability()
    all_routes_dict = {}

    all_routes_dict['rt1'] = xtot_objects.Route('rt1', origin_1, destination, 5, 2, commodity_slate, schedule)

    all_routes_dict['rt2'] = xtot_objects.Route('rt2', origin_1, processor_1, 0.3, 3, commodity_slate, schedule, "transport", 0, 0, 99999)

    all_routes_dict['rt3'] = xtot_objects.Route('rt3', processor_1, destination, 0.5, 3, commodity_slate, schedule)


    return all_routes_dict

#===============================================================================
# MAIN
#===============================================================================
# %%
if __name__ == "__main__":
    run_test_pulp_optimization()
    print "end"