#---------------------------------------------------------------------------------------------------
# Name: xtot_objects
#
# Purpose: This file contains all the objects used to run X-TOT.
#
#---------------------------------------------------------------------------------------------------


from ftot import ureg, Q_

#==========================#
class Schedule(object):

    def __init__(self, day_1, last_day, availability):
        self.first_day = day_1
        self.last_day = last_day
        self.availability = availability
        unavailable_days = ""
        sum_of_available_days = 0
        for i in range(self.first_day, self.last_day+1):
            # converting all availabilities to strings
            # cannot be ints as some are decimals; cannot be floats due to precision errors with zero.
            if self.availability[i] is '0':
                if unavailable_days:  # not an empty string
                    unavailable_days += ", " + str(i)
                else: # initialize string
                    unavailable_days = str(i)
            else:
                sum_of_available_days = sum_of_available_days + 1
        self.unavailable_days = unavailable_days
        self.total_available_days = sum_of_available_days

    def asText(self):
        return "Days {} - {} \n unavailable on days: ({})".format(self.first_day, self.last_day, self.unavailable_days)

    def __str__(self):
        return str(self.availability)

#==========================#
#===============================================================================
class Commodity(object):

    def __init__(self, name, units,):
        self.name = name
        self.units = units
##        self.quantity_per_truckload
##        self.quantity_per_barge
##        self.quantity_per_railcar

    def asText(self):
        return "Name: {}, units: {} ".format(self.name, self.units)

# a specific type of Commodity with additional attributes
class Feedstock(Commodity):

    def __init__(self, feedstock_type, feesdstock_source_category, feedstock_source, crop_yield, units = "kg"):
        self.feedstock_type = feedstock_type
        self.feesdstock_source_category = feesdstock_source_category
        self.feedstock_source = feedstock_source
        self.name = feedstock_type + "_" + feesdstock_source_category + "_" + feedstock_source

        self.crop_yield = crop_yield

        Commodity.__init__(self, self.name, units)

        # add fields yield, type, source category, source, ag_census_fc from xml, ag_census yield field

# a specific type of Commodity with additional attributes
class Fuel(Commodity):

    def __init__(self, fuel_name, primary_process_type, secondary_process_type, tertiary_process_type, units = "bbl"):
        self.fuel_name = fuel_name
        self.primary_process_type = primary_process_type
        self.secondary_process_type = secondary_process_type
        self.tertiary_process_type = tertiary_process_type
        self.name = fuel_name + "_" + primary_process_type + "_" + secondary_process_type + "_" + tertiary_process_type
        demand_met_multiplier = "undefined"

        # switch the demand_met_multiplier based on fuel name and process type
        if self.fuel_name == "jet":
            if self.primary_process_type == "HEFA":
                demand_met_multiplier = 2
            elif self.primary_process_type == "FTx":
                demand_met_multiplier = 2
            elif self.primary_process_type == "AFx":
                #30% alcohol/fuel to 70% petroleum
                demand_met_multiplier = (3+1/3)
            elif self.primary_process_type == "Petroleum_Refinery":
                demand_met_multiplier = 1
            else:
                print "the demand met multiplier was not set"
                raise Exception("the demand met multiplier was not set")

        if self.fuel_name == "diesel":
            # TODO - this assumes no blend resctriction for diesel as diesel.
            # it does not contemplate ASTM 5% HEFA diesel in jet blends.
            demand_met_multiplier = 1
            self.demand_met_multiplier = demand_met_multiplier
        Commodity.__init__(self, self.name, units)


#===============================================================================

#===============================================================================
# It specifies a commodity and quantity in, but also has a unmet demand penalty
##it is always attached to a destination
# moving all of this to Final_Destination_Slate
###===============================================================================
##class Destination_Demand(Product):
##
##    def __init__(self, commodity, demand_quantity_in, udp):
##        self.udp = udp
##        Product.__init__(self, commodity, demand_quantity_in)
##
##    def log_info(self, logger):
##        logger.info("Object Type: \t {}".format(type(self).__name__))
##        logger.info("Commodity Name: \t {}".format(self.name))
##        logger.info("Quantity In (demand): \t {}".format(self.quantity
##        ))
##        logger.info("Commodity Units: \t {}".format(self.units))
##        logger.info("Unmet Demand Penalty: \t{}".format(self.udp))
##        # TODO: print out the commodity info as well.
##        #self.parent_facility.asText(),
#===============================================================================

#===============================================================================
# Commodity Slate is used for two things:
# 1) identify what commodities are valid for a given facility TODO - add helper method to check if commodity is in Commodity_Slate
# 2)  how feedstock and products are related to one another. In other words, commodity_slate is a receipe of how much material
# entering a facility is converted to products leaving.
# TODO - this needs to be though through somemore...
# Feedstock vs Feedstocks vs Feedstock List vs Feedstock Dict
# Product vs Products vs Product List vs Product Dict
# for origins, the input dictionary is empty;
# for destinations, the output dictionary is empty
# input and output commodities dict should have the form dict(name) = (Product or Feedstock)
#===============================================================================
class Commodity_Slate(object):

    def __init__(self, allowed_commodities):

        self.commodities = allowed_commodities

        list_for_text = []

        list_for_names = []

        for k, v in self.commodities.iteritems():

            list_for_names.append(k)

            list_for_text.append(v.asText())

        self.commodity_names = list_for_names

        self.commodity_list_for_text= list_for_text

    def AllAllowedCommodities(self):

        commodity_names = []

        for k, v in self.commodities.iteritems():

            commodity_names.append(k)

        return commodity_names



##    def asText(self):
##
##        return "Commodity Slate Type: = {}, \
##                all_commodity_names: = {}, \
##                commodity_slate_entries: = {}".format(
##                type(self).__name__,
##                self.AllAllowedCommodities(),
##                self.list_for_text)

# dictionaries have the form dict[commodity_name] = (commodity, amount)
# where the "amount" is required for input, produced by output
class Processor_Slate(Commodity_Slate):

    def __init__(self,
            input_commodities_dict,
            output_commodities_dict, process_tuple):

        self.input_commodities_dict = input_commodities_dict
        self.output_commodities_dict = output_commodities_dict
        commodities_only = {}
        self.quantities_only = {}
        self.commodity_list_for_text = []
        for k, v in self.input_commodities_dict.iteritems():
            commodities_only[k] = v[0]
            self.quantities_only[k] = v[1]
            self.commodity_list_for_text.extend((v[0].asText(), v[1]))
        for k, v in self.output_commodities_dict.iteritems():
            commodities_only[k] = v[0]
            self.quantities_only[k] = v[1]
            self.commodity_list_for_text.extend((v[0].asText(), v[1]))
        self.commodities_only = commodities_only
        self.primary_process_type = process_tuple[0]
        self.secondary_process_type = process_tuple[1]
        self.tertiary_process_type = process_tuple[2]

        Commodity_Slate.__init__(self, commodities_only)

    def allowedInputCommodities(self):
        input_commodity_names = []
        for k, v in self.input_commodities_dict.iteritems():
            input_commodity_names.append(k)
        return input_commodity_names

    def allowedOutputCommodities(self):
        output_commodity_names = []
        for k, v in self.output_commodities_dict.iteritems():
            output_commodity_names.append(k)
        return output_commodity_names

    def getQuantity(self, commodity_name):
        return self.quantities_only[commodity_name]


    def asText(self):

        return "Commodity Slate Type: = {}, \
                all_commodity_names: = {}, \
                input_commodity_names: = {}, \
                output_commodity_names: = {}, \
                commodity_slate_entries: = {}".format(
                type(self).__name__,
                self.AllAllowedCommodities(),
                self.allowedInputCommodities(),
                self.allowedOutputCommodities(),
                self.commodity_list_for_text)

# input has form dict[commodity_name] = commodity
class Storage_Slate(Commodity_Slate):
    def __init__(self, allowed_commodites):
        Commodity_Slate.__init__(self, allowed_commodites)

    def asText(self):

        return "Commodity Slate Type: = {}, \
                all_commodity_names: = {}, \
                commodity_slate_entries: = {}".format(
                type(self).__name__,
                self.AllAllowedCommodities(),
                self.commodity_list_for_text)



#input has for dict[commodity_name] = (commodity, quantity_produced)
class PreProcessor_Slate(Commodity_Slate):
    def __init__(self, commodites_created_here):


        self.commodities_created = commodites_created_here

        commodities_only = {}

        self.quantities_only = {}

        self.commodity_list_for_text = []

        for k, v in self.commodities_created.iteritems():

            commodities_only[k] = v[0]

            self.quantities_only[k] = v[1]

            self.commodity_list_for_text.append((v[0].asText(), v[1]))


        Commodity_Slate.__init__(self, commodities_only)

    def getSupply(self, commodity_name):

        return self.quantities_only[commodity_name]

    def asText(self):

        return "Commodity Slate Type: = {}, \
                commodity_slate_names: = {}, \
                commodity_slate_entries: = {}, \
                self.quantities_only {}".format(
                type(self).__name__,
                self.AllAllowedCommodities(),
                self.commodity_list_for_text,
                self.quantities_only)

#input has for dict[commodity_name] = (commodity, quantity_demanded, udp)
class Final_Destination_Slate(Commodity_Slate):

    def __init__(self, commodities_demanded):

        self.commodities_demanded = commodities_demanded

        commodities_only = {}

        self.quantities_only = {}

        self.commodity_list_for_text = []

        for k, v in self.commodities_demanded.iteritems():

            commodities_only[k] = v[0]

            self.quantities_only[k] = v[1]

            self.commodity_list_for_text.append((v[0].asText(), v[1], v[2]))

        Commodity_Slate.__init__(self, commodities_only)

    def getUDP(self, commodity_name):

        return self.commodities_demanded[commodity_name][2]

    def getDemand(self, commodity_name):

        return self.commodities_demanded[commodity_name][1]

    def asText(self):

        return "Commodity Slate Type: = {}, \
                commodity_slate_names: = {}, \
                commodity_slate_entries: = {}".format(
                type(self).__name__,
                self.AllAllowedCommodities(),
                self.commodity_list_for_text)
#===============================================================================
# Facility Class
# The facility has a nameplate_capacity,
# nameplate_capacity_units, and a commodity_slate. The schedule specifices the
# product slate which is a relationship of allowed inputs and outputs.
# facility_Schedule dictates whether the facility is available for each day
#===============================================================================
class Facility(object):

    def __init__(self, name, description, location,
                commodity_slate,
                nameplate_capacity,
                nameplate_capacity_units,
                facility_schedule, has_storage = 1, candidate=0, capex=0):

        self.name = name
        self.description = description
        self.location = location
        self.commodity_slate = commodity_slate #dictionary of commodity slates for processors
        self.nameplate_capacity = nameplate_capacity
        self.nameplate_capacity_units = nameplate_capacity_units
        self.facility_schedule = facility_schedule
        self.has_storage = has_storage
        self.candidate = candidate
        self.capex = capex



    def facility_type_check(self, logger):
        logger.info("Facility is defined as {} \n \
            Allowed commodities:  \n ".format(type(self).__name__, self.commodity_slate.AllAllowedCommodities()))


    def asText(self):

        return "Facility Type: = {},  \n \
                name: = {},  \n \
                description: = {},  \n \
                location: = {}, \n  \
                commodity_slate_type: = {}, \n  \
                nameplate_capacity = {},  \n \
                nameplate_capacity_units = {}, \n \
                facility_schedule: = {},  \n \
                has_storage: = {}".format(
                type(self).__name__,
                self.name,
                self.description,
                self.location,
                type(self.commodity_slate).__name__,
                self.nameplate_capacity,
                self.nameplate_capacity_units,
                self.facility_schedule.asText(),
                self.has_storage)

## removed self.commodity_slate.asText() from facility asText because it wouldn't be formatted for a dict of processor slates -- OG 6-21

##    def log_debug(self, logger):
##            logger.info("Facility Type: = {}, \n \
##                name: = {}, \n \
##                description: = {}, \n \
##                location: = {}, \n \
##                commodity_slate: = {}, \n \
##                nameplate_capacity = {}, \n \
##                nameplate_capacity_units = {},\n \
##                facility_schedule = {}".format(
##                type(self).__name__,
##                self.name.asText(),
##                self.description.asText(),
##                self.location.asText(),
##                self.commodity_slate.asText(),
##                self.nameplate_capacity,
##                self.nameplate_capacity_units,
##                self.facility_schedule.asText()))

#===============================================================================
# Storage Class
# A type of Facility. The storage object is like a Processor,
# but doesn't convert anything. The process is a special kind of Process called
# storage_process.
#===============================================================================

class Storage(Facility):

    def __init__(self, parent_facility):

        Facility.__init__(self,
            parent_facility.name,
            parent_facility.description,
            parent_facility.location,
            parent_facility.commodity_slate,
            parent_facility.nameplate_capacity,
            parent_facility.nameplate_capacity_units,
            parent_facility.facility_schedule,
            parent_facility.has_storage,
            parent_facility.candidate,
            parent_facility.capex)



#===============================================================================
# Pre_Processor Class
# special kind of Facility. The pre-processor is the initial supplier.
# the commodity_slate will have no feedstock, it will only have product
# supplied.
# - consider: Source, Initial_Supplier , and other variable name.
# - pre-processors are optional.
#===============================================================================
class PreProcessor(Facility):

    def __init__(self, parent_facility):

        Facility.__init__(self,
            parent_facility.name,
            parent_facility.description,
            parent_facility.location,
            parent_facility.commodity_slate,
            parent_facility.nameplate_capacity,
            parent_facility.nameplate_capacity_units,
            parent_facility.facility_schedule,
            parent_facility.has_storage,
            parent_facility.candidate,
            parent_facility.capex)

    def getSupply(self, commodity_name):
         return self.commodity_slate.getSupply(commodity_name)

##        if type(self.parent_facility).__name__ == "PreProcessor":
##             return commodity_slate.getSupply(commodity_name)
##        else:
##            logger.info("no supply available; facility is not a Preprocessr")
##            return 0
##
##

#===============================================================================
# Final_Destination Class
# special kind of Facility. The Final_Destination has a demand and unmet
# demand penalty (UDP), as well as a feedstock
#===============================================================================
class Final_Destination(Facility):

    def __init__(self, parent_facility):

        Facility.__init__(self,
            parent_facility.name,
            parent_facility.description,
            parent_facility.location,
            parent_facility.commodity_slate,
            parent_facility.nameplate_capacity,
            parent_facility.nameplate_capacity_units,
            parent_facility.facility_schedule,
            parent_facility.has_storage,
            parent_facility.candidate,
            parent_facility.capex)


    def getUDP(self, commodity_name):
        return self.commodity_slate.getUDP(commodity_name)
##        simplified_commodity_name = commodity_name.split("_")[0]
##        return self.commodity_slate.getUDP(simplified_commodity_name)
##        if type(self.parent_facility).__name__ == "Final_Destination":
##            return self.commodity_slate.getUDP(commodity_name)
##        else:
##            logger.info("no UDP available; facility is not a destination")
##            return 0


    def getDemand(self, commodity_name):
        return self.commodity_slate.getDemand(commodity_name)
##        if type(self.parent_facility).__name__ == "Final_Destination":
##             return commodity_slate.getDemand(commodity_name)
##        else:
##            logger.info("no demand available; facility is not a destination")
##            return 0
##    def get_UDP(self):
##        udp = self.commodity_slate.input_commodities_dict
##
##    return udp
        # TODO - do we want to pull the UDP list at the Vertex object?
        # returns a list of UDP the demand_dict
        pass

#===============================================================================
# Generic_Intermediate_Facility Class
# the generic intermediate facility is a facility that sits
# between a preprocessor/supply and ultimate destination
#===============================================================================
class Intermediate_Processing_Facility(Facility):

    def __init__(self, parent_facility):

        Facility.__init__(self,
            parent_facility.name,
            parent_facility.description,
            parent_facility.location,
            parent_facility.commodity_slate, # dict of slates
            parent_facility.nameplate_capacity,
            parent_facility.nameplate_capacity_units,
            parent_facility.facility_schedule,
            parent_facility.has_storage,
            parent_facility.candidate,
            parent_facility.capex)

    def getQuantity(self, slate_identifier, commodity_name): #slate identifier is the key to tell which item of the slate dict to use here
        return self.commodity_slate[slate_identifier].getQuantity(commodity_name)

    def allowedProcessorInputCommodities(self):
        commodities = []
        for k, v in self.commodity_slate.iteritems():
            commodities.extend(v.allowedInputCommodities())
        return commodities

    def allowedProcessorOutputCommodities(self):
        commodities = []
        for k, v in self.commodity_slate.iteritems():
            commodities.extend(v.allowedOutputCommodities())
        return commodities

    def allAllowedProcessorCommodities(self):
        commodities = []
        for k, v in self.commodity_slate.iteritems():
            commodities.extend(v.allowedOutputCommodities())
            commodities.extend(v.allowedInputCommodities())
        return commodities
#===============================================================================
# facility exploded by time and commodity
# candidate is 0 or 1 to indicate whether deciding to build this facility is part
# of the optimization
# capex is the build cost (TODO total or amortized?); only matters if candidaate = 1
# created a default for commodities to deal with processors, which handle multiple commodities
#===============================================================================

class Vertex(Facility):

    def __init__(self, logger, parent_facility, day,
    commodity_name = "multiple commodities", adjacent_storage_indicator = 0,
    udp = 0, supply = 0, demand = 0, quantity = 0):
        self.parent_facility = parent_facility
        self.day = day
        self.commodity_name = commodity_name
        # 1 if this is a storage/blending vertex adjacent to a facility, zero if this is itself a facility
        self.storage_indicator = adjacent_storage_indicator

        Facility.__init__(self,
            parent_facility.name,
            parent_facility.description,
            parent_facility.location,
            parent_facility.commodity_slate,
            parent_facility.nameplate_capacity,
            parent_facility.nameplate_capacity_units,
            parent_facility.facility_schedule,
            parent_facility.has_storage,
            parent_facility.candidate,
            parent_facility.capex)

        if type(parent_facility).__name__ == "Final_Destination":
            simplified_commodity_name = commodity_name.split("_")[0]
            self.udp = parent_facility.getUDP(simplified_commodity_name)

        if type(parent_facility).__name__ == "PreProcessor":
            self.supply = parent_facility.getSupply(commodity_name)

        if type(parent_facility).__name__ == "Final_Destination":
            simplified_commodity_name = commodity_name.split("_")[0]
            self.demand = parent_facility.getDemand(simplified_commodity_name)


#===============================================================================
# cost1 is currently $ per unit of commodity to transport

class Route(object):

    # construct object
    def __init__(
        self,
        name,
        origin_facility,
        destination_facility,
        cost1,
        cost2,
        commodity_slate,
        schedule,
        route_type = "transport",
        travel_time=0,
        min_capacity=0,
        max_capacity=9999,):

        self.name = name
        self.origin_facility = origin_facility
        self.destination_facility = destination_facility
        self.cost1 = cost1
        self.cost2 = cost2
        self.min_flow = min_capacity
        self.max_flow = max_capacity
        self.duration = travel_time
        self.commodity_slate = commodity_slate
        self.schedule = schedule
        #storage or transport route
        self.route_type = route_type

    # method example, this one is just for printing the object/ debugging
    def asText(self):
        return 'name = {}, \
                origin = {}, \
                destination = {}, \
                cost1 = {}, \
                cost2 = {}, \
                commodites allowed = {}, \
                schedule = {}, \
                route type = {}, \
                time to travel = {} \
                min daily flow = {}, \
                max daily flow = {}'.format(
                self.name,
                self.origin_facility,
                self.destination_facility,
                self.cost1,
                self.cost2,
                self.commodity_slate.AllAllowedCommodities(),
                self.schedule.asText(),
                self.route_type,
                self.duration,
                self.min_flow,
                self.max_flow)
#===============================================================================
#TODO - origin vertex will have to be adjusted for colocated storage
class Edge(Route):
    def __init__(self, logger, parent_route, start_day, commodity_name, origin_vertex_key, destination_vertex_key, duration_override = 0):
        self.route = parent_route
        self.origin_vertex_key = origin_vertex_key
        self.destination_vertex_key = destination_vertex_key
        self.edge_start_day = start_day
        if duration_override == 0:
            self.end_day = start_day+parent_route.duration
        else:
            self.end_day = start_day
        self.commodity_name = commodity_name
        # self.origin_vertex = Vertex()
        Route.__init__(self, parent_route.name,
        parent_route.origin_facility,
        parent_route.destination_facility,
        parent_route.cost1,
        parent_route.cost2,
        parent_route.commodity_slate,
        parent_route.schedule,
        parent_route.route_type,
        parent_route.min_flow,
        parent_route.max_flow,
        parent_route.duration)
   # def edge_consistency_check(self, logger):
##        if self.origin_vertex.parent_facility != self.route.origin_facility:
##            logger.info("ERROR - origin vertex does not align with route")

##        if self.destination_vertex.parent_facility != self.route.destination_facility:
##            logger.info("ERROR - destination vertex does not align with route")

##        if self.duration != self.end_day - self.edge_start_day:
##            logger.info("ERROR - mismatch between vertex days and route duration")
##
##        if self.origin_vertex.commodity_name != self.destination_vertex.commodity_name:
##            logger.info("ERROR - commodity mismatch between origin and destination")


