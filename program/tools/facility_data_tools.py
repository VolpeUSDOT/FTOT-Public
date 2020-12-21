# -*- coding: utf-8 -*-
# the purpose of this  code is to digest facility information from FTOT 3.1 and prior, 
# and create csv data file for each facility.

import argparse
import arcpy
import os



if __name__ == '__main__':
    print "starting main"
    debug=True
    print "debug state : {}".format(debug)
    
    
    # process destinations first
    #---------------------------------------    
    
    # open csv file for writing 
    print "opening a csv file"
    output_file_name = "destination_demand.csv"
    output_dir = "C:\\FTOT\\branches\\2017_08_31_processor\\scenarios\\USDA_oil_seed_dec_2017\\data"
    output_file = os.path.join(output_dir, output_file_name)
    with open(output_file, 'w') as wf:
        
        # write the header line
        header_line = "facility_name,facility_type,commodity,value,units,phase_of_matter,io"
        wf.write(str(header_line+"\n"))
        
        # search cursor on the featureclass
        fields = ["facility_name", "Demand_Jet"]
        fc= "C:\\FTOT\\branches\\2017_08_31_processor\\scenarios\\USDA_oil_seed_dec_2017\\data\\rmp_data_ftot_v3_2.gdb\\network\\ultimate_destinations"
        print "opening fc: {}".format(fc)
        with arcpy.da.SearchCursor(fc, fields) as cursor:
            
             for row in cursor:
                if debug: print "processing row: {}".format(row) 
                facility_name = row[0]
                facility_type = "ultimate_destination"
                commodity = "jet" 
                value = row[1]
                units = "kgal"
                phase_of_matter = "liquid"
                io = "i"
            
                # csv writer.write(row)
                if debug: print "writing airport: {} and demand: {} \t {}".format(facility_name, value, units)
                wf.write("{},{},{},{},{},{},{}\n".format(facility_name,facility_type,commodity,value,units,phase_of_matter,io))
                
                
                
                
    # process rmps
    #--------------------
    print "opening a csv file"
    
    output_file_name = "rmp_supply.csv"
    output_dir = "C:\\FTOT\\branches\\2017_08_31_processor\\scenarios\\USDA_oil_seed_dec_2017\\data"
    output_file = os.path.join(output_dir, output_file_name)
    with open(output_file, 'w') as wf:
        
        # write the header line
        rmp_header_line = "facility_name,facility_type,commodity,value,units,phase_of_matter,max_transport_distance,io"
        header_line = rmp_header_line
        wf.write(str(header_line+"\n"))
        
        # search cursor on the featureclass
        fc= "C:\\FTOT\\branches\\2017_08_31_processor\\scenarios\\USDA_oil_seed_dec_2017\\data\\rmp_data_ftot_v3_2.gdb\\network\\raw_material_producers"
        fields=["facility_name", "Demand_Jet"]
        print "opening fc: {}".format(fc)
        with arcpy.da.SearchCursor(fc, fields) as cursor:
            
             for row in cursor:
                if debug: print "processing row: {}".format(row) 
                facility_name = row[0]
                facility_type = "ultimate_destination"
                commodity = "jet" 
                value = row[1]
                units = "kgal"
                phase_of_matter = "liquid"
                io = "i"
            
                # csv writer.write(row)
                if debug: print "writing airport: {} and demand: {} \t {}".format(facility_name, value, units)
                wf.write("{},{},{},{},{},{},{}\n".format(facility_name,facility_type,commodity,value,units,phase_of_matter,io))

    
def process_rmp_fc(debug=False):
    
    print "debug state : {}".format(debug)

    
    # open csv file for writing 
    print "opening a csv file"
    output_file = os.path.join(output_dir, output_file_name)
    with open(output_file, 'w') as wf:
        
        # write the header line
        rmp_header_line = "facility_name,facility_type,commodity,value,units,phase_of_matter,max_transport_distance,io"
        header_line = rmp_header_line
        wf.write(str(header_line+"\n"))
        
        # search cursor on the featureclass
        fc= "C:\\FTOT\\branches\\2017_08_31_processor\\scenarios\\USDA_oil_seed_dec_2017\\data\\rmp_data_ftot_v3_2.gdb\\network\\raw_material_producers"
        fields=[""]
        print "opening fc: {}".format(fc)
        with arcpy.da.SearchCursor(fc, fields) as cursor:
            
             for row in cursor:
                if debug: print "processing row: {}".format(row) 
                facility_name = row[0]
                facility_type = "ultimate_destination"
                commodity = "jet" 
                value = row[1]
                units = "kgal"
                phase_of_matter = "liquid"
                io = "i"
            
                # csv writer.write(row)
                if debug: print "writing airport: {} and demand: {} \t {}".format(facility_name, value, units)
                wf.write("{},{},{},{},{},{},{}\n".format(facility_name,facility_type,commodity,value,units,phase_of_matter,io))
                

