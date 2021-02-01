# -*- coding: utf-8 -*-
"""
Created on Fri Jan 05 11:00:43 2018

@author: Matthew.Pearlson.CTR
"""


import os
import csv
import math
import datetime
import glob
import ntpath
#import ftot_supporting

# create a big csv file to store all the entries
# batch, from location id, to location id

# get OD pair csv files

# iterate over the OD pair csvs
# and write each line into the new one

def odpair_csv_util():
    
    
    print "copy this file and run it from logs\multiprocessor_batch_solve dir to parse the batch logs. "
    #make a directory for the report to go into
    log_dir = os.curdir 
    print log_dir
    report_directory = os.path.join(log_dir, "report")
    if not os.path.exists(report_directory):
        os.makedirs(report_directory)

    # get all of the csv files matching the pattern
 
    log_files = glob.glob(os.path.join(log_dir, "*.csv"))
    print  "len(log_files): {}".format(len(log_files))                                                    
    
    log_file_list = []
    # add log file name and date to dictionary.  each entry in the array
    # will be a tuple of (log_file_name, datetime object)
    
    for log_file in log_files:

        path_to, the_file_name = ntpath.split(log_file)

        print the_file_name
                    
        the_batch = the_file_name.split("_od_pairs")
        
        with open(log_file, 'r') as rf:
            
            for line in rf:
                if line.find("O_Name"):
                    continue
                elif line.find("D_Name"):
                    continue
                else:
                    message = line.
                
                # do some sorting of the messages here
                if message.find("Batch: ") > -1:
                    if message.find("- Total Runtime (HMS):") > -1:
                        pass
                    else:
                        batch_number =  message.split(" - ")[0]
                        the_task =  message.split(" - ")[1]
                        time_strings = message.split(" - ")[2]
                        start_time = time_strings.split("Runtime (HMS): ")[0].split("Start time: \t")[1].lstrip().rstrip()
                        run_time = time_strings.split("Runtime (HMS): ")[1].lstrip().rstrip()
                        message_dict["ALL_OPERATIONS"].append((the_batch, the_task, start_time, run_time))
                    
                if message.find("Total Runtime (HMS):") > -1:
     
                    run_time =  message.split("Total Runtime (HMS):")[1]
                    message_dict["TOTAL_TIME"].append((the_batch, time_stamp, run_time))
                   
                elif message.find("arcpy.na.Solve()") > -1:
                    run_time =  message.split("Runtime (HMS): ")[1].lstrip().rstrip()
                    message_dict["ROUTE_SOLVE"].append((the_batch, time_stamp, run_time))
                
                    
                message_dict["EVERYTHING"].append((the_batch, time_stamp, message))
                
    # dump to files
    # ---------------
    
    # total_runtime 
    #----------------
    
    for message_type in message_dict:
        report_file_name = 'report_{}_'.format(message_type) + datetime.datetime.now().strftime("%Y_%m_%d_%H-%M-%S") + ".csv"
        report_file = os.path.join(report_directory, report_file_name)
        
        if message_type == "ALL_OPERATIONS":
            with open(report_file, 'w') as wf:
                wf.write('Batch, Opeartion, Start_Time, Run_Time\n')
                for x in message_dict[message_type]:
                    wf.write('{}, {}, {}, {}\n'.format(x[0], x[1], x[2], x[3]))
        else:
            with open(report_file, 'w') as wf:
                wf.write('Batch, Timestamp, Message\n')
                for x in message_dict[message_type]:
                    wf.write('{}, {}, {}'.format(x[0], x[1], x[2]))

    print "Report file location: {}".format(report_file)
    print "all done."



if __name__ == "__main__":

	os.system('cls')

	odpair_csv_util()

