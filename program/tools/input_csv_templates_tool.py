# input CSV template generation tool

import os
from six.moves import input

# ==============================================================================

def run_input_csv_templates_tool():
    os.system('cls')
    print("FTOT input CSV template generation tool")
    print("-----------------------------------------")
    print("")
    print("")
    generate_input_csv_templates()


# ==============================================================================

def generate_input_csv_templates():

    template_list = get_template_list()
    input_data_dir = get_user_configs()
    template_columns = get_columns(template_list)

    for facility in template_columns:
        a_filename = os.path.join(input_data_dir, facility + ".csv")
        if os.path.exists(a_filename):
            print(a_filename + " already exists")
            print("Do you want to overwrite this file?")
            overwrite = input('----------------------> ')
            yes_no = False
            while yes_no == False:
                if (overwrite.lower() == 'y' or overwrite.lower() == 'yes'):
                    yes_no = True
                    print("opening a CSV file for {}: here -- {}".format(facility, a_filename))
                    with open(a_filename, 'w') as wf:
                        # write the header line
                        header_line = template_columns[facility]
                        wf.write(str(header_line + "\n"))
                elif (overwrite.lower() == 'n' or overwrite.lower() == 'no'):
                    print("not overwriting file, returing to main menu")
                    yes_no = True
                else:
                    print("Invalid input")
                    print("Do you want to overwrite this file?")
                    overwrite = input('----------------------> ')
        else:
            print("opening a CSV file for {}: here -- {}".format(facility, a_filename))
            with open(a_filename, 'w') as wf:
                # write the header line
                header_line = template_columns[facility]
                wf.write(str(header_line + "\n"))


# ==============================================================================

# Prompts user to enter which facility types they want to generate templates for
def get_template_list():
    print("Enter a list of facilities to generate CSV templates for, separated by \',\'")
    print("Facilities must be \'rmp\', \'proc\', \'proc_cand\', or \'dest\'")
    template_list = input('----------------------> ').replace('\'','')
    template_list = template_list.split(',')
    improper_input = True
    facility_types = ['rmp', 'proc', 'proc_cand', 'dest']
    while improper_input:
        for t in template_list:
            if t.strip() not in facility_types:
                improper_input = True
                print(t + " is not in list of facilities")
                print("Enter a list of facilities to generate CSV templates for, separated by \',\'")
                print("Facilities must be \'rmp\', \'proc\', \'proc_cand\', or \'dest\'")
                template_list = input('----------------------> ').replace('\'','')
                template_list = template_list.split(',')
                break
            else:
                improper_input = False
    return template_list


# ==============================================================================

# Prompts user for optional columns
def get_columns(templates):
    # dictionary associating facility type with optional columns
    template_columns = {}
    print("Do you want to include optional fields?")
    optional = input('----------------------> ')
    yes_no = False
    # check if user properly answered yes or no
    while yes_no == False:
        # if no return mandatory columns
        if (optional.lower() == 'n' or optional.lower() == 'no'):
            yes_no = True
            for t in templates:
                template_columns[t] = "facility_name,facility_type,commodity,value,units,phase_of_matter,io"
            return template_columns
        # if yes ask for optional columns for each facility type
        # for each facility type, display what the optional columns are
        if (optional.lower() == 'y' or optional.lower() == 'yes'):
            yes_no = True
            for t in templates:
                improper_input = True
                if t == 'rmp':
                    column_types = ['schedule', 'max_transport_distance', 'none']
                    print('Optional fields for rmp are \'schedule\' and \'max_transport_distance\'')
                    print('Enter optional fields, separated by \',\' or enter \'none\' for no optional fields')
                    fields = input('----------------------> ').replace('\'','')
                    fields_split = fields.split(',')
                    for f in fields_split:
                        while improper_input:
                            if f not in column_types:
                                improper_input = True
                                print('An improper field has been inputted')
                                print('Optional fields for rmp are \'schedule\' and \'max_transport_distance\'')
                                print('Enter optional fields, separated by \',\'')
                                fields = input('----------------------> ').replace('\'','')
                                fields_split = fields.split(',')
                                break
                            else:
                                improper_input = False
                    if fields == 'none':
                        template_columns[t] = "facility_name,facility_type,commodity,value,units,phase_of_matter,io"
                    else:
                        template_columns[t] = "facility_name,facility_type,commodity,value,units,phase_of_matter,io," + fields
                elif t == 'proc':
                    column_types = ['schedule', 'max_transport_distance', 'min_capacity', 'max_capacity', 'build_cost', 'none']
                    print('Optional fields for proc are \'schedule\', \'max_transport_distance\', \'min_capacity\', \'max_capacity\', and \'build_cost\'')
                    print('Enter optional fields, separated by \',\' or enter \'none\' for no optional fields')
                    fields = input('----------------------> ').replace('\'','')
                    fields_split = fields.split(',')
                    for f in fields_split:
                        while improper_input:
                            if f not in column_types:
                                improper_input = True
                                print('An improper field has been inputted')
                                print('Optional fields for rmp are \'schedule\', \'max_transport_distance\', \'min_capacity\', \'max_capacity\', and \'build_cost\'')
                                print('Enter optional fields, separated by \',\' or enter \'none\' for no optional fields')
                                fields = input('----------------------> ').replace('\'','')
                                fields_split = fields.split(',')
                                break
                            else:
                                improper_input = False
                    if fields == 'none':
                        template_columns[t] = "facility_name,facility_type,commodity,value,units,phase_of_matter,io"
                    else:
                        template_columns[t] = "facility_name,facility_type,commodity,value,units,phase_of_matter,io," + fields
                elif t == 'proc_cand':
                    column_types = ['schedule', 'max_transport_distance', 'none']
                    print('Optional fields for proc_cand are \'schedule\' and \'max_transport_distance\'')
                    print('Enter optional fields, separated by \',\' or enter \'none\' for no optional fields')
                    fields = input('----------------------> ').replace('\'','')
                    fields_split = fields.split(',')
                    for f in fields_split:
                        while improper_input:
                            if f not in column_types:
                                improper_input = True
                                print('An improper field has been inputted')
                                print('Optional fields for proc_cand are \'schedule\' and \'max_transport_distance\'')
                                print('Enter optional fields, separated by \',\'')
                                fields = input('----------------------> ').replace('\'','')
                                fields_split = fields.split(',')
                                break
                            else:
                                improper_input = False
                    if fields == 'none':
                        template_columns[t] = "facility_name,facility_type,commodity,value,units,phase_of_matter,io"
                    else:
                        template_columns[t] = "facility_name,facility_type,commodity,value,units,phase_of_matter,io," + fields
                elif t == 'dest':
                    column_types = ['schedule', 'none']
                    print('Optional field for dest is \'schedule\'')
                    print('Enter optional field, or enter \'none\' for no optional field')
                    fields = input('----------------------> ').replace('\'','')
                    fields_split = fields.split(',')
                    for f in fields_split:
                        while improper_input:
                            if f not in column_types:
                                improper_input = True
                                print('An improper field has been inputted')
                                print('Optional field for dest is \'schedule\'')
                                print('Enter optional fields, separated by \',\'')
                                fields = input('----------------------> ').replace('\'','')
                                fields_split = fields.split(',')
                                break
                            else:
                                improper_input = False
                    if fields == 'none':
                        template_columns[t] = "facility_name,facility_type,commodity,value,units,phase_of_matter,io"
                    else:
                        template_columns[t] = "facility_name,facility_type,commodity,value,units,phase_of_matter,io," + fields
        else:
            print("Invalid input")
            print("Do you want to include optional fields?")
            optional = input('----------------------> ')
    return template_columns


# ==============================================================================

def get_user_configs():
    # user specified input data directory
    print("directory to store the template CSV files: (drag and drop is fine here)")
    input_data_dir = ""
    while not os.path.exists(input_data_dir):
        input_data_dir = input('----------------------> ').strip('\"')
        print("USER INPUT ----------------->:  {}".format(input_data_dir))
        if not os.path.exists(input_data_dir):
            os.makedirs(input_data_dir)
    return input_data_dir

