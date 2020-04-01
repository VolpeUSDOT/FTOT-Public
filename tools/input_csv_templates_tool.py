# run.bat upgrade tool

# the run.bat upgrade tool is used to generate run.bat files for the user.
# it is a command-line-interface (CLI) tool that prompts the user for the following information:
# - python installation (32 or 64 bit)
# - FTOT program directory
# - Scenario XML File
# - Candidate generation? Y/N
# - Output file location
# ==============================================================================
import os

# ==============================================================================


def run_input_csv_templates_tool():
    print("FTOT input csv template generation tool")
    print("-----------------------------------------")
    print("")
    print("")
    generate_input_csv_templates()


def generate_input_csv_templates():

    print("start: generate_input_csv_templates")
    template_list = ["rmp", "proc", "proc_cand", "dest"]

    input_data_dir = get_user_configs()
    for facility in template_list:
        a_filename = os.path.join(input_data_dir, facility+".csv")
        print("opening a csv file for {}: here -- {}".format(facility, a_filename))
        with open(a_filename, 'w') as wf:
            # write the header line
            header_line = "facility_name,facility_type,commodity,value,units,phase_of_matter,io"
            wf.write(str(header_line + "\n"))


def get_user_configs():
    # user specified input_data directory
    print("directory to store the template CSV files: (drag and drop is fine here)")
    input_data_dir = ""
    while not os.path.exists(input_data_dir):
        input_data_dir = raw_input('----------------------> ')
        print("USER INPUT ----------------->:  {}".format(input_data_dir))
        if not os.path.exists(input_data_dir):
            print("the following path is not valid. Please enter a valid path to python.exe")
            print("os.path.exists == False for: {}".format(input_data_dir))
    return input_data_dir
