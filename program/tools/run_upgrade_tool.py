# run.bat upgrade tool

# the run.bat upgrade tool is used to generate run.bat files for the user
# it is a command-line-interface (CLI) tool that prompts the user for the following information:
# - Python installation
# - FTOT program directory
# - Scenario XML file
# - Candidate generation? Y/N
# - Output file location
# ==============================================================================

import os
from six.moves import input

# ==============================================================================

def run_bat_upgrade_tool():
    os.system('CLS')
    print("FTOT run.bat configuration tool")
    print("-------------------------------")
    print("")
    print("")

    config_params = get_user_configs()

    save_the_new_run_bat_file(config_params)


# ==============================================================================

def get_user_configs():
    
    # Python environment
    python = "C:\\FTOT\\python3_env\\python.exe"
    print("setting python path: {}".format(python))
    while not os.path.exists(python):
        python = input('----------------------> ')
        print("USER INPUT: the python path: {}".format(python))
        if not os.path.exists(python):
            print("warning: The following path is not valid. Please enter a valid path to python.exe.")
            print("warning: os.path.exists == False for: {}".format(python))

    # FTOT program directory
    ftot = "C:\\FTOT\\program\\ftot.py"  # ftot.py location
    print("setting ftot path: {}".format(ftot))
    while not os.path.exists(ftot):
        ftot = input('----------------------> ')
        print("USER INPUT: the ftot.py path: {}".format(ftot))
        if not os.path.exists(ftot):
            print("warning: The following path is not valid. Please enter a valid path to ftot.py.")
            print("warning: os.path.exists == False for: {}".format(ftot))

    # Scenario XML file
    print("-------------------------------")
    print("scenario.xml file path: (drag and drop is fine here)")
    scenario_xml= ""
    while not os.path.exists(scenario_xml):
        scenario_xml = input('----------------------> ').strip('\"')
        print("USER INPUT: the scenario.xml path: {}".format(scenario_xml))
        if not os.path.exists(scenario_xml):
            print("warning: The following path is not valid. Please enter a valid path to a scenario XML file.")
            print("warning: os.path.exists == False for: {}".format(scenario_xml))

    # Candidate generation
    print("-------------------------------")
    print("Do you want to include candidate generation steps?")
    include = input('----------------------> ')
    yes_no = False
    while yes_no == False:
        if (include.lower() == 'y' or include.lower()=='yes'):
            print("Candidate generation will be INCLUDED in the run configuration.")
            candidate_bool = True
            yes_no = True
        elif (include.lower() == 'n' or include.lower()=='no'):
            print("Candidate generation will be EXCLUDED in the run configuration.")
            candidate_bool = False
            yes_no = True
        else:
            print("Invalid input")
            print("Please enter yes or no")
            include = input('----------------------> ')

    # Output file location
    print("--------------------------------------")
    print("location for the new .bat file: (full folder path)")
    output_dir = ""
    while not os.path.exists(output_dir):
        output_dir = input('----------------------> ').strip('\"')
        print("USER INPUT: the folder path: {}".format(output_dir))
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    return [python, ftot, scenario_xml, candidate_bool, output_dir]


# =======================================================================================================================

def save_the_new_run_bat_file(config_params):

    # unpack the config parameters
    python, ftot, scenario_xml, candidate_bool, output_dir = config_params

    run_bat_file = os.path.join(output_dir, "run_v7.bat")

    if os.path.exists(run_bat_file):
        print(run_bat_file + " already exists")
        print("Do you want to overwrite this file?")
        overwrite = input('----------------------> ')
        yes_no = False
        while yes_no == False:
            if (overwrite.lower() == 'y' or overwrite.lower()=='yes'):
                yes_no = True
                write_bat(python, ftot, scenario_xml, candidate_bool, output_dir, run_bat_file)
            elif (overwrite.lower() == 'n' or overwrite.lower()=='no'):
                yes_no = True
                print("new bat file not written")
            else:
                print("Invalid input")
                print("Do you want to overwrite this file?")
                overwrite = input('----------------------> ')
    else:
        write_bat(python, ftot, scenario_xml, candidate_bool, output_dir, run_bat_file)

    print("file location: {}".format(run_bat_file))
    print("all done")


# =======================================================================================================================

def write_bat(python, ftot, scenario_xml, candidate_bool, output_dir, run_bat_file):
    with open(run_bat_file, 'w') as wf:
        print("writing the file: {}".format(run_bat_file))

        # HEADER INFORMATION
        header_info = """
        @ECHO OFF
        cls
        set PYTHONDONTWRITEBYTECODE=1 
        REM   default is #ECHO OFF, cls (clear screen), and disable .pyc files 
        REM   for debugging REM @ECHO OFF line above to see commands 
        REM -------------------------------------------------

        REM run_v7.bat generated by FTOT run.bat upgrade tool
        REM =================================================
       
         
        REM ==============================================
        REM ======== ENVIRONMENT VARIABLES ===============
        REM ==============================================
        set PYTHON="{}"
        set FTOT="{}"
        set XMLSCENARIO="{}"
        """.format(python, ftot, scenario_xml)
        wf.writelines(header_info.replace("        ", ""))  # remove the indentation white space

        ftot_script_chunk_a = """
        
        REM ==============================================
        REM ======== RUN THE FTOT SCRIPT =================
        REM ==============================================

        REM  SETUP: SETUP FTOT FOR THE SCENARIO
        %PYTHON% %FTOT% %XMLSCENARIO% s || exit /b

        REM  FACILITIES: ADD FACILITIES LOCATIONS AND
        REM  COMMODITY DATA FILES TO THE SCENARIO
        %PYTHON% %FTOT% %XMLSCENARIO% f || exit /b

        REM  CONNECTIVITY: CONNECT THE FACILITIES TO THE NETWORK AND
        REM  EXPORT FROM GIS TO NETWORKX MODULE
        %PYTHON% %FTOT% %XMLSCENARIO% c || exit /b

        REM  GRAPH: CREATE THE NETWORKX GRAPHS FROM THE
        REM  NETWORK AND FACILITIES
        %PYTHON% %FTOT% %XMLSCENARIO% g || exit /b
        """

        wf.writelines(ftot_script_chunk_a.replace("        ", ""))  # remove the indentation white space

        if candidate_bool == True:
            ftot_script_chunk_b = """
            REM  OPTIMIZATION: PRE-CANDIDATE GENERATION OPTIMIZATION
            %PYTHON% %FTOT% %XMLSCENARIO% oc || exit /b
            
            REM  FACILITIES 2: ADD FACILITIES LOCATIONS AND
            REM  COMMODITY DATA FILES TO THE SCENARIO
            %PYTHON% %FTOT% %XMLSCENARIO% f2 || exit /b

            REM  CONNECTIVITY 2: CONNECT THE FACILITIES TO THE NETWORK AND
            REM  EXPORT FROM GIS TO NETWORKX MODULE
            %PYTHON% %FTOT% %XMLSCENARIO% c2 || exit /b
            
            REM  GRAPH 2: CREATE THE NETWORKX GRAPHS FROM THE
            REM  NETWORK AND FACILITIES
            %PYTHON% %FTOT% %XMLSCENARIO% g2 || exit /b
            """
            wf.writelines(ftot_script_chunk_b.replace("            ", ""))  # remove the indentation white space

        ftot_script_chunk_c = """
        REM  OPTIMIZATION: SET UP THE OPTIMIZATION PROBLEM
        %PYTHON% %FTOT% %XMLSCENARIO% o1 || exit /b

        REM  OPTIMIZATION: BUILD THE OPTIMIZATION PROBLEM AND SOLVE
        %PYTHON% %FTOT% %XMLSCENARIO% o2 || exit /b

        REM  POST-PROCESSING: POST PROCESS OPTIMAL RESULTS AND PREPARE REPORTING
        %PYTHON% %FTOT% %XMLSCENARIO% p || exit /b

        REM  REPORT: CREATE REPORTS OF THE CONFIGURATION, RESULTS, AND
        REM  WARNINGS FROM THE RUN
        %PYTHON% %FTOT% %XMLSCENARIO% d || exit /b

        REM  MAPS: GENERATE MAPS OF THE SCENARIO FOR EACH STEP
        %PYTHON% %FTOT% %XMLSCENARIO% m || exit /b

        REM  OPTIONAL MAPPING STEP FOR TIME AND COMMODITY DISTANCES
        REM %PYTHON% %FTOT% %XMLSCENARIO% m2 || exit /b
        """
        wf.writelines(ftot_script_chunk_c.replace("        ", ""))  # remove the indentation white space

