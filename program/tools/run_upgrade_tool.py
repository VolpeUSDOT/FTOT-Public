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

    # python environment
    print("run.bat configuration tool | step 1/5:")
    print("-------------------------------")
    print("python.exe location: (drag and drop is fine here)")
    python = ""
    while not os.path.exists(python):
        python = raw_input('----------------------> ')
        print("USER INPUT: the python path: {}".format(python))
        if not os.path.exists(python):
            print("warning: the following path is not valid. Please enter a valid path to python.exe")
            print("warning: os.path.exists == False for: {}".format(python))

    # FTOT program directory
    print("run.bat configuration tool | step 2/5:")
    print("-------------------------------")
    print("ftot.py location: (drag and drop is fine here)")
    ftot = ""
    while not os.path.exists(ftot):
        ftot = raw_input('----------------------> ')
        print("USER INPUT: the ftot.py path: {}".format(ftot))
        if not os.path.exists(ftot):
            print("warning: the following path is not valid. Please enter a valid path to ftot.py")
            print("warning: os.path.exists == False for: {}".format(ftot))

    # Scenario XML File
    print("run.bat configuration tool | step 3/5:")
    print("-------------------------------")
    print("scenario.xml location: (drag and drop is fine here)")
    scenario_xml= ""
    while not os.path.exists(scenario_xml):
        scenario_xml = raw_input('----------------------> ')
        print("USER INPUT: the scenario.xml path: {}".format(scenario_xml))
        if not os.path.exists(scenario_xml):
            print("warning: the following path is not valid. Please enter a valid path to scenario_xml")
            print("warning: os.path.exists == False for: {}".format(scenario_xml))

    # Candidate generation
    print("run.bat configuration tool | step 4/5:")
    print("-------------------------------")
    print("include candidate generation steps)")
    print("(1) YES")
    print("(2) NO")
    candidate_bool= ""
    quit = False
    while not quit:
        candidate_bool = raw_input('----------------------> ')
        print("USER INPUT: candidate_bool: {}".format(candidate_bool))
        if candidate_bool == "1":
            print("Candidate generation will be INCLUDED in the run configuration.")
            candidate_bool = True
            quit = True
        elif candidate_bool == "2":
            print("Candidate generation will be EXCLUDED in the run configuration.")
            candidate_bool = False
            quit = True
        else:
            print("warning: user response: {} is not recognized. Please enter a valid option (1 or 2).".format(candidate_bool))
            quit = False

    # Output file location
    print("run_v5.bat configuration tool | step 5/5:")
    print("--------------------------------------")
    print("run_v5.bat output directory location: (drag and drop is fine here)")
    output_dir = ""
    while not os.path.exists(output_dir):
        output_dir = raw_input('----------------------> ')
        print("USER INPUT: the scenario.xml path: {}".format(output_dir))
        if not os.path.exists(output_dir):
            print("warning: the following path is not valid. Please enter a valid path to output_dir")
            print("warning: os.path.exists == False for: {}".format(output_dir))

    return [python, ftot, scenario_xml, candidate_bool, output_dir]
# =======================================================================================================================


def save_the_new_run_bat_file(config_params):

    # unpack the config parameters
    python, ftot, scenario_xml, candidate_bool, output_dir = config_params

    run_bat_file = os.path.join(output_dir, "run_v5_1.bat")
    with open(run_bat_file, 'wb') as wf:
        print("writing the file: {} ".format(run_bat_file))

        # HEADER INFORMATION
        header_info = """
        @ECHO OFF
        cls
        set PYTHONDONTWRITEBYTECODE=1 
        REM   default is #ECHO OFF, cls (clear screen), and disable .pyc files 
        REM   for debugging REM @ECHO OFF line above to see commands 
        REM -------------------------------------------------

        REM run_v5_1.bat generated by FTOT run.bat upgrade tool
        REM =================================================
       
         
        REM ==============================================
        REM ======== ENVIRONMENT VARIABLES ===============
        REM ==============================================
        set PYTHON="{}"
        set FTOT="{}"
        set XMLSCENARIO="{}"
        """.format(python, ftot, scenario_xml)
        wf.writelines(header_info.replace("        ", "").replace("\n", "\r\n")) # remove the indentation white space

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
        REM  EXPORT THE FROM GIS TO NETWORKX MODULE
        %PYTHON% %FTOT% %XMLSCENARIO% c || exit /b

        REM  GRAPH: CREATE THE NETWORKX GRAPHS FROM THE
        REM  NETWORK AND FACILTIIES
        %PYTHON% %FTOT% %XMLSCENARIO% g || exit /b
        """

        wf.writelines(ftot_script_chunk_a.replace("        ", "").replace("\n", "\r\n")) # remove the indentation white space

        if candidate_bool == True:
            ftot_script_chunk_b = """

            REM OPTMIZATION: PRE-CANDIDATE GENERATION OPTIMIZATION SETUP DATABASE
            %PYTHON% %FTOT% %XMLSCENARIO% oc1 || exit /b
           
            REM OPTMIZATION: PRE-CANDIDATE GENERATION OPTIMIZATION DEFINE & SOLVE PROBLEM
            %PYTHON% %FTOT% %XMLSCENARIO% oc2 || exit /b
            
            REM OPTMIZATION: PRE-CANDIDATE GENERATION OPTIMIZATION POSTPROCESS
            %PYTHON% %FTOT% %XMLSCENARIO% oc3 || exit /b
                      
            REM  FACILITIES 2 : ADD FACILITIES LOCATIONS AND
            REM  COMMODITY DATA FILES TO THE SCENARIO
            %PYTHON% %FTOT% %XMLSCENARIO% f2 || exit /b
    
            REM  CONNECTIVITY 2: CONNECT THE FACILITIES TO THE NETWORK AND
            REM  EXPORT THE FROM GIS TO NETWORKX MODULE
            %PYTHON% %FTOT% %XMLSCENARIO% c2 || exit /b
    
            REM  GRAPH 2 : CREATE THE NETWORKX GRAPHS FROM THE
            REM  NETWORK AND FACILTIIES
            %PYTHON% %FTOT% %XMLSCENARIO% g2 || exit /b
            """
            wf.writelines(ftot_script_chunk_b.replace("            ", "").replace("\n", "\r\n")) # remove the indentation white space


        ftot_script_chunk_c = """
        REM STEP OPTIMIZATION: SET UP THE OPTIMIZATION PROBLEM
        %PYTHON% %FTOT% %XMLSCENARIO% o1 || exit /b

        REM STEP OPTIMIZATION: BUILD THE OPTIMIZATION PROBLEM AND SOLVE
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
        wf.writelines(ftot_script_chunk_c.replace("        ", "").replace("\n", "\r\n")) # remove the indentation white space

    print("file location: {}".format(run_bat_file))
    print("all done.")
