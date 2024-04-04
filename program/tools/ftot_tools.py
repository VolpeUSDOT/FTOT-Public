import os
import lxml_upgrade_tool
import run_upgrade_tool
import input_csv_templates_tool
import scenario_compare_tool
import gridded_data_tool
import xml_text_replacement_tool
import network_disruption_tool
import network_validation_tool as nvt
import scenario_setup_conversion_tool as ssct
from six.moves import input

FTOT_VERSION = "2024.1"
SCHEMA_VERSION = "7.0.4"
VERSION_DATE = "4/3/2024"

header = "\n\
 _______  _______  _______  _______    _______  _______  _______  ___      _______ \n\
|       ||       ||       ||       |  |       ||       ||       ||   |    |       |\n\
|    ___||_     _||   _   ||_     _|  |_     _||   _   ||   _   ||   |    |  _____|\n\
|   |___   |   |  |  | |  |  |   |      |   |  |  | |  ||  | |  ||   |    | |_____ \n\
|    ___|  |   |  |  |_|  |  |   |      |   |  |  |_|  ||  |_|  ||   |___ |_____  |\n\
|   |      |   |  |       |  |   |      |   |  |       ||       ||       | _____| |\n\
|___|      |___|  |_______|  |___|      |___|  |_______||_______||_______||_______|\n"


def xml_tool():
    print("You called the XML tool")
    xml_file_location = lxml_upgrade_tool.repl()
    input("Press [Enter] to continue...")


def bat_tool():
    print("You called the bat tool")
    run_upgrade_tool.run_bat_upgrade_tool()
    input("Press [Enter] to continue...")


def csv_tool():
    print("You called the generate template CSV files tool")
    input_csv_templates_tool.run_input_csv_templates_tool()
    input("Press [Enter] to continue...")


def replace_xml_text_tool():
    print("You called the replace XML text tool")
    xml_text_replacement_tool.run()
    input("Press [Enter] to continue...")


def compare_tool():
    print("You called the scenario compare tool")
    scenario_compare_tool.run_scenario_compare_prep_tool()
    input("Press [Enter] to continue...")


def raster_tool():
    print("You called the aggregate raster data tool")
    gridded_data_tool.run()
    input("Press [Enter] to continue...")


def disrupt_tool():
    print("You called the network disruption tool")
    network_disruption_tool.run_network_disruption_tool()
    input("Press [Enter] to continue...")


def network_validation_tool():
    print("You called the network validation tool")
    nvt.run()
    input("Press [Enter] to continue...")


def scenario_setup_conversion_tool():
    print("You called the scenario setup conversion tool")
    ssct.run()
    input("Press [Enter] to continue...")


def help_tool():
    print("-----------------------------------------")
    print("xml_tool:")
    print("This tool has two options. You can either generate a default XML scenario file from scratch, or update an old XML scenario file to the current schema version, which is " + SCHEMA_VERSION + ".")
    print("-----------------------------------------")

    print("-----------------------------------------")
    print("bat_tool:")
    print("This tool generates a default bat file based on the inputted scenario XML file. You can choose whether to include candidate generation steps.")
    print("-----------------------------------------")

    print("-----------------------------------------")
    print("generate_template_csv_files:")
    print("This tool generates facility CSV templates. You can specify which facility types to generate and add optional input fields.")
    print("-----------------------------------------")

    print("-----------------------------------------")
    print("replace_xml_text:")
    print("This tool modifies specific XML fields for scenarios in a specified folder. If XML files with different schema versions are in the same folder, only files sharing the schema version of the first file alphabetically will get modified.")
    print("-----------------------------------------")

    print("-----------------------------------------")
    print("scenario_compare_tool:")
    print("This tool combines results from multiple scenarios into a single Tableau workbook.")
    print("-----------------------------------------")

    print("-----------------------------------------")
    print("aggregate_raster_data:")
    print("This tool aggregates grid cell production data at a county level.")
    print("-----------------------------------------")

    print("-----------------------------------------")
    print("network_disruption_tool:")
    print("This tool allows you to automatically generate a network disruption CSV associated with a hazard scenario.")
    print("It must be used with raster-based GIS data that identifies exposure levels due to some sort of hazard.")
    print("-----------------------------------------")
   
    print("-----------------------------------------")
    print("network_validation_tool:")
    print("This tool allows you to conduct preliminary validation on custom networks to help ensure they follow the necessary schema.")
    print("-----------------------------------------")
    
    print("-----------------------------------------")
    print("scenario_setup_conversion_tool:")
    print("This tool allows you to convert a scenario setup template filled out by the user into XML and CSV input files for an FTOT run.")
    print("-----------------------------------------")
    
    input("Press [Enter] to continue...")


menuItems = [
    {"xml_tool": xml_tool},
    {"bat_tool": bat_tool},
    {"generate_template_csv_files": csv_tool},
    {"replace_xml_text": replace_xml_text_tool},
    {"scenario_compare_tool": compare_tool},
    {"aggregate_raster_data": raster_tool},
    {"network_disruption_tool": disrupt_tool},
    {"network_validation_tool": network_validation_tool},
    {"scenario_setup_conversion_tool": scenario_setup_conversion_tool},
    {"help": help_tool},
    {"exit": exit}
]


def main():
    while True:
        os.system('cls')
        print(header)
        print('Version Number: ' + FTOT_VERSION + ", (" + VERSION_DATE + ")")
        print('-----------------------------------------')
        for item in menuItems:
            print("[" + str(menuItems.index(item)+1) + "] " + list(item.keys())[0])
        print('-----------------------------------------')
        print('Enter a number below to activate a tool')
        choice = input(">> ")
        try:
            if int(choice) < 0:
                raise ValueError
            # Call the matching function
            list(menuItems[int(choice)-1].values())[0]()
        except (ValueError, IndexError):
            pass


if __name__ == "__main__":
    main()
