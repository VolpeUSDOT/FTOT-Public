# -------------------------------------------------------------------------------
# Name:        XML Text Replacement Tool
# Purpose:     Searches for all xml files in a directory and
#              replaces the text for a specified element
# -------------------------------------------------------------------------------

import os, sys
try:
    from lxml import etree, objectify
except ImportError:
    print ("This script requires the lxml Python library to validate the XML scenario file.")
    print("Download the library here: https://pypi.python.org/pypi/lxml/2.3")
    print("Exiting...")
    sys.exit()


# ==============================================================================

def run():
    print("FTOT XML text replacement tool")
    print("-------------------------------")
    print("")
    print("")
    xml_text_replacement()


# ==============================================================================

def xml_text_replacement():

    print("start: replace text in XMLs")
    print("get user inputs")

    # directory containing xmls where a certain element needs to be updated
    xml_directory_path = get_directory_path()

    while True:

        # xml element to update
        element_to_update = get_element_to_update()
        # new text to replace element's existing text
        new_text = get_new_text()

        element_path = ""
        path_chosen = False
        for subdir, dirs, files in os.walk(xml_directory_path):
            for file in files:
                ext = os.path.splitext(file)[-1].lower()
                if ext == ".xml":
                    full_path_to_xml = os.path.join(subdir, file)
                    xml_etree = load_scenario_config_file(full_path_to_xml)
                    print(file)
                    # identify specific element path
                    # only runs until a valid path is found for one file
                    if not path_chosen:
                        matches = get_all_matches(xml_etree, element_to_update)
                        if len(matches) > 1:
                            print("multiple element matches found. please select the desired element path:")
                            element_path = select_from_menu(matches)
                            path_chosen = True
                        elif len(matches) == 1:
                            element_path = matches[0]
                            path_chosen = True
                    do_the_update(xml_etree, element_path, new_text, full_path_to_xml)

        choice = ""
        while choice not in ["y","n"]:
            print('replace another xml element in this directory? y or n')
            choice = raw_input(">> ")
            if choice.lower() == 'y':
                continue
            elif choice.lower() == 'n':
                return


# ==============================================================================

def select_from_menu(matches):
    # display a menu with cleaned element paths
    # and record path selected by user
    #-------------------------------------------

    for path in matches:
        clean_path = clean_element_path(path)
        print("[" + str(matches.index(path)) + "] " + clean_path)
    choice = raw_input(">> ")
    try:
        if int(choice) < 0: raise ValueError
        return matches[int(choice)]
    except (ValueError, IndexError):
        print("not a valid option. please enter a number from the menu.")
        return select_from_menu(matches)


# ==============================================================================

def load_scenario_config_file(fullPathToXmlConfigFile):

    if not os.path.exists(fullPathToXmlConfigFile):
        raise IOError("XML Scenario File {} not found at specified location.".format(fullPathToXmlConfigFile))

    if fullPathToXmlConfigFile.rfind(".xml") < 0:
        raise IOError("XML Scenario File {} is not an XML file type.".format(fullPathToXmlConfigFile))

    parser = etree.XMLParser(remove_blank_text=True)
    return etree.parse(fullPathToXmlConfigFile, parser)


# ==============================================================================

def clean_element_name(element):
    # remove namespace from front of tag, e.g. {FTOT}Scenario --> Scenario
    # takes an Element or a String as input
    #---------------------------------------------------------------------

    if type(element) is str:
        ns_i = element.find("}")
        return element[ns_i + 1:]
    else:
        ns_i = element.tag.find("}")
        return element.tag[ns_i + 1:]


# ==============================================================================

def clean_element_path(path):
    # remove namespace from front of elements in path (formatted as string)
    # -----------------------------------------------------------------------

    tagged_elements = path.split("/")
    clean_elements = [clean_element_name(elem) for elem in tagged_elements]
    return "./" + "/".join(clean_elements)


# ==============================================================================

def do_the_update(xml_etree, element_path, new_text, full_path_to_xml):
    # update text of element at specified path
    # ----------------------------------------

    try:
        target_elem = xml_etree.findall(element_path)[0]
    except (SyntaxError, IndexError):
        print("...warning: element not found. no changes made.")
        return

    clean_path = clean_element_path(element_path)
    print("element path = {}".format(clean_path))
    if len(target_elem) != 0:
        raise ValueError("Update stopped. This XML element contains subelements. Text cannot be replaced.")
    else:
        print("updating the text:")
        print "old text = {}".format(target_elem.text)
        target_elem.text = new_text
        print "new text = {}".format(target_elem.text)

    save_the_xml_file(full_path_to_xml, xml_etree)


# ==============================================================================

def save_the_xml_file(full_path_to_xml, the_temp_etree):

    with open(full_path_to_xml, 'wb') as wf:
        print("writing the file: {} ".format(full_path_to_xml))
        the_temp_etree.write(wf, pretty_print=True)
        print("done writing xml file: {}".format(full_path_to_xml))


# ==============================================================================

def get_all_matches(xml_etree, element_to_update):
    # load xml and iterate through all elements to identify possible matches
    # return list of candidate paths to elements
    #-----------------------------------------------------------------------

    match_paths = []
    item_counter = 0
    for temp_elem in xml_etree.getiterator():
        item_counter += 1

        # check if the temp_element has the .find attribute
        # if it does, then search for the index
        # to '{' char at the end of namespace (e.g. {FTOT}Scenario)
        # if there is no attribute, we continue b/c its probably a comment.
        if not hasattr(temp_elem.tag, 'find'):
            continue
        clean_temp_elem = clean_element_name(temp_elem)

        # ignore the element if its text is just white space
        if temp_elem.text == "":
            continue

        # store element path if element is a match
        if clean_temp_elem.lower() == element_to_update.lower():
            new_path = xml_etree.getelementpath(temp_elem)
            match_paths.append(new_path)

    return match_paths


# ==============================================================================

def get_directory_path():

    print("directory containing XMLs where a certain element needs to be updated: (drag and drop is fine here)")
    xml_dir = ""
    while not os.path.exists(xml_dir):
        xml_dir = raw_input('----------------------> ')
        print("USER INPUT ----------------->:  {}".format(xml_dir))
        if not os.path.exists(xml_dir):
            print("Path is not valid. Please enter a valid directory path.")
    return xml_dir


# ==============================================================================

def get_element_to_update():

    print('specify XML element to update:')
    element = raw_input('----------------------> ')
    print("USER INPUT ----------------->:  {}".format(element))
    return element


# ==============================================================================

def get_new_text():

    print('specify new text to replace element\'s existing text:')
    text = raw_input('----------------------> ')
    print("USER INPUT ----------------->:  {}".format(text))
    return text