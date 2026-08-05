# ---------------------------------------------------------------------------------------------------
# Name: ftot_suppporting
#
# Purpose:
#
# ---------------------------------------------------------------------------------------------------
import os
import math

import logging
import datetime
import sqlite3
from ftot import ureg, Q_
from six import iteritems

# defining valid types for validating CSV input
# also imported into ftot_networkx in digraph_to_db
valid_commodity_types = [
    "agricultural_bulk",
    "containerized_freight",
    "aggregate_dry_bulk",
    "crude_oil",
    "petroleum_products",
    "liquid_bulk",
    "heavy_bulk",
    "roll_on_roll_off"
]

# <!--Create the logger -->
def create_loggers(dirLocation, task):
    """Create the logger"""

    loggingLocation = os.path.join(dirLocation, "logs")

    if not os.path.exists(loggingLocation):
        os.makedirs(loggingLocation)

    # BELOW ARE THE LOGGING LEVELS. WHATEVER YOU CHOOSE IN SETLEVEL WILL BE SHOWN ALONG WITH HIGHER LEVELS.
    # YOU CAN SET THIS FOR BOTH THE FILE LOG AND THE DOS WINDOW LOG
    # -----------------------------------------------------------------------------------------------------
    # CRITICAL       50
    # ERROR          40
    # WARNING        30
    # RESULT         25
    # CONFIG         24
    # INFO           20
    # RUNTIME        11
    # DEBUG          10
    # DETAILED_DEBUG  5

    logging.RESULT = 25
    logging.addLevelName(logging.RESULT, 'RESULT')

    logging.CONFIG = 19
    logging.addLevelName(logging.CONFIG, 'CONFIG')

    logging.RUNTIME = 11
    logging.addLevelName(logging.RUNTIME, 'RUNTIME')

    logging.DETAILED_DEBUG = 5
    logging.addLevelName(logging.DETAILED_DEBUG, 'DETAILED_DEBUG')

    logger = logging.getLogger('log')
    logger.setLevel(logging.DEBUG)

    logger.runtime = lambda msg, *args: logger._log(logging.RUNTIME, msg, args)
    logger.result = lambda msg, *args: logger._log(logging.RESULT, msg, args)
    logger.config = lambda msg, *args: logger._log(logging.CONFIG, msg, args)
    logger.detailed_debug = lambda msg, *args: logger._log(logging.DETAILED_DEBUG, msg, args)

    # FILE LOG
    # ------------------------------------------------------------------------------
    logFileName = task + "_" + "log_" + datetime.datetime.now().strftime("%Y_%m_%d_%H-%M-%S") + ".log"
    file_log = logging.FileHandler(os.path.join(loggingLocation, logFileName), mode='a', encoding='utf-8')

    file_log.setLevel(logging.DEBUG)


    file_log_format = logging.Formatter('%(asctime)s.%(msecs).03d %(levelname)-8s %(message)s',
                                        datefmt='%m-%d %H:%M:%S')
    file_log.setFormatter(file_log_format)

    # DOS WINDOW LOG
    # ------------------------------------------------------------------------------
    console = logging.StreamHandler()

    console.setLevel(logging.INFO)

    console_log_format = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M:%S')
    console.setFormatter(console_log_format)

    # ADD THE HANDLERS
    # ----------------
    logger.addHandler(file_log)
    logger.addHandler(console)

    # NOTE: with these custom levels you can now do the following
    # test this out once the handlers have been added
    # ------------------------------------------------------------

    return logger


# ==============================================================================


def clean_file_name(value):
    deletechars = r'\/:*?"<>|'
    for c in deletechars:
        value = value.replace(c, '')
    return value;


# ==============================================================================


def get_total_runtime_string(start_time):
    end_time = datetime.datetime.now()

    duration = end_time - start_time

    seconds = duration.total_seconds()

    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)

    hms = str("{:02}:{:02}:{:02}").format(hours, minutes, seconds)

    return hms

