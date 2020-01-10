# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

""" Module for  reading csv data in chunks plugin """

import copy
import uuid
import logging
import os
import pandas as pd
from fledge.common import logger
from fledge.plugins.common import utils
_LOGGER = logger.setup(__name__, level=logging.INFO)


__author__ = "Rajesh Kumar"
__copyright__ = "Copyright (c) 2019 Dianomic Systems"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


_DEFAULT_CONFIG = {
    'plugin': {
        'description': 'Pandas CSV file Reader',
        'type': 'string',
        'default': 'TestPandasPlugin',
        'readonly': 'true'
    },
    'assetName': {
        'description': 'Name of Asset',
        'type': 'string',
        'default': 'TestPandasPlugin',
        'displayName': 'Asset name',
        'mandatory': 'true'
    }
}


def file_path():
    return os.path.join(FILE_PATH, 'train.csv')


FILE_PATH = '/home/foglamp'
FILE_NAME = os.path.join(FILE_PATH, 'train.csv')
CHUNK_SIZE = 1000
ATTRIBUTE_NAME = 'Units Sold'

try:
    df = pd.read_csv(FILE_NAME, iterator=True, chunksize=CHUNK_SIZE)
except (Exception, RuntimeError) as ex:
    _LOGGER.exception("Something wrong with file reading : {}".format(str(ex)))
    raise ex


def generate_data(attribute_name):
    global df
    try:
        element = next(df)
        if element is not None:
            return element[attribute_name].mean(), element[attribute_name].max()
        else:
            return 1, 2
    except StopIteration:
        df = pd.read_csv(FILE_NAME, iterator=True, chunksize=CHUNK_SIZE)
        element = next(df)
        return element[attribute_name].mean(), element[attribute_name].mean()


def plugin_info():
    """ Returns information about the plugin.
    Args:
    Returns:
        dict: plugin information
    Raises:
    """
    return {
        'name': 'Pandas CSV Reader',
        'version': '1.7.0',
        'mode': 'poll',
        'type': 'south',
        'interface': '1.0',
        'config': _DEFAULT_CONFIG
    }


def plugin_init(config):
    """ Initialise the plugin.
    Args:
        config: JSON configuration document for the South plugin configuration category
    Returns:
        data: JSON object to be used in future calls to the plugin
    Raises:
    """

    data = copy.deepcopy(config)
    return data


def plugin_poll(handle):
    """ Extracts data from the sensor and returns it in a JSON document as a Python dict.
    Available for poll mode only.
    Args:
        handle: handle returned by the plugin initialisation call
    Returns:
        returns a sensor reading in a JSON document, as a Python dict, if it is available
        None - If no reading is available
    Raises:
        Exception
    """
    try:
        time_stamp = utils.local_timestamp()
        mean_value, max_value = generate_data(ATTRIBUTE_NAME)
        data = {'asset':  handle['assetName']['value'], 'timestamp': time_stamp, 'key': str(uuid.uuid4()),
                'readings': {"mean_value": mean_value, "max_value": max_value}}
    except (Exception, RuntimeError) as ex:
        _LOGGER.exception("Pandas CSV Reader exception: {}".format(str(ex)))
        raise ex
    else:
        return data


def plugin_reconfigure(handle, new_config):
    """ Reconfigures the plugin

    Args:
        handle: handle returned by the plugin initialisation call
        new_config: JSON object representing the new configuration category for the category
    Returns:
        new_handle: new handle to be used in the future calls
    """
    _LOGGER.info("Old config for sinusoid plugin {} \n new config {}".format(handle, new_config))
    new_handle = copy.deepcopy(new_config)
    return new_handle


def plugin_shutdown(handle):
    """ Shutdowns the plugin doing required cleanup, to be called prior to the South plugin service being shut down.

    Args:
        handle: handle returned by the plugin initialisation call
    Returns:
        plugin shutdown
    """
    _LOGGER.info('Pandas Plugin shut down.')
