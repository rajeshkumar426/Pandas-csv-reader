# from unittest.mock import patch
import pytest
import os

from fledge.plugins.south.TestPandasPlugin import TestPandasPlugin

__author__ = "Rajesh Kumar"
__copyright__ = "Copyright (c) 2018 Dianomic Systems"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

config = TestPandasPlugin._DEFAULT_CONFIG


def test_path_check():
    assert os.path.exists(TestPandasPlugin.file_path()) == 1


def test_plugin_contract():
    # Evaluates if the plugin has all the required methods
    assert callable(getattr(TestPandasPlugin, 'plugin_info'))
    assert callable(getattr(TestPandasPlugin, 'plugin_init'))
    assert callable(getattr(TestPandasPlugin, 'plugin_poll'))
    assert callable(getattr(TestPandasPlugin, 'plugin_shutdown'))
    assert callable(getattr(TestPandasPlugin, 'plugin_reconfigure'))
    assert callable(getattr(TestPandasPlugin, 'generate_data'))


def test_plugin_info():
    assert TestPandasPlugin.plugin_info() == {
        'name': 'Pandas CSV Reader',
        'version': '1.7.0',
        'mode': 'poll',
        'type': 'south',
        'interface': '1.0',
        'config': config
    }


def test_plugin_init():
    assert TestPandasPlugin.plugin_init(config) == config
