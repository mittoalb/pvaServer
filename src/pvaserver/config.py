import os
import sys
import shutil
import pathlib
import argparse
import configparser
import numpy as np
from pathlib import Path

from collections import OrderedDict
from datetime import datetime

from pvaserver import log
from pvaserver import util
from pvaserver import __version__

LOGS_HOME = os.path.join(str(pathlib.Path.home()), 'logs')
CONFIG_FILE_NAME = os.path.join(str(pathlib.Path.home()), 'logs/energy.conf')

SECTIONS = OrderedDict()

SECTIONS['general'] = {
    'config': {
        'default': CONFIG_FILE_NAME,
        'type': str,
        'help': "File name of configuration file",
        'metavar': 'FILE'},
    'logs-home': {
        'default': LOGS_HOME,
        'type': str,
        'help': "Log file directory",
        'metavar': 'FILE'},
    'verbose': {
        'default': False,
        'help': 'Verbose output',
        'action': 'store_true'},
    'testing': {
        'default': False,
        'help': 'Enable test mode to show new motor positions. The motors will not move',
        'action': 'store_true'},
    'force': {
        'default': False,
        'help': 'When set the enegy change will occurs without a confirmation request',
        'action': 'store_true'
        },
    }

SECTIONS['energy'] = {
    'mode': {
        'default': 'Mono',
        'type': str,
        'help': "beamline energy mode",
        'choices': ['Mono','Pink']
        },
    'energy': {
        'default': -1,
        'type': float,
        'help': "Desired energy. Default (-1) = Pink beam"
        },
    }

SECTIONS['init'] = {
    'beamline': {
        'default': '32id',
        'type': str,
        'help': "This parameter is used to select the energy---.json file, e.g. energy2bm.json. This file must be created in the DATA_PATH_LOCAL directory",
        'choices': ['None','2bm', '7bm', '8id', '20bm', '29id', '32id']
        },
    'energyioc-prefix':{
        'default': '32id:Energy:',
        'type': str,
        'help': "The epics IOC hosting the Energy PV, i.e.'2bm:Energy:' "
        },
    'n-move': {
        'default': 16,
        'type': int,
        'help': "Number of PVs that will be used in move interpolation; 32id=16, 2bm=16",
        },
    'n-pos': {
        'default': 40,
        'type': int,
        'help': "Number of PVs that will be used to store motor position; 32id=40, 2bm=3",
        },
    }

MONO_PARAMS = ('init','energy', )
INIT_PARAMS = ('init', )

NICE_NAMES = ('General', 'Energy', 'Init')

def get_config_name():
    """Get the command line --config option."""
    name = CONFIG_FILE_NAME
    for i, arg in enumerate(sys.argv):
        if arg.startswith('--config'):
            if arg == '--config':
                return sys.argv[i + 1]
            else:
                name = sys.argv[i].split('--config')[1]
                if name[0] == '=':
                    name = name[1:]
                return name
    return name

def parse_known_args(parser, subparser=False):
    """
    Parse arguments from file and then override by the ones specified on the
    command line. Use *parser* for parsing and is *subparser* is True take into
    account that there is a value on the command line specifying the subparser.
    """
    if len(sys.argv) > 1:
        subparser_value = [sys.argv[1]] if subparser else []
        config_values = config_to_list(config_name=get_config_name())
        values = subparser_value + config_values + sys.argv[1:]
        #print(subparser_value, config_values, values)
    else:
        values = ""

    return parser.parse_known_args(values)[0]

def config_to_list(config_name=CONFIG_FILE_NAME):
    """
    Read arguments from config file and convert them to a list of keys and
    values as sys.argv does when they are specified on the command line.
    *config_name* is the file name of the config file.
    """
    result = []
    config = configparser.ConfigParser()

    if not config.read([config_name]):
        return []

    for section in SECTIONS:
        for name, opts in ((n, o) for n, o in SECTIONS[section].items() if config.has_option(section, n)):
            value = config.get(section, name)

            if value != '' and value != 'None':
                action = opts.get('action', None)

                if action == 'store_true' and value == 'True':
                    # Only the key is on the command line for this action
                    result.append('--{}'.format(name))

                if not action == 'store_true':
                    if opts.get('nargs', None) == '+':
                        result.append('--{}'.format(name))
                        result.extend((v.strip() for v in value.split(',')))
                    else:
                        result.append('--{}={}'.format(name, value))

    return result
  
class Params(object):
    def __init__(self, sections=()):
        self.sections = sections + ('general', )

    def add_parser_args(self, parser):
        for section in self.sections:
            for name in sorted(SECTIONS[section]):
                opts = SECTIONS[section][name]
                parser.add_argument('--{}'.format(name), **opts)

    def add_arguments(self, parser):
        self.add_parser_args(parser)
        return parser

    def get_defaults(self):
        parser = argparse.ArgumentParser()
        self.add_arguments(parser)

        return parser.parse_args('')

def write(config_file, args=None, sections=None):
    """
    Write *config_file* with values from *args* if they are specified,
    otherwise use the defaults. If *sections* are specified, write values from
    *args* only to those sections, use the defaults on the remaining ones.
    """
    config = configparser.ConfigParser()
    for section in SECTIONS:
        config.add_section(section)
        for name, opts in SECTIONS[section].items():
            if args and sections and section in sections and hasattr(args, name.replace('-', '_')):
                value = getattr(args, name.replace('-', '_'))
                if isinstance(value, list):
                    # print(type(value), value)
                    value = ', '.join(value)
            else:
                value = opts['default'] if opts['default'] is not None else ''

            prefix = '# ' if value == '' else ''

            if name != 'config':
                config.set(section, prefix + name, str(value))
    # print(args.energy_value)
    with open(config_file, 'w') as f:        
        config.write(f)

def log_values(args):
    """Log all values set in the args namespace.

    Arguments are grouped according to their section and logged alphabetically
    using the DEBUG log level thus --verbose is required.
    """
    args = args.__dict__

    log.warning('energy status start')
    for section, name in zip(SECTIONS, NICE_NAMES):
        entries = sorted((k for k in args.keys() if k.replace('_', '-') in SECTIONS[section]))

        # print('log_values', section, name, entries)
        if entries:
            log.info(name)

            for entry in entries:
                value = args[entry] if args[entry] is not None else "-"
                if (value == 'none'):
                    log.warning("  {:<16} {}".format(entry, value))
                elif (value is not False):
                    log.info("  {:<16} {}".format(entry, value))
                elif (value is False):
                    log.warning("  {:<16} {}".format(entry, value))

    log.warning('energy status end')

def save_params_to_config(args):
    # Update current status in default config file.
    # The default confign file name is set in CONFIG_FILE_NAME
    sections = MONO_PARAMS
    write(CONFIG_FILE_NAME, args=args, sections=sections)
    log.info('  *** update config file: %s ' % (CONFIG_FILE_NAME))
    
