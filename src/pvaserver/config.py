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
from pvaserver import __version__

LOGS_HOME = os.path.join(str(pathlib.Path.home()), 'logs')
CONFIG_FILE_NAME = os.path.join(str(pathlib.Path.home()), 'logs/pvaserver.conf')

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
    'version': {
        'default': __version__,
        'action': 'version'},
    }


SECTIONS['server'] = { 
    'frame-rate': {
        'default': 20,
        'type': float,
        'help': "Frames per second"
        },
   'n-frames': {
        'default': 0,
        'type': int,
        'help': "Number of different frames to generate from the input sources; if set to <= 0, the server will use all images found in input files, or it will generate enough images to fill up the image cache if no input files were specified. If the requested number of input frames is greater than the cache size, the server will stop publishing after exhausting generated frames; otherwise, the generated frames will be constantly recycled and republished.",
        },
    'cache-size': {
        'default': 1000,
        'type': int,
        'help': "Number of different frames to cache; if the cache size is smaller than the number of input frames, the new frames will be constantly regenerated as cached ones are published; otherwise, cached frames will be published over and over again as long as the server is running.",
        },
    'runtime': {
        'default': 300,
        'type': float,
        'help': "Server runtime in seconds"
        },
    'channel-name': {
        'default': 'pvapy:image',
        'type': str,
        'help': "Server PVA channel name",
        },
    'notify-pv': {
        'default': None,
        'type': str,
        'help': "CA channel that should be notified on start; for the default Area Detector PVA driver PV that controls image acquisition is 13PVA1:cam1:Acquire",
        },
    'notify-pv-value': {
        'default': 1,
        'type': str,
        'help': "Value for the notification channel; for the Area Detector PVA driver PV this should be set to 'Acquire'",
        },
    'metadata-pv': {
        'default': None,
        'type': str,
        'help': "Comma-separated list of CA channels that should be contain simulated image metadata values",
        },
    'start-delay': {
        'default': 10.0,
        'type': float,
        'help': "Server start delay in seconds"
        },
    'report-period': {
        'default': 1,
        'type': int,
        'help': "Reporting period for publishing frames; if set to <=0 no frames will be reported as published",
        },
    'disable-curses': {
        'default': False,
        'help': 'Disable curses library screen handling. This is enabled by default, except when logging into standard output is turned on.',
        'action': 'store_true'
        },
    'use-sim-data': {
        'default': True,
        'help': 'Set to True when auto generate random data; False when reading data from a directory or a file',
        },

    }


SECTIONS['sim'] = { 
    'minimum': {
        'default': None,
        'type': float,
        'help': "Minimum generated value"
        },
    'maximum': {
        'default': None,
        'type': float,
        'help': "Maximum generated value"
        },    
    'n-x-pixels': {
        'default': 256,
        'type': int,
        'help': "Number of pixels in x dimension",
        },
    'n-y-pixels': {
        'default': 256,
        'type': int,
        'help': "Number of pixels in x dimension",
        },
    'datatype': {
        'default': 'uint8',
        'type': str,
        'help': "Generated datatype",
        'choices': ['int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32', 'float32', 'float64']
        },
    }

SECTIONS['file'] = { 
    'file-name':{
        'default': None,
        'type': str,
        'help': "Input file or folder name containing the images to be streamed."
        },
    'file-format': {
        'default': 'hdf',
        'type': str,
        'help': "File format of the file to be read",
        'choices': ['h5', 'npy', 'tiff'],
        },    
    }

SECTIONS['hdf'] = { 
    'hdf-dataset': {
        'default': '/exchange/data/',
        'help': 'HDF5 dataset path. This option must be specified if HDF5 files are used as input, but otherwise it is ignored.',
        },
    'hdf-compression-mode': {
        'default': False,
        'help': 'Use compressed data from HDF5 file. By default, data will be uncompressed before streaming it.',
        'action': 'store_true'
        },
    }

SECTIONS['npy'] = { 
    'mmap-mode': {
        'default': False,
        'help': 'Use NumPy memory map to load the specified input file. This flag typically results in faster startup and lower memory usage for large files.',
        'action': 'store_true'
        },
    }

PVASERVER_SIM_PARAMS  = ('server', 'sim')
PVASERVER_TOMO_PARAMS = ('server', 'file', 'hdf', 'npy')

NICE_NAMES = ('General', 'Server', 'Simulation', 'File', 'hdf', 'npy')

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
    
