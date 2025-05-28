#!/usr/bin/env python
import os
import time
import argparse
import sys

from datetime import datetime

from pvaserver import config
from pvaserver import adsimserver
from pvaserver import log
from pvaserver import __version__

def init(args):
    if not os.path.exists(str(args.config)):
        config.write(args.config)
    else:
        log.error("{0} already exists".format(args.config))

def run_status(args):

    config.log_values(args)

def run_sim(args):

    args.use_sim_data = True
    run_server(args)

def run_stack(args):

    args.use_sim_data = False
    args.data_stack = True
    run_server(args)

def run_tomo(args):

    args.use_sim_data = False
    args.data_stack = False # single hdf file
    run_server(args)

def run_server(args):

    server = adsimserver.AdSimServer(args)
    server.start()
    expectedRuntime = args.runtime+args.start_delay+server.SHUTDOWN_DELAY
    startTime = time.time()
    try:
        while True:
            time.sleep(1)
            now = time.time()
            runtime = now - startTime
            if runtime > expectedRuntime or server.isDone:
                break
    except KeyboardInterrupt as ex:
        pass
    server.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', **config.SECTIONS['general']['config'])
    parser.add_argument('--version', action='version',
                        version='%(prog)s {}'.format(__version__))

    pvaserver_sim_params   = config.PVASERVER_SIM_PARAMS
    pvaserver_stack_params = config.PVASERVER_STACK_PARAMS
    pvaserver_tomo_params  = config.PVASERVER_TOMO_PARAMS

    cmd_parsers = [
        ('init',        init,          (),                     "Usage: pvaserver init                        - Create configuration file and restore the original default values"),
        ('sim',         run_sim,       pvaserver_sim_params,   "Usage: pvaserver sim                         - Run the PVA server in simulation mode (-h for more options)"),
        ('stack',       run_stack,     pvaserver_stack_params, "Usage: pvaserver stack --file-path /data/    - Run the PVA server loading a stack of images from a folder (-h for more options)"),
        ('tomo',        run_tomo,      pvaserver_tomo_params,  "Usage: pvaserver tomo --file-name tomo.h5    - Run the PVA server loading a tomo dataset (-h for more options)"),
        ('status',      run_status,    pvaserver_tomo_params,  "Usage: pvaserver status                      - Show status"),
    ]

    subparsers = parser.add_subparsers(title="Commands", metavar='')

    for cmd, func, sections, text in cmd_parsers:
        cmd_params = config.Params(sections=sections)
        cmd_parser = subparsers.add_parser(cmd, help=text, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        cmd_parser = cmd_params.add_arguments(cmd_parser)
        cmd_parser.set_defaults(_func=func)

    args = config.parse_known_args(parser, subparser=True)

    # === Check for logs_home ===
    if not hasattr(args, 'logs_home'):
        parser.print_help()
        log.error("Missing required arguments or subcommand (e.g., sim/stack/tomo).")
        sys.exit(1)

    # create logger
    logs_home = args.logs_home

    # make sure logs directory exists
    if not os.path.exists(logs_home):
        os.makedirs(logs_home)

    lfname = os.path.join(logs_home, 'pvaserver_' + datetime.strftime(datetime.now(), "%Y-%m-%d_%H_%M_%S") + '.log')

    log.setup_custom_logger(lfname)
    log.info("Saving log at %s" % lfname)

    try:
        args._func(args)
    except RuntimeError as e:
        log.error(str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
