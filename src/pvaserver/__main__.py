#!/usr/bin/env python
import argparse

from pvaserver import config
from pvaserver import adsimserver
from pvaserver import log
from pvaserver import __version__

def init(args):
    if not os.path.exists(str(args.config)):
        sections = config.INIT_PARAMS
        config.write(args.config, args=args, sections=sections)
        dataio.init_preset(args)
    else:
        log.error("{0} already exists".format(args.config))

def run_server(args):

    a = 0

def main():
    parser = argparse.ArgumentParser(description='PvaPy Area Detector Simulator')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s {version}'.format(version=__version__))
    parser.add_argument('-id', '--input-directory', type=str, dest='input_directory', default=None, help='Directory containing input files to be streamed; if input directory or input file are not provided, random images will be generated')
    parser.add_argument('-if', '--input-file', type=str, dest='input_file', default=None, help='Input file to be streamed; if input directory or input file are not provided, random images will be generated')
    parser.add_argument('-mm', '--mmap-mode', action='store_true', dest='mmap_mode', default=False, help='Use NumPy memory map to load the specified input file. This flag typically results in faster startup and lower memory usage for large files.')
    parser.add_argument('-hds', '--hdf-dataset', dest='hdf_dataset', default=None, help='HDF5 dataset path. This option must be specified if HDF5 files are used as input, but otherwise it is ignored.')
    parser.add_argument('-hcm', '--hdf-compression-mode', dest='hdf_compression_mode', default=False, action='store_true', help='Use compressed data from HDF5 file. By default, data will be uncompressed before streaming it.')
    parser.add_argument('-fps', '--frame-rate', type=float, dest='frame_rate', default=20, help='Frames per second (default: 20 fps)')
    parser.add_argument('-nx', '--n-x-pixels', type=int, dest='n_x_pixels', default=256, help='Number of pixels in x dimension (default: 256 pixels; does not apply if input file file is given)')
    parser.add_argument('-ny', '--n-y-pixels', type=int, dest='n_y_pixels', default=256, help='Number of pixels in x dimension (default: 256 pixels; does not apply if input file is given)')
    parser.add_argument('-dt', '--datatype', type=str, dest='datatype', default='uint8', help='Generated datatype. Possible options are int8, uint8, int16, uint16, int32, uint32, float32, float64 (default: uint8; does not apply if input file is given)')
    parser.add_argument('-mn', '--minimum', type=float, dest='minimum', default=None, help='Minimum generated value (does not apply if input file is given)')
    parser.add_argument('-mx', '--maximum', type=float, dest='maximum', default=None, help='Maximum generated value (does not apply if input file is given)')
    parser.add_argument('-nf', '--n-frames', type=int, dest='n_frames', default=0, help='Number of different frames to generate from the input sources; if set to <= 0, the server will use all images found in input files, or it will generate enough images to fill up the image cache if no input files were specified. If the requested number of input frames is greater than the cache size, the server will stop publishing after exhausting generated frames; otherwise, the generated frames will be constantly recycled and republished.')
    parser.add_argument('-cs', '--cache-size', type=int, dest='cache_size', default=1000, help='Number of different frames to cache (default: 1000); if the cache size is smaller than the number of input frames, the new frames will be constantly regenerated as cached ones are published; otherwise, cached frames will be published over and over again as long as the server is running.')
    parser.add_argument('-rt', '--runtime', type=float, dest='runtime', default=300, help='Server runtime in seconds (default: 300 seconds)')
    parser.add_argument('-cn', '--channel-name', type=str, dest='channel_name', default='pvapy:image', help='Server PVA channel name (default: pvapy:image)')
    parser.add_argument('-npv', '--notify-pv', type=str, dest='notify_pv', default=None, help='CA channel that should be notified on start; for the default Area Detector PVA driver PV that controls image acquisition is 13PVA1:cam1:Acquire')
    parser.add_argument('-nvl', '--notify-pv-value', type=str, dest='notify_pv_value', default='1', help='Value for the notification channel; for the Area Detector PVA driver PV this should be set to "Acquire" (default: 1)')
    parser.add_argument('-mpv', '--metadata-pv', type=str, dest='metadata_pv', default=None, help='Comma-separated list of CA channels that should be contain simulated image metadata values')
    parser.add_argument('-sd', '--start-delay', type=float, dest='start_delay',  default=10.0, help='Server start delay in seconds (default: 10 seconds)')
    parser.add_argument('-rp', '--report-period', type=int, dest='report_period', default=1, help='Reporting period for publishing frames; if set to <=0 no frames will be reported as published (default: 1)')
    parser.add_argument('-dc', '--disable-curses', dest='disable_curses', default=False, action='store_true', help='Disable curses library screen handling. This is enabled by default, except when logging into standard output is turned on.')

    args, unparsed = parser.parse_known_args()
    if len(unparsed) > 0:
        print('Unrecognized argument(s): %s' % ' '.join(unparsed))
        exit(1)

    # server = adsimserver.AdSimServer(inputDirectory=args.input_directory, inputFile=args.input_file, mmapMode=args.mmap_mode, hdfDataset=args.hdf_dataset, hdfCompressionMode=args.hdf_compression_mode, frameRate=args.frame_rate, nFrames=args.n_frames, cacheSize=args.cache_size, nx=args.n_x_pixels, ny=args.n_y_pixels, datatype=args.datatype, minimum=args.minimum, maximum=args.maximum, runtime=args.runtime, channelName=args.channel_name, notifyPv=args.notify_pv, notifyPvValue=args.notify_pv_value, metadataPv=args.metadata_pv, startDelay=args.start_delay, reportPeriod=args.report_period, disableCurses=args.disable_curses)
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

if __name__ == '__main__':
    main()
