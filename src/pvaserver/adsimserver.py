import time
import random
import threading
import queue
import argparse
import os
import os.path
import ctypes.util
import numpy as np
import h5py as h5

import pvaccess as pva

from pvaserver import __version__
from pvaserver import util


class FrameGenerator:
    def __init__(self):
        self.frames = None
        self.nInputFrames = 0
        self.rows = 0
        self.cols = 0
        self.dtype = None
        self.compressorName = None

    def getFrameData(self, frameId):
        if frameId < self.nInputFrames and frameId >= 0:
            return self.frames[frameId]
        return None

    def getFrameInfo(self):
        if self.frames is not None and not self.nInputFrames:
            self.nInputFrames, self.rows, self.cols = self.frames.shape
            self.dtype = self.frames.dtype
        return (self.nInputFrames, self.rows, self.cols, self.dtype, self.compressorName)

    def getUncompressedFrameSize(self):
        return self.rows*self.cols*self.frames[0].itemsize

    def getCompressedFrameSize(self):
        if self.compressorName:
            return len(self.getFrameData(0))
        else:
            return self.getUncompressedFrameSize()

    def getCompressorName(self):
        return self.compressorName

class HdfFileGenerator(FrameGenerator):

    COMPRESSOR_NAME_MAP = {
        '32001' : 'blosc'
    }

    def __init__(self, filePath, datasetPath, compressionMode=False):
        FrameGenerator.__init__(self)
        self.filePath = filePath
        self.datasetPath = datasetPath
        self.dataset = None
        self.compressionMode = compressionMode
        if not h5:
            raise Exception(f'Missing HDF support.')
        if not filePath:
            raise Exception(f'Invalid input file path.')
        if not datasetPath:
            raise Exception(f'Missing HDF dataset specification for input file {filePath}.')
        self.loadInputFile()

    def loadInputFile(self):
        try:
            self.file = h5.File(self.filePath, 'r')
            self.dataset = self.file[self.datasetPath]
            self.frames = self.dataset
            if self.compressionMode:
                for id,params in self.dataset._filters.items():
                    compressorName = self.COMPRESSOR_NAME_MAP.get(id)
                    if compressorName:
                        self.compressorName = compressorName
                        break
            print(f'Loaded input file {self.filePath} (compressor: {self.compressorName})')
        except Exception as ex:
            print(f'Cannot load input file {self.filePath}: {ex}')
            raise

    def getFrameData(self, frameId):
        frameData = None
        if frameId < self.nInputFrames and frameId >= 0:
            if not self.compressorName:
                # Read uncompressed data
                frameData = self.frames[frameId]
            else:
                # Read compressed data directly into numpy array
                data = self.dataset.id.read_direct_chunk((frameId,0,0))
                frameData = np.frombuffer(data[1], dtype=np.uint8)
        return frameData

class NumpyFileGenerator(FrameGenerator):

    def __init__(self, filePath, mmapMode):
        FrameGenerator.__init__(self)
        self.filePath = filePath
        self.mmapMode = mmapMode
        if not filePath:
            raise Exception(f'Invalid input file path.')
        self.loadInputFile()

    def loadInputFile(self):
        try:
            if self.mmapMode:
                self.frames = np.load(self.filePath, mmap_mode='r')
            else:
                self.frames = np.load(self.filePath)
            print(f'Loaded input file {self.filePath}')
        except Exception as ex:
            print(f'Cannot load input file {self.filePath}: {ex}')
            raise

class NumpyRandomGenerator(FrameGenerator):

    def __init__(self, nf, nx, ny, datatype, minimum, maximum):
        FrameGenerator.__init__(self)
        self.nf = nf
        self.nx = nx
        self.ny = ny
        self.datatype = datatype
        self.minimum = minimum
        self.maximum = maximum
        self.generateFrames()

    def generateFrames(self):
        print('Generating random frames')

        # Example frame:
        # frame = np.array([[0,0,0,0,0,0,0,0,0,0],
        #                  [0,0,0,0,1,1,0,0,0,0],
        #                  [0,0,0,1,2,3,2,0,0,0],
        #                  [0,0,0,1,2,3,2,0,0,0],
        #                  [0,0,0,1,2,3,2,0,0,0],
        #                  [0,0,0,0,0,0,0,0,0,0]], dtype=np.uint16)

        dt = np.dtype(self.datatype)
        if not self.datatype.startswith('float'):
            dtinfo = np.iinfo(dt)
            mn = dtinfo.min
            if self.minimum is not None:
                mn = int(max(dtinfo.min, self.minimum))
            mx = dtinfo.max
            if self.maximum is not None:
                mx = int(min(dtinfo.max, self.maximum))
            self.frames = np.random.randint(mn, mx, size=(self.nf, self.ny, self.nx), dtype=dt)
        else:
            # Use float32 for min/max, to prevent overflow errors
            dtinfo = np.finfo(np.float32)
            mn = dtinfo.min
            if self.minimum is not None:
                mn = float(max(dtinfo.min, self.minimum))
            mx = dtinfo.max
            if self.maximum is not None:
                mx = float(min(dtinfo.max, self.maximum))
            self.frames = np.random.uniform(mn, mx, size=(self.nf, self.ny, self.nx))
            if datatype == 'float32':
                self.frames = np.float32(self.frames)

        print(f'Generated frame shape: {self.frames[0].shape}')
        print(f'Range of generated values: [{mn},{mx}]')

class AdSimServer:

    # Uses frame cache of a given size. If the number of input
    # files is larger than the cache size, the server will be constantly 
    # regenerating frames.

    SHUTDOWN_DELAY = 1.0
    MIN_CACHE_SIZE = 1
    CACHE_TIMEOUT = 1.0
    DELAY_CORRECTION = 0.0001
    NOTIFICATION_DELAY = 0.1
    BYTES_IN_MEGABYTE = 1000000
    METADATA_TYPE_DICT = {
        'value' : pva.DOUBLE,
        'timeStamp' : pva.PvTimeStamp()
    }

    def __init__(self, args):
        self.lock = threading.Lock()
        self.deltaT = 0
        self.cacheTimeout = self.CACHE_TIMEOUT
        if args.frame_rate > 0:
            self.deltaT = 1.0/args.frame_rate
            self.cacheTimeout = max(self.CACHE_TIMEOUT, self.deltaT)
        self.runtime = args.runtime
        self.reportPeriod = args.report_period 
        self.metadataIoc = None
        self.frameGeneratorList = []
        self.frameCacheSize = max(args.cache_size, self.MIN_CACHE_SIZE)
        self.nFrames = args.n_frames

        inputFiles = []
        if args.input_directory is not None:
            inputFiles = [os.path.join(args.input_directory, f) for f in os.listdir(args.input_directory) if os.path.isfile(os.path.join(args.input_directory, f))]
        if args.input_file is not None:
            inputFiles.append(args.input_file)
        allowedHdfExtensions = ['h5', 'hdf', 'hdf5']
        for f in inputFiles:
            ext = f.split('.')[-1]
            if ext in allowedHdfExtensions:
                self.frameGeneratorList.append(HdfFileGenerator(f, args.hdf_dataset, args.hdf_compression_mode))
            else:
                self.frameGeneratorList.append(NumpyFileGenerator(f, args.input_file))

        if not self.frameGeneratorList:
            nf = args.n_frames
            if nf <= 0:
                nf = self.frameCacheSize
            self.frameGeneratorList.append(NumpyRandomGenerator(nf, args.n_x_pixels, args.n_y_pixels, args.datatype, args.minimum, args.maximum))

        self.nInputFrames = 0
        for fg in self.frameGeneratorList:
            nInputFrames, self.rows, self.cols, self.dtype, self.compressorName = fg.getFrameInfo()
            self.nInputFrames += nInputFrames
        if self.nFrames > 0:
            self.nInputFrames = min(self.nFrames, self.nInputFrames)

        fg = self.frameGeneratorList[0]
        self.frameRate = args.frame_rate
        self.uncompressedImageSize = util.IntWithUnits(fg.getUncompressedFrameSize(), 'B')
        self.compressedImageSize = util.IntWithUnits(fg.getCompressedFrameSize(), 'B')
        self.compressedDataRate = util.FloatWithUnits(self.compressedImageSize*self.frameRate/self.BYTES_IN_MEGABYTE, 'MBps')
        self.uncompressedDataRate = util.FloatWithUnits(self.uncompressedImageSize*self.frameRate/self.BYTES_IN_MEGABYTE, 'MBps')

        self.channelName = args.channel_name
        self.pvaServer = pva.PvaServer()
        self.setupMetadataPvs(args.metadata_pv)
        self.pvaServer.addRecord(self.channelName, pva.NtNdArray(), None)

        if args.notify_pv and args.notify_pv_value:
            try:
                time.sleep(self.NOTIFICATION_DELAY)
                notifyChannel = pva.Channel(args.notify_pv, pva.CA)
                notifyChannel.put(args.notify_pv_value)
                print(f'Set notification PV {args.notify_pv} to {args.notify_pv_value}')
            except Exception as ex:
                print(f'Could not set notification PV {args.notify_pv} to {args.notify_pv_value}: {ex}')

        # Use PvObjectQueue if cache size is too small for all input frames
        # Otherwise, simple dictionary is good enough
        self.usingQueue = False
        if self.nInputFrames > self.frameCacheSize:
            self.usingQueue = True
            self.frameCache = pva.PvObjectQueue(self.frameCacheSize)
        else:
            self.frameCache = {}

        print(f'Number of input frames: {self.nInputFrames} (size: {self.cols}x{self.rows}, {self.uncompressedImageSize}, type: {self.dtype}, compressor: {self.compressorName}, compressed size: {self.compressedImageSize})')
        print(f'Frame cache type: {type(self.frameCache)} (cache size: {self.frameCacheSize})')
        print(f'Expected data rate: {self.compressedDataRate} (uncompressed: {self.uncompressedDataRate})')

        self.currentFrameId = 0
        self.nPublishedFrames = 0
        self.startTime = 0
        self.lastPublishedTime = 0
        self.startDelay = args.start_delay
        self.isDone = False
        self.screen = None
        self.screenInitialized = False
        self.disableCurses = args.disable_curses

    def setupCurses(self):
        screen = None
        if not self.disableCurses:
            try:
                import curses
                screen = curses.initscr()
                self.curses = curses
            except ImportError as ex:
                pass
        return screen

    def setupMetadataPvs(self, metadataPv):
        self.caMetadataPvs = []
        self.pvaMetadataPvs = []
        self.metadataPvs = []
        if not metadataPv:
            return
        mPvs = metadataPv.split(',')
        for mPv in mPvs:
            if not mPv:
                continue

            # Assume CA is the default protocol
            if mPv.startswith('pva://'):
                self.pvaMetadataPvs.append(mPv.replace('pva://', ''))
            else:
                self.caMetadataPvs.append(mPv.replace('ca://', ''))
        self.metadataPvs = self.caMetadataPvs+self.pvaMetadataPvs
        if self.caMetadataPvs:
            if not os.environ.get('EPICS_DB_INCLUDE_PATH'):
                pvDataLib = os.path.realpath(ctypes.util.find_library('pvData'))
                epicsLibDir = os.path.dirname(pvDataLib)
                dbdDir = os.path.realpath(f'{epicsLibDir}/../../dbd')
                os.environ['EPICS_DB_INCLUDE_PATH'] = dbdDir

        print(f'CA Metadata PVs: {self.caMetadataPvs}')
        if self.caMetadataPvs:
            # Create database and start CA IOC
            import tempfile
            dbFile = tempfile.NamedTemporaryFile(delete=False) 
            dbFile.write(b'record(ao, "$(NAME)") {}\n')
            dbFile.close()

            self.metadataIoc = pva.CaIoc()
            self.metadataIoc.loadDatabase('base.dbd', '', '')
            self.metadataIoc.registerRecordDeviceDriver()
            for mPv in self.caMetadataPvs: 
                print(f'Creating CA metadata record: {mPv}')
                self.metadataIoc.loadRecords(dbFile.name, f'NAME={mPv}')
            self.metadataIoc.start()
            os.unlink(dbFile.name)

        print(f'PVA Metadata PVs: {self.pvaMetadataPvs}')
        if self.pvaMetadataPvs:
            for mPv in self.pvaMetadataPvs: 
                print(f'Creating PVA metadata record: {mPv}')
                mPvObject = pva.PvObject(self.METADATA_TYPE_DICT)
                self.pvaServer.addRecord(mPv, mPvObject, None)

    def getMetadataValueDict(self):
        metadataValueDict = {}
        for mPv in self.metadataPvs: 
            value = random.uniform(0,1)
            metadataValueDict[mPv] = value
        return metadataValueDict

    def updateMetadataPvs(self, metadataValueDict):
        # Returns time when metadata is published
        # For CA metadata will be published before data timestamp
        # For PVA metadata will have the same timestamp as data
        for mPv in self.caMetadataPvs:
            value = metadataValueDict.get(mPv)
            self.metadataIoc.putField(mPv, str(value))
        t = time.time()
        for mPv in self.pvaMetadataPvs:
            value = metadataValueDict.get(mPv)
            mPvObject = pva.PvObject(self.METADATA_TYPE_DICT, {'value' : value, 'timeStamp' : pva.PvTimeStamp(t)})
            self.pvaServer.update(mPv, mPvObject)
        return t
        
    def addFrameToCache(self, frameId, ntnda):
        if not self.usingQueue:
            # Using dictionary
            self.frameCache[frameId] = ntnda
        else:
            # Using PvObjectQueue
            try:
                waitTime = self.startDelay + self.cacheTimeout
                self.frameCache.put(ntnda, waitTime)
            except pva.QueueFull:
                pass
            
    def getFrameFromCache(self):
        if not self.usingQueue:
            # Using dictionary
            cachedFrameId = self.currentFrameId % self.nInputFrames
            if cachedFrameId not in self.frameCache:
            # In case frames were not generated on time, just use first frame
                cachedFrameId = 0
            ntnda = self.frameCache[cachedFrameId]
        else:
            # Using PvObjectQueue
            ntnda = self.frameCache.get(self.cacheTimeout)
        return ntnda

    def frameProducer(self, extraFieldsPvObject=None):
        startTime = time.time()
        frameId = 0
        frameData = None
        while not self.isDone:
            for fg in self.frameGeneratorList:
                nInputFrames, ny, nx, dtype, compressorName = fg.getFrameInfo()
                for fgFrameId in range(0,nInputFrames):
                    if self.isDone or (self.nInputFrames > 0 and frameId >= self.nInputFrames):
                        break
                    frameData = fg.getFrameData(fgFrameId)
                    if frameData is None:
                        break
                    ntnda = util.AdImageUtility.generateNtNdArray2D(frameId, frameData, nx, ny, dtype, compressorName, extraFieldsPvObject)
                    self.addFrameToCache(frameId, ntnda)
                    frameId += 1
            if self.isDone or not self.usingQueue or frameData is None or (self.nInputFrames > 0 and frameId >= self.nInputFrames):
                # All frames are in cache or we cannot generate any more data
                break
        self.printReport(f'Frame producer is done after {frameId} generated frames')

    def prepareFrame(self, t=0):
        # Get cached frame
        frame = self.getFrameFromCache()
        if frame is not None:
            # Correct image id and timestamps
            self.currentFrameId += 1
            frame['uniqueId'] = self.currentFrameId
            if t <= 0:
                t = time.time()
            ts = pva.PvTimeStamp(t)
            frame['timeStamp'] = ts
            frame['dataTimeStamp'] = ts
        return frame

    def framePublisher(self):
        while True:
            if self.isDone:
                return

            # Prepare metadata
            metadataValueDict = self.getMetadataValueDict()

            # Update metadata and take timestamp
            updateTime = self.updateMetadataPvs(metadataValueDict)

            # Prepare frame with a given timestamp
            # so that metadata and image times are as close as possible
            try:
                frame = self.prepareFrame(updateTime)
            except pva.QueueEmpty:
                self.printReport(f'Server exiting after emptying queue')
                self.isDone = True
                return
            except Exception:
                if self.isDone:
                    return
                raise

            # Publish frame
            self.pvaServer.update(self.channelName, frame)
            self.lastPublishedTime = time.time()
            self.nPublishedFrames += 1
            if self.usingQueue and self.nPublishedFrames >= self.nInputFrames:
                self.printReport(f'Server exiting after publishing {self.nPublishedFrames}')
                self.isDone = True
                return

            runtime = 0
            frameRate = 0
            if self.nPublishedFrames > 1:
                runtime = self.lastPublishedTime - self.startTime
                deltaT = runtime/(self.nPublishedFrames - 1)
                frameRate = 1.0/deltaT
            else:
                self.startTime = self.lastPublishedTime
            if self.reportPeriod > 0 and (self.nPublishedFrames % self.reportPeriod) == 0:
                report = 'Published frame id {:6d} @ {:.3f}s (frame rate: {:.4f}fps; runtime: {:.3f}s)'.format(self.currentFrameId, self.lastPublishedTime, frameRate, runtime)
                self.printReport(report)

            if runtime > self.runtime:
                self.printReport(f'Server exiting after reaching runtime of {runtime:.3f} seconds')
                return

            if self.deltaT > 0:
                nextPublishTime = self.startTime + self.nPublishedFrames*self.deltaT
                delay = nextPublishTime - time.time() - self.DELAY_CORRECTION
                if delay > 0:
                    threading.Timer(delay, self.framePublisher).start()
                    return

    def printReport(self, report):
        with self.lock:
            if not self.screenInitialized:
                self.screenInitialized = True
                self.screen = self.setupCurses()
            if self.screen:
                self.screen.erase()
                self.screen.addstr(f'{report}\n')
                self.screen.refresh()
            else:
                print(report)

    def start(self):

        threading.Thread(target=self.frameProducer, daemon=True).start()
        self.pvaServer.start()
        threading.Timer(self.startDelay, self.framePublisher).start()

    def stop(self):
        self.isDone = True
        self.pvaServer.stop()
        runtime = self.lastPublishedTime - self.startTime
        deltaT = 0
        frameRate = 0
        if self.nPublishedFrames > 1:
            deltaT = runtime/(self.nPublishedFrames - 1)
            frameRate = 1.0/deltaT
        dataRate = util.FloatWithUnits(self.uncompressedImageSize*frameRate/self.BYTES_IN_MEGABYTE, 'MBps')
        time.sleep(self.SHUTDOWN_DELAY)
        if self.screen:
            self.curses.endwin()
        print('\nServer runtime: {:.4f} seconds'.format(runtime))
        print('Published frames: {:6d} @ {:.4f} fps'.format(self.nPublishedFrames, frameRate))
        print(f'Data rate: {dataRate}')