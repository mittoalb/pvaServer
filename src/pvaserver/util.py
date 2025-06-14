'''
AD Image Utility class
'''

import time
import numpy as np
import pvaccess as pva
from functools import wraps

class AdImageUtility:
    '''
    This class contains number of utility methods for manipulating
    Area Detector images.    
    '''
    # Area detector color modes values. Source: https://github.com/areaDetector/ADCore/blob/master/ADApp/ADSrc/NDArray.h#L29
    COLOR_MODE_MONO = 0 # [NX, NY]
    COLOR_MODE_RGB1 = 2 # [3, NX, NY]
    COLOR_MODE_RGB2 = 3 # [NX, 3, NY]
    COLOR_MODE_RGB3 = 4 # [NX, NY, 3]

    COLOR_MODES = {
        COLOR_MODE_MONO : "MONO",
        COLOR_MODE_RGB1 : "RGB1",
        COLOR_MODE_RGB2 : "RGB2",
        COLOR_MODE_RGB3 : "RGB3",
    }

    NTNDA_DATA_FIELD_KEY_MAP = {
        np.dtype('uint8')   : 'ubyteValue',
        np.dtype('int8')    : 'byteValue',
        np.dtype('uint16')  : 'ushortValue',
        np.dtype('int16')   : 'shortValue',
        np.dtype('uint32')  : 'uintValue',
        np.dtype('int32')   : 'intValue',
        np.dtype('uint64')  : 'ulongValue',
        np.dtype('int64')   : 'longValue',
        np.dtype('float32') : 'floatValue',
        np.dtype('float64') : 'doubleValue'
    }

    PVA_DATA_TYPE_MAP = {
        np.dtype('uint8')   : pva.UBYTE,
        np.dtype('int8')    : pva.BYTE,
        np.dtype('uint16')  : pva.USHORT,
        np.dtype('int16')   : pva.SHORT,
        np.dtype('uint32')  : pva.UINT,
        np.dtype('int32')   : pva.INT,
        np.dtype('uint64')  : pva.ULONG,
        np.dtype('int64')   : pva.LONG,
        np.dtype('float32') : pva.FLOAT,
        np.dtype('float64') : pva.DOUBLE
    }

    @classmethod
    def reshapeNtNdArray(cls, ntNdArray):
        ''' Reshape area detector numpy array. '''
        # Get color mode
        imageId = ntNdArray['uniqueId']
        colorMode = None
        attributes = []
        if 'attribute' in ntNdArray:
            attributes = ntNdArray['attribute']
        for attribute in attributes:
            if attribute['name'] == 'ColorMode':
                colorMode = attribute['value'][0]['value']
                break

        # Get dimensions
        dims = ntNdArray['dimension']
        nDims = len(dims)

        if colorMode is None and nDims > 2:
            raise pva.InvalidArgument('NTNDArray does not contain ColorMode attribute.')
        colorMode = cls.COLOR_MODE_MONO

        if nDims == 0:
            nx = None
            ny = None
            nz = None
            image = None
            colorMode = None
            fieldKey = None
            return (imageId,image,nx,ny,nz,colorMode,fieldKey)
        if nDims == 2 and colorMode == cls.COLOR_MODE_MONO:
            nx = dims[0]['size']
            ny = dims[1]['size']
            nz = None
        elif nDims == 3 and colorMode == cls.COLOR_MODE_RGB1:
            nx = dims[1]['size']
            ny = dims[2]['size']
            nz = dims[0]['size']
        elif nDims == 3 and colorMode == cls.COLOR_MODE_RGB2:
            nx = dims[0]['size']
            ny = dims[2]['size']
            nz = dims[1]['size']
        elif nDims == 3 and colorMode == cls.COLOR_MODE_RGB3:
            nx = dims[0]['size']
            ny = dims[1]['size']
            nz = dims[2]['size']
        else:
            raise pva.InvalidArgument(f'Invalid dimensions {dims} and color mode {colorMode} combination.')

        # Alternative ways of getting the image array and type
        fieldKey = ntNdArray.getSelectedUnionFieldName()
        ###fieldKey = next(iter(ntNdArray['value'][0].keys()))

        #imageUnionField = ntNdArray.getUnion()
        #image = imageUnionField[fieldKey]
        ##image = ntNdArray.getUnion()[fieldKey]
        ###image = next(iter(ntNdArray['value'][0].values()))
        image = ntNdArray['value'][0][fieldKey]

        if colorMode == cls.COLOR_MODE_MONO:
            image = np.reshape(image, (ny, nx))

        elif colorMode == cls.COLOR_MODE_RGB1:
            image = np.reshape(image, (ny, nx, nz))

        elif colorMode == cls.COLOR_MODE_RGB2:
            image = np.reshape(image, (ny, nz, nx))
            image = np.swapaxes(image, 2, 1)

        elif colorMode == cls.COLOR_MODE_RGB3:
            image = np.reshape(image, (nz, ny, nx))
            image = np.swapaxes(image, 0, 2)
            image = np.swapaxes(image, 0, 1)

        else:
            raise pva.InvalidArgument('Unsupported color mode: {colorMode}')

        return (imageId,image,nx,ny,nz,colorMode,fieldKey)

    @classmethod
    def getNtNdArrayDataFieldKey(cls, image):
        ''' Get NTNDA data field key. '''
        return cls.NTNDA_DATA_FIELD_KEY_MAP.get(image.dtype)

    @classmethod
    def generateNtNdArray2D(cls, imageId, imageData, nx=None, ny=None, dtype=None, compressorName=None, extraFieldsPvObject=None):
        ''' Generate NTNDA for a mono image. '''
        if extraFieldsPvObject is None:
            ntNdArray = pva.NtNdArray()
        else:
            ntNdArray = pva.NtNdArray(extraFieldsPvObject.getStructureDict())

        dataFieldKey = cls.NTNDA_DATA_FIELD_KEY_MAP.get(imageData.dtype)
        data = imageData.flatten()
        if not compressorName:
            pvaDataType = cls.PVA_DATA_TYPE_MAP.get(imageData.dtype)
            ny, nx = imageData.shape
            size = nx*ny*data.itemsize
            ntNdArray['compressedSize'] = size
            ntNdArray['uncompressedSize'] = size
        else:
            dtype = np.dtype(dtype)
            pvaDataType = cls.PVA_DATA_TYPE_MAP.get(dtype)
            codec = pva.PvCodec(compressorName, pva.PvInt(int(pvaDataType)))
            ntNdArray['codec'] = codec
            size = nx*ny*dtype.itemsize
            ntNdArray['uncompressedSize'] = size
            ntNdArray['compressedSize'] = len(data)

        ntNdArray['uniqueId'] = int(imageId)
        dims = [pva.PvDimension(nx, 0, nx, 1, False), \
                pva.PvDimension(ny, 0, ny, 1, False)]
        ntNdArray['dimension'] = dims
        ts = pva.PvTimeStamp(time.time())
        ntNdArray['timeStamp'] = ts
        ntNdArray['dataTimeStamp'] = ts
        ntNdArray['descriptor'] = 'Image generated by PvaPy'

        # Alternative way of setting data
        #u = pva.PvObject({dataFieldKey : [pvaDataType]}, {dataFieldKey : data})
        #ntNdArray.setUnion(u)
        ntNdArray['value'] = {dataFieldKey : data}
        attrs = [pva.NtAttribute('ColorMode', pva.PvInt(0))]
        ntNdArray['attribute'] = attrs
        if extraFieldsPvObject is not None:
            ntNdArray.set(extraFieldsPvObject)
        return ntNdArray

    @classmethod
    def replaceNtNdArrayImage2D(cls, ntNdArray, imageId, image, extraFieldsPvObject=None):
        '''
        Assumes new image is of the same data type as the old one
        and replaces image data, dimensions, etc. in the provided NtNd Array
        '''
        dataFieldKey = cls.NTNDA_DATA_FIELD_KEY_MAP.get(image.dtype)
        pvaDataType = cls.PVA_DATA_TYPE_MAP.get(image.dtype)
        data = image.flatten()
        ntNdArray['uniqueId'] = int(imageId)

        ny, nx = image.shape
        dims = ntNdArray['dimension']
        if dims[0]['size'] != nx or dims[1]['size'] != ny:
            dims = [pva.PvDimension(nx, 0, nx, 1, False), \
                    pva.PvDimension(ny, 0, ny, 1, False)]
            ntNdArray['dimension'] = dims
            size = nx*ny*data.itemsize
            ntNdArray['compressedSize'] = size
            ntNdArray['uncompressedSize'] = size
        ts = pva.PvTimeStamp(time.time())
        ntNdArray['timeStamp'] = ts
        ntNdArray['dataTimeStamp'] = ts

        u = pva.PvObject({dataFieldKey : [pvaDataType]}, {dataFieldKey : data})
        ntNdArray.setUnion(u)
        if extraFieldsPvObject is not None:
            ntNdArray.set(extraFieldsPvObject)
        return ntNdArray
'''
AD Image Utility class
'''

import time
import numpy as np
import pvaccess as pva

class AdImageUtility:
    '''
    This class contains number of utility methods for manipulating
    Area Detector images.    
    '''
    # Area detector color modes values. Source: https://github.com/areaDetector/ADCore/blob/master/ADApp/ADSrc/NDArray.h#L29
    COLOR_MODE_MONO = 0 # [NX, NY]
    COLOR_MODE_RGB1 = 2 # [3, NX, NY]
    COLOR_MODE_RGB2 = 3 # [NX, 3, NY]
    COLOR_MODE_RGB3 = 4 # [NX, NY, 3]

    COLOR_MODES = {
        COLOR_MODE_MONO : "MONO",
        COLOR_MODE_RGB1 : "RGB1",
        COLOR_MODE_RGB2 : "RGB2",
        COLOR_MODE_RGB3 : "RGB3",
    }

    NTNDA_DATA_FIELD_KEY_MAP = {
        np.dtype('uint8')   : 'ubyteValue',
        np.dtype('int8')    : 'byteValue',
        np.dtype('uint16')  : 'ushortValue',
        np.dtype('int16')   : 'shortValue',
        np.dtype('uint32')  : 'uintValue',
        np.dtype('int32')   : 'intValue',
        np.dtype('uint64')  : 'ulongValue',
        np.dtype('int64')   : 'longValue',
        np.dtype('float32') : 'floatValue',
        np.dtype('float64') : 'doubleValue'
    }

    PVA_DATA_TYPE_MAP = {
        np.dtype('uint8')   : pva.UBYTE,
        np.dtype('int8')    : pva.BYTE,
        np.dtype('uint16')  : pva.USHORT,
        np.dtype('int16')   : pva.SHORT,
        np.dtype('uint32')  : pva.UINT,
        np.dtype('int32')   : pva.INT,
        np.dtype('uint64')  : pva.ULONG,
        np.dtype('int64')   : pva.LONG,
        np.dtype('float32') : pva.FLOAT,
        np.dtype('float64') : pva.DOUBLE
    }

    @classmethod
    def reshapeNtNdArray(cls, ntNdArray):
        ''' Reshape area detector numpy array. '''
        # Get color mode
        imageId = ntNdArray['uniqueId']
        colorMode = None
        attributes = []
        if 'attribute' in ntNdArray:
            attributes = ntNdArray['attribute']
        for attribute in attributes:
            if attribute['name'] == 'ColorMode':
                colorMode = attribute['value'][0]['value']
                break

        # Get dimensions
        dims = ntNdArray['dimension']
        nDims = len(dims)

        if colorMode is None and nDims > 2:
            raise pva.InvalidArgument('NTNDArray does not contain ColorMode attribute.')
        colorMode = cls.COLOR_MODE_MONO

        if nDims == 0:
            nx = None
            ny = None
            nz = None
            image = None
            colorMode = None
            fieldKey = None
            return (imageId,image,nx,ny,nz,colorMode,fieldKey)
        if nDims == 2 and colorMode == cls.COLOR_MODE_MONO:
            nx = dims[0]['size']
            ny = dims[1]['size']
            nz = None
        elif nDims == 3 and colorMode == cls.COLOR_MODE_RGB1:
            nx = dims[1]['size']
            ny = dims[2]['size']
            nz = dims[0]['size']
        elif nDims == 3 and colorMode == cls.COLOR_MODE_RGB2:
            nx = dims[0]['size']
            ny = dims[2]['size']
            nz = dims[1]['size']
        elif nDims == 3 and colorMode == cls.COLOR_MODE_RGB3:
            nx = dims[0]['size']
            ny = dims[1]['size']
            nz = dims[2]['size']
        else:
            raise pva.InvalidArgument(f'Invalid dimensions {dims} and color mode {colorMode} combination.')

        # Alternative ways of getting the image array and type
        fieldKey = ntNdArray.getSelectedUnionFieldName()
        ###fieldKey = next(iter(ntNdArray['value'][0].keys()))

        #imageUnionField = ntNdArray.getUnion()
        #image = imageUnionField[fieldKey]
        ##image = ntNdArray.getUnion()[fieldKey]
        ###image = next(iter(ntNdArray['value'][0].values()))
        image = ntNdArray['value'][0][fieldKey]

        if colorMode == cls.COLOR_MODE_MONO:
            image = np.reshape(image, (ny, nx))

        elif colorMode == cls.COLOR_MODE_RGB1:
            image = np.reshape(image, (ny, nx, nz))

        elif colorMode == cls.COLOR_MODE_RGB2:
            image = np.reshape(image, (ny, nz, nx))
            image = np.swapaxes(image, 2, 1)

        elif colorMode == cls.COLOR_MODE_RGB3:
            image = np.reshape(image, (nz, ny, nx))
            image = np.swapaxes(image, 0, 2)
            image = np.swapaxes(image, 0, 1)

        else:
            raise pva.InvalidArgument('Unsupported color mode: {colorMode}')

        return (imageId,image,nx,ny,nz,colorMode,fieldKey)

    @classmethod
    def getNtNdArrayDataFieldKey(cls, image):
        ''' Get NTNDA data field key. '''
        return cls.NTNDA_DATA_FIELD_KEY_MAP.get(image.dtype)

    @classmethod
    def generateNtNdArray2D(cls, imageId, imageData, nx=None, ny=None, dtype=None, compressorName=None, extraFieldsPvObject=None):
        ''' Generate NTNDA for a mono image. '''
        if extraFieldsPvObject is None:
            ntNdArray = pva.NtNdArray()
        else:
            ntNdArray = pva.NtNdArray(extraFieldsPvObject.getStructureDict())

        dataFieldKey = cls.NTNDA_DATA_FIELD_KEY_MAP.get(imageData.dtype)
        data = imageData.flatten()
        if not compressorName:
            pvaDataType = cls.PVA_DATA_TYPE_MAP.get(imageData.dtype)
            ny, nx = imageData.shape
            size = nx*ny*data.itemsize
            ntNdArray['compressedSize'] = size
            ntNdArray['uncompressedSize'] = size
        else:
            dtype = np.dtype(dtype)
            pvaDataType = cls.PVA_DATA_TYPE_MAP.get(dtype)
            codec = pva.PvCodec(compressorName, pva.PvInt(int(pvaDataType)))
            ntNdArray['codec'] = codec
            size = nx*ny*dtype.itemsize
            ntNdArray['uncompressedSize'] = size
            ntNdArray['compressedSize'] = len(data)

        ntNdArray['uniqueId'] = int(imageId)
        dims = [pva.PvDimension(nx, 0, nx, 1, False), \
                pva.PvDimension(ny, 0, ny, 1, False)]
        ntNdArray['dimension'] = dims
        ts = pva.PvTimeStamp(time.time())
        ntNdArray['timeStamp'] = ts
        ntNdArray['dataTimeStamp'] = ts
        ntNdArray['descriptor'] = 'Image generated by PvaPy'

        # Alternative way of setting data
        #u = pva.PvObject({dataFieldKey : [pvaDataType]}, {dataFieldKey : data})
        #ntNdArray.setUnion(u)
        ntNdArray['value'] = {dataFieldKey : data}
        attrs = [pva.NtAttribute('ColorMode', pva.PvInt(0))]
        ntNdArray['attribute'] = attrs
        if extraFieldsPvObject is not None:
            ntNdArray.set(extraFieldsPvObject)
        return ntNdArray

    @classmethod
    def replaceNtNdArrayImage2D(cls, ntNdArray, imageId, image, extraFieldsPvObject=None):
        '''
        Assumes new image is of the same data type as the old one
        and replaces image data, dimensions, etc. in the provided NtNd Array
        '''
        dataFieldKey = cls.NTNDA_DATA_FIELD_KEY_MAP.get(image.dtype)
        pvaDataType = cls.PVA_DATA_TYPE_MAP.get(image.dtype)
        data = image.flatten()
        ntNdArray['uniqueId'] = int(imageId)

        ny, nx = image.shape
        dims = ntNdArray['dimension']
        if dims[0]['size'] != nx or dims[1]['size'] != ny:
            dims = [pva.PvDimension(nx, 0, nx, 1, False), \
                    pva.PvDimension(ny, 0, ny, 1, False)]
            ntNdArray['dimension'] = dims
            size = nx*ny*data.itemsize
            ntNdArray['compressedSize'] = size
            ntNdArray['uncompressedSize'] = size
        ts = pva.PvTimeStamp(time.time())
        ntNdArray['timeStamp'] = ts
        ntNdArray['dataTimeStamp'] = ts

        u = pva.PvObject({dataFieldKey : [pvaDataType]}, {dataFieldKey : data})
        ntNdArray.setUnion(u)
        if extraFieldsPvObject is not None:
            ntNdArray.set(extraFieldsPvObject)
        return ntNdArray


class FloatWithUnits(float):

    DEFAULT_PRECISION = 4

    # Float is immutable, we have to override __new__
    def __new__(self, value, units='', precision=DEFAULT_PRECISION):
        return float.__new__(self, value)

    def __init__(self, value, units='', precision=DEFAULT_PRECISION):
        float.__init__(value)
        self.units = units
        self.precision = precision

    def __repr__(self):
        if self == 0:
            return f'0.0{self.units}'
        return f'{float(self):.{self.precision}f}{self.units}'

    def __str__(self):
        return self.__repr__()


class IntWithUnits(int):

    def __new__(self, value, units=''):
        return int.__new__(self, value)

    def __init__(self, value, units=''):
        int.__init__(value)
        self.units = units

    def __repr__(self):
        return f'{int(self)}{self.units}'

    def __str__(self):
        return self.__repr__()




def ntnda_stream(pv_name: str, *, start_uid: int = 1):
    """
    Decorator that publishes the result of a function returning a 2-D NumPy array
    to a PVAccess NTNDArray process variable (PV).

    Parameters:
    ----------
    pv_name : str
        The name of the NTNDArray PV to publish the images to.
    start_uid : int, optional
        Starting unique identifier (UID) for NTNDArray frames. Defaults to 1.

    Returns:
    -------
    decorator : Callable
        A decorator function that wraps another function to enable PV streaming.

    Notes:
    -----
    - The decorated function must return a 2D NumPy array (grayscale image).
    - The image is wrapped into an NTNDArray structure and published to the PV.
    - UID is incremented with each call to ensure unique frame IDs.
    - Uses `AdImageUtility` for NTNDArray wrapping and updating.

    Example:
    -------
    @ntnda_stream('SIM:IMG')
    def generate_image():
        return np.random.randint(0, 255, (256, 256), dtype=np.uint8)
    """
    dummy = np.zeros((1, 1), dtype=np.uint8)
    nt = AdImageUtility.generateNtNdArray2D(0, dummy)

    server = pva.PvaServer()
    server.addRecord(pv_name, nt)
    uid = start_uid

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal uid, nt
            frame = np.asarray(func(*args, **kwargs))
            if frame.ndim != 2:
                raise ValueError(f"Expected 2-D image, got shape {frame.shape}")

            # Replace the image in the NTNDArray structure
            AdImageUtility.replaceNtNdArrayImage2D(nt, uid, frame)

            # Update the server PV with the new image
            server.update(nt)

            # Increment the frame UID
            uid += 1

            return frame
        return wrapper
    return decorator
