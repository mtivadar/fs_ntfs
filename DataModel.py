import mmap
import os

class DataModel(object):
    def __init__(self, data):
        self.data = data

    def getQWORD(self, offset, asString=False):
        if offset + 8 > len(self.data):
            return None

        b = bytearray(self.data[offset:offset+8])

        d = ((b[7] << 56) | (b[6] << 48) | (b[5] << 40) | (b[4] << 32) | (b[3] << 24) | (b[2] << 16) | (b[1] << 8) | (b[0])) & 0xFFFFFFFFFFFFFFFF

        if not asString:        
            return d

        s = '{0:016X}'.format(d)
        
        return s

    def getDWORD(self, offset, asString=False):
        if offset + 4 >= len(self.data):
            return None

        b = bytearray(self.data[offset:offset+4])

        d = ((b[3] << 24) | (b[2] << 16) | (b[1] << 8) | (b[0])) & 0xFFFFFFFF

        if not asString:        
            return d

        s = '{0:08X}'.format(d)
        
        return s

    def getWORD(self, offset, asString=False):
        if offset + 2 > len(self.data):
            return None

        b = bytearray(self.data[offset:offset+2])

        d = ((b[1] << 8) | (b[0])) & 0xFFFF

        if not asString:        
            return d

        s = '{0:04X}'.format(d)
        
        return s

    def getBYTE(self, offset, asString=False):
        if offset + 1 > len(self.data):
            return None

        b = bytearray(self.data[offset:offset+1])

        d = (b[0]) & 0xFF

        if not asString:        
            return d

        s = '{0:02X}'.format(d)
        
        return s

    def getChar(self, offset):
        if offset < 0:
            return None

        if offset >= len(self.data):
            return None

        return self.data[offset]

    def getStream(self, start, end):
        return bytearray(self.data[start:end])

    def getData(self):
        return self.data

    @property
    def source(self):
        return ''

    def flush(self):
        raise NotImplementedError('method not implemented.')

    def write(self):
        raise NotImplementedError('method not implemented.')

    def close(self):
        pass

    def size(self):
        return len(self.data)

class Slice(object):
    def __init__(self, fo, size):
        self._fo = fo
        self._size = size

    def __len__(self):
        return self._size

    def __getitem__(self, _slice):
        start = _slice.start
        stop  = _slice.stop

        # seek to sector
        # assume 0x200 bytes/sector
        bytes_per_sector = 0x200
        sectors = start / bytes_per_sector

        which_sector = sectors * bytes_per_sector
        self._fo.seek(which_sector)

        # careful for big data
        result = bytearray(self._fo.read(stop - which_sector))
        
        result = result[start-which_sector:]
        return result

class FileDataModel(DataModel):
    def __init__(self, filename):
        self._filename = filename
        self._fo = open(filename, "rb")

        not_normal_file = False
        try:
            self._size = os.path.getsize(self._filename)
        except WindowsError:
            not_normal_file = True
            # ugly hack
            self._size = 8000000000*1024*1024*1024

        self.data = Slice(self._fo, self._size)

        super(FileDataModel, self).__init__(self.data)

    def size(self):
        return self._size

class MappedFileDataModel(DataModel):
    def __init__(self, filename):
        self._filename = filename

        self._f = open(filename, "rb")

        # memory-map the file, size 0 means whole file
        self._mapped = mmap.mmap(self._f.fileno(), 0, access=mmap.ACCESS_COPY)

        super(MappedFileDataModel, self).__init__(self._mapped)

    @property
    def source(self):
        return self._filename

    def flush(self):
        self._f.close()
        # open for writing
        try:
            self._f = open(self._filename, "r+b")
        except Exception, e:
            # could not open for writing
            return False
        self._f.write(self._mapped)

        return True

    def close(self):
        self._mapped.close()
        self._f.close()

    def write(self, offset, stream):
        self._mapped.seek(offset)
        self._mapped.write(stream)

    def size(self):
        return os.path.getsize(self._filename)

class MyByte(bytearray):
    def __init__(self, data):
        self.raw = data
        self._pointer = 0
        super(MyByte, self).__init__(data)

    def __len__(self):
        return len(self.raw)

    def seek(self, a, b=0):
        if b == 0:
            self._pointer = a
        elif b == 1:
            self._pointer += a
        elif b == 2:
            self._pointer = len(self.raw) - b
        else:
            return

        return

    def read(self, size):
        if self._pointer + size > len(self.raw):
            return ''

        data = str(self.raw[self._pointer:self._pointer + size])
        self._pointer += size
        return data

class BufferDataModel(DataModel):
    def __init__(self, data, name):
        self._filename = name
        self.raw = data
        self.data = MyByte(data)

        super(BufferDataModel, self).__init__(self.data)

    @property
    def source(self):
        return self._filename

    def flush(self):
        return False

    def close(self):
        return

    def size(self):
        return len(self.data)
