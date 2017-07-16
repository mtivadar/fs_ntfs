import logging

from . import helper
from . import DataModel
from . import mft

class Boot(object):
    def __init__(self):
        pass

class NTFS(object):
    def __init__(self, dataModel):
        self.dataModel = dataModel

        if self.dataModel.size() < 512:
            raise NtfsError("Invalid NTFS image")

        # get geometry
        self.boot = Boot()
        self.boot.lcn_of_mft              = self.dataModel.getQWORD(0x30)
        self.boot.sectors_per_cluster     = self.dataModel.getBYTE(0x0D)
        self.boot.bytes_per_sector        = self.dataModel.getWORD(0x0B)
        self.boot.clusters_per_mft_record = self.dataModel.getDWORD(0x40)

        self.mft = mft.MFT(self.boot, dataModel)

        # build MFT
        self.mft._get_mft_data_runs()

        # get $AttrDef
        self.mft._build_attrdef()

    @staticmethod
    def fixup_seq_numbers(data, update_seq_array, size_update_seq, update_seq, bytes_per_sector):
        log = helper.Helper.logger()

        size_in_bytes = data.size()

        ## apply fixup
        k = 0
        i = 0

        fixup_array = DataModel.BufferDataModel(update_seq_array, 'fixup')

        while k < size_in_bytes:
            if i >= size_update_seq:
                break

            k += bytes_per_sector
            seq = data.getWORD(k - 2)

            fixup = fixup_array.getWORD(i * 2)

            log.debug('\tlast two bytes of sector: {:04x}, fixup {:04x}'.format(seq, fixup))

            if seq != update_seq:
                log.warning('\tupdate sequence check failed, image may be corrupt, continue anyway')


            fixup_s = fixup_array.getStream(i * 2, i * 2 + 2)
            data.getData()[k-2:k] = fixup_s
            i += 1

class FileNamespace():
    POSIX         = 0x00
    WIN32         = 0x01
    DOS           = 0x02
    WIN32_AND_DOS = 0x03

class NtfsError(Exception):
    def __init__(self, message):
        super(NtfsError, self).__init__(message)
