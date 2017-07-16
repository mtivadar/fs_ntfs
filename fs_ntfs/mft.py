import logging

from . import DataModel

from . import helper
from . import filerecord
from . import attributes
from . import fs_ntfs

class MFT(object):
    def __init__(self, boot, dataModel):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        self.dataModel = dataModel

        if self.dataModel.size() < 512:
            raise fs_ntfs.NtfsError("Invalid NTFS image")

        # compute $MFT cluster
        self.lcn_of_mft              = boot.lcn_of_mft
        self.sectors_per_cluster     = boot.sectors_per_cluster
        self.bytes_per_sector        = boot.bytes_per_sector
        self.clusters_per_mft_record = boot.clusters_per_mft_record

        # file record
        start_mft = self.lcn_of_mft * self.sectors_per_cluster * self.bytes_per_sector

        # so, this is stored on dword, but it seems that only one byte it's considered
        self.clusters_per_mft_record = self._sign_extend(self.clusters_per_mft_record, 8)

        # it's computed like this        
        if self.clusters_per_mft_record < 0:
            self.file_record_size = 1 << -self.clusters_per_mft_record
        else:
            self.file_record_size = self.clusters_per_mft_record * self.sectors_per_cluster * self.bytes_per_sector    

        if start_mft > self.dataModel.size():
            raise fs_ntfs.NtfsError('MFT initialization failed.')
        else:
            # ok
            pass

    def _get_le(self, s):
        n = 0x00

        for x in s[::-1]:
            n = n << 8
            n = n | x

        n = self._sign_extend(n, len(s) * 8)

        return n

    def _sign_extend(self, value, bits):
        sign_bit = 1 << (bits - 1)
        return (value & (sign_bit - 1)) - (value & sign_bit)

    def _get_mft_data_runs(self):
        log = self.logger

        start_mft = self.lcn_of_mft * self.sectors_per_cluster * self.bytes_per_sector
        file_record_size = self.file_record_size

        log.debug('')
        log.debug('=====================     GET $MFT DATA RUNS     =====================')

        i = 0

        log.debug('FILE_RECORD #{0}'.format(i))

        file_record = start_mft + i*file_record_size
        fr = file_record
        off_first_attr = self.dataModel.getWORD(file_record+0x14)


        data = self.dataModel

        real_size = data.getDWORD(fr + 0x18)
        log.debug('Real size of file record: 0x{:1X}'.format(real_size))

        allocated_size = data.getDWORD(fr + 0x1c)
        log.debug('Allocated size of file record: 0x{:0X}'.format(allocated_size))

        file_reference = data.getQWORD(fr + 0x20)
        log.debug('File reference to the base FILE record: 0x{:0X}'.format(file_reference))

        next_attribute_id = data.getWORD(fr + 0x28)
        log.debug('Next Attribute Id: 0x{:0X}'.format(next_attribute_id))

        ao = fr + off_first_attr 
        while 1:
            std_attr_type = data.getDWORD(ao + 0x00)
            if std_attr_type == 0xFFFFFFFF:
                # attribute list ends
                break

            attr_length = data.getDWORD(ao + 0x04)
            non_resident_flag = data.getBYTE(ao + 0x08)
            attr_name_length = data.getBYTE(ao + 0x09)

            if non_resident_flag and not attr_name_length and std_attr_type == 0x80:
                # $DATA
                starting_vcn = data.getQWORD(ao + 0x10)
                last_vcn = data.getQWORD(ao + 0x18)

                log.debug('Starting VCN: 0x{:0X}, last VCN: 0x{:0X}'.format(starting_vcn, last_vcn))

                attr_real_size = data.getQWORD(ao + 0x30)
                log.debug('Real size of the attribute: 0x{:0X}'.format(attr_real_size))
                attr_length_2 = attr_real_size

                # offset to datarun
                offset_to_attribute = data.getWORD(ao + 0x20) 

                log.debug('data runs...')
                s = data.getStream(ao + offset_to_attribute, ao + offset_to_attribute + attr_length - 0x40)

                _log = ''
                for k in s:
                    _log += '0x{:02x}'.format(k) + ' '
                    

                log.debug(_log)
                log.debug('')

                data_runs = self._decode_data_runs(s)

                for data_run in data_runs:
                    n, lcn = data_run

                    file_offset = lcn * self.sectors_per_cluster * self.bytes_per_sector
                    size_in_bytes = n * self.sectors_per_cluster * self.bytes_per_sector

                    log.debug('0x{:04x} clusters @ LCN 0x{:08x}, @ f_offset 0x{:x}, size_in_bytes {:,}'.format(n, lcn, file_offset, size_in_bytes))

                self.mft_data_runs = data_runs
                return data_runs

            ao += attr_length

            # not found
        return None

    def _decode_data_runs(self, stream):
        log = self.logger

        s = stream
        result = []

        prev_lcn_start = 0
        while 1:
            #print '0x{:02x}'.format(k),
            k = s[0]

            if k == 0x00:
                break

            length_size = k & 0x0F
            offset_size = (k & 0xF0) >> 4

            if offset_size == 0x00:
                # sparse file
                # !FIXME, should we do something with it?
                break

            n_clusters = self._get_le(s[1:1 + length_size])
            rel_lcn_start  = self._get_le(s[1 + length_size: 1 + length_size + offset_size])

            lcn_start  = prev_lcn_start + rel_lcn_start

            """
            log.debug('...data runs...')

            q = ''
            for k in stream:
                q += '0x{:02x} '.format(k)

            log.debug(q)
            """

            log.debug('LCN relative 0x{:08x}, length_size: 0x{:x}, offset_size: 0x{:x}, n_clusters: 0x{:04x}, LCN start: 0x{:04x}'.format(rel_lcn_start, length_size, offset_size, n_clusters, lcn_start))

            s = s[1 + length_size + offset_size:]

            result += [(n_clusters, lcn_start)]
            prev_lcn_start = lcn_start

        log.debug('')

        return result

    def _datarun_of_file_record(self, which_file_record):

        # get data run of file_record
        for n, lcn in self.mft_data_runs:
            start_mft = lcn * self.sectors_per_cluster * self.bytes_per_sector
            mft_size_in_bytes = n * self.sectors_per_cluster * self.bytes_per_sector

            n_file_records = mft_size_in_bytes // self.file_record_size

            if which_file_record < n_file_records:
                return (n, lcn, which_file_record)
            else:
                which_file_record -= n_file_records

        return None

    def _build_attrdef(self):
        datarun = self._datarun_of_file_record(4)
        if datarun is None:
            # file record not found
            raise fs_ntfs.NtfsError('Cannot find $AttrDef.')

        n, lcn, rel_record = datarun

        start_mft = lcn * self.sectors_per_cluster * self.bytes_per_sector
        mft_size_in_bytes = n * self.sectors_per_cluster * self.bytes_per_sector

        file_record = start_mft + 4*self.file_record_size

        log = self.logger

        off_first_attr = self.dataModel.getWORD(file_record+0x14)
        data = self.dataModel

        ao = file_record + off_first_attr

        _attrDef = attributes.AttrDef()

        # iterate attributes
        while 1:
            std_attr_type = data.getDWORD(ao + 0x00)
            if std_attr_type == 0xFFFFFFFF:
                break

            # standard attribute header
            attr_length = data.getDWORD(ao + 0x04)
            non_resident_flag = data.getBYTE(ao + 0x08)
            attr_name_length = data.getBYTE(ao + 0x09)

            if non_resident_flag and not attr_name_length and std_attr_type == 0x80:
                # $DATA

                # offset to datarun
                offset_to_attribute = data.getWORD(ao + 0x20) 

                # get dataruns of $AttrDef
                s = data.getStream(ao + offset_to_attribute, ao + offset_to_attribute + attr_length - 0x40)

                data_runs = self._decode_data_runs(s)

                for data_run in data_runs:
                    n, lcn = data_run

                    file_offset = lcn * self.sectors_per_cluster * self.bytes_per_sector
                    size_in_bytes = n * self.sectors_per_cluster * self.bytes_per_sector

                    start = file_offset
                    while file_offset < file_offset + size_in_bytes:
                        label = data.getStream(start, start+0x80)
                        label = helper.Helper._widechar_to_ascii(label)

                        tp = data.getDWORD(start + 0x80)

                        flags = data.getDWORD(start + 0x8c)

                        # last entry
                        if tp == 0x0:
                            break

                        _attrDef.add(label, tp, flags)

                        # next attrdef
                        start += 0xA0

            ao += attr_length

        log.debug('=====================     Dumping $AttrDef...     =====================')
        for a in _attrDef.getAttributes():
            log.debug('Attribute: {:30} type: 0x{:03X}, flags: 0x{:02X}'.format(a.name, a.type, a.flags))

        log.debug('')

        self.AttrDef = _attrDef
        return _attrDef

    def get_file_record(self, which_file_record):
        log = helper.Helper.logger()

        log.debug('==================== [File record #{}] ===================='.format(which_file_record))

        datarun = self._datarun_of_file_record(which_file_record)
        if datarun is None:
            # file record not found
            return None

        n, lcn, rel_record = datarun

        start_mft         = lcn * self.sectors_per_cluster * self.bytes_per_sector
        mft_size_in_bytes =   n * self.sectors_per_cluster * self.bytes_per_sector

        file_record_offset = start_mft + rel_record*self.file_record_size

        # simple check
        fr = file_record_offset

        # get buffered data model
        data = DataModel.BufferDataModel(self.dataModel.getStream(fr, fr + self.file_record_size), 'file_record')
        fr = 0

        magic = data.getStream(fr + 0x00, fr + 0x04)
     
        if magic != b"FILE":
            log.debug('magic does not mach "FILE", instead: {}.'.format(magic))
            return None
            #raise NtfsError('magic should mach "FILE", offset 0x{:x}'.format(fr))


        obj = filerecord.FileRecord(self)

        offset_update_seq = data.getWORD(fr + 0x04)
        log.debug('Offset to update sequence: 0x{:0x}'.format(offset_update_seq))

        size_update_seq = data.getWORD(fr + 0x06)
        log.debug('Size in words of update sequence: 0x{:0x}'.format(size_update_seq))

        update_seq = data.getWORD(fr + offset_update_seq)
        log.debug('Update Sequence number: 0x{:04x}'.format(update_seq))

        # skip update seq number
        update_seq_array = data.getStream(fr + offset_update_seq + 2, fr + offset_update_seq + 2 + size_update_seq * 2)

        g = 'Update Sequence: '
        for x in update_seq_array:
            g += '{:02x} '.format(x)
            
        log.debug('{}'.format(g))

        # fixup things
        fs_ntfs.NTFS.fixup_seq_numbers(data, update_seq_array, size_update_seq, update_seq, self.bytes_per_sector)


        off_first_attr = data.getWORD(fr + 0x14)

        flags = data.getWORD(fr + 0x16)
        log.debug('Flags: 0x{:0X}'.format(flags))

        real_size = data.getDWORD(fr + 0x18)
        log.debug('Real size of file record: 0x{:1X}'.format(real_size))

        allocated_size = data.getDWORD(fr + 0x1c)
        log.debug('Allocated size of file record: 0x{:0X}'.format(allocated_size))

        file_reference = data.getQWORD(fr + 0x20)
        log.debug('File reference to the base FILE record: 0x{:0X}'.format(file_reference))

        next_attribute_id = data.getWORD(fr + 0x28)
        log.debug('Next Attribute Id: 0x{:0X}'.format(next_attribute_id))

        log.debug('')

        obj.inode = which_file_record
        obj.off_first_attr = off_first_attr
        obj.flags = flags
        obj.real_size = real_size
        obj.allocated_size = allocated_size
        obj.file_reference = filerecord.FileReference(file_reference)
        obj.next_attribute_id = next_attribute_id

        #save fs geometry
        obj.sectors_per_cluster = self.sectors_per_cluster
        obj.bytes_per_sector = self.bytes_per_sector

        ao = fr + off_first_attr 

        log.debug('---=== attributes ===---')
        while 1:
            attribute = attributes.Attribute(data, ao)

            std_attr_type = data.getDWORD(ao + 0x00)
            if std_attr_type == 0xFFFFFFFF:
                break

            # standard attribute header
            log.debug('Attribute type: {0}'.format(self.AttrDef.getByType(std_attr_type).name))

            attr_length = data.getDWORD(ao + 0x04)
            log.debug('Length: 0x{:0X}'.format(attr_length))

            non_resident_flag = data.getBYTE(ao + 0x08)

            attr_name_length = data.getBYTE(ao + 0x09)

            log.debug('Non-resident flag: 0x{:0X}, name length: 0x{:0X}'.format(non_resident_flag, attr_name_length))

            # build instance

            attribute.std_header.type = std_attr_type
            attribute.std_header.attrdef = self.AttrDef.getByType(std_attr_type)
            attribute.std_header.length = attr_length
            attribute.std_header.non_resident_flag = non_resident_flag
            attribute.std_header.name_length = attr_name_length

            if not non_resident_flag and not attr_name_length:
                log.debug('Attribute is: {}'.format('resident, not named'))

                offset_to_attribute = data.getWORD(ao + 0x14)
                attr_length_2 = data.getDWORD(ao + 0x10)

                log.debug('Length of the attribute: 0x{:0X}'.format(attr_length_2))
                attr_name = ''

                # data is resident, so this will be length of data
                attribute.std_header.attr_real_size = attr_length_2

            if not non_resident_flag and  attr_name_length:
                log.debug('Attribute is: {}'.format('resident, named'))

                offset_to_attribute = data.getWORD(ao + 0x14)

                attr_name = data.getStream(ao + 0x18, ao + 0x18 + 2 * attr_name_length)
                attr_name = helper.Helper._widechar_to_ascii(attr_name)

                log.debug('Attribute name: {0}'.format(attr_name))

                attr_length_2 = data.getDWORD(ao + 0x10)
                log.debug('Length of the attribute: 0x{:0X}'.format(attr_length_2))

                # data is resident, so this will be length of data
                attribute.std_header.attr_real_size = attr_length_2

            if non_resident_flag and not attr_name_length:

                log.debug('Attribute is: {}'.format('non resident, not named'))

                starting_vcn = data.getQWORD(ao + 0x10)
                last_vcn = data.getQWORD(ao + 0x18)
                log.debug('Starting VCN: 0x{:0X}, last VCN: 0x{:0X}'.format(starting_vcn, last_vcn))

                attr_real_size = data.getQWORD(ao + 0x30)
                log.debug('Real size of the attribute: 0x{:0X}'.format(attr_real_size))
                attr_length_2 = attr_real_size

                # offset to datarun
                offset_to_attribute = data.getWORD(ao + 0x20) 
                attr_name = ''

                attribute.std_header.start_vcn = starting_vcn
                attribute.std_header.last_vcn = last_vcn
                attribute.std_header.attr_real_size = attr_real_size

                s = data.getStream(ao + offset_to_attribute, ao + offset_to_attribute + attr_length - 0x40)
                data_runs = self._decode_data_runs(s)

                attribute.data_runs = data_runs

                for data_run in data_runs:
                    n, lcn = data_run

                    file_offset = lcn * self.sectors_per_cluster * self.bytes_per_sector
                    size_in_bytes = n * self.sectors_per_cluster * self.bytes_per_sector

                    log.debug('0x{:04x} clusters @ LCN 0x{:08x}, @ f_offset 0x{:x}, size_in_bytes {:,}'.format(n, lcn, file_offset, size_in_bytes))

            if non_resident_flag and  attr_name_length:
                log.debug('Attribute is: {}'.format('non resident, named'))

                starting_vcn = data.getQWORD(ao + 0x10)
                last_vcn = data.getQWORD(ao + 0x18)
                log.debug('Starting VCN: 0x{:0X}, last VCN: 0x{:0X}'.format(starting_vcn, last_vcn))

                attr_name = data.getStream(ao + 0x40, ao + 0x40 + 2 * attr_name_length)
                attr_name = helper.Helper._widechar_to_ascii(attr_name)
                
                log.debug('Attribute name: {0}'.format(attr_name))

                attr_real_size = data.getQWORD(ao + 0x30)
                log.debug('Real size of the attribute: 0x{:0X}'.format(attr_real_size))
                attr_length_2 = attr_real_size

                attribute.std_header.start_vcn = starting_vcn
                attribute.std_header.last_vcn = last_vcn
                attribute.std_header.attr_real_size = attr_real_size

                # offset to datarun
                offset_to_attribute = data.getWORD(ao + 0x20) 

                s = data.getStream(ao + offset_to_attribute, ao + offset_to_attribute + attr_length - (2 * attr_name_length + 0x40))
                data_runs = self._decode_data_runs(s)

                attribute.data_runs = data_runs                

                for data_run in data_runs:
                    n, lcn = data_run

                    file_offset = lcn * self.sectors_per_cluster * self.bytes_per_sector
                    size_in_bytes = n * self.sectors_per_cluster * self.bytes_per_sector

                    log.debug('0x{:04x} clusters @ LCN 0x{:08x}, @ f_offset 0x{:x}, size_in_bytes {:,}'.format(n, lcn, file_offset, size_in_bytes))


            # populate std_header

            attribute.std_header.offset_to_attribute = offset_to_attribute
            attribute.std_header.length = attr_length_2
            attribute.std_header.name = attr_name

            attribute.dataModel = self.dataModel

            ao += attr_length

            attribute.obj = attributes.AttributeTypeFactory.recognize(attribute, obj)
            if attribute.obj is None:
                self.logger.debug('Attribute {} (0x{:x}) not supported yet.'.format(attribute.std_header.attrdef.name, attribute.std_header.attrdef.type))
                self.logger.debug('')

            obj.add_attribute(attribute)

        log.debug('---=== end attributes ===---')

        # postprocessing
        log.debug('postprocessing....')
        for attribute in obj.attributes:
            if attribute.obj:
                attribute.obj.postprocess()

        log.debug('')
        return obj

    def get_reparse_points(self):
        log = helper.Helper().logger()

        fr = self.get_filerecord_of_path(r'$Extend\$Reparse')
        if fr is None:
            log.debug('It seems we do not have $Reparse file, exiting.')
            return None

        D = []
        indexs = fr.get_attribute('$INDEX_ROOT')
        for index in indexs:
            if index.attribute.std_header.name == '$R':
                for entry in index.entries:
                    record_number = entry.mft_file_record.record_number
                    fr = self.get_file_record(record_number)
                    if fr is None:
                        log.debug("File record #{} referenced in $Reparse not found!".format(record_number))

                    D += [(record_number, fr.get_displayed_filename(), fr.get_reparse_point())]

        return D

    def get_filerecord_of_path(self, path):
        # we accept windows path

        # this is not an efficient implementation, because all antries are already fetched
        # so we are not using b-trees here actually. everything is fetched when get_file_record
        # is used. Anyway, this is not for speed, is mainly for investigating/research/play/whatever

        log = helper.Helper().logger()

        log.debug('')
        log.debug('traversing path: {}'.format(path))

        path = path.split('\\')

        # start from root
        fileref = 5
        path = path
        for i, current in enumerate(path):
            log.debug('we search for: {}'.format(current))

            root = self.get_file_record(fileref)

            if "$REPARSE_POINT" in root.attributes_dict:
                log.debug('reparse point: {}'.format(root.get_displayed_filename()))

                symlink = root.attributes_dict['$REPARSE_POINT'][0].substitute_path
                log.debug('symlink: {}'.format(symlink))

                # get rid of windows stuff
                symlink = symlink[7:]
                log.debug('resolved path: {}'.format(symlink + '\\' + current))

                # search in symlink target
                fo = self.get_filerecord_of_path(symlink + '\\' + current)
                if fo:
                    fileref = fo.inode
                else:
                    log.debug('not found, abort')
                    return None

                continue

            if '$INDEX_ROOT' in root.attributes_dict:
                # can we have more than one $INDEX_ROOT ?

                entries = root.attributes_dict['$INDEX_ROOT'][0].entries
                for entry in entries:
                    log.debug('entry: {}'.format(entry.filename))

                    if current == entry.filename:
                        fileref = entry.file_reference.record_number
                        log.debug('we select this entry: 0x{:X} (#{})'.format(fileref, fileref))
                        break


            else:
                log.debug('No index_root, no reparse ... nothing to do ...')
                break
                        

        # last file reference
        root = self.get_file_record(fileref)
        filenames = root.get_file_names()
        if filenames is None:
            log.debug('file not found.')
            return None

        filenames = [name for name, namespace in filenames]

        if current in filenames:
            if root._has_reparse_point():
                symlink = root.get_reparse_point()
                root = self.get_filerecord_of_path(symlink)


            log.debug('file found.')
            return root

        log.debug('file not found.')
        return None
