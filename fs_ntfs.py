import logging
import collections

import DataModel

class NtfsError(Exception):
    def __init__(self, message):
        super(NtfsError, self).__init__(message)

class Helper(object):
    @staticmethod
    def _widechar_to_ascii(s):
        return ''.join([chr(c) for c in s if c != 0])

    @staticmethod
    def logger():
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        return logger

    @staticmethod
    def _fixup_seq_numbers(data, update_seq_array, size_update_seq, update_seq, bytes_per_sector):
        log = Helper.logger()

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

class AttrDefEntry(object):
    def __init__(self, a, t, f):
        self._a = a
        self._t = t
        self._f = f

    @property
    def name(self):
        return self._a

    @property
    def type(self):
        return self._t

    @property
    def flags(self):
        return self._f

class AttrDef(object):
    def __init__(self):
        self._Attrs = []
        self._Index = {}
        pass

    def add(self, attribute, _type, flags):
        obj = AttrDefEntry(attribute, _type, flags)
        self._Attrs += [obj]
        self._Index[_type] = obj

    def getByType(self, t):
        if t in self._Index:
            return self._Index[t]
        else:
            raise NtfsError('Attribute type 0x{:0x} not found in $AttrDef.'.format(t))

    def getAttributes(self):
        return self._Attrs

class AttributeStandardHeader(object):
    def __init__(self):
        pass
    
class Attribute_TYPES(object):
    def __init__(self, attr_type):
        self.attr_type = attr_type

    def postprocess(self):
        pass

    def _get_datarun_of_vcn(self, vcn, data_runs):

        k = 0
        for data_run in data_runs:
            
            # file data model from our attribute
            data = self.attribute.data

            n, lcn = data_run

            """
            vcn: 1
            clusters: 1a, 2b, 2c
            -> 2b, vcn_rel: 0
            """

            # vcn is in this data_run ?
            if k <= vcn < k+n:
                return data_run, vcn - k

            k += n

        return None


    def _fetch_vcn(self, vcn, data_run_rel_vcn, datamodel):
        log = Helper.logger()
        file_record = self.file_record

        (n, lcn), rel_vcn = data_run_rel_vcn

        log.debug('\t\tVCN relative to data-run: {}'.format(rel_vcn))

        bytes_per_cluster = file_record.sectors_per_cluster * file_record.bytes_per_sector
        file_offset       = (lcn + rel_vcn) * self.file_record.sectors_per_cluster * self.file_record.bytes_per_sector
        #size_in_bytes     = n * self.file_record.sectors_per_cluster * self.file_record.bytes_per_sector

        # only one vcn
        # is it possible to have more than one cluster/entry ? !TODO
        size_in_bytes     = 1 * self.file_record.sectors_per_cluster * self.file_record.bytes_per_sector

        clusters = datamodel.getStream(file_offset, file_offset + size_in_bytes)

        log.debug('\t\tINDX: 0x{:04x} clusters @ LCN 0x{:04x}, @ f_offset 0x{:x}, size_in_bytes {}'.format(n, lcn, file_offset, size_in_bytes))

        # buffered data model
        data = DataModel.BufferDataModel(clusters, 'lcn')
        return data

class FileReference(object):
    def __init__(self, file_reference):
        self.record_number = file_reference & 0x0000FFFFFFFFFFFF
        self.seq_number = (file_reference & 0xFFFF) >> 48

class IndexHeader(object):
    def __init__(self):
        pass

class IndexEntry(object):
    def __init__(self):
        pass

class Attribute_INDEX_ALLOCATION(Attribute_TYPES):
    @classmethod
    def registered_for(cls, attr_type):
        return attr_type == 0xA0

    def __init__(self, attribute, file_record):
        # $INDEX_ALLOCATION

        self.entries = []
        self.attribute = attribute

        log = Helper.logger()

        return


class Attribute_INDEX_ROOT(Attribute_TYPES):
    @classmethod
    def registered_for(cls, attr_type):
        return attr_type == 0x90

    def _iterate_index_entries_R(self, data, off):
        log = Helper.logger()

        nodes = []
        entries = []
        while 1:
            log.debug('')
            log.debug('-= index entry =-')

            entry = IndexEntry()

            # index entry
            offset_data = data.getWORD(off + 0)
            log.debug('offset to data: 0x{:x}'.format(offset_data))

            size_data = data.getWORD(off + 0x2)
            log.debug('size of data: 0x{:x}'.format(size_data))

            size_entry = data.getWORD(off + 0x8)
            log.debug('size of entry: 0x{:x}'.format(size_entry))

            size_key = data.getWORD(off + 0xA)
            log.debug('size of key: 0x{:x}'.format(size_key))

            r_flags = data.getWORD(off + 0x0C)
            log.debug('flags: 0x{:x}'.format(r_flags))

            tag = data.getDWORD(off + 0x10)
            log.debug('key reparse tag: 0x{:x}'.format(tag))

            key_mft = data.getQWORD(off + 0x14)
            key_mft_fr = FileReference(key_mft).record_number
            log.debug('key mft reference of reparse point: 0x{:x}, 0x{:x}'.format(key_mft, key_mft_fr))
            

            self.file_record.mft.get_file_record(key_mft_fr)

            if r_flags & 1:
                kdata = data.getDWORD(off + 0x20)
                log.debug('vcn 0x{:x}'.format(kdata))

                entry.subnode_vcn = kdata
                nodes += [entry]

            if  r_flags & 2:
                break


            log.debug('')

            entries.append(entry)
            off += size_entry 

        return nodes, entries


    def _iterate_index_entries(self, data, off):
        log = Helper.logger()

        nodes = []
        entries = []
        while 1:
            log.debug('')
            log.debug('-= index entry =-')

            entry = IndexEntry()

            # index entry
            file_reference = data.getQWORD(off + 0)
            #print 'File reference: 0x{:0X}'.format(file_reference)
            entry.file_reference = FileReference(file_reference)

            entry.length_index_entry = data.getWORD(off + 8)
            #print 'Length of the index entry: 0x{:0X}'.format(entry.length_index_entry)

            entry.length_stream = data.getWORD(off + 10)
            #print 'Length of the stream: 0x{:0X}'.format(entry.length_stream)

            entry.index_flags = data.getBYTE(off + 12)
            log.debug('Index flags: 0x{:0X}'.format(entry.index_flags))

            if entry.index_flags & 1:
                entry.subnode_vcn = data.getQWORD(off + entry.length_index_entry - 8)
                log.debug('Last index entry, VCN of the sub-node in the Index Allocation: 0x{:0X}'.format(entry.subnode_vcn))
                nodes += [entry]

            if entry.index_flags & 2:
                # last index entry, exiting
                break


            entry.real_size_of_file = data.getQWORD(off + 0x40)
            log.debug('Real size of file: {:,}'.format(entry.real_size_of_file))

            entry.filename_namespace = data.getBYTE(off + 0x51)
            log.debug('Filename namespace: {}'.format(entry.filename_namespace))

            entry.length_of_filename = data.getBYTE(off + 0x50)
            log.debug('Length of the filename: 0x{:0X}'.format(entry.length_of_filename))

            entry.offset_to_filename = data.getWORD(off + 0x0a)
            log.debug('Offset to filename: 0x{:0X}'.format(entry.offset_to_filename))

            # in documentation, this seems to be fixed offset
            # however, this field seems to be wrong, because it's not always equal to 0x52 ...???
            entry.offset_to_filename = 0x52

            # file name from index (ie_filenname)
            entry.filename = Helper._widechar_to_ascii( data.getStream(off + entry.offset_to_filename, off + entry.offset_to_filename + entry.length_of_filename*2) )
            log.debug('Filename: {}'.format(entry.filename))

            # add entry object
            entries.append(entry)
            off += entry.length_index_entry 

        return nodes, entries


    def _process_INDX(self, data, index_allocation_dataruns, iter_function):
        log = Helper.logger()

        bytes_per_sector = self.file_record.bytes_per_sector

        ofs = 0

        indx_magic = data.getStream(ofs, ofs + 4)
        log.debug('Magic: {}'.format(indx_magic))

        if indx_magic != b'INDX':
            log.debug('Bad magic: {}, continue anyway with next data-run'.format(indx_magic))
            

        self.vcn_idx_record = data.getQWORD(ofs + 16)
        log.debug('VCN of this Index record in the Index Allocation: 0x{:0x}'.format(self.vcn_idx_record))

        self.ofs_first_index_entry = data.getDWORD(ofs + 0x18 + 0x00)
        self.total_size_of_index_entries = data.getDWORD(ofs + 0x18 + 0x04)

        log.debug('Offset to first index entry: 0x{:0X}'.format(self.ofs_first_index_entry))
        log.debug('Total size of index entries: 0x{:0X}'.format(self.total_size_of_index_entries))

        size_update_seq = data.getWORD(ofs + 6)
        log.debug('Size in words of Update Sequence: 0x{:0X}'.format(size_update_seq))

        update_seq = data.getWORD(ofs + 0x28)
        log.debug('Update Sequence number: 0x{:04x}'.format(update_seq))

        update_seq_array = data.getStream(ofs + 0x2a, ofs + 0x2a + size_update_seq * 2)

        g = 'Update Sequence: '
        for x in update_seq_array:
            g += '{:02x} '.format(x)
            
        log.debug('{}'.format(g))

        # fixup things
        Helper._fixup_seq_numbers(data, update_seq_array, size_update_seq, update_seq, self.file_record.bytes_per_sector)

        self.non_leaf_node = data.getBYTE(ofs + 0x18 + 0x0c)
        log.debug('Non-leaf node Flag (has sub-nodes): {}'.format(self.non_leaf_node))

        log.debug('')

        #off = ofs + 0x58 # FIXME! calculat #0x2a + size_update_seq*2 - 2

        # ofs_first_index_entry is relative to 0x18 (documentation says this)
        off = ofs + self.ofs_first_index_entry + 0x18

        log.debug('Iterating {} index...'.format(self.attribute.std_header.name))

        #nodes, entries = self._iterate_index_entries(data, off)
        nodes, entries = iter_function(data, off)
        if len(nodes) > 0:
            log.debug('!!! We have {} nodes !!!'.format(len(nodes)))

        for node in nodes:
            vcn = node.subnode_vcn
            data_run = self._get_datarun_of_vcn(vcn, index_allocation_dataruns)

            if data_run == None:
                log.debug('VCN {} not found in data-run, exiting.'.format(vcn))
                return

            newdata = self._fetch_vcn(vcn, data_run, self.attribute.dataModel)
            log.debug('+++ process b-tree node, vcn: 0x{:x}. +++'.format(vcn))
            self._process_INDX(newdata, index_allocation_dataruns, iter_function)

        # add entries
        self.entries.extend(entries)
        log.debug('')
        return nodes


    def postprocess(self):
        log = Helper.logger()

        # file data model from our attribute
        datamodel = self.attribute.dataModel

        # check if we have sub-nodes from root
        if len(self.root_nodes) == 0:
            log.debug('Nothing to post-process.')
            return

        # check if we have $INDEX_ALLOCATION
        if '$INDEX_ALLOCATION' not in self.file_record.attributes_dict:
            log.debug('We do not have $INDEX_ALLOCATION attribute, exiting.')
            return

        index_allocation = self.file_record.attributes_dict['$INDEX_ALLOCATION'][0]

        # check $I30
        if index_allocation.attribute.std_header.name != '$I30':
            if index_allocation.attribute.std_header.name != '$R':
                log.debug('!!! Index {} not supported yet. !!!'.format(index_allocation.attribute.std_header.name))
                return

        # for debugging purpose
        for data_run in index_allocation.attribute.data_runs:
            
            n, lcn = data_run

            file_offset = lcn * self.file_record.sectors_per_cluster * self.file_record.bytes_per_sector
            size_in_bytes = n * self.file_record.sectors_per_cluster * self.file_record.bytes_per_sector

            total_clusters_in_buffer = n

            log.debug('INDX: 0x{:04x} clusters @ LCN 0x{:04x}, @ f_offset 0x{:x}, size_in_bytes {}'.format(n, lcn, file_offset, size_in_bytes))


        for node in self.root_nodes:
            vcn = node.subnode_vcn
            log.debug('Need VCN: 0x{:0x}'.format(vcn))
            data_run = self._get_datarun_of_vcn(vcn, index_allocation.attribute.data_runs)

            if data_run == None:
                log.debug('VCN {} not found in data-run, exiting.'.format(vcn))
                return

            data = self._fetch_vcn(vcn, data_run, datamodel)

            # we should process INDX, recursively

            if index_allocation.attribute.std_header.name == '$I30':
                self._process_INDX(data, index_allocation.attribute.data_runs, self._iterate_index_entries)
            if index_allocation.attribute.std_header.name == '$R': 
                self._process_INDX(data, index_allocation.attribute.data_runs, self._iterate_index_entries_R)


    def __init__(self, attribute, file_record):

        # $INDEX_ROOT

        self.file_record = file_record
        self.attribute = attribute

        data = attribute.data
        ao   = attribute.ao

        ofs = ao + attribute.std_header.offset_to_attribute

        log = Helper.logger()

        log.debug('Attribute: {} (0x{:X})'.format(attribute.std_header.attrdef.name, attribute.std_header.attrdef.type))

        # index root attr
        self.bytes_per_index_record = data.getDWORD(ofs + 8)
        log.debug('Bytes per Index Record: 0x{:0X}'.format(self.bytes_per_index_record))

        self.clusters_per_index_record = data.getBYTE(ofs + 12)
        log.debug('Clusters per Index Record: 0x{:0X}'.format(self.clusters_per_index_record))


        self.index_header = IndexHeader()
        log.debug('-= index node header =-')
        # index node header
        self.index_header.ofs_first_index_entry = data.getDWORD(ofs + 16 + 0)
        log.debug('Offset to first index entry: 0x{:0X}'.format(self.index_header.ofs_first_index_entry))

        self.index_header.total_size_of_index_entries = data.getDWORD(ofs + 16 + 4)
        log.debug('Total size of index entries: 0x{:0X}'.format(self.index_header.total_size_of_index_entries))

        self.index_header.index_flags = data.getBYTE(ofs + 16 + 0x0c)
        log.debug('Large index (index allocation needed): {}'.format(self.index_header.index_flags))

        self.entries = []

        off = ofs + 16 + 16
        self.root_nodes = []

        if attribute.std_header.name == '$I30':
            # we support only this kind of index
            nodes, entries = self._iterate_index_entries(data, off)
            self.entries.extend(entries)

            log.debug('We have {} sub-nodes:'.format(len(nodes)))

            for node in nodes:
                log.debug('sub-node with VCN: 0x{:x}'.format(node.subnode_vcn))

            self.root_nodes = nodes

        elif attribute.std_header.name == '$R':
            nodes, entries = self._iterate_index_entries(data, off)
            self.entries.extend(entries)

            log.debug('We have {} sub-nodes:'.format(len(nodes)))

            for node in nodes:
                log.debug('sub-node with VCN: 0x{:x}'.format(node.subnode_vcn))

            self.root_nodes = nodes


        else:
            log.debug("!!! Index {} not supported. !!!".format(attribute.std_header.name))

        log.debug('')

class Attribute_DATA(Attribute_TYPES):
    @classmethod
    def registered_for(cls, attr_type):
        return attr_type == 0x80

    def __init__(self, attribute, file_record):
        log = Helper.logger()

        data = attribute.data
        ao   = attribute.ao

        self.attribute = attribute
        self.file_record = file_record

        if not attribute.std_header.non_resident_flag:
            # is resident

            ao = ao + attribute.std_header.offset_to_attribute
            
            self.blob = data.getStream(ao, ao + attribute.std_header.length)
            
            log.debug('data is contained in attribute, {} bytes.'.format(attribute.std_header.length))
            #log.debug(blob)

        if attribute.std_header.non_resident_flag:
            # is non resident, we have data runs

            for data_run in attribute.data_runs:
                n, lcn = data_run

                file_offset = lcn * file_record.sectors_per_cluster * file_record.bytes_per_sector
                size_in_bytes = n * file_record.sectors_per_cluster * file_record.bytes_per_sector

                log.debug('DATA: 0x{:04x} clusters @ LCN 0x{:04x}, @ f_offset 0x{:x}, size_in_bytes {:,}'.format(n, lcn, file_offset, size_in_bytes))

        log.debug('')

    def get_data(self):

        attribute = self.attribute
        file_record = self.file_record
        dataModel = attribute.dataModel

        blob = bytearray()

        size_of_data = attribute.std_header.attr_real_size
        if not attribute.std_header.non_resident_flag:
            yield self.blob

        if attribute.std_header.non_resident_flag:
            for data_run in attribute.data_runs:
                n, lcn = data_run

                file_offset = lcn * file_record.sectors_per_cluster * file_record.bytes_per_sector
                
                # size in bytes is rounded-up to cluster size (could hide data)
                size_in_bytes = n * file_record.sectors_per_cluster * file_record.bytes_per_sector

                # if the file is big, i guess one chunk is not rounded up
                size_to_read = min(size_in_bytes, size_of_data)

                if size_of_data == 0:
                    # I do not know why we have $DATA with real size of zero, but contains data runs...
                    size_to_read = size_in_bytes

                BIG = 100 * 1024 * 1024

                remains_to_read = size_to_read
                to_read = min(size_to_read, BIG)

                while to_read <= remains_to_read:
                    blob = dataModel.getStream(file_offset, file_offset + to_read)
                    yield blob

                    file_offset += to_read
                    remains_to_read -= to_read

                    if remains_to_read == 0:
                        break

                    to_read = min(remains_to_read, BIG)



                #blob += dataModel.getStream(file_offset, file_offset + size_to_read)

                size_of_data -= size_to_read

            self.blob = blob

        # if $data is resident, blob will be set in __init__
#        return self.blob

class Attribute_STANDARD_INFORMATION(Attribute_TYPES):
    @classmethod
    def registered_for(cls, attr_type):
        return attr_type == 0x10

    def __init__(self, attribute, file_record):
        log = Helper.logger()
        log.debug('')

class Attribute_REPARSE_POINT(Attribute_TYPES):
    @classmethod
    def registered_for(cls, attr_type):
        return attr_type == 0xC0

    def __init__(self, attribute, file_record):
        #$REPARSE_POINT

        log = Helper.logger()
        log.debug('')

        data = attribute.data
        ao   = attribute.ao + attribute.std_header.offset_to_attribute

        self.reparse_type = data.getDWORD(ao + 0x00)
        log.debug('Reparse type and flags: 0x{:08X}'.format(self.reparse_type))

        self.data_length = data.getWORD(ao + 0x04)
        log.debug('Reparse data length: 0x{:0X}'.format(self.data_length))

        # assume is symlink

        ao = ao + 0x8

        s_off = data.getWORD(ao + 0x00)
        s_len = data.getWORD(ao + 0x02)

        p_off = data.getWORD(ao + 0x04)
        p_len = data.getWORD(ao + 0x06)


        log.debug('substitute offset 0x{:x}, len 0x{:x}'.format(s_off, s_len))
        log.debug('print offset 0x{:x}, len 0x{:x}'.format(p_off, p_len))

        # documentation seems to be wrong about this ?!?!?
        ao += 0x8

        # it seems i have to add 4 to size ... WHY???
        buff = data.getStream(ao + s_off, ao + s_off + s_len)
        if self.reparse_type == 0xA000000C:
            # TODO! i do not know why is this. documentation says nothing
            buff = data.getStream(ao + s_off, ao + s_off + s_len + 4)

        self.substitute_path = Helper()._widechar_to_ascii(buff)
        log.debug('Substitute path: {}'.format(self.substitute_path))

        buff = data.getStream(ao + p_off, ao + p_off + p_len)
        buff = Helper()._widechar_to_ascii(buff)
        log.debug('Print path: {}'.format(buff))


        path_buffer = data.getStream(ao + 0x10, ao + 0x10 + self.data_length)


class Attribute_ATTRIBUTE_LIST(Attribute_TYPES):
    @classmethod
    def registered_for(cls, attr_type):
        return attr_type == 0x20

    def _fetch_vcns(self, start_vcn, last_vcn, data_runs, datamodel):
        newdata = bytearray()
        for vcn in range(start_vcn, last_vcn + 1):
            data_run = self._get_datarun_of_vcn(vcn, data_runs)

            if data_run == None:
                log.warning('VCN {} not found in data-run, exiting.'.format(vcn))
                return

            newdata += self._fetch_vcn(vcn, data_run, datamodel).raw

        data = DataModel.BufferDataModel(newdata, 'vcns')
        return data

    def __init__(self, attribute, file_record):
        # $ATTRIBUTE_LIST

        log = Helper.logger()
        self.attribute = attribute
        self.file_record = file_record

        log.debug('')

        # data model relative in file record
        data = attribute.data
        ao   = attribute.ao + attribute.std_header.offset_to_attribute

        if attribute.std_header.non_resident_flag:
            # attribute is non-residend, fetch it

            for data_run in attribute.data_runs:
                n, lcn = data_run

                file_offset = lcn * file_record.sectors_per_cluster * file_record.bytes_per_sector
                size_in_bytes = n * file_record.sectors_per_cluster * file_record.bytes_per_sector

                log.debug('DATA: 0x{:04x} clusters @ LCN 0x{:04x}, @ f_offset 0x{:x}, size_in_bytes {}'.format(n, lcn, file_offset, size_in_bytes))

            start_vcn = self.attribute.std_header.start_vcn
            last_vcn = self.attribute.std_header.last_vcn

            data = self._fetch_vcns(start_vcn, last_vcn, self.attribute.data_runs, self.attribute.dataModel)
            # we have new data buffer, so offset is 0now
            ao = 0

       

        attribute_list_length = attribute.std_header.length

        unq_file_records = collections.OrderedDict()

        while attribute_list_length > 0:
            self.type = data.getDWORD(ao + 0x00)
            log.debug('\t\tType: {} (0x{:0X})'.format(self.file_record.mft.AttrDef.getByType(self.type).name, self.type))

            if self.type == 0x0:
                break

            self.record_length = data.getWORD(ao + 0x04)
            log.debug('\t\tRecord length: 0x{:0X}'.format(self.record_length))

            self.name_length = data.getBYTE(ao + 0x06)
            log.debug('\t\tName length: 0x{:0X}'.format(self.name_length))

            self.offset_to_name = data.getBYTE(ao + 0x07)
            log.debug('\t\tOffset to name: 0x{:0X}'.format(self.offset_to_name))

            self.starting_vcn = data.getQWORD(ao + 0x08)
            log.debug('\t\tStarting VCN: 0x{:0X}'.format(self.starting_vcn))

            self.attribute_id = data.getWORD(ao + 0x18)
            #log.debug('\t\tAttribute Id: 0x{:0X}'.format(self.attribute_id))            

            self.base_file_reference = data.getQWORD(ao + 0x10)

            file_reference = FileReference(self.base_file_reference)
            log.debug('\t\tBase file reference: 0x{:0X}'.format(file_reference.record_number))

            if self.name_length != 0:
                self.name = data.getStream(ao + self.offset_to_name, ao + self.offset_to_name + self.name_length*2)

                name = Helper._widechar_to_ascii(self.name)
                log.debug('\t\tName: {}'.format(name))

            log.debug('')

            ao += self.record_length
            attribute_list_length -= self.record_length

            if file_reference.record_number != self.file_record.inode:
                unq_file_records[file_reference.record_number] = ''

        for inode in unq_file_records:

            log.debug('+++++ <file record from attribute list> +++++')
            fr = self.file_record.mft.get_file_record(inode)
            log.debug('+++++ </file record from attribute list> +++++')

            # add attributes
            for attr in fr.attributes:
                self.file_record.add_attribute(attr)

        log.debug('')

class Attribute_FILE_NAME(Attribute_TYPES):
    @classmethod
    def registered_for(cls, attr_type):
        return attr_type == 0x30

    def __init__(self, attribute, file_record):
        # $FILE_NAME

        log = Helper.logger()

        data = attribute.data
        ao   = attribute.ao

        self.allocated_size_of_file = data.getQWORD(ao + attribute.std_header.offset_to_attribute + 0x28)
        log.debug('Allocated size of file: 0x{:0X}'.format(self.allocated_size_of_file))

        self.real_size_of_file = data.getQWORD(ao + attribute.std_header.offset_to_attribute + 0x30)
        log.debug('Real size of file: 0x{:0X}'.format(self.real_size_of_file))

        self.attr_flags = data.getDWORD(ao + attribute.std_header.offset_to_attribute + 0x38)
        log.debug('Flags: 0x{:0X}'.format(self.attr_flags))

        self.filename_length = data.getBYTE(ao + attribute.std_header.offset_to_attribute + 0x40)

        self.filename_namespace = data.getBYTE(ao + attribute.std_header.offset_to_attribute + 0x41)
        log.debug('Filename namespace: {}'.format(self.filename_namespace))

        filename_offset = ao + attribute.std_header.offset_to_attribute + 0x42
        attr_filename = data.getStream(filename_offset, filename_offset + self.filename_length * 2)

        self.attr_filename = Helper._widechar_to_ascii(attr_filename)
        log.debug('File name: {0}'.format(self.attr_filename))

        log.debug('')

class AttributeType(object):
    @staticmethod
    def recognize(attribute, file_record):

        attr_type = attribute.std_header.attrdef.type

        for cls in Attribute_TYPES.__subclasses__():
            if cls.registered_for(attr_type):
                return cls(attribute, file_record)


        return None

class Attribute(object):
    def __init__(self, dataModel, ao):
        self.data = dataModel
        self.ao = ao # stream offset
        self.std_header = AttributeStandardHeader()

    def is_non_resident(self):
        try:
            return self.std_header.non_resident_flag
        except AttributeError:
            raise NtfsError("std_header is probably not set yet!")


class FileRecord(object):
    def __init__(self, mft):
        self.attributes = []
        self.attributes_dict = {}
        self.mft = mft

    def add_attribute(self, attribute):

        name = attribute.std_header.attrdef.name
        if name not in self.attributes_dict:
            self.attributes_dict[name] = [attribute.obj]
        else:
            self.attributes_dict[name] += [attribute.obj]

        self.attributes += [attribute]

    def get_attribute(self, name):
        if name not in self.attributes_dict:
            return None

        return self.attributes_dict[name]

    def get_displayed_filename(self):
        filenames = self.get_file_names()

        for namespace in [FileNamespace.POSIX, FileNamespace.WIN32, FileNamespace.WIN32_AND_DOS, FileNamespace.DOS]:
            L = [u for u, v in filenames if v == namespace]
            if len(L) > 0:
                return L[0]

        return None


    def get_file_names(self):
        filenames_attr = self.get_attribute('$FILE_NAME')
        if filenames_attr is None:
            return []

        filenames_attr = [(attr.attr_filename, attr.filename_namespace) for attr in filenames_attr]
        return filenames_attr

    def fetch_file(self, fo, stream=None):
        log = Helper().logger()

        log.debug('fetch file...')

        written_size = 0
        for chunk in self.get_file_data(stream):
            chunk_size = len(chunk)
            log.debug('\twrite {:,} bytes to file.'.format(chunk_size))
            fo.write(chunk)

            written_size += chunk_size
            log.debug('')

        return written_size

    def get_file_streams(self):
        log = Helper().logger()

        datas = self.get_attribute('$DATA')

        unnamed_datas = []
        streams = {}

        if datas is None:
            # we do not have $DATA
            return streams

        for data in datas:

            name = data.attribute.std_header.name

            if name:
                log.debug('stream name: {}'.format(name))
                if name in streams:
                    streams[name] += [data]
                else:
                    streams[name] = [data]

            else:
                # can there be unnamed streams that coresponde to different streams? 
                # ntfs is wired
                unnamed_datas += [data]

        streams[''] = unnamed_datas

        return streams

    def list_dir(self, levels=1):
        if levels == 0:
            return None

        indexs = self.get_attribute('$INDEX_ROOT')
        if indexs is None:
            return None

        D = collections.OrderedDict()
        for index in indexs:
            
            names = collections.OrderedDict()

            already = set()
            for a in index.entries:
                if a.file_reference.record_number not in already:
                    # we check set to exclude duplicates. almost all
                    # files have DOS & WIN32 namespace filenames

                    name = a.filename

                    if a.filename_namespace & FileNamespace.WIN32:
                        # not quite ok, some of the files will duplicate
                        already.add(a.file_reference.record_number)

                    fr = self.mft.get_file_record(a.file_reference.record_number)
                    Res = fr.list_dir(levels-1)
                    D[name] = Res

        return D

    def _has_reparse_point(self):
        return self.get_attribute('$REPARSE_POINT')

    def _get_reparse_point(self):
        log = Helper().logger()

        root = self

        if self._has_reparse_point():
            log.debug('reparse point: {}'.format(root.get_displayed_filename()))

            reparse = root.get_attribute('$REPARSE_POINT')

            symlink = reparse[0].substitute_path
            log.debug('symlink: {}'.format(symlink))

            # get rid of windows stuff
            symlink = symlink[7:]

            resolved = symlink
            log.debug('resolved path: {}'.format(resolved))

            return resolved

        else:
            return None


    def get_file_size(self, stream=None):
        # also we have real_size_of_file from $FILE_NAME
        # but no ADS

        log = Helper().logger()

        datas = self.get_attribute('$DATA')

        streams = self.get_file_streams()

        if stream is None:
            stream_datas = streams['']
        else:
            if stream in streams:
                stream_datas = streams[stream]
            else:
                log.debug('Stream {} not found.'.format(stream))
                return None

        first_data = stream_datas[0]
        file_size = first_data.attribute.std_header.attr_real_size

        return file_size



    def get_file_data(self, stream=None):
        log = Helper().logger()

        datas = self.get_attribute('$DATA')

        streams = self.get_file_streams()

        if stream is None:
            stream_datas = streams['']
        else:
            if stream in streams:
                stream_datas = streams[stream]
            else:
                log.debug('Stream {} not found.'.format(stream))
                raise StopIteration()
       
        # sort it. do we need to ?
        try:
            stream_datas.sort(key=lambda x: x.attribute.std_header.start_vcn)
        except AttributeError:
            # we have resident attributes that do not have 'start_vcn' field.
            # not sure if we have to treat them differently
            pass

        # we have a case where data is splitted in multiple $DATA attributes put in an $ATTRIBUTE_LIST
        # only first $DATA attribute has real_size set, the rest have 0
        # this is very ambiguous

        first_data = stream_datas[0]
        try:
            if first_data.attribute.std_header.start_vcn != 0:
                log.debug('first data attribute does not have vcn 0 !')
        except AttributeError:
            # we have resident attributes that do not have 'start_vcn' field.
            # not sure if we have to treat them differently
            pass

        file_size = first_data.attribute.std_header.attr_real_size
        log.debug('file size: {:,}'.format(file_size))

        file_chunks = ''
        for data in stream_datas:
            # fetch
            for chunk in data.get_data():
            #chunk = data.get_data()

                log.debug('get {:,} bytes from attribute.'.format(len(chunk)))
                chunk_size = len(chunk)

                if chunk_size > file_size:
                    # we have this shit when data is splitted accros multiple $DATA attributes with multiple clusters each... why ??
                    yield chunk[:file_size]
                else:
                    yield chunk

                file_size -= chunk_size



class MFT(object):
    def __init__(self, dataModel):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        self.dataModel = dataModel

        if self.dataModel.size() < 512:
            raise NtfsError("Invalid NTFS image")

        # compute $MFT cluster
        self.lcn_of_mft = self.dataModel.getQWORD(0x30)
        self.sectors_per_cluster = self.dataModel.getBYTE(0x0D)
        self.bytes_per_sector = self.dataModel.getWORD(0x0B)
        self.clusters_per_mft_record = self.dataModel.getDWORD(0x40)

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
            raise NtfsError('MFT initialization failed.')
        else:
            # ok
            pass

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

                    log.debug('0x{:04x} clusters @ LCN 0x{:04x}, @ f_offset 0x{:x}, size_in_bytes {:,}'.format(n, lcn, file_offset, size_in_bytes))

                self.mft_data_runs = data_runs
                return data_runs

            ao += attr_length

            # not found
        return None


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


    def _widechar_to_ascii(self, s):
        return ''.join([chr(c) for c in s if c != 0])

    def _build_attrdef(self):
        datarun = self._datarun_of_file_record(4)
        if datarun is None:
            # file record not found
            raise NtfsError('Cannot find $AttrDef.')

        n, lcn, rel_record = datarun

        start_mft = lcn * self.sectors_per_cluster * self.bytes_per_sector
        mft_size_in_bytes = n * self.sectors_per_cluster * self.bytes_per_sector

        file_record = start_mft + 4*self.file_record_size

        log = self.logger

        off_first_attr = self.dataModel.getWORD(file_record+0x14)
        data = self.dataModel

        ao = file_record + off_first_attr

        _attrDef = AttrDef()

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
                        label = self._widechar_to_ascii(label)

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
        log = Helper.logger()

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


        obj = FileRecord(self)

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
        Helper._fixup_seq_numbers(data, update_seq_array, size_update_seq, update_seq, self.bytes_per_sector)


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
        obj.file_reference = file_reference
        obj.next_attribute_id = next_attribute_id

        #save fs geometry
        obj.sectors_per_cluster = self.sectors_per_cluster
        obj.bytes_per_sector = self.bytes_per_sector

        ao = fr + off_first_attr 

        log.debug('---=== attributes ===---')
        while 1:
            #attribute = Attribute(self.dataModel, file_record_offset + ao)
            attribute = Attribute(data, ao)

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
                attr_name = Helper._widechar_to_ascii(attr_name)

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

                    log.debug('0x{:04x} clusters @ LCN 0x{:04x}, @ f_offset 0x{:x}, size_in_bytes {:,}'.format(n, lcn, file_offset, size_in_bytes))




            if non_resident_flag and  attr_name_length:
                log.debug('Attribute is: {}'.format('non resident, named'))

                starting_vcn = data.getQWORD(ao + 0x10)
                last_vcn = data.getQWORD(ao + 0x18)
                log.debug('Starting VCN: 0x{:0X}, last VCN: 0x{:0X}'.format(starting_vcn, last_vcn))

                attr_name = data.getStream(ao + 0x40, ao + 0x40 + 2 * attr_name_length)
                attr_name = Helper._widechar_to_ascii(attr_name)
                
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

                    log.debug('0x{:04x} clusters @ LCN 0x{:04x}, @ f_offset 0x{:x}, size_in_bytes {:,}'.format(n, lcn, file_offset, size_in_bytes))


            # populate std_header

            attribute.std_header.offset_to_attribute = offset_to_attribute
            attribute.std_header.length = attr_length_2
            attribute.std_header.name = attr_name

            attribute.dataModel = self.dataModel

            ao += attr_length

            attribute.obj = AttributeType.recognize(attribute, obj)
            if attribute.obj is None:
                self.logger.debug('Attribute {} (0x{:x}) not supported yet.'.format(attribute.std_header.attrdef.name, attribute.std_header.attrdef.type))
                self.logger.debug('')

            obj.add_attribute(attribute)
            #obj.attributes.append(attribute)
            #obj.attributes_dict[attribute.std_header.attrdef.name] = attribute.obj

        log.debug('---=== end attributes ===---')

        # postprocessing
        log.debug('postprocessing....')
        for attribute in obj.attributes:
            if attribute.obj:
                attribute.obj.postprocess()

        log.debug('')
        return obj


    def get_filerecord_of_path(self, path):
        # we accept windows path

        # this is not an efficient implementation, because all antries are already fetched
        # so we are not using b-trees here actually. everything is fetched when get_file_record
        # is used. Anyway, this is not for speed, is mainly for investigating/research/play/whatever

        log = Helper().logger()

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
                symlink = root._get_reparse_point()
                root = self.get_filerecord_of_path(symlink)


            log.debug('file found.')
            return root

        log.debug('file not found.')
        return None


    def _get_le(self, s):
        n = 0x00

        for x in s[::-1]:
            n = n << 8
            n = n | x

        n = self._sign_extend(n, len(s) * 8)

        return n

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

            log.debug('LCN relative 0x{:04x}, length_size: 0x{:x}, offset_size: 0x{:x}, n_clusters: 0x{:04x}, LCN start: 0x{:04x}'.format(rel_lcn_start, length_size, offset_size, n_clusters, lcn_start))

            s = s[1 + length_size + offset_size:]

            result += [(n_clusters, lcn_start)]
            prev_lcn_start = lcn_start

        log.debug('')

        return result


"""
todo:
   din ceva motiv, imaginea are anumiti bytes modificati !. se pare ca acei octeti pica in numele unor fisiere din index
        - am gasit de ce: fixups (update seq). inca nu e perfect rezolvat
        - fixed

   inca nu handleuim VCN sub-nodes la index entry! nu avem exemplu. 
        - avem acum, mai e ceva de lucru. nu primim sortat indexul.
        - fixed. nu e sortat perfect, da e ok

   in index_root poti sa ai intrari si sa si indice ca are subnodes !!!
   deci atentie la cautarea dupa fisier
         - aici e ciudatel putin. cred ca poti ori una ori alta.

   fixup la filerecord si la mft
         - trebuie vazut ....
         - fixed

    size of file ce il luam, nu e corect, este cel allocated.
         - fixed

"""