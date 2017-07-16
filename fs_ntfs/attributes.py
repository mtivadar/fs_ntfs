import collections

from . import helper
from . import DataModel
from . import indexes
from . import filerecord
from . import ntfs

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
            raise ntfs.NtfsError('Attribute type 0x{:0x} not found in $AttrDef.'.format(t))

    def getAttributes(self):
        return self._Attrs

class AttributeStandardHeader(object):
    def __init__(self):
        pass

class Attribute(object):
    def __init__(self, dataModel, ao):
        self.data = dataModel
        self.ao = ao # stream offset
        self.std_header = AttributeStandardHeader()

    def is_non_resident(self):
        try:
            return self.std_header.non_resident_flag
        except AttributeError:
            raise ntfs.NtfsError("std_header is probably not set yet!")

class AttributeTypeFactory(object):
    @staticmethod
    def recognize(attribute, file_record):

        attr_type = attribute.std_header.attrdef.type

        for cls in Attribute_TYPES.__subclasses__():
            if cls.registered_for(attr_type):
                return cls(attribute, file_record)


        return None

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
        log = helper.Helper.logger()
        file_record = self.file_record

        (n, lcn), rel_vcn = data_run_rel_vcn

        log.debug('\t\tVCN relative to data-run: {}'.format(rel_vcn))

        bytes_per_cluster = self.file_record.mft.sectors_per_cluster * self.file_record.mft.bytes_per_sector
        file_offset       = (lcn + rel_vcn) * self.file_record.mft.sectors_per_cluster * self.file_record.mft.bytes_per_sector
        #size_in_bytes     = n * self.file_record.mft.sectors_per_cluster * self.file_record.mft.bytes_per_sector

        # only one vcn
        # is it possible to have more than one cluster/entry ? !TODO
        size_in_bytes     = 1 * self.file_record.mft.sectors_per_cluster * self.file_record.mft.bytes_per_sector

        clusters = datamodel.getStream(file_offset, file_offset + size_in_bytes)

        log.debug('\t\tINDX: 0x{:04x} clusters @ LCN 0x{:04x}, @ f_offset 0x{:x}, size_in_bytes {}'.format(n, lcn, file_offset, size_in_bytes))

        # buffered data model
        data = DataModel.BufferDataModel(clusters, 'lcn')
        return data

class Attribute_INDEX_ALLOCATION(Attribute_TYPES):
    @classmethod
    def registered_for(cls, attr_type):
        return attr_type == 0xA0

    def __init__(self, attribute, file_record):
        # $INDEX_ALLOCATION

        self.entries = []
        self.attribute = attribute

        log = helper.Helper.logger()

        return

class Attribute_INDEX_ROOT(Attribute_TYPES):
    @classmethod
    def registered_for(cls, attr_type):
        return attr_type == 0x90

    def _process_INDX(self, data, index_allocation_dataruns, iter_function):
        log = helper.Helper.logger()

        bytes_per_sector = self.file_record.mft.bytes_per_sector

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
        ntfs.NTFS.fixup_seq_numbers(data, update_seq_array, size_update_seq, update_seq, self.file_record.mft.bytes_per_sector)

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
        log = helper.Helper.logger()

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

        # check if index type is registered

        obj_index = indexes.IndexTypeFactory.recognize(index_allocation.attribute.std_header.name)
        if obj_index is None:
            log.debug('!!! Index {} not supported yet. !!!'.format(index_allocation.attribute.std_header.name))
            return

        # for debugging purpose
        for data_run in index_allocation.attribute.data_runs:
            
            n, lcn = data_run

            file_offset = lcn * self.file_record.mft.sectors_per_cluster * self.file_record.mft.bytes_per_sector
            size_in_bytes = n * self.file_record.mft.sectors_per_cluster * self.file_record.mft.bytes_per_sector

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
            self._process_INDX(data, index_allocation.attribute.data_runs, obj_index.iterate_index_entries)


    def __init__(self, attribute, file_record):

        # $INDEX_ROOT

        self.file_record = file_record
        self.attribute = attribute

        data = attribute.data
        ao   = attribute.ao

        ofs = ao + attribute.std_header.offset_to_attribute

        log = helper.Helper.logger()

        log.debug('Attribute: {} (0x{:X})'.format(attribute.std_header.attrdef.name, attribute.std_header.attrdef.type))

        # index root attr
        self.bytes_per_index_record = data.getDWORD(ofs + 8)
        log.debug('Bytes per Index Record: 0x{:0X}'.format(self.bytes_per_index_record))

        self.clusters_per_index_record = data.getBYTE(ofs + 12)
        log.debug('Clusters per Index Record: 0x{:0X}'.format(self.clusters_per_index_record))


        self.index_header = indexes.IndexHeader()
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


        # i'm not sure why in index_root, index type == $R, we actually have the format from $I30
        obj_index = indexes.IndexTypeFactory.recognize('$I30')#attribute.std_header.name)
        if obj_index is None:
            log.debug("!!! Index {} not supported. !!!".format(attribute.std_header.name))

        if attribute.std_header.name == '$I30':
            # we support only this kind of index
            nodes, entries = obj_index.iterate_index_entries(data, off)

        elif attribute.std_header.name == '$R':
            # cannot use factory because we have to call same interations as in $I30... why... ?
            nodes, entries = obj_index.iterate_index_entries(data, off)

        else:
            log.debug("!!! Index {} not supported. !!!".format(attribute.std_header.name))
            return

        self.entries.extend(entries)

        log.debug('We have {} sub-nodes:'.format(len(nodes)))

        for node in nodes:
            log.debug('sub-node with VCN: 0x{:x}'.format(node.subnode_vcn))

        self.root_nodes = nodes

        log.debug('')

class Attribute_DATA(Attribute_TYPES):
    @classmethod
    def registered_for(cls, attr_type):
        return attr_type == 0x80

    def __init__(self, attribute, file_record):
        log = helper.Helper.logger()

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

                file_offset = lcn * file_record.mft.sectors_per_cluster * file_record.mft.bytes_per_sector
                size_in_bytes = n * file_record.mft.sectors_per_cluster * file_record.mft.bytes_per_sector

                log.debug('DATA: 0x{:04x} clusters @ LCN 0x{:08x}, @ f_offset 0x{:x}, size_in_bytes {:,}'.format(n, lcn, file_offset, size_in_bytes))

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

                file_offset = lcn * file_record.mft.sectors_per_cluster * file_record.mft.bytes_per_sector
                
                # size in bytes is rounded-up to cluster size (could hide data)
                size_in_bytes = n * file_record.mft.sectors_per_cluster * file_record.mft.bytes_per_sector

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


                size_of_data -= size_to_read

            self.blob = blob

class Attribute_STANDARD_INFORMATION(Attribute_TYPES):
    @classmethod
    def registered_for(cls, attr_type):
        return attr_type == 0x10

    def __init__(self, attribute, file_record):
        log = helper.Helper.logger()
        log.debug('')

class Attribute_REPARSE_POINT(Attribute_TYPES):
    @classmethod
    def registered_for(cls, attr_type):
        return attr_type == 0xC0

    def __init__(self, attribute, file_record):
        #$REPARSE_POINT

        log = helper.Helper.logger()
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

        self.substitute_path = helper.Helper._widechar_to_ascii(buff)
        log.debug('Substitute path: {}'.format(self.substitute_path))

        buff = data.getStream(ao + p_off, ao + p_off + p_len)
        buff = helper.Helper._widechar_to_ascii(buff)
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

        log = helper.Helper.logger()
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

                file_offset = lcn * file_record.mft.sectors_per_cluster * file_record.mft.bytes_per_sector
                size_in_bytes = n * file_record.mft.sectors_per_cluster * file_record.mft.bytes_per_sector

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

            file_reference = filerecord.FileReference(self.base_file_reference)
            log.debug('\t\tBase file reference: 0x{:0X}'.format(file_reference.record_number))

            if self.name_length != 0:
                self.name = data.getStream(ao + self.offset_to_name, ao + self.offset_to_name + self.name_length*2)

                name = helper.Helper._widechar_to_ascii(self.name)
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

        log = helper.Helper.logger()

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

        self.attr_filename = helper.Helper._widechar_to_ascii(attr_filename)
        log.debug('File name: {0}'.format(self.attr_filename))

        log.debug('')
