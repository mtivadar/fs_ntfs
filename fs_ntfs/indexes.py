from . import helper
from . import filerecord

class IndexHeader(object):
    def __init__(self):
        pass

class IndexEntry(object):
    def __init__(self):
        pass

class IndexTypeFactory(object):
    @staticmethod
    def recognize(index_name):
        for cls in Index_TYPES.__subclasses__():
            if cls.registered_for(index_name):
                return cls()

        return None

class Index_TYPES(object):
    def __init__(self, index_name):
        self.index_name = index_name

    def iterate_index_entries(self, data, off):
        pass

class Index_R(Index_TYPES):
    @classmethod
    def registered_for(cls, index_name):
        return index_name == '$R'

    def __init__(self):
        # $R

        log = helper.Helper.logger()
        return

    def iterate_index_entries(self, data, off):
        log = helper.Helper.logger()

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

            entry.mft_file_record = filerecord.FileReference(key_mft)
            key_mft_fr = entry.mft_file_record.record_number

            log.debug('key mft reference of reparse point: 0x{:x}, 0x{:x}'.format(key_mft, key_mft_fr))
            
            #self.file_record.mft.get_file_record(key_mft_fr)

            if r_flags & 1:
                vcn = data.getDWORD(off + 0x20)
                log.debug('vcn 0x{:x}'.format(vcn))

                entry.subnode_vcn = vcn
                nodes += [entry]

            if  r_flags & 2:
                break


            log.debug('')

            entries.append(entry)
            off += size_entry 

        return nodes, entries


class Index_I30(Index_TYPES):
    @classmethod
    def registered_for(cls, index_name):
        return index_name == '$I30'

    def __init__(self):
        # $I30

        log = helper.Helper.logger()
        return

    def iterate_index_entries(self, data, off):
        log = helper.Helper.logger()

        nodes = []
        entries = []
        while 1:
            log.debug('')
            log.debug('-= index entry =-')

            entry = IndexEntry()

            # index entry
            file_reference = data.getQWORD(off + 0)
            #print 'File reference: 0x{:0X}'.format(file_reference)
            entry.file_reference = filerecord.FileReference(file_reference)

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
            entry.filename = helper.Helper._widechar_to_ascii( data.getStream(off + entry.offset_to_filename, off + entry.offset_to_filename + entry.length_of_filename*2) )
            log.debug('Filename: {}'.format(entry.filename))

            # add entry object
            entries.append(entry)
            off += entry.length_index_entry 

        return nodes, entries

