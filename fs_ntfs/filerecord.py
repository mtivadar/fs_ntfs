from . import helper
from . import fs_ntfs

class FileReference(object):
    def __init__(self, file_reference):
        self.record_number =  file_reference & 0x0000FFFFFFFFFFFF
        self.seq_number    = (file_reference & 0xFFFF000000000000) >> 48

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

        for namespace in [fs_ntfs.NTFS.FileNamespace.POSIX, fs_ntfs.NTFS.FileNamespace.WIN32, fs_ntfs.NTFS.FileNamespace.WIN32_AND_DOS, fs_ntfs.NTFS.FileNamespace.DOS]:
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
        log = helper.Helper.logger()

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
        log = helper.Helper.logger()

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

        D = []
        for index in indexs:
            
            already = set()

            for a in index.entries:
                if a.filename_namespace == FileNamespace.DOS:
                    # we check set to exclude duplicates. almost all
                    # files have DOS & WIN32 namespace filenames

                    # will we miss files this way ?
                    continue

                if a.file_reference.record_number not in already:

                    already.add(a.file_reference.record_number)
                    name = a.filename

                    if a.file_reference.record_number != 5:
                        fr = self.mft.get_file_record(a.file_reference.record_number)

                        Res = fr.list_dir(levels-1)
                        D.append((name, Res))
                    else:
                        D.append((name, None))

        return D

    def _has_reparse_point(self):
        return self.get_attribute('$REPARSE_POINT')

    def get_reparse_point(self):
        log = helper.Helper.logger()

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

        log = helper.Helper.logger()

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
        log = helper.Helper.logger()

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


