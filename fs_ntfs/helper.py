import logging

class Helper(object):
    @staticmethod
    def _widechar_to_ascii(s):
        return s.decode("utf-16", 'ignore').strip('\x00')

    @staticmethod
    def logger():
        logger = logging.getLogger('fs_ntfs')
        logger.setLevel(logging.DEBUG)
        return logger

