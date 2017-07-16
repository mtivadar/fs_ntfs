import logging

class Helper(object):
    @staticmethod
    def _widechar_to_ascii(s):
        return ''.join([chr(c) for c in s if c != 0])

    @staticmethod
    def logger():
        logger = logging.getLogger('fs_ntfs')
        logger.setLevel(logging.DEBUG)
        return logger

