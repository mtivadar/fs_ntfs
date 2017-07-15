# fs_ntfs
This will let you parse a NTFS file system.\
Attributes supported so far: 
 * **$FILE_NAME**
 * **$INDEX_ROOT**
 * **$INDEX_ALLOCATION**
 * **$DATA**
 * **$ATTRIBUTE_LIST**
 * **$REPARSE_POINT**

Supported functions: 
* parse $MFT
* list files in directories
* save content of files
* save content of alternate data streams
* will handle symlinks

Example file creates a detailed **debug log**, so data may be inspected.

```
usage: ntfs_parse.py [-h] [-f FILERECORD | -s SEARCH] [-w] [-l [LIST]]
                     [-q | -L LOG_FILE]
                     image

positional arguments:
  image                 NTFS files-system image.

optional arguments:
  -h, --help            show this help message and exit
  -f FILERECORD, --filerecord FILERECORD
                        Dump info for file record number.
  -s SEARCH, --search SEARCH
                        Search path. Will dump all info traversing
                        directories.
  -w, --fetch-file      Fetch all file's streams.
  -l [LIST], --list [LIST]
                        List files, specify recursion depth (default is 2).
                        Give -1 for a full recursion.
  -q, --quiet           No logging.
  -L LOG_FILE, --log-file LOG_FILE
                        Write to this logfile.

Usage: example.py \\.\c: -f 0 --fetch-file
       example.py \\.\c: -s $MFT --fetch-file
       example.py \\.\c: -s $MFTMirr
       example.py \\.\c: -s C:\pagefile.sys --fetch-file
       example.py \\.\c: -s "Documents and Settings\All Users\Application Data\Start Menu\desktop.ini" --fetch-file
           note: ?:\ and quotes will be skipped.
       example.py ntfs_image -f 123 --fetch-file
       
```
