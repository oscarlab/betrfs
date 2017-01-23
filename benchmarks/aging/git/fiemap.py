import array
import fcntl
import struct
import collections

# Public API
# From linux/fiemap.h
FIEMAP_FLAG_SYNC = 0x0001
FIEMAP_FLAG_XATTR = 0x0002
FIEMAP_FLAGS_COMPAT = FIEMAP_FLAG_SYNC | FIEMAP_FLAG_XATTR

FIEMAP_EXTENT_LAST = 0x0001
FIEMAP_EXTENT_UNKNOWN = 0x0002
FIEMAP_EXTENT_DELALLOC = 0x0004
FIEMAP_EXTENT_ENCODED = 0x0008
FIEMAP_EXTENT_DATA_ENCRYPTED = 0x0080
FIEMAP_EXTENT_NOT_ALIGNED = 0x0100
FIEMAP_EXTENT_DATA_INLINE = 0x0200
FIEMAP_EXTENT_DATA_TAIL = 0x0400
FIEMAP_EXTENT_UNWRITTEN = 0x0800
FIEMAP_EXTENT_MERGED = 0x1000
FIEMAP_EXTENT_SHARED = 0x2000

# Internals and plumbing
# From asm-generic/ioctl.h
_IOC_NRBITS = 8
_IOC_TYPEBITS = 8
_IOC_SIZEBITS = 14

_IOC_NRSHIFT = 0
_IOC_TYPESHIFT = _IOC_NRSHIFT + _IOC_NRBITS
_IOC_SIZESHIFT = _IOC_TYPESHIFT + _IOC_TYPEBITS
_IOC_DIRSHIFT = _IOC_SIZESHIFT + _IOC_SIZEBITS

_IOC_TYPECHECK = lambda struct: struct.size

_IOC = lambda dir_, type_, nr, size: \
    (dir_ << _IOC_DIRSHIFT) | (type_ << _IOC_TYPESHIFT) \
    | (nr << _IOC_NRSHIFT) | (size << _IOC_SIZESHIFT)
_IOC_WRITE = 1
_IOC_READ = 2
_IOWR = lambda type_, nr, size: \
    _IOC(_IOC_READ | _IOC_WRITE, type_, nr, _IOC_TYPECHECK(size))

# Derived from linux/fiemap.h
_struct_fiemap = struct.Struct('=QQLLLL')
_struct_fiemap_extent = struct.Struct('=QQQQQLLLL')

# From linux/fs.h
_FS_IOC_FIEMAP = _IOWR(ord('f'), 11, _struct_fiemap)

_UINT64_MAX = (2 ** 64) - 1

_fiemap = collections.namedtuple('fiemap',
    'start length flags mapped_extents extent_count extents')
_fiemap_extent = collections.namedtuple('fiemap_extent',
    'logical physical length flags')


# Public API, part 2
def fiemap(fd, start=0, length=_UINT64_MAX, flags=0, count=0):
    '''Retrieve extent mappings of a file using a given file descriptor

    This uses the *fiemap* ioctl implemented in the Linux kernel. See
    `Documentation/fiemap.txt`_ for more information.

    This procedure returns a named tuple containing all non-reserved fields as
    exposed by the fiemap and fiemap_extent structs. See
    :py:func:get_all_mappings if you want to retrieve all mappings of a given
    file.

    Note the attributes of the result don't have an *fm_* or *fe_* prefix.

    .. _Documentation/fiemap.txt: http://www.mjmwired.net/kernel/Documentation/filesystems/fiemap.txt

    :param fd: File descriptor to use
    :type fd: File-like object or `int`
    :param start: Start offset
    :type start: `int`
    :param length: Query length
    :type length: `int`
    :param flags: Flags
    :type flags: `int`
    :param count: Number of extents to request
    :type count: `int`

    :return: Mapping information
    :rtype: `_fiemap`
    '''

    fiemap_buffer = '%s%s' % (
        _struct_fiemap.pack(start, length, flags, 0, count, 0),
        '\0' * (_struct_fiemap_extent.size * count))

    # Turn into mutable C-level array of chars
    buffer_ = array.array('c', fiemap_buffer)

    # Syscall
    ret = fcntl.ioctl(fd, _FS_IOC_FIEMAP, buffer_)

    if ret < 0:
        raise IOError('ioctl')

    # Read out fiemap struct
    fm_start, fm_length, fm_flags, fm_mapped_extents, fm_extent_count, \
        fm_reserved = _struct_fiemap.unpack_from(buffer_)

    # Read out fiemap_extent structs
    fm_extents = []

    offset = _struct_fiemap.size
    for i in xrange(fm_extent_count):
        fe_logical, fe_physical, fe_length, _1, _2, fe_flags, _3, _4, _5 = \
            _struct_fiemap_extent.unpack_from(
                buffer_[offset:offset + _struct_fiemap_extent.size])

        fm_extents.append(
            _fiemap_extent(fe_logical, fe_physical, fe_length, fe_flags))

        del fe_logical, fe_physical, fe_length, fe_flags

        offset += _struct_fiemap_extent.size

    if fm_extents:
        assert fm_extents[-1].flags | FIEMAP_EXTENT_LAST == FIEMAP_EXTENT_LAST

    return _fiemap(
        fm_start, fm_length, fm_flags, fm_mapped_extents, fm_extent_count,
        fm_extents)


def get_all_mappings(fd, start=0, length=_UINT64_MAX, flags=0):
    '''Retrieve all extent mappings of a file using a given file descriptor

    This uses :py:func:`fiemap` to retrieve the mappings, twice.

    This procedure returns a named tuple containing all non-reserved fields as
    exposed by the fiemap and fiemap_extent structs.

    Note the attributes of the result don't have an *fm_* or *fe_* prefix.

    :param fd: File descriptor to use
    :type fd: File-like object or `int`
    :param start: Start offset
    :type start: `int`
    :param length: Query length
    :type length: `int`
    :param flags: Flags
    :type flags: `int`

    :return: Mapping information
    :rtype: `_fiemap`
    '''

    map1 = fiemap(fd, start=start, length=length, flags=flags, count=0)
    map2 = fiemap(fd, start=start, length=length, flags=flags,
        count=map1.mapped_extents)
    return map2


if __name__ == '__main__':
    import sys
    import pprint

    if len(sys.argv) < 2:
        sys.stderr.write('No filename(s) given')
        sys.exit(1)

    for file_ in sys.argv[1:]:
        with open(file_, 'r') as fd:
            print file_
            print '-' * len(file_)
            map_ = get_all_mappings(fd)
            pprint.pprint(map_)
            print