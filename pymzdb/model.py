from itertools import izip
from collections import namedtuple
import struct


BoundingBox = namedtuple('BoundingBox', 'id, data, run_slice_id, first_spectrum_id, last_spectrum_id')


RunSliceHeader = namedtuple('RunSliceHeader', 'id, ms_level, number, begin_mz, end_mz')


Peak = namedtuple('Peak', 'mz, intensity, rt, rs_id, scan_id')


DataEncoding = namedtuple('DataEncoding', 'id, mode, compression, byte_order, mz_precision, int_precision')


class ScanSlice(object):

    __slots__ = ['scan_id', 'run_slice_id', 'mzs', 'ints']

    def __init__(self, scan_id, run_slice_id, mzs, ints):
        self.scan_id = scan_id
        self.run_slice_id = run_slice_id
        self.mzs = mzs
        self.ints = ints

    def to_peaks(self, elution_time_by_scan_id):
        t = elution_time_by_scan_id[self.scan_id]
        return [Peak(mz, int_, t, self.run_slice_id, self.scan_id)
                for mz, int_ in izip(self.mzs, self.ints)] # if int_ > 0]


def build_struct(structure='dfff', max_nb_peaks=300):
    """
    utility functions to unpack bounding boxes

    :return:
    """
    d = {}
    for i in xrange(1, max_nb_peaks):
        s = '<' + structure * i
        d[i] = struct.Struct(s)
    return d

metadata_struct = struct.Struct('<2i')
metadata_struct_tic = struct.Struct('<i')

fitted_struct_str = 'dfff'
high_res_centroid_struct_str = 'df'
low_res_struct_str = 'ff'

fitted_struct_by_nb_peaks = build_struct(structure=fitted_struct_str)
high_res_centroid_struct_by_nb_peaks = build_struct(structure=high_res_centroid_struct_str)
low_res_centroid_struct_by_nb_peaks = build_struct(structure=low_res_struct_str)


def bb_to_scan_slices(data, run_slice_id, struct_by_scan_id):
    offset = 0
    ss = []
    scanids = []
    while offset < len(data):
        scan_id, nb_peaks = metadata_struct.unpack_from(data, offset)
        scanids.append(scan_id)
        offset += 8
        if not nb_peaks:
            continue

        precomputed_struct, size_struct, interval = struct_by_scan_id[scan_id]
        f = precomputed_struct[nb_peaks].unpack_from(data, offset)

        # the following line is less performant
        # may have to write a c extension to perform descent conversion
        # f = np.frombuffer(data, dtype=np.dtype('d,f,f,f'), count=nb_peaks, offset=offset)

        # offset += 20 * nb_peaks
        # s = ScanSlice(scan_id, run_slice_id, f[::4], f[1::4])

        offset += size_struct * nb_peaks
        s = ScanSlice(scan_id, run_slice_id, f[::interval], f[1::interval])

        ss.append(s)

    return ss


def read_tic(data):
    offset = 0
    nb_peaks = metadata_struct_tic.unpack_from(data, offset)[0]
    print nb_peaks
    offset += 4
    fmt = '<' + 'ff' * nb_peaks
    f = struct.unpack_from(fmt, data, offset)
    return f[::2], f[1::2]
