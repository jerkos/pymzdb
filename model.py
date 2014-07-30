__author__ = 'Dubois'

from itertools import izip
from collections import namedtuple
import struct
import bytebuffer_cpp as bbcpp

BoundingBox = namedtuple(
    'BoundingBox', 'id, data, run_slice_id, first_spectrum_id, last_spectrum_id')


RunSliceHeader = namedtuple(
    'RunSliceHeader', 'id, ms_level, number, begin_mz, end_mz')


class Peak(object):

    def __init__(self, mz, int_, rt):
        self.mz = mz
        self.intensity = int_
        self.rt = rt

    def __str__(self):
        return "mz:{}, intensity:{}, time:{}".format(self.mz, self.intensity, self.rt)


class ScanSlice(object):

    def __init__(self, scan_id, run_slice_id, mzs, ints):
        self.scan_id = scan_id
        self.run_slice_id = run_slice_id
        self.mzs = mzs
        self.ints = ints

    def to_peaks(self, elution_time_by_scan_id):
        return [Peak(mz, int_, elution_time_by_scan_id[self.scan_id]) for mz, int_ in izip(self.mzs, self.ints)]


# utility functions to unpack bounding boxes
def build_struct():
    d = {}
    for i in xrange(1, 300):
        s = '<' + 'dfff' * i
        d[i] = struct.Struct(s)
    return d

metadata_struct = struct.Struct('<2i')
fitted_structure_str = 'dfff'

struct_by_nb_peaks = build_struct()


def _build_indexes(data):
    indexes = []
    offset = 0
    while offset < len(data):
        scan_id, nb_peaks = metadata_struct.unpack_from(data, offset)
        indexes.append((scan_id, nb_peaks))

        #skip scan_id and nb_peaks
        offset += 8

        #skip struct size
        offset += 20 * nb_peaks

    return indexes


def bb_to_scan_slices_v2(data, run_slice_id):
    l = '<'
    indexes = _build_indexes(data)
    for scan_id, nb_peaks in indexes:
        l += '2i'
        l += fitted_structure_str * nb_peaks
    f = struct.unpack(l, data)

    ss = []
    offset = 0
    for scan_id, nb_peaks in indexes:
        off = offset + 2
        subdata = f[off: off + nb_peaks * 4]
        s = ScanSlice(scan_id, run_slice_id, subdata[::4], subdata[1::4])
        ss.append(s)
        offset += nb_peaks * 4
    return ss


def bb_to_scan_slices(data, run_slice_id):
    offset = 0
    ss = []
    while offset < len(data):
        scan_id, nb_peaks = metadata_struct.unpack_from(data, offset)
        offset += 8
        if not nb_peaks:
            continue
            #print "detected null bbPeaks"

        #struct_ = '<' + RunSliceIterator.fitted_structure_str * nb_peaks
        #f = struct.unpack_from(struct_, data, offset)

        f = struct_by_nb_peaks[nb_peaks].unpack_from(data, offset)

        # the following line is less performant
        # may have to write a c extension to perform descent conversion
        #f = np.frombuffer(data, dtype=np.dtype('d,f,f,f'), count=nb_peaks, offset=offset)
        offset += 20 * nb_peaks
        s = ScanSlice(scan_id, run_slice_id, f[::4], f[1::4])
        ss.append(s)
    return ss


def bb_to_scan_slices_v3(data, run_slice_id):
    bytebuff = bbcpp.bytebufferCpp(data)
    offset = 0
    ss = []
    while offset < len(data):
        #scan_id, nb_peaks = metadata_struct.unpack_from(data, offset)
        scan_id = bytebuff.get_int(offset)
        offset += 4
        nb_peaks = bytebuff.get_int(offset)
        offset += 4

        if not nb_peaks:
            continue

        end_idx = 20 * nb_peaks
        mzs, ints = [], []
        for i in xrange(offset, end_idx, 20):
            mzs.append(bytebuff.get_double(i))
            ints.append(bytebuff.get_float(i + 8))
            #skip the left and right
        offset += 20 * nb_peaks
        s = ScanSlice(scan_id, run_slice_id, mzs, ints)
        ss.append(s)
    return ss
