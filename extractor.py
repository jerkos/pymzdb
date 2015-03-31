import time
import logging
from collections import deque
from collections import Iterable
from utils import PeakIndex
from _bisect import bisect_left
import multiprocessing

import pyopenms as openms
import numpy as np

from mzdb_reader import MzDBReader
from run_it import RunSliceIterator


__author__ = 'Marco'


# def flatten(l):
#     for i in l:
#         print i, "\n"
#     return list(chain.from_iterable(l))


def flatten(iterable):
    iterator = iter(iterable)
    array, stack = deque(), deque()
    while True:
        try:
            value = next(iterator)
        except StopIteration:
            if not stack:
                return tuple(array)
            iterator = stack.pop()
        else:
            if not isinstance(value, str) \
               and isinstance(value, Iterable):
                stack.append(iterator)
                iterator = iter(value)
            else:
                array.append(value)


class MassTrace(object):

    def __init__(self):
        self.mz = 0.0
        self._apex_peak = None

        #slow on indexing but iterating ?
        self.peaks = deque()

        self.curr_num_sum = 0.0
        self.curr_denom_sum = 0.0

        self.mz_centroid = 0.0

        self._mz_std = 0.0

    @property
    def mz_std(self):
        if not self._mz_std:
            self._mz_std = np.std([p.mz for p in self.peaks])
        return self._mz_std

    def append(self, p):
        self.peaks.append(p)
        self._update_mz_centroid(p)

    def append_left(self, p):
        self.peaks.appendleft(p)
        self._update_mz_centroid(p)

    def _update_mz_centroid(self, p):
        self.curr_denom_sum += p.intensity
        self.curr_num_sum += p.mz * p.intensity
        self.mz_centroid = self.curr_num_sum / self.curr_denom_sum

    @property
    def snr(self):
        first, last = self.peaks[0].intensity, self.peaks[-1].intensity
        return self.apex_peak.intensity / ((first + last) * 0.5)

    @property
    def apex_peak(self):
        return self._apex_peak

    @apex_peak.setter
    def apex_peak(self, p):
        self._apex_peak = p
        self.peaks.append(p)
        self._update_mz_centroid(p)
        self.mz = p.mz

    @property
    def length(self):
        return len(self.peaks)

    def to_openms_mass_trace(self):
        openms_peaks = []
        for p in list(self.peaks):
            openms_p = openms.Peak2D()
            openms_p.setMZ(p.mz)
            openms_p.setIntensity(p.intensity)
            openms_p.setRT(p.rt)
            openms_peaks.append(openms_p)
        return openms.MassTrace(openms_peaks)


class MassTraceExtractor(object):

    mz_extractor_func = lambda peak: peak.mz
    PPM = 0.000001

    def __init__(self, reader, ms_level=1, mz_tol_ppm=15, min_snr=1.5, gap_allowed=1):

        self.reader = reader
        self.rt_by_scan_id = self.reader.elution_time_by_scan_id_by_ms_level[ms_level]

        self.mz_tol_ppm = mz_tol_ppm
        self.min_snr = min_snr
        self.gap_allowed = gap_allowed

        self.scan_ids = sorted(self.rt_by_scan_id.viewkeys())
        self.n = len(self.scan_ids)

        self.rs_mzs_by_rs_id = self.reader.get_rs_mzs_by_rs_id()
        #self.prev_next_scan_ids_by_scan_id = self._prev_next_scan_ids_by_scan_id()

    def _prev_next_scan_ids_by_scan_id(self):
        d = {self.scan_ids[0]: (None, self.scan_ids[1]), self.scan_ids[-1]: (self.scan_ids[-1], None)}
        for i, scan_id in enumerate(self.scan_ids):
            if scan_id in {self.scan_ids[0], self.scan_ids[-1]}:
                continue
            d[scan_id] = (self.scan_ids[i-1], self.scan_ids[i+1])
        return d

    def _extract(self):
        run_slice_it = RunSliceIterator(self.reader, ms_level=1)

        #load in memory
        #rs = [(rsh, rs_ss) for rsh, rs_ss in run_slice_it]

        #init some variables
        all_peaks = []

        peaks_idx_by_scan_id = {scan_id: PeakIndex() for scan_id in self.scan_ids}

        curr_rsh, curr_ss = run_slice_it.next()
        prev_rsh, prev_ss = None, None
        next_rsh, next_ss = None, None

        #init with first rs peaks
        for curr_s in curr_ss:
            curr_peaks = curr_s.to_peaks(self.rt_by_scan_id)
            peaks_idx_by_scan_id[curr_s.scan_id].update(curr_peaks, update_min=True)
            map(all_peaks.append, curr_peaks)

        mass_traces = []
        already_used_peaks = set()

        #i = 0
        while run_slice_it.has_next():

            logging.info("Processing runslice id #{}".format(curr_rsh.id))

            next_rsh, next_ss = run_slice_it.next()

            for next_s in next_ss:
                next_peaks = next_s.to_peaks(self.rt_by_scan_id)
                peaks_idx_by_scan_id[next_s.scan_id].update(next_peaks, update_max=True)
                map(all_peaks.append, next_peaks)

            #slightly more efficient to iterate over this variable than allpeaks
            #since it contains less data points
            not_seen_yet = list(set(all_peaks) - already_used_peaks)
            #heapq.heapify(not_seen_yet)
            #logging.info("Sort #{} peaks by intensity".format(len(not_seen_yet)))
            not_seen_yet.sort(key=lambda p: p.intensity, reverse=True)
            #all_peaks.sort(key=lambda p: p.intensity, reverse=True) #attrgetter('intensity'), reverse=True)

            for peak in not_seen_yet:  #heapq.nlargest(len(not_seen_yet), not_seen_yet):

                if peak in already_used_peaks:
                    continue

                mass_trace = MassTrace()
                mass_trace.apex_peak = peak
                already_used_peaks.add(peak)
                scan_id = peak.scan_id

                curr_mz = mass_trace.mz_centroid  #peak.mz
                tol = curr_mz * self.mz_tol_ppm * self.PPM

                #note: surprisingly the dict approach is slower than working directly on the index
                # with bisect:
                #next_scan_id, prev_scan_id = self.prev_next_scan_ids_by_scan_id[scan_id]
                scan_id_idx = bisect_left(self.scan_ids, scan_id)

                #previous index
                prev_scan_id_idx = scan_id_idx - 1
                prev_scan_id = self.scan_ids[prev_scan_id_idx] if prev_scan_id_idx >= 0 else None

                #next index
                next_scan_id_idx = scan_id_idx + 1
                next_scan_id = self.scan_ids[next_scan_id_idx] if next_scan_id_idx < self.n else None

                #to the right

                right_gap = 0
                while next_scan_id is not None:
                    peaks_idx = peaks_idx_by_scan_id[next_scan_id]
                    p = peaks_idx.get_nearest_entity(curr_mz, tol, already_used_peaks)

                    if p is None:  # or p in already_used_peaks:
                        right_gap += 1
                        if right_gap > self.gap_allowed:
                            break
                    else:
                        mass_trace.append(p)
                        already_used_peaks.add(p)
                    next_scan_id_idx -= 1
                    next_scan_id = self.scan_ids[next_scan_id_idx] if next_scan_id_idx < self.n else None

                #to the left
                left_gap = 0
                while prev_scan_id is not None:
                    peaks_idx = peaks_idx_by_scan_id[prev_scan_id]
                    p = peaks_idx.get_nearest_entity(curr_mz, tol, already_used_peaks)

                    if p is None:  # or p in already_used_peaks:
                        left_gap += 1
                        if left_gap > self.gap_allowed:
                            break
                    else:
                        mass_trace.append_left(p)
                        already_used_peaks.add(p)

                    prev_scan_id_idx -= 1
                    prev_scan_id = self.scan_ids[prev_scan_id_idx] if prev_scan_id_idx >= 0 else None

                if mass_trace.length > 4: # and mass_trace.snr > self.min_snr:
                    mass_traces.append(mass_trace)

            #end for
            logging.info("#{} detected mass traces".format(len(mass_traces)))

            #clear some data points in the set buffer
            peaks_to_remove = {p for p in all_peaks if prev_rsh is not None and p.rs_id == prev_rsh.id}

            #clean already used peaks
            already_used_peaks = already_used_peaks - peaks_to_remove

            #remove prev rs peaks from all peaks
            all_peaks = list(set(all_peaks) - peaks_to_remove)

            #remove peak from the index
            if prev_rsh is not None:
                min_mz, max_mz = self.rs_mzs_by_rs_id[prev_rsh.id]
                for idx in peaks_idx_by_scan_id.viewvalues():
                    idx.remove_by_mass(min_mz, max_mz)

            #reassignment to good variables preparing for the next turn of the loop
            prev_rsh, prev_ss = curr_rsh, curr_ss
            curr_rsh, curr_ss = next_rsh, next_ss
            #i += 1
        #end while

        return mass_traces

    def extract_mass_traces(self, split_on_peakel=True):
        mass_traces = self._extract()
        return mass_traces

# def detect_peakel(mass_trace):
#     new_mass_traces = []
#     pd.detectPeaks(mass_trace, new_mass_traces)
#     return new_mass_traces





if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    s = "D:\\LCMS\\raw_files\\X20140626_006DP_pos_122.raw.mzdb"
    mzdb_reader = MzDBReader(s)
    # run_slice_it = RunSliceIterator(reader, ms_level=1)
    c = 0
    extractor = MassTraceExtractor(mzdb_reader)
    t1 = time.clock()
    x_mass_traces = extractor.extract_mass_traces()
    print "Elpased time: ", time.clock() - t1