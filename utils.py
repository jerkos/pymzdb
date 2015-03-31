from operator import attrgetter

__author__ = 'Dubois'

from collections import defaultdict as ddict
from _bisect import bisect_left


class PeakIndex(object):

    BIN_SIZE = 0.1
    INV_BIN_SIZE = 10

    def __init__(self, peaks=[]):
        """
        we assume that peaks are already sorted
        """
        self.peaks = peaks
        self._index = ddict(list)

        #self.min_val, self.max_val = self.peaks[0].mz, self.peaks[-1].mz
        self.min_val, self.max_val = 60, 895

        for e in self.peaks:
            bin_ = int(e.mz * self.INV_BIN_SIZE)
            self._index[bin_].append(e)

    def empty(self):
        """check emptyness of the index"""
        return len(self._index)

    def update(self, peaks, update_min=False, update_max=False):
        """add new peaks to the index"""
        for e in peaks:
            self._index[int(e.mz * self.INV_BIN_SIZE)].append(e)
        #update max assuming new peaks are all greater than previous set
        if update_min:
            self.min_val = peaks[0].mz
        if update_max:
            self.max_val = peaks[-1].mz

    def remove_by_mass(self, min_mz, max_mz):
        """Remove some peaks to not break the memory"""
        min_idx, max_idx = min_mz * self.INV_BIN_SIZE, max_mz * self.INV_BIN_SIZE
        inter_keys = list(set(range(min_idx, max_idx)).intersection(self._index.viewkeys()))
        for v in inter_keys:
            del self._index[v]

    def get_nearest_entity(self, val, tol, already_used):
        """
        get_nearest_entity(float, float, set) -> object or None

        :type val: float, requested mz value
        :rtype: return closest peak or None
        """
        bin_ = int(val * self.INV_BIN_SIZE)

        matching_peaks = self._index.get(bin_ - 1, []) + self._index.get(bin_, []) + self._index.get(bin_ + 1, [])

        if not matching_peaks:
            return None

        matching_peaks = set(matching_peaks) - already_used

        nearest_p = (None, 1e6)
        for p in matching_peaks:
            diff = abs(p.mz - val)
            if diff > tol:
                continue
            if diff < nearest_p[1]:
                nearest_p = (p, diff)

        return nearest_p[0]

        #shorter but less efficient ?
        #matching_peaks.sort(key=lambda x: abs(x.mz - val))
        #return matching_peaks[0] if (abs(matching_peaks[0].mz - val) < tol) else None

