__author__ = 'Marco'

import sqlite3 as sql
from _bisect import bisect_left
from collections import defaultdict as ddict
from collections import OrderedDict as odict
from model import bb_to_scan_slices, read_tic, DataEncoding, fitted_struct_by_nb_peaks, high_res_centroid_struct_by_nb_peaks, low_res_centroid_struct_by_nb_peaks
#from model import bb_to_scan_slices_v3
import cStringIO


class MzDBReader(object):

    def __init__(self, filename):
        self.connection = sql.connect(filename)
        self.cursor = self.connection.cursor()

        # set optimal pragmas
        self.cursor.executescript("""PRAGMA synchronous=OFF;
                                     PRAGMA journal_mode=OFF;
                                     PRAGMA temp_store=3;
                                     PRAGMA cache_size=8000;
                                     PRAGMA page_size=4096;""")

        self.elution_time_by_scan_id_by_ms_level, self.data_encoding_id_by_scan_id = self._get_elution_time_by_scan_id()
        self.run_slices_begin_mz = self._get_run_slices_begin_mz()
        self.data_encodings = self._get_data_encodings()

        self.data_encodings_by_id = {d.id: d for d in self.data_encodings}

        self.struct_by_scan_id = {}
        for scan_id, de_id in self.data_encoding_id_by_scan_id.iteritems():
            de = self.data_encodings_by_id[de_id]
            if de.mode == 'fitted':
                self.struct_by_scan_id[scan_id] = (fitted_struct_by_nb_peaks, 20, 4)
            elif de.mode == 'centroid':
                if de.mz_precision == 64:
                    self.struct_by_scan_id[scan_id] = (high_res_centroid_struct_by_nb_peaks, 12, 2)
                elif de.mz_precision == 32:
                    self.struct_by_scan_id[scan_id] = (low_res_centroid_struct_by_nb_peaks, 8, 2)
            elif de.mode == 'profile':
                self.struct_by_scan_id[scan_id] = (high_res_centroid_struct_by_nb_peaks, 12, 2)
            else:
                # pass
                raise ValueError("Wrong encoding mode : {}".format(de.mode))

    def _get_data_encodings(self):
        """
        Fetch all data encodings
        :return:
        """
        sql_query = "select id, mode, compression, byte_order, mz_precision, intensity_precision from data_encoding"
        return [DataEncoding(*r) for r in self.cursor.execute(sql_query)]

    def get_rs_mzs_by_rs_id(self):
        sql_query = "SELECT id, begin_mz, end_mz FROM run_slice ORDER BY begin_mz"
        return {row[0]: (int(row[1]), int(row[2])) for row in self.cursor.execute(sql_query)}

    def _get_run_slices_begin_mz(self):
        sql_query = "SELECT begin_mz FROM run_slice ORDER BY begin_mz"
        return [row[0] for row in self.cursor.execute(sql_query)]

    def _get_elution_time_by_scan_id(self):
        sql_query = "SELECT id, time, ms_level, data_encoding_id FROM spectrum"
        self.cursor.execute(sql_query)
        elution_time_by_scan_id_by_ms_level = ddict(odict)
        data_encoding_id_by_scan_id = {}
        for row in self.cursor:
            scan_id = row[0]
            elution_time_by_scan_id_by_ms_level[row[2]][scan_id] = row[1]
            data_encoding_id_by_scan_id[scan_id] = row[3]
        return elution_time_by_scan_id_by_ms_level, data_encoding_id_by_scan_id

    def get_bb_first_scan_id(self, scan_id):
        sql_query = "SELECT bb_first_spectrum_id FROM spectrum WHERE id = ?"
        self.cursor.execute(sql_query, (scan_id,))
        return self.cursor.fetchone()[0]

    def get_scan_for_time(self, time, ms_level=1):
        """
        retrun the closest scan in time specified by time parameter
        ms_level is kw argument if a ms2 or ms1 scan is wanted

        :param time:
        :param ms_level:
        :return:
        """
        elution_time_by_scan_id = self.elution_time_by_scan_id_by_ms_level[ms_level]
        pairs = elution_time_by_scan_id.items()
        times = [t[1] for t in pairs]
        idx = bisect_left(times, time)
        idx_p = idx - 1
        scan_id = pairs[idx][0] if abs(times[idx] - time) < abs(times[idx_p] - time) else pairs[idx_p][0]
        return self.get_scan(scan_id)

    def get_scan(self, scan_id):
        first_scan_id = self.get_bb_first_scan_id(scan_id)
        print first_scan_id
        sql_query = "SELECT id, data, run_slice_id FROM bounding_box WHERE bounding_box.first_spectrum_id = ?"
        self.cursor.execute(sql_query, (first_scan_id,))

        mzs, ints = [], []
        for row in self.cursor:
            scan_slices = bb_to_scan_slices(row[1][:], row[2], self.struct_by_scan_id)
            for s in scan_slices:
                if s.scan_id == scan_id:
                    mzs += s.mzs
                    ints += s.ints
                    break
        return scan_id, mzs, ints

    def get_xic(self, min_mz, max_mz, min_rt=0.0, max_rt=0.0, ms_level=1):

        min_mz_rs = self.run_slices_begin_mz[bisect_left(self.run_slices_begin_mz, min_mz) - 1]
        max_mz_rs = self.run_slices_begin_mz[bisect_left(self.run_slices_begin_mz, max_mz) - 1]

        print "min max rs", min_mz_rs, max_mz_rs

        if min_mz_rs == max_mz_rs:
            sql_query = """SELECT bounding_box.id, data, run_slice_id, first_spectrum_id
                     FROM bounding_box, run_slice WHERE run_slice.ms_level = ?
                     AND bounding_box.run_slice_id = run_slice.id
                     AND run_slice.begin_mz = ?;"""

            self.cursor.execute(sql_query, (ms_level, min_mz_rs))
        else:
            sql_query = """SELECT bounding_box.id, data, run_slice_id, first_spectrum_id
                     FROM bounding_box, run_slice WHERE run_slice.ms_level = ?
                     AND bounding_box.run_slice_id = run_slice.id
                     AND (run_slice.begin_mz = ? OR run_slice.begin_mz = ?);"""
            self.cursor.execute(sql_query, (ms_level, min_mz_rs, max_mz_rs))

        intensities_by_scan_time = ddict(int)

        for row in self.cursor:
            scan_slices = bb_to_scan_slices(row[1][:], row[2], self.struct_by_scan_id)
            for ss in scan_slices:
                time = self.elution_time_by_scan_id_by_ms_level[ms_level][ss.scan_id]
                mzs = ss.mzs
                min_idx = bisect_left(mzs, min_mz)
                max_idx = bisect_left(mzs, max_mz)

                #indexes = np.searchsorted(mzs, [min_mz, max_mz])

                ints = ss.ints[min_idx: max_idx]
                #ints = ss.ints[indexes[0]: indexes[1]]
                if not ints: #.size:
                    continue

                m = max(ints)
                #try:
                if m > intensities_by_scan_time[time]:
                    intensities_by_scan_time[time] = m
                #except KeyError:
                #    intensities_by_scan_time[time] = m

        times = sorted(intensities_by_scan_time.viewkeys()) #intensities_by_scan_time.keys()
        intensities = [intensities_by_scan_time[t] for t in times]
        return times, intensities  #sorted(peaks, key=lambda pi: pi.rt)

    def get_tic(self):
        sql_query = "SELECT data_points FROM chromatogram WHERE id=1"

        self.cursor.execute(sql_query)
        data = self.cursor.fetchone()[0]
        return read_tic(data[:])

    def get_tic_v2(self):
        sql_query = "SELECT time, tic FROM spectrum WHERE spectrum.ms_level = 1"
        self.cursor.execute(sql_query)
        times, intensities = [], []
        for row in self.cursor:
            t, i = row
            times.append(t)
            intensities.append(i)
        return times, intensities