import sqlite3 as sql
import struct
from model import BoundingBox, bb_to_scan_slices
from model import RunSliceHeader
from model import ScanSlice


class RunSliceIterator(object):
    sql_query = """SELECT bounding_box.* FROM bounding_box,
                   run_slice WHERE run_slice.ms_level = ?
                   AND bounding_box.run_slice_id = run_slice.id
                   ORDER BY run_slice.begin_mz"""

    header_query = """select id, ms_level, number, begin_mz, end_mz from run_slice where ms_level = ?"""

    def __init__(self, reader, ms_level):
        self.reader = reader
        self.cursor = reader.cursor
        self.ms_level = ms_level

        self.first_run_slice_bb = None
        self.curr_scan_slices = None

        self.r_header_by_id = self.get_run_slice_header_by_id()

        self.cursor.execute(RunSliceIterator.sql_query, (self.ms_level, ))

        self.first_run_slice_bb = BoundingBox._make(self.cursor.fetchone())

    def get_run_slice_header_by_id(self):
        d = {}
        for row in self.cursor.execute(RunSliceIterator.header_query, (self.ms_level,)):
            d[row[0]] = RunSliceHeader._make(row)
        return d

    def _init_iter(self):
        if self.first_run_slice_bb is None:
            raise StopIteration
        self.curr_scan_slices = bb_to_scan_slices(
            bytes(self.first_run_slice_bb.data), self.first_run_slice_bb.run_slice_id)

        while 1:
            row = self.cursor.fetchone()
            if row is None:
                self.first_run_slice_bb = None
                break
            bb = BoundingBox._make(row)
            if bb.run_slice_id == self.first_run_slice_bb.run_slice_id:
                self.curr_scan_slices += bb_to_scan_slices(
                    bytes(bb.data), bb.run_slice_id)
            else:
                self.first_run_slice_bb = bb
                break

    def has_next(self):
        if self.first_run_slice_bb is None:
            return False
        return True

    def __iter__(self):
        return self

    def next(self):
        self._init_iter()
        run_slice_id = self.curr_scan_slices[0].run_slice_id
        return self.r_header_by_id[run_slice_id], self.curr_scan_slices




if __name__ == '__main__':
    #filename = "D:\\LCMS\\raw_files\\huvec\\OEEMB100712_05.raw.mzDB"
    filename = "C:\\Users\\Marco\\Desktop\\X20140626_006DP_pos_122.raw.mzdb"
    #ri = RunSliceIterator(filename, 1)
    # c, i = 0, 1
    # t1 = time.clock()

    # while ri.has_next():
    #     print "runSlice #{}".format(i)
    #     header, ss = ri.next()
    #     for s in ss:
    #         c += len(s.mzs)
    #     i += 1
    # print "tot_nb_peaks:", c
    # print "Elapsed:", time.clock() - t1
    # reader = MzDBReader(filename)
    # #scan_id, mzs, ints = reader.get_scan(reader.connection.cursor(), 56)
    # #scan_id_, mzs_, ints_ = reader.get_scan(reader.connection.cursor(), 57)
    # import time
    # t1 = time.clock()
    # peaks = reader.get_xic(104.1, 104.2)
    # print time.clock() - t1
    # print "len(peaks):", len(peaks)
    # for p in peaks:
    #     print str(p)

    # import pyqtgraph as pg
    # qapp = pg.mkQApp()
    # pg.setConfigOptions(antialias=True)
    # lw = pg.GraphicsLayoutWidget()
    # #lw.addItem(pg.plot(mzs, ints, pen='b').getPlotItem())
    # #gridItem = pg.GridItem()
    # #pw.addItem(gridItem)
    # #pw.setLimits(xMin=mzs[0], xMax=mzs[-1], yMin=min(ints), yMax=max(ints))
    # #lw.addItem(pg.plot(mzs_, ints_, pen="r").getPlotItem())
    # lw.show()

    #qapp.exec_()