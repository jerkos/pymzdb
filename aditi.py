


__author__ = 'Marco'
__version__ = "0.0.1"

import time
import multiprocessing
import sqlite3
import logging

from PyQt4 import QtGui
from PyQt4.QtGui import QMainWindow, QApplication, QAction, QKeySequence, QDockWidget, QTableView, \
    QStandardItemModel, QMessageBox, QFormLayout, QFileDialog, QStandardItem, QLinearGradient, QBrush, \
    QHeaderView, QWidget, QVBoxLayout, QDoubleSpinBox, QPushButton, QTabWidget
from PyQt4.QtCore import Qt, QThread, pyqtSignal

from colormaps import WithoutBlank
from mzdb_reader import MzDBReader

from sqlite3 import DatabaseError

from pyqtgraph import PlotWidget, mkPen, GridItem, GraphicsLayoutWidget, InfiniteLine, \
    BarGraphItem, setConfigOptions

CONFIG_OPTIONS = {
    'useOpenGL': False, ## by default, this is platform-dependent (see widgets/GraphicsView). Set to True or False to explicitly enable/disable opengl.
    'leftButtonPan': False,  ## if false, left button drags a rubber band for zooming in viewbox
    'foreground': 'k',  ## default foreground color for axes, labels, etc.
    'background': 'w',        ## default background for GraphicsWidget
    'antialias': True,
    'editorCommand': None,  ## command used to invoke code editor from ConsoleWidgets
    'useWeave': True,       ## Use weave to speed up some operations, if it is available
    'weaveDebug': False,    ## Print full error message if weave compile fails
    'exitCleanup': True,    ## Attempt to work around some exit crash bugs in PyQt and PySide
    'enableExperimental': True ## Enable experimental features (the curious can search for this key in the code)
}

setConfigOptions(**CONFIG_OPTIONS)


class XicWidget(QWidget):
    def __init__(self, parent=None):
        QWidget.__init__(self, parent)

        v = QVBoxLayout()
        f = QFormLayout()

        self.mzTolSpinBox = QDoubleSpinBox(self)
        self.mzTolSpinBox.setMaximum(100)
        self.mzTolSpinBox.setMinimum(1)
        self.mzTolSpinBox.setValue(10)

        f.addRow("mz tolerance(ppm):", self.mzTolSpinBox)

        self.mzSpinBox = QDoubleSpinBox(self)
        self.mzSpinBox.setMaximum(2000.0)
        self.mzSpinBox.setMinimum(300.0)
        self.mzSpinBox.setValue(500.0)

        f.addRow("requested m/z:", self.mzSpinBox)

        v.addLayout(f)

        self.plotButton = QPushButton("Plot")

        v.addWidget(self.plotButton)
        self.setLayout(v)

#         #self.plotButton.clicked.connect(self.parent().plot)


# # Preloader thread
def preload_bbs(f):
        sql = "SELECT * FROM bounding_box"
        try:
            connection = sqlite3.connect(str(f.replace("\\", "\\\\")))
            cursor = connection.cursor()
            cursor.execute(sql)
            i = 0
            for row in cursor:
                i += 1
                continue
            connection.close()
            if not i:
                logging.warn("No bounding box found...")
                return False
            return True
        except DatabaseError:
            logging.warn("Database error...")
            return False


class Preloader(QThread):

    loaded = pyqtSignal(object)

    def __init__(self, files, parent=None):
        QThread.__init__(self, parent)
        self.files = files

    def run(self):
        p = multiprocessing.Pool(multiprocessing.cpu_count())
        r = p.map(preload_bbs, self.files)
        p.close()
        #r = [preload_bbs(f)for f in self.files]
        self.loaded.emit((self.files, r))
#
#
# #Xic thread
# #not suitable
# def get_xic(data):
#     f = data[0]
#     min_mz, max_mz = data[1], data[2]
#     return MzDBReader(str(f.replace("\\", "\\\\"))).get_xic(min_mz, max_mz), data[3]
#
#
# class Extractor(QThread):
#
#     extracted = pyqtSignal(object)
#
#     def __init__(self, args, parent=None):
#         QThread.__init__(self, parent)
#         self.args = args
#
#     def run(self):
#         p = multiprocessing.Pool(multiprocessing.cpu_count())
#         r = p.map(get_xic, self.args)
#         self.extracted.emit(r)


class Rawfile(object):

    def __init__(self, abs_path, color, short_path=None):
        self.abs_path = abs_path
        self.reader = MzDBReader(self.abs_path)
        self.qcolor = color
        self.short_path = self.abs_path.split('\\\\')[-1] if short_path is None else short_path
        self.is_checked = True
        self.is_highlighted = False


#Main gui
class Aditi(QMainWindow):

    def __init__(self):
        QMainWindow.__init__(self)

        #title
        self.setWindowTitle("Aditi")
        self.setDockOptions(QMainWindow.VerticalTabs | QMainWindow.AnimatedDocks)
        #self.showMaximized()

        #model
        self.rawfiles_by_short_path = {}
        self.xic_by_rawfile_short_path = {}
        self.tic_by_rawfile_short_path = {}
        self.spec_by_rawfile_short_path = {}

        self.inf_line_tic_item = None
        self.curr_scan_id_by_short_path = {}

        # menu
        self.file_menu = self.menuBar().addMenu('&File')
        #self.file_menu.setTearOffEnabled(False)

        open_action = QAction("&Open...", self)
        open_action.setToolTip("Open a rawfile")
        open_action.setShortcut(QKeySequence(Qt.CTRL + Qt.Key_O))
        self.file_menu.addAction(open_action)
        open_action.triggered.connect(self.show_open_dialog)

        #QObject.connect(open_action, SIGNAL('triggered()'), self.show_open_dialog)

        exit_action = QAction("&Exit", self)
        exit_action.setShortcut(QKeySequence(Qt.CTRL + Qt.Key_Q))
        self.file_menu.addAction(exit_action)
        exit_action.triggered.connect(self.quit)
        #QObject.connect(exit_action, SIGNAL('triggered()'), self.quit)

        self.tab_widget = QTabWidget(self)
        #spectrum plot Widget
        self.graphics_layout_widget = GraphicsLayoutWidget(parent=self.tab_widget)

        self.graphics_layout_widget.keyPressEvent = self.handle_key_press_event

        self.graphics_layout_widget.useOpenGL(False)
        self.graphics_layout_widget.setAntialiasing(False)

        self.plot_widget_tic = self.graphics_layout_widget.addPlot(title="TIC(s)",
                                                                   labels={'left': "Intensity",
                                                                           'bottom': "Retention Time (sec)"})
        self.plot_widget_tic.showGrid(x=True, y=True)

        self.graphics_layout_widget.nextRow()

        self.plot_widget_spectrum = self.graphics_layout_widget.addPlot(title="Spectrum", labels={'left': "Intensity",
                                                                                                  'bottom': "m/z"})
        self.plot_widget_spectrum.showGrid(x=True, y=True)

        #finally add tab
        self.tab_widget.addTab(self.graphics_layout_widget, "Spectrum")

        #Xic plotWidget
        self.plot_widget_xic = PlotWidget(name="MainPlot", labels={'left': "Intensity",
                                                                   'bottom': "Retention Time (sec)"})
        self.plot_widget_xic.showGrid(x=True, y=True)

        self.tab_widget.addTab(self.plot_widget_xic, "Xic extraction")

        self.setCentralWidget(self.tab_widget)

        self.statusBar().showMessage("Ready")

        #dock 1
        self.rawfile_dock_widget = QDockWidget("Rawfiles")
        self.rawfile_table_view = QTableView()
        self.rawfile_table_view.horizontalHeader().setVisible(False)
        self.rawfile_table_view.horizontalHeader().setResizeMode(QHeaderView.ResizeToContents)
        self.rawfile_dock_widget.setWidget(self.rawfile_table_view)

        self.rawfile_model = QStandardItemModel()
        self.rawfile_model.setHorizontalHeaderLabels(["Rawfiles"])
        self.rawfile_table_view.setModel(self.rawfile_model)

        self.rawfile_model.itemChanged.connect(self.item_changed)

        self.addDockWidget(0x2, self.rawfile_dock_widget)

        #xic dock widget extraction parameter
        self.xic_dock_widget = QDockWidget("Xic extraction")

        self.xic_widget = XicWidget()
        self.xic_widget.plotButton.clicked.connect(self.plot)

        self.xic_dock_widget.setWidget(self.xic_widget)
        self.addDockWidget(0x2, self.xic_dock_widget)

    def handle_key_press_event(self, evt):
        if self.inf_line_tic_item is None:
            return

        times = []
        if evt.key() == Qt.Key_Left:
            for rawfile in self.rawfiles_by_short_path.values()[:1]:
                if not rawfile.is_checked:
                    continue
                curr_scan_id = self.curr_scan_id_by_short_path[rawfile.short_path]
                scan_ids = rawfile.reader.elution_time_by_scan_id_by_ms_level[1].keys()
                idx = scan_ids.index(curr_scan_id)
                times.append(rawfile.reader.elution_time_by_scan_id_by_ms_level[1][scan_ids[idx - 1]])
                self.curr_scan_id_by_short_path[rawfile.short_path] = scan_ids[idx - 1]

        elif evt.key() == Qt.Key_Right:
            for rawfile in self.rawfiles_by_short_path.values()[:1]:
                if not rawfile.is_checked:
                    continue
                curr_scan_id = self.curr_scan_id_by_short_path[rawfile.short_path]
                scan_ids = rawfile.reader.elution_time_by_scan_id_by_ms_level[1].keys()
                idx = scan_ids.index(curr_scan_id)
                times.append(rawfile.reader.elution_time_by_scan_id_by_ms_level[1][scan_ids[idx + 1]])
                self.curr_scan_id_by_short_path[rawfile.short_path] = scan_ids[idx + 1]

        self._plot_spectrum()

        self.inf_line_tic_item.setPos(sum(times) / float(len(times)))

    def _plot_spectrum(self):

        self.plot_widget_spectrum.clear()

        min_mz, max_mz = 1e9, 0
        min_int, max_int = 1e10, 0

        for rawfile in self.rawfiles_by_short_path.values():
            if not rawfile.is_checked:
                continue
            scan_id, mzs, intensities = rawfile.reader.get_scan(self.curr_scan_id_by_short_path[rawfile.short_path])
            # min_mz = min(min_mz, mzs[0])
            # max_mz = max(max_mz, mzs[-1])
            # min_int = min(min_int, min(intensities))
            # max_int = max(max_int, max(intensities))
            item = BarGraphItem(x=mzs, height=intensities, width=0.01, pen=rawfile.qcolor, brush=rawfile.qcolor)
            self.plot_widget_spectrum.addItem(item)

        #self.plot_widget_spectrum.setLimits(xMin=min_mz, xMax=max_mz, yMin=min_int, yMax=max_int)

    def plot_spectrum(self, ev):
        #clear
        if ev.button() == Qt.RightButton:
            return

        self.plot_widget_spectrum.clear()

        vb = self.plot_widget_tic.vb
        mouse_point = vb.mapSceneToView(ev.scenePos())
        t = mouse_point.x()
        if self.inf_line_tic_item is None:
            self.inf_line_tic_item = InfiniteLine(pos=t, angle=90)
            self.plot_widget_tic.addItem(self.inf_line_tic_item)
            self.inf_line_tic_item.setMovable(True)
        else:
            self.inf_line_tic_item.setPos(t)

        min_mz, max_mz = 1e9, 0
        min_int, max_int = 1e10, 0

        for rawfile in self.rawfiles_by_short_path.values():
            if not rawfile.is_checked:
                continue
            scan_id, mzs, intensities = rawfile.reader.get_scan_for_time(t)
            self.curr_scan_id_by_short_path[rawfile.short_path] = scan_id
            # min_mz = min(min_mz, mzs[0])
            # max_mz = max(max_mz, mzs[-1])
            # min_int = min(min_int, min(intensities))
            # max_int = max(max_int, max(intensities))
            item = BarGraphItem(x=mzs, height=intensities, width=0.01, pen=rawfile.qcolor, brush=rawfile.qcolor)
            self.plot_widget_spectrum.addItem(item)

        #self.plot_widget_spectrum.setLimits(xMin=min_mz, xMax=max_mz, yMin=min_int, yMax=max_int)

    def item_changed(self, item):
        print "item changed", item.text()
        s = item.text()
        if item.checkState():
            self.rawfiles_by_short_path[s].is_checked = True
        else:
            self.rawfiles_by_short_path[s].is_checked = False
        #self.qApp.emit(SIGNAL('redraw()'))
        self.update_plot_()

    def show_open_dialog(self):
        files = QFileDialog(self).getOpenFileNames()
        if files:
            preload = Preloader(files, self)
            preload.loaded.connect(self.update_rawfile_model)
            preload.start()

    def update_rawfile_model(self, obj):
        files, r = obj[0], obj[1]
        n = len(files)
        not_database = []
        min_time, max_time = 1e9, 0
        min_int, max_int = 1e9, 0
        for i, f in enumerate(files):
            i_f = float(i)
            c = WithoutBlank.get_color(i_f / n, asQColor=True)
            c_ = WithoutBlank.get_color(i_f / n, asQColor=True)
            filename = f.split("\\")[-1]
            abs_path = str(f.replace("\\", "\\\\"))
            if r[i]:
                rawfile = Rawfile(abs_path, c, filename)
                self.rawfiles_by_short_path[filename] = rawfile   #[MzDBReader(abs_path), c, True]
                self.rawfile_model.appendRow(Aditi.get_coloured_root_item(filename, c, c_))

                times, intensities = rawfile.reader.get_tic_v2()
                # min_time = min(min_time, min(times))
                # max_time = max(max_time, max(times))
                # min_int = min(min_int, min(intensities))
                # max_int = max(max_int, max(intensities))
                self.plot_widget_tic.plot(times, intensities, pen=mkPen(color=rawfile.qcolor, width=1.3))

            else:
                not_database.append(str(filename))

        #self.plot_widget_tic.setLimits(xMin=min_time, xMax=max_time, yMin=min_int, yMax=max_int)
        self.plot_widget_tic.scene().sigMouseClicked.connect(self.plot_spectrum)

        if not_database:
            v = "\n".join(not_database)
            QMessageBox.information(self, "Error",
                                    "The following files are not valid sqlite database:\n" + v)

    @staticmethod
    def get_coloured_root_item(filepath, color, colorr):
        root = QStandardItem(filepath)
        gradient = QLinearGradient(-100, -100, 100, 100)
        gradient.setColorAt(0.7, colorr)
        gradient.setColorAt(1, color)
        root.setBackground(QBrush(gradient))
        root.setEditable(False)
        root.setCheckState(Qt.Checked)
        root.setCheckable(True)
        return root

    def quit(self):
        res = QMessageBox.warning(self, "Exiting...", "Are you sure ?", QMessageBox.Ok | QMessageBox.Cancel)
        if res == QMessageBox.Cancel:
            return
        QtGui.qApp.quit()

    def plot(self):
        #clear pw
        self.plot_widget_xic.clear()

        # check sample checked
        checked_files = [rawfile for rawfile in self.rawfiles_by_short_path.values() if rawfile.is_checked]
        mz = self.xic_widget.mzSpinBox.value()
        mz_tol = self.xic_widget.mzTolSpinBox.value()

        mz_diff = mz * mz_tol / 1e6
        min_mz, max_mz = mz - mz_diff, mz + mz_diff

        #Thread implementation not as fast
        # args = [(data[0], min_mz, max_mz, data[2]) for data in checked_files]
        # extractor_thread = Extractor(args, self)
        # extractor_thread.extracted.connect(self._plot)
        # extractor_thread.start()

        min_time_val, max_time_val = 10000, 0
        min_int_val, max_int_val = 1e9, 0
        for rawfile in checked_files:
            t1 = time.clock()
            times, intensities = rawfile.reader.get_xic(min_mz, max_mz)
            print "elapsed: ", time.clock() - t1
            # min_time_val = min(min_time_val, times[0])
            # max_time_val = max(max_time_val, times[-1])
            # min_int_val = min(min_int_val, min(intensities))
            # max_int_val = max(max_int_val, max(intensities))

            item = self.plot_widget_xic.plot(times, intensities, pen=mkPen(color=rawfile.qcolor, width=1.3))
            item.curve.setClickable(True)

            def on_curve_clicked():
                if not rawfile.is_highlighted:
                    item.setPen(mkPen(color=rawfile.qcolor, width=4))
                    rawfile.is_highlighted = True
                else:
                    item.setPen(mkPen(color=rawfile.qcolor, width=2))
                    rawfile.is_highlighted = False

            item.sigClicked.connect(on_curve_clicked)
            #item.sigHovered = on_curve_clicked

            self.xic_by_rawfile_short_path[rawfile.short_path] = item
            self.plot_widget_xic.setTitle(title="Xic@" + str(mz))
            #self.plot_widget_xic.setLimits(xMin=min_time_val, xMax=max_time_val, yMin=min_int_val, yMax=max_int_val)

    def update_plot_(self):
        for rawfile in self.rawfiles_by_short_path.viewvalues():
            if rawfile.is_checked:
                try:
                    self.plot_widget_xic.addItem(self.xic_by_rawfile_short_path[rawfile.short_path])
                except KeyError:
                    mz = self.xic_widget.mzSpinBox.value()
                    mz_tol = self.xic_widget.mzTolSpinBox.value()

                    mz_diff = mz * mz_tol / 1e6
                    min_mz, max_mz = mz - mz_diff, mz + mz_diff
                    times, intensities = rawfile.reader.get_xic(min_mz, max_mz)
                    item = self.plot_widget_xic.plot(times, intensities, pen=mkPen(color=rawfile.qcolor, width=2))
                    self.xic_by_rawfile_short_path[rawfile.short_path] = item
            else:
                try:
                    #self.plot_widget_xic.removeItem(self.xic_by_rawfile_short_path[rawfile.short_path])
                    self.xic_by_rawfile_short_path[rawfile.short_path].hide()
                except KeyError:
                    pass

if __name__ == '__main__':
    import sys
    multiprocessing.freeze_support()
    a = QApplication([])
    aditi = Aditi()
    aditi.show()
    sys.exit(a.exec_())
