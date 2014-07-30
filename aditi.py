from sqlite3 import DatabaseError
from pyqtgraph import PlotWidget, mkPen, GridItem

__author__ = 'Marco'
__version__ = "0.0.1"

import time
import multiprocessing
import sqlite3

from PyQt4 import QtGui
from PyQt4.QtGui import QMainWindow, QApplication, QMenu, QAction, QKeySequence, QDockWidget, QTableView, \
    QStandardItemModel, QMessageBox, QDialog, QFormLayout, QFileDialog, QStandardItem, QColor, QLinearGradient, QBrush, \
    QHeaderView, QWidget, QVBoxLayout, QSpinBox, QDoubleSpinBox, QPushButton, QPen
from PyQt4.QtCore import Qt, QObject, SIGNAL, QThread, pyqtSignal, pyqtSlot

from colormaps import WithoutBlank
from mzdb_reader import MzDBReader


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
            for row in cursor:
                continue
            connection.close()
            return True
        except DatabaseError:
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
        self.plotitem_by_rawfile_short_path = {}

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

        self.plot_widget = PlotWidget(name="MainPlot", labels={'left': "Intensity", 'bottom': "Retention Time (sec)"})
        self.grid_item = GridItem()
        self.plot_widget.addItem(self.grid_item)
        self.setCentralWidget(self.plot_widget)

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

        #QObject.connect(self.rawfile_model, SIGNAL('itemChanged(QStandardItem *)'), self.item_changed)
        self.rawfile_model.itemChanged.connect(self.item_changed)

        self.addDockWidget(0x2, self.rawfile_dock_widget)

        #xic dock widget extraction parameter
        self.xic_dock_widget = QDockWidget("Xic extraction")

        self.xic_widget = XicWidget()
        self.xic_widget.plotButton.clicked.connect(self.plot)
        #QObject.connect(self.xic_widget.plotButton, SIGNAL('clicked()'), self.plot)


        self.xic_dock_widget.setWidget(self.xic_widget)
        self.addDockWidget(0x2, self.xic_dock_widget)

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
        print "called"
        files, r = obj[0], obj[1]
        n = len(files)
        not_database = []
        for i, f in enumerate(files):
            i_f = float(i)
            c = WithoutBlank.get_color(i_f / n, asQColor=True)
            c_ = WithoutBlank.get_color(i_f / n, asQColor=True)
            filename = f.split("\\")[-1]
            abs_path = str(f.replace("\\", "\\\\"))
            if r[i]:
                self.rawfiles_by_short_path[filename] = Rawfile(abs_path, c, filename)   #[MzDBReader(abs_path), c, True]
                self.rawfile_model.appendRow(Aditi.get_coloured_root_item(filename, c, c_))
            else:
                not_database.append(str(filename))
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
        self.plot_widget.clear()
        self.plot_widget.addItem(self.grid_item)

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
            min_time_val = min(min_time_val, times[0])
            max_time_val = max(max_time_val, times[-1])
            min_int_val = min(min_int_val, min(intensities))
            max_int_val = max(max_int_val, max(intensities))

            item = self.plot_widget.plot(times, intensities, pen=mkPen(color=rawfile.qcolor, width=2))
            item.curve.setClickable(True)

            # def on_curve_clicked():
            #     item.setPen(mkPen(color=rawfile.qcolor, width=4))
            # item.sigClicked = on_curve_clicked


            self.plotitem_by_rawfile_short_path[rawfile.short_path] = item
            self.plot_widget.setTitle(title="Xic@" + str(mz))
            self.plot_widget.setLimits(xMin=min_time_val, xMax=max_time_val, yMin=min_int_val, yMax=max_int_val)

    def update_plot_(self):
        for rawfile in self.rawfiles_by_short_path.viewvalues():
            if rawfile.is_checked:
                try:
                    self.plot_widget.addItem(self.plotitem_by_rawfile_short_path[rawfile.short_path])
                except KeyError:
                    mz = self.xic_widget.mzSpinBox.value()
                    mz_tol = self.xic_widget.mzTolSpinBox.value()

                    mz_diff = mz * mz_tol / 1e6
                    min_mz, max_mz = mz - mz_diff, mz + mz_diff
                    times, intensities = rawfile.reader.get_xic(min_mz, max_mz)
                    item = self.plot_widget.plot(times, intensities, pen=mkPen(color=rawfile.qcolor, width=2))
                    self.plotitem_by_rawfile_short_path[rawfile.short_path] = item
            else:
                try:
                    self.plot_widget.removeItem(self.plotitem_by_rawfile_short_path[rawfile.short_path])
                except KeyError:
                    pass

if __name__ == '__main__':
    import sys
    a = QApplication([])
    aditi = Aditi()
    aditi.show()
    sys.exit(a.exec_())
