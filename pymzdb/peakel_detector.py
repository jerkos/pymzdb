__author__ = 'Dubois'

from scipy.signal import savgol_filter

class PeakelDetector(object):

    def __init__(self):
        pass

    def detect_peakel(self, mass_trace):
        fitlered = savgol_filter([p.intensity for p in mass_trace.peaks])