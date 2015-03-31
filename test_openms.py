__author__ = 'Dubois'

if __name__ == "__main__":
    import pyopenms as P
    import time
    exp = P.MSExperiment()
    fh = P.FileHandler()
    path = "D:\\LCMS\\raw_files\\X20140626_006DP_pos_122.mzML"
    fh.loadExperiment(path, exp)
    mtd = P.MassTraceDetection()
    mass_traces = []
    t = time.clock()
    mtd.run(exp, mass_traces)
    print "Mass traces len : ", len(mass_traces)
    print "Elpased : ", time.clock() - t