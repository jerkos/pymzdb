__author__ = 'Marco'
class Colormap(object):

    def __init__(self, name, *args, **kwargs):
        ''' Build a new colormap from given (value,color) list.

           name: str
               Colormap name

           args: [(value,Color),...]
               Value/color couples to build colormap from.
               Values must be normalized between 0 and 1
        '''
        self.name = name
        self.vcolors = []
        self.alpha = 1.0
        for value,color in args:
            self._append(value, color)


    def _append(self, value, color):
        ''' Append a new value/color '''
        self.vcolors.append( [value,color] )
        self.vcolors.sort(lambda x,y: int(x[0]-y[0]))


    def get_color (self, value, asQColor=False, alpha=0.5):
        ''' Get interpolated color from value '''

        if not len(self.vcolors):
            return (0.,0.,0.,self.alpha)
        elif len(self.vcolors) == 1:
            return self.vcolors[0][1]
        elif value < 0.0:
            return self.vcolors[0][1]
        elif value > 1.0:
            return self.vcolors[-1][1]
        sup_color = self.vcolors[0]
        inf_color = self.vcolors[-1]
        for i in xrange (len(self.vcolors)-1):
            if value < self.vcolors[i+1][0]:
                inf_color = self.vcolors[i]
                sup_color = self.vcolors[i+1]
                break
        r = (value-inf_color[0])/(sup_color[0]-inf_color[0])
        if not asQColor:
            return (sup_color[1][0]*r + inf_color[1][0]*(1-r),
                    sup_color[1][1]*r + inf_color[1][1]*(1-r),
                    sup_color[1][2]*r + inf_color[1][2]*(1-r))

        from PyQt4.QtGui import QColor
        q = QColor()
        q.setRedF(sup_color[1][0]*r + inf_color[1][0]*(1-r))
        q.setGreen(sup_color[1][1]*r + inf_color[1][1]*(1-r))
        q.setBlueF(sup_color[1][2]*r + inf_color[1][2]*(1-r))
        q.setAlphaF(alpha)
        return q


    def getQColor(self, value):
        """
        DEPRECATED
        """
        if not len(self.vcolors):
            return (0.,0.,0.,self.alpha)
        elif len(self.vcolors) == 1:
            return self.vcolors[0][1]
        elif value < 0.0:
            return self.vcolors[0][1]
        elif value > 1.0:
            return self.vcolors[-1][1]
        sup_color = self.vcolors[0]
        inf_color = self.vcolors[-1]
        for i in xrange (len(self.vcolors)-1):
            if value < self.vcolors[i+1][0]:
                inf_color = self.vcolors[i]
                sup_color = self.vcolors[i+1]
                break
        r = (value-inf_color[0])/(sup_color[0]-inf_color[0])
        return (sup_color[1][0]*r + inf_color[1][0]*(1-r),
                sup_color[1][1]*r + inf_color[1][1]*(1-r),
                sup_color[1][2]*r + inf_color[1][2]*(1-r))




# Default colormaps
# ------------------------------------------------------------------------------
WithoutBlank = Colormap("WithoutBlank",
                      (0.00, (1.0, 0.0, 0.)),
                      (0.25, (0.5, 0.0, 0.5)),
                      (0.50, (0.0, 0.0, 1.0)),
                      (0.75, (0.0, 0.5, 0.5)),
                      (1.00, (0.0, 1.0, 0.0)))

GreenRed=Colormap("GreenRed",
                  (0.,(0.,1.,0.)),
                  (.5, (.5,.5,0.)),
                  (1.,(1.,0.,0.)))

IceAndFire2 = Colormap("IceAndFire",
                      (0.00, (0.0, 0.0, 1.0)),
                      (0.25, (0.0, 0.5, 1.0)),
                      (0.50, (1.0, 1.0, 1.0)),
                      (0.75, (1.0, 1.0, 0.0)),
                      (1.00, (1.0, 0.0, 0.0)))

IceAndFire2 = Colormap("IceAndFire",
                      (0.00, (0., 0.0, 1.0)),
                        (0.000001, (0., 0.5, 1.0)),
                        (0.000005, (1.0, 1.0, 1.0)),
                        (0.02, (1.0, 1., 0.0)),
                        (1.00, (1.0, 0.0, 0.0)))
Ice = Colormap("Ice",
               (0.00, (0.0, 0.0, 1.0)),
               (0.50, (0.5, 0.5, 1.0)),
               (1.00, (1.0, 1.0, 1.0)))
Fire = Colormap("Fire",
                (0.00, (1.0, 1.0, 1.0)),
                (0.50, (1.0, 1.0, 0.0)),
                (1.00, (1.0, 0.0, 0.0)))
Hot = Colormap("Hot",
               (0.00, (0.0, 0.0, 0.0)),
               (0.33, (1.0, 0.0, 0.0)),
               (0.66, (1.0, 1.0, 0.0)),
               (1.00, (1.0, 1.0, 1.0)))

Grey       = Colormap("Grey", (0., (0.,0.,0.)), (1., (1.,1.,1.)))
Grey_r     = Colormap("Grey_r", (0., (1.,1.,1.)), (1., (0.,0.,0.)))
DarkRed    = Colormap("DarkRed", (0., (0.,0.,0.)), (1., (1.,0.,0.)))
DarkGreen  = Colormap("DarkGreen",(0., (0.,0.,0.)), (1., (0.,1.,0.)))
DarkBlue   = Colormap("DarkBlue", (0., (0.,0.,0.)), (1., (0.,0.,1.)))
LightRed   = Colormap("LightRed", (0., (1.,1.,1.)), (1., (1.,0.,0.)))
LightGreen = Colormap("LightGreen", (0., (1.,1.,1.)), (1., (0.,1.,0.)))
LightBlue  = Colormap("LightBlue", (0., (1.,1.,1.)), (1., (0.,0.,1.)))