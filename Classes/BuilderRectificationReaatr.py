from Classes.BuilderRectificationReaddr import BuilderRectificationReaddr
from constants import TAB_REAATR_RECT


class BuilderRectificationReaatr(BuilderRectificationReaddr):
    table = TAB_REAATR_RECT


##TODO La count degli elementi di questa classe va fatta lanciando una query di count sul sistema sorgente con la where
##identica a quella creata da BuilderRectificationReaddr. In questo momento viene utilizzata la count ritornata da
##BuilderRectificationReaddr, ma è la count della tab principale, non di questa.