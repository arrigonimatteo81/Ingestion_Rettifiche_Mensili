from Classes.BuilderRectificationReagdg import BuilderRectificationReagdg


class BuilderRectificationReaatn(BuilderRectificationReagdg):
    table = "REAATN_RECT"

##TODO La count degli elementi di questa classe va fatta lanciando una query di count sul sistema sorgente con la where
##identica a quella creata da BuilderRectificationReagdg. In questo momento viene utilizzata la count ritornata da
##BuilderRectificationReagdg, ma è la count della tab principale, non di questa.