from Classes.BuilderRectificationDefault import BuilderRectificationDefault


class BuilderRectificationReagdg(BuilderRectificationDefault):
    table = "REAGDG_RECT"

    def getCount(self) -> int:
        return self.count_key

    def buildWhereConditionFromIdFile(self):
        rows = self.dbSource.returnQueryContent(f"SELECT RecInput from REAEX6 where idFile = '{self.id_file}'")
        dig = list(map(lambda l: (l[0][2: 7], l[0][7: 23], int(l[0][23: 29])), rows))
        dag = list(filter(lambda f: f[2] == 202301, dig)) #TODO sistemare il periodo che deve essere variabile
        self.count_key = len(dag)
        return list(map(lambda t: f"(banca='{t[0]}' and ndg='{t[1]}' and periodo_rif={t[2]})", dag))

