from Classes.BuilderRectificationDefault import BuilderRectificationDefault


class BuilderRectificationReaddr(BuilderRectificationDefault):
    table = "READDR_RECT"

    def buildWhereConditionFromIdFile(self):
        rows = self.dbSource.returnQueryContent(f"SELECT RecInput from REAEX6 where idFile = '{self.id_file}'")
        dig = list(map(lambda l: (l[0][2: 7], l[0][7: 12], l[0][12: 36], int(l[0][36: 42])), rows))
        dag = list(filter(lambda f: f[3] == 202301, dig))  # TODO sistemare il periodo che deve essere variabile

        if len(dag) == 0:
            where_cond = " 1=0 "  # WHERE CHE MI PERMETTE DI NON ESEGUIRE LA QUERY, NON CI SONO DATI DA PRENDERE
        else:
            where_cond = " or ".join(
                list(map(lambda t: f"(banca='{t[0]}' and cod_uo='{t[1]}' and num_partita='{t[2]}' "
                                   f"and periodo_rif={t[3]})", dag)))

        return "WHERE" + where_cond, len(dag)
