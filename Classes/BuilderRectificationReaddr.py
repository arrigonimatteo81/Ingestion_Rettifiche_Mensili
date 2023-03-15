from typing import Any

from Classes.BuilderRectificationDefault import BuilderRectificationDefault
from Etl.EtlRequest import EtlRequest
from dateutil.relativedelta import relativedelta
from datetime import datetime

from constants import TAB_READDR_RECT


def scomponiRiga(riga: tuple[Any]):
    return riga[0][2: 7], riga[0][7: 12], riga[0][12: 36], int(riga[0][36: 42])


def componiStringa(riga: tuple[Any, Any, Any, int]):
    return f"(readdr.banca='{riga[0]}' and readdr.cod_uo='{riga[1]}' and readdr.num_partita='{riga[2]}' " \
           f"and readdr.periodo_rif={riga[3]})"


class BuilderRectificationReaddr(BuilderRectificationDefault):
    table = TAB_READDR_RECT

    def __init__(self, etl_request: EtlRequest):
        super().__init__(etl_request)
        past_date = datetime.today() - relativedelta(months=1)
        self.periodo_rif = int(past_date.strftime('%Y%m'))

    def buildWhereConditionFromIdFile(self) -> tuple[str, int]:
        rows = self.dbSource.returnQueryContent(f"SELECT RecInput from REAEX6 where idFile = '{self.id_file}'")
        dig = list(map(lambda l: scomponiRiga(l), rows))
        dag = list(filter(lambda f: f[3] == self.periodo_rif, dig))
        return "WHERE" + " or ".join(list(map(lambda t: componiStringa(t), dag))), len(dag)
