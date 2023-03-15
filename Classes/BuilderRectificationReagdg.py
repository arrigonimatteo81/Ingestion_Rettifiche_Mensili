from typing import Any

from dateutil.relativedelta import relativedelta
from datetime import datetime
from Classes.BuilderRectificationDefault import BuilderRectificationDefault
from Etl.EtlRequest import EtlRequest
from constants import TAB_REAGDG_RECT


def scomponiRiga(riga: tuple[Any]):
    return riga[0][2: 7], riga[0][7: 23], int(riga[0][23: 29])


def componiStringa(riga: tuple[Any, Any, int]):
    return f"(reagdg.banca='{riga[0]}' and reagdg.ndg='{riga[1]}' and reagdg.periodo_rif={riga[2]})"


class BuilderRectificationReagdg(BuilderRectificationDefault):
    table = TAB_REAGDG_RECT

    def __init__(self, etl_request: EtlRequest):
        super().__init__(etl_request)
        past_date = datetime.today() - relativedelta(months=1)
        self.periodo_rif = int(past_date.strftime('%Y%m'))

    def buildWhereConditionFromIdFile(self) -> tuple[str, int]:
        rows = self.dbSource.returnQueryContent(f"SELECT RecInput from REAEX6 where idFile = '{self.id_file}'")
        dig = list(map(lambda l: scomponiRiga(l), rows))
        dag = list(filter(lambda f: f[2] == self.periodo_rif, dig))
        return "WHERE" + " or ".join(list(map(lambda t: componiStringa(t), dag))), len(dag)
