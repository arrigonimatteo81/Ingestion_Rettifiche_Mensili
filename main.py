import json
import sys
from types import SimpleNamespace

from Classes.BuilderRectificationDefault import BuilderRectificationDefault
from Classes.BuilderRectificationReaatn import BuilderRectificationReaatn
from Classes.BuilderRectificationReaatr import BuilderRectificationReaatr
from Classes.BuilderRectificationReaddr import BuilderRectificationReaddr
from Classes.BuilderRectificationReagdg import BuilderRectificationReagdg
from Etl.EtlRequest import EtlRequest
from Process.SubProcessLogMensile import SubProcessLogMensile
from Registro.RegistroMensile import RegistroMensile
from constants import TAB_REAGDG, TAB_READDR, TAB_REAATN, TAB_REAATR
from utils import configure_log


def switch_classes(request: EtlRequest) -> BuilderRectificationDefault:
    if request.semaforo.tabella == TAB_REAGDG:
        return BuilderRectificationReagdg(request)
    if request.semaforo.tabella == TAB_READDR:
        return BuilderRectificationReaddr(request)
    if request.semaforo.tabella == TAB_REAATN:
        return BuilderRectificationReaatn(request)
    if request.semaforo.tabella == TAB_REAATR:
        return BuilderRectificationReaatr(request)


if __name__ == '__main__':
    jsonEtlRequest = sys.argv[1]
    req = json.loads(jsonEtlRequest, object_hook=lambda d: SimpleNamespace(**d))

    subprocessLog = SubProcessLogMensile()

    if not subprocessLog.SubprocessIsRunning(req):
        subprocessLog.InitSubprocessLog(req)
        configure_log()

        cls = switch_classes(req)
        response = cls.ingest()

        if response.status == 'OK':
            subprocessLog.CloseSubprocessLogOK(response)
            registro = RegistroMensile
            registro.upsertRegistro(provenienza=req.semaforo.provenienza, tipo_caricamento=req.semaforo.tipoCaricamento,
                                    banca=req.semaforo.abi, tabella=req.semaforo.tabella, semaforo_id=req.semaforo.id)
            subprocessLog.InitSubrocessSilver(req, response)
        else:
            subprocessLog.CloseSubprocessLogKO(response)
