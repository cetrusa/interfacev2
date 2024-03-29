from scripts.extrae_bi.apipowerbi import Api_PowerBi
from scripts.StaticPage import StaticPage
import logging

# from celery import shared_task
# @shared_task

from django_rq import job


@job("default", timeout=1800)
def actualiza_bi_task(database_name, IdtReporteIni, IdtReporteFin):
    try:
        logging.info("Iniciando proceso de extracción BI")
        ApiPBi = Api_PowerBi(database_name, IdtReporteIni, IdtReporteFin)
        resultado = ApiPBi.run_datasetrefresh()
        logging.info(f"Proceso de actualización BI finalizado: {resultado}")

        if resultado.get("success"):
            return {"success": True}
        else:
            return {"success": False, "error_message": "Proceso no fue exitoso"}
    except Exception as e:
        return {"success": False, "error_message": str(e)}