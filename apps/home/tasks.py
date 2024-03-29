import os
from scripts.extrae_bi.cubo import CuboVentas
from scripts.config import ConfigBasic
from scripts.extrae_bi.cargue_zip import CargueZip
from scripts.extrae_bi.interface import InterfaceContable
from scripts.extrae_bi.plano import InterfacePlano
from scripts.extrae_bi.extrae_bi_call import Extrae_Bi
from scripts.StaticPage import StaticPage
import logging

# from celery import shared_task
# @shared_task

from django_rq import job
from scripts.extrae_bi.cargue_plano_tsol import (
    CarguePlano,
)  # Asegúrate de importar tu clase CarguePlano


@job("default", timeout=3600)
def cubo_ventas_task(database_name, IdtReporteIni, IdtReporteFin):
    try:
        logging.info("Iniciando proceso de CuboVentas")
        cubo_ventas = CuboVentas(database_name, IdtReporteIni, IdtReporteFin)
        resultado = cubo_ventas.procesar_datos()
        logging.info(f"Proceso de CuboVentas finalizado: {resultado}")

        # Asegúrate de que el resultado es un diccionario y contiene las claves esperadas
        if isinstance(resultado, dict):
            if "success" in resultado and resultado["success"]:
                if "file_path" in resultado and "file_name" in resultado:
                    return resultado
                else:
                    logging.error(
                        "El resultado de CuboVentas no incluye file_path o file_name"
                    )
            else:
                logging.error("El proceso de CuboVentas no fue exitoso")
        else:
            logging.error("El resultado de CuboVentas no tiene el formato esperado")

        # Si llega aquí, significa que algo salió mal
        return {
            "success": False,
            "error_message": "Resultado inesperado al procesar datos",
        }

    except Exception as e:
        error_msg = f"Excepción al ejecutar cubo_ventas_task: {e}"
        logging.error(error_msg)
        return {"success": False, "error_message": error_msg}


@job("default", timeout=3600)
def interface_task(database_name, IdtReporteIni, IdtReporteFin):
    try:
        logging.info("Iniciando proceso de Interface")
        interface = InterfaceContable(database_name, IdtReporteIni, IdtReporteFin)
        resultado = interface.procesar_datos()
        logging.info(f"Proceso de Interface Contable finalizado: {resultado}")

        # Asegúrate de que el resultado es un diccionario y contiene las claves esperadas
        if isinstance(resultado, dict):
            if "success" in resultado and resultado["success"]:
                if "file_path" in resultado and "file_name" in resultado:
                    return resultado
                else:
                    logging.error(
                        "El resultado de Interface Contable no incluye file_path o file_name"
                    )
            else:
                logging.error("El proceso de Interface Contable no fue exitoso")
        else:
            logging.error(
                "El resultado de Interface Contable no tiene el formato esperado"
            )

        # Si llega aquí, significa que algo salió mal
        return {
            "success": False,
            "error_message": "Resultado inesperado al procesar datos",
        }

    except Exception as e:
        error_msg = f"Excepción al ejecutar interface_task: {e}"
        logging.error(error_msg)
        return {"success": False, "error_message": error_msg}


@job("default", timeout=3600)
def cargue_zip_task(database_name, zip_file_path):
    try:
        print("aqui estoy en cargue_zip_task")
        logging.info("Iniciando proceso de Procesar ZIP")
        cargue_zip = CargueZip(database_name, zip_file_path)
        print("aqui estoy en cargue_zip_task listo para cargar el zip")
        resultado = cargue_zip.procesar_zip()
        logging.info(f"Proceso de Procesar zip finalizado: {resultado}")

        # Asegúrate de que el resultado es un diccionario y contiene las claves esperadas
        if isinstance(resultado, dict):
            if "success" in resultado and resultado["success"]:
                return resultado
            else:
                logging.error("El proceso de Procesar zip no fue exitoso")
        else:
            logging.error("El resultado de Procesar zip no tiene el formato esperado")

        # Si llega aquí, significa que algo salió mal
        return {
            "success": False,
            "error_message": "Resultado inesperado al procesar datos",
        }

    except Exception as e:
        error_msg = f"Excepción al ejecutar cargue_zip_task: {e}"
        logging.error(error_msg)
        return {"success": False, "error_message": error_msg}


@job("default", timeout=3600)
def cargue_plano_task(database_name):
    try:
        logging.info("Iniciando proceso de Cargue de Archivos Planos")
        cargue_plano = CarguePlano(database_name)
        logging.info("Procesando archivo plano")
        resultado = (
            cargue_plano.procesar_plano()
        )  # Asegúrate de que CarguePlano tenga este método

        if isinstance(resultado, dict):
            if "success" in resultado and resultado["success"]:
                return resultado
            else:
                logging.error("El proceso de cargue de archivos planos no fue exitoso")
        else:
            logging.error(
                "El resultado del cargue de archivos planos no tiene el formato esperado"
            )

        return {
            "success": False,
            "error_message": "Resultado inesperado al procesar datos",
        }

    except Exception as e:
        error_msg = f"Excepción al ejecutar cargue_plano_task: {e}"
        logging.error(error_msg)
        return {"success": False, "error_message": error_msg}


"""
@job
def cargue_zip_task(database_name, zip_file):
    print("aqui estoy en cargue_zip_task")
    try:
        cargue_zip = CargueZip(database_name)
        nit = cargue_zip.procesar_zip(zip_file)
        if nit == True:
            return {"success": True, "message": "Archivo ZIP extraído exitosamente."}
        else:
            return {
                "success": False,
                "message": "El NIT de la empresa no coincide con el nombre del archivo ZIP.",
            }

    except Exception as e:
        logging.exception("Error al ejecutar cargue_zip_task")
        raise
"""

"""
@job
def interface_task(database_name, IdtReporteIni, IdtReporteFin):
    try:
        interface_contable = Interface_Contable(
            database_name, IdtReporteIni, IdtReporteFin
        )
        interface_contable.Procedimiento_a_Excel()
        file_path = StaticPage.file_path
        file_name = StaticPage.archivo_plano
        return {"file_path": file_path, "file_name": file_name}
    except Exception as e:
        logging.exception("Error al ejecutar interface_contable_task")
        raise
"""

"""
@job
def plano_task(database_name, IdtReporteIni, IdtReporteFin):
    try:
        interface_plano = InterfaceContable(
            database_name, IdtReporteIni, IdtReporteFin
        )
        interface_plano.Procedimiento_a_Plano()
        file_path = StaticPage.file_path
        file_name = StaticPage.archivo_plano
        return {"file_path": file_path, "file_name": file_name}
    except Exception as e:
        logging.exception("Error al ejecutar interface_plano_task")
        raise
"""


@job("default", timeout=3600)
def plano_task(database_name, IdtReporteIni, IdtReporteFin):
    try:
        logging.info("Iniciando proceso de Procesar Plano")
        interface = InterfacePlano(database_name, IdtReporteIni, IdtReporteFin)
        resultado = interface.evaluar_y_procesar_datos()
        logging.info(f"Proceso de Interface Contable finalizado: {resultado}")

        # Asegúrate de que el resultado es un diccionario y contiene las claves esperadas
        if isinstance(resultado, dict):
            if "success" in resultado and resultado["success"]:
                if "file_path" in resultado and "file_name" in resultado:
                    return resultado
                else:
                    logging.error(
                        "El resultado de Procesar Plano no incluye file_path o file_name"
                    )
            else:
                logging.error("El proceso de Procesar Plano no fue exitoso")
        else:
            logging.error("El resultado de Procesar Plano no tiene el formato esperado")

        # Si llega aquí, significa que algo salió mal
        return {
            "success": False,
            "error_message": "Resultado inesperado al procesar datos",
        }

    except Exception as e:
        error_msg = f"Excepción al ejecutar plano_task: {e}"
        logging.error(error_msg)
        return {"success": False, "error_message": error_msg}


@job("default", timeout=3600)
def extrae_bi_task(database_name, IdtReporteIni, IdtReporteFin):
    try:
        logging.info("Iniciando proceso de extracción BI")
        extrae_bi = Extrae_Bi(database_name, IdtReporteIni, IdtReporteFin)
        print("listo para procesar")
        resultado = extrae_bi.extractor()
        if resultado.get("success"):
            return {"success": True}
        else:
            return {"success": False, "error_message": "Proceso no fue exitoso"}
    except Exception as e:
        return {"success": False, "error_message": str(e)}
