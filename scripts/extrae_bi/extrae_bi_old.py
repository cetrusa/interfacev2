import pandas as pd
from distutils.log import error

# import mariadb
from sqlalchemy.sql import text
from scripts.config import ConfigBasic
from scripts.StaticPage import StaticPage
import json
from django.core.exceptions import ImproperlyConfigured

####################################################################
import logging

logging.basicConfig(
    filename="log.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)
####################################################################
logging.info("Inciando Proceso")


class Extrae_Bi:
    def __init__(self, database_name):
        ConfigBasic(database_name)

    def extractor(self):
        ItpReporte = StaticPage.txProcedureExtrae
        for a in ItpReporte:
            with StaticPage.conin.connect() as connectionin:
                sql = text(f"SELECT * FROM powerbi_adm.conf_sql WHERE nbSql = {a}")
                result = connectionin.execute(sql)
                df9 = pd.DataFrame(result)
                StaticPage.txTabla = str(df9["txTabla"].values[0])
                StaticPage.nmReporte = str(df9["nmReporte"].values[0])
                StaticPage.nmProcedure_out = str(df9["nmProcedure_out"].values[0])
                StaticPage.nmProcedure_in = str(df9["nmProcedure_in"].values[0])
                StaticPage.txSql = str(df9["txSql"].values[0])
                logging.info(f"Se va a procesar {StaticPage.nmReporte}")
                try:
                    self.procedimiento_a_sql(
                        IdtReporteIni=StaticPage.IdtReporteIni,
                        IdtReporteFin=StaticPage.IdtReporteFin,
                        nmReporte=StaticPage.nmReporte,
                        nmProcedure_in=StaticPage.nmProcedure_in,
                        nmProcedure_out=StaticPage.nmProcedure_out,
                        txTabla=StaticPage.txTabla,
                    )
                    logging.info(
                        f"La información se generó con éxito de {StaticPage.nmReporte}"
                    )
                except Exception as e:
                    logging.info(
                        f"No fue posible extraer la información de {StaticPage.nmReporte} por {e}"
                    )

    def insertar_sql(self, resultado_out, txTabla):
        with StaticPage.conin3.connect() as connectionin:
            cursorbi = connectionin.execution_options(isolation_level="READ COMMITTED")
            resultado_out.to_sql(
                name=txTabla,
                con=cursorbi,
                if_exists="append",
                index=False,
                index_label=None,
            )
            # logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
            return logging.info("los datos se han insertado correctamente")

    def consulta_sql_out(
        self, nmProcedure_out, IdtReporteIni, IdtReporteFin, nmReporte
    ):
        with StaticPage.conout.connect() as connectionout:
            cursorout = connectionout.execution_options(
                isolation_level="READ COMMITTED"
            )
            sqlout = text(
                f"CALL {str(nmProcedure_out)}('{str(IdtReporteIni)}','{str(IdtReporteFin)}','{str(nmReporte)}');"
            )
            resultado = pd.read_sql_query(sql=sqlout, con=cursorout)
            return resultado

    def consulta_sql_bi(self, IdtReporteIni, IdtReporteFin):
        with StaticPage.conin.connect() as connectionin:
            cursorbi = connectionin
            sqldelete = text(StaticPage.txSql)
            cursorbi.execute(sqldelete, {"fi": IdtReporteIni, "ff": IdtReporteFin})
        return logging.info("Datos fueron borrados")

    def procedimiento_a_sql(
        self,
        IdtReporteIni,
        IdtReporteFin,
        nmReporte,
        nmProcedure_in,
        nmProcedure_out,
        txTabla,
    ):
        # Iniciamos las conexiones
        EsVacio = True
        if (
            nmReporte == "update_cubo_bi"
            or nmReporte == "borra_impactos_bi"
            or nmReporte == "impactos_bi"
        ):
            try:
                self.consulta_sql_bi(IdtReporteIni, IdtReporteFin)
                logging.info(f"Se ha realizado el proceso de {txTabla}")
                logging.info(
                    f"Se han insertado los datos {txTabla}, entre {IdtReporteIni} y {IdtReporteFin}"
                )
                EsVacio = True
            except Exception as e:
                logging.error(e)
        else:
            StaticPage.resultado_out = self.consulta_sql_out(
                nmProcedure_out=nmProcedure_out,
                IdtReporteIni=IdtReporteIni,
                IdtReporteFin=IdtReporteFin,
                nmReporte=nmReporte,
            )
            logging.info(f"{nmReporte} contiene {len(StaticPage.resultado_out.index)}")
            EsVacio = False
            # self.consulta_sql_bi(nmProcedure_in=nmProcedure_in,IdtReporteIni=IdtReporteIni,IdtReporteFin=IdtReporteFin,nmReporte=nmReporte)
            logging.info(f"el procemiento {txTabla} si funciono")

        # Evaluamos si esta vacio el dataFrame
        if EsVacio == False and StaticPage.resultado_out.empty == False:
            logging.info(f"Si hay datos para {txTabla} ")
            self.consulta_sql_bi(
                IdtReporteIni=IdtReporteIni, IdtReporteFin=IdtReporteFin
            )
            logging.info(f"Se ha realizado el proceso de {txTabla}")
            # resultado_out.to_csv(nmReporte.lower()+'.txt',sep='|',index=False,header=False,float_format='%.0f')
            self.insertar_sql(txTabla=txTabla, resultado_out=StaticPage.resultado_out)
            logging.info(
                f"Se han insertado los datos {txTabla}, entre {IdtReporteIni} y {IdtReporteFin}"
            )
        else:
            logging.info(f"No hay datos para {txTabla} ")

        logging.info("Se ejecutó el procedimiento")
