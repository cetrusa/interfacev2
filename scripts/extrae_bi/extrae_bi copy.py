import os
import pandas as pd
from sqlalchemy import create_engine
import logging
from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
import ast
import json
import ast
import sqlalchemy
import time
from sqlalchemy import text

# Configuración del logging
logging.basicConfig(
    filename="logExtractor.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)


class DataBaseConnection:
    def __init__(self, config, mysql_engine=None, sqlite_engine=None):
        self.config = config
        # Asegurarse de que los engines son instancias de conexión válidas y no cadenas
        self.engine_mysql_bi = (
            mysql_engine if mysql_engine else self.create_engine_mysql_bi()
        )
        self.engine_mysql_out = (
            mysql_engine if mysql_engine else self.create_engine_mysql_out()
        )
        self.engine_sqlite = (
            sqlite_engine if sqlite_engine else create_engine("sqlite:///mydata.db")
        )
        # print(self.engine_sqlite)

    def create_engine_mysql_bi(self):
        # Simplificación en la obtención de los parámetros de configuración
        user, password, host, port, database = (
            self.config.get("nmUsrIn"),
            self.config.get("txPassIn"),
            self.config.get("hostServerIn"),
            self.config.get("portServerIn"),
            self.config.get("dbBi"),
        )
        return con.ConexionMariadb3(
            str(user), str(password), str(host), int(port), str(database)
        )

    def create_engine_mysql_out(self):
        # Simplificación en la obtención de los parámetros de configuración
        user, password, host, port, database = (
            self.config.get("nmUsrOut"),
            self.config.get("txPassOut"),
            self.config.get("hostServerOut"),
            self.config.get("portServerOut"),
            self.config.get("dbSidis"),
        )
        return con.ConexionMariadb3(
            str(user), str(password), str(host), int(port), str(database)
        )


class Extrae_Bi:
    def __init__(self, database_name, IdtReporteIni, IdtReporteFin):
        self.database_name = database_name
        self.IdtReporteIni = IdtReporteIni
        self.IdtReporteFin = IdtReporteFin
        self.configurar(database_name)

    def configurar(self, database_name):
        try:
            config_basic = ConfigBasic(database_name)
            self.config = config_basic.config
            config_basic.print_configuration()
            # print(self.config.get("txProcedureExtrae", []))
            self.db_connection = DataBaseConnection(config=self.config)
            self.engine_sqlite = self.db_connection.engine_sqlite
            self.engine_mysql_bi = self.db_connection.engine_mysql_bi
            self.engine_mysql_out = self.db_connection.engine_mysql_out
            print("Configuraciones preliminares de actualización terminadas")
        except Exception as e:
            logging.error(f"Error al inicializar Actualización: {e}")
            raise

    def extractor(self):
        print("Iniciando extractor")
        try:
            txProcedureExtrae = self.config.get("txProcedureExtrae", [])
            # print("txProcedureExtrae:", txProcedureExtrae)
            if isinstance(txProcedureExtrae, str):
                txProcedureExtrae = ast.literal_eval(txProcedureExtrae)
            # print("Tipo de txProcedureExtrae:", type(txProcedureExtrae))  # Esto debería mostrarte <class 'list'>
            for a in txProcedureExtrae:
                print("Procesando:", a)
                with self.engine_mysql_bi.connect() as connectionin:
                    sql = text("SELECT * FROM powerbi_adm.conf_sql WHERE nbSql = :a")
                    result = connectionin.execute(sql, {"a": a})
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())

                    if not df.empty:
                        self.txTabla = str(df["txTabla"].values[0])
                        self.nmReporte = str(df["nmReporte"].values[0])
                        self.nmProcedure_out = str(df["nmProcedure_out"].values[0])
                        self.nmProcedure_in = str(df["nmProcedure_in"].values[0])
                        self.txSql = str(df["txSql"].values[0])
                        self.txSqlExtrae = str(df["txSqlExtrae"].values[0])
                        # print(self.txTabla)
                        # print(self.nmReporte)
                        # print(self.nmProcedure_out)
                        # print(self.nmProcedure_in)
                        # print(self.txSql)
                        # print(self.txSqlExtrae)

                        logging.info(f"Se va a procesar {self.nmReporte}")

                        try:
                            self.procedimiento_a_sql()
                            logging.info(
                                f"La información se generó con éxito de {self.nmReporte}"
                            )
                        except Exception as e:
                            logging.info(
                                f"No fue posible extraer la información de {self.nmReporte} por {e}"
                            )
                            print(
                                f"Error al ejecutar procedimiento_a_sql para {self.nmReporte}: {e}"
                            )
                    else:
                        logging.warning(
                            f"No se encontraron resultados para nbSql = {a}"
                        )
                        print(f"No se encontraron resultados para nbSql = {a}")
            print("Extracción completada con éxito")
            return {"success": True}
        except Exception as e:
            print(f"Error general en el extractor: {e}")
            return {"success": False, "error": str(e)}
        except Exception as e:
            logging.error(
                f"Error durante la ejecución de la lista de procedimientos con nbSql = {a}: {e}"
            )
            return {"success": False, "error": str(e)}
        finally:
            logging.info("Finalizado el procedimiento de ejecución SQL.")

    def insertar_sql(self, resultado_out):
        with self.engine_mysql_bi.connect() as connectionin:
            cursorbi = connectionin.execution_options(isolation_level="READ COMMITTED")
            resultado_out.to_sql(
                name=self.txTabla,
                con=cursorbi,
                if_exists="append",
                index=False,
                index_label=None,
            )
            # logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
            return logging.info("los datos se han insertado correctamente")

    def consulta_sql_out_extrae(self):
        max_retries = 3  # Define aquí el número máximo de reintentos
        retry_count = 0

        while retry_count < max_retries:
            try:
                with self.engine_mysql_out.connect() as connection:
                    cursorout = connection.execution_options(
                        isolation_level="READ COMMITTED"
                    )
                    # Preparar el llamado al procedimiento almacenado de manera segura
                    sqlout = text(self.txSqlExtrae)
                    # print(sqlout)
                    # Ejecutar el procedimiento almacenado pasando los parámetros de forma segura
                    resultado = pd.read_sql_query(
                        sql=sqlout,
                        con=cursorout,
                        params={"fi": self.IdtReporteIni, "ff": self.IdtReporteFin},
                    )
                    return resultado

            except sqlalchemy.exc.IntegrityError as e:
                logging.error(f"Error de integridad: {e}")
                retry_count += 1
                time.sleep(1)  # Espera antes de reintentar

            except sqlalchemy.exc.ProgrammingError as e:
                logging.error(f"Error de programación: {e}")
                retry_count += 1
                time.sleep(1)  # Espera antes de reintentar

            except Exception as e:
                logging.error(f"Error desconocido: {e}")
                retry_count += 1
                time.sleep(1)  # Espera antes de reintentar

        # Si se alcanza este punto, se agotaron los reintentos sin éxito
        return None
    
    def consulta_sql_bi(self):
        try:
            # Establecer conexión con la base de datos BI
            with self.engine_mysql_bi.connect() as connection:
                # Iniciar una transacción manualmente
                trans = connection.begin()
                
                try:
                    # Preparar y ejecutar la consulta SQL
                    print(f"Revisemos la consulta: {self.txSql}")
                    sqldelete = text(self.txSql)
                    result = connection.execute(
                        sqldelete, {"fi": self.IdtReporteIni, "ff": self.IdtReporteFin}
                    )
                    
                    # Obtener el número de filas afectadas y hacer commit manualmente
                    rows_deleted = result.rowcount
                    trans.commit()  # Confirmar los cambios explícitamente
                    
                    # Registrar el éxito de la operación
                    logging.info(f"Datos borrados correctamente. Filas afectadas: {rows_deleted} {self.txSql}")
                    print(f"Datos borrados correctamente. Filas afectadas: {rows_deleted} {self.txSql}")
                    
                    # Devolver el número de filas borradas
                    return rows_deleted
                    
                except:
                    # En caso de error, hacer rollback de la transacción
                    trans.rollback()
                    raise
                
        except Exception as e:
            # Registrar y manejar cualquier excepción
            logging.error(f"Error al borrar datos: {e}")
            raise

    # def consulta_sql_bi(self):
    #     try:
    #         # Establecer conexión con la base de datos BI
    #         with self.engine_mysql_bi.connect() as connection:
    #             # Preparar la consulta SQL con parámetros de seguridad
    #             print(f"Revisemos la consulta: {self.txSql}")
    #             sqldelete = text(self.txSql)
                
    #             # Ejecutar la consulta con los parámetros proporcionados
    #             result = connection.execute(
    #                 sqldelete, {"fi": self.IdtReporteIni, "ff": self.IdtReporteFin}
    #             )
                
    #             # Obtener el número de filas afectadas
    #             rows_deleted = result.rowcount
                
    #             # Registrar el éxito de la operación incluyendo el número de filas borradas
    #             logging.info(
    #                 f"Datos borrados correctamente entre las fechas proporcionadas. Filas afectadas: {rows_deleted} {self.txSql}"
    #             )
    #             print(
    #                 f"Datos borrados correctamente entre las fechas proporcionadas. Filas afectadas: {rows_deleted} {self.txSql}"
    #             )
                
    #             # Devolver el número de filas borradas
    #             return rows_deleted
                
    #     except Exception as e:
    #         # Registrar cualquier excepción ocurrida durante la ejecución
    #         logging.error(f"Error al borrar datos: {e}")
    #         # Podría ser adecuado manejar la excepción de forma más específica o re-lanzarla
    #         raise

    def procedimiento_a_sql(self):
        for intento in range(3):  # Intentar la conexión hasta tres veces
            try:
                # Verificar directamente si txSqlExtrae tiene un valor adecuado y no es "None"
                if self.txSqlExtrae and self.txSqlExtrae != "None":
                    # print(f"Dentro del if, self.txSqlExtrae: {self.txSqlExtrae}")
                    # print(type(self.txSqlExtrae))
                    print("estamos ingresando a buscar un resultado")
                    resultado_out = self.consulta_sql_out_extrae()
                    if resultado_out is not None and not resultado_out.empty:
                        self.consulta_sql_bi()
                        self.insertar_sql(resultado_out=resultado_out)
                else:
                    # Si txSqlExtrae está vacío o es "None", ejecutar consulta_sql_bi directamente
                    self.consulta_sql_bi()

                logging.info(f"Proceso completado para {self.txTabla}.")
                return  # Sale del bucle después de completar el proceso con éxito

            except Exception as e:
                logging.error(
                    f"Error en procedimiento_a_sql (Intento {intento + 1}/3): {e}"
                )
                if intento >= 2:
                    logging.error(
                        "Se agotaron los intentos. No se pudo ejecutar el procedimiento."
                    )
                    break  # Sale del bucle si se alcanza el máximo de intentos
                else:
                    logging.info(
                        f"Reintentando procedimiento (Intento {intento + 1}/3)..."
                    )
                    time.sleep(5)  # Espera antes de reintentar

    # def procedimiento_a_sql(self):
    #     for intento in range(3):  # Intentar la conexión hasta tres veces
    #         try:
    #             if not self.config.get("txSqlExtrae"):
    #                 # Si txSqlExtrae no tiene valor, solo ejecuta consulta_sql_bi
    #                 self.consulta_sql_bi()
    #             else:
    #                 # Si txSqlExtrae tiene algún valor, primero ejecuta consulta_sql_out_extrae
    #                 resultado_out = self.consulta_sql_out_extrae()
    #                 if resultado_out is not None and not resultado_out.empty:
    #                     # Solo si hay resultados, ejecuta consulta_sql_bi e insertar_sql
    #                     self.consulta_sql_bi()
    #                     self.insertar_sql(resultado_out=resultado_out)
    #                 else:
    #                     # Si resultado_out es None o está vacío, se podría registrar este caso
    #                     logging.info("resultado_out está vacío o es None.")

    #             logging.info(f"Proceso completado para {self.config.get('txTabla')}.")
    #             return  # Sale del bucle después de completar el proceso con éxito

    #         except Exception as e:
    #             logging.error(f"Error en procedimiento_a_sql (Intento {intento + 1}/3): {e}")
    #             if intento >= 2:  # Si se agotaron los intentos
    #                 logging.error("Se agotaron los intentos. No se pudo ejecutar el procedimiento.")
    #                 break  # Sale del bucle si se alcanza el máximo de intentos
    #             else:
    #                 logging.info(f"Reintentando procedimiento (Intento {intento + 1}/3)...")
    #                 time.sleep(5)  # Esperar 5 segundos antes de reintentar
