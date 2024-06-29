import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging
import ast
import time
from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic

# Configuración del logging
logging.basicConfig(
    filename="logExtractor.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)


class DataBaseConnection:
    """
    Clase para gestionar las conexiones a las bases de datos MySQL y SQLite.

    Atributos:
        config (dict): Configuración para las conexiones.
        engine_mysql_bi (object): Engine de SQLAlchemy para la base de datos BI.
        engine_mysql_out (object): Engine de SQLAlchemy para la base de datos de salida.
        engine_sqlite (object): Engine de SQLAlchemy para la base de datos SQLite.
    """

    def __init__(self, config, mysql_engine=None, sqlite_engine=None):
        """
        Inicializa una instancia de DataBaseConnection.

        Args:
            config (dict): Configuración para las conexiones.
            mysql_engine (object, opcional): Engine de SQLAlchemy para MySQL, si ya existe.
            sqlite_engine (object, opcional): Engine de SQLAlchemy para SQLite, si ya existe.
        """
        self.config = config
        self.engine_mysql_bi = mysql_engine or self.create_engine_mysql("in")
        self.engine_mysql_out = mysql_engine or self.create_engine_mysql("out")
        self.engine_sqlite = sqlite_engine or create_engine("sqlite:///mydata.db")

    def create_engine_mysql(self, db_type):
        """
        Crea un engine de SQLAlchemy para una base de datos MySQL.

        Args:
            db_type (str): Tipo de base de datos ('bi' o 'out').

        Returns:
            object: Engine de SQLAlchemy para la base de datos especificada.
        """
        user, password, host, port, database = (
            self.config.get(f"nmUsr{db_type.capitalize()}"),
            self.config.get(f"txPass{db_type.capitalize()}"),
            self.config.get(f"hostServer{db_type.capitalize()}"),
            self.config.get(f"portServer{db_type.capitalize()}"),
            self.config.get(f"db{db_type.capitalize()}"),
        )
        return con.ConexionMariadb3(user, password, host, int(port), database)


class ExtraeBI:
    """
    Clase para gestionar la extracción de datos de la base de datos BI.

    Atributos:
        database_name (str): Nombre de la base de datos.
        IdtReporteIni (int): ID de inicio del reporte.
        IdtReporteFin (int): ID de fin del reporte.
        config (dict): Configuración para las conexiones.
        db_connection (DataBaseConnection): Instancia de conexión a bases de datos.
    """

    def __init__(self, database_name, IdtReporteIni, IdtReporteFin):
        """
        Inicializa una instancia de ExtraeBI.

        Args:
            database_name (str): Nombre de la base de datos.
            IdtReporteIni (int): ID de inicio del reporte.
            IdtReporteFin (int): ID de fin del reporte.
        """
        self.database_name = database_name
        self.IdtReporteIni = IdtReporteIni
        self.IdtReporteFin = IdtReporteFin
        self.configurar(database_name)

    def configurar(self, database_name):
        """
        Configura la instancia de ExtraeBI.

        Args:
            database_name (str): Nombre de la base de datos.
        """
        try:
            self.config_basic = ConfigBasic(database_name)
            self.config = self.config_basic.config
            self.db_connection = DataBaseConnection(config=self.config)
            logging.info("Configuraciones preliminares de actualización terminadas")
        except Exception as e:
            logging.error(f"Error al inicializar Actualización: {e}")
            raise

    def ejecutar_procedimiento(self, nombre_procedimiento):
        """
        Ejecuta un procedimiento almacenado especificado.

        Args:
            nombre_procedimiento (str): Nombre del procedimiento almacenado.
        """
        try:
            sql = text(
                f"SELECT * FROM powerbi_adm.conf_sql WHERE nbSql = :nombre_procedimiento"
            )
            df = self.config_basic.execute_sql_query(
                sql, {"nombre_procedimiento": nombre_procedimiento}
            )
            if df.empty:
                logging.warning(
                    f"No se encontraron resultados para nbSql = {nombre_procedimiento}"
                )
                return

            self.procesar_fila(df.iloc[0])
        except Exception as e:
            logging.error(
                f"Error al ejecutar procedimiento {nombre_procedimiento}: {e}"
            )

    def procesar_fila(self, fila):
        """
        Procesa una fila de resultados de la consulta SQL.

        Args:
            fila (pd.Series): Fila de resultados de la consulta SQL.
        """
        self.txTabla = fila["txTabla"]
        self.nmReporte = fila["nmReporte"]
        self.nmProcedure_out = fila["nmProcedure_out"]
        self.nmProcedure_in = fila["nmProcedure_in"]
        self.txSql = fila["txSql"]
        self.txSqlExtrae = fila["txSqlExtrae"]

        try:
            self.procedimiento_a_sql()
            logging.info(f"La información se generó con éxito de {self.nmReporte}")
        except Exception as e:
            logging.error(
                f"No fue posible extraer la información de {self.nmReporte} por {e}"
            )

    def procedimiento_a_sql(self):
        """
        Ejecuta el procedimiento almacenado y maneja los reintentos en caso de fallos.
        """
        for intento in range(3):
            try:
                if self.txSqlExtrae and self.txSqlExtrae != "None":
                    resultado_out = self.consulta_sql_out_extrae()
                    if resultado_out is not None and not resultado_out.empty:
                        self.consulta_sql_bi()
                        self.insertar_sql(resultado_out)
                else:
                    self.consulta_sql_bi()
                logging.info(f"Proceso completado para {self.txTabla}.")
                return
            except Exception as e:
                logging.error(
                    f"Error en procedimiento_a_sql (Intento {intento + 1}/3): {e}"
                )
                if intento >= 2:
                    logging.error(
                        "Se agotaron los intentos. No se pudo ejecutar el procedimiento."
                    )
                else:
                    logging.info(
                        f"Reintentando procedimiento (Intento {intento + 1}/3)..."
                    )
                    time.sleep(5)

    def consulta_sql_out_extrae(self):
        """
        Ejecuta una consulta SQL en la base de datos de salida con reintentos.

        Returns:
            pd.DataFrame: Resultado de la consulta SQL.
        """
        max_retries = 3
        for retry_count in range(max_retries):
            try:
                with self.db_connection.engine_mysql_out.connect() as connection:
                    cursorout = connection.execution_options(
                        isolation_level="READ COMMITTED"
                    )
                    sqlout = text(self.txSqlExtrae)
                    resultado = pd.read_sql_query(
                        sql=sqlout,
                        con=cursorout,
                        params={"fi": self.IdtReporteIni, "ff": self.IdtReporteFin},
                    )
                    return resultado
            except (
                sqlalchemy.exc.IntegrityError,
                sqlalchemy.exc.ProgrammingError,
            ) as e:
                logging.error(f"Error en consulta SQL: {e}")
                time.sleep(1)
            except Exception as e:
                logging.error(f"Error desconocido: {e}")
                time.sleep(1)
        return None

    def consulta_sql_bi(self):
        """
        Ejecuta una consulta SQL en la base de datos BI y borra los datos correspondientes.

        Returns:
            int: Número de filas borradas.
        """
        try:
            with self.db_connection.engine_mysql_bi.connect() as connection:
                trans = connection.begin()
                try:
                    sqldelete = text(self.txSql)
                    result = connection.execute(
                        sqldelete, {"fi": self.IdtReporteIni, "ff": self.IdtReporteFin}
                    )
                    rows_deleted = result.rowcount
                    trans.commit()
                    logging.info(
                        f"Datos borrados correctamente. Filas afectadas: {rows_deleted} {self.txSql}"
                    )
                    return rows_deleted
                except:
                    trans.rollback()
                    raise
        except Exception as e:
            logging.error(f"Error al borrar datos: {e}")
            raise

    def insertar_sql(self, resultado_out):
        """
        Inserta el resultado de una consulta en la base de datos BI.

        Args:
            resultado_out (pd.DataFrame): Datos a insertar en la base de datos.
        """
        with self.db_connection.engine_mysql_bi.connect() as connection:
            cursorbi = connection.execution_options(isolation_level="READ COMMITTED")
            resultado_out.to_sql(
                name=self.txTabla, con=cursorbi, if_exists="append", index=False
            )
            logging.info("Los datos se han insertado correctamente")

    def extractor(self):
        """
        Inicia el proceso de extracción de datos.

        Returns:
            dict: Resultado del proceso de extracción.
        """
        logging.info("Iniciando extractor")
        try:
            txProcedureExtrae = self.config.get("txProcedureExtrae", [])
            if isinstance(txProcedureExtrae, str):
                txProcedureExtrae = ast.literal_eval(txProcedureExtrae)
            for nombre_procedimiento in txProcedureExtrae:
                self.ejecutar_procedimiento(nombre_procedimiento)
            logging.info("Extracción completada con éxito")
            return {"success": True}
        except Exception as e:
            logging.error(f"Error general en el extractor: {e}")
            return {"success": False, "error": str(e)}
        finally:
            logging.info("Finalizado el procedimiento de ejecución SQL.")
