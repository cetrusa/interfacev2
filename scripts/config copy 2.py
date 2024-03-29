from scripts.StaticPage import StaticPage, DinamicPage
from scripts.conexion import Conexion as con
import json
from django.core.exceptions import ImproperlyConfigured
import pandas as pd
import ast
from sqlalchemy.sql import text
import logging

logging.basicConfig(
    filename="log.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)
logging.info("Iniciando Proceso")


def get_secret(secret_name, secrets_file="secret.json"):
    try:
        with open(secrets_file) as f:
            secrets = json.loads(f.read())
        return secrets[secret_name]
    except KeyError:
        raise ImproperlyConfigured(f"La variable {secret_name} no existe")
    except FileNotFoundError:
        raise ImproperlyConfigured(
            f"No se encontró el archivo de configuración {secrets_file}"
        )


class ConfigBasic:
    def __init__(self, database_name):
        try:
            self.setup_static_page(database_name)
        except json.JSONDecodeError as e:
            logging.error(f"Error al decodificar JSON: {e}")
        except KeyError as e:
            logging.error(f"Clave no encontrada en el archivo de configuración: {e}")
        except FileNotFoundError as e:
            logging.error(f"Archivo no encontrado: {e}")
        except pd.errors.EmptyDataError as e:
            logging.error(f"No se encontraron datos en la consulta SQL: {e}")
        except IndexError as e:
            logging.error(f"Índice fuera de rango en el DataFrame: {e}")
        except Exception as e:
            logging.error(f"Error desconocido en ConfigBasic: {e}")

    def setup_static_page(self, database_name):
        StaticPage.name = str(database_name)
        StaticPage.dir_actual = str("puente1dia")
        StaticPage.nmDt = StaticPage.dir_actual
        logging.info(f"Configurando para la base de datos: {StaticPage.name}")

        StaticPage.con = con.ConexionMariadb3(
            get_secret("DB_USERNAME"),
            get_secret("DB_PASS"),
            get_secret("DB_HOST"),
            int(get_secret("DB_PORT")),
            get_secret("DB_NAME"),
        )

        self.fetch_database_config()
        self.setup_date_config()
        self.setup_server_config()
        self.setup_connections()

    def execute_sql_query(self, sql_query):
        try:
            with StaticPage.con.connect() as connection:
                result = pd.read_sql_query(sql=sql_query, con=connection)
                if result.empty:
                    logging.warning(f"Consulta SQL no devolvió datos: {sql_query}")
                return result
        except Exception as e:
            logging.error(f"Error al ejecutar consulta SQL: {sql_query}, Error: {e}")
            return pd.DataFrame()

    def fetch_database_config(self):
        sql = (
            f"SELECT * FROM powerbi_adm.conf_empresas WHERE name = '{StaticPage.name}';"
        )
        df = self.execute_sql_query(sql)
        if not df.empty:
            self.assign_static_page_attributes(df)

    def assign_static_page_attributes(self, df):
        for field in [
            "id",
            "nmEmpresa",
            "name",
            "nbServerSidis",
            "dbSidis",
            "nbServerBi",
            "dbBi",
            "txProcedureExtrae",
            "txProcedureCargue",
            "nmProcedureExcel",
            "txProcedureExcel",
            "nmProcedureInterface",
            "txProcedureInterface",
            "nmProcedureExcel2",
            "txProcedureExcel2",
            "nmProcedureCsv",
            "txProcedureCsv",
            "nmProcedureCsv2",
            "txProcedureCsv2",
            "nmProcedureSql",
            "txProcedureSql",
            "report_id_powerbi",
            "dataset_id_powerbi",
            "url_powerbi",
            "id_tsol",
        ]:
            if field in df:
                value = df[field].values[0] if not df[field].empty else None
                setattr(StaticPage, field, value)
                print(
                    f"{field}: {getattr(StaticPage, field)}"
                )  # Imprimir el valor asignado
            else:
                logging.warning(
                    f"Campo {field} no encontrado en los resultados para {StaticPage.name}"
                )

    def setup_date_config(self):
        date_config = self.fetch_date_config(StaticPage.nmDt)
        if not date_config:
            date_config = self.fetch_date_config("puente1dia")  # Valor por defecto

        for key, value in date_config.items():
            setattr(StaticPage, key, value)

    def fetch_date_config(self, nmDt):
        sql = f"SELECT * FROM powerbi_adm.conf_dt WHERE nmDt = '{nmDt}';"
        df = self.execute_sql_query(sql)
        if not df.empty:
            return {
                "txDtIni": df["txDtIni"].values[0],
                "txDtFin": df["txDtFin"].values[0],
                "IdtReporteIni": self.execute_sql_query(df["txDtIni"].values[0])[
                    "IdtReporteIni"
                ].values[0],
                "IdtReporteFin": self.execute_sql_query(df["txDtFin"].values[0])[
                    "IdtReporteFin"
                ].values[0],
            }
        return None

    def setup_server_config(self):
        self.assign_server_config(StaticPage.nbServerSidis, "Out")
        self.assign_server_config(StaticPage.nbServerBi, "In")

    def assign_server_config(self, server_name, suffix):
        server_sql = (
            f"SELECT * FROM powerbi_adm.conf_server WHERE nbServer = '{server_name}';"
        )
        tipo_sql = f"SELECT * FROM powerbi_adm.conf_tipo WHERE nbTipo = '{{nbTipo}}';"

        server_df = self.execute_sql_query(server_sql)
        if not server_df.empty:
            tipo_df = self.execute_sql_query(
                tipo_sql.replace("{{nbTipo}}", server_df["nbTipo"].values[0])
            )
            if not tipo_df.empty:
                setattr(
                    StaticPage, f"hostServer{suffix}", server_df["hostServer"].values[0]
                )
                setattr(
                    StaticPage, f"portServer{suffix}", server_df["portServer"].values[0]
                )
                setattr(StaticPage, f"nmUsr{suffix}", tipo_df["nmUsr"].values[0])
                setattr(StaticPage, f"txPass{suffix}", tipo_df["txPass"].values[0])

    def setup_connections(self):
        setattr(
            StaticPage,
            "conin",
            con.ConexionMariadb3(
                user=getattr(StaticPage.nmUsrIn),
                password=getattr(StaticPage.txPassIn),
                host=getattr(StaticPage.hostServerIn),
                port=getattr(StaticPage.portServerIn),
                database=getattr(StaticPage.dbBi),
            ),
        )
        setattr(
            StaticPage,
            "conin2",
            con.ConexionMariadb3(
                user=getattr(StaticPage.nmUsrIn),
                password=getattr(StaticPage.txPassIn),
                host=getattr(StaticPage.hostServerIn),
                port=getattr(StaticPage.portServerIn),
                database=getattr(StaticPage.dbBi),
            ),
        )
        setattr(
            StaticPage,
            "conout",
            con.ConexionMariadb3(
                user=getattr(StaticPage.nmUsrOut),
                password=getattr(StaticPage.txPassOut),
                host=getattr(StaticPage.hostServerOut),
                port=getattr(StaticPage.portServerOut),
                database=getattr(StaticPage.dbSidis),
            ),
        )
        setattr(
            StaticPage,
            "conin3",
            con.ConexionMariadb3(
                user=getattr(StaticPage.nmUsrIn),
                password=getattr(StaticPage.txPassIn),
                host=getattr(StaticPage.hostServerIn),
                port=getattr(StaticPage.portServerIn),
                database=getattr(StaticPage.dbBi),
            ),
        )


# Uso del código
# try:
#     config = ConfigBasic("nombre_base_datos")
# except ImproperlyConfigured as e:
#     logging.error(f"Error de configuración: {e}")
