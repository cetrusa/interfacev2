import os, sys
import pandas as pd
from os import path, system
from time import time
from distutils.log import error
from sqlalchemy import create_engine, text
import sqlalchemy
import pymysql
import csv
import zipfile
from zipfile import ZipFile
from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
import json
import msal
import requests
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE
import ast

with open("secret.json") as f:
    secret = json.loads(f.read())

    def get_secret(secret_name, secrets_file="secret.json"):
        try:
            with open(secrets_file) as f:
                secrets = json.loads(f.read())
            return secrets[secret_name]
        except KeyError:
            raise ValueError(f"La variable {secret_name} no existe.")
        except FileNotFoundError:
            raise ValueError(
                f"No se encontró el archivo de configuración {secrets_file}."
            )


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

    def execute_query_mysql(self, query, chunksize=None):
        # Usar el engine correctamente
        with self.create_engine_mysql.connect() as connection:
            cursor = connection.execution_options(isolation_level="READ COMMITTED")
            return pd.read_sql_query(query, cursor, chunksize=chunksize)

    def execute_sql_sqlite(self, sql, params=None):
        # Usar el engine correctamente
        with self.engine_sqlite.connect() as connection:
            return connection.execute(sql, params)

    def execute_query_mysql_chunked(self, query, table_name, chunksize=50000):
        try:
            # Primero, elimina la tabla si ya existe
            self.eliminar_tabla_sqlite(table_name)
            # Luego, realiza la consulta y almacena los resultados en SQLite
            with self.engine_mysql.connect() as connection:
                cursor = connection.execution_options(isolation_level="READ COMMITTED")

                for chunk in pd.read_sql_query(query, con=cursor, chunksize=chunksize):
                    chunk.to_sql(
                        name=table_name,
                        con=self.engine_sqlite,
                        if_exists="append",
                        index=False,
                    )

                # Retorna el total de registros almacenados en la tabla SQLite
                with self.engine_sqlite.connect() as connection:
                    # Modificar aquí para usar fetchone correctamente
                    total_records = connection.execute(
                        text(f"SELECT COUNT(*) FROM {table_name}")
                    ).fetchone()[0]
                return total_records

        except Exception as e:
            logging.error(f"Error al ejecutar el query: {e}")
            print(f"Error al ejecutar el query: {e}")
            raise
        print("terminamos de ejecutar el query")

    def eliminar_tabla_sqlite(self, table_name):
        sql = text(f"DROP TABLE IF EXISTS {table_name}")
        with self.engine_sqlite.connect() as connection:
            connection.execute(sql)


class Api_PowerBi:
    def __init__(self, database_name, IdtReporteIni, IdtReporteFin):
        self.database_name = database_name
        self.IdtReporteIni = IdtReporteIni
        self.IdtReporteFin = IdtReporteFin
        self.configurar(database_name)

    def configurar(self, database_name):
        try:
            config_basic = ConfigBasic(database_name)
            self.config = config_basic.config
            # config_basic.print_configuration()
            # print(self.config.get("txProcedureExtrae", []))
            self.db_connection = DataBaseConnection(config=self.config)
            self.engine_sqlite = self.db_connection.engine_sqlite
            self.engine_mysql_bi = self.db_connection.engine_mysql_bi
            self.engine_mysql_out = self.db_connection.engine_mysql_out
            print("Configuraciones preliminares de actualización terminadas")
        except Exception as e:
            logging.error(f"Error al inicializar Actualización: {e}")
            raise

    def request_access_token_refresh(self):
        app_id = get_secret("CLIENT_ID")
        # print(app_id)
        tenant_id = get_secret("TENANT_ID")
        # print(tenant_id)
        authority_url = "https://login.microsoftonline.com/" + tenant_id
        scopes = ["https://analysis.windows.net/powerbi/api/.default"]

        username = self.config.get("nmUsrPowerbi")
        # print(username)
        password = self.config.get("txPassPowerbi")
        # print(password)
        # Step 1. Generate Power BI Access Token
        client = msal.PublicClientApplication(app_id, authority=authority_url)
        token_response = client.acquire_token_by_username_password(
            username=self.config.get("nmUsrPowerbi"),
            password=self.config.get("txPassPowerbi"),
            scopes=scopes,
        )
        if not "access_token" in token_response:
            raise Exception(token_response["error_description"])

        access_id = token_response.get("access_token")
        return access_id

    def run_datasetrefresh_solo_inicio(self):
        access_id = self.request_access_token_refresh()

        dataset_id = self.config.get("dataset_id_powerbi")
        endpoint = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes"
        headers = {"Authorization": f"Bearer " + access_id}

        response = requests.post(endpoint, headers=headers)
        if response.status_code == 202:
            print("Dataset refreshed")
        else:
            print(response.reason)
            print(response.json())

    def run_datasetrefresh(self):
        print("Iniciando refresh")
        access_id = self.request_access_token_refresh()
        dataset_id = self.config.get("dataset_id_powerbi")
        endpoint = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes"
        headers = {"Authorization": f"Bearer " + access_id}

        response = requests.post(endpoint, headers=headers)
        print(response)
        if response.status_code == 202:
            print("Refresh iniciado, verificando estado...")
            return (
                self.get_status_history()
            )  # Verificar el estado y retornar el resultado
        else:
            print(response.reason)
            return {"success": False, "error_message": response.reason}

    def get_report_id(self):
        report_id = self.config.get("report_id_powerbi")
        return report_id

    def generate_embed_token(self, report_id):
        access_id = self.request_access_token_refresh()

        # Reemplaza con el ID del grupo de trabajo en Power BI donde se encuentra el informe
        workspace_id = get_secret("GROUP_ID")

        endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/GenerateToken"
        headers = {
            "Authorization": f"Bearer " + access_id,
            "Content-Type": "application/json",
        }
        payload = {"accessLevel": "View"}
        response = requests.post(endpoint, headers=headers, json=payload)

        if response.status_code == 200:
            return response.json()["token"]
        else:
            raise Exception("No se pudo generar el token de incrustación.")

    def get_status_history(self):
        access_id = self.request_access_token_refresh()
        dataset_id = self.config.get("dataset_id_powerbi")
        endpoint = (
            f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes?$top=1"
        )
        headers = {"Authorization": f"Bearer " + access_id}

        max_attempts = 15
        refresh_interval = 240  # 4 minutos
        attempt = 1
        refresh_status = None  # Estado inicial desconocido

        while attempt <= max_attempts:
            response = requests.get(endpoint, headers=headers)
            if response.status_code == 200:
                try:
                    refresh_status = response.json().get("value")[0].get("status")
                    if refresh_status == "Completed":
                        print("Refresh completado exitosamente.")
                        return {"success": True}
                    elif refresh_status == "Failed":
                        error_message = (
                            response.json()
                            .get("value")[0]
                            .get("error", "Error desconocido")
                        )
                        print("El refresh falló. Mensaje de error:", error_message)
                        return {"success": False, "error_message": error_message}
                except (json.decoder.JSONDecodeError, IndexError) as e:
                    print("Falló al obtener el estado del refresh:", e)
                    return {
                        "success": False,
                        "error_message": "Falló al obtener el estado del refresh.",
                    }
            else:
                print("Respuesta inesperada al verificar el estado:", response.reason)
                return {"success": False, "error_message": response.reason}

            time.sleep(refresh_interval)
            attempt += 1

        # Si se agotan los intentos sin completarse
        print("El refresh no se completó en el número de intentos especificado.")
        return {
            "success": False,
            "error_message": "El refresh no se completó en el tiempo esperado.",
        }

    def send_email(self, error_message):
        host = "smtp.gmail.com"
        port = 587
        username = "torredecontrolamovil@gmail.com"
        password = "dldaqtceiesyybje"

        from_addr = "torredecontrolamovil@gmail.com"
        to_addr = ["cesar.trujillo@amovil.co", "soporte@amovil.co"]

        msg = MIMEMultipart()
        msg["From"] = from_addr
        msg["To"] = COMMASPACE.join(to_addr)
        msg["Subject"] = f"Error Bi {self.config.get('nmEmpresa')}"

        body = f"Error en el Bi de {self.config.get('nmEmpresa')}, {error_message}"

        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(host, port) as server:
            server.starttls()
            server.login(username, password)
            server.sendmail(from_addr, to_addr, msg.as_string())
            server.quit()
