import os
import pandas as pd
from sqlalchemy import create_engine, text
from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
import logging
from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
import ast
import xlsxwriter

# Configuración del logging
logging.basicConfig(
    filename="logInterface.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)


class DataBaseConnection:
    """
    Clase para manejar las conexiones a bases de datos MySQL y SQLite.

    Esta clase facilita la conexión a las bases de datos y la ejecución de consultas SQL,
    incluyendo la ejecución de consultas grandes en fragmentos (chunks).

    Attributes:
        config (dict): Configuración utilizada para las conexiones a las bases de datos.
        engine_mysql (sqlalchemy.engine.base.Engine): Motor SQLAlchemy para la base de datos MySQL.
        engine_sqlite (sqlalchemy.engine.base.Engine): Motor SQLAlchemy para la base de datos SQLite.
    """

    def __init__(self, config, mysql_engine=None, sqlite_engine=None):
        """
        Inicializa la instancia de DataBaseConnection con la configuración proporcionada.

        Args:
            config (dict): Configuración para las conexiones a las bases de datos.
            mysql_engine (sqlalchemy.engine.base.Engine, opcional): Motor SQLAlchemy para la base de datos MySQL.
            sqlite_engine (sqlalchemy.engine.base.Engine, opcional): Motor SQLAlchemy para la base de datos SQLite.
        """
        self.config = config
        # Establecer o crear el motor para MySQL
        self.engine_mysql = mysql_engine if mysql_engine else self.create_engine_mysql()
        # print(self.engine_mysql)
        # Establecer o crear el motor para SQLite
        self.engine_sqlite = (
            sqlite_engine if sqlite_engine else create_engine("sqlite:///mydata.db")
        )
        # print(self.engine_sqlite)

    def create_engine_mysql(self):
        """
        Crea un motor SQLAlchemy para la conexión a la base de datos MySQL.

        Utiliza la configuración proporcionada para establecer la conexión.

        Returns:
            sqlalchemy.engine.base.Engine: Motor SQLAlchemy para la base de datos MySQL.
        """
        # Extraer los parámetros de configuración para la conexión MySQL
        user, password, host, port, database = (
            self.config.get("nmUsrIn"),
            self.config.get("txPassIn"),
            self.config.get("hostServerIn"),
            self.config.get("portServerIn"),
            self.config.get("dbBi"),
        )
        # Crear y retornar el motor de conexión
        return con.ConexionMariadb3(
            str(user), str(password), str(host), int(port), str(database)
        )

    def execute_query_mysql(self, query, chunksize=None):
        """
        Ejecuta una consulta SQL en la base de datos MySQL.

        Args:
            query (str): La consulta SQL a ejecutar.
            chunksize (int, opcional): El tamaño del fragmento para la ejecución de la consulta.

        Returns:
            DataFrame: Un DataFrame de pandas con los resultados de la consulta.
        """
        # Conectar a la base de datos y ejecutar la consulta
        with self.create_engine_mysql.connect() as connection:
            cursor = connection.execution_options(isolation_level="READ COMMITTED")
            return pd.read_sql_query(query, cursor, chunksize=chunksize)

    def execute_sql_sqlite(self, sql, params=None):
        """
        Ejecuta una sentencia SQL en la base de datos SQLite.

        Args:
            sql (str): La sentencia SQL a ejecutar.
            params (dict, opcional): Parámetros para la sentencia SQL.

        Returns:
            Resultado de la ejecución de la sentencia.
        """
        # Conectar a la base de datos SQLite y ejecutar la sentencia
        with self.engine_sqlite.connect() as connection:
            return connection.execute(sql, params)

    def execute_query_mysql_chunked(self, query, table_name, chunksize=50000):
        """
        Ejecuta una consulta SQL en la base de datos MySQL y almacena los resultados en SQLite,
        procesando la consulta en fragmentos (chunks).

        Args:
            query (str): La consulta SQL a ejecutar en MySQL.
            table_name (str): El nombre de la tabla en SQLite donde se almacenarán los resultados.
            chunksize (int, opcional): El tamaño del fragmento para la ejecución de la consulta.

        Returns:
            int: El número total de registros almacenados en la tabla SQLite.
        """
        try:
            # Eliminar la tabla en SQLite si ya existe
            self.eliminar_tabla_sqlite(table_name)
            # Conectar a MySQL y ejecutar la consulta en fragmentos
            with self.engine_mysql.connect() as connection:
                cursor = connection.execution_options(isolation_level="READ COMMITTED")
                for chunk in pd.read_sql_query(query, con=cursor, chunksize=chunksize):
                    # Almacenar cada fragmento en la tabla SQLite
                    chunk.to_sql(
                        name=table_name,
                        con=self.engine_sqlite,
                        if_exists="append",
                        index=False,
                    )

            # Contar y retornar el total de registros en la tabla SQLite
            with self.engine_sqlite.connect() as connection:
                total_records = connection.execute(
                    text(f"SELECT COUNT(*) FROM {table_name}")
                ).fetchone()[0]
            return total_records

        except Exception as e:
            # Registrar y propagar cualquier excepción que ocurra
            logging.error(f"Error al ejecutar el query: {e}")
            print(f"Error al ejecutar el query: {e}")
            raise

    def eliminar_tabla_sqlite(self, table_name):
        """
        Elimina una tabla específica en la base de datos SQLite.

        Args:
            table_name (str): El nombre de la tabla a eliminar.
        """
        # Ejecutar la sentencia para eliminar la tabla si existe
        sql = text(f"DROP TABLE IF EXISTS {table_name}")
        with self.engine_sqlite.connect() as connection:
            connection.execute(sql)


class InterfaceContable:
    """
    Clase InterfaceContable para manejar la generación de informes contables.

    Esta clase se encarga de configurar la conexión a la base de datos, procesar los datos y generar informes
    en formato Excel basados en los datos de una base de datos contable.

    Attributes:
        database_name (str): Nombre de la base de datos a utilizar.
        IdtReporteIni (str): Identificador del inicio del rango de reportes.
        IdtReporteFin (str): Identificador del fin del rango de reportes.
        file_path (str): Ruta del archivo Excel generado.
        archivo_interface (str): Nombre del archivo Excel generado.
        config (dict): Configuración para las conexiones a bases de datos y otras operaciones.
        db_connection (DataBaseConnection): Objeto para manejar la conexión a las bases de datos.
        engine_sqlite (sqlalchemy.engine.base.Engine): Motor SQLAlchemy para la base de datos SQLite.
        engine_mysql (sqlalchemy.engine.base.Engine): Motor SQLAlchemy para la base de datos MySQL.
    """

    def __init__(self, database_name, IdtReporteIni, IdtReporteFin):
        """
        Inicializa la instancia de InterfaceContable.

        Args:
            database_name (str): Nombre de la base de datos.
            IdtReporteIni (str): Identificador del inicio del rango de reportes.
            IdtReporteFin (str): Identificador del fin del rango de reportes.
        """
        self.database_name = database_name
        self.IdtReporteIni = IdtReporteIni
        self.IdtReporteFin = IdtReporteFin
        self.configurar(database_name)
        self.file_path = None
        self.archivo_interface = None

    def configurar(self, database_name):
        """
        Configura la conexión a las bases de datos y establece las variables de entorno necesarias.

        Esta función crea una configuración básica y establece las conexiones a las bases de datos MySQL y SQLite
        utilizando los parámetros de configuración.

        Args:
            database_name (str): Nombre de la base de datos para configurar las conexiones.

        Raises:
            Exception: Propaga cualquier excepción que ocurra durante la configuración.
        """
        try:
            # Crear un objeto de configuración básica
            config_basic = ConfigBasic(database_name)
            self.config = config_basic.config
            # Establecer conexiones a las bases de datos
            self.db_connection = DataBaseConnection(config=self.config)
            self.engine_sqlite = self.db_connection.engine_sqlite
            self.engine_mysql = self.db_connection.engine_mysql
        except Exception as e:
            # Registrar y propagar excepciones que ocurran durante la configuración
            logging.error(f"Error al inicializar Interface: {e}")
            raise

    def generate_sqlout(self, hoja):
        """
        Genera la consulta SQL para ser ejecutada, basada en la configuración y el nombre de la hoja proporcionada.

        Este método construye una consulta SQL utilizando un procedimiento almacenado, cuyo nombre se obtiene de la configuración.
        La consulta generada depende del nombre de la base de datos configurada y puede incluir parámetros adicionales
        si la base de datos es específica (en este caso, 'powerbi_tym_eje').

        Args:
            hoja (str): El nombre de la hoja para la cual se generará la consulta SQL.

        Returns:
            sqlalchemy.sql.elements.TextClause: Un objeto TextClause de SQLAlchemy que representa la consulta SQL.
        """
        # Obtener el nombre del procedimiento almacenado de la configuración
        sql = self.config["nmProcedureInterface"]

        # Generar la consulta SQL adecuada basándose en el nombre de la base de datos
        if self.config["dbBi"] == "powerbi_tym_eje":
            # Si la base de datos es 'powerbi_tym_eje', incluir parámetros adicionales en la llamada
            return text(
                f"CALL {sql}('{self.IdtReporteIni}','{self.IdtReporteFin}','','{str(hoja)}',0,0,0);"
            )

        # Para otras bases de datos, utilizar una forma más genérica de la consulta
        return text(
            f"CALL {sql}('{self.IdtReporteIni}','{self.IdtReporteFin}','','{str(hoja)}');"
        )

    def guardar_datos(self, table_name, hoja, total_records, writer):
        if total_records > 1000000:
            self.guardar_datos_excel_xlsxwriter(table_name, hoja, writer)
        elif total_records > 250000:
            self.guardar_datos_excel_xlsxwriter(table_name, hoja, writer)
        else:
            self.guardar_datos_excel_xlsxwriter(table_name, hoja, writer)

    def generar_nombre_archivo(self, ext=".xlsx"):
        """
        Genera el nombre del archivo y la ruta completa para el archivo de salida, basado en los atributos de la clase.

        Args:
            ext (str, opcional): La extensión del archivo a generar. Por defecto es ".xlsx".

        Returns:
            tuple: Un par de valores que incluyen el nombre del archivo y la ruta completa del archivo.
        """
        # Formar el nombre del archivo usando los detalles de la base de datos y el rango de fechas
        # 'self.database_name', 'self.IdtReporteIni' y 'self.IdtReporteFin' son atributos de la clase
        self.archivo_interface = f"Interface_Contable_{self.database_name}_de_{self.IdtReporteIni}_a_{self.IdtReporteFin}{ext}"

        # Construir la ruta completa del archivo, generalmente en un directorio 'media'
        self.file_path = os.path.join("media", self.archivo_interface)

        # Devolver el nombre del archivo y la ruta completa
        return self.archivo_interface, self.file_path

    def guardar_datos_excel_xlsxwriter(self, table_name, hoja, writer):
        """
        Guarda los datos de una tabla SQLite en una hoja de un archivo Excel, utilizando el motor xlsxwriter.

        Args:
            table_name (str): Nombre de la tabla en SQLite de donde se extraen los datos.
            hoja (str): Nombre de la hoja en el archivo Excel donde se guardarán los datos.
            writer (pd.ExcelWriter): Objeto ExcelWriter que se utiliza para escribir en el archivo Excel.
        """
        # Definir el tamaño de cada bloque de datos (chunk)
        chunksize = 50000

        # Establecer conexión con la base de datos SQLite
        with self.engine_sqlite.connect() as connection:
            # Iniciar la fila desde donde empezar a escribir en el archivo Excel
            startrow = 0

            # Iterar sobre los bloques (chunks) de datos de la tabla SQLite
            for chunk in pd.read_sql_query(
                f"SELECT * FROM {table_name}", self.engine_sqlite, chunksize=chunksize
            ):
                # Imprimir el tamaño del chunk actual para seguimiento
                print(f"Procesando chunk de tamaño: {len(chunk)}")

                # Escribir el bloque de datos en el archivo Excel
                # 'header' se establece en False para todos los chunks después del primero
                chunk.to_excel(
                    writer,
                    sheet_name=hoja,
                    startrow=startrow,
                    index=False,
                    header=not bool(startrow),
                )

                # Actualizar la fila de inicio para el siguiente bloque
                startrow += len(chunk)

                # Imprimir la próxima fila inicial para seguimiento
                print(f"Próximo startrow será: {startrow}")

            # Establecer el estado de la hoja de Excel como visible
            writer.sheets[hoja].sheet_state = "visible"

    def procesar_hoja(self, hoja, writer):
        """
        Procesa una hoja específica de datos, ejecutando una consulta SQL, guardando los resultados en una hoja de Excel,
        y luego eliminando la tabla SQLite temporal.

        Args:
            hoja (str): Nombre de la hoja a procesar, que también se utiliza para nombrar la tabla SQLite temporal y la hoja de Excel.
            writer (pd.ExcelWriter): Objeto ExcelWriter que se utiliza para escribir en el archivo Excel.

        Returns:
            bool: Verdadero si la hoja se procesó con éxito, Falso si ocurrió una excepción.
        """
        try:
            # Generar la consulta SQL para esta hoja específica
            sqlout = self.generate_sqlout(hoja)
            # print(sqlout)

            # Crear un nombre de tabla temporal en SQLite basado en el nombre de la hoja
            table_name = f"my_table_{self.database_name}_{hoja}"

            # Ejecutar la consulta SQL y guardar los resultados en la tabla SQLite
            # Devuelve el total de registros procesados
            total_records = self.db_connection.execute_query_mysql_chunked(
                query=sqlout, table_name=table_name
            )
            print(f"Total de registros: {total_records}")

            # Guardar los datos de la tabla SQLite en el archivo Excel
            self.guardar_datos(table_name, hoja, total_records, writer)

            # Eliminar la tabla SQLite temporal después de guardar los datos
            self.db_connection.eliminar_tabla_sqlite(table_name)
            print(f"Procesamiento de la hoja {hoja} finalizado")

            return True

        except Exception as e:
            # En caso de cualquier excepción, registrar el error y devolver un estado de error
            print(f"Error al procesar la hoja {hoja}: {e}")
            logging.error(f"Error al procesar la hoja {hoja}: {e}")
            return {
                "success": False,
                "error_message": f"Error al procesar la hoja {hoja}: {e}",
            }

    def procesar_datos(self):
        """
        Procesa los datos para todas las hojas especificadas en la configuración, guardándolos en un archivo Excel.

        Este método genera el nombre del archivo, procesa cada hoja especificada en la configuración y guarda los datos
        en el archivo Excel. Si ocurre un error durante el procesamiento de cualquier hoja, el método termina prematuramente.

        Returns:
            dict: Un diccionario indicando el éxito o fracaso del proceso y, en caso de éxito, la ruta y el nombre del archivo generado.
        """
        # Generar el nombre del archivo para guardar los datos de todas las hojas
        self.archivo_interface, self.file_path = self.generar_nombre_archivo()

        # Convertir la configuración de hojas a procesar de una cadena a una lista, si es necesario
        txProcedureInterface_str = self.config["txProcedureInterface"]
        if isinstance(txProcedureInterface_str, str):
            try:
                self.config["txProcedureInterface"] = ast.literal_eval(
                    txProcedureInterface_str
                )
            except ValueError as e:
                # Registrar el error y asignar una lista vacía en caso de falla en la conversión
                logging.error(f"Error al convertir txProcedureInterface a lista: {e}")
                self.config["txProcedureInterface"] = []

        # Verificar si hay hojas para procesar
        if not self.config["txProcedureInterface"]:
            return {"success": False, "error_message": "No hay datos para procesar"}

        print("Procesando datos para iniciar el proceso")

        # Crear un único ExcelWriter para guardar todas las hojas
        with pd.ExcelWriter(self.file_path, engine="xlsxwriter") as writer:
            for hoja in self.config["txProcedureInterface"]:
                print(f"Procesando hoja {hoja}")
                # Procesar cada hoja individualmente
                if not self.procesar_hoja(hoja, writer):
                    # En caso de error en el procesamiento de una hoja, devolver un estado de error
                    return {
                        "success": False,
                        "error_message": f"Error al procesar la hoja {hoja}",
                    }

        print("Proceso finalizado")
        print(self.file_path)
        print(self.archivo_interface)

        # Devolver información del archivo generado en caso de éxito
        return {
            "success": True,
            "file_path": self.file_path,
            "file_name": self.archivo_interface,
        }


"""
import os,sys
# from unipath import Path
import pandas as pd
from os import path, system
from distutils.log import error
import ast
from sqlalchemy.sql import text
import sqlalchemy
import pymysql
from scripts.StaticPage import StaticPage
from scripts.conexion import Conexion
from scripts.config import ConfigBasic
import time
import csv
import zipfile
from zipfile import ZipFile
from django.http import HttpResponse,FileResponse,JsonResponse
from io import BytesIO

####################################################################
import logging
logging.basicConfig(filename="log.txt", level=logging.DEBUG,
                    format="%(asctime)s %(message)s", filemode="w")
####################################################################
logging.info('Inciando Proceso')

class Interface_Contable:
    StaticPage = StaticPage()
    def __init__(self,database_name,IdtReporteIni, IdtReporteFin):
        
        ConfigBasic(database_name)
        self.IdtReporteIni=IdtReporteIni
        self.IdtReporteFin=IdtReporteFin

    def Procedimiento_a_Excel(self):
        a = StaticPage.dbBi
        IdDs = ''
        compra = 0
        consig = 0
        nd = 0
        sql = StaticPage.nmProcedureInterface
        StaticPage.archivo_plano = f"Interface_Contable_{StaticPage.name}_de_{self.IdtReporteIni}_a_{self.IdtReporteFin}.xlsx"
        StaticPage.file_path = os.path.join('media', StaticPage.archivo_plano)
        if StaticPage.txProcedureInterface:    
            with pd.ExcelWriter( StaticPage.file_path, engine='openpyxl') as writer:
                for hoja in StaticPage.txProcedureInterface:
                    if a == 'powerbi_tym_eje':
                        sqlout = text(f"CALL {sql}('{self.IdtReporteIni}','{self.IdtReporteFin}','{IdDs}','{hoja}','{compra}','{consig}','{nd}');")     
                    else:
                        sqlout = text(f"CALL {sql}('{self.IdtReporteIni}','{self.IdtReporteFin}','{IdDs}','{hoja}');")
                    try:
                        with StaticPage.conin2.connect() as connectionout:
                            cursor = connectionout.execution_options(isolation_level="READ COMMITTED")
                            resultado = pd.read_sql_query(sql=sqlout, con=cursor)
                            resultado.to_excel(writer, index=False, sheet_name=hoja, header=True)
                            writer.sheets[hoja].sheet_state = 'visible'
                    except Exception as e:
                        print(logging.info(f'No fue posible generar la información por {e}'))
        else:
            return JsonResponse({'success': True, 'error_message': f'La empresa {StaticPage.nmEmpresa} no maneja interface contable'})
                    
    def Procedimiento_a_Plano(self):
        a = StaticPage.dbBi
        IdDs = ''
        StaticPage.archivo_plano = f"Plano_{StaticPage.name}_de_{self.IdtReporteIni}_a_{self.IdtReporteFin}.zip"
        StaticPage.file_path = os.path.join('media', StaticPage.archivo_plano)
        if StaticPage.txProcedureCsv:
            print('aqui')
            sql = StaticPage.nmProcedureCsv
            with zipfile.ZipFile(StaticPage.file_path, "w") as zf:
                for a in StaticPage.txProcedureCsv:
                    with zf.open(a+'.txt', "w") as buffer:
                        sqlout = text(f"CALL {sql}('{self.IdtReporteIni}','{self.IdtReporteFin}','{IdDs}','{a}');")
                        with StaticPage.conin2.connect() as connectionout:
                            cursor = connectionout.execution_options(isolation_level="READ COMMITTED")
                            resultado = pd.read_sql_query(sql=sqlout, con=cursor)
                            resultado.to_csv(buffer,sep='|',index=False,float_format='%.2f',header=True)
        elif StaticPage.txProcedureCsv2:
            sql2 = StaticPage.nmProcedureCsv2   
            with zipfile.ZipFile(StaticPage.file_path, "w") as zf:
                print(StaticPage.txProcedureCsv2)
                for a in StaticPage.txProcedureCsv2:
                    with zf.open(a+'.txt', "w") as buffer:
                        sqlout = text(f"CALL {sql2}('{self.IdtReporteIni}','{self.IdtReporteFin}','{IdDs}','{a}');")
                        with StaticPage.conin2.connect() as connectionout:
                            cursor = connectionout.execution_options(isolation_level="READ COMMITTED")
                            resultado = pd.read_sql_query(sql=sqlout, con=cursor)
                            resultado.to_csv(buffer,sep=',',index=False,header=False,float_format='%.0f')
                            # time.sleep(1)
        else:
            return JsonResponse({'success': True, 'error_message': f'La empresa {StaticPage.nmEmpresa} no maneja archivo plano'})
        
        
        """
