import os
import pandas as pd
from sqlalchemy import create_engine, text
from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
import logging
from scripts.StaticPage import StaticPage
from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
import ast
import xlsxwriter
import zipfile
from zipfile import ZipFile

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


class InterfacePlano:
    """
    Clase InterfaceContable para manejar la generación de informes contables.

    Esta clase se encarga de configurar la conexión a la base de datos, procesar los datos y generar informes
    en formato Excel basados en los datos de una base de datos contable.

    Attributes:
        database_name (str): Nombre de la base de datos a utilizar.
        IdtReporteIni (str): Identificador del inicio del rango de reportes.
        IdtReporteFin (str): Identificador del fin del rango de reportes.
        file_path (str): Ruta del archivo Excel generado.
        archivo_plano (str): Nombre del archivo Excel generado.
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
        self.archivo_plano = None

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
            # config_basic.print_configuration()
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
        # Obtener el nombre del procedimiento almacenado de la configuración
        sql = self.config["nmProcedureCsv"]

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

    def guardar_datos(self, table_name, buffer):
        self.guardar_datos_csv(table_name, buffer)

    def generar_nombre_archivo(self, ext=".zip"):
        """
        Genera el nombre del archivo y la ruta completa para el archivo de salida, basado en los atributos de la clase.

        Args:
            ext (str, opcional): La extensión del archivo a generar. Por defecto es ".xlsx".

        Returns:
            tuple: Un par de valores que incluyen el nombre del archivo y la ruta completa del archivo.
        """
        # Formar el nombre del archivo usando los detalles de la base de datos y el rango de fechas
        # 'self.database_name', 'self.IdtReporteIni' y 'self.IdtReporteFin' son atributos de la clase
        self.archivo_plano = f"Interface_Contable_{self.database_name}_de_{self.IdtReporteIni}_a_{self.IdtReporteFin}{ext}"

        # Construir la ruta completa del archivo, generalmente en un directorio 'media'
        self.file_path = os.path.join("media", self.archivo_plano)

        # Devolver el nombre del archivo y la ruta completa
        return self.archivo_plano, self.file_path

    def guardar_datos_csv(self, table_name, buffer):
        """
        Guarda los datos de una tabla SQLite en un archivo CSV, manejando grandes volúmenes de datos.

        Args:
        table_name (str): Nombre de la tabla en SQLite de donde se extraerán los datos.
        file_path (str): Ruta del archivo CSV de destino.

        Descripción:
        Este método extrae datos de una tabla SQLite en bloques (chunks) de tamaño definido
        y los guarda en un archivo CSV. Es útil para manejar grandes volúmenes de datos que
        no caben en la memoria de una sola vez.

        El archivo CSV se escribe en modo 'append', lo que permite añadir datos al archivo
        existente sin sobrescribirlo. Cada bloque de datos se lee de la tabla SQLite y se
        escribe en el archivo CSV hasta que todos los datos han sido procesados.

        Se utiliza un tamaño de bloque (chunksize) de 100000, lo que significa que los datos se
        leerán y escribirán en bloques de 100000 filas a la vez. Este valor puede ajustarse según
        las necesidades de rendimiento y la capacidad de la memoria.
        """
        chunksize = 50000  # Define el tamaño de cada bloque de datos

        with self.engine_sqlite.connect() as connection:
            for chunk in pd.read_sql_query(
                f"SELECT * FROM {table_name}", self.engine_sqlite, chunksize=chunksize
            ):
                if chunk.empty:
                    print(f"No hay datos en el chunk para la tabla {table_name}")
                    continue

                try:
                    chunk.to_csv(
                        buffer, sep="|", index=False, float_format="%.2f", header=True
                    )
                    print(f"Datos escritos en el buffer para la tabla {table_name}")
                except Exception as e:
                    print(f"Error al escribir datos en el buffer: {e}")
                    raise

    def procesar_hoja(self, hoja, buffer):
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
            self.guardar_datos(table_name, buffer)

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
        self.archivo_plano, self.file_path = self.generar_nombre_archivo()

        # Convertir la configuración de hojas a procesar de una cadena a una lista, si es necesario
        txProcedureCsv_str = self.config["txProcedureCsv"]
        if isinstance(txProcedureCsv_str, str):
            try:
                self.config["txProcedureCsv"] = ast.literal_eval(txProcedureCsv_str)
            except ValueError as e:
                # Registrar el error y asignar una lista vacía en caso de falla en la conversión
                logging.error(f"Error al convertir txProcedureCsv a lista: {e}")
                self.config["txProcedureCsv"] = []

        # Verificar si hay hojas para procesar
        if not self.config["txProcedureCsv"]:
            return {"success": False, "error_message": "No hay datos para procesar"}

        print("Procesando datos para iniciar el proceso")

        # Crear un único Zip para guardar todas las hojas
        with zipfile.ZipFile(self.file_path, "w") as zf:
            for hoja in self.config["txProcedureCsv"]:
                print(f"Procesando hoja {hoja}")
                # Procesar cada hoja individualmente
                with zf.open(hoja + ".txt", "w") as buffer:
                    if not self.procesar_hoja(hoja, buffer):
                        # En caso de error en el procesamiento de una hoja, devolver un estado de error
                        return {
                            "success": False,
                            "error_message": f"Error al procesar la hoja {hoja}",
                        }

        print("Proceso finalizado")
        # print(self.file_path)
        # print(self.archivo_plano)

        # Devolver información del archivo generado en caso de éxito
        return {
            "success": True,
            "file_path": self.file_path,
            "file_name": self.archivo_plano,
        }

    def generate_sqlout2(self, hoja):
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
        sql = self.config["nmProcedureCsv2"]

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

    def guardar_datos2(self, table_name, buffer):
        self.guardar_datos_csv2(table_name, buffer)

    def guardar_datos_csv2(self, table_name, buffer):
        """
        Guarda los datos de una tabla SQLite en un archivo CSV, manejando grandes volúmenes de datos.

        Args:
        table_name (str): Nombre de la tabla en SQLite de donde se extraerán los datos.
        file_path (str): Ruta del archivo CSV de destino.

        Descripción:
        Este método extrae datos de una tabla SQLite en bloques (chunks) de tamaño definido
        y los guarda en un archivo CSV. Es útil para manejar grandes volúmenes de datos que
        no caben en la memoria de una sola vez.

        El archivo CSV se escribe en modo 'append', lo que permite añadir datos al archivo
        existente sin sobrescribirlo. Cada bloque de datos se lee de la tabla SQLite y se
        escribe en el archivo CSV hasta que todos los datos han sido procesados.

        Se utiliza un tamaño de bloque (chunksize) de 100000, lo que significa que los datos se
        leerán y escribirán en bloques de 100000 filas a la vez. Este valor puede ajustarse según
        las necesidades de rendimiento y la capacidad de la memoria.
        """
        chunksize = 50000  # Define el tamaño de cada bloque de datos

        with self.engine_sqlite.connect() as connection:
            # Iterar sobre los bloques (chunks) de datos de la tabla SQLite
            for chunk in pd.read_sql_query(
                f"SELECT * FROM {table_name}", self.engine_sqlite, chunksize=chunksize
            ):
                # Escribir el bloque de datos en el archivo CSV en modo 'append'
                chunk.to_csv(
                    buffer, sep=",", index=False, header=False, float_format="%.0f"
                )

    def procesar_hoja2(self, hoja, buffer):
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
            sqlout = self.generate_sqlout2(hoja)
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
            self.guardar_datos2(table_name, buffer)

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

    def procesar_datos2(self):
        """
        Procesa los datos para todas las hojas especificadas en la configuración, guardándolos en un archivo Excel.

        Este método genera el nombre del archivo, procesa cada hoja especificada en la configuración y guarda los datos
        en el archivo Excel. Si ocurre un error durante el procesamiento de cualquier hoja, el método termina prematuramente.

        Returns:
            dict: Un diccionario indicando el éxito o fracaso del proceso y, en caso de éxito, la ruta y el nombre del archivo generado.
        """
        # Generar el nombre del archivo para guardar los datos de todas las hojas
        self.archivo_plano, self.file_path = self.generar_nombre_archivo()

        # Convertir la configuración de hojas a procesar de una cadena a una lista, si es necesario
        txProcedureCsv2_str = self.config["txProcedureCsv2"]
        if isinstance(txProcedureCsv2_str, str):
            try:
                self.config["txProcedureCsv2"] = ast.literal_eval(txProcedureCsv2_str)
            except ValueError as e:
                # Registrar el error y asignar una lista vacía en caso de falla en la conversión
                logging.error(f"Error al convertir txProcedureCsv2 a lista: {e}")
                self.config["txProcedureCsv2"] = []

        # Verificar si hay hojas para procesar
        if not self.config["txProcedureCsv2"]:
            return {"success": False, "error_message": "No hay datos para procesar"}

        print("Procesando datos para iniciar el proceso")

        # Crear un único Zip para guardar todas las hojas
        with zipfile.ZipFile(self.file_path, "w") as zf:
            for hoja in self.config["txProcedureCsv2"]:
                print(f"Procesando hoja {hoja}")
                # Procesar cada hoja individualmente
                with zf.open(hoja + ".txt", "w") as buffer:
                    if not self.procesar_hoja2(hoja, buffer):
                        # En caso de error en el procesamiento de una hoja, devolver un estado de error
                        return {
                            "success": False,
                            "error_message": f"Error al procesar la hoja {hoja}",
                        }

        print("Proceso finalizado")
        # print(self.file_path)
        print(self.archivo_plano)

        # Devolver información del archivo generado en caso de éxito
        return {
            "success": True,
            "file_path": self.file_path,
            "file_name": self.archivo_plano,
        }

    def evaluar_y_procesar_datos(self):
        """
        Evalúa las configuraciones 'txProcedureCsv' y 'txProcedureCsv2' y decide qué método de procesamiento utilizar.

        Si ambas listas están vacías, se informa que no hay datos para procesar. Si alguna de las listas tiene datos,
        se llama al método correspondiente para procesar esos datos.

        Returns:
            dict: Un diccionario indicando el éxito o fracaso del proceso o, en caso de que no haya datos, un mensaje indicativo.
        """
        # Convertir las configuraciones de hojas a procesar de una cadena a una lista
        txProcedureCsv = self.obtener_lista_hojas("txProcedureCsv")
        txProcedureCsv2 = self.obtener_lista_hojas("txProcedureCsv2")

        # Verificar si ambas listas están vacías
        if not txProcedureCsv and not txProcedureCsv2:
            return {"success": False, "error_message": "La empresa no maneja planos"}

        # Si txProcedureCsv tiene datos, procesarlos
        if txProcedureCsv:
            return self.procesar_datos()
        else:
            # Si txProcedureCsv2 tiene datos, procesarlos
            if txProcedureCsv2:
                return self.procesar_datos2()

    def obtener_lista_hojas(self, config_key):
        """
        Convierte la configuración de hojas dada por 'config_key' de una cadena a una lista.

        Args:
            config_key (str): Clave en la configuración para obtener la lista de hojas.

        Returns:
            list: Lista de hojas a procesar.
        """
        hojas_str = self.config.get(config_key, "")
        if isinstance(hojas_str, str):
            try:
                return ast.literal_eval(hojas_str)
            except ValueError as e:
                logging.error(f"Error al convertir {config_key} a lista: {e}")
        return []
