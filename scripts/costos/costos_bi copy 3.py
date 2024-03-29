from datetime import datetime

import zipfile
import os
import pandas as pd
import logging

# from scripts.conexion import Conexion as con
from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from django.contrib import sessions
import re
import ast
from django.core.exceptions import ImproperlyConfigured
import json
import unicodedata

logging.basicConfig(
    filename="log.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
logging.info("Iniciando Proceso CargueZip")


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

    def __init__(self, config):
        """
        Inicializa la instancia de DataBaseConnection con la configuración proporcionada.

        Args:
            config (dict): Configuración para las conexiones a las bases de datos.
            mysql_engine (sqlalchemy.engine.base.Engine, opcional): Motor SQLAlchemy para la base de datos MySQL.
            sqlite_engine (sqlalchemy.engine.base.Engine, opcional): Motor SQLAlchemy para la base de datos SQLite.
        """
        self.config = config
        # Establecer o crear el motor para MySQL
        self.engine_mysql_bi = self.create_engine_mysql_bi()
        self.engine_mysql_conf = self.create_engine_mysql_conf()
        # print(self.engine_mysql_bi)
        # print(self.engine_mysql_conf)
        # Establecer o crear el motor para SQLite
        self.engine_sqlite = create_engine("sqlite:///mydata.db")
        # print(self.engine_sqlite)

    def create_engine_mysql_bi(self):
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

    def create_engine_mysql_conf(self):
        """
        Crea un motor SQLAlchemy para la conexión a la base de datos MySQL.

        Utiliza la configuración proporcionada para establecer la conexión.

        Returns:
            sqlalchemy.engine.base.Engine: Motor SQLAlchemy para la base de datos MySQL.
        """
        # Extraer los parámetros de configuración para la conexión MySQL
        user, password, host, port, database = (
            get_secret("DB_USERNAME"),
            get_secret("DB_PASS"),
            get_secret("DB_HOST"),
            int(get_secret("DB_PORT")),
            get_secret("DB_NAME"),
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
        with self.create_engine_mysql_bi.connect() as connection:
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
            with self.engine_mysql_bi.connect() as connection:
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


class CargueHistoricoCostos:
    """
    Clase para manejar el proceso de carga de historico de costos.

    Esta clase se encargará de procesar los archivos planos, realizar limpieza y validaciones
    necesarias, y cargar los datos en la base de datos.
    """

    def __init__(self, database_name):
        print("listo iniciando aqui en la clase de zip")
        """
        Inicializa la instancia de CargueHistoricoCostos.

        Args:
            database_name (str): Nombre de la base de datos.
        """
        self.database_name = database_name
        self.configurar(database_name)

    def configurar(self, database_name):
        print("listo iniciando aqui en la configuracion")
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
            self.engine_mysql_bi = self.db_connection.engine_mysql_bi
            self.engine_mysql_conf = self.db_connection.engine_mysql_conf
        except Exception as e:
            # Registrar y propagar excepciones que ocurran durante la configuración
            logging.error(f"Error al inicializar Interface: {e}")
            raise

    def procesar_todas_las_fechas(self, fecha_corte=None):
        if fecha_corte is not None:
            fecha_cortestr = fecha_corte.strftime("%Y-%m-%d")
        elif fecha_corte is None:
            fecha_corte = datetime.now()
            fecha_cortestr = fecha_corte.strftime("%Y-%m-%d")
        df_fechas = self.obtener_fechas_de_movimientos(fecha_cortestr)
        df_fechas["almacen"] = df_fechas["almacen"].astype(str)
        df_fechas["producto"] = df_fechas["producto"].astype(str)
        for row in df_fechas.itertuples():
            try:
                fecha = row.fecha
                almacen = row.almacen
                producto = row.producto
                print(f"Procesando datos para la fecha: {fecha}")
                self.procesar_datos_por_fecha(fecha, almacen, producto)
            except ValueError as e:
                print(f"Error al convertir el valor de producto: {row.producto}")
                print(f"Error: {e}")
                continue

    def cargar_compras(self, fecha, almacen, producto):
        """Carga las compras en un DataFrame.

        Args:
            fecha: Fecha a procesar.
            almacen: Almacén a procesar.
            producto: Producto a procesar.

        Returns:
            DataFrame con las compras.
        """

        parametros = {"fecha": fecha, "almacen": almacen, "producto": producto}

        sqlcompras = text(
            """
            SELECT 
            DATE(m.dtContabilizacion) fecha,
            m.nbAlmacen almacen,
            m.nbProducto producto, 
            COALESCE(((SUM((m.flPrecioUnitario * m.flCantidad))) / (SUM(m.flCantidad))),0) AS flPrecioUnitario, 
            COALESCE(SUM(m.flCantidad),0) AS flCantidad
            FROM mmovlogistico m
            WHERE m.nbMovimientoClase = '200' 
            AND DATE(m.dtContabilizacion) = :fecha
            AND m.nbAlmacen = :almacen
            AND m.nbProducto = :producto
            GROUP BY DATE(m.dtContabilizacion), m.nbAlmacen, m.nbProducto;
            """
        )

        df_compras = pd.read_sql(
            sqlcompras,
            self.engine_mysql_bi,
            params=parametros,
        )
        if df_compras.empty:
            # Opción 1: Devolver un DataFrame vacío
            return df_compras
        return df_compras

    def cargar_movimientos(self, fecha, almacen, producto):
        """Carga los movimientos en un DataFrame.

        Args:
            fecha: Fecha a procesar.
            almacen: Almacén a procesar.
            producto: Producto a procesar.

        Returns:
            DataFrame con los movimientos.
        """

        parametros = {"fecha": fecha, "almacen": almacen, "producto": producto}

        sqlmovimientos = text(
            """
            SELECT
                DATE( m.dtContabilizacion ) fecha,
                m.nbAlmacen almacen,
                m.nbProducto producto,
                COALESCE ( SUM( m.flCantidad ), 0 ) AS flCantidad
            FROM
                mmovlogistico m 
            WHERE
                m.nbMovimientoClase != '200' 
                AND DATE( m.dtContabilizacion ) = :fecha 
                AND m.nbAlmacen = :almacen 
                AND m.nbProducto = :producto 
            GROUP BY
                DATE( m.dtContabilizacion ),
                m.nbAlmacen,
                m.nbProducto;
            """
        )

        df_otros_movimientos = pd.read_sql(
            sqlmovimientos,
            self.engine_mysql_bi,
            params=parametros,
        )

        return df_otros_movimientos

    def cargar_costos_iniciales(self, almacen, producto):
        """Carga los costos iniciales para un almacén y producto específicos en un DataFrame.

        Args:
            almacen (str): El código o identificador del almacén.
            producto (str): El código o identificador del producto.

        Returns:
            DataFrame con los costos iniciales para el almacén y producto especificados.
            Si no se encuentran datos, devuelve un DataFrame vacío con la estructura de columnas esperada.
        """

        print(
            f"Cargando costos iniciales para el almacén {almacen} y producto {producto}..."
        )

        sql_costos_iniciales = text(
            """
            SELECT 
                m.nbAlmacen almacen,
                m.nbProducto producto,
                MIN(DATE(m.dtContabilizacion)) AS fecha,
                m.flPrecioUnitario costoPromedioFinal,
                m.flCantidad unidadesFinal
            FROM mmovlogistico m
            INNER JOIN cmovimientoclase c ON m.nbMovimientoClase = c.nbMovimientoClase AND c.tpMovimientoClase = 'E'
            WHERE m.nbAlmacen = :almacen AND m.nbProducto = :producto
            GROUP BY m.nbAlmacen, m.nbProducto;
            """
        )

        df_costos_iniciales = pd.read_sql(
            sql_costos_iniciales,
            self.engine_mysql_bi,
            params={"almacen": almacen, "producto": producto},
        )

        if df_costos_iniciales.empty:
            print(
                f"No se encontraron costos iniciales para el almacén {almacen} y producto {producto}."
            )
            df_costos_iniciales = pd.DataFrame(
                columns=["almacen", "producto", "costoPromedioFinal", "unidadesFinal"]
            )

        return df_costos_iniciales

    def obtener_fechas_de_movimientos(self, fecha_corte=datetime.now()):
        with self.engine_mysql_bi.connect() as connection:
            consulta_fechas = text(
                """
                SELECT 
                    DATE(m.dtContabilizacion) AS fecha,
                    m.nbAlmacen almacen,
                    m.nbProducto producto
                FROM mmovlogistico m
                WHERE DATE(m.dtContabilizacion) <= :fechaCorte
                GROUP BY DATE(m.dtContabilizacion), m.nbAlmacen, m.nbProducto;
                """
            )
            df_fechas = pd.read_sql_query(
                consulta_fechas, connection, params={"fechaCorte": fecha_corte}
            )
            return df_fechas

    def insertar_o_actualizar(self, df_resultado):
        with self.engine_mysql_bi.connect() as connection:
            for index, row in df_resultado.iterrows():
                # Asumiendo que la validación de datos ya se ha realizado
                try:
                    sql_upsert = text(
                        """
                        INSERT INTO HistoricoCostoPromedio 
                        (fechaHora, almacen, producto, costoPromedioInicial, unidadesInicial, costoCompradia, unidadesCompradia, unidadesMovimientodia, costoPromedioFinal, unidadesFinal)
                        VALUES 
                        (:fechaHora, :almacen, :producto, :costoPromedioInicial, :unidadesInicial, :costoCompradia, :unidadesCompradia, :unidadesMovimientodia, :costoPromedioFinal, :unidadesFinal)
                        ON DUPLICATE KEY UPDATE 
                        costoPromedioInicial=VALUES(costoPromedioInicial), unidadesInicial=VALUES(unidadesInicial), costoCompradia=VALUES(costoCompradia), unidadesCompradia=VALUES(unidadesCompradia), unidadesMovimientodia=VALUES(unidadesMovimientodia), costoPromedioFinal=VALUES(costoPromedioFinal), unidadesFinal=VALUES(unidadesFinal);
                    """
                    )
                    parametros = row.to_dict()
                    resultado = connection.execute(sql_upsert, parametros)
                    if resultado.rowcount < 1:
                        logging.warning(
                            f"Ninguna fila afectada al insertar/actualizar el registro para {parametros}"
                        )
                    else:
                        logging.info(
                            f"Registro insertado/actualizado con éxito para {parametros}"
                        )
                except Exception as e:
                    logging.error(
                        f"Error al insertar/actualizar el registro: {e}, Datos: {parametros}"
                    )

    def procesar_datos_por_fecha(self, fecha, almacen, producto):
        """Procesa los datos para una fecha específica.

        Args:
            fecha: Fecha a procesar.
            almacen: Almacén a procesar.
            producto: Producto a procesar.
        """

        # Cargar datos de compras y otros movimientos
        df_compras = self.cargar_compras(fecha, almacen, producto)
        df_otros_movimientos = self.cargar_movimientos(fecha, almacen, producto)

        # Unificar los DataFrames de compras y otros movimientos
        if df_compras is not None and not df_compras.empty:
            df_total_movimientos = pd.concat(
                [df_compras, df_otros_movimientos], ignore_index=True
            )
        else:
            df_total_movimientos = (
                df_otros_movimientos
                if df_otros_movimientos is not None
                else pd.DataFrame()
            )

        if df_total_movimientos.empty:
            print(
                f"No hay movimientos para procesar en la fecha {fecha} para el almacén {almacen} y el producto {producto}."
            )
            return

        # Asegurar que los valores faltantes en flCantidad sean tratados como 0
        df_total_movimientos["flCantidad"] = df_total_movimientos["flCantidad"].fillna(
            0
        )

        # Obtener el último registro de HistoricoCostoPromedio
        df_historico_previo = self._get_historico_previo(fecha, almacen, producto)

        # Llenar campos específicos según la fuente de datos
        if df_compras is not None and not df_compras.empty:
            df_total_movimientos["costoCompradia"] = df_compras["flPrecioUnitario"]
            df_total_movimientos["unidadesCompradia"] = df_compras["flCantidad"]
        else:
            df_total_movimientos["costoCompradia"] = 0
            df_total_movimientos["unidadesCompradia"] = 0

        if df_otros_movimientos is not None and not df_otros_movimientos.empty:
            df_total_movimientos["unidadesMovimientodia"] = df_otros_movimientos[
                "flCantidad"
            ]
        else:
            df_total_movimientos["unidadesMovimientodia"] = 0

        # # Realizar cálculos sobre el DataFrame
        # df_resultado = df_total_movimientos.apply(
        #     lambda x: self._calcular_valores_dia(
        #         x, fecha, almacen, producto, df_historico_previo
        #     ),
        #     axis=1,
        # )
        resultados = []
        for index, row in df_total_movimientos.iterrows():
            resultado_fila = self._calcular_valores_dia(
                row, fecha, almacen, producto, df_historico_previo
            )
            resultados.append(resultado_fila.iloc[0])

        df_resultado = pd.DataFrame(resultados)
        df_resultado.fillna(
            0, inplace=True
        )  # Reemplaza todos los NaN por 0 en el DataFrame

        # Insertar o actualizar registros en la base de datos
        self.insertar_o_actualizar(df_resultado)

    def _calcular_valores_dia(
        self, df_producto, fecha, almacen, producto, df_historico_previo
    ):
        """Calcula los valores para un día específico.

        Args:
            df_producto: DataFrame con los datos del producto para un día.
            fecha: Fecha a procesar.
            almacen: Almacén a procesar.
            producto: Producto a procesar.
            df_historico_previo: DataFrame con el último registro de HistoricoCostoPromedio.

        Returns:
            DataFrame con los valores calculados.
        """
        # Intenta obtener el último registro de HistoricoCostoPromedio o costos iniciales
        if not df_historico_previo.empty:
            try:
                resultado_previo = df_historico_previo.loc[(almacen, producto), :]
            except KeyError:
                print(
                    f"No se encontró un registro histórico previo para el almacén {almacen} y producto {producto}."
                )
                resultado_previo = None
        else:
            df_costos_iniciales = self.cargar_costos_iniciales(almacen, producto)
            if not df_costos_iniciales.empty:
                resultado_previo = df_costos_iniciales.iloc[0].to_dict()
            else:
                resultado_previo = {
                    "almacen": almacen,
                    "producto": producto,
                    "costoPromedioFinal": 0,
                    "unidadesFinal": 0,
                }

        # Extrae valores de resultado_previo, que ahora siempre es un diccionario
        costoPromedioInicial = resultado_previo.get("costoPromedioFinal", 0)
        unidadesInicial = resultado_previo.get("unidadesFinal", 0)

        # Calcular valores
        costoCompradia = df_producto["costoCompradia"] if not df_producto.empty else 0
        unidadesCompradia = (
            df_producto["unidadesCompradia"] if not df_producto.empty else 0
        )
        unidadesMovimientodia = (
            df_producto["unidadesMovimientodia"] if not df_producto.empty else 0
        )

        unidadesFinal = unidadesInicial + unidadesCompradia + unidadesMovimientodia
        costoPromedioFinal = (
            (
                (unidadesInicial + unidadesMovimientodia) * costoPromedioInicial
                + unidadesCompradia * costoCompradia
            )
            / unidadesFinal
            if unidadesFinal != 0
            else costoPromedioInicial
        )

        return pd.DataFrame(
            [
                {
                    "fechaHora": fecha,
                    "almacen": almacen,
                    "producto": producto,
                    "costoPromedioInicial": costoPromedioInicial,
                    "unidadesInicial": unidadesInicial,
                    "costoCompradia": costoCompradia,
                    "unidadesCompradia": unidadesCompradia,
                    "unidadesMovimientodia": unidadesMovimientodia,
                    "costoPromedioFinal": costoPromedioFinal,
                    "unidadesFinal": unidadesFinal,
                }
            ]
        )

    def _get_historico_previo(self, fecha_str, almacen, producto):
        """Obtiene el último registro de HistoricoCostoPromedio para una fecha específica.

        Args:
            fecha_str: Fecha a procesar.
            almacen: Almacén a procesar.
            producto: Producto a procesar.

        Returns:
            DataFrame con el último registro de HistoricoCostoPromedio.
        """

        query = text(
            """
            SELECT almacen, producto, costoPromedioFinal, unidadesFinal
            FROM HistoricoCostoPromedio
            WHERE fechaHora <= :fecha
            AND almacen = :almacen
            AND producto = :producto
            ORDER BY fechaHora DESC
            LIMIT 1;
            """
        )

        with self.engine_mysql_bi.connect() as connection:
            resultado = connection.execute(
                query, {"fecha": fecha_str, "almacen": almacen, "producto": producto}
            ).fetchone()

        if resultado:
            df_historico_previo = pd.DataFrame(
                [resultado],
                columns=["almacen", "producto", "costoPromedioFinal", "unidadesFinal"],
            )
            df_historico_previo.set_index(["almacen", "producto"], inplace=True)
        else:
            df_historico_previo = pd.DataFrame(
                columns=["almacen", "producto", "costoPromedioFinal", "unidadesFinal"]
            ).set_index(["almacen", "producto"])

        return df_historico_previo
