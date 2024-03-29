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
        print(self.engine_mysql_bi)
        print(self.engine_mysql_conf)
        # Establecer o crear el motor para SQLite
        self.engine_sqlite = create_engine("sqlite:///mydata.db")
        print(self.engine_sqlite)

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
        for fecha, almacen, producto in zip(df_fechas["fecha"], df_fechas["almacen"], df_fechas["producto"]): 
            print(f"Procesando datos para la fecha: {fecha}")
            self.procesar_datos_por_fecha(fecha, almacen, producto)   


    # def cargar_compras(self, fecha):
    #     # Cargar las compras en un DataFrame
    #     parametros = {"fecha": fecha}  # Diccionario de parámetros

    #     sqlcompras = text(
    #         """
    #         SELECT 
    #             DATE(m.dtContabilizacion),
    #             m.nbAlmacen,
    #             m.nbProducto, 
    #             SUM((m.flPrecioUnitario * m.flCantidad))/SUM(m.flCantidad) AS flPrecioUnitario,  
    #             SUM(m.flCantidad) AS flCantidad
    #         FROM mmovlogistico m
    #         WHERE m.nbMovimientoClase = '200' AND DATE(m.dtContabilizacion) = :fecha
    #         GROUP BY DATE(m.dtContabilizacion), m.nbAlmacen, m.nbProducto;
    #         """
    #     )
    #     self.df_compras = pd.read_sql(
    #         sqlcompras,
    #         self.engine_mysql_bi,
    #         params=parametros,  # Pasar parámetros a la consulta
    #     )

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
            DATE(m.dtContabilizacion),
            m.nbAlmacen,
            m.nbProducto, 
            SUM((m.flPrecioUnitario * m.flCantidad))/SUM(m.flCantidad) AS flPrecioUnitario,  
            SUM(m.flCantidad) AS flCantidad
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
            DATE(m.dtContabilizacion),
            m.nbAlmacen,
            m.nbProducto,  
            SUM(m.flCantidad) AS flCantidad
            FROM mmovlogistico m
            WHERE m.nbMovimientoClase != '200' 
            AND DATE(m.dtContabilizacion) = :fecha
            AND m.nbAlmacen = :almacen
            AND m.nbProducto = :producto
            GROUP BY DATE(m.dtContabilizacion), m.nbAlmacen, m.nbProducto;
            """
        )

        df_otros_movimientos = pd.read_sql(
            sqlmovimientos,
            self.engine_mysql_bi,
            params=parametros,
        )

        return df_otros_movimientos



    def cargar_costos_iniciales(self):

        """Carga los costos iniciales en un DataFrame.

        Args:

        Returns:
            DataFrame con los costos iniciales.
        """

        print("Cargando costos iniciales...")

        sql_costos_iniciales = """
        SELECT 
            m.nbAlmacen,
            m.nbProducto,
            MIN(DATE(m.dtContabilizacion)) AS dtContabilizacion,
            m.flPrecioUnitario,
            m.flCantidad
        FROM mmovlogistico m
        INNER JOIN cmovimientoclase c ON m.nbMovimientoClase = c.nbMovimientoClase AND c.tpMovimientoClase = 'E'
        GROUP BY m.nbAlmacen, m.nbProducto;
        """

        df_costos_iniciales = pd.read_sql(
            sql_costos_iniciales, self.engine_mysql_bi
        )

        return df_costos_iniciales



    def obtener_fechas_de_movimientos(self, fecha_corte=datetime.now()):
        with self.engine_mysql_bi.connect() as connection:
            consulta_fechas = text(
                """
                SELECT fecha, almacen, producto
                FROM movimientos
                WHERE fecha <= :fecha_corte
                GROUP BY fecha, almacen, producto
                ORDER BY fecha ASC
            """
            )
            df_fechas = connection.execute(
                consulta_fechas, {"fechaCorte": fecha_corte}
            ).fetchall()
            return df_fechas

    def insertar_o_actualizar(self, df_resultado):
        with self.engine_mysql_bi.connect() as connection:
            for index, row in df_resultado.iterrows():
                # Verificar si existe un registro previo
                existe_registro = self._verificar_registro_previo(
                    connection, row["almacen"], row["producto"], row["fechaHora"]
                )

                if existe_registro:
                    # Actualizar el último registro
                    self._actualizar_registro(connection, row)
                else:
                    # Insertar nuevo registro
                    self._insertar_registro(connection, row)

    def _verificar_registro_previo(self, connection, almacen, producto, fecha):
        query = text(
            """
            SELECT COUNT(*) FROM HistoricoCostoPromedio
            WHERE almacen = :almacen AND producto = :producto AND fechaHora = :fecha;
        """
        )
        resultado = connection.execute(
            query, {"almacen": almacen, "producto": producto, "fecha": fecha}
        ).scalar()
        return resultado > 0

    def _actualizar_registro(self, connection, row):
        update_query = text(
            """
            UPDATE HistoricoCostoPromedio
            SET costoPromedioFinal = :costoPromedioFinal, unidadesFinal = :unidadesFinal
            WHERE almacen = :almacen AND producto = :producto AND fechaHora = :fechaHora;
        """
        )
        connection.execute(update_query, row.to_dict())

    def _insertar_registro(self, connection, row):
        insert_query = text(
            """
            INSERT INTO HistoricoCostoPromedio (fechaHora, almacen, producto, costoPromedioInicial, unidadesInicial, costoCompradia, unidadesCompradia, unidadesMovimientodia, costoPromedioFinal, unidadesFinal)
            VALUES (:fechaHora, :almacen, :producto, :costoPromedioInicial, :unidadesInicial, :costoCompradia, :unidadesCompradia, :unidadesMovimientodia, :costoPromedioFinal, :unidadesFinal);
        """
        )
        connection.execute(insert_query, row.to_dict())


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
            print(f"No hay movimientos para procesar en la fecha {fecha} para el almacén {almacen} y el producto {producto}.")
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

        df_total_movimientos["unidadesMovimientodia"] = self._get_unidades_movimientodia(
            almacen, producto, fecha
        )

        # Realizar cálculos sobre el DataFrame
        df_resultado = df_total_movimientos.apply(
            lambda x: self._calcular_valores_dia(x, fecha, almacen, producto, df_historico_previo), axis=1
        )

        # Insertar o actualizar registros en la base de datos
        self.insertar_o_actualizar(df_resultado)




    def _calcular_valores_dia(self, df_producto, fecha, almacen, producto, df_historico_previo):

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

        # Obtener el último registro de HistoricoCostoPromedio
        resultado_previo = df_historico_previo.loc[(almacen, producto), :]

        # Si no hay un registro previo, usar valores por defecto
        costoPromedioInicial, unidadesInicial = (
            resultado_previo["costoPromedioFinal", "unidadesFinal"]
            if not resultado_previo.empty
            else (0, 0)
        )

        # Calcular valores
        costoCompradia = df_producto["costoCompradia"] if not df_producto.empty else 0
        unidadesCompradia = df_producto["unidadesCompradia"] if not df_producto.empty else 0
        unidadesMovimientodia = df_producto["unidadesMovimientodia"] if not df_producto.empty else 0

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
            },
            index=[(almacen, producto)],
        )




    # def procesar_datos_por_fecha(self, fecha,almacen,producto):
    #     # Cargar datos de compras y otros movimientos
    #     df_compras = self.cargar_compras(fecha)
    #     df_otros_movimientos = self.cargar_movimientos(fecha)

    #     # Unificar los DataFrames de compras y otros movimientos
    #     if df_compras is not None and not df_compras.empty:
    #         df_total_movimientos = pd.concat(
    #             [df_compras, df_otros_movimientos], ignore_index=True
    #         )
    #     else:
    #         df_total_movimientos = (
    #             df_otros_movimientos
    #             if df_otros_movimientos is not None
    #             else pd.DataFrame()
    #         )

    #     if df_total_movimientos.empty:
    #         print("No hay movimientos para procesar en esta fecha.")
    #         return

    #     # Asegurar que los valores faltantes en flCantidad sean tratados como 0
    #     df_total_movimientos["flCantidad"] = df_total_movimientos["flCantidad"].fillna(
    #         0
    #     )

    #     # Agrupar por almacén y producto para sumar las cantidades
    #     df_agrupado = (
    #         df_total_movimientos.groupby(["nbAlmacen", "nbProducto"])
    #         .agg({"flCantidad": "sum"})
    #         .reset_index()
    #     )

    #     # Obtener el último registro de HistoricoCostoPromedio
    #     df_historico_previo = self._get_historico_previo(fecha)

    #     # Realizar cálculos sobre el DataFrame agrupado
    #     df_resultado = df_agrupado.apply(
    #         lambda x: self._calcular_valores_dia(x, fecha, df_historico_previo), axis=1
    #     )

    #     # Insertar o actualizar registros en la base de datos
    #     self.insertar_o_actualizar(df_resultado)

    # def _calcular_valores_dia(self, df_producto, fecha_str, df_historico_previo):
    #     almacen = df_producto.name[0]
    #     producto = df_producto.name[1]

    #     # Obtener el último registro de HistoricoCostoPromedio
    #     resultado_previo = df_historico_previo.loc[(almacen, producto), :]

    #     costoPromedioInicial, unidadesInicial = (
    #         resultado_previo["costoPromedioFinal", "unidadesFinal"]
    #         if not resultado_previo.empty
    #         else (0, 0)
    #     )

    #     # Calcular valores
    #     costoCompradia = (
    #         df_producto["flPrecioUnitario"].iloc[0] if not df_producto.empty else 0
    #     )
    #     unidadesCompradia = (
    #         df_producto["flCantidad"].iloc[0] if not df_producto.empty else 0
    #     )
    #     unidadesMovimientodia = self._get_unidades_movimientodia(
    #         almacen, producto, fecha_str
    #     )

    #     unidadesFinal = unidadesInicial + unidadesCompradia + unidadesMovimientodia

    #     costoPromedioFinal = (
    #         (
    #             (unidadesInicial + unidadesMovimientodia) * costoPromedioInicial
    #             + unidadesCompradia * costoCompradia
    #         )
    #         / unidadesFinal
    #         if unidadesFinal != 0
    #         else costoPromedioInicial
    #     )

    #     return pd.DataFrame(
    #         {
    #             "fechaHora": fecha_str,
    #             "almacen": almacen,
    #             "producto": producto,
    #             "costoPromedioInicial": costoPromedioInicial,
    #             "unidadesInicial": unidadesInicial,
    #             "costoCompradia": costoCompradia,
    #             "unidadesCompradia": unidadesCompradia,
    #             "unidadesMovimientodia": unidadesMovimientodia,
    #             "costoPromedioFinal": costoPromedioFinal,
    #             "unidadesFinal": unidadesFinal,
    #         },
    #         index=[(almacen, producto)],
    #     )

    # def _get_historico_previo(self, fecha_str):
    #     query = text(
    #         """
    #         SELECT almacen, producto, costoPromedioFinal, unidadesFinal
    #         FROM HistoricoCostoPromedio
    #         WHERE fechaHora < :fecha
    #         ORDER BY fechaHora DESC
    #         LIMIT 1;
    #         """
    #     )

    #     with self.engine_mysql_bi.connect() as connection:
    #         resultado = connection.execute(query, {"fecha": fecha_str}).fetchall()

    #     if resultado:
    #         df_historico_previo = pd.DataFrame(
    #             resultado,
    #             columns=["almacen", "producto", "costoPromedioFinal", "unidadesFinal"],
    #         ).set_index(["almacen", "producto"])
    #     else:
    #         df_historico_previo = pd.DataFrame()

    #     return df_historico_previo
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
            WHERE fechaHora < :fecha
            AND almacen = :almacen
            AND producto = :producto
            ORDER BY fechaHora DESC
            LIMIT 1;
            """
        )

        with self.engine_mysql_bi.connect() as connection:
            resultado = connection.execute(query, {"fecha": fecha_str, "almacen": almacen, "producto": producto}).fetchall()

        if resultado:
            df_historico_previo = pd.DataFrame(
            resultado,
            columns=["almacen", "producto", "costoPromedioFinal", "unidadesFinal"],
            ).set_index(["almacen", "producto"])
        else:
            df_historico_previo = pd.DataFrame()

        return df_historico_previo



# def procesar_datos_por_fecha(self, fecha):
#     fecha_str = fecha.strftime("%Y-%m-%d")
#     self.cargar_datos(fecha_str)
#     with self.engine_mysql_bi.connect() as connection:
#         # Asumiendo que necesitamos procesar cada combinación única de almacén y producto para la fecha
#         for almacen, df_almacen in self.df_compras.groupby("nbAlmacen"):
#             for producto, df_producto in df_almacen.groupby("nbProducto"):
#                 # Filtrar los otros movimientos
#                 df_otros_movimientos_filtrado = self.df_otros_movimientos[
#                     (self.df_otros_movimientos["nbAlmacen"] == almacen)
#                     & (self.df_otros_movimientos["nbProducto"] == producto)
#                 ]

#                 # Consultar el último registro previo
#                 query_previo = text(
#                     """
#                     SELECT costoPromedioFinal, unidadesFinal
#                     FROM HistoricoCostoPromedio
#                     WHERE almacen = :almacen AND producto = :producto
#                     AND fechaHora < :fecha
#                     ORDER BY fechaHora DESC
#                     LIMIT 1;
#                 """
#                 )
#                 resultado = connection.execute(
#                     query_previo,
#                     {"almacen": almacen, "producto": producto, "fecha": fecha_str},
#                 ).fetchone()

#                 if resultado:
#                     costoPromedioInicial, unidadesInicial = resultado
#                 else:
#                     # Si no hay registro previo, buscar en df_costos_iniciales
#                     df_costos_iniciales_filtrado = self.df_costos_iniciales[
#                         (self.df_costos_iniciales["nbAlmacen"] == almacen)
#                         & (self.df_costos_iniciales["nbProducto"] == producto)
#                     ]
#                     if not df_costos_iniciales_filtrado.empty:
#                         costoPromedioInicial = df_costos_iniciales_filtrado[
#                             "flPrecioUnitario"
#                         ].iloc[0]
#                         unidadesInicial = 0  # Asumir unidades iniciales en 0 si no hay registro previo
#                     else:
#                         costoPromedioInicial = 0
#                         unidadesInicial = 0

#                 # Calcular valores para el día
#                 costoCompradia = (
#                     df_producto["flPrecioUnitario"]
#                     if not df_producto.empty
#                     else 0
#                 )
#                 unidadesCompradia = (
#                     df_producto["flCantidad"] if not df_producto.empty else 0
#                 )
#                 unidadesMovimientodia = (
#                     df_otros_movimientos_filtrado["flCantidad"]
#                     if not df_otros_movimientos_filtrado.empty
#                     else 0
#                 )

#                 unidadesFinal = (
#                     unidadesInicial + unidadesCompradia + unidadesMovimientodia
#                 )
#                 if unidadesFinal != 0:
#                     costoPromedioFinal = (
#                         (
#                             (unidadesInicial + unidadesMovimientodia)
#                             * costoPromedioInicial
#                         )
#                         + (unidadesCompradia * costoCompradia)
#                     ) / unidadesFinal
#                 else:
#                     costoPromedioFinal = (
#                         costoPromedioInicial  # Evitar división por cero
#                     )

#                 # Insertar o actualizar el registro
#                 if resultado:
#                     # Actualizar el último registro
#                     update_query = text(
#                         """
#                         UPDATE HistoricoCostoPromedio
#                         SET costoPromedioFinal = :costoPromedioFinal, unidadesFinal = :unidadesFinal
#                         WHERE almacen = :almacen AND producto = :producto AND fechaHora = :fecha;
#                     """
#                     )
#                     connection.execute(
#                         update_query,
#                         {
#                             "costoPromedioFinal": costoPromedioFinal,
#                             "unidadesFinal": unidadesFinal,
#                             "almacen": almacen,
#                             "producto": producto,
#                             "fecha": fecha_str,
#                         },
#                     )
#                 else:
#                     # Insertar nuevo registro
#                     insert_query = text(
#                         """
#                         INSERT INTO HistoricoCostoPromedio (fechaHora, almacen, producto, costoPromedioInicial, unidadesInicial, costoCompradia, unidadesCompradia, unidadesMovimientodia, costoPromedioFinal, unidadesFinal)
#                         VALUES (:fecha, :almacen, :producto, :costoPromedioInicial, :unidadesInicial, :costoCompradia, :unidadesCompradia, :unidadesMovimientodia, :costoPromedioFinal, :unidadesFinal);
#                     """
#                     )
#                     connection.execute(
#                         insert_query,
#                         {
#                             "fecha": fecha_str,
#                             "almacen": almacen,
#                             "producto": producto,
#                             "costoPromedioInicial": costoPromedioInicial,
#                             "unidadesInicial": unidadesInicial,
#                             "costoCompradia": costoCompradia,
#                             "unidadesCompradia": unidadesCompradia,
#                             "unidadesMovimientodia": unidadesMovimientodia,
#                             "costoPromedioFinal": costoPromedioFinal,
#                             "unidadesFinal": unidadesFinal,
#                         },
#                     )


# def cargar_datos(self, fecha):
#     # Cargar las compras en un DataFrame
#     parametros = {'fecha': fecha}  # Diccionario de parámetros

#     sqlcompras = text("""
#         SELECT
#             m.dtContabilizacion,
#             m.nbAlmacen,
#             m.nbProducto,
#             SUM((m.flPrecioUnitario * m.flCantidad))/SUM(m.flCantidad) AS flPrecioUnitario,
#             SUM(m.flCantidad) AS flCantidad
#         FROM mmovlogistico m
#         WHERE m.nbMovimientoClase = '200' AND dtContabilizacion = :fecha
#         GROUP BY m.dtContabilizacion, m.nbAlmacen, m.nbProducto;
#         """)
#     self.df_compras = pd.read_sql(sqlcompras,
#         self.engine_mysql_bi,
#         params=parametros  # Pasar parámetros a la consulta
#     )

#     sqlmovimientos = text("""
#         SELECT
#             m.dtContabilizacion,
#             m.nbAlmacen,
#             m.nbProducto,
#             SUM(m.flCantidad) AS flCantidad
#         FROM mmovlogistico m
#         WHERE m.nbMovimientoClase != '200' AND dtContabilizacion = :fecha
#         GROUP BY m.dtContabilizacion, m.nbAlmacen, m.nbProducto;
#         """)
#     self.df_otros_movimientos = pd.read_sql(sqlmovimientos,
#         self.engine_mysql_bi,
#         params=parametros  # Pasar parámetros a la consulta
#     )
