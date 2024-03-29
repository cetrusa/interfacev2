from datetime import datetime
import time

import zipfile
import os
import numpy as np
import pandas as pd
import logging

# from scripts.conexion import Conexion as con
from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
from sqlalchemy import text, inspect
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from django.contrib import sessions
import re
import ast
from django.core.exceptions import ImproperlyConfigured
import json
import unicodedata

import sqlite3
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, Float, String, Date, ForeignKey
from sqlalchemy.dialects.mysql import DOUBLE, VARCHAR, BIT
from sqlalchemy.orm import relationship
from sqlalchemy import update
from sqlalchemy import bindparam, tuple_
from sqlalchemy import and_

logging.basicConfig(
    filename="cargueinfoventas.txt",
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


Base = declarative_base()


class FactFacturas(Base):
    __tablename__ = "fact_facturas"
    empresa = Column(String(30), primary_key=True)
    cliente_id = Column(String(30), primary_key=True)
    zona_id = Column(String(30), primary_key=True)
    fecha_factura = Column(Date, nullable=False)
    factura_id = Column(String(30), nullable=False, unique=True)
    vlrbruto = Column(Float)
    planilla_id = Column(String(30))
    pedido_id = Column(String(30))
    ordcompra_id = Column(String(30))
    # Definir índices si es necesario
    # relationships si es necesario


class FactFacturasItems(Base):
    __tablename__ = "fact_facturas_items"
    empresa = Column(String(30), primary_key=True)
    factura_id = Column(
        String(30), ForeignKey("fact_facturas.factura_id"), primary_key=True
    )
    producto_id = Column(String(10), primary_key=True)
    nro_linea = Column(String(10), primary_key=True)
    bodega_id = Column(String(10), primary_key=True)
    tplinea_id = Column(String(1), primary_key=True)
    virtual_id = Column(String(10))
    cant = Column(Float, default=0)
    costo = Column(Float, default=0)
    vlrbruto = Column(Float, default=0)
    # relationships si es necesario


class FactNotasCredito(Base):
    __tablename__ = "fact_notas_credito"
    empresa = Column(String(30), primary_key=True)
    cliente_id = Column(String(30), primary_key=True)
    zona_id = Column(String(30), primary_key=True)
    fecha_factura = Column(Date, nullable=False)
    factura_id = Column(String(30), primary_key=True, nullable=False, unique=True)
    vlrbruto = Column(Float)
    planilla_id = Column(String(30))
    pedido_id = Column(String(30))
    ordcompra_id = Column(String(30))
    # Definir índices si es necesario
    # relationships si es necesario


class FactNotasCreditoItems(Base):
    __tablename__ = "fact_notas_credito_items"
    empresa = Column(String(30), primary_key=True)
    factura_id = Column(
        String(30), ForeignKey("fact_notas_credito.factura_id"), primary_key=True
    )
    producto_id = Column(String(10), primary_key=True)
    nro_linea = Column(String(10), primary_key=True)
    bodega_id = Column(String(10), primary_key=True)
    tplinea_id = Column(String(1), primary_key=True)
    virtual_id = Column(String(10))
    cant = Column(Float, default=0)
    costo = Column(Float, default=0)
    vlrbruto = Column(Float, default=0)
    # relationships si es necesario


class TmpInfoVentas(Base):
    __tablename__ = "tmp_infoventas"
    Cod_cliente = Column("Cod. cliente", VARCHAR(30), primary_key=True, nullable=False)
    Cod_vendedor = Column(
        "Cod. vendedor", VARCHAR(30), primary_key=True, nullable=False
    )
    Cod_producto = Column(
        "Cod. productto", VARCHAR(30), primary_key=True, nullable=False
    )
    Fecha = Column("Fecha", Date)
    Fac_numero = Column("Fac. numero", VARCHAR(30), primary_key=True, nullable=False)
    Cantidad = Column("Cantidad", Integer)
    Vta_neta = Column("Vta neta", DOUBLE(18, 2))
    Tipo = Column("Tipo", VARCHAR(30), primary_key=True, nullable=False)
    Costo = Column("Costo", DOUBLE(18, 2))
    Unidad = Column("Unidad", VARCHAR(30))
    Pedido = Column("Pedido", VARCHAR(30))
    Codigo_bodega = Column("Codigo bodega", VARCHAR(30))
    nbLinea = Column(
        "nbLinea", VARCHAR(5), primary_key=True, nullable=False, default="1"
    )
    procesado = Column("procesado", BIT(1), default=b"0")


class CargueInfoVentas:
    """
    Clase para manejar el proceso de carga de historico de costos.

    Esta clase se encargará de procesar los archivos planos, realizar limpieza y validaciones
    necesarias, y cargar los datos en la base de datos.
    """

    def __init__(self, database_name, IdtReporteIni, IdtReporteFin):
        """
        Inicializa la instancia de CargueHistoricoCostos.

        Args:
            database_name (str): Nombre de la base de datos.

        Raises:
            ValueError: Si el nombre de la base de datos es vacío o nulo.
        """

        if not database_name:
            raise ValueError("El nombre de la base de datos no puede ser vacío o nulo.")

        self.database_name = database_name
        self.IdtReporteIni = IdtReporteIni
        self.IdtReporteFin = IdtReporteFin
        self.configurar()
        self.empresa = self.obtener_id_empresa()

    def configurar(self):
        """
        Configura la conexión a las bases de datos y establece las variables de entorno necesarias.

        Esta función crea una configuración básica y establece las conexiones a las bases de datos MySQL y SQLite
        utilizando los parámetros de configuración.

        Raises:
            Exception: Propaga cualquier excepción que ocurra durante la configuración.
        """

        try:
            # Crear un objeto de configuración básica
            config_basic = ConfigBasic(self.database_name)
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

    def obtener_id_empresa(self):
        """
        Aqui hallamos el id de la empresa

        """
        with self.engine_mysql_bi.connect() as connection:
            result = connection.execute(text("SELECT id FROM dim_empresa LIMIT 1;"))
            empresa_id = result.scalar()  # Obtiene el primer valor de la primera fila
            return empresa_id

    def preparar_datos_temporales(self, empresa, IdtReporteIni, IdtReporteFin):
        """
        Prepara datos temporales para facturas y notas, agrupando información relevante
        de la tabla 'tmp_infoventas' para el rango de fechas proporcionado.
        """
        parametros = {
            "empresa": empresa,
            "IdtReporteIni": IdtReporteIni,
            "IdtReporteFin": IdtReporteFin,
        }

        sqltemporal = text(
            """
            SELECT 
                :empresa AS empresa, 
                t.`Cod. cliente` AS cliente_id, 
                t.`Cod. vendedor` AS zona_id, 
                t.Fecha AS fecha_factura, 
                t.`Fac. numero` AS factura_id, 
                SUM(t.`Vta neta`) AS vlrbruto, 
                '' AS planilla_id,
                t.`Pedido` AS pedido_id,
                '' AS ordcompra_id,
                t.`Tipo`
            FROM tmp_infoventas t
            WHERE t.Fecha BETWEEN :IdtReporteIni AND :IdtReporteFin
            GROUP BY 
                t.`Cod. cliente`, 
                t.`Cod. vendedor`, 
                t.Fecha, 
                t.`Fac. numero`, 
                t.`Pedido`,
                t.`Tipo`;
            """
        )

        df_temporal = pd.read_sql(sqltemporal, self.engine_mysql_bi, params=parametros)

        if df_temporal.empty:
            df_temporal = pd.DataFrame(
                columns=[
                    "empresa",
                    "cliente_id",
                    "zona_id",
                    "fecha_factura",
                    "factura_id",
                    "vlrbruto",
                    "planilla_id",
                    "pedido_id",
                    "ordcompra_id",
                    "Tipo",
                ]
            ).astype(
                {
                    "empresa": "object",
                    "cliente_id": "object",
                    "zona_id": "object",
                    "fecha_factura": "datetime64[ns]",
                    "factura_id": "object",
                    "vlrbruto": "float",
                    "planilla_id": "object",
                    "pedido_id": "object",
                    "ordcompra_id": "object",
                    "Tipo": "object",
                }
            )
        return df_temporal

    def preparar_datos_items_temporales(self, empresa, IdtReporteIni, IdtReporteFin):
        """
        Prepara datos temporales para items de facturas y notas, extrayendo información relevante
        de la tabla 'tmp_infoventas' para el rango de fechas proporcionado.
        """
        parametros = {
            "empresa": empresa,
            "IdtReporteIni": IdtReporteIni,
            "IdtReporteFin": IdtReporteFin,
        }

        sqltemporal = text(
            """
            SELECT 
                :empresa AS empresa, 
                t.`Fac. numero` AS factura_id,  
                t.`Cod. productto` AS producto_id, 
                t.`nbLinea` AS nro_linea, 
                t.`Codigo bodega` AS bodega_id, 
                t.`Tipo`as tplinea_id, 
                NULL AS virtual_id,  -- Asumiendo que no hay campo equivalente
                t.`Cantidad` AS cant, 
                t.`Costo` AS costo, 
                t.`Vta neta` AS vlrbruto
            FROM tmp_infoventas t 
            WHERE t.Fecha BETWEEN :IdtReporteIni AND :IdtReporteFin;
            """
        )

        df_temporal_items = pd.read_sql(
            sqltemporal, self.engine_mysql_bi, params=parametros
        )

        if df_temporal_items.empty:
            df_temporal_items = pd.DataFrame(
                columns=[
                    "empresa",
                    "factura_id",
                    "producto_id",
                    "nro_linea",
                    "bodega_id",
                    "tplinea_id",
                    "virtual_id",  # Aunque es siempre NULL, lo incluimos para mantener la estructura
                    "cant",
                    "costo",
                    "vlrbruto",
                ]
            ).astype(
                {
                    "empresa": "object",
                    "factura_id": "object",
                    "producto_id": "object",
                    "nro_linea": "object",
                    "bodega_id": "object",
                    "tplinea_id": "object",
                    "virtual_id": "object",  # Aunque siempre es NULL, especificamos el tipo por consistencia
                    "cant": "float",
                    "costo": "float",
                    "vlrbruto": "float",
                }
            )
        return df_temporal_items

    def insertar_registros_ignore(self, modelo, data_to_insert):
        inicio = time.time()
        Session = sessionmaker(bind=self.engine_mysql_bi)
        session = Session()
        try:
            # Filtrar nuevos registros
            nuevos_registros = self.filtrar_nuevos_registros(
                modelo, data_to_insert, session
            )
            # print(nuevos_registros)
            # logging.debug(f"Datos a insertar: {nuevos_registros}")

            # Usa bulk_insert_mappings correctamente
            session.bulk_insert_mappings(modelo, nuevos_registros)
            session.commit()
            print(
                f"{len(nuevos_registros)} registros insertados en {modelo.__tablename__}."
            )
        except IntegrityError as e:
            session.rollback()
            print(
                f"Error de integridad al insertar registros en {modelo.__tablename__}: {e}"
            )
        except SQLAlchemyError as e:
            session.rollback()
            print(
                f"Error de SQLAlchemy al insertar registros en {modelo.__tablename__}: {e}"
            )
        finally:
            fin = time.time()
            tiempo_transcurrido = fin - inicio
            print(f"Tiempo transcurrido en insertar_registros_ignore: {tiempo_transcurrido} segundos.")
            session.close()

    # def filtrar_nuevos_registros(self, modelo, data_to_insert, session):
    #     claves_unicas = [
    #         columna.name
    #         for columna in modelo.__table__.columns
    #         if columna.primary_key or columna.unique
    #     ]
    #     todos_nuevos_registros = []  # Acumula nuevos registros de todos los batches

    #     for batch in self.chunk_data(data_to_insert, batch_size=50000):
    #         # Asegúrate de que cada clave exista en cada registro del batch
    #         # Esto es crítico para evitar KeyError
    #         batch = [
    #             record
    #             for record in batch
    #             if all(key in record for key in claves_unicas)
    #         ]

    #         if (
    #             not batch
    #         ):  # Si el batch está vacío después de filtrar, continúa con el siguiente
    #             continue

    #         # Consulta para encontrar registros existentes
    #         existing_records_query = (
    #             session.query(modelo)
    #             .filter(
    #                 tuple_(*[getattr(modelo, key) for key in claves_unicas]).in_(
    #                     [
    #                         tuple(record[key] for key in claves_unicas)
    #                         for record in batch
    #                     ]
    #                 )
    #             )
    #             .all()
    #         )
    #         existing_records = {
    #             self.construct_key(record, claves_unicas)
    #             for record in existing_records_query
    #         }

    #         # Filtra los registros nuevos
    #         nuevos_registros = [
    #             record
    #             for record in batch
    #             if self.construct_key(record, claves_unicas) not in existing_records
    #         ]
    #         todos_nuevos_registros.extend(nuevos_registros)

    #     return todos_nuevos_registros
    
        

    def filtrar_nuevos_registros(self, modelo, data_to_insert, session):
        # Obtener las claves únicas del modelo
        inicio = time.time()  # Inicio de la medición de tiempo
        claves_unicas = self.obtener_claves_unicas(modelo)
        todos_nuevos_registros = []

        for batch in self.chunk_data(data_to_insert, batch_size=50000):
            # Asegurar que el batch solo contiene registros con todas las claves únicas
            batch = [record for record in batch if all(key in record for key in claves_unicas)]
            if not batch:
                continue

            # Preparar la lista de tuplas de claves únicas para la consulta de existencia
            keys_to_check = [self.construct_key(record, claves_unicas) for record in batch]

            # Consulta para encontrar registros existentes basados en claves únicas
            existing_records_query = session.query(modelo).filter(
                tuple_(*[getattr(modelo, key) for key in claves_unicas]).in_(keys_to_check)
            ).all()

            # Crear un conjunto de claves únicas de los registros existentes
            existing_records_keys = {self.construct_key(record, claves_unicas) for record in existing_records_query}

            # Filtrar fuera los registros nuevos comparando las claves
            nuevos_registros = [
                record for record in batch
                if self.construct_key(record, claves_unicas) not in existing_records_keys
            ]

            todos_nuevos_registros.extend(nuevos_registros)
        final = time.time()  # Final de la medición de tiempo
        tiempo_transcurrido = final - inicio
        print(f"Tiempo transcurrido en filtrar nuevos registros: {tiempo_transcurrido} segundos.")
        return todos_nuevos_registros

    def obtener_claves_unicas(self,modelo):
        """Obtiene las claves únicas y primarias de un modelo SQLAlchemy."""
        return [
            columna.name
            for columna in modelo.__table__.columns
            if columna.primary_key or columna.unique
        ]
        
    def construct_key(self, record, key_columns):
        """
        Construye una clave única para un registro.

        Args:
            record (dict or object): El registro del que se construirá la clave.
            key_columns (list of str): Nombres de las columnas o atributos que formarán la clave.

        Returns:
            tuple: Una tupla que representa la clave única del registro.

        Raises:
            ValueError: Si `record` está vacío o `key_columns` no contiene elementos.
        """
        if not record:
            raise ValueError("El registro no puede estar vacío.")
        if not key_columns:
            raise ValueError("Debe proporcionar al menos una columna clave.")

        if isinstance(record, dict):
            return tuple(record.get(key) for key in key_columns)
        else:
            return tuple(getattr(record, key, None) for key in key_columns)


    def chunk_data(self, data, batch_size):
        # Utility method to yield chunks of data
        for i in range(0, len(data), batch_size):
            yield data[i : i + batch_size]

    def marcar_registros_como_procesados(self):
        inicio = time.time()  # Inicio de la medición de tiempo
        Session = sessionmaker(bind=self.engine_mysql_bi)
        session = Session()
        try:
            # Paso 1: Recuperar y combinar identificadores de registros
            facturas_items_ids = {tuple(row) for row in session.query(
                FactFacturasItems.factura_id,
                FactFacturasItems.producto_id,
                FactFacturasItems.tplinea_id,
                FactFacturasItems.nro_linea,
            ).all()}
            notas_credito_items_ids = {tuple(row) for row in session.query(
                FactNotasCreditoItems.factura_id,
                FactNotasCreditoItems.producto_id,
                FactNotasCreditoItems.tplinea_id,
                FactNotasCreditoItems.nro_linea,
            ).all()}
            combined_ids = facturas_items_ids.union(notas_credito_items_ids)

            # Paso 2: Preparar una sola consulta de actualización
            if combined_ids:
                # SQL Alchemy soporta estructuras de tupla para in_, se puede adaptar según sea necesario
                session.query(TmpInfoVentas).filter(
                    tuple_(TmpInfoVentas.Fac_numero, TmpInfoVentas.Cod_producto, TmpInfoVentas.Tipo, TmpInfoVentas.nbLinea).in_(combined_ids)
                ).update({"procesado": 1}, synchronize_session='fetch')

            session.commit()
        except Exception as e:
            session.rollback()
            logging.error(f"Error al marcar registros como procesados: {e}")
        finally:
            session.close()
            final = time.time()  # Final de la medición de tiempo
            tiempo_transcurrido = final - inicio
            print(f"Tiempo transcurrido en marcar registros procesados: {tiempo_transcurrido} segundos.")

    def eliminar_registros_procesados(self):
        
        """
        Elimina los registros marcados como procesados en la tabla 'tmp_infoventas'.
        """
        inicio = time.time()
        # Uso de gestor de contexto para manejar la sesión
        Session = sessionmaker(bind=self.engine_mysql_bi)
        with Session() as session:
            try:
                # Ejecutar la operación de eliminación
                session.query(TmpInfoVentas).filter(TmpInfoVentas.procesado == 1).delete(synchronize_session='fetch')

                # Comprometer los cambios
                session.commit()
                logging.info("Registros procesados eliminados exitosamente de 'tmp_infoventas'.")
            except Exception as e:
                # Revertir los cambios en caso de error
                session.rollback()
                logging.error(f"Error al eliminar registros procesados: {e}")
        final = time.time()  # Final de la medición de tiempo
        tiempo_transcurrido = final - inicio
        print(f"Tiempo transcurrido en eliminar registros procesados: {tiempo_transcurrido} segundos.")

    def procesar_cargue_ventas(self):
        inicio = time.time()
        empresa = self.obtener_id_empresa()

        for fecha in pd.date_range(start=self.IdtReporteIni, end=self.IdtReporteFin):
            fecha_str = fecha.strftime("%Y-%m-%d")
            df_temporal = self.preparar_datos_temporales(empresa, fecha_str, fecha_str)

            if df_temporal.empty:
                print(
                    f"No hay datos para procesar en la fecha {fecha_str}. Continuando con la siguiente fecha."
                )
                continue  # Salta al siguiente ciclo del bucle si df_temporal está vacío

            # El resto del proceso como filtrado de df_facturas, df_notas_credito, etc.

            df_facturas = df_temporal[df_temporal["Tipo"] == "0"].copy()
            # print(df_facturas)

            df_notas_credito = df_temporal[df_temporal["Tipo"] == "1"].copy()
            # print(df_notas_credito)

            # Asegúrate de eliminar la columna 'Tipo' si ya no es necesaria
            df_facturas.drop(columns=["Tipo"], inplace=True)
            df_notas_credito.drop(columns=["Tipo"], inplace=True)
            # print(df_facturas)
            # print(df_facturas.to_dict("records"))
            # print(df_notas_credito)
            # Continúa con la inserción solo si los DataFrames no están vacíos
            if not df_facturas.empty:
                self.insertar_registros_ignore(
                    FactFacturas, df_facturas.to_dict("records")
                )
            if not df_notas_credito.empty:
                self.insertar_registros_ignore(
                    FactNotasCredito, df_notas_credito.to_dict("records")
                )

            # Repite el proceso similar para df_facturas_items y df_notas_credito_items
            df_temporal_items = self.preparar_datos_items_temporales(
                empresa, fecha_str, fecha_str
            )

            df_facturas_items = df_temporal_items[
                df_temporal_items["tplinea_id"] == "0"
            ].copy()
            if not df_facturas_items.empty:
                self.insertar_registros_ignore(
                    FactFacturasItems, df_facturas_items.to_dict("records")
                )
            df_notas_credito_items = df_temporal_items[
                df_temporal_items["tplinea_id"] == "1"
            ].copy()
            if not df_notas_credito_items.empty:
                self.insertar_registros_ignore(
                    FactNotasCreditoItems, df_notas_credito_items.to_dict("records")
                )

            # Procesa los registros marcados y elimina los procesados después de cada fecha
            
            final = time.time()  # Final de la medición de tiempo
            tiempo_transcurrido = final - inicio
            print(f"Tiempo transcurrido en proceso cargue de ventas: {tiempo_transcurrido} segundos.")

        """
        Este método es responsable de procesar y cargar datos relacionados con ventas y notas de crédito desde
        una fuente de datos temporal hacia las tablas definitivas en la base de datos. El proceso garantiza que
        solo se inserten registros nuevos, omitiendo los ya existentes para evitar duplicados.

        Proceso:
        1. Obtiene el ID de la empresa para la cual se está realizando el proceso de carga.
        Este ID se utiliza para asociar todos los registros de ventas y notas de crédito.

        2. Prepara los datos temporales para facturas y notas de crédito a través del método
        `preparar_datos_temporales`. Este paso implica filtrar y agrupar la información relevante
        de la tabla 'tmp_infoventas' para el rango de fechas especificado por `IdtReporteIni` y `IdtReporteFin`.

        3. Separa los datos temporales en dos DataFrames distintos, uno para facturas (`df_facturas`) y otro
        para notas de crédito (`df_notas_credito`), utilizando el campo 'Tipo' como criterio de filtrado.

        4. Utiliza el método `insertar_registros_ignore` para insertar los registros filtrados en las tablas
        `FactFacturas` y `FactNotasCredito`, respectivamente. Este paso asegura que solo se añadan
        registros nuevos, ignorando aquellos que ya existan en la base de datos.

        5. Prepara los datos temporales para los ítems de facturas y notas de crédito mediante el método
        `preparar_datos_items_temporales`, siguiendo un proceso similar al de las facturas y notas de crédito.

        6. Separa los datos de ítems en dos DataFrames adicionales, uno para ítems de facturas (`df_facturas_items`)
        y otro para ítems de notas de crédito (`df_notas_credito_items`), usando el mismo criterio basado
        en el campo 'Tipo'.

        7. Finalmente, inserta los registros de ítems filtrados en las tablas `FactFacturasItems` y
        `FactNotasCreditoItems`, utilizando el mismo método `insertar_registros_ignore` para evitar
        la inserción de registros duplicados.

        Importante:
        - Este método asume la existencia de una columna 'Tipo' en los DataFrames resultantes que distingue
        entre facturas (valor '0') y notas de crédito (valor '1').
        - El método `insertar_registros_ignore` se encarga de filtrar y añadir solo los registros nuevos,
        basándose en las claves únicas definidas para cada modelo de tabla, para mantener la integridad de los datos.
        """
