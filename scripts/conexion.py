import sys
import sqlalchemy
import pymysql

class Conexion:

    def ConexionMariadb3(user,password,host,port,database):
        # Conectar con la Plataforma Mariadb
        try:
            connect_args = {}
            pool = sqlalchemy.create_engine(
            # Equivalent URL:
            # mariadb+mariadbconnector://<db_user>:<db_pass>@<db_host>:<db_port>/<db_name>
            # drivername="postgresql+pg8000", # en caso de requerir para postgresql
            sqlalchemy.engine.url.URL.create(
                #drivername="mariadb+mariadbconnector",
                # drivername="mariadb+pymysql",
                drivername="mysql+pymysql",
                username=user,
                password=password,
                host=host,
                port=port,
                database=database,
            ),
            connect_args=connect_args,
            # Pool size is the maximum number of permanent connections to keep.
            #pool_size=5,
            # Temporarily exceeds the set pool_size if no connections are available.
            #max_overflow=2,
            # The total number of concurrent connections for your application will be
            # a total of pool_size and max_overflow.
            # SQLAlchemy automatically uses delays between failed connection attempts,
            # but provides no arguments for configuration.
            # 'pool_timeout' is the maximum number of seconds to wait when retrieving a
            # new connection from the pool. After the specified amount of time, an
            # exception will be thrown.
            pool_timeout=28800,  # 900 seconds (15 minutos)
            # 'pool_recycle' is the maximum number of seconds a connection can persist.
            # Connections that live longer than the specified amount of time will be
            # re-established
            pool_recycle=28800,  # 1 hora
            )
            
        except Exception as e:
            print(f"Error al conectar con la Plataforma Mariadb: {e}")
            sys.exit(1)
        return pool