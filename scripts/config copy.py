from scripts.StaticPage import StaticPage
from scripts.conexion import Conexion as con
import json
from django.core.exceptions import ImproperlyConfigured
import pandas as pd
import ast
from sqlalchemy.sql import text

####################################################################
import logging
logging.basicConfig(filename="log.txt", level=logging.DEBUG,
                    format="%(asctime)s %(message)s", filemode="w")
####################################################################
logging.info('Inciando Proceso')

with open("secret.json") as f:
    secret = json.loads(f.read())

    def get_secret(secret_name, secrets=secret):
        try:
            return secrets[secret_name]
        except:
            msg = "la variable %s no existe" % secret_name
            raise ImproperlyConfigured(msg)

class ConfigBasic():
    try:
        StaticPage = StaticPage()
        def __init__(self,database_name):
            
            StaticPage.name = str(database_name)
            
            # StaticPage.name = 'altimax'
            StaticPage.dir_actual = str('puente1dia')
            StaticPage.nmDt = StaticPage.dir_actual
            logging.info(StaticPage.name)
            logging.info(StaticPage.nmDt)

            StaticPage.con = con.ConexionMariadb3(get_secret("DB_USERNAME"),get_secret("DB_PASS"),get_secret("DB_HOST"),int(get_secret("DB_PORT")),get_secret("DB_NAME"))
            
            with StaticPage.con.connect() as connectionout:
                StaticPage.cursor = connectionout.execution_options(isolation_level="READ COMMITTED")
                sql = text(f"SELECT * FROM powerbi_adm.conf_empresas WHERE name = '{StaticPage.name}';")
                result = pd.read_sql_query(sql=sql, con=StaticPage.cursor)
                df = pd.DataFrame(result)
                StaticPage.id= df['id'].values[0]
                #logging.info(id)
                StaticPage.nmEmpresa=df['nmEmpresa'].values[0]
                logging.info(StaticPage.nmEmpresa)
                StaticPage.name=df['name'].values[0]
                StaticPage.nbServerSidis=df['nbServerSidis'].values[0]
                StaticPage.dbSidis=df['dbSidis'].values[0]
                StaticPage.nbServerBi=df['nbServerBi'].values[0]
                StaticPage.dbBi=df['dbBi'].values[0]
                StaticPage.txProcedureExtrae=ast.literal_eval(df['txProcedureExtrae'].values[0])
                StaticPage.txProcedureCargue=ast.literal_eval(df['txProcedureCargue'].values[0])
                StaticPage.nmProcedureExcel=df['nmProcedureExcel'].values[0]
                StaticPage.txProcedureExcel=ast.literal_eval(df['txProcedureExcel'].values[0])
                StaticPage.nmProcedureInterface=df['nmProcedureInterface'].values[0]
                StaticPage.txProcedureInterface=ast.literal_eval(df['txProcedureInterface'].values[0])
                StaticPage.nmProcedureExcel2=df['nmProcedureExcel2'].values[0]
                StaticPage.txProcedureExcel2=ast.literal_eval(df['txProcedureExcel2'].values[0])
                StaticPage.nmProcedureCsv=df['nmProcedureCsv'].values[0]
                StaticPage.txProcedureCsv=ast.literal_eval(df['txProcedureCsv'].values[0])
                StaticPage.nmProcedureCsv2=df['nmProcedureCsv2'].values[0]
                StaticPage.txProcedureCsv2=ast.literal_eval(df['txProcedureCsv2'].values[0])
                StaticPage.nmProcedureSql=df['nmProcedureSql'].values[0]
                StaticPage.txProcedureSql=ast.literal_eval(df['txProcedureSql'].values[0])
                StaticPage.report_id_powerbi=df['report_id_powerbi'].values[0]
                StaticPage.dataset_id_powerbi=df['dataset_id_powerbi'].values[0]
                StaticPage.url_powerbi=df['url_powerbi'].values[0]
                
                # Procedimientos para la extracción
                
                # Iniciamos a definir las fechas
                sql = text(f"SELECT COUNT(*) FROM powerbi_adm.conf_dt WHERE nmDt = '{StaticPage.nmDt}';")
                result = pd.read_sql_query(sql=sql, con=StaticPage.cursor)
                cdf = pd.DataFrame(result)
                count=cdf['COUNT(*)'].values[0]
                if count == [1]:
                    sql = text(f"SELECT * FROM powerbi_adm.conf_dt WHERE nmDt = '{StaticPage.nmDt}';")
                    result = pd.read_sql_query(sql=sql, con=StaticPage.cursor)
                    df2 = pd.DataFrame(result)
                    StaticPage.txDtIni=str( df2['txDtIni'].values[0])
                    StaticPage.txDtFin=str( df2['txDtFin'].values[0])
                    sql = text(StaticPage.txDtIni)
                    result = pd.read_sql_query(sql=sql, con=StaticPage.cursor)
                    df3 = pd.DataFrame(result)
                    StaticPage.IdtReporteIni=df3['IdtReporteIni'].values[0]
                    sql = text(StaticPage.txDtFin)
                    result = pd.read_sql_query(sql=sql, con=StaticPage.cursor)
                    df4 = pd.DataFrame(result)
                    StaticPage.IdtReporteFin=df4['IdtReporteFin'].values[0]
                else:
                    sql = text("SELECT * FROM powerbi_adm.conf_dt WHERE nmDt = 'puente1dia';")
                    result = pd.read_sql_query(sql=sql, con=StaticPage.cursor)
                    df2 = pd.DataFrame(result)
                    StaticPage.txDtIni=str( df2['txDtIni'].values[0])
                    StaticPage.txDtFin=str( df2['txDtFin'].values[0])
                    sql = text(StaticPage.txDtIni)
                    result = pd.read_sql_query(sql=sql, con=StaticPage.cursor)
                    df3 = pd.DataFrame(result)
                    StaticPage.IdtReporteIni=df3['IdtReporteIni'].values[0]
                    sql = text(StaticPage.txDtFin)
                    result = pd.read_sql_query(sql=sql, con=StaticPage.cursor)
                    df4 = pd.DataFrame(result)
                    StaticPage.IdtReporteFin=df4['IdtReporteFin'].values[0]
                # Prepraramos datos de conexión
                sql = text(f"SELECT * FROM powerbi_adm.conf_server WHERE nbServer = '{StaticPage.nbServerSidis}';")
                result = pd.read_sql_query(sql=sql, con=StaticPage.cursor)
                df5 = pd.DataFrame(result)
                StaticPage.hostServerOut=str(df5['hostServer'].values[0])
                StaticPage.portServerOut=int(df5['portServer'].values[0])
                StaticPage.nbTipo=df5['nbTipo'].values[0]
                sql = text(f"SELECT * FROM powerbi_adm.conf_tipo WHERE nbTipo = '{StaticPage.nbTipo}';")
                result = pd.read_sql_query(sql=sql, con=StaticPage.cursor)
                df6 = pd.DataFrame(result)
                StaticPage.nmUsrOut=str(df6['nmUsr'].values[0])
                StaticPage.txPassOut=str(df6['txPass'].values[0])
                sql = text(f"SELECT * FROM powerbi_adm.conf_server WHERE nbServer = '{StaticPage.nbServerBi}' ;")
                result = pd.read_sql_query(sql=sql, con=StaticPage.cursor)
                df7 = pd.DataFrame(result)
                StaticPage.hostServerIn=str(df7['hostServer'].values[0])
                StaticPage.portServerIn=int(df7['portServer'].values[0])
                StaticPage.nbTipoBi=df7['nbTipo'].values[0]
                sql = text(f"SELECT * FROM powerbi_adm.conf_tipo WHERE nbTipo = '{StaticPage.nbTipoBi}'")
                result = pd.read_sql_query(sql=sql, con=StaticPage.cursor)
                df8 = pd.DataFrame(result)
                StaticPage.nmUsrIn=str(df8['nmUsr'].values[0])
                StaticPage.txPassIn=str(df8['txPass'].values[0])
                sql = text(f"SELECT * FROM powerbi_adm.conf_tipo WHERE nbTipo = '3';")
                result = pd.read_sql_query(sql=sql, con=StaticPage.cursor)
                df9 = pd.DataFrame(result)
                StaticPage.nmUsrPowerbi=str(df9['nmUsr'].values[0])
                StaticPage.txPassPowerbi=str(df9['txPass'].values[0])
                StaticPage.conin = con.ConexionMariadb3(user=StaticPage.nmUsrIn,password=StaticPage.txPassIn,host=StaticPage.hostServerIn,port=StaticPage.portServerIn,database=StaticPage.dbBi)
                StaticPage.conin2 = con.ConexionMariadb3(user=StaticPage.nmUsrIn,password=StaticPage.txPassIn,host=StaticPage.hostServerIn,port=StaticPage.portServerIn,database=StaticPage.dbBi)
                StaticPage.conout = con.ConexionMariadb3(user=StaticPage.nmUsrOut,password=StaticPage.txPassOut,host=StaticPage.hostServerOut,port=StaticPage.portServerOut,database=StaticPage.dbSidis)
                StaticPage.conin3 = con.ConexionMariadb3(user=StaticPage.nmUsrIn,password=StaticPage.txPassIn,host=StaticPage.hostServerIn,port=StaticPage.portServerIn,database=StaticPage.dbBi)
            
    except Exception as e:
        print(logging.error(e))
