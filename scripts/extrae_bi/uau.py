import win32com.client
import os
import time
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.utils import COMMASPACE
from email import encoders
import smtplib
import datetime
import markdown
import logging
from dateutil.relativedelta import relativedelta
from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
from sqlalchemy import create_engine, text


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


class CompiUpdate:
    def __init__(self, database_name):
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        self.file_path = None
        self.local_copy_path = None
        self.setup_logging()
        self.configurar(database_name)

    def configurar(self, database_name):
        try:
            self.config_basic = ConfigBasic(database_name)
            self.config = self.config_basic.config
            # config_basic.print_configuration()
            # print(self.config.get("txProcedureExtrae", []))
            self.db_connection = DataBaseConnection(config=self.config)
            self.engine_sqlite = self.db_connection.engine_sqlite
            self.engine_mysql_bi = self.db_connection.engine_mysql_bi
            self.engine_mysql_out = self.db_connection.engine_mysql_out
            # print("Configuraciones preliminares de actualización terminadas")
        except Exception as e:
            logging.error(f"Error al inicializar Actualización: {e}")
            raise

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            filename="process.log",
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

    def find_file(self, file_paths):
        for path in file_paths:
            if os.path.exists(path):
                logging.info(f"Archivo encontrado exitosamente en: {path}")
                return path
        logging.error("No se pudo encontrar el archivo.")
        return None

    def list_slicer_names(self):
        excel = win32com.client.Dispatch("Excel.Application")
        try:
            excel.Visible = True
            workbook = excel.Workbooks.Open(self.file_path)
            slicer_caches = workbook.SlicerCaches
            for slicer_cache in slicer_caches:
                print(f"Slicer Cache Name: {slicer_cache.Name}")
        except Exception as e:
            print(f"Error accessing SlicerCaches: {e}")
        finally:
            workbook.Close(SaveChanges=False)
            excel.Quit()

    def refresh_excel(self):
        logging.info("Inicia Proceso de actualización de Excel")
        self.file_path = self.find_file(
            [
                "G:\\OneDrive\\OneDrive - Asistencia Movil SAS\\Compi_bi.xlsx",
                "C:\\OneDrive - Asistencia Movil SAS\\Compi_bi.xlsx",
                "D:\\OneDrive - Asistencia Movil SAS\\Compi_bi.xlsx",
            ]
        )
        print(self.local_copy_path)
        if self.file_path:
            self.local_copy_path = (
                f"{self.file_path[0]}:\\Powerbi\\Compi\\Compi_bi_{self.timestamp}.xlsx"
            )
            print(self.local_copy_path)
            logging.info(f"La ruta para la copia local será: {self.local_copy_path}")
            self._open_and_process_excel()
        else:
            logging.error("Proceso terminado debido a la falta del archivo requerido.")

    def _open_and_process_excel(self):
        excel = win32com.client.Dispatch("Excel.Application")
        try:
            excel.Visible = True
            workbook = excel.Workbooks.Open(self.file_path)

            # Obtiene la fecha actual
            current_date = datetime.datetime.now()
            current_month_name = current_date.strftime("%Y%m")
            # Obtiene la fecha un mes atrás
            last_month_date = current_date - datetime.timedelta(
                days=10
            )  # puedes ajustar los días si es necesario
            last_month_name = last_month_date.strftime("%Y%m")

            # Verifica si el valor actual del slicer es el mismo que el mes actual
            slicer_cache = workbook.SlicerCaches("SegmentaciónDeDatos_Período")

            print(slicer_cache.VisibleSlicerItemsList)

            current_month_value = "[Calendario].[Período].&[" + current_month_name + "]"

            if slicer_cache.VisibleSlicerItemsList == [current_month_value]:
                print("El slicer ya está seleccionado en el mismo valor.")
                # Actualiza las conexiones sin ejecutar la macro completa
                for connection in workbook.Connections:
                    connection.Refresh()
            else:
                print("Seleccionando nuevo valor en el slicer...")
                # Limpia los filtros del slicer
                # slicer_cache.ClearAllFilters()
                # Establece el valor deseado en la lista de elementos visibles
                slicer_cache.VisibleSlicerItemsList = [current_month_value]
                # Actualiza las conexiones sin ejecutar la macro completa
                for connection in workbook.Connections:
                    connection.Refresh()

            # Lista para almacenar los valores de los periodos
            period_values = []

            # Genera los valores para el mes actual y los dos meses anteriores
            for i in range(3):
                month_date = current_date - relativedelta(months=i)
                month_name = month_date.strftime("%Y%m")
                period_values.append("[Calendario].[Período].&[" + month_name + "]")

            slicer_cache_clientes = workbook.SlicerCaches(
                "SegmentaciónDeDatos_Período1"
            )

            # Comprueba si el slicer ya está configurado con los valores correctos
            if set(slicer_cache_clientes.VisibleSlicerItemsList) == set(period_values):
                print("El slicer ya está seleccionado en los valores correctos.")
            else:
                print("Seleccionando nuevos valores en el slicer...")
                # Limpia los filtros del slicer si es necesario
                # slicer_cache_clientes.ClearAllFilters()

                # Establece los valores deseados en la lista de elementos visibles
                slicer_cache_clientes.VisibleSlicerItemsList = period_values

                # Actualiza las conexiones
                for connection in workbook.Connections:
                    connection.Refresh()

            time.sleep(60)

            # workbook.Save()

            # Espera un poco para asegurarse de que OneDrive sincronice los cambios
            time.sleep(10)  # Espera 2 segundos, puedes ajustar este valor

            # Guarda una copia del archivo
            workbook.SaveCopyAs(self.local_copy_path)
            workbook.Close(SaveChanges=False)
            # Abre la copia local
            copy_workbook = excel.Workbooks.Open(self.local_copy_path)

            # Elimina conexiones
            for connection in copy_workbook.Connections:
                connection.Delete()

            time.sleep(2)  # Espera 2 segundos, puedes ajustar este valor

            # copy_workbook.SlicerCaches("SegmentaciónDeDatos_Período").Delete()

            # Guarda y cierra la copia local
            copy_workbook.Save()
            copy_workbook.Close()

            # Cierra Excel
            excel.Quit()
            logging.info("Termina proceso Excel")
            # Espera un poco para asegurarse de que OneDrive sincronice los cambios
            time.sleep(2)  # Espera 2 segundos, puedes ajustar este valor

            self.send_email()

        except Exception as e:
            logging.error(f"Error during Excel processing: {e}")
        finally:
            workbook.Close(SaveChanges=False)
            excel.Quit()
            logging.info("Excel process completed.")
            

    def send_email(self):
        logging.info("Inicia envío de correos")
        # Indica que vas a usar las variables globales
        sql = text("SELECT * FROM powerbi_adm.conf_tipo WHERE nbTipo = '6';")
        # print(sql)
        df = self.config_basic.execute_sql_query(sql)
        # print(df)
        if not df.empty:
            # Corrige la asignación aquí
            self.config["nmUsrCorreo"] = df["nmUsr"].iloc[0]
            self.config["txPassCorreo"] = df["txPass"].iloc[0]
        else:
            # Considera si necesitas manejar el caso de un DataFrame vacío de manera diferente
            print("No se encontraron configuraciones de Correo.")
            

        host = "smtp.gmail.com"
        port = 587
        username = self.config["nmUsrCorreo"]
        password = self.config["txPassCorreo"]

        from_addr = "torredecontrolamovil@gmail.com"
        to_addr = [
            "lider.proyectos@amovil.co",
        ]
        bcc_addr = [
            "cesar.trujillo@amovil.co",
        ]
        with open("difusion.txt", "r") as file:
            cc_addr = [line.strip() for line in file]

        msg = MIMEMultipart()
        msg["From"] = from_addr
        msg["To"] = COMMASPACE.join(to_addr)
        msg["Cc"] = COMMASPACE.join(cc_addr)
        msg["Bcc"] = COMMASPACE.join(bcc_addr)
        msg["Subject"] = "Analitica Compi tienda"

        # logo_path = "logouau.png"

        # with open(logo_path, "rb") as logo_file:
        #     logo_data = logo_file.read()

        # logo_image = MIMEImage(logo_data)
        # logo_image.add_header("Content-ID", "<logo>")
        # msg.attach(logo_image)

        html_message = f"""
        <html>
            <head>
            <style>
            body {{ font-family: Arial; }}
            p {{ font-size: 12px; }}
            </style>
            </head>
            <body>
                <h3>Cordial Saludo,</h3>
                <p>Adjunto encontrará el seguimiento acumulado de Compi tienda, cuya información corresponde al mes actual y hace referencia a las cifras consolidadas en la aplicación.</p>
                <p>Mensaje generado de manera automática, por favor no responder.</p>
                <br>
                <img src="https://drive.google.com/uc?export=view&id=1nCnHpLIepkZ37MJPuAxcOMyeq0Bg2Iny" alt="Logo de la empresa">
            </body>
        </html>
        """

        msg.attach(MIMEText(html_message, "html"))

        # filename = self.local_copy_path.split("\\")[-1]
        # self.local_copy_path
        # filename = os.path.basename(self.local_copy_path)
        # Adjuntar el archivo
        with open(self.local_copy_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename= {self.local_copy_path}")

        msg.attach(part)

        # Intentar enviar el correo hasta tres veces en caso de error
        max_retries = 3
        exceptions = []  # Lista para almacenar excepciones
        sent_successfully = (
            False  # Variable para rastrear si el correo se envió con éxito
        )

        for _ in range(max_retries):
            try:
                with smtplib.SMTP(host, port) as server:
                    server.starttls()
                    server.login(username, password)
                    server.sendmail(from_addr, to_addr + cc_addr, msg.as_string())
                    logging.info("Proceso de envío de correo completado")
                    sent_successfully = True  # El correo se envió con éxito
                # Si el correo se envió con éxito, salir del bucle
                break
            except Exception as e:
                exceptions.append(e)  # Guardar la excepción en la lista
                logging.error(f"Error al enviar el correo: {e}")
                error_message = "Error en el proceso de envío de correo: " + str(e)
                logging.error(error_message)
                self.send_email_notification(error_message)

        if sent_successfully:
            # Elimina el archivo una vez que el correo se ha enviado con éxito
            try:
                os.remove(self.local_copy_path)
                print("Archivo eliminado exitosamente.")
                logging.info("Archivo eliminado exitosamente.")
            except Exception as e:
                logging.error(f"Error al eliminar el archivo: {e}")
                print(f"Error al eliminar el archivo: {e}")

    def send_email_notification(self, error_message):
        logging.info("Inicia envío de correos")
        # Indica que vas a usar las variables globales
        

        host = "smtp.gmail.com"
        port = 587
        username = "torredecontrolamovil@gmail.com"
        password = "dldaqtceiesyybje"
        from_addr = "torredecontrolamovil@gmail.com"
        to_addr = [
            "cesar.trujillo@amovil.co",
        ]
        with open("difusionerror.txt", "r") as file:
            cc_addr = [line.strip() for line in file]

        msg = MIMEMultipart()
        msg["From"] = from_addr
        msg["To"] = COMMASPACE.join(to_addr)
        msg["Cc"] = COMMASPACE.join(cc_addr)
        msg["Subject"] = f"Error Compi Tienda"

        html_message = f"""
        <html>
            <head>
            <style>
            body {{ font-family: Arial; }}
            p {{ font-size: 12px; }}
            </style>
            </head>
            <body>
                <h3>Verifica el log,</h3>
                <p>Adjunto encontrará el log de ejecución observe detalladamente cual proceso no se completo</p>
                <br>
            </body>
        </html>
        """

        msg.attach(MIMEText(html_message, "html"))

        filename = "log.txt"
        # Adjuntar el archivo
        with open(filename, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename= {filename}")

        msg.attach(part)

        # Intentar enviar el correo hasta tres veces en caso de error
        max_retries = 3

        for _ in range(max_retries):
            try:
                with smtplib.SMTP(host, port) as server:
                    server.starttls()
                    server.login(username, password)
                    server.sendmail(from_addr, to_addr + cc_addr, msg.as_string())
                # Si el correo se envió con éxito, salir del bucle
                break
            except Exception as e:
                logging.error(f"Error al enviar el correo: {e}")
                # Puedes agregar un retraso aquí si lo deseas antes de reintentar

        logging.info("Proceso de envío de correo completado")
