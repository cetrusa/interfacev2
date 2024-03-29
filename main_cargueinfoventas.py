from scripts.extrae_bi.cargue_infoventas import CargueInfoVentas
from datetime import datetime

def main():
    # Definir los parámetros de configuración para el proceso de carga
    database_name = 'distrijass'  # Nombre de la base de datos a utilizar
    IdtReporteIni = '2023-01-01'  # Fecha de inicio del rango de reporte
    IdtReporteFin = '2023-12-31'  # Fecha de fin del rango de reporte
    
    # Crear una instancia de CargueInfoVentas con los parámetros especificados
    cargue_infoventas = CargueInfoVentas(database_name, IdtReporteIni, IdtReporteFin)
    
    # Ejecutar el proceso de carga de ventas
    cargue_infoventas.procesar_cargue_ventas()
    cargue_infoventas.marcar_registros_como_procesados()
    cargue_infoventas.eliminar_registros_procesados()

if __name__ == "__main__":
    main()
