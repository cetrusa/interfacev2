from django.db import models

# Create your models here.
class PermisosBarra(models.Model):
    class Meta:
        managed = False
        permissions = (
            ('nav_bar', 'Ver la barra de menú'),
            ('cubo', 'Generar cubo de ventas'),
            ('interface', 'Generar interface contable'),
            ('plano', 'Generar archivo plano'),
            ('informe_bi', 'Informe Bi'),
            ('actualizar_base', 'Actualzación de datos'),
            ('actualizacion_bi', 'Actualizar Bi'),
            ('admin', 'Ir al Administrador'),
        )
        
class ConfDt(models.Model):
    nbDt = models.BigIntegerField(primary_key=True,verbose_name='Id')
    nmDt = models.CharField(max_length=100, null=True, blank=True,verbose_name='Nombre Rango de Fecha')
    txDtIni = models.TextField(null=True, blank=True, verbose_name='Fecha Inicial')
    txDtFin = models.TextField(null=True, blank=True,verbose_name='Fecha Final')

    class Meta:
        db_table = 'conf_dt'
        # managed = False
        verbose_name = 'Configuración Rango de Fecha'
        verbose_name_plural = 'Configuración Rangos de Fechas'

class ConfEmpresas(models.Model):
    id = models.BigIntegerField(primary_key=True,verbose_name='id')
    nmEmpresa = models.CharField(max_length=100, null=True, blank=True, verbose_name='Nombre Empresa')
    name = models.CharField(max_length=100, null=True, blank=True,verbose_name='Nombre de la Base')
    nbServerSidis = models.BigIntegerField(null=True, blank=True,verbose_name='Id Servidor Sidis')
    dbSidis = models.CharField(max_length=150, null=True, blank=True, verbose_name='Base de datos Sidis')
    nbServerBi = models.BigIntegerField(null=True, blank=True,verbose_name='Id Servidor PowerBi')
    dbBi = models.CharField(max_length=150, null=True, blank=True,verbose_name='Base de datos Bi')
    txProcedureExtrae = models.CharField(max_length=100, null=True, blank=True,verbose_name='Procesos sql Extractor')
    txProcedureCargue = models.TextField(null=True, blank=True,verbose_name='Procesos sql del Cargue')
    nmProcedureInterface = models.CharField(max_length=30, null=True, blank=True,verbose_name='Procedimiento Interface')
    txProcedureInterface = models.TextField(null=True, blank=True,verbose_name='Procesos sql de Interface')
    nmProcedureExcel = models.CharField(max_length=30, null=True, blank=True,verbose_name='Procedimiento a Excel')
    txProcedureExcel = models.TextField(null=True, blank=True,verbose_name='Procesos sql a Excel')
    nmProcedureExcel2 = models.CharField(max_length=30, null=True, blank=True,verbose_name='Procedimiento a Excel2')
    txProcedureExcel2 = models.TextField(null=True, blank=True,verbose_name='Procesos sql a Excel2')
    nmProcedureCsv = models.CharField(max_length=30, null=True, blank=True,verbose_name='Procedimiento a Csv')
    txProcedureCsv = models.TextField(null=True, blank=True,verbose_name='Procesos sql a Csv')
    nmProcedureCsv2 = models.CharField(max_length=30, null=True, blank=True,verbose_name='Procedimiento a Csv2')
    txProcedureCsv2 = models.TextField(null=True, blank=True,verbose_name='Procesos sql a Csv2')
    nmProcedureSql = models.CharField(max_length=30, null=True, blank=True,verbose_name='Procedimiento a Sql')
    txProcedureSql = models.TextField(null=True, blank=True,verbose_name='Procesos sql a Sql')
    report_id_powerbi = models.CharField(max_length=255, null=True, blank=True,verbose_name='Id Reporte PowerBi')
    dataset_id_powerbi = models.CharField(max_length=255, null=True, blank=True,verbose_name='Dataset PowerBi')
    url_powerbi = models.TextField(null=True, blank=True,verbose_name='Url Pública PowerBi')
    estado = models.IntegerField(null=True, blank=True,verbose_name='Activo')

    def __str__(self):
        return f'{self.id}-{self.nmEmpresa}'
    
    class Meta:
        db_table = 'conf_empresas'
        # managed = False
        verbose_name = 'Configuración Empresa'
        verbose_name_plural = 'Configuración Empresas'
        
    

class ConfServer(models.Model):
    nbServer = models.BigIntegerField(primary_key=True,verbose_name='Id del Servidor')
    nmServer = models.CharField(max_length=30, null=True, blank=True,verbose_name='Descripción del Servidor')
    hostServer = models.CharField(max_length=100, null=True, blank=True, verbose_name='Host')
    portServer = models.CharField(max_length=10, null=True, blank=True,verbose_name='Puerto')
    nbTipo = models.BigIntegerField(null=True, blank=True,verbose_name='Tipo')
    
    def __str__(self):
        return f'{self.nbServer}-{self.nmServer}'

    class Meta:
        db_table = 'conf_server'
        # managed = False
        verbose_name = 'Configuración Servidor'
        verbose_name_plural = 'Configuración Servidores'
        
        
class ConfSql(models.Model):
    nbSql = models.BigIntegerField(primary_key=True,verbose_name='Id del Proceso')
    txSql = models.TextField(null=True, blank=True,verbose_name='Sql Script')
    nmReporte = models.CharField(max_length=100, null=True, blank=True,verbose_name='Nombre del Proceso')
    txTabla = models.CharField(max_length=100, null=True, blank=True,verbose_name='Tabla de Inserción')
    txDescripcion = models.CharField(max_length=255, null=True, blank=True,verbose_name='Descripción del Proceso')
    nmProcedure_out = models.CharField(max_length=100, null=True, blank=True,verbose_name='Nombre del Procedimiento Extractor')
    nmProcedure_in = models.CharField(max_length=100, null=True, blank=True)
    
    def __str__(self):
        return f'{self.nbSql}-{self.txDescripcion}-{self.nmReporte}'

    class Meta:
        db_table = 'conf_sql'
        # managed = False
        verbose_name = 'Configuración Proceso Sql'
        verbose_name_plural = 'Configuración Procesos Sql'

class ConfTipo(models.Model):
    nbTipo = models.BigIntegerField(primary_key=True,verbose_name='Id')
    nmUsr = models.CharField(max_length=50, null=True, blank=True,verbose_name='usuario')
    txPass = models.TextField(null=True, blank=True,verbose_name='password')

    class Meta:
        db_table = 'conf_tipo'
        # managed = False
        verbose_name = 'Configuración Tipo Servidor'
        verbose_name_plural = 'Configuración Tipos de Servidores'

    def __str__(self):
        return f'{self.nbTipo}'