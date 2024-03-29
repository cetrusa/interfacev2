from django.contrib import admin

# Register your models here.
from .models import ConfDt, ConfEmpresas, ConfServer, ConfSql, ConfTipo

class ConfDtAdmin(admin.ModelAdmin):

    def get_verbose_fields(self, obj):
        fields = []
        for field in obj._meta.fields:
            value = getattr(obj, field.name)
            if value:
                fields.append('{}: {}'.format(field.verbose_name, str(value)))
        return ', '.join(fields)
    get_verbose_fields.short_description = 'Rangos de Fechas'

    list_display = ('get_verbose_fields',)
    
class ConfEmpresasAdmin(admin.ModelAdmin):

    list_display = ('id','name','nmEmpresa')
    
class ConfServerAdmin(admin.ModelAdmin):

    list_display = ('nbServer','nmServer')

class ConfSqlAdmin(admin.ModelAdmin):

    list_display = ('txDescripcion',)
    
class ConfTipoAdmin(admin.ModelAdmin):

    list_display = ('nbTipo',)
    
        
admin.site.register(ConfDt,ConfDtAdmin)
admin.site.register(ConfEmpresas,ConfEmpresasAdmin)
admin.site.register(ConfServer,ConfServerAdmin)
admin.site.register(ConfSql,ConfSqlAdmin)
admin.site.register(ConfTipo)