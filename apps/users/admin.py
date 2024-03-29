from django import forms
from django.contrib import admin
from .models import User, RegistroAuditoria, UserProfile
from django.contrib.auth.models import Permission
from django.db.models import Q
from django.contrib.auth.models import Permission
from apps.permisos.models import ConfEmpresas


class UserProfileInline(admin.StackedInline):
    model = UserProfile


class UserAdminForm(forms.ModelForm):
    conf_empresas = forms.ModelMultipleChoiceField(
        queryset=ConfEmpresas.objects.all(),
        widget=forms.CheckboxSelectMultiple,
        required=False,
    )

    class Meta:
        model = User
        fields = "__all__"


class UserAdmin(admin.ModelAdmin):
    form = UserAdminForm
    list_display = (
        "username",
        "email",
        "nombres",
        "apellidos",
        "genero",
        "codregistro",
    )
    filter_horizontal = ("conf_empresas",)

    # def get_form(self, request, obj=None, **kwargs):
    #     form = super().get_form(request, obj, **kwargs)
    #     # Filtra los permisos para incluir permisos personalizados y permisos predeterminados
    #     permisos_personalizados = Q(content_type__app_label='apps.permisos')
    #     permisos_predeterminados = Q(content_type__app_label__in=['auth', 'contenttypes', 'sessions', 'admin', 'models'])
    #     form.base_fields['user_permissions'].queryset = Permission.objects.filter(
    #         permisos_personalizados | permisos_predeterminados
    #     )
    #     return form


# class DatabaseAdmin(admin.ModelAdmin):
#     form = UserAdminForm
#     list_display = ('name')

admin.site.register(RegistroAuditoria)
# admin.site.register(Database)
admin.site.register(User, UserAdmin)
admin.site.register(Permission)
