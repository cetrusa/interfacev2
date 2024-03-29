from django.db import models
from django.db.models import JSONField
from django.conf import settings

# Create your models here.
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin

from .managers import UserManager

from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin
from django.db import models
from apps.permisos.models import ConfEmpresas

# class Database(models.Model):
#     name = models.CharField(max_length=100)

#     def __str__(self):
#         return self.name


class User(AbstractBaseUser, PermissionsMixin):
    GENDER_CHOISES = (
        ("M", "Masculino"),
        ("F", "Femenino"),
        ("O", "Otros"),
    )
    username = models.CharField(max_length=10, unique=True, verbose_name="Usuario")
    email = models.EmailField(verbose_name="e-mail")
    nombres = models.CharField(max_length=50, verbose_name="Nombres")
    apellidos = models.CharField(max_length=50, verbose_name="Apellidos")
    genero = models.CharField(
        max_length=1, choices=GENDER_CHOISES, blank=True, verbose_name="Género"
    )
    codregistro = models.CharField(
        max_length=6, blank=True, verbose_name="Código de Registro"
    )
    conf_empresas = models.ManyToManyField(
        ConfEmpresas, blank=True, verbose_name="Bases de datos"
    )
    #
    is_staff = models.BooleanField(default=False, verbose_name="Administrador")
    is_active = models.BooleanField(default=False, verbose_name="Activo")
    is_superuser = models.BooleanField(default=False, verbose_name="Superusuario")

    USERNAME_FIELD = "username"

    REQUIRED_FIELDS = ["email"]

    objects = UserManager()

    def get_short_name(self):
        return self.username

    def get_full_name(self):
        return self.nombres + " " + self.apellidos

    def __str__(self):
        return (
            self.username
            + " - "
            + self.nombres
            + " - "
            + self.apellidos
            + " - "
            + self.email
        )


class UserProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    conf_empresas = models.ManyToManyField(ConfEmpresas)


class RegistroAuditoria(models.Model):
    usuario = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    fecha_hora = models.DateTimeField(auto_now_add=True)
    ip = models.GenericIPAddressField()
    transaccion = models.CharField(max_length=255)
    detalle = JSONField()
    database_name = models.CharField(max_length=100, null=True, blank=True)
    city = models.CharField(max_length=100, null=True, blank=True)

    def __str__(self):
        return (
            self.fecha_hora.strftime("%Y-%m-%d %H:%M:%S")
            + " - "
            + self.database_name
            + " - "
            + self.user.get_full_name()
            + " - "
            + (str(self.city) if self.city else "")
        )

    class Meta:
        db_table = "registro_auditoria"
        verbose_name = "Registro de Auditoria"
        verbose_name_plural = "Registro de Auditorias"
