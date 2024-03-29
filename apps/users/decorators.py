from functools import wraps
from apps.users.models import RegistroAuditoria
import json
from django.utils import timezone
import geocoder
from scripts.StaticPage import StaticPage


def grabar_auditoria(request, detalle):
    # ip = request.META.get('HTTP_X_FORWARDED_FOR', request.META.get('REMOTE_ADDR', None))
    ip = (
        request.META.get("HTTP_X_FORWARDED_FOR", request.META.get("REMOTE_ADDR", ""))
        .split(",")[0]
        .strip()
    )
    gc = geocoder.ip(ip)

    usuario = request.user
    transaccion = request.path_info
    database_name = StaticPage.name
    city = gc.city

    registro = RegistroAuditoria(
        usuario=usuario,
        ip=ip,
        transaccion=transaccion,
        detalle=detalle,
        database_name=database_name,
        city=city,
    )
    registro.save()


def registrar_auditoria(view_func):
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        # Ejecutamos la vista y obtenemos la respuesta
        response = view_func(request, *args, **kwargs)

        # Grabamos los datos de auditoría en la base de datos
        detalle = {
            "metodo": request.method,
            "datos": (
                request.POST.dict() if request.method == "POST" else request.GET.dict()
            ),
        }
        grabar_auditoria(request, detalle)

        return response

    return wrapper
