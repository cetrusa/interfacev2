from django.shortcuts import render, redirect
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.decorators import permission_required
from django.utils.decorators import method_decorator

from apps.users.decorators import registrar_auditoria
from django.urls import reverse_lazy, reverse
import requests
from django.http import HttpResponse, FileResponse, JsonResponse
from django.template.response import TemplateResponse

# Create your views here.
from django.views.generic import TemplateView, View
from apps.users.views import BaseView
from scripts.embedded.powerbi import AadService, PbiEmbedService

from scripts.conexion import Conexion
from scripts.extrae_bi.extrae_bi_call import Extrae_Bi
from scripts.extrae_bi.apipowerbi import Api_PowerBi
from scripts.config import ConfigBasic
from django.contrib.auth.decorators import login_required
from apps.users.decorators import registrar_auditoria
from scripts.embedded.powerbi import PbiEmbedService
from django.core.exceptions import ImproperlyConfigured
import json
from .tasks import actualiza_bi_task
from django.contrib import messages


with open("secret.json") as f:
    secret = json.loads(f.read())

    def get_secret(secret_name, secrets=secret):
        try:
            return secrets[secret_name]
        except:
            msg = "la variable %s no existe" % secret_name
            raise ImproperlyConfigured(msg)


class EliminarReporteFetched(View):
    def post(self, request, *args, **kwargs):
        if "report_fetched" in request.session:
            del request.session["report_fetched"]
            request.session.modified = True
        return JsonResponse({"success": True})


class ActualizacionBiPage(LoginRequiredMixin, BaseView):
    """
    Vista para la página de generación de actualización de información.

    Esta vista maneja la solicitud del usuario para generar la actualización de la base de datos y del Bi,
    iniciando una tarea en segundo plano y devolviendo el ID de dicha tarea.
    """

    # Nombre de la plantilla a utilizar para renderizar la vista
    template_name = "bi/actualizacion.html"

    # URL para redirigir en caso de que el usuario no esté autenticado
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(registrar_auditoria)
    @method_decorator(
        permission_required("permisos.actualizacion_bi", raise_exception=True)
    )
    def dispatch(self, request, *args, **kwargs):
        """
        Método para despachar la solicitud, aplicando decoradores de auditoría y
        permisos requeridos.
        """
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """
        Maneja la solicitud POST para iniciar el proceso de generación de la actualización de la base de datos y del Bi.

        Recoge los datos del formulario, valida la entrada y, si es válida,
        inicia una tarea asincrónica para generar la actualización de la base de datos y del Bi.
        """
        request.session["template_name"] = self.template_name
        database_name = request.POST.get("database_select")
        IdtReporteIni = request.POST.get("IdtReporteIni")
        IdtReporteFin = request.POST.get("IdtReporteFin")
        if not all([database_name, IdtReporteIni, IdtReporteFin]):
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Se debe seleccionar la base de datos y las fechas.",
                },
                status=400,
            )

        try:
            task = actualiza_bi_task.delay(database_name, IdtReporteIni, IdtReporteFin)
            # Guardamos el ID de la tarea en la sesión del usuario
            request.session["task_id"] = task.id
            return JsonResponse(
                {
                    "success": True,
                    "task_id": task.id,
                }
            )  # Devuelve el ID de la tarea al frontend
        except Exception as e:
            return JsonResponse({"success": False, "error_message": f"Error: {str(e)}"})

    def get(self, request, *args, **kwargs):
        """
        Maneja la solicitud GET, devolviendo la plantilla de la página del cubo de ventas.
        """
        database_name = request.session.get("database_name")
        if not database_name:
            messages.warning(request, "Debe seleccionar una empresa antes de continuar.")
            return redirect("home_app:panel")

        context = self.get_context_data(**kwargs)
        return self.render_to_response(context)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = "bi_app:actualizacion_bi"
        return context


def reporte_embed(request):
    # Utiliza la función get_embed_token_report() del módulo powerbi.py para obtener la información necesaria
    # para incrustar el informe de Power BI
    database_name = StaticPage.name
    ConfigBasic(database_name)
    clase = PbiEmbedService()
    embed_info_json = clase.get_embed_params_for_single_report(
        workspace_id=get_secret("GROUP_ID"), report_id=f"{StaticPage.report_id_powerbi}"
    )
    embed_info = json.loads(embed_info_json)

    # Imprime la información de embed_info para depurar
    # print("embed_info:", embed_info)
    # Verifica si se ha producido algún error al obtener la información de incrustación
    if "error" in embed_info:
        context = {"error_message": embed_info["error"]}
    else:
        context = {
            "EMBED_URL": embed_info["reportConfig"][0]["embedUrl"],
            "EMBED_ACCESS_TOKEN": embed_info["accessToken"],
            "REPORT_ID": embed_info["reportConfig"][0]["reportId"],
            "TOKEN_TYPE": 1,  # Establece en 1 para utilizar el token de incrustación
            "TOKEN_EXPIRY": embed_info["tokenExpiry"],  # Agrega tokenExpiry al contexto
            "form_url": "bi_app:reporte_embed2",  # Agrega form_url al contexto
        }

    return render(request, "bi/reporte_embedv2.html", context)


class IncrustarBiPage(LoginRequiredMixin, BaseView):
    template_name = "bi/reporte_embed.html"
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(registrar_auditoria)
    @method_decorator(permission_required("permisos.informe_bi", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        """
        Método para despachar la solicitud, aplicando decoradores de auditoría y
        permisos requeridos.
        """
        return super().dispatch(request, *args, **kwargs)

    def process_request(self, request):
        # print(request.session.items())
        database_name = request.POST.get("database_select")
        if not database_name:
            database_name = request.session.get("database_name")
            print(database_name)
            if not database_name:
                return redirect("home_app:panel")

        request.session["database_name"] = database_name
        try:
            config_basic = ConfigBasic(database_name)
            self.config = config_basic.config
            clase = PbiEmbedService()
            embed_info_json = clase.get_embed_params_for_single_report(
                workspace_id=get_secret("GROUP_ID"),
                report_id=self.config.get("report_id_powerbi"),
            )
            embed_info = json.loads(embed_info_json)
            if "error" in embed_info:
                context = {"error_message": embed_info["error"]}
            else:
                context = {
                    "EMBED_URL": embed_info["reportConfig"][0]["embedUrl"],
                    "EMBED_ACCESS_TOKEN": embed_info["accessToken"],
                    "REPORT_ID": embed_info["reportConfig"][0]["reportId"],
                    "TOKEN_TYPE": 1,  # Establece en 1 para utilizar el token de incrustación
                    "TOKEN_EXPIRY": embed_info[
                        "tokenExpiry"
                    ],  # Agrega tokenExpiry al contexto
                    "form_url": "bi_app:reporte_embed",  # Agrega form_url al contexto
                }

            return context
        except Exception as e:
            return {
                "error_message": f"Error: no se pudo ejecutar el script. Razón: {e}"
            }

    def post(self, request, *args, **kwargs):
        context = self.process_request(request)
        database_name = request.POST.get("database_select")
        request.session["database_name"] = database_name
        if "error_message" in context:
            context = {"error_message": context.get("error")}
        return render(request, self.template_name, context)

    def get(self, request, *args, **kwargs):
        """
        Maneja la solicitud GET, devolviendo la plantilla de la página del cubo de ventas.
        """
        database_name = request.session.get("database_name")
        if not database_name:
            messages.warning(request, "Debe seleccionar una empresa antes de continuar.")
            return redirect("home_app:panel")

        context = self.process_request(request)
        if "error_message" in context:
            context = {"error_message": context.get("error")}
        return render(request, self.template_name, context)
        

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        return context


# esta clase es para embebido normalito de la url publica
# class EmbedReportPage(LoginRequiredMixin, BaseView):
#     template_name = "bi/reporte_bi.html"
#     StaticPage.template_name = template_name
#     login_url = reverse_lazy('users_app:user-login')

#     @method_decorator(registrar_auditoria)
#     @method_decorator(permission_required('permisos.informe_bi', raise_exception=True))
#     def dispatch(self, request, *args, **kwargs):
#         return super().dispatch(request, *args, **kwargs)

#     def post(self, request, *args, **kwargs):
#         database_name = request.POST.get('database_select')

#         if not database_name:
#             return redirect('home_app:panel')

#         request.session['database_name'] = database_name
#         try:
#             config = ConfigBasic(database_name)
#             url_powerbi= config.StaticPage.url_powerbi
#             return JsonResponse({'url_powerbi': url_powerbi})
#         except Exception as e:
#             return JsonResponse({'success': False, 'error_message': f"Error: no se pudo ejecutar el script. Razón: {e}"})

#     def get(self, request, *args, **kwargs):
#         return super().get(request, *args, **kwargs)

#     def get_context_data(self, **kwargs):
#         context = super().get_context_data(**kwargs)
#         context['form_url'] = 'bi_app:reporte_bi'
#         return context
