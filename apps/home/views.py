from django.contrib import messages
import subprocess
from django.shortcuts import render, redirect
from django.contrib.auth.mixins import LoginRequiredMixin
from django.urls import reverse_lazy, reverse
import os
from django.http import HttpResponse, FileResponse, JsonResponse
import io
from django.views.generic import View
from django.utils.decorators import method_decorator
from apps.users.decorators import registrar_auditoria
from django.views.decorators.csrf import csrf_exempt, csrf_protect
from django.contrib.auth.decorators import user_passes_test
from django.contrib.auth.decorators import login_required, permission_required
from django.http import HttpResponseRedirect
from scripts.conexion import Conexion
from scripts.config import ConfigBasic
from scripts.StaticPage import StaticPage, DinamicPage
from scripts.extrae_bi.extrae_bi_call import Extrae_Bi
from scripts.extrae_bi.interface import InterfaceContable
from django.contrib.auth.mixins import UserPassesTestMixin
from .tasks import cubo_ventas_task, interface_task, plano_task, extrae_bi_task
from django.http import JsonResponse
from django.views import View

# importaciones para celery
# from celery.result import AsyncResult

# importaciones para rq
from django_rq import get_queue
from rq.job import Job
from rq.job import NoSuchJobError
from django_rq import get_connection

from django.views.generic import TemplateView
from apps.users.views import BaseView


class HomePage(LoginRequiredMixin, BaseView):
    template_name = "home/panel.html"

    login_url = reverse_lazy("users_app:user-login")

    def post(self, request, *args, **kwargs):
        request.session["template_name"] = self.template_name
        database_name = request.POST.get("database_select")
        if not database_name:
            return redirect("home_app:panel")

        request.session["database_name"] = database_name
        StaticPage.name = database_name

        return redirect("home_app:panel")

    def get(self, request, *args, **kwargs):
        return super().get(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = "home_app:panel"
        return context


class DownloadFileView(BaseView):
    login_url = reverse_lazy("users_app:user-login")

    def get(self, request):
        template_name = request.session.get("template_name", "home/panel.html")
        file_path = request.session.get("file_path")
        file_name = request.session.get("file_name")

        f = None  # Define f fuera del bloque try para que esté disponible en el bloque except

        if file_path and file_name:
            try:
                f = open(file_path, "rb")
                response = FileResponse(f)
                response["Content-Disposition"] = f'attachment; filename="{file_name}"'
                return response
            except IOError:
                # Si f ha sido asignado y está abierto, ciérralo.
                if f:
                    f.close()
                messages.error(request, "Error al abrir el archivo")
        else:
            messages.error(request, "Archivo no encontrado")
        return render(request, "home/panel.html", {"template_name": template_name})


class DeleteFileView(BaseView):
    login_url = reverse_lazy("users_app:user-login")

    def post(self, request):
        template_name = request.session.get("template_name")
        file_path = request.session.get("file_path")

        if file_path is None:
            return JsonResponse(
                {"success": False, "error_message": "No hay archivo para eliminar."}
            )

        try:
            os.remove(file_path)
            # Borra la ruta del archivo y el nombre del archivo de la sesión.
            del request.session["file_path"]
            del request.session["file_name"]
            return HttpResponseRedirect(reverse(self.template_name))
        except FileNotFoundError:
            return JsonResponse(
                {"success": False, "error_message": "El archivo no existe."}
            )
        except Exception as e:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": f"Error: no se pudo ejecutar el script. Razón: {str(e)}",
                }
            )


class CheckTaskStatusView(BaseView):
    """
    Vista para comprobar el estado de una tarea asincrónica y recuperar su resultado.

    Esta vista espera recibir un ID de tarea y devuelve el estado de la tarea,
    así como su resultado si la tarea ha finalizado correctamente.
    """

    def post(self, request, *args, **kwargs):
        """
        Maneja la solicitud POST para comprobar el estado de una tarea asincrónica.

        :param request: Objeto HttpRequest.
        :return: JsonResponse con el estado de la tarea o un mensaje de error.
        """
        task_id = request.POST.get("task_id")
        if not task_id:
            return JsonResponse({"error": "No task ID provided"}, status=400)

        connection = get_connection()
        try:
            job = Job.fetch(task_id, connection=connection)
            if job.is_finished:
                return self.handle_finished_job(request, job)
            elif job.is_failed:
                return JsonResponse({"error": "Task execution failed"}, status=500)
            else:
                return JsonResponse({"status": job.get_status()})

        except NoSuchJobError:
            return JsonResponse({"status": "notfound", "error": "Task not found"})
        


    def handle_finished_job(self, request, job):
        """
        Maneja el caso cuando una tarea ha finalizado.

        :param request: Objeto HttpRequest.
        :param job: Objeto Job que representa la tarea finalizada.
        :return: JsonResponse con el resultado de la tarea o un mensaje de error.
        """
        resultado = job.result
        if resultado is None:
            return JsonResponse({"error": "Task finished with no result"}, status=500)

        # Si el resultado fue exitoso pero no incluye los detalles del archivo, continúa de todas formas
        if resultado.get("success"):
            response_data = {"status": "finished", "result": resultado}

            # Solo agrega file_path y file_name a la sesión si están presentes
            if "file_path" in resultado and "file_name" in resultado:
                request.session["file_path"] = resultado["file_path"]
                request.session["file_name"] = resultado["file_name"]
                # Podrías querer incluir también esta información en la respuesta
                response_data.update({"file_path": resultado["file_path"], "file_name": resultado["file_name"]})

            return JsonResponse(response_data)

        # Si el resultado no fue exitoso o no cumple con las expectativas
        return JsonResponse(
            {"error": "Task completed but result is not as expected"}, status=500
        )



class CuboPage(LoginRequiredMixin, BaseView):
    """
    Vista para la página de generación del Cubo de Ventas.

    Esta vista maneja la solicitud del usuario para generar un cubo de ventas,
    iniciando una tarea en segundo plano y devolviendo el ID de dicha tarea.
    """

    # Nombre de la plantilla a utilizar para renderizar la vista
    template_name = "home/cubo.html"

    # URL para redirigir en caso de que el usuario no esté autenticado
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(registrar_auditoria)
    @method_decorator(permission_required("permisos.cubo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        """
        Método para despachar la solicitud, aplicando decoradores de auditoría y
        permisos requeridos.
        """
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """
        Maneja la solicitud POST para iniciar el proceso de generación del cubo de ventas.

        Recoge los datos del formulario, valida la entrada y, si es válida,
        inicia una tarea asincrónica para generar el cubo de ventas.
        """
        database_name = request.POST.get("database_select")
        IdtReporteIni = request.POST.get("IdtReporteIni")
        IdtReporteFin = request.POST.get("IdtReporteFin")
        request.session["template_name"] = self.template_name

        if not all([database_name, IdtReporteIni, IdtReporteFin]):
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Se debe seleccionar la base de datos y las fechas.",
                },
                status=400,
            )

        try:
            task = cubo_ventas_task.delay(database_name, IdtReporteIni, IdtReporteFin)
            request.session["task_id"] = task.id
            return JsonResponse({"success": True, "task_id": task.id})
        except Exception as e:
            return JsonResponse(
                {"success": False, "error_message": f"Error: {str(e)}"}, status=500
            )

    def get(self, request, *args, **kwargs):
        """
        Maneja la solicitud GET, devolviendo la plantilla de la página del cubo de ventas.
        """
        context = self.get_context_data(**kwargs)
        return self.render_to_response(context)

    def get_context_data(self, **kwargs):
        """
        Obtiene el contexto necesario para la plantilla.

        :return: Contexto que incluye la URL del formulario.
        """
        context = super().get_context_data(**kwargs)
        context["form_url"] = "home_app:cubo"
        return context


class InterfacePage(LoginRequiredMixin, BaseView):
    """
    Vista para la página de generación de Interface Contable.

    Esta vista maneja la solicitud del usuario para generar una interface contable,
    iniciando una tarea en segundo plano y devolviendo el ID de dicha tarea.
    """

    # Nombre de la plantilla a utilizar para renderizar la vista
    template_name = "home/interface.html"

    # URL para redirigir en caso de que el usuario no esté autenticado
    login_url = reverse_lazy("users_app:user-login")
    print("aqui estoy en interfacepage")

    @method_decorator(registrar_auditoria)
    @method_decorator(permission_required("permisos.interface", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        """
        Método para despachar la solicitud, aplicando decoradores de auditoría y
        permisos requeridos.
        """
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """
        Maneja la solicitud POST para iniciar el proceso de generación de la interface.

        Recoge los datos del formulario, valida la entrada y, si es válida,
        inicia una tarea asincrónica para generar el cubo de ventas.
        """
        database_name = request.POST.get("database_select")
        IdtReporteIni = request.POST.get("IdtReporteIni")
        IdtReporteFin = request.POST.get("IdtReporteFin")
        request.session["template_name"] = self.template_name
        print("aqui estoy en interfacepage en el post")
        if not all([database_name, IdtReporteIni, IdtReporteFin]):
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Se debe seleccionar la base de datos y las fechas.",
                },
                status=400,
            )

        try:
            print(
                "aqui estoy en interfacepage en el try antes de task = interface_task.delay"
            )
            task = interface_task.delay(database_name, IdtReporteIni, IdtReporteFin)
            print(
                "aqui estoy en interfacepage en el try despues de task = interface_task.delay"
            )
            request.session["task_id"] = task.id
            return JsonResponse({"success": True, "task_id": task.id})
        except Exception as e:
            return JsonResponse(
                {"success": False, "error_message": f"Error: {str(e)}"}, status=500
            )

    def get(self, request, *args, **kwargs):
        """
        Maneja la solicitud GET, devolviendo la plantilla de la página de la interface.
        """
        context = self.get_context_data(**kwargs)
        return self.render_to_response(context)

    def get_context_data(self, **kwargs):
        """
        Obtiene el contexto necesario para la plantilla.

        :return: Contexto que incluye la URL del formulario.
        """
        context = super().get_context_data(**kwargs)
        context["form_url"] = "home_app:interface"
        return context


"""
class InterfacePage(LoginRequiredMixin, BaseView):
    template_name = "home/interface.html"

    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(registrar_auditoria)
    @method_decorator(permission_required("permisos.interface", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        request.session["template_name"] = self.template_name
        database_name = request.POST.get("database_select")
        IdtReporteIni = request.POST.get("IdtReporteIni")
        IdtReporteFin = request.POST.get("IdtReporteFin")

        if not database_name:
            return redirect("home_app:panel")

        if not database_name or not IdtReporteIni or not IdtReporteFin:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Se debe seleccionar la base de datos y las fechas.",
                }
            )

        request.session["database_name"] = database_name
        IdtReporteIni = request.POST.get("IdtReporteIni")
        IdtReporteFin = request.POST.get("IdtReporteFin")
        StaticPage.name = database_name
        try:
            task = interface_task.delay(database_name, IdtReporteIni, IdtReporteFin)
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
        return super().get(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = "home_app:interface"
        return context
"""


class PlanoPage(LoginRequiredMixin, BaseView):
    """
    Vista para la página de generación de Interface Contable.

    Esta vista maneja la solicitud del usuario para generar una interface contable,
    iniciando una tarea en segundo plano y devolviendo el ID de dicha tarea.
    """

    # Nombre de la plantilla a utilizar para renderizar la vista
    template_name = "home/plano.html"

    # URL para redirigir en caso de que el usuario no esté autenticado
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(registrar_auditoria)
    @method_decorator(permission_required("permisos.plano", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        """
        Método para despachar la solicitud, aplicando decoradores de auditoría y
        permisos requeridos.
        """
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """
        Maneja la solicitud POST para iniciar el proceso de generación de la interface.

        Recoge los datos del formulario, valida la entrada y, si es válida,
        inicia una tarea asincrónica para generar el cubo de ventas.
        """
        database_name = request.POST.get("database_select")
        IdtReporteIni = request.POST.get("IdtReporteIni")
        IdtReporteFin = request.POST.get("IdtReporteFin")
        request.session["template_name"] = self.template_name

        if not all([database_name, IdtReporteIni, IdtReporteFin]):
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Se debe seleccionar la base de datos y las fechas.",
                },
                status=400,
            )

        try:
            task = plano_task.delay(database_name, IdtReporteIni, IdtReporteFin)
            request.session["task_id"] = task.id
            return JsonResponse({"success": True, "task_id": task.id})
        except Exception as e:
            return JsonResponse(
                {"success": False, "error_message": f"Error: {str(e)}"}, status=500
            )

    def get(self, request, *args, **kwargs):
        """
        Maneja la solicitud GET, devolviendo la plantilla de la página de la interface.
        """
        context = self.get_context_data(**kwargs)
        return self.render_to_response(context)

    def get_context_data(self, **kwargs):
        """
        Obtiene el contexto necesario para la plantilla.

        :return: Contexto que incluye la URL del formulario.
        """
        context = super().get_context_data(**kwargs)
        context["form_url"] = "home_app:plano"
        return context


"""
class PlanoPage(LoginRequiredMixin, BaseView):
    template_name = "home/plano.html"

    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(registrar_auditoria)
    @method_decorator(permission_required("permisos.plano", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        request.session["template_name"] = self.template_name
        database_name = request.POST.get("database_select")
        IdtReporteIni = request.POST.get("IdtReporteIni")
        IdtReporteFin = request.POST.get("IdtReporteFin")

        if not database_name:
            return redirect("home_app:panel")

        if not database_name or not IdtReporteIni or not IdtReporteFin:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Se debe seleccionar la base de datos y las fechas.",
                }
            )

        request.session["database_name"] = database_name
        StaticPage.name = database_name

        try:
            task = plano_task.delay(database_name, IdtReporteIni, IdtReporteFin)
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
        return super().get(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = "home_app:plano"
        return context
"""


class ActualizacionPage(LoginRequiredMixin, BaseView):
    """
    Vista para la página de generación de actualización de información.

    Esta vista maneja la solicitud del usuario para generar la actualización de la base de datos,
    iniciando una tarea en segundo plano y devolviendo el ID de dicha tarea.
    """

    # Nombre de la plantilla a utilizar para renderizar la vista
    template_name = "home/actualizacion.html"

    # URL para redirigir en caso de que el usuario no esté autenticado
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(registrar_auditoria)
    @method_decorator(
        permission_required("permisos.actualizar_base", raise_exception=True)
    )
    def dispatch(self, request, *args, **kwargs):
        """
        Método para despachar la solicitud, aplicando decoradores de auditoría y
        permisos requeridos.
        """
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """
        Maneja la solicitud POST para iniciar el proceso de generación de la actualización de la base de datos.

        Recoge los datos del formulario, valida la entrada y, si es válida,
        inicia una tarea asincrónica para generar la actualización de la base de datos.
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
            task = extrae_bi_task.delay(database_name, IdtReporteIni, IdtReporteFin)
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
        context = self.get_context_data(**kwargs)
        return self.render_to_response(context)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = "home_app:actualizacion"
        return context


class PruebaPage(LoginRequiredMixin, BaseView):
    template_name = "home/prueba.html"

    login_url = reverse_lazy("users_app:user-login")

    def post(self, request, *args, **kwargs):
        request.session["template_name"] = self.template_name
        # database_name = request.session.get('database_name') or request.POST.get('database_select')
        database_name = request.POST.get("database_select")
        if not database_name:
            return redirect("home_app:panel")

        request.session["database_name"] = database_name
        StaticPage.name = database_name
        if not database_name:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Debe seleccionar una base de datos.",
                }
            )

    def get(self, request, *args, **kwargs):
        return super().get(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = "home_app:prueba"
        return context
