from django.contrib import messages
import subprocess
from django.shortcuts import render, redirect
from django.contrib.auth.mixins import LoginRequiredMixin
from django.urls import reverse_lazy, reverse
import os
from django.http import HttpResponse, FileResponse, HttpResponseNotFound, JsonResponse
import io
from django.views.generic import View
import requests
from django.utils.decorators import method_decorator
from apps.users.decorators import registrar_auditoria
from django.views.decorators.csrf import csrf_exempt, csrf_protect
from django.contrib.auth.decorators import user_passes_test
from django.contrib.auth.decorators import login_required, permission_required
from django.http import HttpResponseRedirect
from scripts.conexion import Conexion
from scripts.config import ConfigBasic
from scripts.StaticPage import StaticPage
from scripts.extrae_bi.extrae_bi_call import Extrae_Bi
from scripts.extrae_bi.cubo import Cubo_Ventas
from scripts.extrae_bi.interface import Interface_Contable
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
    StaticPage.template_name = template_name
    login_url = reverse_lazy("users_app:user-login")

    def post(self, request, *args, **kwargs):
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


class Planos_Page(LoginRequiredMixin, BaseView):
    template_name = "home/panel_left_planos.html"
    StaticPage.template_name = template_name
    login_url = reverse_lazy("users_app:user-login")

    def post(self, request, *args, **kwargs):
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
        context["form_url"] = "home_app:panel_left_planos"
        return context


class DownloadFileView(View):
    template_name = StaticPage.template_name
    login_url = reverse_lazy("users_app:user-login")

    def get(self, request):
        form_url = request.session.get("form_url")  # URL de retorno
        file_path = request.session.get("file_path")
        file_name = request.session.get("file_name")

        if file_path and file_name:
            try:
                with open(file_path, "rb") as f:
                    response = FileResponse(f)
                    response[
                        "Content-Disposition"
                    ] = f'attachment; filename="{file_name}"'
                    return response
            except IOError:
                messages.error(request, "Error al abrir el archivo")
        else:
            messages.error(request, "Archivo no encontrado")

        # Redirigir a la URL de retorno o a una URL por defecto
        return redirect(form_url) if form_url else redirect("home_app:panel")


class DeleteFileView(View):
    template_name = StaticPage.template_name
    login_url = reverse_lazy("users_app:user-login")

    def post(self, request):
        file_path = request.session.get("file_path")

        if not file_path or not os.path.exists(file_path):
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "No hay archivo para eliminar o no existe.",
                }
            )

        try:
            os.remove(file_path)
            del request.session["file_path"]
            del request.session["file_name"]
            return JsonResponse({"success": True})
        except Exception as e:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": f"Error: no se pudo ejecutar el script. Razón: {str(e)}",
                }
            )


class CheckTaskStatusView(View):
    def post(self, request, *args, **kwargs):
        task_id = request.POST.get("task_id")
        if not task_id:
            return JsonResponse({"error": "No task ID provided"}, status=400)

        connection = get_connection()

        try:
            job = Job.fetch(task_id, connection=connection)
            if job.is_finished:
                if "file_path" in job.result and "file_name" in job.result:
                    request.session["file_path"] = job.result["file_path"]
                    request.session["file_name"] = job.result["file_name"]
                return JsonResponse({"status": "finished", "result": job.result})
            elif job.is_failed:
                return JsonResponse(
                    {"status": "failed", "error": "Task execution failed"}, status=500
                )
            else:
                return JsonResponse({"status": job.get_status()})
        except NoSuchJobError:
            return JsonResponse({"status": "notfound", "error": "Task not found"})


class CuboPage(LoginRequiredMixin, BaseView):
    template_name = "home/cubo.html"
    StaticPage.template_name = template_name
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(registrar_auditoria)
    @method_decorator(permission_required("permisos.cubo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
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
            task = cubo_ventas_task.delay(database_name, IdtReporteIni, IdtReporteFin)
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
        context["form_url"] = "home_app:cubo"
        return context


class InterfacePage(LoginRequiredMixin, BaseView):
    template_name = "home/interface.html"
    StaticPage.template_name = template_name
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(registrar_auditoria)
    @method_decorator(permission_required("permisos.interface", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
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


class PlanoPage(LoginRequiredMixin, BaseView):
    template_name = "home/plano.html"
    StaticPage.template_name = template_name
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(registrar_auditoria)
    @method_decorator(permission_required("permisos.plano", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
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


class ActualizacionPage(LoginRequiredMixin, BaseView):
    template_name = "home/actualizacion.html"
    StaticPage.template_name = template_name
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(registrar_auditoria)
    @method_decorator(
        permission_required("permisos.actualizar_base", raise_exception=True)
    )
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        database_name = request.POST.get("database_select")
        # database_name = request.session.get('database_name') or request.POST.get('database_select')
        if not database_name:
            return redirect("home_app:panel")

        request.session["database_name"] = database_name
        StaticPage.name = database_name
        try:
            task = extrae_bi_task.delay(database_name)
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
        context["form_url"] = "home_app:actualizacion"
        return context


class PruebaPage(LoginRequiredMixin, BaseView):
    template_name = "home/prueba.html"
    login_url = reverse_lazy("users_app:user-login")

    def post(self, request, *args, **kwargs):
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
