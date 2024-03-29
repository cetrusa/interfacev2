import logging
from django.views import View
from django.shortcuts import render, redirect
from django.contrib import messages
from django.urls import reverse_lazy
import zipfile
import os
from django.contrib.auth.mixins import LoginRequiredMixin
import sqlalchemy
from apps.home.tasks import cargue_zip_task, cargue_plano_task
from scripts.config import ConfigBasic
from scripts.StaticPage import StaticPage, DinamicPage
import re
from django.conf import settings
from sqlalchemy import create_engine, text
from apps.users.views import BaseView
from django.utils.decorators import method_decorator
from apps.users.decorators import registrar_auditoria
from django.views.decorators.csrf import csrf_exempt, csrf_protect
from django.contrib.auth.decorators import user_passes_test
from django.contrib.auth.decorators import login_required, permission_required
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from django.http import request
from scripts.extrae_bi.cargue_zip import CargueZip


class UploadZipView(LoginRequiredMixin, BaseView):
    template_name = "cargues/cargue.html"
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(registrar_auditoria)
    # @method_decorator(permission_required("permisos.cubo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        request.session["template_name"] = self.template_name
        database_name = request.POST.get("database_select")
        zip_file = request.FILES.get("zip_file")
        
        if not zip_file:
            return JsonResponse(
                {"success": False, "error_message": "No se subió ningún archivo."},
                status=400,
            )

        if not database_name:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Se debe seleccionar la base de datos.",
                }
            )

        # Guarda el archivo en el servidor
        file_path = self.save_zip_file(zip_file)
        zip_file_path = file_path

        if not database_name:
            return redirect("home_app:panel")

        if not database_name:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Se debe seleccionar la base de datos.",
                }
            )

        request.session["database_name"] = database_name
        try:
            # cargue_zip = CargueZip(database_name)
            # cargue_zip.procesar_zip()
            print("aqui estoy listo para iniciar la tarea asicrona")
            task = cargue_zip_task.delay(database_name, zip_file_path)

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

    def save_zip_file(self, zip_file):
        media_path = os.path.join("media", zip_file.name)
        with open(media_path, "wb+") as destination:
            for chunk in zip_file.chunks():
                destination.write(chunk)
        return media_path

    def get(self, request, *args, **kwargs):
        return super().get(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = "cargues_app:cargue"
        return context


class UploadPlanoFilesView(LoginRequiredMixin, BaseView):
    template_name = "cargues/cargue_planos_tsol.html"
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(registrar_auditoria)
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        request.session["template_name"] = self.template_name
        database_name = request.POST.get("database_select")
        plano_files = request.FILES.getlist("plano_files")  # Obtener múltiples archivos

        if not plano_files:
            return JsonResponse(
                {"success": False, "error_message": "No se subieron archivos."},
                status=400,
            )
        else:
            # Guarda los archivos en el servidor
            for file in plano_files:
                try:
                    self.save_plano_file(database_name, file)
                except Exception as e:
                    return JsonResponse(
                        {
                            "success": False,
                            "error_message": f"Error al procesar archivo {file.name}: {str(e)}",
                        }
                    )

        if not database_name:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Se debe seleccionar la base de datos.",
                }
            )
     
        request.session["database_name"] = database_name
        
        try:
            print("aqui estoy listo para iniciar la tarea asicrona")
            task = cargue_plano_task.delay(database_name)
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

    def save_plano_file(self, database_name, file):
        # Construir la ruta del archivo. Se crea una carpeta dentro de 'media' con el nombre de la base de datos
        # y una subcarpeta 'plano_files' para almacenar los archivos.
        media_path = os.path.join("media", database_name, file.name)

        # Asegurarse de que las carpetas existan o crearlas
        os.makedirs(os.path.dirname(media_path), exist_ok=True)

        # Escribir el archivo en el sistema de archivos
        with open(media_path, "wb+") as destination:
            for chunk in file.chunks():
                destination.write(chunk)

        # Devolver la ruta del archivo guardado
        return media_path

    def get(self, request, *args, **kwargs):
        return super().get(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context[
            "form_url"
        ] = "cargues_app:cargue_planos_tsol"  # Actualizar según tu configuración de URL
        return context
