#
from django.urls import path

from . import views

app_name = "cargues_app"

urlpatterns = [
    path(
        'cargue/', 
        views.UploadZipView.as_view(),
        name='cargue',
    ),
    path(
        'cargue_planos_tsol/', 
        views.UploadPlanoFilesView.as_view(),
        name='cargue_planos_tsol',
    )
]