#
from django.urls import path

from . import views

app_name = "home_app"

urlpatterns = [
    path(
        'panel/', 
        views.HomePage.as_view(),
        name='panel',
    ),
    path(
        'panel_left_planos/', 
        views.HomePage.as_view(),
        name='panel_left_planos',
    ),
    path(
        'cubo/', 
        views.CuboPage.as_view(), 
        name='cubo',
    ),
    path(
        'interface/', 
        views.InterfacePage.as_view(), 
        name='interface',
    ),
    path(
        'actualizacion/', 
        views.ActualizacionPage.as_view(), 
        name='actualizacion',
    ),
    path(
        'plano/', 
        views.PlanoPage.as_view(), 
        name='plano',
    ),
    path(
        'prueba/', 
        views.PruebaPage.as_view(), 
        name='prueba',
    ),
   path('download_file/', views.DownloadFileView.as_view(), name='download_file'),
   path('delete_file/', views.DeleteFileView.as_view(), name='delete_file'),
   path('check-task-status/', views.CheckTaskStatusView.as_view(), name='check_task_status'),
]