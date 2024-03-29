#
from django.urls import path

from . import views

app_name = "bi_app"

urlpatterns = [
    path(
        'actualizacion-bi/', 
        views.ActualizacionBiPage.as_view(),
        name='actualizacion_bi',
    ),
    # path(
    #     'reporte-bi/', 
    #     views.EmbedReportPage.as_view(),
    #     name='reporte_bi',
    # ),
    path(
        'reporte_embed/', 
        views.IncrustarBiPage.as_view(),
        name='reporte_embed',
    ),
    # path('embed_info/', views.EmbedInfoView.as_view(), name='embed_info'),
    path('reporte_embed2/', views.reporte_embed, name='reporte_embed2'),
    path('eliminar_reporte_fetched/', views.EliminarReporteFetched.as_view(), name='eliminar_reporte_fetched'),
    
]